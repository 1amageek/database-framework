import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum PQIndexStorageKey: Int {
    case codebooks = 0
    case metadata = 1
    case codes = 2
    case vectors = 3
}

struct PQIndexReader: Sendable {
    private let subspace: Subspace
    private let dimensions: Int
    private let metric: VectorMetric
    private let parameters: PQParameters

    init(
        subspace: Subspace,
        dimensions: Int,
        metric: VectorMetric,
        parameters: PQParameters
    ) throws(VectorIndexError) {
        guard dimensions > 0 else {
            throw .invalidArgument("PQ vector dimensions must be positive")
        }
        guard dimensions % parameters.m == 0 else {
            throw .invalidArgument(
                "PQ vector dimensions \(dimensions) are not divisible by \(parameters.m) subquantizers"
            )
        }
        self.subspace = subspace
        self.dimensions = dimensions
        self.metric = metric
        self.parameters = parameters
    }

    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }
        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        let metadata = try await loadMetadata(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        if metadata.vectorCount == 0 {
            guard try await !hasStoredVector(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            ) else {
                throw VectorIndexError.invalidStructure(
                    "PQ metadata reports an empty index with persisted vectors"
                )
            }
            return try VectorSearchAccumulator(
                k: k,
                workMeter: workMeter
            ).finish()
        }

        let codebooks = try await loadCodebookViews(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        if !metadata.trained {
            guard codebooks.isEmpty else {
                throw VectorIndexError.invalidStructure(
                    "PQ untrained metadata has persisted codebooks"
                )
            }
            return try await exactSearch(
                queryVector: queryVector,
                k: k,
                expectedVectorCount: metadata.vectorCount,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
        }
        guard !codebooks.isEmpty else {
            throw VectorIndexError.invalidStructure("PQ index not trained")
        }

        let quantizer = try PersistedProductQuantizer(
            dimensions: dimensions,
            subquantizerCount: parameters.m,
            centroidCount: parameters.ksub,
            codebooks: codebooks
        )
        try workMeter.consume(
            pqEncodingWorkUnits(),
            at: .indexScan
        )
        let (distanceTableEntries, distanceTableOverflow) = parameters.m
            .multipliedReportingOverflow(by: parameters.ksub)
        guard !distanceTableOverflow else {
            throw VectorIndexError.invalidArgument(
                "PQ distance table exceeds the current platform limit"
            )
        }
        let distanceTableEntryCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<Double>.stride)
        ).multiplied(
            by: UInt64(distanceTableEntries)
        ).bytes
        let distanceTableReservation = try workMeter.reserveIntermediate(
            bytes: metric == .cosine
                ? try DatabaseIntermediateFootprint(
                    bytes: distanceTableEntryCount
                ).multiplied(by: 2).bytes
                : distanceTableEntryCount,
            at: .indexScan
        )
        defer { distanceTableReservation.release() }
        let distanceTable = try quantizer.distanceTable(
            for: queryVector,
            metric: metric
        )

        var nearest = try VectorSearchAccumulator(k: k, workMeter: workMeter)
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let (begin, end) = codesSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var scannedCodeCount = 0

        try await cursor.consume { key, code in
            try workMeter.consume(at: .indexScan)
            guard codesSubspace.contains(key) else {
                throw VectorIndexError.invalidStructure("Invalid PQ code primary key")
            }
            guard code.count == parameters.m else {
                throw VectorIndexError.invalidStructure("Invalid PQ code length")
            }
            let packedPrimaryKey = key[
                codesSubspace.prefix.count..<key.count
            ]
            let vectorKey = vectorsSubspace.prefix.appending(
                contentsOf: packedPrimaryKey
            )
            try workMeter.consume(at: .indexScan)
            guard let vectorBytes = try await readPointValue(
                using: transaction,
                for: vectorKey,
                snapshot: snapshot,
                workMeter: workMeter,
                at: .indexScan
            ) else {
                throw VectorIndexError.invalidStructure(
                    "PQ code has no persisted canonical vector"
                )
            }
            let vector = try VectorConversion.persistedVector(
                vectorBytes,
                expectedCount: dimensions
            )
            try workMeter.consume(
                pqEncodingWorkUnits(),
                at: .indexScan
            )
            scannedCodeCount += 1
            try nearest.insert(
                packedPrimaryKey: packedPrimaryKey,
                distance: try quantizer.validatedDistance(
                    for: code,
                    vector: vector,
                    using: distanceTable
                )
            )
        }
        guard scannedCodeCount == metadata.vectorCount else {
            throw VectorIndexError.invalidStructure(
                "PQ metadata vector count disagrees with persisted codes"
            )
        }

        return try nearest.finish()
    }

    private func loadMetadata(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> PQMetadata {
        let key = subspace.pack(Tuple([PQIndexStorageKey.metadata.rawValue]))
        try workMeter.consume(at: .indexScan)
        guard let value = try await readPointValue(
            using: transaction,
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
        ) else {
            throw VectorIndexError.invalidStructure("PQ metadata is missing")
        }
        do {
            let metadata = try PQMetadata(packed: value)
            guard metadata.m == parameters.m,
                  metadata.dimensions == dimensions else {
                throw VectorIndexError.invalidStructure(
                    "PQ metadata does not match the configured index"
                )
            }
            return metadata
        } catch let error as VectorIndexError {
            throw error
        } catch {
            throw VectorIndexError.invalidStructure("Invalid PQ metadata")
        }
    }

    private func exactSearch(
        queryVector: Vector,
        k: Int,
        expectedVectorCount: Int,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var nearest = try VectorSearchAccumulator(k: k, workMeter: workMeter)
        var scannedVectorCount = 0
        try await cursor.consume { key, value in
            try workMeter.consume(at: .indexScan)
            guard vectorsSubspace.contains(key) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid PQ vector primary key"
                )
            }
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            scannedVectorCount += 1
            try nearest.insert(
                packedPrimaryKey: key[
                    vectorsSubspace.prefix.count..<key.count
                ],
                distance: try VectorConversion.distance(
                    metric: metric,
                    from: queryVector,
                    to: vector
                )
            )
        }
        guard scannedVectorCount == expectedVectorCount else {
            throw VectorIndexError.invalidStructure(
                "PQ metadata vector count disagrees with persisted vectors"
            )
        }
        return try nearest.finish()
    }

    private func pqEncodingWorkUnits() -> UInt64 {
        let (workUnits, overflow) = UInt64(dimensions)
            .multipliedReportingOverflow(by: UInt64(parameters.ksub))
        return overflow ? UInt64.max : workUnits
    }

    private func hasStoredVector(
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let range = subspace.subspace(PQIndexStorageKey.vectors.rawValue).range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var found = false
        try await cursor.consume { _, _ in
            try workMeter.consume(at: .indexScan)
            found = true
        }
        return found
    }

    func loadCodebookViews(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> DatabaseSharedRetainedArray<PersistedVectorView> {
        let workMeter = workMeter
            ?? VectorRetainedMatches.makeUnboundedWorkMeter()
        let codebooksSubspace = subspace.subspace(
            PQIndexStorageKey.codebooks.rawValue
        )
        let range = codebooksSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        let dimensionsPerSubquantizer = dimensions / parameters.m
        let (expectedCodebookValueCount, overflow) = parameters.ksub
            .multipliedReportingOverflow(by: dimensionsPerSubquantizer)
        guard !overflow else {
            throw VectorIndexError.invalidArgument(
                "PQ codebook shape exceeds the current platform limit"
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<PersistedVectorView>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(
                PersistedVectorView.self
            ),
            expectedCount: parameters.m
        )
        var expectedIndex = 0
        try await cursor.consume { key, value in
            try workMeter.consume(at: .indexScan)
            let keyTuple: Tuple
            do {
                keyTuple = try codebooksSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid PQ codebook key sequence"
                )
            }
            guard keyTuple.count == 1,
                  case .signedInteger(let encodedIndex) =
                    try keyTuple.value(at: 0),
                  Int(exactly: encodedIndex) == expectedIndex else {
                throw VectorIndexError.invalidStructure(
                    "Invalid PQ codebook key sequence"
                )
            }
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .indexScan
            )
            let payloadReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(value.count),
                at: .indexScan
            )
            do {
                let retained = try DatabaseRetainedByteString.make(
                    value,
                    reservation: payloadReservation,
                    at: .indexScan
                )
                let codebook = try VectorConversion.persistedVector(
                    retained,
                    expectedCount: expectedCodebookValueCount
                )
                builder.append(codebook, using: admission)
            } catch {
                payloadReservation.release()
                throw error
            }
            expectedIndex += 1
        }
        guard builder.isEmpty || builder.count == parameters.m else {
            throw VectorIndexError.invalidStructure("Invalid PQ codebook count")
        }
        return try builder.finish().moveToSharedOwnership(at: .indexScan)
    }

    /// Preserves the public maintainer result at its explicit output boundary.
    func search(
        queryVector: Vector,
        k: Int,
        transaction: any TransactionAccess
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let workMeter = VectorRetainedMatches.makeUnboundedWorkMeter()
        let retained = try await search(
            queryVector: queryVector,
            k: k,
            transaction: transaction,
            snapshot: true,
            workMeter: workMeter
        )
        return try retained.promotedOutput()
    }

}
