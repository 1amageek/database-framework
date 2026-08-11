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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        guard queryVector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: queryVector.count
            )
        }
        guard k > 0 else {
            throw VectorIndexError.invalidArgument("k must be positive")
        }

        let metadata = try await loadMetadata(transaction: transaction)
        if metadata.vectorCount == 0 {
            guard try await !hasStoredVector(transaction: transaction) else {
                throw VectorIndexError.invalidStructure(
                    "PQ metadata reports an empty index with persisted vectors"
                )
            }
            return []
        }

        let codebooks = try await loadCodebookViews(transaction: transaction)
        if !metadata.trained {
            guard codebooks.isEmpty else {
                throw VectorIndexError.invalidStructure(
                    "PQ untrained metadata has persisted codebooks"
                )
            }
            return try await exactSearch(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
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
        let distanceTable = try quantizer.distanceTable(
            for: queryVector,
            metric: metric
        )

        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )
        let codesSubspace = subspace.subspace(PQIndexStorageKey.codes.rawValue)
        let (begin, end) = codesSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )

        try await cursor.consume { key, code in
            try workMeter?.consume(at: .indexScan)
            let primaryKey: Tuple
            do {
                primaryKey = try codesSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure("Invalid PQ code primary key")
            }
            guard code.count == parameters.m else {
                throw VectorIndexError.invalidStructure("Invalid PQ code length")
            }
            nearest.insert(
                (
                    primaryKey: try primaryKey.elements(),
                    distance: try quantizer.distance(
                        for: code,
                        using: distanceTable
                    )
                )
            )
        }

        return nearest.sorted()
    }

    private func loadMetadata(
        transaction: any TransactionAccess
    ) async throws -> PQMetadata {
        let key = subspace.pack(Tuple([PQIndexStorageKey.metadata.rawValue]))
        guard let value = try await transaction.getValue(
            for: key,
            snapshot: true
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
        transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        let vectorsSubspace = subspace.subspace(PQIndexStorageKey.vectors.rawValue)
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        var nearest = MinHeap<(primaryKey: [any TupleElement], distance: Double)>(
            maxSize: k,
            heapType: .max,
            comparator: { $0.distance > $1.distance }
        )
        try await cursor.consume { key, value in
            try workMeter?.consume(at: .indexScan)
            let primaryKey: Tuple
            do {
                primaryKey = try vectorsSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid PQ vector primary key"
                )
            }
            let vector = try VectorConversion.persistedVector(
                value,
                expectedCount: dimensions
            )
            nearest.insert(
                (
                    primaryKey: try primaryKey.elements(),
                    distance: try VectorConversion.distance(
                        metric: metric,
                        from: queryVector,
                        to: vector
                    )
                )
            )
        }
        return nearest.sorted()
    }

    private func hasStoredVector(
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let range = subspace.subspace(PQIndexStorageKey.vectors.rawValue).range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        let row = try await cursor.next()
        try await cursor.finish()
        return row != nil
    }

    func loadCodebookViews(
        transaction: any TransactionAccess
    ) async throws -> [PersistedVectorView] {
        let codebooksSubspace = subspace.subspace(
            PQIndexStorageKey.codebooks.rawValue
        )
        let (begin, end) = codebooksSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
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
        var codebooks: [PersistedVectorView] = []
        var expectedIndex = 0
        try await cursor.consume { key, value in
            do {
                let keyTuple = try codebooksSubspace.unpack(key)
                guard keyTuple.count == 1,
                      case .signedInteger(let encodedIndex) = try keyTuple.value(at: 0),
                      Int(exactly: encodedIndex) == expectedIndex else {
                    throw VectorIndexError.invalidStructure(
                        "Invalid PQ codebook key sequence"
                    )
                }
            } catch let error as VectorIndexError {
                throw error
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid PQ codebook key sequence"
                )
            }
            codebooks.append(
                try VectorConversion.persistedVector(
                    value,
                    expectedCount: expectedCodebookValueCount
                )
            )
            expectedIndex += 1
        }

        guard codebooks.isEmpty || codebooks.count == parameters.m else {
            throw VectorIndexError.invalidStructure("Invalid PQ codebook count")
        }
        return codebooks
    }
}
