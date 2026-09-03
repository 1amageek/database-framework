import DatabaseTypes
import DatabaseEngine
import DatabaseKit
import DatabaseMath
import StorageKit
import SwiftHNSW

private let hnswGraphSnapshotVersion: Int64 = 1
private let hnswGraphSnapshotChunkSize = 80 * 1024

private struct HNSWGraphMetadata: Sendable {
    let version: Int64
    let byteCount: Int
    let chunkSize: Int
    let chunkCount: Int
    let revision: Int64
}

private struct HNSWStoredArchive: HNSWArchiveBytes {
    let bytes: ByteString

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        try bytes.withUnsafeBytes(body)
    }
}

private struct HNSWPrimaryKeySnapshot: Sendable {
    let values: [UInt64: ByteString]
    let retainedByteCount: Int
}

private struct HNSWPrimaryKeyMappingSlot: Sendable {
    let label: UInt64
    let packedPrimaryKey: ByteString
}

/// Owns an exact-size cache copy when a storage result has an unknown or
/// enclosing owner. The allocation is initialized completely before the
/// immutable `ByteString` is exposed, borrowed pointers cannot escape the
/// synchronous callback, and this owner performs exactly one deallocation.
/// The caller validates and admits `count` before construction. UInt8 storage
/// has alignment one, is never rebound, and is never mutated after init, so
/// concurrent immutable borrows do not require additional synchronization.
private final class HNSWPrimaryKeyBytesOwner:
    ByteStringOwner,
    @unchecked Sendable
{
    let count: Int
    let retainedByteCount: Int?
    let isStorageSelfContained = true
    private let allocation: UnsafeMutableRawPointer?

    init(copying bytes: ByteString) {
        count = bytes.count
        retainedByteCount = bytes.count
        guard !bytes.isEmpty else {
            allocation = nil
            return
        }
        let allocation = UnsafeMutableRawPointer.allocate(
            byteCount: bytes.count,
            alignment: MemoryLayout<UInt8>.alignment
        )
        bytes.withUnsafeBytes { source in
            UnsafeMutableRawBufferPointer(
                start: allocation,
                count: bytes.count
            ).copyMemory(from: source)
        }
        self.allocation = allocation
    }

    deinit {
        allocation?.deallocate()
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try body(
            UnsafeRawBufferPointer(start: allocation, count: count)
        )
    }
}

private struct HNSWValidatedRestoreProfile: Sendable {
    let cacheCost: Int
    let additionalRestoreByteCount: Int
}

struct HNSWIndexStorage: Sendable {
    let vectorsSubspace: Subspace
    let labelsSubspace: Subspace
    let primaryKeysSubspace: Subspace
    let nextLabelKey: ByteString

    private let subspace: Subspace
    let dimensions: Int
    let metric: VectorMetric
    private let parameters: HNSWParameters
    private let graphCache: HNSWGraphCache
    private let resourceLimits: HNSWGraphResourceLimits
    private let graphChunksSubspace: Subspace
    private let graphMetadataKey: ByteString

    init(
        subspace: Subspace,
        dimensions: Int,
        metric: VectorMetric,
        parameters: HNSWParameters,
        graphCache: HNSWGraphCache,
        resourceLimits: HNSWGraphResourceLimits
    ) {
        self.subspace = subspace
        self.dimensions = dimensions
        self.metric = metric
        self.parameters = parameters
        self.graphCache = graphCache
        self.resourceLimits = resourceLimits
        self.vectorsSubspace = subspace.subspace("v")
        self.labelsSubspace = subspace.subspace("l")
        self.primaryKeysSubspace = subspace.subspace("p")
        self.graphChunksSubspace = subspace.subspace("_graphChunks")
        self.graphMetadataKey = subspace.pack(Tuple("_graphMetadata"))
        self.nextLabelKey = subspace.pack(Tuple("_nextLabel"))
    }

    /// Converts SwiftHNSW's public metric values to the canonical database
    /// distance contract. SwiftHNSW reports inner product as `1 - dot`, while
    /// the other vector backends report `-dot`.
    func canonicalDistance(
        from hnswDistance: Float
    ) throws -> Double {
        guard hnswDistance.isFinite else {
            throw VectorIndexError.invalidStructure(
                "HNSW produced a non-finite comparison distance"
            )
        }
        switch metric {
        case .dotProduct:
            return Double(hnswDistance) - 1.0
        case .cosine, .euclidean:
            return Double(hnswDistance)
        }
    }

    func validateSearchDistance(
        _ hnswDistance: Float,
        label: UInt64,
        queryVector: Vector,
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> Double {
        let vectorKey = vectorsSubspace.pack(HNSWLabelCodec.tuple(label))
        try workMeter.consume(at: .indexScan)
        guard let vectorBytes = try await readPointValue(
            using: transaction,
            for: vectorKey,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
        ) else {
            throw VectorIndexError.invalidStructure(
                "HNSW search result has no persisted canonical vector"
            )
        }
        try workMeter.consume(UInt64(dimensions), at: .indexScan)
        let candidate = try VectorConversion.persistedVector(
            vectorBytes,
            expectedCount: dimensions
        )
        let expectedDistance = try VectorConversion.distance(
            metric: metric,
            from: queryVector,
            to: candidate
        )
        let actualDistance = try canonicalDistance(from: hnswDistance)
        let tolerance = max(1e-5, abs(expectedDistance) * 1e-4)
        guard abs(actualDistance - expectedDistance) <= tolerance else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph distance disagrees with its persisted canonical vector"
            )
        }
        return actualDistance
    }

    func validateSearchDistance(
        _ hnswDistance: Float,
        label: UInt64,
        queryVector: Vector,
        transaction: any TransactionAccess
    ) async throws -> Double {
        try await validateSearchDistance(
            hnswDistance,
            label: label,
            queryVector: queryVector,
            transaction: transaction,
            snapshot: true,
            workMeter: VectorRetainedMatches.makeUnboundedWorkMeter()
        )
    }

    /// Prepares values for SwiftHNSW's Float32 comparison arithmetic. Cosine
    /// inputs whose norm would overflow Float32 are normalized with Double
    /// intermediates. L2 and inner-product inputs cannot be rescaled without
    /// changing their metric, so an unsafe magnitude is rejected explicitly.
    func graphVector(from vector: Vector) throws -> Vector {
        try retainedGraphVector(
            from: vector,
            workMeter: nil
        ).unmeteredOutput()
    }

    func retainedGraphVector(
        from vector: Vector,
        workMeter: DatabaseWorkMeter?
    ) throws -> HNSWRetainedGraphVector {
        try makeGraphVector(from: vector, workMeter: workMeter)
    }

    private func makeGraphVector(
        from vector: Vector,
        workMeter: DatabaseWorkMeter?
    ) throws -> HNSWRetainedGraphVector {
        guard vector.elementType == .float32 else {
            throw VectorIndexError.invalidArgument(
                "HNSW graph vectors require Float32 elements"
            )
        }
        guard vector.count == dimensions else {
            throw VectorIndexError.dimensionMismatch(
                expected: dimensions,
                actual: vector.count
            )
        }

        var maximumMagnitude = 0.0
        guard vector.withFloat32Elements({ elements in
            for element in elements {
                maximumMagnitude = max(
                    maximumMagnitude,
                    abs(Double(element))
                )
            }
            return ()
        }) != nil else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph vector storage is inconsistent"
            )
        }

        let dimensionCount = Double(max(dimensions, 1))
        let comparisonScale: Double
        switch metric {
        case .euclidean:
            comparisonScale = 4.0 * dimensionCount
        case .dotProduct, .cosine:
            comparisonScale = dimensionCount
        }
        let safeMagnitude = 0.5 * DatabaseMath.squareRoot(
            Double(Float.greatestFiniteMagnitude) / comparisonScale
        )
        guard maximumMagnitude > safeMagnitude else {
            return HNSWRetainedGraphVector(
                vector: vector,
                reservation: nil
            )
        }

        guard metric == .cosine else {
            throw VectorIndexError.invalidArgument(
                "HNSW Float32 comparison arithmetic cannot represent this vector magnitude"
            )
        }
        guard maximumMagnitude > 0 else {
            return HNSWRetainedGraphVector(
                vector: vector,
                reservation: nil
            )
        }

        var scaledNormSquared = 0.0
        guard vector.withFloat32Elements({ elements in
            for element in elements {
                let scaled = Double(element) / maximumMagnitude
                scaledNormSquared += scaled * scaled
            }
            return ()
        }) != nil else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph vector storage is inconsistent"
            )
        }
        let inverseNorm = 1.0 / DatabaseMath.squareRoot(
            scaledNormSquared
        )
        guard let workMeter else {
            var normalized: [Float] = []
            normalized.reserveCapacity(dimensions)
            guard vector.withFloat32Elements({ elements in
                for element in elements {
                    normalized.append(
                        Float(
                            (Double(element) / maximumMagnitude)
                                * inverseNorm
                        )
                    )
                }
                return ()
            }) != nil else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph vector storage is inconsistent"
                )
            }
            do {
                return HNSWRetainedGraphVector(
                    vector: try Vector(float32: normalized),
                    reservation: nil
                )
            } catch {
                throw VectorIndexError.invalidStructure(
                    "HNSW cosine normalization produced an invalid vector"
                )
            }
        }
        var normalized = try DatabaseRetainedArrayBuilder<Float>(
            workMeter: workMeter,
            stage: .indexScan,
            layout: try DatabaseRetainedArrayLayout.forElement(Float.self),
            expectedCount: dimensions
        )
        guard try vector.withFloat32Elements({ elements in
            for element in elements {
                let value = Float(
                    (Double(element) / maximumMagnitude) * inverseNorm
                )
                try normalized.append(
                    footprint: DatabaseIntermediateFootprint(),
                    at: .indexScan,
                    make: { value }
                )
            }
            return ()
        }) != nil else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph vector storage is inconsistent"
            )
        }
        let retained = normalized.finish().moveRetainingReservation()
        do {
            return HNSWRetainedGraphVector(
                vector: try Vector(float32: retained.elements),
                reservation: retained.reservation
            )
        } catch {
            retained.reservation.release()
            throw VectorIndexError.invalidStructure(
                "HNSW cosine normalization produced an invalid vector"
            )
        }
    }

    func loadOrCreateIndex(
        transaction: any TransactionReadAccess,
        additionalCapacity: Int = 0,
        snapshot: Bool = true
    ) async throws -> HNSWIndexF32 {
        if let graphData = try await loadGraphSnapshotData(
            transaction: transaction,
            snapshot: snapshot
        ) {
            _ = try inspectAndValidateRestore(from: graphData)
            let index = try loadPersistedIndex(from: graphData)
            try ensureCapacity(index, additionalCount: additionalCapacity)
            return index
        }

        let maxElements = max(
            try await estimateMaxElements(
                transaction: transaction,
                snapshot: snapshot
            ),
            additionalCapacity
        )
        do {
            try validateNewIndexBudget(elementCapacity: maxElements)
            return try HNSWIndexF32(
                dimensions: dimensions,
                maxElements: maxElements,
                metric: metric.toHNSWMetric,
                configuration: parameters.hnswConfiguration
            )
        } catch HNSWError.invalidArgument(let message) {
            throw VectorIndexError.invalidArgument(message)
        }
    }

    func ensureCapacity(
        _ index: HNSWIndexF32,
        additionalCount: Int
    ) throws {
        guard additionalCount > 0 else {
            return
        }

        let requiredCapacity = try checkedSum(
            index.count,
            additionalCount,
            message: "HNSW graph capacity exceeds the current platform limit"
        )
        guard requiredCapacity > index.capacity else {
            return
        }

        var nextCapacity = max(index.capacity, 1)
        while nextCapacity < requiredCapacity {
            let (doubledCapacity, overflow) = nextCapacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph capacity exceeds the current platform limit"
                )
            }
            nextCapacity = doubledCapacity
        }
        try index.resize(to: nextCapacity)
    }

    func saveIndex(
        _ index: HNSWIndexF32,
        transaction: any TransactionAccess
    ) async throws {
        let archive = try index.serializedArchive()
        let graphBytes = ByteString(archive.bytes)
        _ = try inspectAndValidateRestore(from: graphBytes)
        try await saveGraphSnapshot(
            graphBytes,
            transaction: transaction
        )
    }

    func loadSearchSnapshot(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> HNSWRetainedSearchSnapshot {
        try workMeter.consume(at: .indexScan)
        if let metadataBytes = try await readPointValue(
            using: transaction,
            for: graphMetadataKey,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
        ) {
            let metadata = try decodeGraphMetadata(metadataBytes)
            let cacheKey = HNSWGraphCache.Key(
                transactionDomain: transaction.transactionDomain,
                subspacePrefix: subspace.prefix,
                dimensions: dimensions,
                metric: metric.toHNSWMetric.rawValue,
                metadata: HNSWGraphCache.MetadataIdentity(
                    version: metadata.version,
                    byteCount: metadata.byteCount,
                    chunkSize: metadata.chunkSize,
                    chunkCount: metadata.chunkCount,
                    revision: metadata.revision
                )
            )
            if let cached = graphCache.get(cacheKey) {
                return HNSWRetainedSearchSnapshot(cached: cached)
            }

            let primaryKeyReservation = try workMeter.reserveIntermediate(
                at: .indexScan
            )
            let primaryKeys = try await loadPrimaryKeysByLabel(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter,
                reservation: primaryKeyReservation
            )

            let graphData = try await loadChunkedGraphSnapshot(
                metadata: metadata,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
            let restoreProfile = try inspectAndValidateRestore(
                from: graphData,
                primaryKeys: primaryKeys
            )
            let nativeRestoreReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(restoreProfile.additionalRestoreByteCount),
                at: .indexScan
            )
            let index = try loadPersistedIndex(from: graphData)
            for label in primaryKeys.values.keys {
                try workMeter.consume(at: .indexScan)
                guard index.contains(label: label) else {
                    throw VectorIndexError.invalidStructure(
                        "HNSW active graph labels and primary-key mappings disagree"
                    )
                }
            }
            try await validateForwardMappings(
                primaryKeys: primaryKeys.values,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
            let snapshot = HNSWGraphCache.Snapshot(
                index: index,
                primaryKeysByLabel: primaryKeys.values
            )
            if graphCache.set(
                snapshot,
                for: cacheKey,
                cost: restoreProfile.cacheCost
            ) {
                primaryKeyReservation.release()
                nativeRestoreReservation.release()
                return HNSWRetainedSearchSnapshot(cached: snapshot)
            }
            return HNSWRetainedSearchSnapshot(
                requestOwned: snapshot,
                graphData: graphData,
                primaryKeyReservation: primaryKeyReservation,
                restoreReservation: nativeRestoreReservation
            )
        }

        let primaryKeyReservation = try workMeter.reserveIntermediate(
            at: .indexScan
        )
        defer { primaryKeyReservation.release() }
        let primaryKeys = try await loadPrimaryKeysByLabel(
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter,
            reservation: primaryKeyReservation
        )

        guard primaryKeys.values.isEmpty,
              try await !hasPersistedVector(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
              ),
              try await !hasPersistedForwardMapping(
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
              ) else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot is missing for persisted index entries"
            )
        }

        return .empty
    }

    func loadSearchSnapshot(
        transaction: any TransactionAccess
    ) async throws -> HNSWRetainedSearchSnapshot {
        try await loadSearchSnapshot(
            transaction: transaction,
            snapshot: true,
            workMeter: VectorRetainedMatches.makeUnboundedWorkMeter()
        )
    }

    /// Verifies persisted HNSW facts without consulting the process cache.
    /// Scrubbing must observe current transaction state even when metadata was
    /// corrupted without advancing the graph revision.
    func validateStoredEntry(
        label: UInt64,
        primaryKey: Tuple,
        transaction: any TransactionReadAccess
    ) async throws {
        let mappingKey = primaryKeysSubspace.pack(HNSWLabelCodec.tuple(label))
        guard let mappingValue = try await transaction.getValue(
            for: mappingKey,
            snapshot: true
        ) else {
            throw VectorIndexError.invalidStructure(
                "HNSW reverse primary-key mapping is missing"
            )
        }
        guard mappingValue == primaryKey.pack() else {
            throw VectorIndexError.invalidStructure(
                "HNSW forward and reverse primary-key mappings disagree"
            )
        }
        guard let graphData = try await loadGraphSnapshotData(
            transaction: transaction
        ) else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot is missing"
            )
        }
        _ = try inspectAndValidateRestore(from: graphData)
        let index = try loadPersistedIndex(from: graphData)
        guard index.contains(label: label) else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph does not contain the mapped label"
            )
        }
    }

    private func loadPersistedIndex(
        from graphBytes: ByteString
    ) throws -> HNSWIndexF32 {
        do {
            return try HNSWIndexF32.restore(
                from: HNSWStoredArchive(bytes: graphBytes),
                dimensions: dimensions,
                metric: metric.toHNSWMetric,
                maxElements: 0
            )
        } catch {
            throw VectorIndexError.invalidStructure("Invalid HNSW graph snapshot")
        }
    }

    private func loadGraphSnapshotData(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true
    ) async throws -> ByteString? {
        guard let metadataBytes = try await transaction.getValue(
            for: graphMetadataKey,
            snapshot: snapshot
        ) else {
            return nil
        }
        return try await loadChunkedGraphSnapshot(
            metadata: try decodeGraphMetadata(metadataBytes),
            transaction: transaction,
            snapshot: snapshot
        )
    }

    private func saveGraphSnapshot(
        _ graphBytes: ByteString,
        transaction: any TransactionAccess
    ) async throws {
        let byteCount = graphBytes.count
        guard resourceLimits.maximumSnapshotByteCount > 0 else {
            throw VectorIndexError.invalidArgument(
                "The maximum HNSW graph snapshot byte count must be positive"
            )
        }
        guard byteCount <= resourceLimits.maximumSnapshotByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot exceeds the configured byte limit"
            )
        }
        let chunkCount = byteCount / hnswGraphSnapshotChunkSize
            + (byteCount % hnswGraphSnapshotChunkSize == 0 ? 0 : 1)
        guard resourceLimits.maximumTransactionMutationByteCount > 0 else {
            throw VectorIndexError.invalidArgument(
                "The maximum HNSW transaction mutation byte count must be positive"
            )
        }
        let revision = try await nextGraphSnapshotRevision(
            transaction: transaction
        )
        let metadata = Tuple(
            hnswGraphSnapshotVersion,
            Int64(byteCount),
            Int64(hnswGraphSnapshotChunkSize),
            Int64(chunkCount),
            revision
        ).pack()
        let range = graphChunksSubspace.range()
        var mutationByteCount = try checkedSum(
            range.begin.count,
            range.end.count,
            message: "HNSW transaction mutation byte count overflow"
        )
        for chunkIndex in 0..<chunkCount {
            guard let encodedChunkIndex = Int64(exactly: chunkIndex) else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph chunk index exceeds the storage format"
                )
            }
            let start = chunkIndex * hnswGraphSnapshotChunkSize
            let end = min(start + hnswGraphSnapshotChunkSize, byteCount)
            let key = graphChunksSubspace.pack(Tuple(encodedChunkIndex))
            let rowByteCount = try checkedSum(
                key.count,
                end - start,
                message: "HNSW transaction mutation byte count overflow"
            )
            mutationByteCount = try checkedSum(
                mutationByteCount,
                rowByteCount,
                message: "HNSW transaction mutation byte count overflow"
            )
        }
        mutationByteCount = try checkedSum(
            mutationByteCount,
            try checkedSum(
                graphMetadataKey.count,
                metadata.count,
                message: "HNSW transaction mutation byte count overflow"
            ),
            message: "HNSW transaction mutation byte count overflow"
        )
        guard mutationByteCount
                <= resourceLimits.maximumTransactionMutationByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW snapshot transaction exceeds the configured mutation byte limit"
            )
        }
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)

        for chunkIndex in 0..<chunkCount {
            let encodedChunkIndex = Int64(chunkIndex)
            let start = chunkIndex * hnswGraphSnapshotChunkSize
            let end = min(start + hnswGraphSnapshotChunkSize, byteCount)
            try transaction.setValue(
                graphBytes[start..<end],
                for: graphChunksSubspace.pack(Tuple(encodedChunkIndex))
            )
        }
        try transaction.setValue(metadata, for: graphMetadataKey)
    }

    private func loadChunkedGraphSnapshot(
        metadata decoded: HNSWGraphMetadata,
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws -> ByteString {
        guard resourceLimits.maximumSnapshotByteCount > 0 else {
            throw VectorIndexError.invalidArgument(
                "The maximum HNSW graph snapshot byte count must be positive"
            )
        }
        guard decoded.byteCount <= resourceLimits.maximumSnapshotByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot exceeds the configured byte limit"
            )
        }
        let wholeChunks = decoded.byteCount / decoded.chunkSize
        let partialChunkCount = decoded.byteCount % decoded.chunkSize == 0 ? 0 : 1
        let (expectedChunkCount, chunkCountOverflow) = wholeChunks.addingReportingOverflow(
            partialChunkCount
        )
        guard !chunkCountOverflow else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot chunk count exceeds the current platform limit"
            )
        }
        guard decoded.chunkCount == expectedChunkCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot chunk count does not match byte count"
            )
        }

        let retainedBuffer: DatabaseRetainedMutableByteBuffer?
        var output: [UInt8]
        if let workMeter {
            let reservation = try workMeter.reserveIntermediate(
                bytes: UInt64(decoded.byteCount),
                at: .indexScan
            )
            retainedBuffer = DatabaseRetainedMutableByteBuffer(
                count: decoded.byteCount,
                reservation: reservation
            )
            output = []
        } else {
            retainedBuffer = nil
            output = [UInt8](repeating: 0, count: decoded.byteCount)
        }
        var loadedByteCount = 0
        for chunkIndex in 0..<decoded.chunkCount {
            let chunkKey = graphChunksSubspace.pack(Tuple(Int64(chunkIndex)))
            let expectedChunkSize = min(
                decoded.chunkSize,
                decoded.byteCount - loadedByteCount
            )
            let chunk: ByteString?
            if let workMeter {
                try workMeter.consume(at: .indexScan)
                chunk = try await readPointValue(
                    using: transaction,
                    for: chunkKey,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    at: .indexScan
                )
            } else {
                chunk = try await transaction.getValue(
                    for: chunkKey,
                    snapshot: snapshot,
                    maximumByteCount: expectedChunkSize
                )
            }
            guard let chunk else {
                throw VectorIndexError.invalidStructure(
                    "Missing HNSW graph snapshot chunk \(chunkIndex)"
                )
            }
            guard chunk.count == expectedChunkSize else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph snapshot chunk \(chunkIndex) has an invalid size"
                )
            }
            if let retainedBuffer, let workMeter {
                try DatabaseByteProcessingMeter.consume(
                    byteCount: chunk.count,
                    workMeter: workMeter,
                    stage: .indexScan
                )
                retainedBuffer.append(copying: chunk)
            } else {
                output.withUnsafeMutableBytes { destination in
                    chunk.withUnsafeBytes { source in
                        let target = UnsafeMutableRawBufferPointer(
                            rebasing: destination[
                                loadedByteCount..<(loadedByteCount + source.count)
                            ]
                        )
                        target.copyMemory(from: source)
                    }
                }
            }
            loadedByteCount += expectedChunkSize
        }
        guard loadedByteCount == decoded.byteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot byte count mismatch"
            )
        }
        if let retainedBuffer {
            return retainedBuffer.finalize()
        }
        return ByteString(output)
    }

    private func decodeGraphMetadata(
        _ metadata: ByteString
    ) throws -> HNSWGraphMetadata {
        do {
            return try decodeGraphMetadataPayload(metadata)
        } catch let error as VectorIndexError {
            throw error
        } catch {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph snapshot metadata"
            )
        }
    }

    private func decodeGraphMetadataPayload(
        _ metadata: ByteString
    ) throws -> HNSWGraphMetadata {
        var cursor = TupleCursor(bytes: metadata)

        func requireCanonicalInt64() throws -> Int64 {
            let start = cursor.consumedByteCount
            let value = try cursor.requireInt64()
            guard cursor.consumedByteCount - start
                    == canonicalTupleInt64ByteCount(value) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW graph snapshot metadata"
                )
            }
            return value
        }

        let version = try requireCanonicalInt64()
        let byteCountValue = try requireCanonicalInt64()
        let chunkSizeValue = try requireCanonicalInt64()
        let chunkCountValue = try requireCanonicalInt64()
        guard version == hnswGraphSnapshotVersion else {
            throw VectorIndexError.invalidStructure(
                "Unsupported HNSW graph snapshot version \(version)"
            )
        }

        let revision: Int64
        if cursor.isAtEnd {
            revision = 0
        } else {
            revision = try requireCanonicalInt64()
            guard cursor.isAtEnd else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW graph snapshot metadata"
                )
            }
        }
        guard byteCountValue >= 0,
              chunkSizeValue > 0,
              chunkCountValue >= 0,
              revision >= 0,
              byteCountValue <= Int64(Int.max),
              chunkSizeValue <= Int64(Int.max),
              chunkCountValue <= Int64(Int.max)
        else {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph snapshot chunk dimensions"
            )
        }
        guard chunkSizeValue == Int64(hnswGraphSnapshotChunkSize) else {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph snapshot chunk size"
            )
        }
        return HNSWGraphMetadata(
            version: version,
            byteCount: Int(byteCountValue),
            chunkSize: Int(chunkSizeValue),
            chunkCount: Int(chunkCountValue),
            revision: revision
        )
    }

    private func canonicalTupleInt64ByteCount(_ value: Int64) -> Int {
        guard value != 0 else { return 1 }
        var magnitude = value > 0
            ? UInt64(value)
            : 0 &- UInt64(bitPattern: value)
        var payloadByteCount = 1
        while magnitude > UInt64(UInt8.max) {
            payloadByteCount += 1
            magnitude >>= 8
        }
        return 1 + payloadByteCount
    }

    private func nextGraphSnapshotRevision(
        transaction: any TransactionAccess
    ) async throws -> Int64 {
        guard let currentMetadata = try await transaction.getValue(
            for: graphMetadataKey,
            snapshot: false
        ) else {
            return 1
        }
        let currentRevision = try decodeGraphMetadata(currentMetadata).revision
        let (nextRevision, overflow) = currentRevision.addingReportingOverflow(1)
        guard !overflow else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot revision exhausted"
            )
        }
        return nextRevision
    }

    private func loadPrimaryKeysByLabel(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter? = nil,
        reservation: DatabaseIntermediateReservation? = nil
    ) async throws -> HNSWPrimaryKeySnapshot {
        guard resourceLimits.maximumPrimaryKeyCount >= 0,
              resourceLimits.maximumPrimaryKeyByteCount >= 0 else {
            throw VectorIndexError.invalidArgument(
                "HNSW primary-key resource limits must not be negative"
            )
        }
        let (begin, end) = primaryKeysSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )

        let mappingLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(
                MemoryLayout<[UInt64: ByteString]>.stride
            ),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<HNSWPrimaryKeyMappingSlot>.stride)
            )
        )
        guard let mappingContainerBytes = Int(exactly: mappingLayout.containerByteCount)
        else {
            throw VectorIndexError.invalidStructure(
                "HNSW primary-key mapping exceeds the current platform limit"
            )
        }
        try reservation?.reserveAdditional(
            bytes: mappingLayout.containerByteCount,
            at: .indexScan
        )
        var primaryKeysByLabel: [UInt64: ByteString] = [:]
        var accountedCapacity = 0
        var retainedByteCount = mappingContainerBytes
        var persistedByteCount = 0
        try await cursor.consume { key, value in
            guard primaryKeysByLabel.count < resourceLimits.maximumPrimaryKeyCount else {
                throw VectorIndexError.invalidStructure(
                    "HNSW primary-key mapping exceeds the configured count limit"
                )
            }
            let rowByteCount = try checkedSum(
                key.count,
                value.count,
                message: "HNSW primary-key mapping byte count overflow"
            )
            let retainedRowByteCount = try checkedSum(
                rowByteCount,
                64,
                message: "HNSW primary-key retained byte count overflow"
            )
            persistedByteCount = try checkedSum(
                persistedByteCount,
                retainedRowByteCount,
                message: "HNSW primary-key retained byte count overflow"
            )
            guard persistedByteCount <= resourceLimits.maximumPrimaryKeyByteCount else {
                throw VectorIndexError.invalidStructure(
                    "HNSW primary-key mapping exceeds the configured byte limit"
                )
            }
            try validateCanonicalPrimaryKey(
                value,
                workMeter: workMeter
            )
            guard primaryKeysSubspace.contains(key) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW primary-key mapping key"
                )
            }
            let labelBytes = key[
                primaryKeysSubspace.prefix.count..<key.count
            ]
            let labelValue = try HNSWLabelCodec.decodePacked(labelBytes)
            let (requiredCount, countOverflow) = primaryKeysByLabel.count
                .addingReportingOverflow(1)
            guard !countOverflow else {
                throw VectorIndexError.invalidStructure(
                    "HNSW primary-key mapping count overflow"
                )
            }
            let growth = try mappingLayout.growth(
                from: accountedCapacity,
                toFit: requiredCount
            )
            guard let growthByteCount = Int(exactly: growth.additionalByteCount)
            else {
                throw VectorIndexError.invalidStructure(
                    "HNSW primary-key mapping exceeds the current platform limit"
                )
            }
            retainedByteCount = try checkedSum(
                retainedByteCount,
                try checkedSum(
                    growthByteCount,
                    value.count,
                    message: "HNSW primary-key retained byte count overflow"
                ),
                message: "HNSW primary-key retained byte count overflow"
            )
            try reservation?.reserveAdditional(
                rows: 1,
                bytes: try DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                ).adding(
                    DatabaseIntermediateFootprint(bytes: UInt64(value.count))
                ).bytes,
                at: .indexScan
            )
            try workMeter?.consume(at: .indexScan)
            if growth.capacity != accountedCapacity {
                primaryKeysByLabel.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            let retainedValue: ByteString
            if value.isStorageSelfContained,
               value.retainedByteCount == value.count {
                retainedValue = value
            } else {
                if let workMeter {
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: value.count,
                        workMeter: workMeter,
                        stage: .indexScan
                    )
                }
                // The cache must not retain an opaque backend batch or a large
                // enclosing row owner. This is the sole ownership-transfer
                // copy; exact self-contained owners take the branch above.
                retainedValue = ByteString(
                    retaining: HNSWPrimaryKeyBytesOwner(copying: value)
                )
            }
            guard primaryKeysByLabel.updateValue(
                retainedValue,
                forKey: labelValue
            ) == nil else {
                throw VectorIndexError.invalidStructure(
                    "Duplicate HNSW primary-key label"
                )
            }
        }
        return HNSWPrimaryKeySnapshot(
            values: primaryKeysByLabel,
            retainedByteCount: retainedByteCount
        )
    }

    private func inspectAndValidateRestore(
        from graphData: ByteString,
        primaryKeys: HNSWPrimaryKeySnapshot? = nil
    ) throws -> HNSWValidatedRestoreProfile {
        guard resourceLimits.maximumRetainedByteCount > 0 else {
            throw VectorIndexError.invalidArgument(
                "The maximum HNSW retained byte count must be positive"
            )
        }
        let profile: HNSWArchiveResourceProfile
        do {
            profile = try HNSWIndexF32.inspectArchiveResourceProfile(
                from: HNSWStoredArchive(bytes: graphData),
                dimensions: dimensions,
                metric: metric.toHNSWMetric
            )
        } catch {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph archive"
            )
        }
        guard profile.labelCount <= resourceLimits.maximumPrimaryKeyCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph label count exceeds the configured count limit"
            )
        }
        if let primaryKeys {
            guard profile.activeLabelCount == primaryKeys.values.count else {
                throw VectorIndexError.invalidStructure(
                    "HNSW active graph label count and primary-key mappings disagree"
                )
            }
        }
        let cachedBytes = try checkedSum(
            profile.estimatedRetainedPayloadByteCount,
            primaryKeys?.retainedByteCount ?? 0,
            message: "HNSW retained payload exceeds the current platform limit"
        )
        let restorePeakBytes = try checkedSum(
            profile.estimatedRestoreWorkingPayloadByteCount,
            primaryKeys?.retainedByteCount ?? 0,
            message: "HNSW restore payload exceeds the current platform limit"
        )
        guard cachedBytes <= resourceLimits.maximumRetainedByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW retained payload exceeds the configured byte limit"
            )
        }
        guard restorePeakBytes <= resourceLimits.maximumRetainedByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW restore payload exceeds the configured byte limit"
            )
        }
        guard profile.estimatedRestoreWorkingPayloadByteCount
                >= profile.archiveByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW restore profile excludes its archive payload"
            )
        }
        return HNSWValidatedRestoreProfile(
            cacheCost: cachedBytes,
            additionalRestoreByteCount:
                profile.estimatedRestoreWorkingPayloadByteCount
                    - profile.archiveByteCount
        )
    }

    private func validateNewIndexBudget(
        elementCapacity: Int
    ) throws {
        guard resourceLimits.maximumRetainedByteCount > 0 else {
            throw VectorIndexError.invalidArgument(
                "The maximum HNSW retained byte count must be positive"
            )
        }
        let vectorScalars = try checkedProduct(
            max(1, elementCapacity),
            dimensions,
            message: "HNSW vector capacity exceeds the current platform limit"
        )
        let vectorBytes = try checkedProduct(
            vectorScalars,
            MemoryLayout<Float>.stride,
            message: "HNSW vector capacity exceeds the current platform limit"
        )
        let eagerVectorBytes = vectorBytes <= 64 * 1_024 * 1_024
            ? vectorBytes
            : 0
        let queryBytes = try checkedProduct(
            dimensions,
            MemoryLayout<Float>.stride,
            message: "HNSW query workspace exceeds the current platform limit"
        )
        let retainedBytes = try checkedSum(
            eagerVectorBytes,
            queryBytes,
            message: "HNSW initial memory estimate exceeds the current platform limit"
        )
        guard retainedBytes <= resourceLimits.maximumRetainedByteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW initial memory estimate exceeds the configured byte limit"
            )
        }
    }

    private func validateForwardMappings(
        primaryKeys: [UInt64: ByteString],
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter? = nil
    ) async throws {
        let (begin, end) = labelsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var observedCount = 0
        var observedByteCount = 0
        try await cursor.consume { key, value in
            guard observedCount < resourceLimits.maximumPrimaryKeyCount else {
                throw VectorIndexError.invalidStructure(
                    "HNSW forward label mapping exceeds the configured count limit"
                )
            }
            let rowByteCount = try checkedSum(
                key.count,
                value.count,
                message: "HNSW forward label mapping byte count overflow"
            )
            observedByteCount = try checkedSum(
                observedByteCount,
                rowByteCount,
                message: "HNSW forward label mapping byte count overflow"
            )
            guard observedByteCount
                    <= resourceLimits.maximumPrimaryKeyByteCount else {
                throw VectorIndexError.invalidStructure(
                    "HNSW forward label mapping exceeds the configured byte limit"
                )
            }
            guard labelsSubspace.contains(key) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW forward primary-key mapping"
                )
            }
            let primaryKey = key[labelsSubspace.prefix.count..<key.count]
            let label = try HNSWLabelCodec.decodePacked(value)
            try workMeter?.consume(at: .indexScan)
            if let workMeter {
                try DatabaseByteProcessingMeter.consume(
                    byteCount: primaryKey.count,
                    workMeter: workMeter,
                    stage: .indexScan
                )
            }
            guard primaryKeys[label] == primaryKey else {
                throw VectorIndexError.invalidStructure(
                    "HNSW forward and reverse primary-key mappings disagree"
                )
            }
            observedCount += 1
        }
        guard observedCount == primaryKeys.count else {
            throw VectorIndexError.invalidStructure(
                "HNSW forward primary-key mapping count is inconsistent"
            )
        }
    }

    private func validateCanonicalPrimaryKey(
        _ packedPrimaryKey: ByteString,
        workMeter: DatabaseWorkMeter?
    ) throws {
        let scratchBytes = try DatabaseIntermediateFootprint(
            bytes: 128
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(packedPrimaryKey.count)
            ).multiplied(by: 96)
        ).bytes
        let scratch = try workMeter?.reserveIntermediate(
            bytes: scratchBytes,
            at: .indexScan
        )
        defer { scratch?.release() }
        if let workMeter {
            try DatabaseByteProcessingMeter.consume(
                byteCount: packedPrimaryKey.count,
                passes: 2,
                workMeter: workMeter,
                stage: .indexScan
            )
        }
        do {
            guard try Tuple(packed: packedPrimaryKey).pack()
                    == packedPrimaryKey else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW reverse primary-key mapping"
                )
            }
        } catch let error as VectorIndexError {
            throw error
        } catch {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW reverse primary-key mapping"
            )
        }
    }

    private func hasPersistedVector(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
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

    private func hasPersistedForwardMapping(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let (begin, end) = labelsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
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

    private func checkedSum(
        _ lhs: Int,
        _ rhs: Int,
        message: String
    ) throws -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw VectorIndexError.invalidStructure(message)
        }
        return result
    }

    private func checkedProduct(
        _ lhs: Int,
        _ rhs: Int,
        message: String
    ) throws -> Int {
        let (result, overflow) = lhs.multipliedReportingOverflow(by: rhs)
        guard !overflow else {
            throw VectorIndexError.invalidStructure(message)
        }
        return result
    }

    private func estimateMaxElements(
        transaction: any TransactionReadAccess,
        snapshot: Bool = true
    ) async throws -> Int {
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 100_001,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .iterator
        )
        var entryCount = 0
        try await cursor.consume { _, _ in
            entryCount += 1
        }
        return max(1, min(entryCount, 100_000) * 2)
    }
}
