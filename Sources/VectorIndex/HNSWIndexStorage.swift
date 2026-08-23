import DatabaseTypes
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
    let values: [UInt64: Tuple]
    let retainedByteCount: Int
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

    /// Prepares values for SwiftHNSW's Float32 comparison arithmetic. Cosine
    /// inputs whose norm would overflow Float32 are normalized with Double
    /// intermediates. L2 and inner-product inputs cannot be rescaled without
    /// changing their metric, so an unsafe magnitude is rejected explicitly.
    func graphVector(from vector: Vector) throws -> Vector {
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
            return vector
        }

        guard metric == .cosine else {
            throw VectorIndexError.invalidArgument(
                "HNSW Float32 comparison arithmetic cannot represent this vector magnitude"
            )
        }
        guard maximumMagnitude > 0 else {
            return vector
        }

        var scaledNormSquared = 0.0
        var normalized: [Float] = []
        normalized.reserveCapacity(dimensions)
        guard vector.withFloat32Elements({ elements in
            for element in elements {
                let scaled = Double(element) / maximumMagnitude
                scaledNormSquared += scaled * scaled
            }
            let inverseNorm = 1.0 / DatabaseMath.squareRoot(
                scaledNormSquared
            )
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
            return try Vector(float32: normalized)
        } catch {
            throw VectorIndexError.invalidStructure(
                "HNSW cosine normalization produced an invalid vector"
            )
        }
    }

    func loadOrCreateIndex(
        transaction: any TransactionAccess,
        additionalCapacity: Int = 0
    ) async throws -> HNSWIndexF32 {
        if let graphData = try await loadGraphSnapshotData(transaction: transaction) {
            _ = try inspectAndValidateRestore(from: graphData)
            let index = try loadPersistedIndex(from: graphData)
            try ensureCapacity(index, additionalCount: additionalCapacity)
            return index
        }

        let maxElements = max(
            try await estimateMaxElements(transaction: transaction),
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
        transaction: any TransactionAccess
    ) async throws -> HNSWGraphCache.Snapshot {
        if let metadataBytes = try await transaction.getValue(
            for: graphMetadataKey,
            snapshot: true
        ) {
            _ = try decodeGraphMetadata(metadataBytes)
            let cacheKey = HNSWGraphCache.Key(
                subspacePrefix: subspace.prefix,
                dimensions: dimensions,
                metric: metric.toHNSWMetric.rawValue,
                metadata: metadataBytes
            )
            if let cached = graphCache.get(cacheKey) {
                return cached
            }

            let primaryKeys = try await loadPrimaryKeysByLabel(
                transaction: transaction
            )

            let graphData = try await loadChunkedGraphSnapshot(
                metadata: metadataBytes,
                transaction: transaction
            )
            let cacheCost = try inspectAndValidateRestore(
                from: graphData,
                primaryKeys: primaryKeys
            )
            let index = try loadPersistedIndex(from: graphData)
            let graphLabels = index.allLabels.sorted()
            let mappedLabels = primaryKeys.values.keys.sorted()
            guard graphLabels == mappedLabels else {
                throw VectorIndexError.invalidStructure(
                    "HNSW active graph labels and primary-key mappings disagree"
                )
            }
            try await validateForwardMappings(
                primaryKeys: primaryKeys.values,
                transaction: transaction
            )
            let snapshot = HNSWGraphCache.Snapshot(
                index: index,
                primaryKeysByLabel: primaryKeys.values
            )
            graphCache.set(
                snapshot,
                for: cacheKey,
                cost: cacheCost
            )
            return snapshot
        }

        let primaryKeys = try await loadPrimaryKeysByLabel(
            transaction: transaction
        )

        guard primaryKeys.values.isEmpty,
              try await !hasPersistedVector(transaction: transaction),
              try await !hasPersistedForwardMapping(transaction: transaction) else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot is missing for persisted index entries"
            )
        }

        return HNSWGraphCache.Snapshot(
            index: try await loadOrCreateIndex(transaction: transaction),
            primaryKeysByLabel: [:]
        )
    }

    /// Verifies persisted HNSW facts without consulting the process cache.
    /// Scrubbing must observe current transaction state even when metadata was
    /// corrupted without advancing the graph revision.
    func validateStoredEntry(
        label: UInt64,
        primaryKey: Tuple,
        transaction: any TransactionAccess
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
        let mappedPrimaryKey: Tuple
        do {
            mappedPrimaryKey = try Tuple(packed: mappingValue)
        } catch {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW reverse primary-key mapping"
            )
        }
        guard mappedPrimaryKey.pack() == primaryKey.pack() else {
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
        transaction: any TransactionAccess
    ) async throws -> ByteString? {
        guard let metadataBytes = try await transaction.getValue(
            for: graphMetadataKey,
            snapshot: true
        ) else {
            return nil
        }
        return try await loadChunkedGraphSnapshot(
            metadata: metadataBytes,
            transaction: transaction
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
        metadata: ByteString,
        transaction: any TransactionAccess
    ) async throws -> ByteString {
        let decoded = try decodeGraphMetadata(metadata)
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

        var output = [UInt8](repeating: 0, count: decoded.byteCount)
        var loadedByteCount = 0
        for chunkIndex in 0..<decoded.chunkCount {
            let chunkKey = graphChunksSubspace.pack(Tuple(Int64(chunkIndex)))
            guard let chunk = try await transaction.getValue(
                for: chunkKey,
                snapshot: true
            ) else {
                throw VectorIndexError.invalidStructure(
                    "Missing HNSW graph snapshot chunk \(chunkIndex)"
                )
            }
            let expectedChunkSize = min(
                decoded.chunkSize,
                decoded.byteCount - loadedByteCount
            )
            guard chunk.count == expectedChunkSize else {
                throw VectorIndexError.invalidStructure(
                    "HNSW graph snapshot chunk \(chunkIndex) has an invalid size"
                )
            }
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
            loadedByteCount += expectedChunkSize
        }
        guard loadedByteCount == decoded.byteCount else {
            throw VectorIndexError.invalidStructure(
                "HNSW graph snapshot byte count mismatch"
            )
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
        let tuple = try Tuple(packed: metadata)
        guard tuple.count == 4 || tuple.count == 5,
              case .signedInteger(let version) = try tuple.value(at: 0),
              case .signedInteger(let byteCountValue) = try tuple.value(at: 1),
              case .signedInteger(let chunkSizeValue) = try tuple.value(at: 2),
              case .signedInteger(let chunkCountValue) = try tuple.value(at: 3)
        else {
            throw VectorIndexError.invalidStructure(
                "Invalid HNSW graph snapshot metadata"
            )
        }
        guard version == hnswGraphSnapshotVersion else {
            throw VectorIndexError.invalidStructure(
                "Unsupported HNSW graph snapshot version \(version)"
            )
        }

        let revision: Int64
        if tuple.count == 5 {
            guard case .signedInteger(let value) = try tuple.value(at: 4) else {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW graph snapshot revision"
                )
            }
            revision = value
        } else {
            revision = 0
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
        transaction: any TransactionAccess
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
            snapshot: true,
            streamingMode: .iterator
        )

        var primaryKeysByLabel: [UInt64: Tuple] = [:]
        var retainedByteCount = 0
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
            retainedByteCount = try checkedSum(
                retainedByteCount,
                retainedRowByteCount,
                message: "HNSW primary-key retained byte count overflow"
            )
            guard retainedByteCount <= resourceLimits.maximumPrimaryKeyByteCount else {
                throw VectorIndexError.invalidStructure(
                    "HNSW primary-key mapping exceeds the configured byte limit"
                )
            }
            do {
                let labelTuple = try primaryKeysSubspace.unpack(key)
                let labelValue = try HNSWLabelCodec.decode(labelTuple)
                guard primaryKeysByLabel.updateValue(
                    try Tuple(packed: value),
                    forKey: labelValue
                ) == nil else {
                    throw VectorIndexError.invalidStructure(
                        "Duplicate HNSW primary-key label"
                    )
                }
            } catch let error as VectorIndexError {
                throw error
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW primary-key mapping"
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
    ) throws -> Int {
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
        return cachedBytes
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
        primaryKeys: [UInt64: Tuple],
        transaction: any TransactionAccess
    ) async throws {
        let (begin, end) = labelsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 0,
            reverse: false,
            snapshot: true,
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
            let primaryKey: Tuple
            do {
                primaryKey = try labelsSubspace.unpack(key)
            } catch {
                throw VectorIndexError.invalidStructure(
                    "Invalid HNSW forward primary-key mapping"
                )
            }
            let label = try HNSWLabelCodec.decodePacked(value)
            guard primaryKeys[label]?.pack() == primaryKey.pack() else {
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

    private func hasPersistedVector(
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 1,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        let value = try await cursor.next()
        try await cursor.finish()
        return value != nil
    }

    private func hasPersistedForwardMapping(
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let (begin, end) = labelsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 1,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        let value = try await cursor.next()
        try await cursor.finish()
        return value != nil
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
        transaction: any TransactionAccess
    ) async throws -> Int {
        let (begin, end) = vectorsSubspace.range()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: 100_001,
            reverse: false,
            snapshot: true,
            streamingMode: .iterator
        )
        var entryCount = 0
        try await cursor.consume { _, _ in
            entryCount += 1
        }
        return max(1, min(entryCount, 100_000) * 2)
    }
}
