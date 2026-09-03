import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

package struct VectorFusionIndexReadExecutor: FusionIndexReadExecutor {
    package let indexType: IndexType = .vector

    package init() {}

    package func validate(
        _ request: FusionIndexValidationRequest
    ) throws {
        let prepared = try parameters(from: request.source.parameters)
        guard request.descriptor.type == indexType,
              request.descriptor.fieldIdentities
                == request.source.referencedFields,
              request.descriptor.fieldNames == [prepared.fieldName],
              case .vector(
                let embedding,
                let dimensions,
                let metric
              ) = request.descriptor.declaration.definition,
              embedding == request.source.referencedFields.first,
              dimensions == prepared.dimensions,
              metric == prepared.metric else {
            throw invalid(VectorReadParameter.fieldName)
        }
        guard request.scoring == .annotation(
            name: "distance",
            order: .lowerIsBetter
        ) else {
            throw invalid("scoring")
        }
    }

    package func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            guard request.limit > 0 else { return .satisfiedLimit }
            let prepared = try parameters(from: request.source.parameters)
            let layout = try VectorFusionPhysicalLayout(
                request.access.index.physicalLayout
            )
            var topK = try VectorFusionTopK(
                limit: request.limit,
                workMeter: request.workMeter
            )
            try await scan(
                layout: layout,
                prepared: prepared,
                request: request,
                topK: &topK
            )
            try topK.emit(to: output)
            return .exhausted
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    package func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            guard request.limit > 0 else { return .satisfiedLimit }
            let prepared = try parameters(from: request.source.parameters)
            let layout = try VectorFusionPhysicalLayout(
                request.access.index.physicalLayout
            )
            var topK = try VectorFusionTopK(
                limit: request.limit,
                workMeter: request.workMeter
            )
            for index in 0..<candidates.count {
                let primaryKey = candidates.primaryKey(at: index)
                guard let vector = try await vector(
                    for: primaryKey,
                    layout: layout,
                    request: request
                ) else {
                    continue
                }
                try consider(
                    primaryKey: primaryKey,
                    vectorBytes: vector,
                    prepared: prepared,
                    request: request,
                    topK: &topK
                )
            }
            try topK.emit(to: output)
            return .exhausted
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    private func scan(
        layout: VectorFusionPhysicalLayout,
        prepared: VectorFusionPreparedRead,
        request: FusionIndexReadRequest,
        topK: inout VectorFusionTopK
    ) async throws {
        let root = request.access.index.subspace
        switch layout {
        case .flat:
            try await scanVectors(
                in: root,
                prepared: prepared,
                request: request,
                topK: &topK
            )
        case .hnsw:
            let labels = root.subspace("l")
            let cursor = try openFusionSubspaceCursor(
                using: request.access,
                in: labels,
                reverse: false
            )
            while let row = try await cursor.next() {
                let primaryKey = try primaryKey(
                    from: row.key,
                    subspace: labels
                )
                let label = try HNSWLabelCodec.decodePacked(row.value)
                let reverse = try await pointValue(
                    in: root.subspace("p"),
                    tuple: HNSWLabelCodec.tuple(label),
                    request: request
                )
                guard reverse == primaryKey else {
                    throw VectorFusionStorageError.corrupted
                }
                guard let vector = try await pointValue(
                    in: root.subspace("v"),
                    tuple: HNSWLabelCodec.tuple(label),
                    request: request
                ) else {
                    throw VectorFusionStorageError.corrupted
                }
                try consider(
                    primaryKey: primaryKey,
                    vectorBytes: vector,
                    prepared: prepared,
                    request: request,
                    topK: &topK
                )
            }
        case .ivf(let nlist):
            let assignments = root.subspace(
                IVFIndexStorageKey.assignments.rawValue
            )
            let cursor = try openFusionSubspaceCursor(
                using: request.access,
                in: assignments,
                reverse: false
            )
            while let row = try await cursor.next() {
                let primaryKey = try primaryKey(
                    from: row.key,
                    subspace: assignments
                )
                let cluster = try clusterID(from: row.value, nlist: nlist)
                guard let vector = try await pointValue(
                    in: root
                        .subspace(IVFIndexStorageKey.lists.rawValue)
                        .subspace(cluster),
                    primaryKey: primaryKey,
                    request: request
                ) else {
                    throw VectorFusionStorageError.corrupted
                }
                try consider(
                    primaryKey: primaryKey,
                    vectorBytes: vector,
                    prepared: prepared,
                    request: request,
                    topK: &topK
                )
            }
        case .pq:
            try await scanVectors(
                in: root.subspace(PQIndexStorageKey.vectors.rawValue),
                prepared: prepared,
                request: request,
                topK: &topK
            )
        }
    }

    private func scanVectors(
        in subspace: Subspace,
        prepared: VectorFusionPreparedRead,
        request: FusionIndexReadRequest,
        topK: inout VectorFusionTopK
    ) async throws {
        let cursor = try openFusionSubspaceCursor(
            using: request.access,
            in: subspace,
            reverse: false
        )
        while let row = try await cursor.next() {
            try consider(
                primaryKey: try primaryKey(
                    from: row.key,
                    subspace: subspace
                ),
                vectorBytes: row.value,
                prepared: prepared,
                request: request,
                topK: &topK
            )
        }
    }

    private func vector(
        for primaryKey: ByteString,
        layout: VectorFusionPhysicalLayout,
        request: FusionIndexReadRequest
    ) async throws -> ByteString? {
        let root = request.access.index.subspace
        switch layout {
        case .flat:
            return try await pointValue(
                in: root,
                primaryKey: primaryKey,
                request: request
            )
        case .hnsw:
            guard let labelValue = try await pointValue(
                in: root.subspace("l"),
                primaryKey: primaryKey,
                request: request
            ) else {
                return nil
            }
            let label = try HNSWLabelCodec.decodePacked(labelValue)
            let labelTuple = HNSWLabelCodec.tuple(label)
            guard let reverse = try await pointValue(
                in: root.subspace("p"),
                tuple: labelTuple,
                request: request
            ), reverse == primaryKey,
                  let vector = try await pointValue(
                    in: root.subspace("v"),
                    tuple: labelTuple,
                    request: request
                  ) else {
                throw VectorFusionStorageError.corrupted
            }
            return vector
        case .ivf(let nlist):
            guard let assignment = try await pointValue(
                in: root.subspace(IVFIndexStorageKey.assignments.rawValue),
                primaryKey: primaryKey,
                request: request
            ) else {
                return nil
            }
            let cluster = try clusterID(from: assignment, nlist: nlist)
            guard let vector = try await pointValue(
                in: root
                    .subspace(IVFIndexStorageKey.lists.rawValue)
                    .subspace(cluster),
                primaryKey: primaryKey,
                request: request
            ) else {
                throw VectorFusionStorageError.corrupted
            }
            return vector
        case .pq:
            return try await pointValue(
                in: root.subspace(PQIndexStorageKey.vectors.rawValue),
                primaryKey: primaryKey,
                request: request
            )
        }
    }

    private func consider(
        primaryKey: ByteString,
        vectorBytes: ByteString,
        prepared: VectorFusionPreparedRead,
        request: FusionIndexReadRequest,
        topK: inout VectorFusionTopK
    ) throws {
        let candidate: PersistedVectorView
        do {
            candidate = try VectorConversion.persistedVector(
                vectorBytes,
                expectedCount: prepared.dimensions
            )
        } catch {
            throw VectorFusionStorageError.corrupted
        }
        try request.workMeter.consume(
            UInt64(prepared.dimensions),
            at: .indexScan
        )
        let distance: Double
        do {
            distance = try VectorConversion.distance(
                metric: prepared.metric,
                from: prepared.queryVector,
                to: candidate
            )
        } catch {
            throw VectorFusionStorageError.corrupted
        }
        guard distance.isFinite else {
            throw VectorFusionStorageError.nonFiniteDistance
        }
        try topK.consider(primaryKey: primaryKey, distance: distance)
    }

    private func primaryKey(
        from key: ByteString,
        subspace: Subspace
    ) throws -> ByteString {
        guard subspace.contains(key) else {
            throw VectorFusionStorageError.corrupted
        }
        let primaryKey = key[subspace.prefix.count..<key.endIndex]
        var cursor = TupleCursor(bytes: primaryKey)
        guard try cursor.next() != nil else {
            throw VectorFusionStorageError.corrupted
        }
        while try cursor.next() != nil {}
        return primaryKey
    }

    private func clusterID(
        from bytes: ByteString,
        nlist: Int
    ) throws -> Int64 {
        do {
            var cursor = TupleCursor(bytes: bytes)
            let cluster = try cursor.requireInt64()
            guard cursor.isAtEnd,
                  cluster >= 0,
                  cluster < Int64(nlist) else {
                throw VectorFusionStorageError.corrupted
            }
            return cluster
        } catch let error as VectorFusionStorageError {
            throw error
        } catch {
            throw VectorFusionStorageError.corrupted
        }
    }

    private func pointValue(
        in subspace: Subspace,
        primaryKey: ByteString,
        request: FusionIndexReadRequest
    ) async throws -> ByteString? {
        let byteCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(subspace.prefix.count)
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(primaryKey.count))
        ).bytes
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: byteCount,
            at: .indexScan
        )
        do {
            // A point-read API requires one owned contiguous key. The primary
            // key itself remains a borrowed view; only this boundary key is
            // materialized and its exact lifetime is request-metered.
            let key = subspace.prefix.appending(contentsOf: primaryKey)
            let value = try await request.access.getValue(key: key)?.bytes
            reservation.release()
            return value
        } catch {
            reservation.release()
            throw error
        }
    }

    private func pointValue(
        in subspace: Subspace,
        tuple: Tuple,
        request: FusionIndexReadRequest
    ) async throws -> ByteString? {
        let keyByteCount = try DatabaseIntermediateFootprint(
            bytes: UInt64(subspace.prefix.count)
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(tuple.packedByteCount)
            )
        ).bytes
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: keyByteCount,
            at: .indexScan
        )
        do {
            let value = try await request.access.getValue(
                key: subspace.pack(tuple)
            )?.bytes
            reservation.release()
            return value
        } catch {
            reservation.release()
            throw error
        }
    }

    private func parameters(
        from values: [String: FieldValue]
    ) throws -> VectorFusionPreparedRead {
        let expected: Set<String> = [
            VectorReadParameter.fieldName,
            VectorReadParameter.dimensions,
            VectorReadParameter.queryVector,
            VectorReadParameter.metric,
        ]
        guard Set(values.keys) == expected,
              case .string(let fieldName)? = values[
                VectorReadParameter.fieldName
              ],
              case .int64(let encodedDimensions)? = values[
                VectorReadParameter.dimensions
              ],
              let dimensions = Int(exactly: encodedDimensions),
              dimensions > 0,
              case .vector(let queryVector)? = values[
                VectorReadParameter.queryVector
              ],
              queryVector.elementType == .float32,
              queryVector.count == dimensions,
              case .string(let encodedMetric)? = values[
                VectorReadParameter.metric
              ],
              let metric = VectorMetric(rawValue: encodedMetric) else {
            throw invalid(VectorReadParameter.queryVector)
        }
        return VectorFusionPreparedRead(
            fieldName: fieldName,
            dimensions: dimensions,
            metric: metric,
            queryVector: queryVector
        )
    }

    private func invalid(_ parameter: String) -> FusionExecutionError {
        .invalidIndexInput(indexType: indexType, parameter: parameter)
    }

    private func sanitizedExecutionError(_ error: any Error) -> any Error {
        if error is TupleError || error is VectorIndexError {
            return FusionExecutionError.corruptedIndex(indexType)
        }
        if let storageError = error as? VectorFusionStorageError {
            switch storageError {
            case .corrupted:
                return FusionExecutionError.corruptedIndex(indexType)
            case .nonFiniteDistance:
                return FusionExecutionError.invalidIndexScore(indexType)
            }
        }
        if let cleanup = error as? StorageRangeTerminalCleanupError {
            return StorageRangeTerminalCleanupError(
                cleanupError: sanitizedExecutionError(cleanup.cleanupError)
            )
        }
        if let cleanup = error as? StorageRangeCleanupError {
            return StorageRangeCleanupError(
                iterationError: sanitizedExecutionError(
                    cleanup.iterationError
                ),
                cleanupError: sanitizedExecutionError(cleanup.cleanupError)
            )
        }
        return error
    }
}

private struct VectorFusionPreparedRead: Sendable {
    let fieldName: String
    let dimensions: Int
    let metric: VectorMetric
    let queryVector: Vector
}

private enum VectorFusionPhysicalLayout: Sendable {
    case flat
    case hnsw
    case ivf(nlist: Int)
    case pq

    init(_ layout: IndexPhysicalLayout) throws {
        guard layout.revision == 1 else {
            throw FusionExecutionError.executionContractViolation
        }
        switch layout.name {
        case "vector.flat":
            guard layout.parameters.fields.isEmpty else {
                throw FusionExecutionError.executionContractViolation
            }
            self = .flat
        case "vector.hnsw":
            try Self.requirePositiveIntegers(
                ["efConstruction", "m"],
                in: layout.parameters
            )
            self = .hnsw
        case "vector.ivf":
            try Self.requirePositiveIntegers(
                ["kmeansIterations", "nlist"],
                in: layout.parameters
            )
            guard let encodedNList = layout.parameters["nlist"]?.int64Value,
                  let nlist = Int(exactly: encodedNList) else {
                throw FusionExecutionError.executionContractViolation
            }
            self = .ivf(nlist: nlist)
        case "vector.pq":
            try Self.requirePositiveIntegers(
                ["ksub", "m", "niter"],
                in: layout.parameters
            )
            self = .pq
        default:
            throw FusionExecutionError.executionContractViolation
        }
    }

    private static func requirePositiveIntegers(
        _ names: Set<String>,
        in parameters: FieldObject
    ) throws {
        guard Set(parameters.fields.map { $0.key }) == names else {
            throw FusionExecutionError.executionContractViolation
        }
        for name in names {
            guard let value = parameters[name]?.int64Value,
                  value > 0 else {
                throw FusionExecutionError.executionContractViolation
            }
        }
    }
}

private enum VectorFusionStorageError: Error, Sendable {
    case corrupted
    case nonFiniteDistance
}
