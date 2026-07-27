import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

struct DatabasePersistentJobStore: Sendable {
    struct PreparedSpecification: Sendable {
        let value: DatabasePersistentJobSpecification
        let encoded: ByteString
        let digest: ByteString
    }

    private let container: DBContainer
    private let specifications: Subspace
    private let plans: Subspace
    private let states: Subspace
    private let due: Subspace
    private let resultManifests: Subspace
    private let resultChunks: Subspace
    private let wireLimits: DatabaseWireLimits
    private let storageLimits: DatabasePersistentJobStorageLimits
    private let unsuccessfulOutcomeWireLimits: DatabaseWireLimits

    init(
        container: DBContainer,
        wireLimits: DatabaseWireLimits,
        storageLimits: DatabasePersistentJobStorageLimits
    ) async throws {
        try storageLimits.validate(wireLimits: wireLimits)
        let root = try await container.engine.resolveOrCreateNamespace(
            path: ["database-framework", "persistent-jobs"]
        )
        self.container = container
        self.specifications = root.subspace("specifications")
        self.plans = root.subspace("plans")
        self.states = root.subspace("states")
        self.due = root.subspace("due")
        self.resultManifests = root.subspace("result-manifests")
        self.resultChunks = root.subspace("result-chunks")
        self.wireLimits = wireLimits
        self.storageLimits = storageLimits
        self.unsuccessfulOutcomeWireLimits =
            try storageLimits.unsuccessfulOutcomeWireLimits(
                basedOn: wireLimits
            )
    }

    func create(
        specification preparedSpecification: PreparedSpecification,
        plan: DatabasePersistentJobPlan,
        state: DatabasePersistentJobState,
        transaction: any TransactionAccess
    ) async throws {
        let specification = preparedSpecification.value
        let jobID = specification.jobID
        let existingSpecification = try await transaction.getValue(
            for: specificationKey(jobID),
            snapshot: false
        )
        let existingPlan = try await transaction.getValue(
            for: planKey(jobID),
            snapshot: false
        )
        let existingState = try await transaction.getValue(
            for: stateKey(jobID),
            snapshot: false
        )
        guard existingSpecification == nil,
              existingPlan == nil,
              existingState == nil else {
            throw DatabaseJobRuntimeError.duplicateJobIdentifier(jobID)
        }

        try specification.validate()
        try plan.validate()
        try state.validate()
        let specificationBytes = preparedSpecification.encoded
        let specificationDigest = preparedSpecification.digest
        guard plan.jobID == jobID,
              plan.operation == specification.operation,
              plan.specificationDigest == specificationDigest,
              DatabasePersistentJobDigest.plan(
                  operation: specification.operation,
                  payload: plan.payload
              ) == specification.planDigest,
              state.jobID == jobID,
              state.specificationDigest == specificationDigest,
              state.revision == 0,
              state.status == .pending,
              state.updatedAt >= specification.createdAt else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        let planBytes = try encodePlan(plan)
        let stateBytes = try encodeState(state)

        try transaction.setValue(
            specificationBytes,
            for: specificationKey(jobID)
        )
        try transaction.setValue(
            planBytes,
            for: planKey(jobID)
        )
        try transaction.setValue(
            stateBytes,
            for: stateKey(jobID)
        )
        try writeDueEntry(for: state, transaction: transaction)
    }

    func prepareSpecification(
        _ specification: DatabasePersistentJobSpecification
    ) throws -> PreparedSpecification {
        try specification.validate()
        let encoded = try encodeSpecification(specification)
        return PreparedSpecification(
            value: specification,
            encoded: encoded,
            digest: DatabasePersistentJobDigest.specification(
                operation: specification.operation,
                payload: encoded
            )
        )
    }

    func load(
        _ jobID: DatabaseTypes.UUID,
        transaction: any TransactionAccess,
        snapshot: Bool = false
    ) async throws -> DatabasePersistentJobSnapshot? {
        let specificationValue = try await transaction.getValue(
            for: specificationKey(jobID),
            snapshot: snapshot
        )
        let planValue = try await transaction.getValue(
            for: planKey(jobID),
            snapshot: snapshot
        )
        let stateValue = try await transaction.getValue(
            for: stateKey(jobID),
            snapshot: snapshot
        )
        guard specificationValue != nil || planValue != nil
                || stateValue != nil else {
            return nil
        }
        guard let specificationStorage = specificationValue else {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
        guard let planStorage = planValue else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
        guard let stateStorage = stateValue else {
            throw DatabaseJobRuntimeError.corruptedState
        }

        let specificationBytes = specificationStorage
        let planBytes = planStorage
        let stateBytes = stateStorage
        let specification = try decodeSpecification(specificationBytes)
        let plan = try decodePlan(planBytes)
        let state = try decodeState(stateBytes)
        let specificationDigest = DatabasePersistentJobDigest.specification(
            operation: specification.operation,
            payload: specificationBytes
        )

        guard specification.jobID == jobID else {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
        guard plan.jobID == jobID,
              plan.operation == specification.operation,
              plan.specificationDigest == specificationDigest,
              DatabasePersistentJobDigest.plan(
                  operation: specification.operation,
                  payload: plan.payload
              ) == specification.planDigest else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
        guard state.jobID == jobID,
              state.specificationDigest == specificationDigest,
              state.updatedAt >= specification.createdAt else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        return DatabasePersistentJobSnapshot(
            specification: specification,
            specificationDigest: specificationDigest,
            plan: plan,
            state: state
        )
    }

    func load(_ jobID: DatabaseTypes.UUID) async throws -> DatabasePersistentJobSnapshot? {
        try await container.engine.withTransaction(configuration: .readOnly) {
            transaction in
            try await load(jobID, transaction: transaction, snapshot: true)
        }
    }

    func loadState(
        _ jobID: DatabaseTypes.UUID,
        specificationDigest: ByteString,
        transaction: any TransactionAccess
    ) async throws -> DatabasePersistentJobState {
        guard let value = try await transaction.getValue(
            for: stateKey(jobID),
            snapshot: false
        ) else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        let state = try decodeState(value)
        guard state.jobID == jobID,
              state.specificationDigest == specificationDigest else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        return state
    }

    func storeState(
        _ state: DatabasePersistentJobState,
        replacing previous: DatabasePersistentJobState,
        transaction: any TransactionAccess
    ) throws {
        let expectedRevision = previous.revision.addingReportingOverflow(1)
        guard !expectedRevision.overflow,
              state.jobID == previous.jobID,
              state.specificationDigest == previous.specificationDigest,
              state.revision == expectedRevision.partialValue else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        try state.validate()
        let stateBytes = try encodeState(state)
        if let previousScheduledAt = previous.scheduledAt {
            try transaction.clear(
                key: dueKey(previousScheduledAt, jobID: state.jobID)
            )
        }
        try writeDueEntry(for: state, transaction: transaction)
        try transaction.setValue(
            stateBytes,
            for: stateKey(state.jobID)
        )
    }

    func storeResult(
        _ responsePayload: ByteString,
        snapshot: DatabasePersistentJobSnapshot,
        completedAt: Timestamp,
        transaction: any TransactionAccess
    ) async throws -> JobResultDigest {
        let count = responsePayload.count
        guard count <= storageLimits.maximumResultBytes else {
            throw DatabaseJobRuntimeError.responseTooLarge(
                actual: count,
                maximum: storageLimits.maximumResultBytes
            )
        }
        guard try await transaction.getValue(
            for: resultManifestKey(snapshot.specification.jobID),
            snapshot: false
        ) == nil else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        let chunkSize = storageLimits.resultChunkBytes
        let chunkCount = count == 0 ? 0 : ((count - 1) / chunkSize) + 1
        guard let exactChunkSize = UInt32(exactly: chunkSize),
              let exactChunkCount = UInt32(exactly: chunkCount),
              let exactTotalBytes = UInt64(exactly: count) else {
            throw DatabaseJobRuntimeError.responseTooLarge(
                actual: count,
                maximum: storageLimits.maximumResultBytes
            )
        }
        let responseDigest = DatabasePersistentJobDigest.result(
            operation: snapshot.specification.operation,
            payload: responsePayload
        )
        var chunkDigests: [ByteString] = []
        chunkDigests.reserveCapacity(chunkCount)
        for chunkIndex in 0..<chunkCount {
            let start = chunkIndex * chunkSize
            let end = min(start + chunkSize, count)
            let chunk = responsePayload[start..<end]
            let exactIndex = UInt32(chunkIndex)
            chunkDigests.append(
                DatabasePersistentJobDigest.chunk(
                    operation: snapshot.specification.operation,
                    index: exactIndex,
                    payload: chunk
                )
            )
            try transaction.setValue(
                chunk,
                for: resultChunkKey(
                    snapshot.specification.jobID,
                    index: exactIndex
                )
            )
        }
        let manifest = DatabasePersistentJobResultManifest(
            jobID: snapshot.specification.jobID,
            operation: snapshot.specification.operation,
            specificationDigest: snapshot.specificationDigest,
            responseDigest: responseDigest,
            totalBytes: exactTotalBytes,
            chunkBytes: exactChunkSize,
            chunkCount: exactChunkCount,
            chunkDigests: chunkDigests,
            createdAt: completedAt
        )
        try manifest.validate()
        let manifestBytes = try ServerPayloadEncoder.encode(
            manifest,
            limits: wireLimits
        )
        guard manifestBytes.count <= storageLimits.maximumSpecificationBytes else {
            throw DatabaseJobRuntimeError.specificationTooLarge(
                actual: manifestBytes.count,
                maximum: storageLimits.maximumSpecificationBytes
            )
        }
        try transaction.setValue(
            manifestBytes,
            for: resultManifestKey(snapshot.specification.jobID)
        )
        return responseDigest
    }

    func loadResultManifest(
        for snapshot: DatabasePersistentJobSnapshot
    ) async throws -> DatabasePersistentJobResultManifest {
        try await container.engine.withTransaction(configuration: .readOnly) {
            transaction in
            guard let value = try await transaction.getValue(
                for: resultManifestKey(snapshot.specification.jobID),
                snapshot: true
            ) else {
                throw DatabaseJobRuntimeError.corruptedResult
            }
            guard value.count <= storageLimits.maximumSpecificationBytes else {
                throw DatabaseJobRuntimeError.corruptedResult
            }
            let manifest: DatabasePersistentJobResultManifest
            do {
                manifest = try ServerPayloadDecoder.decode(
                    DatabasePersistentJobResultManifest.self,
                    from: value,
                    limits: wireLimits
                )
                try manifest.validate()
            } catch let error as DatabaseJobRuntimeError {
                throw error
            } catch {
                throw DatabaseJobRuntimeError.corruptedResult
            }
            guard manifest.jobID == snapshot.specification.jobID,
                  manifest.operation == snapshot.specification.operation,
                  manifest.specificationDigest == snapshot.specificationDigest,
                  manifest.responseDigest == snapshot.state.resultDigest,
                  manifest.totalBytes <= UInt64(storageLimits.maximumResultBytes),
                  manifest.chunkBytes == UInt32(storageLimits.resultChunkBytes) else {
                throw DatabaseJobRuntimeError.corruptedResult
            }
            return manifest
        }
    }

    func loadResultChunk(
        manifest: DatabasePersistentJobResultManifest,
        index: UInt32
    ) async throws -> ByteString {
        guard index < manifest.chunkCount,
              Int(index) < manifest.chunkDigests.count else {
            throw DatabaseJobRuntimeError.invalidResultContinuation
        }
        return try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            guard let value = try await transaction.getValue(
                for: resultChunkKey(manifest.jobID, index: index),
                snapshot: true
            ) else {
                throw DatabaseJobRuntimeError.resultChunkMissing(
                    jobID: manifest.jobID,
                    index: index
                )
            }
            let chunk = value
            let expectedCount = Self.expectedChunkCount(
                manifest: manifest,
                index: index
            )
            guard chunk.count == expectedCount,
                  DatabasePersistentJobDigest.chunk(
                      operation: manifest.operation,
                      index: index,
                      payload: chunk
                  ) == manifest.chunkDigests[Int(index)] else {
                throw DatabaseJobRuntimeError.corruptedResult
            }
            return chunk
        }
    }

    func dueJobs(
        through timestamp: Timestamp,
        limit: Int
    ) async throws -> [DatabasePersistentJobDueEntry] {
        guard limit > 0 else { return [] }
        return try await container.engine.withTransaction(
            configuration: .readOnly
        ) { transaction in
            let range = due.range()
            let entries = try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                limit: limit,
                snapshot: true,
                streamingMode: .iterator
            )
            var dueJobs: [DatabasePersistentJobDueEntry] = []
            dueJobs.reserveCapacity(entries.count)
            for (key, value) in entries {
                let entry = try scheduledEntry(from: key, value: value)
                guard entry.timestamp <= timestamp else { break }
                dueJobs.append(entry.dueEntry)
            }
            return dueJobs
        }
    }

    func earliestScheduledAt() async throws -> Timestamp? {
        try await container.engine.withTransaction(configuration: .readOnly) {
            transaction in
            let range = due.range()
            let entries = try await transaction.collectRange(
                begin: range.begin,
                end: range.end,
                limit: 1,
                snapshot: true,
                streamingMode: .exact
            )
            guard let first = entries.first else { return nil }
            return try scheduledEntry(from: first.0, value: first.1).timestamp
        }
    }

    private func encodeSpecification(
        _ specification: DatabasePersistentJobSpecification
    ) throws -> ByteString {
        let bytes = try ServerPayloadEncoder.encode(
            specification,
            limits: wireLimits
        )
        guard bytes.count <= storageLimits.maximumSpecificationBytes else {
            throw DatabaseJobRuntimeError.specificationTooLarge(
                actual: bytes.count,
                maximum: storageLimits.maximumSpecificationBytes
            )
        }
        return bytes
    }

    private func encodePlan(
        _ plan: DatabasePersistentJobPlan
    ) throws -> ByteString {
        let bytes = try ServerPayloadEncoder.encode(plan, limits: wireLimits)
        guard bytes.count <= storageLimits.maximumPlanBytes else {
            throw DatabaseJobRuntimeError.planTooLarge(
                actual: bytes.count,
                maximum: storageLimits.maximumPlanBytes
            )
        }
        return bytes
    }

    private func encodeState(
        _ state: DatabasePersistentJobState
    ) throws -> ByteString {
        guard state.operationStatePayload.count
                <= storageLimits.maximumOperationStateBytes else {
            throw DatabaseJobRuntimeError.stateTooLarge(
                actual: state.operationStatePayload.count,
                maximum: storageLimits.maximumOperationStateBytes
            )
        }
        try validateUnsuccessfulOutcome(in: state)
        let bytes = try ServerPayloadEncoder.encode(state, limits: wireLimits)
        guard bytes.count <= storageLimits.maximumStateBytes else {
            throw DatabaseJobRuntimeError.stateTooLarge(
                actual: bytes.count,
                maximum: storageLimits.maximumStateBytes
            )
        }
        return bytes
    }

    private func decodeSpecification(
        _ bytes: ByteString
    ) throws -> DatabasePersistentJobSpecification {
        guard bytes.count <= storageLimits.maximumSpecificationBytes else {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
        do {
            let value = try ServerPayloadDecoder.decode(
                DatabasePersistentJobSpecification.self,
                from: bytes,
                limits: wireLimits
            )
            try value.validate()
            return value
        } catch let error as DatabaseJobRuntimeError {
            throw error
        } catch {
            throw DatabaseJobRuntimeError.corruptedSpecification
        }
    }

    private func decodePlan(
        _ bytes: ByteString
    ) throws -> DatabasePersistentJobPlan {
        guard bytes.count <= storageLimits.maximumPlanBytes else {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
        do {
            let value = try ServerPayloadDecoder.decode(
                DatabasePersistentJobPlan.self,
                from: bytes,
                limits: wireLimits
            )
            try value.validate()
            guard value.payload.count
                    <= storageLimits.maximumPlanPayloadBytes else {
                throw DatabaseJobRuntimeError.corruptedPlan
            }
            return value
        } catch let error as DatabaseJobRuntimeError {
            throw error
        } catch {
            throw DatabaseJobRuntimeError.corruptedPlan
        }
    }

    private func decodeState(
        _ bytes: ByteString
    ) throws -> DatabasePersistentJobState {
        guard bytes.count <= storageLimits.maximumStateBytes else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        do {
            let value = try ServerPayloadDecoder.decode(
                DatabasePersistentJobState.self,
                from: bytes,
                limits: wireLimits
            )
            try value.validate()
            guard value.operationStatePayload.count
                    <= storageLimits.maximumOperationStateBytes else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            do {
                try validateUnsuccessfulOutcome(in: value)
            } catch {
                throw DatabaseJobRuntimeError.corruptedState
            }
            return value
        } catch let error as DatabaseJobRuntimeError {
            throw error
        } catch {
            throw DatabaseJobRuntimeError.corruptedState
        }
    }

    private func validateUnsuccessfulOutcome(
        in state: DatabasePersistentJobState
    ) throws(DatabaseJobRuntimeError) {
        let outcome: DatabaseJobUnsuccessfulOutcome
        if let pending = state.pendingUnsuccessfulOutcome {
            outcome = pending
        } else if let failure = state.failure {
            outcome = .failed(failure)
        } else {
            return
        }
        do {
            _ = try PersistentJobPayloadStorage.encodedByteCount(
                outcome,
                limits: unsuccessfulOutcomeWireLimits
            )
        } catch let error as DatabaseWireError {
            throw DatabaseJobRuntimeError
                .unsuccessfulOutcomeExceedsLimits(error)
        } catch {
            throw DatabaseJobRuntimeError
                .unsuccessfulOutcomeExceedsLimits(
                    .invalidFieldValueWireState
                )
        }
    }

    private func writeDueEntry(
        for state: DatabasePersistentJobState,
        transaction: any TransactionAccess
    ) throws {
        guard let scheduledAt = state.scheduledAt else { return }
        let value = try ServerPayloadEncoder.encode(
            DatabasePersistentJobDueEntry(
                jobID: state.jobID,
                stateRevision: state.revision
            ),
            limits: wireLimits
        )
        try transaction.setValue(
            value,
            for: dueKey(scheduledAt, jobID: state.jobID)
        )
    }

    private func specificationKey(_ jobID: DatabaseTypes.UUID) -> ByteString {
        specifications.pack(Tuple(jobID))
    }

    private func planKey(_ jobID: DatabaseTypes.UUID) -> ByteString {
        plans.pack(Tuple(jobID))
    }

    private func stateKey(_ jobID: DatabaseTypes.UUID) -> ByteString {
        states.pack(Tuple(jobID))
    }

    private func resultManifestKey(_ jobID: DatabaseTypes.UUID) -> ByteString {
        resultManifests.pack(Tuple(jobID))
    }

    private func resultChunkKey(
        _ jobID: DatabaseTypes.UUID,
        index: UInt32
    ) -> ByteString {
        resultChunks.pack(Tuple(jobID, Int64(index)))
    }

    private func dueKey(
        _ timestamp: Timestamp,
        jobID: DatabaseTypes.UUID
    ) -> ByteString {
        due.pack(
            Tuple(
                timestamp.secondsSinceUnixEpoch,
                Int64(timestamp.nanoseconds),
                jobID
            )
        )
    }

    private func scheduledEntry(
        from key: ByteString,
        value: ByteString
    ) throws -> (
        timestamp: Timestamp,
        dueEntry: DatabasePersistentJobDueEntry
    ) {
        do {
            guard due.contains(key), key.count > due.prefix.count else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            let suffixStart = key.startIndex + due.prefix.count
            let encoded = key[suffixStart..<key.endIndex]
            var offset = encoded.startIndex
            guard offset < encoded.endIndex else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            offset += 1
            let seconds = try Int64.decodeTuple(from: encoded, at: &offset)
            guard offset < encoded.endIndex else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            offset += 1
            let nanoseconds = try Int64.decodeTuple(from: encoded, at: &offset)
            guard offset < encoded.endIndex else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            offset += 1
            let keyJobID = try DatabaseTypes.UUID.decodeTuple(
                from: encoded,
                at: &offset
            )
            let dueEntry = try ServerPayloadDecoder.decode(
                DatabasePersistentJobDueEntry.self,
                from: value,
                limits: wireLimits
            )
            guard offset == encoded.endIndex,
                  nanoseconds >= 0,
                  let exactNanoseconds = UInt32(exactly: nanoseconds),
                  keyJobID == dueEntry.jobID else {
                throw DatabaseJobRuntimeError.corruptedState
            }
            return (
                try Timestamp(
                    secondsSinceUnixEpoch: seconds,
                    nanoseconds: exactNanoseconds
                ),
                dueEntry
            )
        } catch let error as DatabaseJobRuntimeError {
            throw error
        } catch {
            throw DatabaseJobRuntimeError.corruptedState
        }
    }

    private static func expectedChunkCount(
        manifest: DatabasePersistentJobResultManifest,
        index: UInt32
    ) -> Int {
        let start = UInt64(index) * UInt64(manifest.chunkBytes)
        let remaining = manifest.totalBytes - start
        return Int(min(remaining, UInt64(manifest.chunkBytes)))
    }
}
