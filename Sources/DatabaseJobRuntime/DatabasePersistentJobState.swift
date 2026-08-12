import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabasePersistentJobState: DatabaseRuntimePayloadValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    package let jobID: DatabaseTypes.UUID
    package let specificationDigest: ByteString
    package let revision: UInt64
    package let status: JobStatusOperation.State
    package let operationStatePayload: ByteString
    package let completedWorkUnits: UInt64
    package let totalWorkUnits: UInt64?
    package let executionCount: UInt64
    package let currentSliceAttempt: UInt32
    package let unsuccessfulOutcomeCommitAttempt: UInt64
    package let pendingUnsuccessfulOutcome: DatabaseJobUnsuccessfulOutcome?
    package let lastUnsuccessfulOutcomeCommitError: RemoteOperationError?
    package let cancellationRequested: Bool
    package let nextAttemptAt: Timestamp?
    package let leaseOwner: DatabaseTypes.UUID?
    package let leaseToken: DatabaseTypes.UUID?
    package let leaseExpiresAt: Timestamp?
    package let resultDigest: JobResultDigest?
    package let failure: RemoteOperationError?
    package let updatedAt: Timestamp

    package var scheduledAt: Timestamp? {
        switch status {
        case .pending:
            return nextAttemptAt
        case .running:
            return leaseExpiresAt
        case .committingUnsuccessfulOutcome:
            return leaseToken == nil ? nextAttemptAt : leaseExpiresAt
        case .succeeded, .failed, .cancelled:
            return nil
        }
    }

    package func acquiringLease(
        owner: DatabaseTypes.UUID,
        token: DatabaseTypes.UUID,
        expiresAt: Timestamp,
        updatedAt: Timestamp
    ) throws -> Self {
        switch status {
        case .pending, .running:
            guard executionCount < UInt64.max,
                  currentSliceAttempt < UInt32.max,
                  pendingUnsuccessfulOutcome == nil else {
                throw DatabaseJobRuntimeError.invalidStateTransition
            }
            return try advancing(
                status: .running,
                operationStatePayload: operationStatePayload,
                completedWorkUnits: completedWorkUnits,
                totalWorkUnits: totalWorkUnits,
                executionCount: executionCount + 1,
                currentSliceAttempt: currentSliceAttempt + 1,
                unsuccessfulOutcomeCommitAttempt: 0,
                pendingUnsuccessfulOutcome: nil,
                lastUnsuccessfulOutcomeCommitError: nil,
                cancellationRequested: cancellationRequested,
                nextAttemptAt: nil,
                leaseOwner: owner,
                leaseToken: token,
                leaseExpiresAt: expiresAt,
                resultDigest: nil,
                failure: nil,
                updatedAt: updatedAt
            )
        case .committingUnsuccessfulOutcome:
            guard unsuccessfulOutcomeCommitAttempt < UInt64.max,
                  pendingUnsuccessfulOutcome != nil else {
                throw DatabaseJobRuntimeError.invalidStateTransition
            }
            return try advancing(
                status: .committingUnsuccessfulOutcome,
                operationStatePayload: operationStatePayload,
                completedWorkUnits: completedWorkUnits,
                totalWorkUnits: totalWorkUnits,
                executionCount: executionCount,
                currentSliceAttempt: currentSliceAttempt,
                unsuccessfulOutcomeCommitAttempt:
                    unsuccessfulOutcomeCommitAttempt + 1,
                pendingUnsuccessfulOutcome: pendingUnsuccessfulOutcome,
                lastUnsuccessfulOutcomeCommitError:
                    lastUnsuccessfulOutcomeCommitError,
                cancellationRequested: false,
                nextAttemptAt: nil,
                leaseOwner: owner,
                leaseToken: token,
                leaseExpiresAt: expiresAt,
                resultDigest: nil,
                failure: nil,
                updatedAt: updatedAt
            )
        case .succeeded, .failed, .cancelled:
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
    }

    package func continuing(
        operationStatePayload: ByteString,
        cumulativeWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        nextAttemptAt: Timestamp,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .pending,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: cumulativeWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: 0,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func succeeding(
        cumulativeWorkUnits: UInt64,
        totalWorkUnits: UInt64,
        resultDigest: JobResultDigest,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .succeeded,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: cumulativeWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: nil,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: resultDigest,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func retrying(
        at nextAttemptAt: Timestamp,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .pending,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func schedulingUnsuccessfulOutcomeCommit(
        _ outcome: DatabaseJobUnsuccessfulOutcome,
        nextAttemptAt: Timestamp,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .pending || status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .committingUnsuccessfulOutcome,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: outcome,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func schedulingCancellationOutcomeCommitAfterCheckpoint(
        operationStatePayload: ByteString,
        cumulativeWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .running,
              cancellationRequested else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .committingUnsuccessfulOutcome,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: cumulativeWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: .cancelled,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: updatedAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func schedulingUnsuccessfulOutcomeCommitRetry(
        after error: RemoteOperationError,
        at nextAttemptAt: Timestamp,
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .committingUnsuccessfulOutcome,
              pendingUnsuccessfulOutcome != nil,
              leaseOwner != nil,
              leaseToken != nil,
              leaseExpiresAt != nil else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .committingUnsuccessfulOutcome,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: unsuccessfulOutcomeCommitAttempt,
            pendingUnsuccessfulOutcome: pendingUnsuccessfulOutcome,
            lastUnsuccessfulOutcomeCommitError: error,
            cancellationRequested: false,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    package func completingUnsuccessfulOutcomeCommit(
        updatedAt: Timestamp
    ) throws -> Self {
        guard status == .committingUnsuccessfulOutcome,
              let pendingUnsuccessfulOutcome,
              unsuccessfulOutcomeCommitAttempt > 0,
              nextAttemptAt == nil,
              leaseOwner != nil,
              leaseToken != nil,
              leaseExpiresAt != nil else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        let completedStatus: JobStatusOperation.State
        let failure: RemoteOperationError?
        switch pendingUnsuccessfulOutcome {
        case .failed(let error):
            completedStatus = .failed
            failure = error
        case .cancelled:
            completedStatus = .cancelled
            failure = nil
        }
        return try advancing(
            status: completedStatus,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: unsuccessfulOutcomeCommitAttempt,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: nil,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: failure,
            updatedAt: updatedAt
        )
    }

    package func requestingCancellation(updatedAt: Timestamp) throws -> Self {
        guard status == .running, !cancellationRequested else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: status,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: unsuccessfulOutcomeCommitAttempt,
            pendingUnsuccessfulOutcome: pendingUnsuccessfulOutcome,
            lastUnsuccessfulOutcomeCommitError: lastUnsuccessfulOutcomeCommitError,
            cancellationRequested: true,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: leaseOwner,
            leaseToken: leaseToken,
            leaseExpiresAt: leaseExpiresAt,
            resultDigest: resultDigest,
            failure: failure,
            updatedAt: updatedAt
        )
    }

    package func isCancellationRequest(after leasedState: Self) -> Bool {
        let nextRevision = leasedState.revision.addingReportingOverflow(1)
        guard !nextRevision.overflow else { return false }
        return revision == nextRevision.partialValue
            && jobID == leasedState.jobID
            && specificationDigest == leasedState.specificationDigest
            && status == .running
            && leasedState.status == .running
            && operationStatePayload == leasedState.operationStatePayload
            && completedWorkUnits == leasedState.completedWorkUnits
            && totalWorkUnits == leasedState.totalWorkUnits
            && executionCount == leasedState.executionCount
            && currentSliceAttempt == leasedState.currentSliceAttempt
            && unsuccessfulOutcomeCommitAttempt
                == leasedState.unsuccessfulOutcomeCommitAttempt
            && pendingUnsuccessfulOutcome == leasedState.pendingUnsuccessfulOutcome
            && lastUnsuccessfulOutcomeCommitError
                == leasedState.lastUnsuccessfulOutcomeCommitError
            && cancellationRequested
            && !leasedState.cancellationRequested
            && nextAttemptAt == leasedState.nextAttemptAt
            && leaseOwner == leasedState.leaseOwner
            && leaseToken == leasedState.leaseToken
            && leaseExpiresAt == leasedState.leaseExpiresAt
            && resultDigest == leasedState.resultDigest
            && failure == leasedState.failure
            && updatedAt >= leasedState.updatedAt
    }

    package func validate() throws {
        let accountedLeaseCount = executionCount.addingReportingOverflow(
            unsuccessfulOutcomeCommitAttempt
        )
        guard specificationDigest.count == DatabaseRequestDigest.byteCount,
              executionCount >= UInt64(currentSliceAttempt),
              !accountedLeaseCount.overflow,
              accountedLeaseCount.partialValue <= revision,
              totalWorkUnits.map({ $0 >= completedWorkUnits }) ?? true,
              nextAttemptAt.map({ $0 >= updatedAt }) ?? true else {
            throw DatabaseJobRuntimeError.corruptedState
        }
        switch status {
        case .pending:
            guard nextAttemptAt != nil,
                  leaseOwner == nil,
                  leaseToken == nil,
                  leaseExpiresAt == nil,
                  resultDigest == nil,
                  failure == nil,
                  unsuccessfulOutcomeCommitAttempt == 0,
                  pendingUnsuccessfulOutcome == nil,
                  lastUnsuccessfulOutcomeCommitError == nil,
                  !cancellationRequested else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .running:
            guard currentSliceAttempt > 0,
                  nextAttemptAt == nil,
                  leaseOwner != nil,
                  leaseToken != nil,
                  leaseExpiresAt != nil,
                  resultDigest == nil,
                  failure == nil,
                  unsuccessfulOutcomeCommitAttempt == 0,
                  pendingUnsuccessfulOutcome == nil,
                  lastUnsuccessfulOutcomeCommitError == nil else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .committingUnsuccessfulOutcome:
            let hasLease = leaseOwner != nil
                && leaseToken != nil
                && leaseExpiresAt != nil
            let isScheduled = leaseOwner == nil
                && leaseToken == nil
                && leaseExpiresAt == nil
                && nextAttemptAt != nil
            let hasConsistentScheduledDiagnostic = !isScheduled
                || (unsuccessfulOutcomeCommitAttempt == 0)
                    == (lastUnsuccessfulOutcomeCommitError == nil)
            guard pendingUnsuccessfulOutcome != nil,
                  resultDigest == nil,
                  failure == nil,
                  !cancellationRequested,
                  (hasLease || isScheduled),
                  !hasLease || nextAttemptAt == nil,
                  !hasLease || unsuccessfulOutcomeCommitAttempt > 0,
                  hasConsistentScheduledDiagnostic else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .succeeded:
            guard nextAttemptAt == nil,
                  leaseOwner == nil,
                  leaseToken == nil,
                  leaseExpiresAt == nil,
                  resultDigest != nil,
                  failure == nil,
                  unsuccessfulOutcomeCommitAttempt == 0,
                  pendingUnsuccessfulOutcome == nil,
                  lastUnsuccessfulOutcomeCommitError == nil,
                  !cancellationRequested else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .failed:
            guard nextAttemptAt == nil,
                  leaseOwner == nil,
                  leaseToken == nil,
                  leaseExpiresAt == nil,
                  resultDigest == nil,
                  failure != nil,
                  unsuccessfulOutcomeCommitAttempt > 0,
                  pendingUnsuccessfulOutcome == nil,
                  lastUnsuccessfulOutcomeCommitError == nil,
                  !cancellationRequested else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .cancelled:
            guard nextAttemptAt == nil,
                  leaseOwner == nil,
                  leaseToken == nil,
                  leaseExpiresAt == nil,
                  resultDigest == nil,
                  failure == nil,
                  unsuccessfulOutcomeCommitAttempt > 0,
                  pendingUnsuccessfulOutcome == nil,
                  lastUnsuccessfulOutcomeCommitError == nil,
                  !cancellationRequested else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        }
    }

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeUInt8(Self.formatVersion)
        try jobID.encode(into: &writer)
        try writer.writeBytes(specificationDigest)
        writer.writeUInt64(revision)
        writer.writeUInt8(status.rawValue)
        try writer.writeBytes(operationStatePayload)
        writer.writeUInt64(completedWorkUnits)
        writer.writeBool(totalWorkUnits != nil)
        if let totalWorkUnits { writer.writeUInt64(totalWorkUnits) }
        writer.writeUInt64(executionCount)
        writer.writeUInt32(currentSliceAttempt)
        writer.writeUInt64(unsuccessfulOutcomeCommitAttempt)
        writer.writeBool(pendingUnsuccessfulOutcome != nil)
        if let pendingUnsuccessfulOutcome {
            try Self.encode(
                pendingUnsuccessfulOutcome,
                into: &writer
            )
        }
        writer.writeBool(lastUnsuccessfulOutcomeCommitError != nil)
        if let lastUnsuccessfulOutcomeCommitError {
            try Self.encode(
                lastUnsuccessfulOutcomeCommitError,
                into: &writer
            )
        }
        writer.writeBool(cancellationRequested)
        try Self.encode(nextAttemptAt, into: &writer)
        try Self.encode(leaseOwner, into: &writer)
        try Self.encode(leaseToken, into: &writer)
        try Self.encode(leaseExpiresAt, into: &writer)
        writer.writeBool(resultDigest != nil)
        if let resultDigest {
            try resultDigest.encode(into: &writer)
        }
        writer.writeBool(failure != nil)
        if let failure { try Self.encode(failure, into: &writer) }
        try updatedAt.encode(into: &writer)
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        let jobID = try DatabaseTypes.UUID(from: &reader)
        let specificationDigest = try reader.readBytes()
        let revision = try reader.readUInt64()
        let rawStatus = try reader.readUInt8()
        guard let status = JobStatusOperation.State(rawValue: rawStatus) else {
            throw .invalidValueTag(rawStatus)
        }
        let operationStatePayload = try reader.readBytes()
        let completedWorkUnits = try reader.readUInt64()
        let totalWorkUnits = try reader.readBool()
            ? try reader.readUInt64()
            : nil
        let executionCount = try reader.readUInt64()
        let currentSliceAttempt = try reader.readUInt32()
        let unsuccessfulOutcomeCommitAttempt = try reader.readUInt64()
        let pendingUnsuccessfulOutcome = try reader.readBool()
            ? try Self.decodeUnsuccessfulOutcome(from: &reader)
            : nil
        let lastUnsuccessfulOutcomeCommitError = try reader.readBool()
            ? try Self.decodeRemoteError(from: &reader)
            : nil
        let cancellationRequested = try reader.readBool()
        let nextAttemptAt = try Self.decodeTimestamp(from: &reader)
        let leaseOwner = try Self.decodeUUID(from: &reader)
        let leaseToken = try Self.decodeUUID(from: &reader)
        let leaseExpiresAt = try Self.decodeTimestamp(from: &reader)
        let resultDigest = try reader.readBool()
            ? try JobResultDigest(from: &reader)
            : nil
        let failure = try reader.readBool()
            ? try Self.decodeRemoteError(from: &reader)
            : nil
        let updatedAt = try Timestamp(from: &reader)
        self.init(
            jobID: jobID,
            specificationDigest: specificationDigest,
            revision: revision,
            status: status,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: unsuccessfulOutcomeCommitAttempt,
            pendingUnsuccessfulOutcome: pendingUnsuccessfulOutcome,
            lastUnsuccessfulOutcomeCommitError: lastUnsuccessfulOutcomeCommitError,
            cancellationRequested: cancellationRequested,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: leaseOwner,
            leaseToken: leaseToken,
            leaseExpiresAt: leaseExpiresAt,
            resultDigest: resultDigest,
            failure: failure,
            updatedAt: updatedAt
        )
    }

    package init(
        jobID: DatabaseTypes.UUID,
        specificationDigest: ByteString,
        revision: UInt64,
        status: JobStatusOperation.State,
        operationStatePayload: ByteString,
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        executionCount: UInt64,
        currentSliceAttempt: UInt32,
        unsuccessfulOutcomeCommitAttempt: UInt64,
        pendingUnsuccessfulOutcome: DatabaseJobUnsuccessfulOutcome?,
        lastUnsuccessfulOutcomeCommitError: RemoteOperationError?,
        cancellationRequested: Bool,
        nextAttemptAt: Timestamp?,
        leaseOwner: DatabaseTypes.UUID?,
        leaseToken: DatabaseTypes.UUID?,
        leaseExpiresAt: Timestamp?,
        resultDigest: JobResultDigest?,
        failure: RemoteOperationError?,
        updatedAt: Timestamp
    ) {
        self.jobID = jobID
        self.specificationDigest = specificationDigest
        self.revision = revision
        self.status = status
        self.operationStatePayload = operationStatePayload
        self.completedWorkUnits = completedWorkUnits
        self.totalWorkUnits = totalWorkUnits
        self.executionCount = executionCount
        self.currentSliceAttempt = currentSliceAttempt
        self.unsuccessfulOutcomeCommitAttempt = unsuccessfulOutcomeCommitAttempt
        self.pendingUnsuccessfulOutcome = pendingUnsuccessfulOutcome
        self.lastUnsuccessfulOutcomeCommitError = lastUnsuccessfulOutcomeCommitError
        self.cancellationRequested = cancellationRequested
        self.nextAttemptAt = nextAttemptAt
        self.leaseOwner = leaseOwner
        self.leaseToken = leaseToken
        self.leaseExpiresAt = leaseExpiresAt
        self.resultDigest = resultDigest
        self.failure = failure
        self.updatedAt = updatedAt
    }

    private func advancing(
        status: JobStatusOperation.State,
        operationStatePayload: ByteString,
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        executionCount: UInt64,
        currentSliceAttempt: UInt32,
        unsuccessfulOutcomeCommitAttempt: UInt64,
        pendingUnsuccessfulOutcome: DatabaseJobUnsuccessfulOutcome?,
        lastUnsuccessfulOutcomeCommitError: RemoteOperationError?,
        cancellationRequested: Bool,
        nextAttemptAt: Timestamp?,
        leaseOwner: DatabaseTypes.UUID?,
        leaseToken: DatabaseTypes.UUID?,
        leaseExpiresAt: Timestamp?,
        resultDigest: JobResultDigest?,
        failure: RemoteOperationError?,
        updatedAt: Timestamp
    ) throws -> Self {
        let (nextRevision, overflow) = revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseJobRuntimeError.stateRevisionOverflow
        }
        guard updatedAt >= self.updatedAt,
              nextAttemptAt.map({ $0 >= updatedAt }) ?? true else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return Self(
            jobID: jobID,
            specificationDigest: specificationDigest,
            revision: nextRevision,
            status: status,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            unsuccessfulOutcomeCommitAttempt: unsuccessfulOutcomeCommitAttempt,
            pendingUnsuccessfulOutcome: pendingUnsuccessfulOutcome,
            lastUnsuccessfulOutcomeCommitError: lastUnsuccessfulOutcomeCommitError,
            cancellationRequested: cancellationRequested,
            nextAttemptAt: nextAttemptAt,
            leaseOwner: leaseOwner,
            leaseToken: leaseToken,
            leaseExpiresAt: leaseExpiresAt,
            resultDigest: resultDigest,
            failure: failure,
            updatedAt: updatedAt
        )
    }

    private static func encode(
        _ value: DatabaseJobUnsuccessfulOutcome,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        let storedValue: FieldValue
        do {
            storedValue = try value.persistentJobValue()
        } catch {
            throw .invalidFieldValueWireState
        }
        try storedValue.encode(into: &writer)
    }

    private static func encode(
        _ value: RemoteOperationError,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try encode(.failed(value), into: &writer)
    }

    private static func decodeUnsuccessfulOutcome(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> DatabaseJobUnsuccessfulOutcome {
        let storedValue = try FieldValue(from: &reader)
        do {
            return try DatabaseJobUnsuccessfulOutcome(
                persistentJobValue: storedValue
            )
        } catch {
            throw .invalidFieldValueWireState
        }
    }

    private static func decodeRemoteError(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> RemoteOperationError {
        let outcome = try decodeUnsuccessfulOutcome(from: &reader)
        guard case .failed(let error) = outcome else {
            throw .invalidFieldValueWireState
        }
        return error
    }

    private static func encode(
        _ value: Timestamp?,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeBool(value != nil)
        if let value { try value.encode(into: &writer) }
    }

    private static func encode(
        _ value: DatabaseTypes.UUID?,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeBool(value != nil)
        if let value { try value.encode(into: &writer) }
    }

    private static func decodeTimestamp(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> Timestamp? {
        try reader.readBool() ? try Timestamp(from: &reader) : nil
    }

    private static func decodeUUID(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> DatabaseTypes.UUID? {
        try reader.readBool() ? try DatabaseTypes.UUID(from: &reader) : nil
    }
}
