import DatabaseValue
import DatabaseWire

struct DatabasePersistentJobState: DatabaseWireValue, Sendable, Hashable {
    private static let formatVersion: UInt8 = 1

    let jobID: DatabaseUUID
    let specificationDigest: DatabaseBytes
    let revision: UInt64
    let status: JobStatusOperation.State
    let operationStatePayload: DatabaseBytes
    let completedWorkUnits: UInt64
    let totalWorkUnits: UInt64?
    let executionCount: UInt64
    let currentSliceAttempt: UInt32
    let cancellationRequested: Bool
    let nextAttemptAt: DatabaseTimestamp?
    let leaseOwner: DatabaseUUID?
    let leaseToken: DatabaseUUID?
    let leaseExpiresAt: DatabaseTimestamp?
    let resultDigest: DatabaseJobResultDigest?
    let failure: DatabaseRemoteError?
    let updatedAt: DatabaseTimestamp

    var scheduledAt: DatabaseTimestamp? {
        switch status {
        case .pending:
            return nextAttemptAt
        case .running:
            return leaseExpiresAt
        case .succeeded, .failed, .cancelled:
            return nil
        }
    }

    func acquiringLease(
        owner: DatabaseUUID,
        token: DatabaseUUID,
        expiresAt: DatabaseTimestamp,
        updatedAt: DatabaseTimestamp
    ) throws -> Self {
        guard status == .pending || status == .running,
              executionCount < UInt64.max,
              currentSliceAttempt < UInt32.max else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .running,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount + 1,
            currentSliceAttempt: currentSliceAttempt + 1,
            cancellationRequested: cancellationRequested,
            nextAttemptAt: nil,
            leaseOwner: owner,
            leaseToken: token,
            leaseExpiresAt: expiresAt,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    func continuing(
        operationStatePayload: DatabaseBytes,
        cumulativeWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        nextAttemptAt: DatabaseTimestamp,
        updatedAt: DatabaseTimestamp
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

    func succeeding(
        cumulativeWorkUnits: UInt64,
        totalWorkUnits: UInt64,
        resultDigest: DatabaseJobResultDigest,
        updatedAt: DatabaseTimestamp
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

    func retrying(
        at nextAttemptAt: DatabaseTimestamp,
        updatedAt: DatabaseTimestamp
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

    func failing(
        _ error: DatabaseRemoteError,
        updatedAt: DatabaseTimestamp
    ) throws -> Self {
        guard status == .pending || status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .failed,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            cancellationRequested: false,
            nextAttemptAt: nil,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: error,
            updatedAt: updatedAt
        )
    }

    func requestingCancellation(updatedAt: DatabaseTimestamp) throws -> Self {
        guard status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: status,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
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

    func cancelling(updatedAt: DatabaseTimestamp) throws -> Self {
        guard status == .pending || status == .running else {
            throw DatabaseJobRuntimeError.invalidStateTransition
        }
        return try advancing(
            status: .cancelled,
            operationStatePayload: operationStatePayload,
            completedWorkUnits: completedWorkUnits,
            totalWorkUnits: totalWorkUnits,
            executionCount: executionCount,
            currentSliceAttempt: currentSliceAttempt,
            cancellationRequested: false,
            nextAttemptAt: nil,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: updatedAt
        )
    }

    func validate() throws {
        guard specificationDigest.count == DatabaseRequestDigest.byteCount,
              executionCount >= UInt64(currentSliceAttempt),
              totalWorkUnits.map({ $0 >= completedWorkUnits }) ?? true else {
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
                  failure == nil else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        case .succeeded:
            guard nextAttemptAt == nil,
                  leaseOwner == nil,
                  leaseToken == nil,
                  leaseExpiresAt == nil,
                  resultDigest != nil,
                  failure == nil,
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
                  !cancellationRequested else {
                throw DatabaseJobRuntimeError.corruptedState
            }
        }
    }

    func encode(
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
        if let failure { try failure.encode(into: &writer) }
        try updatedAt.encode(into: &writer)
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        let version = try reader.readUInt8()
        guard version == Self.formatVersion else {
            throw .unsupportedProtocolVersionValue(UInt16(version))
        }
        let jobID = try DatabaseUUID(from: &reader)
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
        let cancellationRequested = try reader.readBool()
        let nextAttemptAt = try Self.decodeTimestamp(from: &reader)
        let leaseOwner = try Self.decodeUUID(from: &reader)
        let leaseToken = try Self.decodeUUID(from: &reader)
        let leaseExpiresAt = try Self.decodeTimestamp(from: &reader)
        let resultDigest = try reader.readBool()
            ? try DatabaseJobResultDigest(from: &reader)
            : nil
        let failure = try reader.readBool()
            ? try DatabaseRemoteError(from: &reader)
            : nil
        let updatedAt = try DatabaseTimestamp(from: &reader)
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

    init(
        jobID: DatabaseUUID,
        specificationDigest: DatabaseBytes,
        revision: UInt64,
        status: JobStatusOperation.State,
        operationStatePayload: DatabaseBytes,
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        executionCount: UInt64,
        currentSliceAttempt: UInt32,
        cancellationRequested: Bool,
        nextAttemptAt: DatabaseTimestamp?,
        leaseOwner: DatabaseUUID?,
        leaseToken: DatabaseUUID?,
        leaseExpiresAt: DatabaseTimestamp?,
        resultDigest: DatabaseJobResultDigest?,
        failure: DatabaseRemoteError?,
        updatedAt: DatabaseTimestamp
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
        operationStatePayload: DatabaseBytes,
        completedWorkUnits: UInt64,
        totalWorkUnits: UInt64?,
        executionCount: UInt64,
        currentSliceAttempt: UInt32,
        cancellationRequested: Bool,
        nextAttemptAt: DatabaseTimestamp?,
        leaseOwner: DatabaseUUID?,
        leaseToken: DatabaseUUID?,
        leaseExpiresAt: DatabaseTimestamp?,
        resultDigest: DatabaseJobResultDigest?,
        failure: DatabaseRemoteError?,
        updatedAt: DatabaseTimestamp
    ) throws -> Self {
        let (nextRevision, overflow) = revision.addingReportingOverflow(1)
        guard !overflow else {
            throw DatabaseJobRuntimeError.stateRevisionOverflow
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
        _ value: DatabaseTimestamp?,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeBool(value != nil)
        if let value { try value.encode(into: &writer) }
    }

    private static func encode(
        _ value: DatabaseUUID?,
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        writer.writeBool(value != nil)
        if let value { try value.encode(into: &writer) }
    }

    private static func decodeTimestamp(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> DatabaseTimestamp? {
        try reader.readBool() ? try DatabaseTimestamp(from: &reader) : nil
    }

    private static func decodeUUID(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) -> DatabaseUUID? {
        try reader.readBool() ? try DatabaseUUID(from: &reader) : nil
    }
}
