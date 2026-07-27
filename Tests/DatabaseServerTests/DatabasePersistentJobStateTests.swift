import DatabaseKit
@testable import DatabaseServer
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import Testing

@Suite("Persistent Database Job State Tests")
struct DatabasePersistentJobStateTests {
    @Test("Initial pending state preserves its canonical wire representation")
    func initialPendingStateRoundTrip() throws {
        let timestamp = try Timestamp(secondsSinceUnixEpoch: 1_000)
        let state = DatabasePersistentJobState(
            jobID: DatabaseTypes.UUID(high: 0, low: 1),
            specificationDigest: ByteString(
                repeating: 0xa5,
                count: DatabaseRequestDigest.byteCount
            ),
            revision: 0,
            status: .pending,
            operationStatePayload: [0x01, 0x02, 0x03],
            completedWorkUnits: 0,
            totalWorkUnits: nil,
            executionCount: 0,
            currentSliceAttempt: 0,
            unsuccessfulOutcomeCommitAttempt: 0,
            pendingUnsuccessfulOutcome: nil,
            lastUnsuccessfulOutcomeCommitError: nil,
            cancellationRequested: false,
            nextAttemptAt: timestamp,
            leaseOwner: nil,
            leaseToken: nil,
            leaseExpiresAt: nil,
            resultDigest: nil,
            failure: nil,
            updatedAt: timestamp
        )

        try state.validate()
        let encoded = try ServerPayloadEncoder.encode(state)
        let decoded = try ServerPayloadDecoder.decode(
            DatabasePersistentJobState.self,
            from: encoded
        )

        #expect(decoded == state)
        try decoded.validate()
    }
}
