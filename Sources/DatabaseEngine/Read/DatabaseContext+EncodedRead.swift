import DatabaseKit
import DatabaseTypes
import StorageKit

extension DatabaseContext {
    /// Retains a producer's payload owners through encoding and authoritative close.
    @_spi(DatabaseExecution)
    public func withEncodedReadSnapshot<Payload: Sendable>(
        workMeter: DatabaseWorkMeter,
        executionDeadline: TransactionExecutionDeadline,
        produce: @escaping @Sendable (DatabaseReadSnapshot) async throws -> Payload,
        measure: @escaping @Sendable (borrowing Payload) throws -> UInt64,
        encode: @escaping @Sendable (borrowing Payload, inout MutableSpan<UInt8>) throws -> Void
    ) async throws -> EncodedQueryResponse {
        guard ActiveDatabaseTransactionContext.binding == nil else {
            throw SQLBlockExecutionError.enclosingTransaction
        }
        let payload = try await withReadSnapshot(workMeter: workMeter, executionDeadline: executionDeadline) { snapshot in
            let produced = try await produce(snapshot)
            defer { withExtendedLifetime(produced) {} }
            return try EncodedQueryPayload.prepare(workMeter: workMeter,
                measure: { try measure(produced) },
                encode: { destination in try encode(produced, &destination) })
        }
        try workMeter.checkpoint(at: .resultMaterialization)
        return EncodedQueryResponse(payload)
    }
}
