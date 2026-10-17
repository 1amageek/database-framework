/// Logical restoration without transaction completion authority.
public protocol DatabaseTransactionCheckpointing: Sendable {
    func createCheckpoint() async throws -> DatabaseTransactionCheckpoint
    func rollback(to checkpoint: DatabaseTransactionCheckpoint) async throws
    func release(_ checkpoint: DatabaseTransactionCheckpoint) async throws
}
