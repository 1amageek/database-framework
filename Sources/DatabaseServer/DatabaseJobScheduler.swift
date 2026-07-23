import DatabaseValue

public protocol DatabaseJobScheduler: Sendable {
    func schedule(at timestamp: DatabaseTimestamp) async throws
}
