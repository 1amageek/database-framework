import DatabaseTypes

public protocol DatabaseWallClock: Sendable {
    func now() -> Timestamp
}
