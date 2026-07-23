import DatabaseValue

public protocol DatabaseWallClock: Sendable {
    func now() -> DatabaseTimestamp
}
