import DatabaseValue

public protocol DatabaseUUIDGenerator: Sendable {
    func generate() -> DatabaseUUID
}
