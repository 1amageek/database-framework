import DatabaseTypes

public protocol DatabaseUUIDGenerator: Sendable {
    func generate() -> DatabaseTypes.UUID
}
