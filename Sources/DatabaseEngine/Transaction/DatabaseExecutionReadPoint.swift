import DatabaseTypes

/// Opaque identity of the storage snapshot used by one admitted database
/// read. It carries no transaction or engine authority.
@_spi(DatabaseExecution)
public struct DatabaseExecutionReadPoint: Sendable, Hashable {
    public enum Position: Sendable, Hashable {
        case version(UInt64)
        case opaque(ByteString)
    }

    public let domainIdentifier: String
    public let position: Position

    public init(
        domainIdentifier: String,
        position: Position
    ) {
        self.domainIdentifier = domainIdentifier
        self.position = position
    }
}
