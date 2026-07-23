import DatabaseWire

public struct DatabaseResponsePreparationError:
    Error,
    Sendable,
    CustomStringConvertible {
    public let wireError: DatabaseWireError

    public init(wireError: DatabaseWireError) {
        self.wireError = wireError
    }

    public var description: String {
        "Database response exceeds its canonical wire contract: \(wireError)"
    }
}
