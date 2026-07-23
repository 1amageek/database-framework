import DatabaseWire

public protocol DatabaseCommand: Sendable {
    associatedtype Descriptor: DatabaseCommandDescriptor

    var identifier: String { get }
}

public extension DatabaseCommand {
    var identifier: String {
        Descriptor.identifier
    }
}
