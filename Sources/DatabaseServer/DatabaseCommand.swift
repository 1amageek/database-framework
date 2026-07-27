import DatabaseKit

public protocol DatabaseCommand: Sendable {
    var declaration: CommandDeclaration { get }
}

public extension DatabaseCommand {
    var identifier: CommandIdentifier {
        declaration.identifier
    }
}
