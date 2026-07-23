import DatabaseWire

public struct DatabaseOperationRegistry: Sendable {
    private let handlers: [AnyDatabaseOperationHandler]

    public init(
        handlers: [AnyDatabaseOperationHandler],
        requiredOperations: [DatabaseOperationIdentifier] =
            DatabaseOperationIdentifier.allCases
    ) throws {
        let sortedHandlers = handlers.sorted {
            $0.identifier.rawValue < $1.identifier.rawValue
        }
        if sortedHandlers.count > 1 {
            for index in 1..<sortedHandlers.count {
                guard sortedHandlers[index - 1].identifier !=
                        sortedHandlers[index].identifier else {
                    throw DatabaseOperationRegistryError.duplicate(
                        sortedHandlers[index].identifier
                    )
                }
            }
        }

        let missing = requiredOperations
            .filter { required in
                !sortedHandlers.contains { $0.identifier == required }
            }
            .sorted { $0.rawValue < $1.rawValue }
        guard missing.isEmpty else {
            throw DatabaseOperationRegistryError.missing(missing)
        }
        self.handlers = sortedHandlers
    }

    func resolve(
        _ identifier: DatabaseOperationIdentifier
    ) -> AnyDatabaseOperationHandler? {
        handlers.first { $0.identifier == identifier }
    }
}
