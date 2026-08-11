@_spi(DatabaseWireRuntime) import DatabaseWire

public struct DatabaseResumableOperationRegistry: Sendable {
    private let operations: [AnyDatabaseResumableOperation]

    public var identifiers: [JobOperationIdentifier] {
        operations.map { $0.operation }
    }

    public init(operations: [AnyDatabaseResumableOperation]) throws {
        let sorted = operations.sorted {
            $0.operation.lexicographicallyPrecedes($1.operation)
        }
        if sorted.count > 1 {
            for index in 1..<sorted.count {
                guard sorted[index - 1].operation != sorted[index].operation else {
                    throw DatabaseResumableOperationRegistryError.duplicateOperation(
                        sorted[index].operation
                    )
                }
            }
        }
        self.operations = sorted
    }

    public func resolve(
        _ identifier: JobOperationIdentifier
    ) throws -> AnyDatabaseResumableOperation {
        guard let operation = operations.first(where: {
            $0.operation == identifier
        }) else {
            throw DatabaseResumableOperationRegistryError.unsupportedOperation(
                identifier
            )
        }
        return operation
    }

    package func including(
        _ operation: AnyDatabaseResumableOperation
    ) throws -> DatabaseResumableOperationRegistry {
        try DatabaseResumableOperationRegistry(
            operations: operations + [operation]
        )
    }
}
