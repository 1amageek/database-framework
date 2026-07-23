import DatabaseWire

public struct DatabaseResumableOperationRegistry: Sendable {
    private let operations: [AnyDatabaseResumableOperation]

    public var identifiers: [DatabaseJobOperationIdentifier] {
        operations.map(\.operation)
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
        _ identifier: DatabaseJobOperationIdentifier
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
}
