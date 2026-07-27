import DatabaseTypes

/// Type-erased identifier generator used by long-lived database runtimes.
public final class AnyDatabaseUUIDGenerator: DatabaseUUIDGenerator, Sendable {
    private let generateIdentifier: @Sendable () -> DatabaseTypes.UUID

    public init<Generator: DatabaseUUIDGenerator>(_ generator: Generator) {
        self.generateIdentifier = {
            generator.generate()
        }
    }

    public func generate() -> DatabaseTypes.UUID {
        generateIdentifier()
    }
}
