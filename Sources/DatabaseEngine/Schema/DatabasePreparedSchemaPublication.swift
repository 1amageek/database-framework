/// Fully validated publication retained across the authoritative outer commit.
package final class DatabasePreparedSchemaPublication: Sendable {
    private let container: DBContainer
    private let generation: DatabaseSchemaGeneration
    private let lease: DatabaseSchemaLease

    init(container: DBContainer, generation: DatabaseSchemaGeneration,
        lease: DatabaseSchemaLease
    ) {
        self.container = container
        self.generation = generation
        self.lease = lease
    }

    /// The lifecycle owner calls this only after authoritative commit success.
    package func publish() {
        withExtendedLifetime(lease) {
            container.publishPreparedSchemaGeneration(generation)
        }
    }
}
