/// Separates an idempotent management result from an attempt's candidate change.
struct DatabaseSchemaStagingOutcome: Sendable {
    let publication: DatabaseSchemaPublicationResult
    let state: DatabaseSchemaStagingState?
}
