/// Immutable candidate shared by the current attempt and its checkpoints.
final class DatabaseSchemaStagingState: Sendable {
    let lease: DatabaseSchemaLease
    let publication: DatabasePreparedSchemaPublication

    init(
        container: DBContainer,
        generation: DatabaseSchemaGeneration,
        sourceLease: DatabaseSchemaLease,
        reservation: DatabaseIntermediateReservation
    ) {
        // Candidate leases retain the published source and claim without
        // registering an unpublished identifier in the global lease counts.
        let publishedSource = sourceLease.publicationSource ?? sourceLease
        let lease = DatabaseSchemaLease(generation, token: DatabaseSchemaLeaseToken {
            withExtendedLifetime((publishedSource, reservation)) {}
        }, publicationSource: publishedSource)
        self.lease = lease
        self.publication = DatabasePreparedSchemaPublication(
            container: container, generation: generation, lease: lease)
    }
}
