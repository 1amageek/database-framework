import DatabaseKit

/// A fully admitted physical generation retained before the first storage write.
package final class DatabaseSQLSchemaCandidate: Sendable {
    let sourceLease: DatabaseSchemaLease
    let lease: DatabaseSchemaLease
    let prepared: DatabasePreparedSchemaGeneration
    let reservation: DatabaseIntermediateReservation

    init(sourceLease: DatabaseSchemaLease, generation: DatabaseSchemaGeneration,
        prepared: DatabasePreparedSchemaGeneration, reservation: DatabaseIntermediateReservation
    ) {
        self.sourceLease = sourceLease
        self.prepared = prepared
        self.reservation = reservation
        self.lease = DatabaseSchemaLease(generation, token: DatabaseSchemaLeaseToken {
            withExtendedLifetime((sourceLease, reservation)) {}
        }, publicationSource: sourceLease.publicationSource ?? sourceLease)
    }
}
