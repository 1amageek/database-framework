import DatabaseKit

/// Admits framework-owned canonical storage adaptation before allocation.
///
/// This path never executes application model decoding or default expressions.
/// It validates and orders primitive persisted fields using the registered
/// schema, whose exact retained output footprint is computable before the
/// canonical owner is created.
enum CanonicalStoredModelAdmission {
    static func admit(
        _ source: PersistedModel,
        runtime: EntityRuntimeRegistration,
        reservation: DatabaseIntermediateReservation,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> (model: PersistedModel, retainedByteCount: UInt64) {
        try runtime.canonicalizedStoredModel(
            source,
            reservation: reservation,
            workMeter: workMeter,
            stage: stage
        )
    }
}
