import DatabaseKit
import DatabaseTypes

/// One decoded canonical model coupled to its request-memory reservation.
///
/// The raw model and accounting components are never returned separately.
/// Callers may authorize through a scoped borrow and then consume this owner
/// into the retained collection entry used by downstream execution.
package struct DatabaseRetainedStoredModel: ~Copyable, Sendable {
    private let model: PersistedModel
    private let retainedModelFootprint: DatabaseIntermediateFootprint
    private let queryRowFootprint: DatabaseIntermediateFootprint
    private let reservation: DatabaseIntermediateReservation

    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    package borrowing func withModel<Failure: Error>(
        _ body: (borrowing PersistedModel) throws(Failure) -> Void
    ) throws(Failure) {
        try body(model)
        withExtendedLifetime(reservation) {}
    }

    package consuming func makeEntry()
        -> DatabaseRetainedPersistedModels.Entry {
        DatabaseRetainedPersistedModels.Entry(
            model: model,
            retainedModelFootprint: retainedModelFootprint,
            queryRowFootprint: queryRowFootprint,
            reservation: reservation
        )
    }

    package static func decode(
        _ data: ByteString,
        entity: String,
        runtime: EntityRuntimeRegistration,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRetainedStoredModel {
        try DatabaseByteProcessingMeter.consume(
            byteCount: data.count,
            workMeter: workMeter,
            stage: stage
        )
        let decodedFootprint = try PersistableStorageCodec.decodedFootprint(
            data,
            expectedEntity: entity
        )
        try workMeter.checkpoint(at: stage)
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: decodedFootprint.retainedByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: decodedFootprint.transientByteCount
                )
            ).bytes,
            at: stage
        )

        try DatabaseByteProcessingMeter.consume(
            byteCount: data.count,
            passes: 2,
            workMeter: workMeter,
            stage: stage
        )
        let admitted = try admitCanonicalModel(
            data,
            expectedEntity: entity,
            runtime: runtime,
            reservation: reservation,
            transientByteCount: decodedFootprint.transientByteCount,
            workMeter: workMeter,
            stage: stage
        )
        try workMeter.checkpoint(at: stage)
        reservation.releaseGuaranteedPartial(
            bytes: decodedFootprint.retainedByteCount
        )
        return DatabaseRetainedStoredModel(
            model: admitted.model,
            retainedModelFootprint: DatabaseIntermediateFootprint(
                rows: 1,
                bytes: admitted.retainedByteCount
            ),
            queryRowFootprint: try CanonicalRelationalFootprintMeter.footprint(
                of: admitted.model,
                workMeter: workMeter
            ),
            reservation: reservation
        )
    }

    private static func admitCanonicalModel(
        _ data: ByteString,
        expectedEntity: String,
        runtime: EntityRuntimeRegistration,
        reservation: DatabaseIntermediateReservation,
        transientByteCount: UInt64,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> (model: PersistedModel, retainedByteCount: UInt64) {
        let persistedModel = try DataAccess.deserializePersistedModel(
            data,
            expectedEntity: expectedEntity
        )
        reservation.releaseGuaranteedPartial(bytes: transientByteCount)
        return try CanonicalStoredModelAdmission.admit(
            persistedModel,
            runtime: runtime,
            reservation: reservation,
            workMeter: workMeter,
            stage: stage
        )
    }
}
