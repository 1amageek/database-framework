import DatabaseTypes

/// One query-scoped scalar whose retained payload remains coupled to the
/// request meter until a destination has admitted its own copy.
package struct DatabaseQueryScopedFieldValue: Sendable {
    private enum Provenance: Sendable {
        case borrowedSource
        case ownedDerived(DatabaseIntermediateReservation)
    }

    private let value: FieldValue
    private let provenance: Provenance

    private init(
        value: FieldValue,
        provenance: Provenance
    ) {
        self.value = value
        self.provenance = provenance
    }

    package static func borrowing(_ value: FieldValue) -> Self {
        Self(value: value, provenance: .borrowedSource)
    }

    package static func retaining(
        _ value: FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> Self {
        let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: workMeter,
            stage: stage
        )
        let reservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: stage
        )
        return Self(value: value, provenance: .ownedDerived(reservation))
    }

    package static func retaining(
        _ value: FieldValue,
        reservation: DatabaseIntermediateReservation
    ) -> Self {
        Self(value: value, provenance: .ownedDerived(reservation))
    }

    /// Admits a safe maximum before invoking the producer, validates the
    /// resulting value, and shrinks the live claim to its exact footprint.
    package static func producing(
        maximumFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ makeValue: () throws -> FieldValue
    ) throws -> Self {
        let reservation = try workMeter.reserveIntermediate(
            rows: maximumFootprint.rows,
            bytes: maximumFootprint.bytes,
            at: stage
        )
        return try producing(
            maximumFootprint: maximumFootprint,
            reservation: reservation,
            stage: stage,
            makeValue
        )
    }

    package static func producing(
        maximumFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ makeValue: () async throws -> FieldValue
    ) async throws -> Self {
        let reservation = try workMeter.reserveIntermediate(
            rows: maximumFootprint.rows,
            bytes: maximumFootprint.bytes,
            at: stage
        )
        do {
            let value = try await makeValue()
            return try finishProduction(
                value,
                maximumFootprint: maximumFootprint,
                reservation: reservation,
                stage: stage
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package static func producingOptional(
        maximumFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ makeValue: () throws -> FieldValue?
    ) throws -> Self? {
        let reservation = try workMeter.reserveIntermediate(
            rows: maximumFootprint.rows,
            bytes: maximumFootprint.bytes,
            at: stage
        )
        do {
            guard let value = try makeValue() else {
                reservation.release()
                return nil
            }
            return try finishProduction(
                value,
                maximumFootprint: maximumFootprint,
                reservation: reservation,
                stage: stage
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package static func producingOptional(
        maximumFootprint: DatabaseIntermediateFootprint,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        _ makeValue: () async throws -> FieldValue?
    ) async throws -> Self? {
        let reservation = try workMeter.reserveIntermediate(
            rows: maximumFootprint.rows,
            bytes: maximumFootprint.bytes,
            at: stage
        )
        do {
            guard let value = try await makeValue() else {
                reservation.release()
                return nil
            }
            return try finishProduction(
                value,
                maximumFootprint: maximumFootprint,
                reservation: reservation,
                stage: stage
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    static func producing(
        maximumFootprint: DatabaseIntermediateFootprint,
        reservation: DatabaseIntermediateReservation,
        stage: DatabaseWorkStage,
        _ makeValue: () throws -> FieldValue
    ) throws -> Self {
        do {
            return try finishProduction(
                makeValue(),
                maximumFootprint: maximumFootprint,
                reservation: reservation,
                stage: stage
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    package func withValue<Result, Failure: Error>(
        _ body: (borrowing FieldValue) throws(Failure) -> Result
    ) throws(Failure) -> Result {
        defer { withExtendedLifetime(provenance) {} }
        return try body(value)
    }

    var retainedReservation: DatabaseIntermediateReservation? {
        guard case .ownedDerived(let reservation) = provenance else {
            return nil
        }
        return reservation
    }

    private static func finishProduction(
        _ value: FieldValue,
        maximumFootprint: DatabaseIntermediateFootprint,
        reservation: DatabaseIntermediateReservation,
        stage: DatabaseWorkStage
    ) throws -> Self {
        let actualFootprint = try CanonicalRelationalFootprintMeter
            .valueFootprint(
                of: value,
                workMeter: reservation.workMeter,
                stage: stage
            )
        guard actualFootprint.rows <= maximumFootprint.rows,
              actualFootprint.bytes <= maximumFootprint.bytes else {
            throw DatabaseQueryScopedFieldValueError
                .payloadFootprintExceeded(
                    maximumRows: maximumFootprint.rows,
                    maximumBytes: maximumFootprint.bytes,
                    actualRows: actualFootprint.rows,
                    actualBytes: actualFootprint.bytes
                )
        }
        try reservation.releasePartial(
            rows: maximumFootprint.rows - actualFootprint.rows,
            bytes: maximumFootprint.bytes - actualFootprint.bytes
        )
        return retaining(value, reservation: reservation)
    }
}
