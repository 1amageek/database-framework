import DatabaseKit
import QueryAST

/// A metadata row whose producer-owned payload remains admitted while aliased.
struct SQLInformationSchemaRow: Sendable {
    let values: [QueryValue]
    let payloadLease: DatabaseQueryPayloadLease

    private init(values: [QueryValue], reservation: DatabaseIntermediateReservation) {
        self.values = values
        self.payloadLease = DatabaseQueryPayloadLease(retaining: reservation)
    }

    /// Function-local accumulation; callers cannot supply an unverified byte count.
    struct TextBuffer {
        private var reservation: DatabaseIntermediateReservation?
        private var byteCount: UInt64 = 0

        init() {}

        mutating func serialize(workMeter: DatabaseWorkMeter,
            _ render: (SQLSerializer) throws(SQLSerializationError) -> String) throws -> String {
            try workMeter.consume(at: .bindingCandidate)
            let occupied = try DatabaseIntermediateFootprint(bytes: workMeter.retainedIntermediateBytes)
                .adding(DatabaseIntermediateFootprint(bytes: workMeter.pendingPointReadBytes)).bytes
            let maximum = workMeter.budget.maximumIntermediateBytes
            let available = occupied <= maximum ? maximum - occupied : 0
            let structural = QueryStructuralLimits.default
            let capacity = min(available, structural.maximumRetainedBytes)
            let claim = try workMeter.reserveIntermediate(bytes: capacity, at: .bindingCandidate)
            do {
                let value = try render(SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: capacity)))
                let bytes = UInt64(value.utf8.count)
                precondition(bytes <= capacity, "Canonical writer exceeded its admitted capacity")
                try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: .bindingCandidate)
                try claim.releasePartial(bytes: capacity - bytes)
                if let reservation { try reservation.absorbAll(from: claim) }
                else { reservation = claim }
                byteCount = try DatabaseIntermediateFootprint(bytes: byteCount)
                    .adding(DatabaseIntermediateFootprint(bytes: bytes)).bytes
                return value
            } catch {
                claim.release()
                if let cause = error as? SQLSerializationError,
                   case .outputLimit(let actual, _) = cause,
                   capacity < structural.maximumRetainedBytes {
                    let resource = DatabaseWorkLimitError.maximumIntermediateBytes(stage: .bindingCandidate,
                        consumed: occupied, requested: actual, maximum: maximum)
                    throw SQLExecutionFailure(underlyingError: cause, diagnostic: SQLDiagnostics.diagnostic(for: resource))
                }
                throw error
            }
        }

        mutating func finish(columnNames: [String], values: [String: QueryValue],
            workMeter: DatabaseWorkMeter) throws -> SQLInformationSchemaRow {
            let footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                count: columnNames.count, valueAt: { values[columnNames[$0]] ?? .primitive(.null) },
                annotations: [:], version: nil, workMeter: workMeter, stage: .bindingCandidate)
            precondition(footprint.bytes >= byteCount, "Row footprint omits its serialized text")
            let leaseStorage = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: 1, element: DatabaseIntermediateReservation.self)
            let remainder = try DatabaseIntermediateFootprint(rows: footprint.rows, bytes: footprint.bytes - byteCount)
                .adding(leaseStorage)
            let rowClaim = try workMeter.reserveIntermediate(rows: remainder.rows, bytes: remainder.bytes, at: .bindingCandidate)
            do {
                if let reservation { try rowClaim.absorbAll(from: reservation) }
                reservation = nil
                byteCount = 0
                return SQLInformationSchemaRow(values: columnNames.map { values[$0] ?? .primitive(.null) },
                    reservation: rowClaim)
            } catch {
                rowClaim.release()
                throw error
            }
        }
    }
}
