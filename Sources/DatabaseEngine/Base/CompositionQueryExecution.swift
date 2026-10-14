#if DATABASE_MULTI_BASE
import DatabaseKit

/// Framework-owned failure contract for a read-only Composition plan.
public enum CompositionQueryError: Error, Sendable, Equatable,
    CustomStringConvertible
{
    case unsupportedPlan(String)
    case aggregateFailure(String)
    case invalidExecutionConfiguration(String)
    case workspaceCorrupted

    public var description: String {
        switch self {
        case .unsupportedPlan(let reason):
            return "Composition query plan is unsupported: \(reason)"
        case .aggregateFailure(let reason):
            return "Composition aggregate failed: \(reason)"
        case .invalidExecutionConfiguration(let reason):
            return "Composition query configuration is invalid: \(reason)"
        case .workspaceCorrupted:
            return "Composition query workspace is corrupted"
        }
    }
}

/// Request-scoped semantic options consumed by the Composition planner.
@_spi(DatabaseExecution)
public struct CompositionQueryExecutionOptions: Sendable {
    public let pageSize: Int
    public let readContext: ReadExecutionContext

    public init(
        pageSize: Int,
        readContext: ReadExecutionContext
    ) {
        self.pageSize = pageSize
        self.readContext = readContext
    }
}

/// Immutable identity and consistency metadata fixed before any row is emitted.
@_spi(DatabaseExecution)
public struct CompositionQueryMetadata: Sendable {
    public let composition: CompositionResolution
    public let basePlacementGenerations: [Base.ID: UInt64]
    public let schemaGeneration: UInt64
    public let consistency: DatabaseKit.DatabaseReadConsistency
    public let columns: [QueryColumn]

    public init(
        composition: CompositionResolution,
        basePlacementGenerations: [Base.ID: UInt64],
        schemaGeneration: UInt64,
        consistency: DatabaseKit.DatabaseReadConsistency,
        columns: [QueryColumn]
    ) {
        self.composition = composition
        self.basePlacementGenerations = basePlacementGenerations
        self.schemaGeneration = schemaGeneration
        self.consistency = consistency
        self.columns = columns
    }
}

/// One canonical row and its complete Base lineage.
@_spi(DatabaseExecution)
public struct CompositionQueryRow: Sendable {
    public let row: QueryRow
    public let origin: CompositionOrigin
    package let payloadLeases: [DatabaseQueryPayloadLease]?

    public init(row: QueryRow, origin: CompositionOrigin) {
        self.row = row
        self.origin = origin
        self.payloadLeases = nil
    }

    package init(ownedRow: DatabaseQueryScopedQueryRowOwner, origin: CompositionOrigin) {
        self.row = ownedRow.withRow { copy $0 }
        self.origin = origin
        self.payloadLeases = ownedRow.payloadLeases
    }

    package init(row: QueryRow, origin: CompositionOrigin, payloadLeases: [DatabaseQueryPayloadLease]) {
        self.row = row
        self.origin = origin
        self.payloadLeases = payloadLeases
    }

    /// Measures destination storage while this event retains its known producer
    /// claims. The consumer must keep the event alive until final output transfer.
    @_spi(DatabaseExecution)
    public func retainedTransferFootprint(
        workMeter: DatabaseWorkMeter
    ) throws -> (rows: UInt64, bytes: UInt64) {
        let footprint: DatabaseIntermediateFootprint
        if let payloadLeases {
            guard !payloadLeases.isEmpty,
                  payloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            footprint = try CanonicalRelationalFootprintMeter.positionalValueArrayFootprint(
                count: row.values.count, valueAt: { _ in .primitive(.null) },
                annotations: row.annotations, version: row.version,
                workMeter: workMeter, stage: .resultMaterialization)
        } else {
            footprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row, workMeter: workMeter, stage: .resultMaterialization)
        }
        return (rows: footprint.rows, bytes: footprint.bytes)
    }
}

/// Streaming boundary shared by local applications and remote adapters.
@_spi(DatabaseExecution)
public enum CompositionQueryEvent: Sendable {
    case began(CompositionQueryMetadata)
    case row(CompositionQueryRow)
}

#endif
