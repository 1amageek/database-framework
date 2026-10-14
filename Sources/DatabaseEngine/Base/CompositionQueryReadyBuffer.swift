#if DATABASE_MULTI_BASE
import DatabaseKit

/// Request-bounded publication storage that cannot invoke host code while
/// the producing Composition snapshot owns live Domain transactions.
package actor CompositionQueryReadyBuffer {
    private let workMeter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private var metadata: CompositionQueryMetadata?
    private var rows: [CompositionQueryRow] = []
    private var accountedCapacity = 0

    package init(workMeter: DatabaseWorkMeter) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(CompositionQueryRow.self)
        self.layout = layout
        self.workMeter = workMeter
        self.reservation = try workMeter.reserveIntermediate(
            bytes: layout.containerByteCount + layout.sharedOwnerByteCount,
            at: .resultMaterialization
        )
    }

    package func append(_ event: CompositionQueryEvent) throws {
        try workMeter.checkpoint(at: .resultMaterialization)
        switch event {
        case .began(let value):
            guard metadata == nil, rows.isEmpty else {
                throw CompositionQueryError.workspaceCorrupted
            }
            var footprint = try DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: value.columns.capacity, element: QueryColumn.self
            ).adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: value.composition.bases.capacity, element: Base.ID.self
            ))
            footprint = try footprint.adding(DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<CompositionQueryMetadata>.stride)
            ))
            for column in value.columns {
                footprint = try footprint.adding(DatabaseIntermediateFootprint(
                    bytes: UInt64(column.name.utf8.count)
                ))
            }
            for base in value.composition.bases {
                footprint = try footprint.adding(DatabaseIntermediateFootprint(
                    bytes: UInt64(base.value.utf8.count)
                ))
            }
            // The captured placement map and consistency read points share
            // snapshot storage. Keep a conservative entry bound after close.
            footprint = try footprint.adding(DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<(Base.ID, UInt64)>.stride) + 64
            ).multiplied(by: UInt64(value.basePlacementGenerations.capacity)))
            guard case .federated(let readPoints) = value.consistency else {
                throw CompositionQueryError.workspaceCorrupted
            }
            footprint = try footprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: readPoints.capacity, element: DomainReadPoint.self
            ))
            for point in readPoints {
                footprint = try footprint.adding(DatabaseIntermediateFootprint(
                    bytes: UInt64(point.domainID.utf8.count)
                ))
                if case .opaque(let bytes) = point.position {
                    guard let retained = bytes.retainedByteCount else {
                        throw CompositionQueryError.invalidExecutionConfiguration("read point retained capacity is unknown")
                    }
                    footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(retained)))
                }
            }
            try reservation.reserveAdditional(bytes: footprint.bytes, at: .resultMaterialization)
            metadata = value
        case .row(let value):
            guard metadata != nil, let leases = value.payloadLeases,
                  !leases.isEmpty, leases.allSatisfy({ $0.workMeter === workMeter }) else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let nextCount = rows.count.addingReportingOverflow(1)
            guard !nextCount.overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(currentCapacity: accountedCapacity)
            }
            let growth = try layout.growth(from: accountedCapacity, toFit: nextCount.partialValue)
            var footprint = DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)
            footprint = try footprint.adding(DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: leases.count + 1, element: DatabaseQueryPayloadLease.self
            ))
            // Derived origins share metadata, DISTINCT workspace or JOIN
            // provenance leases. Their existing backing storage stays admitted.
            // Row values and their producer claims are shared; only this
            // destination's array/provenance storage requires a new claim.
            try reservation.reserveAdditional(bytes: footprint.bytes, at: .resultMaterialization)
            if growth.capacity != accountedCapacity {
                rows.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
                if rows.capacity > accountedCapacity {
                    let extra = try DatabaseIntermediateFootprint(
                        bytes: layout.elementCapacitySlotByteCount
                    ).multiplied(by: UInt64(rows.capacity - accountedCapacity))
                    try reservation.reserveAdditional(bytes: extra.bytes, at: .resultMaterialization)
                    accountedCapacity = rows.capacity
                }
            }
            var retainedLeases: [DatabaseQueryPayloadLease] = []
            retainedLeases.reserveCapacity(leases.count + 1)
            if retainedLeases.capacity > leases.count + 1 {
                let extra = try DatabaseIntermediateFootprint(
                    bytes: UInt64(MemoryLayout<DatabaseQueryPayloadLease>.stride)
                ).multiplied(by: UInt64(retainedLeases.capacity - leases.count - 1))
                try reservation.reserveAdditional(bytes: extra.bytes, at: .resultMaterialization)
            }
            retainedLeases.append(contentsOf: leases)
            retainedLeases.append(DatabaseQueryPayloadLease(retaining: reservation))
            rows.append(CompositionQueryRow(
                row: value.row, origin: value.origin,
                payloadLeases: retainedLeases
            ))
        }
    }

    /// Called only after withReadSnapshot has authoritatively closed every
    /// producing resource. Claims stay live across each consumer suspension.
    package func publish(
        _ emit: @Sendable (CompositionQueryEvent) async throws -> Bool
    ) async throws {
        guard let metadata else { throw CompositionQueryError.workspaceCorrupted }
        try workMeter.checkpoint(at: .resultMaterialization)
        guard try await emit(.began(metadata)) else { return }
        for row in rows {
            try workMeter.checkpoint(at: .resultMaterialization)
            guard try await emit(.row(row)) else { return }
        }
    }
}
#endif
