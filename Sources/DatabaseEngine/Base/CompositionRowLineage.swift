#if DATABASE_MULTI_BASE
import DatabaseKit
import DatabaseTypes

/// Retains request-bounded lineage while the canonical SQL owner transforms rows.
final class CompositionRowLineage: Sendable {
    private static let annotation = "__composition_member_lineage"
    private let members: [DatabaseCompositionMember]
    private let byteCount: Int
    private let workMeter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation

    init(members: [DatabaseCompositionMember], workMeter: DatabaseWorkMeter) throws {
        guard !members.isEmpty else { throw CompositionQueryError.workspaceCorrupted }
        self.byteCount = members.count / 8 + (members.count % 8 == 0 ? 0 : 1)
        self.workMeter = workMeter
        self.reservation = try workMeter.reserveIntermediate(bytes:
            DatabaseIntermediateCollectionMeter.arrayFootprint(count: members.count,
                element: DatabaseCompositionMember.self).bytes, at: .resultMaterialization)
        self.members = members
    }

    var payloadLease: DatabaseQueryPayloadLease { DatabaseQueryPayloadLease(retaining: reservation) }

    func member(for annotations: [String: FieldValue]) throws -> DatabaseCompositionMember {
        guard case .bytes(let bits)? = annotations[Self.annotation], bits.count == byteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }
        var selected: Int?
        for index in members.indices {
            try workMeter.consume(at: .expressionEvaluation)
            if bits[index / 8] & (UInt8(1) << UInt8(index % 8)) != 0 {
                guard selected == nil else { throw CompositionQueryError.workspaceCorrupted }
                selected = index
            }
        }
        guard let selected else { throw CompositionQueryError.workspaceCorrupted }
        return members[selected]
    }

    func annotatedFootprint(of row: CanonicalSourceRow) throws -> DatabaseIntermediateFootprint {
        guard row.annotations[Self.annotation] == nil else { throw CompositionQueryError.workspaceCorrupted }
        return try CanonicalRelationalFootprintMeter.sourceAliasFootprint(of: row,
            workMeter: workMeter, stage: .resultMaterialization).adding(annotationFootprint())
    }

    private func annotationFootprint() throws -> DatabaseIntermediateFootprint {
        try CanonicalRelationalFootprintMeter.fieldEntryFootprint(
            nameUTF8Count: Self.annotation.utf8.count, value: .bytes([]),
            workMeter: workMeter, stage: .resultMaterialization)
            .adding(DatabaseIntermediateFootprint(bytes: UInt64(byteCount)))
    }

    /// The caller admits annotatedFootprint before this destination is allocated.
    func annotate(_ row: CanonicalSourceRow, memberIndex: Int,
        retaining lease: DatabaseQueryPayloadLease) throws -> CanonicalSourceRow {
        row.replacingAnnotations(try annotate(row.annotations, memberIndex: memberIndex), retaining: lease)
    }

    private func annotate(_ existing: [String: FieldValue], memberIndex: Int) throws -> [String: FieldValue] {
        guard members.indices.contains(memberIndex), existing[Self.annotation] == nil else {
            throw CompositionQueryError.workspaceCorrupted
        }
        try workMeter.consume(UInt64(byteCount), at: .resultMaterialization)
        let bits = ByteString.copying(count: byteCount) { destination in
            for index in 0..<byteCount { destination[index] = 0 }
            destination[memberIndex / 8] = UInt8(1) << UInt8(memberIndex % 8)
        }
        var annotations = existing
        annotations[Self.annotation] = .bytes(bits)
        return annotations
    }

    func merge(_ left: [String: FieldValue], _ right: [String: FieldValue]) throws -> [String: FieldValue] {
        guard case .bytes(let first)? = left[Self.annotation],
              case .bytes(let second)? = right[Self.annotation],
              first.count == byteCount, second.count == byteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }
        try workMeter.consume(UInt64(byteCount), at: .deduplication)
        // Retain the replaced dictionary/bit owner through canonical result transfer.
        let dictionary = try CanonicalRelationalFootprintMeter.positionalValueAliasArrayFootprint(
            count: 0, valueAt: { _ in .primitive(.null) }, annotations: left,
            version: nil, workMeter: workMeter, stage: .deduplication)
        try reservation.reserveAdditional(bytes: try dictionary.adding(
            DatabaseIntermediateFootprint(bytes: UInt64(byteCount))).bytes, at: .deduplication)
        let bits = ByteString.copying(count: byteCount) { destination in
            for index in 0..<byteCount { destination[index] = first[index] | second[index] }
        }
        var merged = left
        merged[Self.annotation] = .bytes(bits)
        return merged
    }

    func result(_ row: borrowing QueryRow, retaining leases: [DatabaseQueryPayloadLease]) throws -> CompositionQueryRow {
        guard case .bytes(let bits)? = row.annotations[Self.annotation], bits.count == byteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }
        var count = 0
        for index in members.indices {
            try workMeter.consume(2, at: .resultMaterialization)
            if bits[index / 8] & (UInt8(1) << UInt8(index % 8)) != 0 { count += 1 }
        }
        guard count > 0 else { throw CompositionQueryError.workspaceCorrupted }
        let layout = try DatabaseRetainedArrayLayout.forElement(Base.ID.self)
        let growth = try layout.growth(from: 0, toFit: count)
        let rowStorage = try CanonicalRelationalFootprintMeter.queryRowAliasFootprint(
            of: row, workMeter: workMeter, stage: .resultMaterialization)
        let leaseStorage = try DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: leases.count + 1, element: DatabaseQueryPayloadLease.self)
        try reservation.reserveAdditional(bytes: try rowStorage.adding(leaseStorage)
            .adding(DatabaseIntermediateFootprint(bytes: layout.containerByteCount))
            .adding(DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).bytes,
            at: .resultMaterialization)
        var contributors: [Base.ID] = []
        contributors.reserveCapacity(growth.capacity)
        if contributors.capacity > growth.capacity {
            try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(
                bytes: layout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(contributors.capacity - growth.capacity)).bytes,
                at: .resultMaterialization)
        }
        for index in members.indices where bits[index / 8] & (UInt8(1) << UInt8(index % 8)) != 0 {
            contributors.append(members[index].baseID)
        }
        // Snapshot member order is the canonical order fixed by its Composition lease.
        var annotations = row.annotations
        annotations.removeValue(forKey: Self.annotation)
        let output = QueryRow(validatedValues: row.values, annotations: annotations, version: row.version)
        let origin: CompositionOrigin = contributors.count == 1
            ? .source(contributors[0]) : .derived(contributors: contributors)
        var retainedLeases: [DatabaseQueryPayloadLease] = []
        retainedLeases.reserveCapacity(leases.count + 1)
        if retainedLeases.capacity > leases.count + 1 {
            try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<DatabaseQueryPayloadLease>.stride))
                .multiplied(by: UInt64(retainedLeases.capacity - leases.count - 1)).bytes,
                at: .resultMaterialization)
        }
        retainedLeases.append(contentsOf: leases)
        retainedLeases.append(payloadLease)
        return CompositionQueryRow(row: output, origin: origin, payloadLeases: retainedLeases)
    }
}
#endif
