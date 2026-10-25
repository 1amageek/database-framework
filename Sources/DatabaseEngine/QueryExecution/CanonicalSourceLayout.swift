import DatabaseKit
import DatabaseTypes

final class CanonicalSourceLayout: Sendable {
    struct Slot: Sendable, Hashable {
        let name: String
        let scope: String?
        static func == (left: Self, right: Self) -> Bool {
            CanonicalSourceName.equal(left.scope, right.scope) && CanonicalSourceName.equal(left.name, right.name)
        }
        func hash(into hasher: inout Hasher) {
            hasher.combine(scope.map(CanonicalSourceName.init)); hasher.combine(CanonicalSourceName(name))
        }
    }

    struct BindingSlot: Sendable {
        let address: Slot
        let valueIndex: Int?
    }

    /// Identity addressing borrows the existing slot owner. Only compressed or
    /// composed frames require a separately admitted sparse mapping array.
    struct BindingSlots: RandomAccessCollection, Sendable {
        private let slots: [Slot]
        private let mapping: [BindingSlot]?

        init(slots: [Slot], mapping: [BindingSlot]?) {
            self.slots = slots
            self.mapping = mapping
        }

        var startIndex: Int { 0 }
        var endIndex: Int { mapping?.count ?? slots.count }
        var usesIdentityMapping: Bool { mapping == nil }

        subscript(index: Int) -> BindingSlot {
            if let mapping { return mapping[index] }
            return BindingSlot(address: slots[index], valueIndex: index)
        }
    }

    let slots: [Slot]
    let visibleSlots: [Int]
    let scopes: [String]
    let coalescedColumns: CanonicalSourceNames
    let reservation: DatabaseIntermediateReservation?
    let payloadLeases: [DatabaseQueryPayloadLease]
    let types: [QueryColumnType?]
    let nullability: [Bool]
    let frameSizes: [Int]
    let frameScopes: [[String]]
    let bindingSlots: BindingSlots

    init(slots: [Slot], scopes: [String] = [], coalescedColumns: CanonicalSourceNames = [], reservation: DatabaseIntermediateReservation? = nil, types: [QueryColumnType?]? = nil, nullability: [Bool]? = nil, visibleSlots: [Int]? = nil, payloadLeases: [DatabaseQueryPayloadLease] = [], frameSizes: [Int]? = nil, frameScopes: [[String]]? = nil, bindingSlots: [BindingSlot]? = nil) {
        self.bindingSlots = BindingSlots(slots: slots, mapping: bindingSlots)
        self.frameSizes = frameSizes ?? [slots.count]
        self.frameScopes = frameScopes ?? [scopes]
        precondition(self.frameScopes.count == self.frameSizes.count)
        precondition(self.frameSizes.reduce(0, +) == slots.count)
        self.slots = slots
        self.scopes = scopes
        self.coalescedColumns = coalescedColumns
        self.reservation = reservation
        self.payloadLeases = payloadLeases
        self.types = types ?? Array(repeating: nil, count: slots.count)
        self.nullability = nullability ?? Array(repeating: true, count: slots.count)
        precondition(self.types.count == slots.count && self.nullability.count == slots.count)
        self.visibleSlots = visibleSlots ?? Array(slots.indices)
        precondition(self.visibleSlots.allSatisfy(slots.indices.contains))
    }

    func sourcePresenceIndex(named name: String) -> Int? {
        var offset = 0
        for scopes in frameScopes {
            if let index = scopes.firstIndex(where: { CanonicalSourceName.equal($0, name) }) { return offset + index }
            offset += scopes.count
        }
        return nil
    }

    func matching(_ column: ColumnRef) -> [Int] {
        var offset = 0
        for (frame, count) in frameSizes.enumerated() {
            let indices = offset..<(offset + count)
            let matches: [Int]
            if let scope = column.table {
                matches = indices.filter {
                    (CanonicalSourceName.equal(slots[$0].scope, scope) && CanonicalSourceName.equal(slots[$0].name, column.column))
                        || (slots[$0].scope == nil && CanonicalSourceName.equal(slots[$0].name, column.displayName))
                }
                if !matches.isEmpty || frameScopes[frame].contains(where: { CanonicalSourceName.equal($0, scope) }) {
                    return matches
                }
            } else {
                matches = visibleSlots.filter { indices.contains($0) && CanonicalSourceName.equal(slots[$0].name, column.column) }
                if !matches.isEmpty { return matches }
            }
            offset += count
        }
        return []
    }

    func typedColumns() throws -> [QueryColumn] {
        let addresses = slots
        return try addresses.indices.map { index in
            guard let type = types[index] else {
                throw CanonicalReadError.unsupportedSelectQuery("A source column has no declared query type")
            }
            return try QueryColumn(number: UInt32(index + 1), name: addresses[index].name,
                                   type: type, isNullable: nullability[index])
        }
    }


}
