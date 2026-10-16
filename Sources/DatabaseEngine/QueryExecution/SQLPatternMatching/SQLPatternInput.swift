import DatabaseKit

struct SQLPatternInput: Sendable {
    private final class Storage: Sendable {
        let text: String
        let leases: [DatabaseQueryPayloadLease]
        let reservation: DatabaseIntermediateReservation
        init(text: String, leases: [DatabaseQueryPayloadLease], reservation: DatabaseIntermediateReservation) {
            self.text = text; self.leases = leases; self.reservation = reservation
        }
    }
    private let storage: Storage
    let bounds: Range<String.Index>
    private init(storage: Storage) {
        self.storage = storage
        bounds = storage.text.startIndex..<storage.text.endIndex
    }
    private init(storage: Storage, bounds: Range<String.Index>) {
        self.storage = storage; self.bounds = bounds
    }
    func bounded(_ range: Range<String.Index>) -> Self {
        precondition(range.lowerBound >= bounds.lowerBound && range.upperBound <= bounds.upperBound)
        return Self(storage: storage, bounds: range)
    }
    var meter: DatabaseWorkMeter { storage.reservation.workMeter }
    // Kernel-only access; returned slices always retain this owner.
    var text: String { storage.text }

    static func retaining(_ value: String, leases: [DatabaseQueryPayloadLease],
                          workMeter: DatabaseWorkMeter) throws -> Self {
        guard !leases.isEmpty else { throw SQLPatternError.meterMismatch }
        for lease in leases {
            try workMeter.consume(at: .expressionEvaluation)
            guard lease.workMeter === workMeter else { throw SQLPatternError.meterMismatch }
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self)
        let growth = try layout.growth(from: 0, toFit: leases.count)
        let bytes = try DatabaseIntermediateFootprint(bytes: layout.containerByteCount + layout.sharedOwnerByteCount)
            .adding(.init(bytes: growth.additionalByteCount))
            .adding(.init(bytes: UInt64(MemoryLayout<String>.stride)))
        let claim = try workMeter.reserveIntermediate(bytes: bytes.bytes, at: .expressionEvaluation)
        return Self(storage: Storage(text: value, leases: leases, reservation: claim))
    }
    static func admitting(_ value: String, workMeter: DatabaseWorkMeter) throws -> Self {
        try admitting(value[...], workMeter: workMeter)
    }
    static func admitting(_ value: Substring, workMeter: DatabaseWorkMeter) throws -> Self {
        var bytes: UInt64 = UInt64(MemoryLayout<String>.stride)
        for scalar in value.unicodeScalars {
            try workMeter.consume(at: .expressionEvaluation)
            bytes = try DatabaseIntermediateFootprint(bytes: bytes).adding(.init(bytes: utf8Width(scalar))).bytes
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self)
        let claim = try workMeter.reserveIntermediate(bytes: bytes + layout.containerByteCount + layout.sharedOwnerByteCount,
                                                     at: .expressionEvaluation)
        return Self(storage: Storage(text: String(value), leases: [], reservation: claim))
    }
    static func utf8Width(_ scalar: Unicode.Scalar) -> UInt64 {
        switch scalar.value { case 0...0x7f: 1; case 0...0x7ff: 2; case 0...0xffff: 3; default: 4 }
    }
}
