import DatabaseKit

/// A transient binding result; executable plans retain its values, never this owner.
final class CanonicalQueryBindingScope: Sendable {
    let named: [NamedSubquery]
    let scoped: CanonicalQueryBindingEnvironment
    let ctes: [CanonicalBoundCTE]
    private let metadataLease: DatabaseQueryPayloadLease

    private static var storageByteCount: UInt64 {
        typealias InstanceLayout = (
            [NamedSubquery], CanonicalQueryBindingEnvironment,
            [CanonicalBoundCTE], DatabaseQueryPayloadLease
        )
        let alignment = max(MemoryLayout<Int>.alignment, MemoryLayout<InstanceLayout>.alignment)
        let header = MemoryLayout<(Int, Int)>.stride
        let alignedHeader = ((header + alignment - 1) / alignment) * alignment
        return UInt64(alignedHeader + MemoryLayout<InstanceLayout>.stride)
    }

    static func create(named: [NamedSubquery], scoped: CanonicalQueryBindingEnvironment,
        ctes: [CanonicalBoundCTE], workMeter: DatabaseWorkMeter) throws -> CanonicalQueryBindingScope {
        let claim = try workMeter.reserveIntermediate(bytes: storageByteCount, at: .bindingCandidate)
        return CanonicalQueryBindingScope(named: named, scoped: scoped, ctes: ctes,
            metadataLease: DatabaseQueryPayloadLease(retaining: claim))
    }

    private init(named: [NamedSubquery], scoped: CanonicalQueryBindingEnvironment,
        ctes: [CanonicalBoundCTE], metadataLease: DatabaseQueryPayloadLease) {
        self.named = named
        self.scoped = scoped
        self.ctes = ctes
        self.metadataLease = metadataLease
    }
}
