import DatabaseKit

/// Immutable lexical definition metadata; runtime cache identity adds its invocation.
final class CanonicalBoundCTE: Sendable {
    let identity: Int
    let definition: NamedSubquery
    let plan: CanonicalBoundSelect
    let referenceCount: Int
    let isVolatile: Bool
    let recursive: CanonicalBoundRecursiveDefinition?
    let metadataLease: DatabaseQueryPayloadLease

    static var storageByteCount: UInt64 {
        typealias InstanceLayout = (
            Int, NamedSubquery, CanonicalBoundSelect, Int, Bool,
            CanonicalBoundRecursiveDefinition?, DatabaseQueryPayloadLease
        )
        let alignment = max(MemoryLayout<Int>.alignment, MemoryLayout<InstanceLayout>.alignment)
        let header = MemoryLayout<(Int, Int)>.stride
        let alignedHeader = ((header + alignment - 1) / alignment) * alignment
        return UInt64(alignedHeader + MemoryLayout<InstanceLayout>.stride)
    }

    static func create(identity: Int, definition: NamedSubquery, plan: CanonicalBoundSelect,
        referenceCount: Int, isVolatile: Bool, recursive: CanonicalBoundRecursiveDefinition? = nil,
        workMeter: DatabaseWorkMeter) throws -> CanonicalBoundCTE {
        let claim = try workMeter.reserveIntermediate(bytes: storageByteCount, at: .bindingCandidate)
        return CanonicalBoundCTE(identity: identity, definition: definition, plan: plan,
            referenceCount: referenceCount, isVolatile: isVolatile, recursive: recursive,
            metadataLease: DatabaseQueryPayloadLease(retaining: claim))
    }

    private init(identity: Int, definition: NamedSubquery, plan: CanonicalBoundSelect,
        referenceCount: Int, isVolatile: Bool, recursive: CanonicalBoundRecursiveDefinition?,
        metadataLease: DatabaseQueryPayloadLease) {
        self.identity = identity
        self.definition = definition
        self.plan = plan
        self.referenceCount = referenceCount
        self.isVolatile = isVolatile
        self.recursive = recursive
        self.metadataLease = metadataLease
    }
}
