import DatabaseTypes
import DatabaseKit
import StorageKit

/// Canonical container-wide inverse-reference catalog.
public enum RelationshipReferenceCatalog {
    private static let root = Subspace(
        prefix: Tuple(["_database_framework", "relationship_reference"]).pack()
    )

    public static func set(
        target: EntityReference,
        owner: EntityReference,
        descriptor: RelationshipDescriptor,
        transaction: any TransactionAccess
    ) throws {
        let ownerBytes = try RelationshipIdentityCodec.encode(owner)
        try transaction.setValue(
            ownerBytes,
            for: try entryKey(
                target: target,
                ownerBytes: ownerBytes,
                descriptor: descriptor
            )
        )
    }

    public static func clear(
        target: EntityReference,
        owner: EntityReference,
        descriptor: RelationshipDescriptor,
        transaction: any TransactionAccess
    ) throws {
        let ownerBytes = try RelationshipIdentityCodec.encode(owner)
        try transaction.clear(
            key: try entryKey(
                target: target,
                ownerBytes: ownerBytes,
                descriptor: descriptor
            )
        )
    }

    public static func referrers(
        of target: EntityReference,
        descriptor: RelationshipDescriptor,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> [EntityReference] {
        guard limit > 0 else {
            throw RelationshipReferenceError.invalidScanLimit(limit)
        }
        var identities: [EntityReference] = []
        identities.reserveCapacity(limit)
        var continuation: ByteString?
        while identities.count < limit {
            let page = try await referrerPage(
                of: target,
                descriptor: descriptor,
                continuation: continuation,
                limit: min(256, limit - identities.count),
                transaction: transaction
            )
            identities.append(contentsOf: page.identities)
            guard let next = page.continuation else {
                break
            }
            continuation = next
        }
        return identities
    }

    public static func referrerPage(
        of target: EntityReference,
        descriptor: RelationshipDescriptor,
        continuation: ByteString?,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> RelationshipReferenceIdentityPage {
        guard limit > 0, limit <= 256 else {
            throw RelationshipReferenceError.invalidScanLimit(limit)
        }
        let prefix = try referenceSubspace(
            target: target,
            descriptor: descriptor
        )
        let (begin, end) = prefix.range()
        let beginSelector: KeySelector
        if let continuation {
            guard prefix.contains(continuation) else {
                throw RelationshipReferenceError.corruptedCatalogEntry
            }
            beginSelector = .firstGreaterThan(continuation)
        } else {
            beginSelector = .firstGreaterOrEqual(begin)
        }
        let rows = try await TransactionRangeCollection.collect(using: transaction,
            from: beginSelector,
            to: .firstGreaterOrEqual(end),
            limit: limit + 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        let visibleRows = rows.prefix(limit)
        let identities = try visibleRows.map { _, value in
            try RelationshipIdentityCodec.decode(value)
        }
        return RelationshipReferenceIdentityPage(
            identities: identities,
            continuation: rows.count > limit ? visibleRows.last?.0 : nil
        )
    }

    private static func entryKey(
        target: EntityReference,
        ownerBytes: ByteString,
        descriptor: RelationshipDescriptor
    ) throws -> ByteString {
        try referenceSubspace(target: target, descriptor: descriptor)
            .pack(Tuple([ownerBytes]))
    }

    private static func referenceSubspace(
        target: EntityReference,
        descriptor: RelationshipDescriptor
    ) throws -> Subspace {
        root
            .subspace(descriptor.name)
            .subspace(try RelationshipIdentityCodec.encode(target))
    }
}
