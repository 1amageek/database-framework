import DatabaseKit

/// Index definition
///
/// Defines a secondary index on entity fields. Indexes are maintained automatically
/// when entities are inserted, updated, or deleted.
///
public struct Index: Sendable {
    // MARK: - Properties

    /// Unique index name
    public let name: String

    /// Index kind (metadata only - no execution logic)
    public let kind: IndexKindMetadata

    /// Root expression defining indexed fields (string-based, for serialization compatibility)
    public let rootExpression: KeyExpression

    /// Subspace key (defaults to index name)
    public let subspaceKey: String

    /// Item types this index applies to (nil = universal, applies to all types)
    ///
    /// Compatible with Persistable types across entity, graph, and document models.
    public let itemTypes: Set<String>?

    /// Whether this index enforces uniqueness constraint
    ///
    /// When true, duplicate index values are not allowed. During online indexing,
    /// violations are tracked instead of immediately rejected, allowing the build
    /// to complete and violations to be reviewed afterward.
    ///
    /// **Reference**: FDB Record Layer unique index constraint
    public let isUnique: Bool

    /// Field names stored in the index entry's main value bytes
    ///
    /// When non-empty, the index maintainer writes these field values
    /// directly into the index entry value, enabling index-only scans.
    public let storedFieldNames: [String]

    // MARK: - Initialization

    public init(
        name: String,
        kind: IndexKindMetadata,
        rootExpression: KeyExpression,
        subspaceKey: String? = nil,
        itemTypes: Set<String>? = nil,
        isUnique: Bool = false,
        storedFieldNames: [String] = []
    ) {
        self.name = name
        self.kind = kind
        self.rootExpression = rootExpression
        self.subspaceKey = subspaceKey ?? name
        self.itemTypes = itemTypes
        self.isUnique = isUnique
        self.storedFieldNames = storedFieldNames
    }

    public init<Kind: IndexKind>(
        name: String,
        kind: Kind,
        rootExpression: KeyExpression,
        subspaceKey: String? = nil,
        itemTypes: Set<String>? = nil,
        isUnique: Bool = false,
        storedFieldNames: [String] = []
    ) {
        self.name = name
        self.kind = IndexKindMetadata(kind)
        self.rootExpression = rootExpression
        self.subspaceKey = subspaceKey ?? name
        self.itemTypes = itemTypes
        self.isUnique = isUnique
        self.storedFieldNames = storedFieldNames
    }
}
