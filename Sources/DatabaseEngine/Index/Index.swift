import Core

/// Index definition
///
/// Defines a secondary index on record fields. Indexes are maintained automatically
/// when records are inserted, updated, or deleted.
///
/// **Note**: This is the FDBRuntime version which provides the core structure.
/// The full Index implementation with all features is in fdb-record-layer.
///
/// **KeyPath Optimization**:
/// Index can optionally store the original `AnyKeyPath` array for direct KeyPath-based
/// field extraction, bypassing string-based lookup. When `keyPaths` is available,
/// IndexMaintainer can use direct subscript access (`model[keyPath: kp]`) which is
/// more type-safe and performant than string-based `@dynamicMemberLookup` access.
///
/// **Sendable Safety**:
/// `@unchecked Sendable` is used because `AnyKeyPath` is actually thread-safe
/// (immutable value types referencing type metadata). Swift's concurrency checker
/// doesn't recognize this, so we explicitly mark it.
public struct Index: @unchecked Sendable {
    // MARK: - Properties

    /// Unique index name
    public let name: String

    /// Index kind (metadata only - no execution logic)
    public let kind: any IndexKind

    /// Root expression defining indexed fields (string-based, for serialization compatibility)
    public let rootExpression: KeyExpression

    /// Original KeyPaths for direct field extraction (optional optimization)
    ///
    /// When available, IndexMaintainer can use direct KeyPath subscript access
    /// instead of string-based extraction through `@dynamicMemberLookup`.
    ///
    /// **Benefits of KeyPath storage**:
    /// - Type-safe field access at compile time
    /// - Refactoring-friendly (IDE renames propagate)
    /// - Direct subscript access without string parsing
    /// - Reduced runtime overhead
    ///
    /// **Note**: `@unchecked Sendable` because `AnyKeyPath` is thread-safe
    /// (they are immutable value types referencing type metadata).
    public let keyPaths: [AnyKeyPath]?

    /// Subspace key (defaults to index name)
    public let subspaceKey: String

    /// Item types this index applies to (nil = universal, applies to all types)
    ///
    /// **Terminology**: Uses "itemTypes" (not "recordTypes") for layer-independent terminology.
    /// Compatible with Persistable types across all layers (record-layer, graph-layer, document-layer).
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

    /// Initialize an index with KeyExpression only (legacy compatibility)
    ///
    /// - Parameters:
    ///   - name: Unique index name
    ///   - kind: Index kind (any IndexKind protocol implementation)
    ///   - rootExpression: Expression defining indexed fields
    ///   - subspaceKey: Optional subspace key (defaults to name)
    ///   - itemTypes: Optional set of item type names this index applies to (nil = universal)
    ///   - isUnique: Whether this index enforces uniqueness (default: false)
    public init(
        name: String,
        kind: any IndexKind,
        rootExpression: KeyExpression,
        subspaceKey: String? = nil,
        itemTypes: Set<String>? = nil,
        isUnique: Bool = false,
        storedFieldNames: [String] = []
    ) {
        self.name = name
        self.kind = kind
        self.rootExpression = rootExpression
        self.keyPaths = nil
        self.subspaceKey = subspaceKey ?? name
        self.itemTypes = itemTypes
        self.isUnique = isUnique
        self.storedFieldNames = storedFieldNames
    }

    /// Initialize an index with both KeyExpression and KeyPaths (optimized)
    ///
    /// **Recommended**: Use this initializer when KeyPaths are available (e.g., from IndexDescriptor)
    /// to enable direct KeyPath-based field extraction.
    ///
    /// - Parameters:
    ///   - name: Unique index name
    ///   - kind: Index kind (any IndexKind protocol implementation)
    ///   - rootExpression: Expression defining indexed fields (for backward compatibility)
    ///   - keyPaths: Original KeyPaths for direct extraction
    ///   - subspaceKey: Optional subspace key (defaults to name)
    ///   - itemTypes: Optional set of item type names this index applies to (nil = universal)
    ///   - isUnique: Whether this index enforces uniqueness (default: false)
    ///   - storedFieldNames: Field names for covering index suffix subkey (default: empty)
    public init(
        name: String,
        kind: any IndexKind,
        rootExpression: KeyExpression,
        keyPaths: [AnyKeyPath],
        subspaceKey: String? = nil,
        itemTypes: Set<String>? = nil,
        isUnique: Bool = false,
        storedFieldNames: [String] = []
    ) {
        self.name = name
        self.kind = kind
        self.rootExpression = rootExpression
        self.keyPaths = keyPaths
        self.subspaceKey = subspaceKey ?? name
        self.itemTypes = itemTypes
        self.isUnique = isUnique
        self.storedFieldNames = storedFieldNames
    }
}
