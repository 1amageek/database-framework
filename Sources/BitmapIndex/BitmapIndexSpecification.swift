import DatabaseKit

/// Validated runtime semantics for a bitmap index.
struct BitmapIndexSpecification: Sendable {
    static let identifier = IndexDefinition.bitmap.identifier

    let metadata: IndexKindMetadata

    init(
        _ metadata: IndexKindMetadata
    ) throws(IndexKindMetadataError) {
        try metadata.validateIdentity(
            identifier: Self.identifier,
            subspaceStructure: .hierarchical
        )
        try metadata.validateMetadataKeys()
        try metadata.validateFieldCount(1)
        self.metadata = metadata
    }
}
