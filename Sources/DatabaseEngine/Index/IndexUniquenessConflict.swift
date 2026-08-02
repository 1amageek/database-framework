import DatabaseTypes
import StorageKit

/// One physical conflict reported by a uniqueness-capable index maintainer.
public struct IndexUniquenessConflict: Sendable {
    /// Tuple-packed conflicting values relative to the index subspace.
    public let valueKey: ByteString
    public let conflictingValues: [FieldValue]
    public let existingPrimaryKey: Tuple

    public init(
        valueKey: ByteString,
        conflictingValues: [FieldValue],
        existingPrimaryKey: Tuple
    ) {
        self.valueKey = valueKey
        self.conflictingValues = conflictingValues
        self.existingPrimaryKey = existingPrimaryKey
    }
}
