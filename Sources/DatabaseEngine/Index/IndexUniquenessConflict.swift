import DatabaseTypes
import StorageKit

/// One physical conflict reported by a uniqueness-capable index maintainer.
public struct IndexUniquenessConflict: Sendable {
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
