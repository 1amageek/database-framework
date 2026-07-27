import DatabaseTypes
import DatabaseKit
import StorageKit

/// A decoded physical index entry with its logical indexed values and owner.
public struct IndexPhysicalEntry: Sendable {
    public let indexedValues: [FieldValue]
    public let primaryKey: Tuple

    public init(
        indexedValues: [FieldValue],
        primaryKey: Tuple
    ) {
        self.indexedValues = indexedValues
        self.primaryKey = primaryKey
    }
}
