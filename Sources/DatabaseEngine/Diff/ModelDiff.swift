#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// The ordered field transitions between two instances of one model identity.
public struct ModelDiff: Sendable, Hashable {
    public let typeName: String
    public let idString: String
    public let changes: [FieldChange]
    public let timestamp: Date
    public let oldVersion: VersionInfo?
    public let newVersion: VersionInfo?

    public init(
        typeName: String,
        idString: String,
        changes: [FieldChange],
        timestamp: Date,
        oldVersion: VersionInfo?,
        newVersion: VersionInfo?
    ) {
        self.typeName = typeName
        self.idString = idString
        self.changes = changes
        self.timestamp = timestamp
        self.oldVersion = oldVersion
        self.newVersion = newVersion
    }

    public var changedFields: [String] {
        changes.compactMap {
            $0.changeType == .unchanged ? nil : $0.fieldPath
        }
    }

    public var modifiedFields: [String] {
        changes.compactMap {
            $0.changeType == .modified ? $0.fieldPath : nil
        }
    }

    public func change(for fieldPath: String) -> FieldChange? {
        changes.first { $0.fieldPath == fieldPath }
    }
}
