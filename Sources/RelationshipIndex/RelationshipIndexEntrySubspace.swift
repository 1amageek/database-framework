// RelationshipIndexEntrySubspace.swift
// RelationshipIndex - Internal metadata key-space for relationship index entries

import StorageKit

internal enum RelationshipIndexEntrySubspace {
    private static let marker: Bytes = [0xFF, 0x52, 0x49, 0x45]

    static func make(from indexSubspace: Subspace) -> Subspace {
        Subspace(prefix: indexSubspace.prefix + marker)
    }
}
