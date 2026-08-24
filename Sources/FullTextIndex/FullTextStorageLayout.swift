import DatabaseTypes
import StorageKit

/// Canonical physical subspaces and statistic keys for one full-text index.
enum FullTextStorageLayout {
    static func terms(in root: Subspace) -> Subspace {
        root.subspace("terms")
    }

    static func documents(in root: Subspace) -> Subspace {
        root.subspace("docs")
    }

    static func statistics(in root: Subspace) -> Subspace {
        root.subspace("stats")
    }

    static func documentFrequencies(in root: Subspace) -> Subspace {
        root.subspace("df")
    }

    static func documentCountKey(in root: Subspace) -> ByteString {
        statistics(in: root).pack(Tuple("N"))
    }

    static func totalDocumentLengthKey(in root: Subspace) -> ByteString {
        statistics(in: root).pack(Tuple("totalLength"))
    }
}
