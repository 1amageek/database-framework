/// Storage isolation used by one RDF dataset read.
public enum RDFDatasetReadMode: Sendable, Equatable {
    /// A read that does not participate in transaction conflict detection.
    case snapshot

    /// A conflict-tracked read used to protect decisions made by mutations.
    case serializable

    package var usesSnapshotReads: Bool {
        switch self {
        case .snapshot:
            true
        case .serializable:
            false
        }
    }
}
