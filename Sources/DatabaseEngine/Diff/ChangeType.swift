/// The semantic transition represented by one field change.
public enum ChangeType: Sendable, Hashable {
    case added
    case removed
    case modified
    case unchanged
}
