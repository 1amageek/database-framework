/// Distinguishes the SPARQL implicit aggregate group from an explicit
/// `GROUP BY` clause. Their empty-input semantics are intentionally different.
public enum SPARQLGroupingPlan: Sendable, Hashable {
    case implicitSingleGroup
    case explicit([SPARQLGroupKeyPlan])

    public var keys: [SPARQLGroupKeyPlan] {
        switch self {
        case .implicitSingleGroup:
            return []
        case .explicit(let keys):
            return keys
        }
    }

    public var createsGroupForEmptyInput: Bool {
        switch self {
        case .implicitSingleGroup:
            return true
        case .explicit:
            return false
        }
    }
}
