/// Membership in a valid, compiled OWL data range.
public enum DataRangeMembership: Sendable, Equatable {
    case member
    case notMember(XSDDiagnostic)

    public var isMember: Bool {
        if case .member = self { return true }
        return false
    }
}
