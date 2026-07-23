/// A failure that prevents a sound satisfiability decision.
public enum OntologyReasoningFailure: Error, Sendable, Equatable, CustomStringConvertible {
    case datatype(XSDValidationFailure)
    case incompleteDatatypeReasoning(XSDDiagnostic)

    public var description: String {
        switch self {
        case .datatype(let failure):
            return failure.description
        case .incompleteDatatypeReasoning(let diagnostic):
            return diagnostic.description
        }
    }
}
