/// Failures that prevent a datatype or data-range membership decision.
public enum XSDValidationFailure: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidLexicalForm(
        lexicalForm: String,
        datatype: String,
        diagnostic: XSDDiagnostic
    )
    case unsupportedDatatype(String)
    case invalidRestriction(XSDDiagnostic)
    case resourceLimitExceeded(
        resource: String,
        limit: Int,
        actual: Int
    )

    public var description: String {
        switch self {
        case .invalidLexicalForm(let lexicalForm, let datatype, let diagnostic):
            return "Invalid lexical form '\(lexicalForm)' for \(datatype): \(diagnostic)"
        case .unsupportedDatatype(let datatype):
            return "Unsupported datatype: \(datatype)"
        case .invalidRestriction(let diagnostic):
            return "Invalid datatype restriction: \(diagnostic)"
        case .resourceLimitExceeded(let resource, let limit, let actual):
            return "Resource limit exceeded for \(resource): \(actual) > \(limit)"
        }
    }
}
