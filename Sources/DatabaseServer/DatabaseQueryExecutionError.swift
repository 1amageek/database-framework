public enum DatabaseQueryExecutionError: Error, Sendable, CustomStringConvertible {
    case pageLimitMustBePositive
    case solutionModifierMustBeNonNegative(name: String, value: Int)
    case continuationNotSupported(String)
    case mutationRequiresMutationOperation
    case unresolvedConstructTerm(String)
    case nonRDFBinding(String)
    case invalidRDFTermRole(String)
    case invalidRDFLiteralDatatype(String)
    case unsupportedRDFLiteral(String)
    case rdfLiteralTooLarge(requiredUTF8Count: UInt64, maximumUTF8Count: UInt64)
    case reifiedTripleRequiresTemplateContext
    case describeVariableRequiresPattern(String)
    case invalidDescribeResource(String)

    public var description: String {
        switch self {
        case .pageLimitMustBePositive:
            return "Query page limit must be greater than zero"
        case .solutionModifierMustBeNonNegative(let name, let value):
            return "Query solution modifier \(name) must be non-negative, got \(value)"
        case .continuationNotSupported(let statement):
            return "\(statement) does not accept a continuation"
        case .mutationRequiresMutationOperation:
            return "Mutation statements must be sent through mutation.execute"
        case .unresolvedConstructTerm(let name):
            return "CONSTRUCT template variable '\(name)' is not bound"
        case .nonRDFBinding(let value):
            return "Graph query binding is not a canonical RDF term: \(value)"
        case .invalidRDFTermRole(let value):
            return "RDF term cannot be used in this statement position: \(value)"
        case .invalidRDFLiteralDatatype(let datatype):
            return "RDF literal has an invalid datatype or annotation: \(datatype)"
        case .unsupportedRDFLiteral(let kind):
            return "Value cannot be represented as an RDF literal: \(kind)"
        case .rdfLiteralTooLarge(let required, let maximum):
            return "RDF literal requires \(required) UTF-8 bytes; maximum is \(maximum)"
        case .reifiedTripleRequiresTemplateContext:
            return "Reified triple syntax requires a graph pattern or CONSTRUCT template context"
        case .describeVariableRequiresPattern(let name):
            return "DESCRIBE variable '\(name)' requires a WHERE pattern"
        case .invalidDescribeResource(let resource):
            return "DESCRIBE resource '\(resource)' cannot be used as an RDF subject"
        }
    }
}
