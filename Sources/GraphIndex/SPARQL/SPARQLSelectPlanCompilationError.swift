public enum SPARQLSelectPlanCompilationError: Error, Sendable, Equatable {
    case namedSubqueriesUnsupported
    case accessPathUnsupported
    case unsupportedSource
    case allFromProjectionUnsupported
    case explicitDatasetInSubquery
    case negativeSolutionModifier(name: String, value: Int)
    case duplicateProjectionVariable(String)
}

extension SPARQLSelectPlanCompilationError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .namedSubqueriesUnsupported:
            return "Named subqueries are not part of SPARQL Select algebra"
        case .accessPathUnsupported:
            return "A SPARQL Select plan cannot contain a feature-specific access path"
        case .unsupportedSource:
            return "A SPARQL Select plan requires a graph-pattern source"
        case .allFromProjectionUnsupported:
            return "SPARQL Select does not support table-qualified wildcard projection"
        case .explicitDatasetInSubquery:
            return "A SPARQL SubSelect inherits its outer dataset"
        case .negativeSolutionModifier(let name, let value):
            return "SPARQL \(name) must be non-negative, received \(value)"
        case .duplicateProjectionVariable(let variable):
            return "SPARQL projection contains duplicate variable '\(variable)'"
        }
    }
}
