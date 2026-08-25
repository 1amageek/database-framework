// SPARQLFunctionError.swift
// DatabaseEngine - Errors for SPARQL() function execution


/// Errors that occur during SPARQL() function execution
public enum SPARQLFunctionError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible {
    /// Invalid arguments provided to SPARQL() function
    ///
    /// Expected format: SPARQL(TypeName, 'SPARQL query string', ['?variable'])
    /// The third parameter (variable name) is optional.
    case invalidArguments(String)

    /// Type not found in schema
    ///
    /// The first argument must be a valid type name registered in the schema.
    case typeNotFound(String)

    /// Graph index not found for type
    ///
    /// The specified type does not have a graph index defined.
    case graphIndexNotFound(String)

    /// More than one RDF dataset index is declared for the selected type.
    case ambiguousGraphIndexes(
        typeName: String,
        candidates: [String]
    )

    /// Missing variable in SPARQL result
    ///
    /// The specified variable does not exist in the binding.
    case missingVariable(String)

    /// Multiple variables not supported
    ///
    /// SPARQL() function in SQL IN predicate only supports single-variable projections.
    /// For multi-variable queries, execute SPARQL directly.
    case multipleVariablesNotSupported

    public var description: String {
        switch self {
        case .invalidArguments(let message):
            return "Invalid arguments to SPARQL() function: \(message)"
        case .typeNotFound(let typeName):
            return "Type '\(typeName)' not found in schema"
        case .graphIndexNotFound(let typeName):
            return "Graph index not found for type '\(typeName)'"
        case .ambiguousGraphIndexes(let typeName, let candidates):
            return "Multiple RDF dataset indexes found for type '\(typeName)': \(candidates.joined(separator: ", "))"
        case .missingVariable(let varName):
            return "Variable '\(varName)' not found in SPARQL result"
        case .multipleVariablesNotSupported:
            return "SPARQL() function only supports single-variable projections in SQL IN predicate. " +
                   "Query returns multiple variables. " +
                   "Use explicit variable selection: SPARQL(Type, 'SELECT ?var WHERE ...', '?var')"
        }
    }
}
