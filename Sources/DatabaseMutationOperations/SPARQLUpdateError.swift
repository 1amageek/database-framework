import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseKit
import DatabaseTypes

public enum SPARQLUpdateError: Error, Sendable, Equatable, CustomStringConvertible {
    case unresolvedPrefixedName(prefix: String, local: String)
    case variableInGroundData(String)
    case blankNodeNotAllowed(String)
    case nonRDFBinding(variable: String, value: FieldValue)
    case invalidRDFTermRole(String)
    case effectCountOverflow

    public var description: String {
        switch self {
        case .unresolvedPrefixedName(let prefix, let local):
            return "Unresolved prefixed name '\(prefix):\(local)' in SPARQL update"
        case .variableInGroundData(let variable):
            return "Variable '\(variable)' is not allowed in ground SPARQL update data"
        case .blankNodeNotAllowed(let identifier):
            return "Blank node '_:\(identifier)' is not allowed in this SPARQL update"
        case .nonRDFBinding(let variable, let value):
            return "Variable '\(variable)' is bound to non-RDF value '\(value)'"
        case .invalidRDFTermRole(let value):
            return "RDF term '\(value)' is invalid in its SPARQL update position"
        case .effectCountOverflow:
            return "SPARQL update effect count overflowed UInt64"
        }
    }
}

#endif
