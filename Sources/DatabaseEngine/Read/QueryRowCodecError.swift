public enum QueryRowCodecError: Error, Sendable, Equatable, CustomStringConvertible {
    case unknownField(type: String, field: String)
    case duplicateField(type: String, field: String)
    case missingField(type: String, field: String)
    case invalidFieldIdentity(type: String, field: String)
    case incompatibleField(type: String, field: String)
    case duplicateColumn(String)
    case arityMismatch(expected: Int, actual: Int)
    case incompatibleValue(column: UInt32)
    case nullInRequiredColumn(column: UInt32)
    case richValueRequiresDeclaredAdapter(field: String)
    case entityMismatch(expected: String, actual: String)

    public var description: String {
        switch self {
        case .unknownField(let type, let field):
            return "Query row field '\(field)' is not declared by '\(type)'"
        case .duplicateField(let type, let field):
            return "Query row field '\(field)' is duplicated by '\(type)'"
        case .missingField(let type, let field):
            return "Query row is missing required field '\(field)' of '\(type)'"
        case .invalidFieldIdentity(let type, let field):
            return "Query row field '\(field)' has invalid identity in '\(type)'"
        case .incompatibleField(let type, let field):
            return "Query row field '\(field)' is incompatible with '\(type)'"
        case .duplicateColumn(let name):
            return "Query row column '\(name)' cannot be projected to a unique name"
        case .arityMismatch(let expected, let actual):
            return "Query row has \(actual) cells; expected \(expected)"
        case .incompatibleValue(let column):
            return "Query row cell at column \(column) has an incompatible type"
        case .nullInRequiredColumn(let column):
            return "Query row cell at nonnullable column \(column) is null"
        case .richValueRequiresDeclaredAdapter(let field):
            return "Query row field '\(field)' requires a declared rich-value adapter"
        case .entityMismatch(let expected, let actual):
            return "Query row entity '\(actual)' does not match '\(expected)'"
        }
    }
}
