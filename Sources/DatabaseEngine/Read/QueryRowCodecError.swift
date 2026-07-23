public enum QueryRowCodecError: Error, Sendable, CustomStringConvertible {
    case unknownField(type: String, field: String)
    case duplicateField(type: String, field: String)

    public var description: String {
        switch self {
        case .unknownField(let type, let field):
            return "Query row field '\(field)' is not declared by '\(type)'"
        case .duplicateField(let type, let field):
            return "Query row field '\(field)' is duplicated by '\(type)'"
        }
    }
}
