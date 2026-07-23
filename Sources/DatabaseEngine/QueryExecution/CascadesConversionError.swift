public enum CascadesConversionError: Error, Sendable, Equatable,
    CustomStringConvertible
{
    case likePatternRequiresString(field: String)
    case ilikePatternRequiresString(field: String)

    public var description: String {
        switch self {
        case .likePatternRequiresString(let field):
            return "LIKE pattern for field '\(field)' must be a string"
        case .ilikePatternRequiresString(let field):
            return "ILIKE pattern for field '\(field)' must be a string"
        }
    }
}
