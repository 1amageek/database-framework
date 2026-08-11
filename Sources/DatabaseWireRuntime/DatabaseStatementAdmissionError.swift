enum DatabaseStatementAdmissionError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case featureUnavailable(String)

    var description: String {
        switch self {
        case .featureUnavailable(let reason):
            return "Statement feature is unavailable: \(reason)"
        }
    }
}
