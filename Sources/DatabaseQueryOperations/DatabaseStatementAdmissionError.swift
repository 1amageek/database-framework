import DatabaseOperationCore
package enum DatabaseStatementAdmissionError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case featureUnavailable(String)

    package var description: String {
        switch self {
        case .featureUnavailable(let reason):
            return "Statement feature is unavailable: \(reason)"
        }
    }
}
