/// Failures of explicitly supplied SQL time zone rules.
public enum SQLTimeZoneError: Error, Sendable, Equatable, CustomStringConvertible {
    case unknownZone
    case rulesUnavailable
    case unsupportedData
    case malformedData
    case rangeUnavailable
    case admissionExceeded

    public var description: String {
        switch self {
        case .unknownZone: "Unknown supplied SQL time zone"
        case .rulesUnavailable: "SQL time zone rules are unavailable"
        case .unsupportedData: "Unsupported SQL time zone rules data"
        case .malformedData: "Malformed SQL time zone rules data"
        case .rangeUnavailable: "SQL time zone rules do not cover this instant"
        case .admissionExceeded: "SQL time zone rules admission limit exceeded"
        }
    }
}
