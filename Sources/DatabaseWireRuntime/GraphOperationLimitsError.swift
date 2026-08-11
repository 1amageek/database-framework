#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
public enum GraphOperationLimitsError:
    Error,
    Sendable,
    Equatable,
    CustomStringConvertible
{
    case nonPositiveMaximumLoadDocumentBytes

    public var description: String {
        switch self {
        case .nonPositiveMaximumLoadDocumentBytes:
            return "The SPARQL LOAD document byte limit must be positive"
        }
    }
}
#endif
