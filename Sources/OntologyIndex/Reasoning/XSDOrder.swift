/// XSD facet ordering result. Unordered is a valid result for partial orders.
public enum XSDOrder: Sendable, Equatable {
    case less
    case equal
    case greater
    case unordered
}
