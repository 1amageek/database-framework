/// A malformed or absent parameter in an index-native read request.
public enum IndexReadParameterError: Error, Sendable, Equatable {
    case missing(name: String)
    case invalid(name: String, expected: String)
}
