import DatabaseKit

public enum QueryConversionError: Error, Sendable {
    case literal(LiteralConversionError)
    case directory(DirectoryPathError)
    case negativeLimit(Int)
    case negativeOffset(Int)
}
