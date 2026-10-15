import DatabaseKit

/// A failure to preserve a semantic value in executable SQL text.
public enum SQLSerializationError: Error, Sendable, Equatable {
    case unsupportedLanguage(String)
    case unsupportedSource(String)
    case unsupportedMetadata(String)
    case unsupportedType(String)
    case unsupportedLiteral(String)
    case invalidStructure(String)
    case outputLimit(actual: UInt64, maximum: UInt64)
    case structuralLimit(String)
    case constant(String)
}
