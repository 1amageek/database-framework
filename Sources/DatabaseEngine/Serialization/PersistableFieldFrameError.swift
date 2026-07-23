/// Typed failures shared by canonical entity and index-projection frames.
public enum PersistableFieldFrameError: Error, Sendable, Equatable {
    case missingCompiledSchema(entity: String)
    case invalidMagicLength(actual: Int)
    case invalidMagic
    case unsupportedVersion(UInt16)
    case entityMismatch(expected: String, actual: String)
    case frameTooLarge(actual: Int, maximum: Int)
    case duplicateFieldName(String)
    case duplicateFieldNumber(UInt32)
}
