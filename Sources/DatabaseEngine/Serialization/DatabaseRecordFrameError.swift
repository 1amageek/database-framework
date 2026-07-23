/// Typed failures shared by canonical record and index-projection frames.
public enum DatabaseRecordFrameError: Error, Sendable, Equatable {
    case missingCompiledSchema(entity: String)
    case invalidMagicLength(actual: Int)
    case invalidMagic
    case unsupportedVersion(UInt16)
    case entityMismatch(expected: String, actual: String)
    case frameTooLarge(actual: Int, maximum: Int)
    case duplicateFieldName(String)
    case duplicateFieldNumber(UInt32)
}
