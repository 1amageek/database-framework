import DatabaseKit

public enum StorageFrameError: Error, Sendable, Equatable {
    case negativeLimit
    case frameTooLarge(actual: Int, maximum: Int)
    case stringTooLarge(actual: Int, maximum: Int)
    case byteStringTooLarge(actual: Int, maximum: Int)
    case collectionTooLarge(actual: Int, maximum: Int)
    case nestingTooDeep(actual: Int, maximum: Int)
    case byteCountOverflow
    case integerOutOfRange
    case truncated
    case trailingBytes
    case invalidBool(UInt8)
    case invalidUTF8
    case invalidMagic
    case unsupportedVersion(UInt16)
    case invalidTimestamp
    case invalidValueTag(UInt8)
    case invalidReferenceIdentifierTag(UInt8)
    case invalidValue
    case invalidRDFTermLimits(RDFTermStorageLimitsError)
    case invalidRDFTerm(RDFTermStorageError)
}
