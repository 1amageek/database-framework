public enum DatabaseFormatDescriptorError: Error, Sendable, Equatable {
    case invalidSize(actual: Int, expected: Int)
    case invalidMagic
    case unsupportedDescriptorVersion(UInt8)
    case unsupportedRecordFormatVersion(UInt16)
    case unsupportedEnvelopeVersion(UInt8)
    case unsupportedPayloadEncoding(UInt8)
    case checksumMismatch(expected: UInt32, actual: UInt32)
    case integerOutOfRange
    case invalidStorageConfiguration(ItemStorageConfigurationError)
}
