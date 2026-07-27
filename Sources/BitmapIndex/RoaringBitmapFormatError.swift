/// A failure to encode or decode the persisted Roaring bitmap representation.
public enum RoaringBitmapFormatError: Error, Sendable, Equatable {
    case encodedSizeOverflow
    case truncated(offset: Int, requiredByteCount: Int, availableByteCount: Int)
    case invalidSignature
    case unsupportedFormatVersion(UInt8)
    case invalidContainerCount(UInt32)
    case invalidArrayCount(UInt32)
    case invalidBitmapWordCount(Int)
    case invalidRunCount(UInt32)
    case unknownContainerType(UInt8)
    case duplicateContainer(UInt16)
    case emptyContainer(UInt16)
    case unorderedArray(container: UInt16)
    case invalidRun(container: UInt16)
    case trailingBytes(Int)
}
