import DatabaseValue

public indirect enum FieldValueTupleCodecError: Error, Sendable, Equatable {
    case unsupportedElementType(String)
    case invalidEnvelopeMagic
    case unsupportedEnvelopeVersion(UInt8)
    case invalidRootTag(UInt8)
    case unknownTag(UInt8)
    case truncated
    case trailingBytes
    case invalidUTF8
    case invalidNibble(UInt8)
    case incompleteNibblePair
    case nonCanonicalVarint
    case integerOverflow
    case invalidRDFTerm(DatabaseRDFTermCodecError)
    case invalidArrayElement(index: Int, reason: FieldValueTupleCodecError)
    case maximumEncodedBytesExceeded(actual: Int, maximum: Int)
    case maximumCollectionCountExceeded(actual: Int, maximum: Int)
    case maximumDepthExceeded(actual: Int, maximum: Int)
    case maximumObjectCountExceeded(actual: Int, maximum: Int)
}
