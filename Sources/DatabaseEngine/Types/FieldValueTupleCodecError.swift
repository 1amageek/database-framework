import DatabaseTypes
import DatabaseKit

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
    case nonCanonicalDecimal
    case integerOverflow
    case unknownIdentifierTag(UInt8)
    case invalidRDFTerm(RDFTermStorageError)
    case invalidRDFTermLimits(RDFTermStorageLimitsError)
    case invalidFieldObject(FieldObjectError)
    case invalidEntityReference(EntityReferenceError)
    case invalidCivilDate(CivilDateError)
    case invalidCivilTime(CivilTimeError)
    case invalidTimestamp(TimestampError)
    case invalidTimeSpan(TimeSpanError)
    case invalidGeographicPoint(GeographicPointError)
    case invalidGeographicPosition(GeographicPositionError)
    case invalidVector(VectorError)
    case invalidArrayElement(index: Int, reason: FieldValueTupleCodecError)
    case invalidObjectField(index: Int, reason: FieldValueTupleCodecError)
    case invalidIdentifierComponent(
        index: Int,
        reason: FieldValueTupleCodecError
    )
    case maximumEncodedBytesExceeded(actual: Int, maximum: Int)
    case maximumCollectionCountExceeded(actual: Int, maximum: Int)
    case maximumDepthExceeded(actual: Int, maximum: Int)
    case maximumObjectCountExceeded(actual: Int, maximum: Int)
}
