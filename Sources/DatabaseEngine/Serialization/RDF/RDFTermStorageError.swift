import DatabaseKit
import DatabaseTypes

/// A deterministic failure to encode or decode persisted RDF bytes.
public enum RDFTermStorageError: Error, Sendable, Equatable {
    case truncated
    case trailingBytes
    case unknownTag(UInt8)
    case invalidUTF8
    case invalidIRI(RDFIRIError)
    case invalidBlankNodeIdentifier
    case invalidTripleSubject
    case invalidTriplePredicate
    case invalidDatatypeIRI
    case invalidLanguageTag
    case nonCanonicalLanguageTag
    case invalidLiteralAnnotation(UInt8)
    case invalidDirection(UInt8)
    case invalidRole(
        expected: RDFTermRole,
        actual: RDFTermKind
    )
    case nonCanonicalVarint
    case nonCanonicalStringEncoding
    case byteCountOverflow
    case maximumBytesExceeded(actual: Int, maximum: Int)
    case maximumDepthExceeded(actual: Int, maximum: Int)
    case maximumObjectCountExceeded(actual: Int, maximum: Int)
}
