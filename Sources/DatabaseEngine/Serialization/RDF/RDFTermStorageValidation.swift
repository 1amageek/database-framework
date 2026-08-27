import DatabaseKit
import DatabaseTypes

/// Proof produced by bounded validation of borrowed canonical RDF bytes.
package struct RDFTermStorageValidation: Sendable, Equatable {
    package let kind: RDFTermKind
    package let fingerprint: RDFTermStorageFingerprint
    package let objectCount: Int
    package let maximumDepth: Int
    package let literalCount: Int
    package let stringCount: Int
    package let decodedStringByteCount: Int
    package let blankNodeCount: Int
    package let blankNodeStringByteCount: Int

    init(
        kind: RDFTermKind,
        fingerprint: RDFTermStorageFingerprint,
        objectCount: Int,
        maximumDepth: Int,
        literalCount: Int,
        stringCount: Int,
        decodedStringByteCount: Int,
        blankNodeCount: Int,
        blankNodeStringByteCount: Int
    ) {
        self.kind = kind
        self.fingerprint = fingerprint
        self.objectCount = objectCount
        self.maximumDepth = maximumDepth
        self.literalCount = literalCount
        self.stringCount = stringCount
        self.decodedStringByteCount = decodedStringByteCount
        self.blankNodeCount = blankNodeCount
        self.blankNodeStringByteCount = blankNodeStringByteCount
    }
}
