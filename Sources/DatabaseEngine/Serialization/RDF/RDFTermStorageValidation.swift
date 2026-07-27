import DatabaseKit
import DatabaseTypes

/// Proof produced by bounded validation of borrowed canonical RDF bytes.
package struct RDFTermStorageValidation: Sendable, Equatable {
    package let kind: RDFTermKind
    package let fingerprint: RDFTermStorageFingerprint
    package let objectCount: Int
    package let maximumDepth: Int

    init(
        kind: RDFTermKind,
        fingerprint: RDFTermStorageFingerprint,
        objectCount: Int,
        maximumDepth: Int
    ) {
        self.kind = kind
        self.fingerprint = fingerprint
        self.objectCount = objectCount
        self.maximumDepth = maximumDepth
    }
}
