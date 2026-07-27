import DatabaseTypes

/// A measured RDF storage representation that initializes final storage directly.
///
/// The plan keeps the validated semantic value and exact byte count together so
/// callers do not need to allocate an intermediate payload before writing into
/// an enclosing DatabaseEngine storage frame.
package struct RDFTermStorageEncoding: Sendable {
    package let term: RDFTerm
    package let limits: RDFTermStorageLimits
    package let byteCount: Int
    package let objectCount: Int
    package let maximumDepth: Int

    init(
        term: RDFTerm,
        limits: RDFTermStorageLimits,
        byteCount: Int,
        objectCount: Int,
        maximumDepth: Int
    ) {
        self.term = term
        self.limits = limits
        self.byteCount = byteCount
        self.objectCount = objectCount
        self.maximumDepth = maximumDepth
    }
}
