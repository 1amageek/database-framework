import DatabaseTypes

/// A decoded RDF term together with the resources consumed by its canonical form.
package struct RDFTermStorageDecoding: Sendable {
    package let term: RDFTerm
    package let objectCount: Int
    package let maximumDepth: Int

    init(
        term: RDFTerm,
        objectCount: Int,
        maximumDepth: Int
    ) {
        self.term = term
        self.objectCount = objectCount
        self.maximumDepth = maximumDepth
    }
}
