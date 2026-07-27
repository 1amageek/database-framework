import DatabaseTypes

/// One concrete path connection in the active RDF graph.
///
/// Property-path algebra retains graph endpoints instead of materializing
/// query-variable dictionaries at every recursive operator. Visible bindings
/// are constructed once, after the complete path expression has been
/// evaluated.
struct SPARQLPropertyPathMatch: Sendable, Hashable {
    let start: RDFTerm
    let end: RDFTerm

    init(
        start: RDFTerm,
        end: RDFTerm
    ) {
        self.start = start
        self.end = end
    }
}
