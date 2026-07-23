import DatabaseValue

/// One concrete path connection in the active RDF graph.
///
/// Property-path algebra retains graph endpoints instead of materializing
/// query-variable dictionaries at every recursive operator. Visible bindings
/// are constructed once, after the complete path expression has been
/// evaluated.
struct SPARQLPropertyPathMatch: Sendable, Hashable {
    let start: DatabaseRDFTerm
    let end: DatabaseRDFTerm

    init(
        start: DatabaseRDFTerm,
        end: DatabaseRDFTerm
    ) {
        self.start = start
        self.end = end
    }
}
