import QueryAST
import QueryIR
import Testing

@Suite("SPARQL IRI reference resolution")
struct SPARQLIRIResolutionTests {
    @Test("RFC 3986 reference vectors resolve deterministically")
    func referenceResolutionVectors() throws {
        let base = "http://a/b/c/d;p?q"
        let vectors = [
            ("g:h", "g:h"),
            ("g", "http://a/b/c/g"),
            ("./g", "http://a/b/c/g"),
            ("g/", "http://a/b/c/g/"),
            ("/g", "http://a/g"),
            ("//g", "http://g"),
            ("?y", "http://a/b/c/d;p?y"),
            ("g?y", "http://a/b/c/g?y"),
            ("#s", "http://a/b/c/d;p?q#s"),
            ("g#s", "http://a/b/c/g#s"),
            ("g?y#s", "http://a/b/c/g?y#s"),
            (";x", "http://a/b/c/;x"),
            ("g;x", "http://a/b/c/g;x"),
            ("g;x?y#s", "http://a/b/c/g;x?y#s"),
            ("", "http://a/b/c/d;p?q"),
            (".", "http://a/b/c/"),
            ("./", "http://a/b/c/"),
            ("..", "http://a/b/"),
            ("../", "http://a/b/"),
            ("../g", "http://a/b/g"),
            ("../..", "http://a/"),
            ("../../", "http://a/"),
            ("../../g", "http://a/g"),
            ("../../../g", "http://a/g"),
            ("../../../../g", "http://a/g"),
            ("/./g", "http://a/g"),
            ("/../g", "http://a/g"),
            ("g.", "http://a/b/c/g."),
            (".g", "http://a/b/c/.g"),
            ("g..", "http://a/b/c/g.."),
            ("..g", "http://a/b/c/..g"),
            ("./../g", "http://a/b/g"),
            ("./g/.", "http://a/b/c/g/"),
            ("g/./h", "http://a/b/c/g/h"),
            ("g/../h", "http://a/b/c/h"),
            ("g;x=1/./y", "http://a/b/c/g;x=1/y"),
            ("g;x=1/../y", "http://a/b/c/y"),
            ("g?y/./x", "http://a/b/c/g?y/./x"),
            ("g?y/../x", "http://a/b/c/g?y/../x"),
            ("g#s/./x", "http://a/b/c/g#s/./x"),
            ("g#s/../x", "http://a/b/c/g#s/../x"),
        ]

        for (reference, expected) in vectors {
            let triples = try parseTriples(
                "BASE <\(base)>\nSELECT * WHERE { <\(reference)> <urn:p> <urn:o> }"
            )
            #expect(
                triples.first?.subject == .iri(expected),
                "reference=<\(reference)>"
            )
        }
    }

    @Test("Query and fragment references replace matching base components")
    func queryAndFragmentReferencesReplaceComponents() throws {
        let triples = try parseTriples(
            """
            BASE <http://a.example/b/c/d;p?q#old>
            SELECT * WHERE { <g> <?y> <#s> }
            """
        )

        #expect(triples[0].subject == .iri("http://a.example/b/c/g"))
        #expect(triples[0].predicate == .iri("http://a.example/b/c/d;p?y"))
        #expect(triples[0].object == .iri("http://a.example/b/c/d;p?q#s"))
    }

    @Test("Dot segments are removed after merging relative paths")
    func dotSegmentsAreRemovedAfterMerge() throws {
        let triples = try parseTriples(
            """
            BASE <http://a.example/b/c/d;p?q>
            SELECT * WHERE {
              <../g> <urn:p> <urn:o> .
              <./g> <urn:p> <urn:o> .
              <g/./h> <urn:p> <urn:o> .
              <g/../h> <urn:p> <urn:o> .
              </g/../h> <urn:p> <urn:o> .
            }
            """
        )

        #expect(triples.map(\.subject) == [
            .iri("http://a.example/b/g"),
            .iri("http://a.example/b/c/g"),
            .iri("http://a.example/b/c/g/h"),
            .iri("http://a.example/b/c/h"),
            .iri("http://a.example/h"),
        ])
    }

    @Test("Network paths inherit only the scheme and retain Unicode spelling")
    func networkPathRetainsUnicodeSpelling() throws {
        let triples = try parseTriples(
            """
            BASE <https://base.example/root/>
            SELECT * WHERE {
              <//例え.example/東京/../祭> <urn:p> <urn:o>
            }
            """
        )

        #expect(triples[0].subject == .iri("https://例え.example/祭"))
    }

    @Test("An empty reference inherits path and query but removes the fragment")
    func emptyReferenceUsesBaseDocumentIRI() throws {
        let triples = try parseTriples(
            """
            BASE <https://example.org/a/b?old#fragment>
            SELECT * WHERE { <> <urn:p> <urn:o> }
            """
        )

        #expect(triples[0].subject == .iri("https://example.org/a/b?old"))
    }

    @Test("A later relative BASE resolves against the current BASE")
    func relativeBaseUpdatesResolutionContext() throws {
        let triples = try parseTriples(
            """
            BASE <https://example.org/a/b/c/>
            BASE <../runtime/>
            SELECT * WHERE { <item> <urn:p> <urn:o> }
            """
        )

        #expect(
            triples[0].subject
                == .iri("https://example.org/a/b/runtime/item")
        )
    }

    @Test("A colon in the first relative path segment is rejected")
    func invalidRelativePathWithColonIsRejected() {
        #expect(throws: SPARQLParser.ParseError.invalidIRI("1bad:value")) {
            _ = try SPARQLParser().parse(
                """
                BASE <https://example.org/base/>
                SELECT * WHERE { <1bad:value> <urn:p> <urn:o> }
                """
            )
        }
    }

    private func parseTriples(_ sparql: String) throws -> [TriplePattern] {
        let statement = try SPARQLParser().parse(sparql)
        guard case .select(let query) = statement,
              case .graphPattern(.basic(let basicGraphPattern)) = query.source else {
            throw SPARQLParser.ParseError.invalidSyntax(
                message: "Expected a SELECT basic graph pattern",
                position: 0
            )
        }
        return try basicGraphPattern.triplePatterns()
    }
}
