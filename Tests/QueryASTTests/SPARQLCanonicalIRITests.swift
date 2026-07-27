import QueryAST
import DatabaseKit
import Testing

@Suite("SPARQL canonical IRI lowering")
struct SPARQLCanonicalIRITests {
    @Test("PREFIX terms and datatypes are lowered to absolute IRIs")
    func prefixTermsBecomeAbsoluteIRIs() throws {
        let statement = try SPARQLParser().parse(
            """
            PREFIX ex: <https://example.invalid/vocabulary/>
            SELECT ?subject WHERE {
              ?subject ex:score "1"^^ex:scoreType .
            }
            """
        )

        guard case .select(let query) = statement,
              case .graphPattern(.basic(let basicGraphPattern)) = query.source else {
            Issue.record("Expected one canonical SELECT basic graph pattern")
            return
        }
        let triples = try basicGraphPattern.triplePatterns()
        guard triples.count == 1 else {
            Issue.record("Expected one canonical SELECT triple")
            return
        }
        #expect(
            triples[0].predicate
                == .iri("https://example.invalid/vocabulary/score")
        )
        #expect(
            triples[0].object
                == .literal(
                    .typedLiteral(
                        value: "1",
                        datatype: "https://example.invalid/vocabulary/scoreType"
                    )
                )
        )
    }

    @Test("DESCRIBE and update graph names use the same canonical lowering")
    func graphFormsBecomeAbsoluteIRIs() throws {
        let describe = try SPARQLParser().parse(
            """
            PREFIX ex: <https://example.invalid/resource/>
            DESCRIBE ex:item
            """
        )
        guard case .describe(let query) = describe else {
            Issue.record("Expected DESCRIBE")
            return
        }
        #expect(query.selection == .resources(
            first: .iri("https://example.invalid/resource/item"),
            additional: []
        ))

        let insert = try SPARQLParser().parse(
            """
            PREFIX ex: <https://example.invalid/>
            INSERT DATA {
              GRAPH ex:graph { ex:s ex:p ex:o . }
            }
            """
        )
        guard case .insertData(let query) = try requireSingleSPARQLUpdateOperation(insert),
              let quad = query.quads.first else {
            Issue.record("Expected INSERT DATA quad")
            return
        }
        #expect(quad.graph == .iri("https://example.invalid/graph"))
        #expect(quad.triple.subject == .iri("https://example.invalid/s"))
        #expect(quad.triple.predicate == .iri("https://example.invalid/p"))
        #expect(quad.triple.object == .iri("https://example.invalid/o"))
    }

    @Test("Undefined prefixes fail during parsing")
    func undefinedPrefixesFail() {
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parse(
                "SELECT * WHERE { ?subject missing:predicate ?object . }"
            )
        }
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parse(
                "SELECT * WHERE { ?subject <https://example.invalid/p> \"1\"^^missing:type . }"
            )
        }
    }

    @Test("Update quad graph names require canonical absolute IRIs")
    func updateQuadGraphNamesRequireAbsoluteIRIs() {
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parse(
                "INSERT DATA { GRAPH <relative> { <https://example.invalid/s> <https://example.invalid/p> <https://example.invalid/o> } }"
            )
        }
        #expect(throws: SPARQLParser.ParseError.self) {
            _ = try SPARQLParser().parse(
                "DELETE DATA { GRAPH missing:graph { <https://example.invalid/s> <https://example.invalid/p> <https://example.invalid/o> } }"
            )
        }
    }
}
