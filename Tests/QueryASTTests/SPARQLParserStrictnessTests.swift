import Testing
import TestHeartbeat
import DatabaseKit
@testable import QueryAST

@Suite("SPARQL Parser Strictness", .heartbeat)
struct SPARQLParserStrictnessTests {
    @Test("Query forms reject a trailing update separator")
    func trailingTokens() throws {
        let statements = [
            "SELECT * WHERE { ?s ?p ?o } ;",
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } ;",
            "ASK { ?s ?p ?o } ;",
            "DESCRIBE <http://example.org/resource> ;",
        ]

        for statement in statements {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(statement)
            }
        }
    }

    @Test("parseSelect rejects trailing tokens")
    func parseSelectTrailingTokens() throws {
        #expect(throws: SPARQLParser.ParseError.self) {
            try SPARQLParser().parseSelect(
                "SELECT * WHERE { ?s ?p ?o } ;"
            )
        }
    }

    @Test("SELECT requires a projection and group graph pattern")
    func selectRequiredSyntax() throws {
        let invalidQueries = [
            "SELECT WHERE { ?s ?p ?o }",
            "SELECT *",
            "SELECT * WHERE",
        ]

        for query in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parseSelect(query)
            }
        }
    }

    @Test("WHERE keyword is optional but its group graph pattern is required")
    func optionalWhereKeyword() throws {
        let withoutKeyword = try SPARQLParser().parseSelect(
            "SELECT * { ?s ?p ?o }"
        )
        let withKeyword = try SPARQLParser().parseSelect(
            "SELECT * WHERE { ?s ?p ?o }"
        )
        #expect(withoutKeyword.source == withKeyword.source)
    }

    @Test("ASK accepts optional WHERE and requires a group graph pattern")
    func askRequiredSyntax() throws {
        let withoutKeyword = try SPARQLParser().parse(
            "ASK { ?s ?p ?o }"
        )
        let withKeyword = try SPARQLParser().parse(
            "ASK WHERE { ?s ?p ?o }"
        )

        guard case .ask(let first) = withoutKeyword,
              case .ask(let second) = withKeyword else {
            Issue.record("Expected ASK statements")
            return
        }
        #expect(first == second)

        for query in ["ASK", "ASK WHERE"] {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(query)
            }
        }
    }

    @Test("FROM clauses require a resolvable IRI")
    func strictDatasetIRI() throws {
        let invalidQueries = [
            "SELECT * FROM WHERE { ?s ?p ?o }",
            "SELECT * FROM NAMED WHERE { ?s ?p ?o }",
            "SELECT * FROM missing:graph WHERE { ?s ?p ?o }",
        ]

        for query in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parseSelect(query)
            }
        }

        let query = try SPARQLParser().parseSelect(
            "PREFIX ex: <http://example.org/> SELECT * FROM ex:graph WHERE { ?s ?p ?o }"
        )
        #expect(query.dataset == .explicit(
            defaultGraphs: ["http://example.org/graph"],
            namedGraphs: []
        ))
    }

    @Test("LIMIT and OFFSET accept either order")
    func limitOffsetOrder() throws {
        let query = try SPARQLParser().parseSelect(
            "SELECT * WHERE { ?s ?p ?o } OFFSET 4 LIMIT 9"
        )
        #expect(query.limit == 9)
        #expect(query.offset == 4)
    }

    @Test("LIMIT and OFFSET reject missing, invalid, overflow, and duplicate values")
    func strictLimitOffsetValues() throws {
        let invalidQueries = [
            "SELECT * WHERE { ?s ?p ?o } LIMIT",
            "SELECT * WHERE { ?s ?p ?o } OFFSET",
            "SELECT * WHERE { ?s ?p ?o } LIMIT -1",
            "SELECT * WHERE { ?s ?p ?o } OFFSET 1.5",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 999999999999999999999999999999999999999",
            "SELECT * WHERE { ?s ?p ?o } LIMIT 1 LIMIT 2",
            "SELECT * WHERE { ?s ?p ?o } OFFSET 1 OFFSET 2",
        ]

        for query in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parseSelect(query)
            }
        }

        let invalidConstructs = [
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT",
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } OFFSET -1",
        ]
        for query in invalidConstructs {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(query)
            }
        }
    }

    @Test("CLEAR requires an explicit valid target")
    func strictClearTarget() throws {
        let invalidUpdates = [
            "CLEAR",
            "CLEAR SILENT",
            "CLEAR GRAPH",
            "CLEAR UNKNOWN",
        ]

        for update in invalidUpdates {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("CLEAR GRAPH lowers prefixed names to canonical IRIs")
    func clearPrefixedGraph() throws {
        let statement = try SPARQLParser().parse(
            "PREFIX ex: <http://example.org/> CLEAR GRAPH ex:graph"
        )
        guard case .clear(let query) = try requireSingleSPARQLUpdateOperation(statement) else {
            Issue.record("Expected CLEAR statement")
            return
        }
        #expect(query.target == .graph("http://example.org/graph"))
    }

    @Test("DESCRIBE star and explicit resources remain structurally distinct")
    func describeSelection() throws {
        let allStatement = try SPARQLParser().parse(
            "DESCRIBE * WHERE { ?resource ?predicate ?object } LIMIT 2"
        )
        let resourceStatement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            DESCRIBE ex:fixed ?resource WHERE { ?resource ?predicate ?object }
            """
        )

        guard case .describe(let allQuery) = allStatement,
              case .describe(let resourceQuery) = resourceStatement else {
            Issue.record("Expected DESCRIBE statements")
            return
        }
        #expect(allQuery.selection == .all)
        #expect(allQuery.modifiers.limit == 2)
        #expect(resourceQuery.selection == .resources(
            first: .iri("http://example.org/fixed"),
            additional: [.variable("resource")]
        ))

        #expect(throws: SPARQLParser.ParseError.self) {
            try SPARQLParser().parse("DESCRIBE WHERE { ?s ?p ?o }")
        }
    }

    @Test("ASK and DESCRIBE preserve datasets and solution modifiers")
    func nonSelectQueryMetadata() throws {
        let askStatement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            ASK FROM ex:default FROM NAMED ex:named
            WHERE { ?subject ?predicate ?object }
            GROUP BY ?subject
            HAVING (?subject = ex:item)
            ORDER BY ?subject
            OFFSET 3 LIMIT 4
            """
        )
        guard case .ask(let ask) = askStatement else {
            Issue.record("Expected ASK statement")
            return
        }
        #expect(ask.dataset == .explicit(
            defaultGraphs: ["http://example.org/default"],
            namedGraphs: ["http://example.org/named"]
        ))
        #expect(ask.modifiers.groupBy.count == 1)
        #expect(ask.modifiers.having.count == 1)
        #expect(ask.modifiers.orderBy.count == 1)
        #expect(ask.modifiers.offset == 3)
        #expect(ask.modifiers.limit == 4)

        let describeStatement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            DESCRIBE ?resource FROM NAMED ex:named
            WHERE { GRAPH ex:named { ?resource ?predicate ?object } }
            ORDER BY ?resource LIMIT 1 OFFSET 2
            """
        )
        guard case .describe(let describe) = describeStatement else {
            Issue.record("Expected DESCRIBE statement")
            return
        }
        #expect(describe.dataset == .explicit(
            defaultGraphs: [],
            namedGraphs: ["http://example.org/named"]
        ))
        #expect(describe.modifiers.orderBy.count == 1)
        #expect(describe.modifiers.limit == 1)
        #expect(describe.modifiers.offset == 2)
    }

    @Test("CONSTRUCT shorthand accepts only a basic triple template")
    func strictConstructShorthand() throws {
        let statement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            CONSTRUCT FROM ex:default WHERE { ?s ex:p ?o }
            ORDER BY ?s LIMIT 5
            """
        )
        guard case .construct(let query) = statement else {
            Issue.record("Expected CONSTRUCT statement")
            return
        }
        #expect(query.template.count == 1)
        #expect(
            query.pattern == .basic(
                BasicGraphPattern(triples: query.template)
            )
        )
        #expect(query.dataset == .explicit(
            defaultGraphs: ["http://example.org/default"],
            namedGraphs: []
        ))
        #expect(query.modifiers.orderBy.count == 1)
        #expect(query.modifiers.limit == 5)

        let invalidQueries = [
            "CONSTRUCT WHERE { ?s ?p ?o FILTER(?o = ?s) }",
            "CONSTRUCT WHERE { { ?s ?p ?o } UNION { ?a ?b ?c } }",
            "CONSTRUCT FROM <http://example.org/default> { ?s ?p ?o }",
        ]
        for invalidQuery in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(invalidQuery)
            }
        }
    }

    @Test("SPARQL update graph IRIs use one canonical fail-closed path")
    func canonicalUpdateIRIs() throws {
        let loadStatement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            LOAD SILENT ex:source INTO GRAPH ex:destination
            """
        )
        guard case .load(let load) = try requireSingleSPARQLUpdateOperation(loadStatement) else {
            Issue.record("Expected LOAD statement")
            return
        }
        #expect(load.source == "http://example.org/source")
        #expect(load.destination == "http://example.org/destination")
        #expect(load.silent)

        let createStatement = try SPARQLParser().parse(
            "BASE <http://example.org/base/> CREATE GRAPH <created>"
        )
        let dropStatement = try SPARQLParser().parse(
            "BASE <http://example.org/base/> DROP SILENT GRAPH <dropped>"
        )
        #expect(try requireSingleSPARQLUpdateOperation(createStatement) == .createGraph(
            CreateSPARQLGraphQuery(
                graph: "http://example.org/base/created"
            )
        ))
        #expect(try requireSingleSPARQLUpdateOperation(dropStatement) == .drop(
            DropQuery(
                target: .graph("http://example.org/base/dropped"),
                silent: true
            )
        ))

        let modifyStatement = try SPARQLParser().parse(
            """
            PREFIX ex: <http://example.org/>
            DELETE { ?s ?p ?o }
            USING ex:default USING NAMED ex:named
            WHERE { ?s ?p ?o }
            """
        )
        guard case .modify(let modify) = try requireSingleSPARQLUpdateOperation(modifyStatement) else {
            Issue.record("Expected DELETE/INSERT statement")
            return
        }
        #expect(modify.using == [
            GraphRef(iri: "http://example.org/default"),
            GraphRef(iri: "http://example.org/named", isNamed: true),
        ])

        let invalidUpdates = [
            "LOAD <http://example.org/source> INTO <http://example.org/destination>",
            "CREATE <http://example.org/graph>",
            "DROP <http://example.org/graph>",
            "LOAD missing:source",
            "CREATE GRAPH <relative-graph>",
        ]
        for update in invalidUpdates {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("WITH is preserved separately from USING on Modify")
    func withModifyPreservesDatasetRoles() throws {
        let statement = try SPARQLParser().parse(
            """
            BASE <https://example.org/runtime/>
            WITH <active>
            DELETE { ?subject <urn:old> ?value }
            INSERT { ?subject <urn:new> ?value }
            USING <query-default>
            USING NAMED <query-named>
            WHERE { ?subject <urn:old> ?value }
            """
        )
        guard case .modify(let query) = try requireSingleSPARQLUpdateOperation(statement),
              case .deleteAndInsert(let deletePattern, let insertPattern) = query.action else {
            Issue.record("Expected a DELETE/INSERT statement")
            return
        }

        #expect(query.withGraph == "https://example.org/runtime/active")
        #expect(query.using == [
            GraphRef(iri: "https://example.org/runtime/query-default"),
            GraphRef(
                iri: "https://example.org/runtime/query-named",
                isNamed: true
            ),
        ])
        #expect(deletePattern.count == 1)
        #expect(insertPattern.count == 1)
    }

    @Test("DELETE WHERE has one canonical quad-pattern representation")
    func deleteWhereUsesOneCanonicalPattern() throws {
        let statement = try SPARQLParser().parse(
            """
            DELETE WHERE {
              ?subject <urn:p> ?object .
              GRAPH ?graph { ?subject <urn:q> ?other }
            }
            """
        )
        guard case .deleteWhere(let query) = try requireSingleSPARQLUpdateOperation(statement) else {
            Issue.record("Expected a DELETE WHERE statement")
            return
        }

        #expect(query.pattern.count == 2)
        #expect(query.pattern[0].graph == nil)
        #expect(query.pattern[1].graph == .variable("graph"))

        let invalid = [
            "DELETE WHERE { _:blank <urn:p> ?object }",
            "DELETE WHERE { ?subject <urn:p>/<urn:q> ?object }",
            "WITH <urn:g> DELETE WHERE { ?subject <urn:p> ?object }",
        ]
        for update in invalid {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("DROP supports every graph-store target")
    func dropGraphStoreTargets() throws {
        #expect(try requireSingleSPARQLUpdateOperation(SPARQLParser().parse("DROP DEFAULT")) == .drop(
            DropQuery(target: .default)
        ))
        #expect(try requireSingleSPARQLUpdateOperation(SPARQLParser().parse("DROP NAMED")) == .drop(
            DropQuery(target: .named)
        ))
        #expect(try requireSingleSPARQLUpdateOperation(SPARQLParser().parse("DROP ALL")) == .drop(
            DropQuery(target: .all)
        ))
        #expect(
            try requireSingleSPARQLUpdateOperation(
                SPARQLParser().parse("DROP SILENT GRAPH <urn:graph>")
            ) == .drop(
                    DropQuery(target: .graph("urn:graph"), silent: true)
                )
        )
    }

    @Test("ADD COPY and MOVE use restricted typed endpoints")
    func graphTransferOperations() throws {
        let add = try SPARQLParser().parse(
            "BASE <https://example.org/> ADD SILENT DEFAULT TO GRAPH <archive>"
        )
        #expect(try requireSingleSPARQLUpdateOperation(add) == .graphTransfer(
            GraphTransferQuery(
                operation: .add,
                source: .default,
                destination: .graph("https://example.org/archive"),
                silent: true
            )
        ))

        let copy = try SPARQLParser().parse(
            "PREFIX ex: <https://example.org/> COPY GRAPH ex:source TO DEFAULT"
        )
        #expect(try requireSingleSPARQLUpdateOperation(copy) == .graphTransfer(
            GraphTransferQuery(
                operation: .copy,
                source: .graph("https://example.org/source"),
                destination: .default
            )
        ))

        let move = try SPARQLParser().parse(
            "MOVE GRAPH <urn:source> TO GRAPH <urn:destination>"
        )
        #expect(try requireSingleSPARQLUpdateOperation(move) == .graphTransfer(
            GraphTransferQuery(
                operation: .move,
                source: .graph("urn:source"),
                destination: .graph("urn:destination")
            )
        ))

        let invalid = [
            "ADD NAMED TO DEFAULT",
            "COPY ALL TO DEFAULT",
            "MOVE GRAPH <urn:source> DEFAULT",
            "COPY GRAPH <relative> TO DEFAULT",
        ]
        for update in invalid {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("RDF terms and datatype IRIs are absolute after prologue lowering")
    func absoluteRDFTerms() throws {
        let invalidQueries = [
            "SELECT * WHERE { <relative> <urn:p> <urn:o> }",
            "SELECT * WHERE { <urn:s> <relative> <urn:o> }",
            "SELECT * WHERE { <urn:s> <urn:p> <relative> }",
            "SELECT * WHERE { <urn:s> <urn:p> \"value\"^^<relative> }",
            "PREFIX ex:local <urn:base:> SELECT * WHERE { ?s ?p ?o }",
            "BASE <relative> SELECT * WHERE { ?s ?p ?o }",
        ]
        for query in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(query)
            }
        }

        let statement = try SPARQLParser().parse(
            "BASE <https://example.invalid/base/> SELECT * WHERE { <subject> <predicate> \"value\"^^<datatype> }"
        )
        guard case .select(let query) = statement,
              case .graphPattern(.basic(let basicGraphPattern)) = query.source,
              case .triple(let triple)? = basicGraphPattern.elements.first else {
            Issue.record("Expected a BASE-resolved triple")
            return
        }
        #expect(triple.subject == .iri("https://example.invalid/base/subject"))
        #expect(triple.predicate == .iri("https://example.invalid/base/predicate"))
        #expect(triple.object == .literal(.typedLiteral(
            value: "value",
            datatype: "https://example.invalid/base/datatype"
        )))
    }

    @Test("String suffixes and ORDER direction fail closed")
    func strictStringSuffixesAndOrderDirection() {
        let invalidQueries = [
            "SELECT * WHERE { <urn:s> <urn:p> \"value\"^^42 }",
            "SELECT * WHERE { <urn:s> <urn:p> \"value\"@ }",
            "SELECT * WHERE { <urn:s> <urn:p> \"value\"@en--sideways }",
            "SELECT * WHERE { <urn:s> <urn:p> \"unterminated }",
            "SELECT * WHERE { ?s ?p ?o } ORDER BY ?s ASC",
        ]
        for query in invalidQueries {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(query)
            }
        }
    }

    @Test("Update data and templates enforce SPARQL term restrictions")
    func updateTermRestrictions() {
        let invalidUpdates = [
            "INSERT DATA { ?s <urn:p> <urn:o> }",
            "DELETE DATA { <urn:s> <urn:p> ?o }",
            "DELETE DATA { _:blank <urn:p> <urn:o> }",
            "DELETE { _:blank <urn:p> ?o } WHERE { ?s <urn:p> ?o }",
            "INSERT DATA { <urn:s> <urn:p>/<urn:q> <urn:o> }",
            "DELETE { ?s <urn:p>/<urn:q> ?o } WHERE { ?s <urn:p> ?o }",
            "SELECT * WHERE { ?subject }",
            "SELECT * WHERE { GRAPH \"not-a-graph\" { ?s ?p ?o } }",
        ]
        for update in invalidUpdates {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("Update templates support graph variables while DATA remains ground")
    func updateTemplateGraphVariables() throws {
        let statement = try SPARQLParser().parse(
            """
            DELETE { GRAPH ?graph { ?subject <urn:old> ?value } }
            INSERT { GRAPH ?graph { ?subject <urn:new> ?value } }
            WHERE { GRAPH ?graph { ?subject <urn:old> ?value } }
            """
        )
        guard case .modify(let update) = try requireSingleSPARQLUpdateOperation(statement),
              case .deleteAndInsert(let deletePattern, let insertPattern) = update.action else {
            Issue.record("Expected DELETE/INSERT statement")
            return
        }
        #expect(deletePattern.first?.graph == .variable("graph"))
        #expect(insertPattern.first?.graph == .variable("graph"))

        let invalidDataUpdates = [
            "INSERT DATA { GRAPH ?graph { <urn:s> <urn:p> <urn:o> } }",
            "DELETE DATA { GRAPH ?graph { <urn:s> <urn:p> <urn:o> } }",
        ]
        for update in invalidDataUpdates {
            #expect(throws: SPARQLParser.ParseError.self) {
                try SPARQLParser().parse(update)
            }
        }
    }

    @Test("Language annotations are validated and canonicalized")
    func canonicalLanguageAnnotations() throws {
        let statement = try SPARQLParser().parse(
            "SELECT * WHERE { <urn:s> <urn:p> \"value\"@EN--RTL }"
        )
        guard case .select(let query) = statement,
              case .graphPattern(.basic(let basicGraphPattern)) = query.source,
              case .triple(let triple)? = basicGraphPattern.elements.first else {
            Issue.record("Expected a language-annotated triple")
            return
        }
        #expect(triple.object == .literal(.dirLangLiteral(
            value: "value",
            language: "en",
            direction: "rtl"
        )))
    }

    @Test("GROUP BY aliases are explicit bindings before grouping")
    func groupByAliasLowering() throws {
        let query = try SPARQLParser().parseSelect(
            "SELECT ?key WHERE { ?s <urn:p> ?o } GROUP BY (STR(?s) AS ?key)"
        )
        #expect(query.groupBy == [.variable(Variable("key"))])
        guard case .graphPattern(
            .bind(_, variable: let alias, expression: _)
        ) = query.source else {
            Issue.record("Expected GROUP BY alias to lower to BIND")
            return
        }
        #expect(alias == "key")

        #expect(throws: SPARQLParser.ParseError.self) {
            try SPARQLParser().parseSelect(
                "SELECT * WHERE { ?s ?p ?o } GROUP BY (STR(?s) AS)"
            )
        }
    }

    @Test("Large HAVING lists build a bounded-depth conjunction")
    func balancedHavingConjunction() throws {
        let conditions = Array(
            repeating: "(?subject = ?subject)",
            count: 1_024
        ).joined(separator: " ")
        let query = try SPARQLParser().parseSelect(
            "SELECT ?subject WHERE { ?subject ?predicate ?object } HAVING \(conditions)"
        )
        let having = try #require(query.having)
        #expect(conjunctionDepth(having) <= 11)
    }

    private func conjunctionDepth(
        _ expression: DatabaseKit.Expression
    ) -> Int {
        guard case .and(let lhs, let rhs) = expression else { return 1 }
        return 1 + max(conjunctionDepth(lhs), conjunctionDepth(rhs))
    }

    @Test("Unterminated and invalid IRI references are lexical failures")
    func invalidIRIReferences() {
        #expect(
            throws: SPARQLParser.ParseError.invalidIRI(
                "Unterminated IRI reference at position 5"
            )
        ) {
            try SPARQLParser().parse("LOAD <unterminated")
        }
        #expect(throws: SPARQLParser.ParseError.self) {
            try SPARQLParser().parse(
                "LOAD <http://example.org/invalid value>"
            )
        }
    }
}
