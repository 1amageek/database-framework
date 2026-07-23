import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import QueryIR
import QueryAST
import StorageKit
import Testing
@testable import GraphIndex

@Suite("SPARQL blank node pattern semantics")
struct SPARQLBlankNodePatternTests {
    private let predicate = "https://example.invalid/predicate"

    @Test("A query blank node lowers to one non-projectable variable per BGP")
    func blankNodeLowersToScopedVariable() throws {
        let pattern = try GraphPatternConverter.convert(
            .basic([
                TriplePattern(
                    subject: .blankNode("same"),
                    predicate: .iri(predicate),
                    object: .blankNode("same")
                ),
            ])
        )

        guard case .basic(let triples) = pattern,
              triples.count == 1,
              case .variable(let subject) = triples[0].subject,
              case .variable(let object) = triples[0].object else {
            Issue.record("Expected one lowered basic graph pattern")
            return
        }
        #expect(subject == object)
        #expect(SPARQLInternalVariable.isInternal(subject))
    }

    @Test("A query blank node matches any RDF subject instead of one stored label")
    func blankNodeIsExistential() async throws {
        let executionContext = try await makeSPARQLExecutionContext(
            quads: [
                RDFQuad(
                    subject: .blankNode("stored"),
                    predicate: .iri(predicate),
                    object: .iri("https://example.invalid/object/blank")
                ),
                RDFQuad(
                    subject: .iri("https://example.invalid/subject/iri"),
                    predicate: .iri(predicate),
                    object: .iri("https://example.invalid/object/iri")
                ),
            ]
        )
        let pattern = try GraphPatternConverter.convert(
            .basic([
                TriplePattern(
                    subject: .blankNode("query"),
                    predicate: .iri(predicate),
                    object: .variable("object")
                ),
            ])
        )

        let (bindings, _) = try await executionContext.executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter()
        )

        #expect(bindings.count == 2)
        let objects = Set(bindings.compactMap { binding -> String? in
            guard case .rdfTerm(.iri(let iri)) = binding["?object"] else {
                return nil
            }
            return iri
        })
        #expect(
            objects == [
                "https://example.invalid/object/blank",
                "https://example.invalid/object/iri",
            ]
        )
    }

    @Test("Repeated query blank labels impose an equality constraint")
    func repeatedBlankNodeLabelSharesOneExistential() async throws {
        let selfNode = DatabaseRDFTerm.iri(
            "https://example.invalid/subject/self"
        )
        let executionContext = try await makeSPARQLExecutionContext(
            quads: [
                RDFQuad(
                    subject: selfNode,
                    predicate: .iri(predicate),
                    object: selfNode
                ),
                RDFQuad(
                    subject: .iri("https://example.invalid/subject/other"),
                    predicate: .iri(predicate),
                    object: .iri("https://example.invalid/object/other")
                ),
            ]
        )
        let pattern = try GraphPatternConverter.convert(
            .basic([
                TriplePattern(
                    subject: .blankNode("same"),
                    predicate: .iri(predicate),
                    object: .blankNode("same")
                ),
            ])
        )

        let (bindings, _) = try await executionContext.executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter()
        )

        #expect(bindings.count == 1)
    }

    @Test("A parsed triple and property path share one blank-node binding")
    func parsedMixedBasicGraphPatternSharesBlankNodeBinding() async throws {
        let firstPredicate = "https://example.invalid/first"
        let stepPredicate = "https://example.invalid/step"
        let lastPredicate = "https://example.invalid/last"
        let matchingSubject = DatabaseRDFTerm.iri(
            "https://example.invalid/subject/matching"
        )
        let tripleOnlySubject = DatabaseRDFTerm.iri(
            "https://example.invalid/subject/triple-only"
        )
        let pathOnlySubject = DatabaseRDFTerm.iri(
            "https://example.invalid/subject/path-only"
        )
        let matchingMiddle = DatabaseRDFTerm.iri(
            "https://example.invalid/middle/matching"
        )
        let pathOnlyMiddle = DatabaseRDFTerm.iri(
            "https://example.invalid/middle/path-only"
        )
        let expectedValue = "https://example.invalid/value/matching"
        let expectedTarget = "https://example.invalid/target/matching"
        let executionContext = try await makeSPARQLExecutionContext(
            quads: [
                RDFQuad(
                    subject: matchingSubject,
                    predicate: .iri(firstPredicate),
                    object: .iri(expectedValue)
                ),
                RDFQuad(
                    subject: matchingSubject,
                    predicate: .iri(stepPredicate),
                    object: matchingMiddle
                ),
                RDFQuad(
                    subject: matchingMiddle,
                    predicate: .iri(lastPredicate),
                    object: .iri(expectedTarget)
                ),
                RDFQuad(
                    subject: tripleOnlySubject,
                    predicate: .iri(firstPredicate),
                    object: .iri("https://example.invalid/value/triple-only")
                ),
                RDFQuad(
                    subject: pathOnlySubject,
                    predicate: .iri(stepPredicate),
                    object: pathOnlyMiddle
                ),
                RDFQuad(
                    subject: pathOnlyMiddle,
                    predicate: .iri(lastPredicate),
                    object: .iri("https://example.invalid/target/path-only")
                ),
            ]
        )
        let query = try SPARQLParser().parseSelect(
            """
            SELECT ?value ?target WHERE {
                _:shared <https://example.invalid/first> ?value .
                _:shared <https://example.invalid/step>/<https://example.invalid/last> ?target
            }
            """
        )
        guard case .graphPattern(let queryPattern) = query.source else {
            Issue.record("Expected a graph-pattern source")
            return
        }
        let pattern = try GraphPatternConverter.convert(queryPattern)

        let (bindings, _) = try await executionContext.executor.execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter()
        )

        #expect(bindings.count == 1)
        let binding = try #require(bindings.first)
        #expect(binding["?value"] == .rdfTerm(.iri(expectedValue)))
        #expect(binding["?target"] == .rdfTerm(.iri(expectedTarget)))
    }

    private struct SPARQLExecutionContext {
        let executor: SPARQLQueryExecutor
    }

    private func makeSPARQLExecutionContext(
        quads: [RDFQuad]
    ) async throws -> SPARQLExecutionContext {
        let engine = InMemoryEngine()
        let store = CanonicalRDFGraphStore(
            rootSubspace: Subspace(
                prefix: Tuple("sparql-blank-node-pattern-tests").pack()
            )
        )
        _ = try await engine.withTransaction(
            configuration: .batch
        ) { transaction in
            let meter = makeMeter()
            for quad in quads {
                _ = try await store.insert(
                    quad,
                    transaction: transaction,
                    workMeter: meter
                )
            }
        }
        return SPARQLExecutionContext(
            executor: SPARQLQueryExecutor(
                database: engine,
                datasetScanner: store
            )
        )
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            )
        )
    }
}
