import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import QueryAST
import StorageKit
import TestSupport
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
                try quad(
                    subject: .blankNode(identifier: "stored"),
                    predicate: predicate,
                    object: .iri(
                        validating:
                            "https://example.invalid/object/blank"
                    )
                ),
                try quad(
                    subject: .iri(
                        validating:
                            "https://example.invalid/subject/iri"
                    ),
                    predicate: predicate,
                    object: .iri(
                        validating:
                            "https://example.invalid/object/iri"
                    )
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

        let (bindings, _) = try await executeSPARQLTest(
            executor: executionContext.executor,
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: executionContext.database
        )

        #expect(bindings.count == 2)
        let objects = Set(bindings.compactMap { binding -> String? in
            guard case .rdfTerm(.iri(let iri)) = binding["?object"] else {
                return nil
            }
            return iri.rawValue
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
        let selfNode = try RDFTerm.iri(
            validating: "https://example.invalid/subject/self"
        )
        let executionContext = try await makeSPARQLExecutionContext(
            quads: [
                try quad(
                    subject: selfNode,
                    predicate: predicate,
                    object: selfNode
                ),
                try quad(
                    subject: .iri(
                        validating:
                            "https://example.invalid/subject/other"
                    ),
                    predicate: predicate,
                    object: .iri(
                        validating:
                            "https://example.invalid/object/other"
                    )
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

        let (bindings, _) = try await executeSPARQLTest(
            executor: executionContext.executor,
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: executionContext.database
        )

        #expect(bindings.count == 1)
    }

    @Test("A parsed triple and property path share one blank-node binding")
    func parsedMixedBasicGraphPatternSharesBlankNodeBinding() async throws {
        let firstPredicate = "https://example.invalid/first"
        let stepPredicate = "https://example.invalid/step"
        let lastPredicate = "https://example.invalid/last"
        let matchingSubject = try RDFTerm.iri(
            validating: "https://example.invalid/subject/matching"
        )
        let tripleOnlySubject = try RDFTerm.iri(
            validating: "https://example.invalid/subject/triple-only"
        )
        let pathOnlySubject = try RDFTerm.iri(
            validating: "https://example.invalid/subject/path-only"
        )
        let matchingMiddle = try RDFTerm.iri(
            validating: "https://example.invalid/middle/matching"
        )
        let pathOnlyMiddle = try RDFTerm.iri(
            validating: "https://example.invalid/middle/path-only"
        )
        let expectedValue = "https://example.invalid/value/matching"
        let expectedTarget = "https://example.invalid/target/matching"
        let executionContext = try await makeSPARQLExecutionContext(
            quads: [
                try quad(
                    subject: matchingSubject,
                    predicate: firstPredicate,
                    object: .iri(validating: expectedValue)
                ),
                try quad(
                    subject: matchingSubject,
                    predicate: stepPredicate,
                    object: matchingMiddle
                ),
                try quad(
                    subject: matchingMiddle,
                    predicate: lastPredicate,
                    object: .iri(validating: expectedTarget)
                ),
                try quad(
                    subject: tripleOnlySubject,
                    predicate: firstPredicate,
                    object: .iri(
                        validating:
                            "https://example.invalid/value/triple-only"
                    )
                ),
                try quad(
                    subject: pathOnlySubject,
                    predicate: stepPredicate,
                    object: pathOnlyMiddle
                ),
                try quad(
                    subject: pathOnlyMiddle,
                    predicate: lastPredicate,
                    object: .iri(
                        validating:
                            "https://example.invalid/target/path-only"
                    )
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

        let (bindings, _) = try await executeSPARQLTest(
            executor: executionContext.executor,
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: executionContext.database
        )

        #expect(bindings.count == 1)
        let binding = try #require(bindings.first)
        let expectedValueTerm = try RDFTerm.iri(
            validating: expectedValue
        )
        let expectedTargetTerm = try RDFTerm.iri(
            validating: expectedTarget
        )
        #expect(
            binding["?value"]
                == .rdfTerm(expectedValueTerm)
        )
        #expect(
            binding["?target"]
                == .rdfTerm(expectedTargetTerm)
        )
    }

    private struct SPARQLExecutionContext {
        let executor: SPARQLQueryExecutor
        let database: InMemoryEngine
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
        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(
            configuration: .batch,
            clock: TestProcessMonotonicClock()
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
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock(
                    now: Timestamp(secondsSinceUnixEpoch: 0)
                ),
                datasetScanner: store
            ),
            database: engine
        )
    }

    private func quad(
        subject: RDFTerm,
        predicate: String,
        object: RDFTerm
    ) throws -> RDFQuad {
        try RDFQuad(
            validatingSubject: subject,
            predicate: .iri(validating: predicate),
            object: object
        )
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
