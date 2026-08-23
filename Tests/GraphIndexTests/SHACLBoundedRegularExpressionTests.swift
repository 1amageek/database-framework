import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import TestHeartbeat
import TestSupport
import Testing
@testable import GraphIndex

@Suite("Bounded SHACL regular expressions", .heartbeat)
struct SHACLBoundedRegularExpressionTests {
    @Test("SHACL patterns preserve SPARQL matching and flag semantics")
    func matchingAndFlags() throws {
        let plain = try SHACLRegularExpression(
            pattern: "^[a-z]+$",
            flags: nil
        )
        let insensitive = try SHACLRegularExpression(
            pattern: "^[a-z]+$",
            flags: "i"
        )

        #expect(try plain.matches("calendar"))
        #expect(try !plain.matches("Calendar"))
        #expect(try insensitive.matches("Calendar"))
    }

    @Test("Invalid syntax and flags are shape-definition failures")
    func invalidPatternFailures() {
        for (pattern, flags) in [("[", nil), ("value", "q")] {
            do {
                _ = try SHACLRegularExpression(
                    pattern: pattern,
                    flags: flags
                )
                Issue.record("An invalid SHACL pattern was accepted")
            } catch let error as SHACLError {
                guard case .invalidPattern(let failedPattern, let reason) = error
                else {
                    Issue.record("Invalid syntax produced the wrong SHACL error")
                    continue
                }
                #expect(failedPattern == pattern)
                #expect(!reason.isEmpty)
            } catch {
                Issue.record("Invalid syntax produced an untyped error")
            }
        }
    }

    @Test("Pattern and NFA expansion limits remain typed")
    func compilationResourceLimits() {
        assertResourceLimit(
            pattern: String(
                repeating: "a",
                count: SPARQLExecutionLimits
                    .maximumRegularExpressionPatternUTF8Count + 1
            ),
            resource: "regularExpression.patternUTF8Bytes"
        )
        assertResourceLimit(
            pattern: "((a)){4096}",
            resource: "regularExpression.nfaStates"
        )
    }

    @Test("Input limits fail before matching oversized values")
    func inputResourceLimit() throws {
        let expression = try SHACLRegularExpression(
            pattern: "a",
            flags: nil
        )
        let input = String(
            repeating: "a",
            count: SPARQLExecutionLimits.maximumLiteralUTF8Count + 1
        )

        do {
            _ = try expression.matches(input)
            Issue.record("An oversized SHACL value was accepted")
        } catch let error as SHACLError {
            guard case .resourceLimitExceeded(
                let resource,
                let limit,
                let actual
            ) = error else {
                Issue.record("An oversized SHACL value produced the wrong error")
                return
            }
            #expect(resource == "regularExpression.inputUTF8Bytes")
            #expect(limit == SPARQLExecutionLimits.maximumLiteralUTF8Count)
            #expect(actual == limit + 1)
        } catch {
            Issue.record("An oversized SHACL value produced an untyped error")
        }
    }

    @Test(
        "Nested quantifiers cannot trigger catastrophic backtracking",
        .timeLimit(.minutes(1))
    )
    func nestedQuantifiersAreBounded() throws {
        let expression = try SHACLRegularExpression(
            pattern: "^(a+)+$",
            flags: nil
        )
        let input = String(repeating: "a", count: 50_000) + "!"

        #expect(try !expression.matches(input))
    }

    @Test("The validator converts only a non-match into a violation")
    func validatorMatchFlow() async throws {
        let conforming = try await validate(
            pattern: "^[a-z]+$",
            flags: "i",
            value: .string("Calendar")
        )
        let violation = try await validate(
            pattern: "^[a-z]+$",
            flags: nil,
            value: .string("Calendar")
        )

        #expect(conforming.conforms)
        #expect(conforming.results.isEmpty)
        #expect(!violation.conforms)
        #expect(violation.results.count == 1)
        #expect(
            violation.results[0].sourceConstraintComponent
                == "sh:PatternConstraintComponent"
        )
    }

    @Test("The validator propagates pattern resource failures")
    func validatorResourceFailureFlow() async throws {
        let pattern = String(
            repeating: "a",
            count: SPARQLExecutionLimits
                .maximumRegularExpressionPatternUTF8Count + 1
        )

        do {
            _ = try await validate(
                pattern: pattern,
                flags: nil,
                value: .string("calendar")
            )
            Issue.record("A SHACL resource failure became a report")
        } catch let error as SHACLError {
            guard case .resourceLimitExceeded(
                let resource,
                let limit,
                let actual
            ) = error else {
                Issue.record("The validator produced the wrong SHACL error")
                return
            }
            #expect(resource == "regularExpression.patternUTF8Bytes")
            #expect(
                limit == SPARQLExecutionLimits
                    .maximumRegularExpressionPatternUTF8Count
            )
            #expect(actual == limit + 1)
        } catch {
            Issue.record("The validator produced an untyped resource error")
        }
    }

    private func validate(
        pattern: String,
        flags: String?,
        value: RDFTerm
    ) async throws -> SHACLValidationReport {
        let engine = InMemoryEngine()
        let executor = SPARQLQueryExecutor(
            database: engine,
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(
                now: Timestamp(secondsSinceUnixEpoch: 0)
            ),
            sources: []
        )
        let budget = SHACLValidationWorkBudget(
            budget: ExecutionBudget(
                maximumRows: 1_000,
                maximumWorkUnits: 50_000_000,
                timeoutMilliseconds: 60_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let shapes = SHACLShapesGraph(
            iri: "urn:test:bounded-patterns",
            shapes: [
                .node(
                    NodeShape(
                        identifier: try .iri(
                            validating: "urn:test:bounded-pattern"
                        ),
                        constraints: [.pattern(pattern, flags: flags)]
                    )
                )
            ]
        )

        return try await engine.withTransaction { transaction in
            let targetResolver = SHACLTargetResolver(
                executor: executor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                budget: budget
            )
            let evaluator = SHACLConstraintEvaluator(
                executor: executor,
                transaction: transaction,
                dataGraph: .defaultGraph,
                budget: budget
            )
            let validator = SHACLValidator(
                shapesGraph: shapes,
                targetResolver: targetResolver,
                constraintEvaluator: evaluator,
                budget: budget
            )
            return try await validator.validate(focusNodes: [value])
        }
    }

    private func assertResourceLimit(
        pattern: String,
        resource: String
    ) {
        do {
            _ = try SHACLRegularExpression(pattern: pattern, flags: nil)
            Issue.record("A resource-exhausting SHACL pattern was accepted")
        } catch let error as SHACLError {
            guard case .resourceLimitExceeded(
                let actualResource,
                let limit,
                let actual
            ) = error else {
                Issue.record("A SHACL resource limit produced the wrong error")
                return
            }
            #expect(actualResource == resource)
            #expect(actual > limit)
        } catch {
            Issue.record("A SHACL resource limit produced an untyped error")
        }
    }
}
