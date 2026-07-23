import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import QueryIR
import Synchronization
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL ORDER BY decoration", .heartbeat)
struct BindingSorterDecorationTests {
    @Test("Direct binding fingerprints match canonical row fingerprints")
    func directFingerprintMatchesCanonicalRow() throws {
        let rdfTerm = DatabaseRDFTerm.tripleTerm(
            subject: .iri("urn:subject"),
            predicate: .iri("urn:predicate"),
            object: .literal(
                DatabaseRDFLiteral(
                    lexicalForm: "calendar",
                    datatype: DatabaseXSDDatatype.string.typedLiteralDatatype
                )
            )
        )
        let fields: [String: FieldValue] = [
            "?null": .null,
            "?bool": .bool(true),
            "?signed": .int64(-42),
            "?unsigned": .uint64(UInt64.max),
            "?double": .double(-0.0),
            "?string": .string("予定📅"),
            "?bytes": .data(DatabaseBytes([0x00, 0x80, 0xFF])),
            "?array": .array([
                .string("nested"),
                .array([.int64(7), .null])
            ]),
            "?rdf": .rdfTerm(rdfTerm)
        ]
        let binding = VariableBinding(fields)
        let directMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget()
        )
        let canonicalMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget()
        )

        let direct = try binding.canonicalFingerprint(workMeter: directMeter)
        let canonical = try CanonicalRowFingerprint.compute(
            QueryRow(fields: fields.mapValues(\.asDatabaseValue)),
            workMeter: canonicalMeter
        )

        #expect(direct == canonical)
        #expect(direct.count == 32)
        #expect(
            directMeter.consumedWorkUnits
                == canonicalMeter.consumedWorkUnits
        )
    }

    @Test("Fingerprint is independent of dictionary insertion order")
    func fingerprintUsesCanonicalVariableOrder() throws {
        var ascending: [String: FieldValue] = [:]
        ascending["?a"] = .int64(1)
        ascending["?b"] = .string("two")
        ascending["?c"] = .bool(true)

        var descending: [String: FieldValue] = [:]
        descending["?c"] = .bool(true)
        descending["?b"] = .string("two")
        descending["?a"] = .int64(1)

        let first = try VariableBinding(ascending).canonicalFingerprint(
            workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
        )
        let second = try VariableBinding(descending).canonicalFingerprint(
            workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
        )

        #expect(first == second)
    }

    @Test("Fingerprint preserves numeric value types")
    func fingerprintPreservesValueType() throws {
        func fingerprint(_ value: FieldValue) throws -> DatabaseBytes {
            try VariableBinding(["?value": value]).canonicalFingerprint(
                workMeter: DatabaseWorkMeter(
                    budget: DatabaseExecutionBudget()
                )
            )
        }

        let signed = try fingerprint(.int64(1))
        let unsigned = try fingerprint(.uint64(1))
        let floatingPoint = try fingerprint(.double(1))

        #expect(signed != unsigned)
        #expect(signed != floatingPoint)
        #expect(unsigned != floatingPoint)
    }

    @Test("Owned byte values are borrowed once while fingerprinting")
    func fingerprintBorrowsByteOwnerOnce() throws {
        let owner = BindingFingerprintBorrowCountingOwner(
            bytes: [0x00, 0x01, 0x80, 0xFF]
        )
        let binding = VariableBinding([
            "?payload": .array([
                .data(DatabaseBytes(retaining: owner)),
                .string("suffix")
            ])
        ])

        let fingerprint = try binding.canonicalFingerprint(
            workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
        )

        #expect(fingerprint.count == 32)
        #expect(owner.borrowCount == 1)
    }

    @Test("Each ORDER BY expression is evaluated once per row")
    func evaluatesEachKeyOncePerRow() throws {
        let evaluationCount = Mutex(0)
        let bindings = [
            VariableBinding(["?value": .int64(3)]),
            VariableBinding(["?value": .int64(1)]),
            VariableBinding(["?value": .int64(2)])
        ]
        let key = BindingSortKey { binding in
            evaluationCount.withLock { $0 += 1 }
            return binding["?value"]
        }

        let sorted = try BindingSorter.sort(
            bindings,
            by: [key],
            workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
        )

        #expect(evaluationCount.withLock { $0 } == bindings.count)
        #expect(sorted.compactMap { $0["?value"]?.int64Value } == [1, 2, 3])
    }

    @Test("ORDER BY releases its scratch reservation after sorting")
    func releasesScratchReservationAfterSorting() throws {
        let bindings = [
            VariableBinding(["?value": .int64(3)]),
            VariableBinding(["?value": .int64(1)]),
            VariableBinding(["?value": .int64(2)])
        ]
        let workMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 4_096
            )
        )

        let sorted = try BindingSorter.sort(
            bindings,
            by: [.variable("?value")],
            workMeter: workMeter
        )

        #expect(sorted.compactMap { $0["?value"]?.int64Value } == [1, 2, 3])
        #expect(workMeter.peakIntermediateRows == 3)
        #expect(workMeter.peakIntermediateBytes > 0)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Scratch rejection occurs before key evaluation")
    func scratchRejectionPrecedesKeyEvaluation() {
        let evaluationCount = Mutex(0)
        let bindings = [
            VariableBinding(["?value": .int64(2)]),
            VariableBinding(["?value": .int64(1)])
        ]
        let original = bindings
        let key = BindingSortKey { binding in
            evaluationCount.withLock { $0 += 1 }
            return binding["?value"]
        }
        let workMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumIntermediateRows: 2,
                maximumIntermediateBytes: 1
            )
        )

        do {
            _ = try BindingSorter.sort(
                bindings,
                by: [key],
                workMeter: workMeter
            )
            Issue.record("An oversized ORDER BY scratch plan was accepted")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(let stage, _, _, _) = error else {
                Issue.record("ORDER BY produced the wrong work-limit error")
                return
            }
            #expect(stage == .sortInput)
        } catch {
            Issue.record("ORDER BY produced an untyped scratch error: \(error)")
        }

        #expect(evaluationCount.withLock { $0 } == 0)
        #expect(bindings == original)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Async ORDER BY uses and releases the same scratch ledger")
    func asyncSortReleasesScratchReservation() async throws {
        let expression = try SPARQLExpressionPlan(
            .variable(Variable("value"))
        )
        let key = SPARQLOrderKeyPlan(
            expression: expression,
            ascending: true,
            nullsLast: false
        )
        let bindings = [
            VariableBinding(["?value": .int64(3)]),
            VariableBinding(["?value": .int64(1)]),
            VariableBinding(["?value": .int64(2)])
        ]
        let evaluationCount = Mutex(0)
        let workMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumIntermediateRows: 3,
                maximumIntermediateBytes: 4_096
            )
        )

        let sorted = try await BindingSorter.sort(
            bindings,
            by: [key],
            workMeter: workMeter
        ) { _, binding in
            evaluationCount.withLock { $0 += 1 }
            return binding["?value"]
        }

        #expect(evaluationCount.withLock { $0 } == bindings.count)
        #expect(sorted.compactMap { $0["?value"]?.int64Value } == [1, 2, 3])
        #expect(workMeter.peakIntermediateRows == 3)
        #expect(workMeter.peakIntermediateBytes > 0)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Async scratch rejection occurs before expression evaluation")
    func asyncScratchRejectionPrecedesEvaluation() async throws {
        let expression = try SPARQLExpressionPlan(
            .variable(Variable("value"))
        )
        let key = SPARQLOrderKeyPlan(
            expression: expression,
            ascending: true,
            nullsLast: false
        )
        let bindings = [
            VariableBinding(["?value": .int64(2)]),
            VariableBinding(["?value": .int64(1)])
        ]
        let original = bindings
        let evaluationCount = Mutex(0)
        let workMeter = DatabaseWorkMeter(
            budget: DatabaseExecutionBudget(
                maximumIntermediateRows: 2,
                maximumIntermediateBytes: 1
            )
        )

        do {
            _ = try await BindingSorter.sort(
                bindings,
                by: [key],
                workMeter: workMeter
            ) { _, binding in
                evaluationCount.withLock { $0 += 1 }
                return binding["?value"]
            }
            Issue.record("An oversized async ORDER BY scratch plan was accepted")
        } catch let error as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(let stage, _, _, _) = error else {
                Issue.record("Async ORDER BY produced the wrong work-limit error")
                return
            }
            #expect(stage == .sortInput)
        } catch {
            Issue.record("Async ORDER BY produced an untyped scratch error: \(error)")
        }

        #expect(evaluationCount.withLock { $0 } == 0)
        #expect(bindings == original)
        #expect(workMeter.retainedIntermediateRows == 0)
        #expect(workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Semantic expression errors become unbound ordering keys")
    func expressionErrorBecomesUnboundKey() throws {
        let expression = QueryIR.Expression.variable(Variable("missing"))
        let key = BindingSortKey { binding in
            try ExpressionEvaluator.evaluateForOrdering(
                expression,
                binding: binding
            )
        }

        let sorted = try BindingSorter.sort(
            [VariableBinding(), VariableBinding(["?other": .string("value")])],
            by: [key],
            workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
        )

        #expect(sorted.count == 2)
    }

    @Test("Runtime failures abort ORDER BY")
    func runtimeFailurePropagates() {
        let key = BindingSortKey { _ in
            throw SPARQLExpressionEvaluationError.resourceLimitExceeded(
                stage: "test",
                required: 2,
                maximum: 1
            )
        }

        #expect(throws: SPARQLExpressionEvaluationError.self) {
            try BindingSorter.sort(
                [VariableBinding(), VariableBinding()],
                by: [key],
                workMeter: DatabaseWorkMeter(budget: DatabaseExecutionBudget())
            )
        }
    }
}

private final class BindingFingerprintBorrowCountingOwner: DatabaseByteOwner {
    private let bytes: [UInt8]
    private let state = Mutex(0)

    init(bytes: [UInt8]) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    var borrowCount: Int {
        state.withLock { $0 }
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        state.withLock { $0 += 1 }
        try bytes.withUnsafeBytes(body)
    }
}
