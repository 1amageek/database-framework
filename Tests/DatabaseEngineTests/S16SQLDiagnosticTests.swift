import DatabaseKit
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("S16 typed SQL diagnostics", .timeLimit(.minutes(1)))
struct S16SQLDiagnosticTests {
    @Test("SQLSTATE and source position validate intrinsic bounds")
    func diagnosticValidation() throws {
        for state in ["2350", "235000", "23é00", "abcde", "23 05"] {
            #expect(throws: SQLDiagnosticError.invalidSQLState) {
                try SQLDiagnostic(sqlState: state, category: .constraint, message: "failure")
            }
        }
        #expect(throws: SQLDiagnosticError.invalidPosition) {
            try SQLDiagnostic(sqlState: "42601", category: .syntax, message: "failure", position: 0)
        }
        let diagnostic = try SQLDiagnostic(sqlState: "23505", category: .constraint,
            message: "failure", position: 1, schema: "名", table: "Table", column: "c", constraint: "positive")
        #expect(diagnostic.position == 1)
        #expect(diagnostic.schema == "名")
        #expect(diagnostic.table == "Table")
    }

    @Test("Constraint kind controls classification independently of the name")
    func constraintKinds() {
        let cases: [(SQLConstraintViolationKind, String)] = [
            (.notNull, "23502"), (.check, "23514"), (.unique, "23505"),
            (.foreignKey, "23503"), (.generatedValue, "428C9")]
        for (kind, state) in cases {
            let cause = SQLConstraintError.violation(kind: kind, relation: "opaque-id",
                constraint: "unique_check_fk", schema: "Schema", table: "Table", column: "Column")
            let diagnostic = SQLDiagnostics.diagnostic(for: cause)
            #expect(diagnostic.sqlState == state)
            #expect(diagnostic.category == .constraint)
            #expect(diagnostic.schema == "Schema")
            #expect(diagnostic.table == "Table")
            #expect(diagnostic.column == "Column")
            #expect(diagnostic.constraint == "unique_check_fk")
            #expect(diagnostic.position == nil)
        }
    }

    @Test("Name type cardinality grouping numeric regex and temporal cases classify")
    func semanticCases() {
        let cases: [(any Error, String)] = [
            (DatabaseExpressionEvaluationError.missingColumn("unknown"), "42703"),
            (DatabaseExpressionEvaluationError.ambiguousColumn("name"), "42702"),
            (DatabaseExpressionEvaluationError.typeMismatch(operation: "ADD"), "42804"),
            (SQLPreparedExecutionError.arity(name: "query", expected: 1, actual: 2), "42601"),
            (DatabaseExpressionEvaluationError.groupingViolation, "42803"),
            (DatabaseExpressionEvaluationError.windowingViolation, "42P20"),
            (DatabaseExpressionEvaluationError.invalidFunctionArity(function: "rank"), "42883"),
            (DatabaseAggregateEvaluationError.invalidGroupedExpression("group"), "42803"),
            (CanonicalReadError.invalidScalarSubquery(rowCount: 2, columnCount: 1), "21000"),
            (SQLMutationError.cardinalityViolation, "21000"),
            (DatabaseExpressionEvaluationError.divisionByZero, "22012"),
            (DatabaseExpressionEvaluationError.numericOverflow, "22003"),
            (DatabaseExpressionEvaluationError.invalidPattern(offset: 1, reason: "pattern"), "2201B"),
            (DatabaseExpressionEvaluationError.invalidTemporalInput, "22007"),
            (DatabaseExpressionEvaluationError.temporalValueOutOfRange, "22008"),
            (SQLCatalogBindingError.missingRelation(namespace: "名", name: "表"), "42P01")]
        for (cause, state) in cases { #expect(SQLDiagnostics.diagnostic(for: cause).sqlState == state) }
        let parameterCases: [(QueryParameterBindingError, String, SQLDiagnosticCategory)] = [
            (.missingPosition(1), "42P02", .name),
            (.missingName("missing"), "42P02", .name),
            (.invalidPosition(0), "22023", .data),
            (.invalidName, "22023", .data),
            (.duplicatePosition(1), "22023", .data),
            (.duplicateName("duplicate"), "22023", .data),
            (.unsupportedValue(.position(1)), "0A000", .unsupported),
            (.invalidStructure(.invalidSQLControl), "42601", .syntax),
            (.invalidStructure(.resourceLimitExceeded(resource: .totalNodes, actual: 2, maximum: 1)), "54000", .resource),
            (.invalidTraversalState, "XX000", .internalError)]
        for (cause, state, category) in parameterCases {
            let diagnostic = SQLDiagnostics.diagnostic(for: cause)
            #expect(diagnostic.sqlState == state)
            #expect(diagnostic.category == category)
            #expect(diagnostic.position == nil)
            let failure = SQLExecutionFailure(underlyingError: cause, diagnostic: diagnostic)
            #expect(failure.underlyingError as? QueryParameterBindingError == cause)
        }
    }

    @Test("Real binder branches retain typed grouping windowing and arity failures")
    func binderFailureBranches() throws {
        let binder = CanonicalQueryTypeBinder(columns: [], resolveColumn: { reference in
            throw DatabaseExpressionEvaluationError.missingColumn(reference.column)
        }, wildcardSlots: { _ in [] })
        let aggregate = Expression.aggregate(.sql(SQLAggregateCall(function: .sum, arguments: [.int(1)])))
        let window = Expression.window(WindowCall(function: .analytic(.rank, arguments: []),
            reference: .specification(WindowSpecification())))
        #expect(throws: DatabaseExpressionEvaluationError.groupingViolation) {
            try binder.bindPredicate(aggregate)
        }
        #expect(throws: DatabaseExpressionEvaluationError.windowingViolation) {
            try binder.bindPredicate(window)
        }
        #expect(throws: DatabaseExpressionEvaluationError.invalidFunctionArity(function: "sum")) {
            try binder.bind(.aggregate(.sql(SQLAggregateCall(function: .sum, arguments: []))))
        }
    }

    @Test("Storage serialization commit ambiguity cancellation limits and unsupported remain distinct")
    func runtimeCases() {
        let cases: [(any Error, String, SQLDiagnosticCategory)] = [
            (StorageError.transactionConflict, "40001", .serialization),
            (StorageError(code: .commitUnknownResult, message: "unknown"), "40003", .transaction),
            (CancellationError(), "57014", .cancellation),
            (SQLBlockExecutionError.statementTimeout, "57014", .cancellation),
            (StorageError(code: .transactionCancelled, message: "cancel"), "57014", .cancellation),
            (StorageError(code: .transactionTooLarge, message: "limit"), "54000", .resource),
            (DatabaseEntityMutationError.changeLimitExceeded(actual: 100, maximum: 99), "54000", .resource),
            (StorageError.unsupportedOperation("feature"), "0A000", .unsupported),
            (SecurityError(operation: .list, targetType: "Entity", reason: "denied"), "42501", .authorization),
            (QueryStructuralValidationError.invalidSQLControl, "42601", .syntax),
            (QueryStructuralValidationError.resourceLimitExceeded(resource: .totalNodes, actual: 2, maximum: 1), "54000", .resource)]
        for (cause, state, category) in cases {
            let diagnostic = SQLDiagnostics.diagnostic(for: cause)
            #expect(diagnostic.sqlState == state)
            #expect(diagnostic.category == category)
        }
    }

    @Test("Binding wrappers classify the actual cause without inventing a location")
    func bindingWrapper() {
        let cause = CanonicalSQLBindingFailure(cause: .expression(.missingColumn("不存在")), occurrence: nil, root: nil)
        let diagnostic = SQLDiagnostics.diagnostic(for: cause)
        #expect(diagnostic.sqlState == "42703")
        #expect(diagnostic.column == "不存在")
        #expect(diagnostic.position == nil)
    }

    @Test("Primary and every cleanup cause survive the diagnostic boundary")
    func retainedCleanup() throws {
        let primary = DiagnosticCause()
        let first = StorageError.backendError("first cleanup")
        let second = StorageError.backendError("second cleanup")
        let composite = StorageTransactionCleanupError(operationError: primary, cancellationError: first)
            .addingCancellationError(second)
        let failure = SQLExecutionFailure(underlyingError: composite, diagnostic: SQLDiagnostics.diagnostic(for: composite))
        let retained = try #require(failure.underlyingError as? StorageTransactionCleanupError)
        #expect((retained.operationError as? DiagnosticCause) === primary)
        #expect(retained.cancellationErrors.count == 2)
        #expect(retained.cancellationErrors[0] as? StorageError == first)
        #expect(retained.cancellationErrors[1] as? StorageError == second)
        #expect(failure.diagnostic.sqlState == "XX000")
    }

    @Test("Cancellation remains primary when mandatory cleanup also fails")
    func cancelledCleanup() throws {
        let cleanup = StorageError.backendError("cleanup")
        let cause = StorageTransactionCleanupError(operationError: CancellationError(), cancellationError: cleanup)
        let failure = SQLExecutionFailure(underlyingError: cause, diagnostic: SQLDiagnostics.diagnostic(for: cause))
        #expect(failure.diagnostic.sqlState == "57014")
        let retained = try #require(failure.underlyingError as? StorageTransactionCleanupError)
        #expect(retained.operationError is CancellationError)
        #expect(retained.cancellationErrors.first as? StorageError == cleanup)
    }

    @Test("Unknown failures stay explicit without reflection or message inference")
    func unknownFailure() {
        let diagnostic = SQLDiagnostics.diagnostic(for: DiagnosticCause())
        #expect(diagnostic.sqlState == "XX000")
        #expect(diagnostic.category == .internalError)
        #expect(diagnostic.position == nil)
    }
}

private final class DiagnosticCause: Error, Sendable {}
