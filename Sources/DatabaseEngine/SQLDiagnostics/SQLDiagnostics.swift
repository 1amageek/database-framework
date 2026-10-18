import DatabaseKit
import StorageKit

/// Classifies native cases without parsing descriptions or user-defined names.
public enum SQLDiagnostics {
    public static func diagnostic(for error: any Error) -> SQLDiagnostic {
        if let failure = error as? SQLExecutionFailure { return failure.diagnostic }
        if let failure = error as? StorageTransactionCleanupError { return diagnostic(for: failure.operationError) }
        if let failure = error as? StorageRangeCleanupError { return diagnostic(for: failure.iterationError) }
        if let failure = error as? StorageRangeTerminalCleanupError { return diagnostic(for: failure.cleanupError) }
        if let failure = error as? SQLRelationCleanupError { return diagnostic(for: failure.operationError) }
        if let failure = error as? SQLSequenceRequestCleanupError { return diagnostic(for: failure.operationError) }
        if let failure = error as? DatabaseReadSessionCleanupError { return diagnostic(for: failure.operationError) }
        if let failure = error as? CanonicalSQLBindingFailure {
            switch failure.cause {
            case .expression(let cause): return diagnostic(for: cause)
            case .read(let cause): return diagnostic(for: cause)
            case .catalog(let cause): return diagnostic(for: cause)
            case .resolution(let cause): return diagnostic(for: cause)
            }
        }
        if error is CancellationError { return make("57014", .cancellation, "SQL request was cancelled") }
        if error is TransactionExecutionDeadlineExceeded { return make("57014", .cancellation, "SQL request deadline exceeded") }
        if let error = error as? QueryParameterBindingError {
            switch error {
            case .missingPosition, .missingName:
                return make("42P02", .name, error.description)
            case .invalidPosition, .invalidName, .duplicatePosition, .duplicateName:
                return make("22023", .data, error.description)
            case .unsupportedValue:
                return make("0A000", .unsupported, error.description)
            case .invalidStructure(let cause):
                return diagnostic(for: cause)
            case .invalidTraversalState:
                return make("XX000", .internalError, error.description)
            }
        }
        if let error = error as? QueryStructuralValidationError {
            switch error {
            case .resourceLimitExceeded, .invalidResourceClaim: return make("54000", .resource, error.description)
            case .invalidQueryValue(let cause): return queryValue(cause)
            default: return make("42601", .syntax, error.description)
            }
        }
        if let error = error as? SQLConstraintError { return constraint(error) }
        if let error = error as? DatabaseExpressionEvaluationError { return expression(error) }
        if let error = error as? CanonicalReadError { return read(error) }
        if let error = error as? DatabaseAggregateEvaluationError {
            switch error {
            case .invalidGroupedExpression: return make("42803", .syntax, "Invalid grouped expression")
            case .numericOverflow, .countOverflow, .resultNotRepresentable, .nonFiniteValue:
                return make("22003", .numeric, "Aggregate numeric value is out of range")
            default: return make("42804", .type, "Invalid aggregate operand type")
            }
        }
        if let error = error as? SQLCatalogBindingError {
            switch error {
            case .missingNamespace(let name): return make("3F000", .name, "Unknown SQL schema", schema: name)
            case .missingRelation(let namespace, let name): return make("42P01", .name, "Unknown SQL relation", schema: namespace, table: name)
            case .missingField(let entity, _): return make("42703", .name, "Unknown physical field", table: entity)
            case .missingEntity(let name): return make("42P01", .name, "Unknown SQL entity", table: name)
            default: return make("XX000", .internalError, "SQL catalog binding failed")
            }
        }
        if let error = error as? SQLReadResolutionError {
            switch error {
            case .missingPhysicalColumn(let entity, let field): return make("42703", .name, "Unknown physical SQL column", table: entity, column: field)
            case .generationMismatch: return make("40001", .serialization, "SQL schema generation changed")
            }
        }
        if let error = error as? SQLPreparedExecutionError {
            switch error {
            case .duplicateName: return make("42P05", .name, "Prepared statement already exists")
            case .unknownName: return make("26000", .name, "Unknown prepared statement")
            case .arity: return make("42601", .syntax, "Prepared statement argument count does not match")
            case .argumentType: return make("42804", .type, "Prepared statement argument type does not match")
            case .indeterminateParameter: return make("42P18", .type, "Indeterminate parameter type")
            case .namedParameter: return make("0A000", .unsupported, "Named prepared parameters are unsupported")
            case .staleBinding: return make("0A000", .unsupported, "Prepared statement schema binding is stale")
            }
        }
        if let error = error as? SQLBlockExecutionError {
            switch error {
            case .statementTimeout: return make("57014", .cancellation, "SQL statement timeout exceeded")
            case .unsupportedStatement: return make("0A000", .unsupported, "Unsupported SQL statement")
            case .unknownCheckpoint: return make("3B001", .transaction, "Unknown savepoint")
            case .invalidTransactionControl: return make("25000", .transaction, "Invalid transaction control")
            case .invalidSettingValue: return make("22023", .data, "Invalid SQL setting value")
            case .mutationLimitOutOfRange: return make("54000", .resource, "SQL mutation limit exceeds runtime range")
            }
        }
        if let error = error as? SQLMutationError {
            switch error {
            case .cardinalityViolation: return make("21000", .cardinality, "SQL mutation affects a row more than once")
            case .unsupportedConflictInference, .unsupportedHiddenIdentifier, .deferrableConflictTarget:
                return make("0A000", .unsupported, "Unsupported SQL mutation feature")
            case .invalidStatement, .invalidConflictTarget: return make("42601", .syntax, "Invalid SQL mutation")
            case .identityRequestRequired, .identityReplayDiverged: return make("XX000", .internalError, "SQL identity execution failed")
            }
        }
        if let error = error as? SQLPatternError {
            switch error {
            case .invalidPattern: return make("2201B", .data, "Invalid regular expression")
            case .unsupportedPattern, .unsupportedFlag: return make("0A000", .unsupported, "Unsupported regular expression feature")
            case .meterMismatch: return make("XX000", .internalError, "SQL pattern meter mismatch")
            default: return make("22023", .data, "Invalid regular expression argument")
            }
        }
        if let error = error as? SQLTimeZoneError { return timeZone(error) }
        if error is SQLCollectionError { return make("22023", .data, "Invalid SQL collection argument") }
        if let error = error as? StorageError { return storage(error) }
        if let error = error as? DatabaseWorkLimitError {
            if case .deadline = error { return make("57014", .cancellation, "SQL request deadline exceeded") }
            return make("54000", .resource, "SQL request resource limit exceeded")
        }
        if error is SecurityError { return make("42501", .authorization, "SQL access denied") }
        if let error = error as? FieldSecurityError {
            switch error {
            case .readNotAllowed, .writeNotAllowed: return make("42501", .authorization, "SQL field access denied")
            case .unsupportedFieldValue: return make("0A000", .unsupported, "SQL field value cannot be adapted")
            }
        }
        if let error = error as? SQLSessionEvaluationError {
            if case .unauthenticatedPrincipal = error { return make("28000", .authorization, "SQL principal is unauthenticated") }
            return make("22023", .data, "SQL session context is unavailable or invalid")
        }
        if let error = error as? SQLCopyExecutionError {
            switch error {
            case .malformedInput, .columnCount: return make("22P04", .data, "Invalid COPY input")
            case .invalidUTF8: return make("22021", .data, "Invalid COPY text encoding")
            case .invalidTarget: return make("42809", .type, "Invalid COPY target")
            case .unknownRetainedInputSize: return make("54000", .resource, "COPY input size cannot be admitted")
            case .invalidStatementOutcome: return make("XX000", .internalError, "Invalid COPY statement outcome")
            }
        }
        if let error = error as? SQLDDLExecutionError {
            switch error {
            case .missingObject: return make("42704", .name, "SQL object does not exist")
            case .duplicateObject: return make("42710", .name, "SQL object already exists")
            case .dependentObject: return make("2BP01", .name, "Dependent SQL objects exist")
            case .invalidDefinition: return make("42601", .syntax, "Invalid SQL object definition")
            case .protectedObject: return make("42501", .authorization, "SQL object is protected")
            }
        }
        if let error = error as? SQLSequenceExecutionError {
            switch error {
            case .unknownSequence: return make("42P01", .name, "Unknown SQL sequence")
            case .sequenceAlreadyExists: return make("42P07", .name, "SQL sequence already exists")
            case .exhausted: return make("2200H", .numeric, "SQL sequence limit exceeded")
            case .invalidArguments, .invalidRestart: return make("22023", .data, "Invalid SQL sequence argument")
            case .currentValueUnavailable, .lastValueUnavailable: return make("55000", .transaction, "SQL sequence value is not yet defined")
            case .dependentIdentity: return make("2BP01", .name, "SQL sequence has dependent identity")
            default: return make("XX000", .internalError, "SQL sequence execution failed")
            }
        }
        if let error = error as? SQLViewExecutionError {
            switch error {
            case .readOnly(let name): return make("42809", .type, "SQL view is read-only", table: name)
            case .notPopulated(let name): return make("55000", .transaction, "Materialized view is not populated", table: name)
            case .missingDependency(let name): return make("42P01", .name, "SQL view dependency is missing", table: name)
            case .missingColumn(let relation, let column): return make("42703", .name, "SQL view column is missing", table: relation, column: column)
            case .incompatibleColumn(let relation, let column): return make("42804", .type, "SQL view column type is incompatible", table: relation, column: column)
            case .cyclicDependency, .invalidDefinition, .incompatibleReplacement: return make("42P16", .syntax, "Invalid SQL view definition")
            }
        }
        return make("XX000", .internalError, "Unclassified SQL execution failure")
    }

    private static func constraint(_ error: SQLConstraintError) -> SQLDiagnostic {
        switch error {
        case .violation(let kind, _, let constraint, let schema, let table, let column):
            let state: String
            switch kind {
            case .notNull: state = "23502"
            case .check: state = "23514"
            case .unique: state = "23505"
            case .foreignKey: state = "23503"
            case .generatedValue: state = "428C9"
            }
            return make(state, .constraint, "SQL constraint violation", schema: schema,
                table: table, column: column, constraint: constraint)
        case .unknownConstraint(let name): return make("42704", .name, "Unknown SQL constraint", constraint: name)
        case .notDeferrable(let name): return make("55000", .constraint, "SQL constraint is not deferrable", constraint: name)
        case .generatedAssignment(_, let column), .identityAssignment(_, let column):
            return make("428C9", .constraint, "SQL column cannot be assigned", column: column)
        case .invalidDeclaration: return make("42601", .syntax, "Invalid SQL constraint declaration")
        }
    }

    private static func read(_ error: CanonicalReadError) -> SQLDiagnostic {
        switch error {
        case .expressionEvaluation(let cause, _): return diagnostic(for: cause)
        case .aggregateEvaluation(let cause): return diagnostic(for: cause)
        case .invalidScalarSubquery, .invalidMembershipSubquery: return make("21000", .cardinality, "SQL subquery has invalid cardinality")
        case .incompatibleLiteralType: return make("42804", .type, "Incompatible SQL literal type")
        case .paginationValueExceedsRuntimeRange: return make("54000", .resource, "SQL pagination exceeds runtime range")
        case .unsupportedSource, .unsupportedSelectQuery, .unsupportedExpression, .unsupportedAccessPath, .executorNotRegistered:
            return make("0A000", .unsupported, "Unsupported SQL query feature")
        default: return make("XX000", .internalError, "SQL read execution failed")
        }
    }

    private static func expression(_ error: DatabaseExpressionEvaluationError) -> SQLDiagnostic {
        switch error {
        case .missingColumn(let name): return make("42703", .name, error.description, column: name)
        case .ambiguousColumn(let name): return make("42702", .name, error.description, column: name)
        case .unboundParameter: return make("42P02", .name, error.description)
        case .groupingViolation: return make("42803", .syntax, error.description)
        case .windowingViolation: return make("42P20", .syntax, error.description)
        case .invalidFunctionArity: return make("42883", .type, error.description)
        case .typeMismatch: return make("42804", .type, error.description)
        case .divisionByZero: return make("22012", .numeric, error.description)
        case .numericOverflow, .numericUnderflow, .nonFiniteValue, .inexactDecimalResult: return make("22003", .numeric, error.description)
        case .numericDomain: return make("22000", .numeric, error.description)
        case .unsupportedExpression, .unsupportedFunction, .unsupportedPattern: return make("0A000", .unsupported, error.description)
        case .invalidPattern: return make("2201B", .data, error.description)
        case .invalidCast: return make("22P02", .data, error.description)
        case .invalidTemporalInput: return make("22007", .data, error.description)
        case .temporalValueOutOfRange: return make("22008", .data, error.description)
        case .timeZone(let cause): return timeZone(cause)
        case .collection(let cause): return diagnostic(for: cause)
        case .queryValue(let cause): return queryValue(cause)
        case .missingTemporalEnvironment: return make("XX000", .internalError, error.description)
        case .invalidFunctionArgument, .invalidRDFLiteral: return make("22023", .data, error.description)
        }
    }

    private static func queryValue(_ error: QueryValueError) -> SQLDiagnostic {
        switch error {
        case .resourceLimit, .unknownRetainedByteCount: return make("54000", .resource, "SQL value admission exceeded")
        case .numericOutOfRange, .arithmeticOverflow: return make("22003", .numeric, "SQL numeric value is out of range")
        case .intervalOutOfRange, .timeOutOfRange: return make("22008", .data, "SQL temporal value is out of range")
        case .elementTypeMismatch, .incompatibleType: return make("42804", .type, "SQL value type is incompatible")
        default: return make("22023", .data, "Invalid SQL value")
        }
    }

    private static func timeZone(_ error: SQLTimeZoneError) -> SQLDiagnostic {
        switch error {
        case .admissionExceeded: return make("54000", .resource, "SQL time zone admission exceeded")
        case .unknownZone: return make("22023", .data, "Unknown SQL time zone")
        case .rangeUnavailable: return make("22008", .data, "SQL time zone range unavailable")
        case .rulesUnavailable, .unsupportedData: return make("0A000", .unsupported, "SQL time zone rules unavailable")
        case .malformedData: return make("XX000", .internalError, "SQL time zone rules are malformed")
        }
    }

    private static func storage(_ error: StorageError) -> SQLDiagnostic {
        switch error.code {
        case .transactionConflict, .transactionTooOld, .transactionFutureVersion, .transactionBusy:
            return make("40001", .serialization, error.message)
        case .commitUnknownResult: return make("40003", .transaction, error.message)
        case .transactionCancelled, .transactionTimedOut: return make("57014", .cancellation, error.message)
        case .transactionTooLarge, .keyTooLarge, .valueTooLarge, .resourceUnavailable:
            return make("54000", .resource, error.message)
        case .unsupportedOperation: return make("0A000", .unsupported, error.message)
        case .connectionFailure: return make("08006", .transaction, error.message)
        default: return make("XX000", .internalError, error.message)
        }
    }

    private static func make(_ state: String, _ category: SQLDiagnosticCategory, _ message: String,
        schema: String? = nil, table: String? = nil, column: String? = nil, constraint: String? = nil) -> SQLDiagnostic {
        do {
            return try SQLDiagnostic(sqlState: state, category: category, message: message,
                schema: schema, table: table, column: column, constraint: constraint)
        } catch {
            preconditionFailure("SQLDiagnostics contains an invalid static SQLSTATE")
        }
    }
}
