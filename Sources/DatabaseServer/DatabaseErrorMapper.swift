import Core
import DatabaseEngine
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import OntologyIndex
import QueryAST
import QueryIR
import RelationshipIndex
import StorageKit

public protocol DatabaseErrorMapper: Sendable {
    func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> DatabaseRemoteError
}

extension DatabaseErrorMapper {
    public func remoteError(
        for error: any Error,
        context: DatabaseOperationContext
    ) -> DatabaseRemoteError {
        remoteError(for: error, context: context, limits: .default)
    }
}

public struct CanonicalDatabaseErrorMapper: DatabaseErrorMapper {
    public init() {}

    public func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> DatabaseRemoteError {
        remoteError(
            for: error,
            context: context,
            limits: limits,
            mappingDepth: 0
        )
    }

    private func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits,
        mappingDepth: Int
    ) -> DatabaseRemoteError {
        let mappingDepthLimit = max(0, limits.maximumNestingDepth / 2)
        guard mappingDepth <= mappingDepthLimit else {
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "ERROR_MAPPING_DEPTH_EXCEEDED",
                message: "Nested error mapping exceeded the configured depth",
                retryability: .never
            )
        }
        if let remote = error as? DatabaseRemoteError {
            return remote
        }
        if let cleanupError = error as? StorageTransactionCleanupError {
            return makeRemoteError(
                for: cleanupError,
                context: context,
                limits: limits,
                mappingDepth: mappingDepth
            )
        }
        if let responseError = error as? DatabaseResponsePreparationError {
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "RESPONSE_RESOURCE_LIMIT",
                message: responseError.description,
                retryability: .never
            )
        }
        if error is DatabaseWireError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_WIRE_PAYLOAD",
                message: String(describing: error),
                retryability: .never
            )
        }
        if error is DatabaseRecordDecodingError || error is QueryRowCodecError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_RECORD",
                message: String(describing: error),
                retryability: .never
            )
        }
        if let limitError = error as? DatabaseRuntimeLimitError {
            if case .executionTimedOut(let timeoutMilliseconds) = limitError {
                return DatabaseRemoteError(
                    category: .resourceLimit,
                    code: "EXECUTION_TIMED_OUT",
                    message: limitError.description,
                    retryability: .never,
                    details: [
                        DatabaseObjectField(
                            number: 1,
                            name: "timeoutMilliseconds",
                            value: .uint64(UInt64(timeoutMilliseconds))
                        ),
                    ]
                )
            }
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "RESOURCE_LIMIT",
                message: limitError.description,
                retryability: .never
            )
        }
        if let deadlineError = error as? TransactionExecutionDeadlineExceeded {
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "EXECUTION_TIMED_OUT",
                message: deadlineError.description,
                retryability: .never,
                details: [
                    DatabaseObjectField(
                        number: 1,
                        name: "timeoutMilliseconds",
                        value: .uint64(deadlineError.timeoutMilliseconds)
                    ),
                ]
            )
        }
        if let workLimitError = error as? DatabaseWorkLimitError {
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "QUERY_RESOURCE_LIMIT",
                message: workLimitError.description,
                retryability: .never
            )
        }
        if let mutationByteError = error as? TransactionMutationByteLimitError {
            return Self.map(mutationByteError)
        }
        if let storageLimitError = error as? DatabaseStorageLimitError {
            return Self.map(storageLimitError)
        }
        if let queryError = error as? DatabaseQueryExecutionError {
            return Self.map(queryError)
        }
        if let datasetError = error as? RDFDatasetValidationError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_SPARQL_DATASET",
                message: datasetError.description,
                retryability: .never
            )
        }
        if let conversionError = error as? GraphPatternConversionError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_GRAPH_PATTERN",
                message: String(describing: conversionError),
                retryability: .never
            )
        }
        if let planError = error as? SPARQLSelectPlanCompilationError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_SPARQL_SELECT_PLAN",
                message: String(describing: planError),
                retryability: .never
            )
        }
        if let compilationError = error as? SPARQLExpressionCompilationError {
            let category: DatabaseErrorCategory
            let code: String
            switch compilationError {
            case .resourceLimitExceeded:
                category = .resourceLimit
                code = "QUERY_RESOURCE_LIMIT"
            default:
                category = .invalidRequest
                code = "INVALID_SPARQL_EXPRESSION"
            }
            return DatabaseRemoteError(
                category: category,
                code: code,
                message: compilationError.description,
                retryability: .never
            )
        }
        if let literalError = error as? SPARQLLiteralConversionError {
            return Self.map(literalError)
        }
        if let expressionError = error as? SPARQLExpressionEvaluationError {
            return Self.map(expressionError)
        }
        if let sparqlError = error as? SPARQLQueryError {
            return Self.map(sparqlError)
        }
        if let scannerError = error as? RDFDatasetScannerError {
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "CORRUPTED_RDF_INDEX",
                message: String(describing: scannerError),
                retryability: .never
            )
        }
        if let validationFailure = error as? XSDValidationFailure {
            return Self.map(validationFailure)
        }
        if let graphQueryError = error as? DatabaseGraphQueryError {
            return Self.map(graphQueryError)
        }
        if let graphError = error as? DatabaseGraphAlgorithmError {
            return Self.map(graphError)
        }
        if error is QueryParameterBindingError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_QUERY_PARAMETER",
                message: String(describing: error),
                retryability: .never
            )
        }
        if let structuralError = error as? QueryStructuralValidationError {
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "QUERY_RESOURCE_LIMIT",
                message: structuralError.description,
                retryability: .never
            )
        }
        if let semanticError = error as? SPARQLSemanticValidationError {
            let category: DatabaseErrorCategory
            let code: String
            switch semanticError {
            case .structural:
                category = .resourceLimit
                code = "QUERY_RESOURCE_LIMIT"
            default:
                category = .invalidRequest
                code = "INVALID_SPARQL_SEMANTICS"
            }
            return DatabaseRemoteError(
                category: category,
                code: code,
                message: semanticError.description,
                retryability: .never
            )
        }
        if error is SQLParser.ParseError || error is SPARQLParser.ParseError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_QUERY_SYNTAX",
                message: String(describing: error),
                retryability: .never
            )
        }
        if let loadError = error as? SPARQLLoadSourceError {
            return Self.map(loadError)
        }
        if let updateError = error as? SPARQLUpdateError {
            return Self.map(updateError)
        }
        if let graphStoreError = error as? RDFGraphStoreError {
            return Self.map(graphStoreError)
        }
        if let mutationError = error as? DatabaseMutationError {
            return Self.map(mutationError)
        }
        if let jobError = error as? DatabaseJobRuntimeError {
            return Self.map(jobError)
        }
        if let maintenanceError = error as? DatabaseMaintenanceRuntimeError {
            return Self.map(maintenanceError)
        }
        if let indexError = error as? DatabaseIndexRebuildError {
            return Self.map(indexError)
        }
        if let catalogError = error as? DatabasePartitionCatalogError {
            return Self.map(catalogError)
        }
        if let compactionError = error as? DatabaseStorageCompactionError {
            return Self.map(compactionError)
        }
        if let registryError = error as? DatabaseResumableOperationRegistryError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "JOB_OPERATION_NOT_RESUMABLE",
                message: registryError.description,
                retryability: .never
            )
        }
        if let commandError = error as? DatabaseCommandRegistryError {
            return Self.map(commandError)
        }
        if let documentError = error as? DatabaseRDFDocumentStoreError {
            return Self.map(documentError)
        }
        if let ontologyError = error as? DatabaseOntologyProcessingError {
            return Self.map(ontologyError)
        }
        if let shaclError = error as? DatabaseSHACLValidationError {
            return Self.map(shaclError)
        }
        if let shaclValidationError = error as? SHACLError {
            return Self.map(shaclValidationError)
        }
        if let dataSourceError = error as? DatabaseSHACLDataSourceError {
            return Self.map(dataSourceError)
        }
        if let expressionError = error as? DatabaseExpressionEvaluationError {
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_MUTATION_EXPRESSION",
                message: expressionError.description,
                retryability: .never
            )
        }
        if error is SecurityError {
            return DatabaseRemoteError(
                category: .authorization,
                code: "ACCESS_DENIED",
                message: String(describing: error),
                retryability: .never
            )
        }
        if error is RelationshipError || error is RelationshipReferenceError {
            return DatabaseRemoteError(
                category: .constraint,
                code: "RELATIONSHIP_CONSTRAINT",
                message: String(describing: error),
                retryability: .never
            )
        }
        if let storageError = error as? StorageError {
            let retryability: DatabaseRetryability
            switch storageError.retryDisposition {
            case .safe:
                retryability = .backoff
            case .requiresIdempotency:
                retryability = .immediate
            case .never:
                retryability = .never
            }
            let category: DatabaseErrorCategory
            switch storageError.code {
            case .transactionTooLarge, .keyTooLarge, .valueTooLarge:
                category = .resourceLimit
            case .backendFailure, .backendContractViolation, .dataCorruption:
                category = .internalFailure
            default:
                category = .unavailable
            }
            var details: [DatabaseObjectField] = []
            if let backendCode = storageError.backendCode {
                details.append(DatabaseObjectField(
                    number: 1,
                    name: "backendCode",
                    value: .int64(Int64(backendCode))
                ))
            }
            if let byteLimitViolation = storageError.byteLimitViolation {
                details.append(DatabaseObjectField(
                    number: 2,
                    name: "observedByteCount",
                    value: .uint64(byteLimitViolation.observedByteCount)
                ))
                details.append(DatabaseObjectField(
                    number: 3,
                    name: "maximumByteCount",
                    value: .uint64(byteLimitViolation.maximumByteCount)
                ))
                details.append(DatabaseObjectField(
                    number: 4,
                    name: "resource",
                    value: .string(byteLimitViolation.resource.rawValue)
                ))
                details.append(DatabaseObjectField(
                    number: 5,
                    name: "measurement",
                    value: .string(byteLimitViolation.measurement.rawValue)
                ))
            }
            return DatabaseRemoteError(
                category: category,
                code: storageError.code.rawValue.uppercased(),
                message: storageError.description,
                retryability: retryability,
                details: details
            )
        }
        if let readError = error as? CanonicalReadError {
            return Self.map(readError)
        }
        if let contextError = error as? FDBContextError,
           case .preconditionFailed = contextError {
            return DatabaseRemoteError(
                category: .conflict,
                code: "PRECONDITION_FAILED",
                message: String(describing: contextError),
                retryability: .never
            )
        }
        return DatabaseRemoteError(
            category: .internalFailure,
            code: "SERVER_FAILURE",
            message: String(describing: error),
            retryability: .never
        )
    }

    private func makeRemoteError(
        for error: StorageTransactionCleanupError,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits,
        mappingDepth: Int
    ) -> DatabaseRemoteError {
        let operationError = remoteError(
            for: error.operationError,
            context: context,
            limits: limits,
            mappingDepth: mappingDepth + 1
        )
        let maximumCancellationErrors = max(
            0,
            min(
                limits.maximumCollectionCount,
                limits.maximumObjectCount / 6
            )
        )
        var cancellationErrors: [DatabaseRemoteError] = []
        cancellationErrors.reserveCapacity(
            min(error.cancellationErrors.count, maximumCancellationErrors)
        )
        for cancellationError in error.cancellationErrors
            .prefix(maximumCancellationErrors) {
            cancellationErrors.append(
                remoteError(
                    for: cancellationError,
                    context: context,
                    limits: limits,
                    mappingDepth: mappingDepth + 1
                )
            )
        }
        return DatabaseRemoteError(
            category: .internalFailure,
            code: "TRANSACTION_CLEANUP_FAILURE",
            message: "Transaction operation and cancellation both failed",
            retryability: .never,
            details: [
                DatabaseObjectField(
                    number: 1,
                    name: "operationError",
                    value: .object(Self.fields(for: operationError))
                ),
                DatabaseObjectField(
                    number: 2,
                    name: "cancellationErrors",
                    value: .array(
                        cancellationErrors.map {
                            .object(Self.fields(for: $0))
                        }
                    )
                ),
            ]
        )
    }

    private static func fields(
        for error: DatabaseRemoteError
    ) -> [DatabaseObjectField] {
        [
            DatabaseObjectField(
                number: 1,
                name: "category",
                value: .uint64(UInt64(error.category.rawValue))
            ),
            DatabaseObjectField(
                number: 2,
                name: "code",
                value: .string(error.code)
            ),
            DatabaseObjectField(
                number: 3,
                name: "message",
                value: .string(error.message)
            ),
            DatabaseObjectField(
                number: 4,
                name: "retryability",
                value: .uint64(UInt64(error.retryability.rawValue))
            ),
            DatabaseObjectField(
                number: 5,
                name: "details",
                value: .object(error.details)
            ),
        ]
    }

    private static func map(
        _ error: TransactionMutationByteLimitError
    ) -> DatabaseRemoteError {
        switch error {
        case .exceeded(let actual, let maximum):
            guard let actualBytes = UInt64(exactly: actual),
                  let maximumBytes = UInt64(exactly: maximum) else {
                return DatabaseRemoteError(
                    category: .internalFailure,
                    code: "MUTATION_ADMISSION_INVARIANT_VIOLATION",
                    message: error.description,
                    retryability: .never
                )
            }
            return DatabaseRemoteError(
                category: .resourceLimit,
                code: "MUTATION_AGGREGATE_TOO_LARGE",
                message: error.description,
                retryability: .never,
                details: [
                    DatabaseObjectField(
                        number: 1,
                        name: "actualBytes",
                        value: .uint64(actualBytes)
                    ),
                    DatabaseObjectField(
                        number: 2,
                        name: "maximumBytes",
                        value: .uint64(maximumBytes)
                    ),
                ]
            )
        case .invalidMaximum:
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "MUTATION_ADMISSION_CONFIGURATION_FAILURE",
                message: error.description,
                retryability: .never
            )
        case .alreadyConfigured, .configurationAfterAdmission:
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "MUTATION_ADMISSION_STATE_FAILURE",
                message: error.description,
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseStorageLimitError
    ) -> DatabaseRemoteError {
        let code: String
        let actual: Int
        let maximum: Int
        switch error {
        case .keyTooLarge(let size, let limit):
            code = "KEY_TOO_LARGE"
            actual = size
            maximum = limit
        case .valueTooLarge(let size, let limit):
            code = "VALUE_TOO_LARGE"
            actual = size
            maximum = limit
        }
        guard let actualBytes = UInt64(exactly: actual),
              let maximumBytes = UInt64(exactly: maximum) else {
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "STORAGE_LIMIT_INVARIANT_VIOLATION",
                message: error.description,
                retryability: .never
            )
        }
        return DatabaseRemoteError(
            category: .resourceLimit,
            code: code,
            message: error.description,
            retryability: .never,
            details: [
                DatabaseObjectField(
                    number: 1,
                    name: "actualBytes",
                    value: .uint64(actualBytes)
                ),
                DatabaseObjectField(
                    number: 2,
                    name: "maximumBytes",
                    value: .uint64(maximumBytes)
                ),
            ]
        )
    }

    private static func map(
        _ error: SPARQLLoadSourceError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        let retryability: DatabaseRetryability
        switch error {
        case .notConfigured:
            category = .internalFailure
            code = "SPARQL_LOAD_NOT_CONFIGURED"
            retryability = .never
        case .sourceNotFound:
            category = .notFound
            code = "SPARQL_LOAD_SOURCE_NOT_FOUND"
            retryability = .never
        case .accessDenied:
            category = .authorization
            code = "SPARQL_LOAD_ACCESS_DENIED"
            retryability = .never
        case .unsupportedMediaType, .invalidDocument:
            category = .invalidRequest
            code = "INVALID_SPARQL_LOAD_DOCUMENT"
            retryability = .never
        case .transportFailure:
            category = .unavailable
            code = "SPARQL_LOAD_SOURCE_UNAVAILABLE"
            retryability = .backoff
        case .documentTooLarge, .tripleLimitExceeded:
            category = .resourceLimit
            code = "SPARQL_LOAD_RESOURCE_LIMIT"
            retryability = .never
        case .internalFailure:
            category = .internalFailure
            code = "SPARQL_LOAD_SOURCE_FAILURE"
            retryability = .never
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: retryability
        )
    }

    private static func map(
        _ error: SPARQLUpdateError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .unresolvedPrefixedName, .variableInGroundData,
             .blankNodeNotAllowed, .nonRDFBinding, .invalidRDFTermRole:
            category = .invalidRequest
            code = "INVALID_SPARQL_UPDATE"
        case .effectCountOverflow:
            category = .internalFailure
            code = "SPARQL_UPDATE_RUNTIME_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: RDFGraphStoreError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .graphAlreadyExists:
            category = .conflict
            code = "RDF_GRAPH_ALREADY_EXISTS"
        case .graphNotFound:
            category = .notFound
            code = "RDF_GRAPH_NOT_FOUND"
        case .keyTooLarge:
            category = .resourceLimit
            code = "RDF_GRAPH_RESOURCE_LIMIT"
        case .invalidQuad, .invalidTermEncoding:
            category = .invalidRequest
            code = "INVALID_RDF_GRAPH_MUTATION"
        case .invalidPhysicalIndex, .catalogPrefixMismatch,
             .catalogTruncatedKey, .catalogUnexpectedTupleType,
             .catalogTrailingTupleData, .invalidCatalogGraph,
             .invalidCatalogGraphName, .invalidCatalogMarker,
             .missingCatalogForStoredQuad, .quadCountOverflow:
            category = .internalFailure
            code = "RDF_GRAPH_STORE_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseQueryExecutionError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .rdfLiteralTooLarge:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .pageLimitMustBePositive, .solutionModifierMustBeNonNegative,
             .continuationNotSupported,
             .mutationRequiresMutationOperation, .unresolvedConstructTerm,
             .nonRDFBinding, .invalidRDFTermRole,
             .invalidRDFLiteralDatatype, .unsupportedRDFLiteral,
             .reifiedTripleRequiresTemplateContext,
             .describeVariableRequiresPattern, .invalidDescribeResource:
            category = .invalidRequest
            code = "INVALID_QUERY"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: SPARQLLiteralConversionError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .literalTooLarge:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .nullTermUnsupported, .arrayTermUnsupported,
             .invalidLexicalForm:
            category = .invalidRequest
            code = "INVALID_RDF_LITERAL"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: .never
        )
    }

    private static func map(
        _ error: SPARQLExpressionEvaluationError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .resourceLimitExceeded:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .runtimeInvariant:
            category = .internalFailure
            code = "SPARQL_RUNTIME_FAILURE"
        case .unsupportedExpression:
            category = .invalidRequest
            code = "UNSUPPORTED_SPARQL_EXPRESSION"
        case .unboundVariable, .typeError, .invalidFunctionArguments,
             .invalidRegularExpression:
            category = .invalidRequest
            code = "INVALID_SPARQL_EXPRESSION"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: SPARQLQueryError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .propertyPathExpressionDepthLimitExceeded,
             .propertyPathTraversalDepthLimitExceeded,
             .propertyPathResultLimitExceeded,
             .aggregateResultOutOfRange:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .executionFailed, .invalidOntologyPredicateIRI:
            category = .internalFailure
            code = "SPARQL_RUNTIME_FAILURE"
        case .indexNotConfigured, .indexNotFound, .invalidPattern,
             .variableConflict, .noPatterns, .invalidGroupBy, .invalidVariable,
             .invalidPagination, .invalidRDFTerm, .invalidGraphBinding,
             .invalidPropertyPathConfiguration:
            category = .invalidRequest
            code = "INVALID_SPARQL_QUERY"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: XSDValidationFailure
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .resourceLimitExceeded:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .invalidLexicalForm, .unsupportedDatatype, .invalidRestriction:
            category = .invalidRequest
            code = "INVALID_XSD_VALUE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(_ error: DatabaseMutationError) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .emptyMutation:
            category = .invalidRequest
            code = "EMPTY_MUTATION"
        case .recordNotFound:
            category = .notFound
            code = "RECORD_NOT_FOUND"
        case .recordAlreadyExists, .recordVersionMismatch, .idempotencyKeyConflict:
            category = .conflict
            code = "MUTATION_CONFLICT"
        case .mutationLimitExceeded, .preconditionLimitExceeded,
             .idempotencyKeyTooLarge, .relationshipWorkLimitExceeded:
            category = .resourceLimit
            code = "MUTATION_LIMIT"
        case .idempotencyRecordCorrupted, .logicalVersionOverflow,
             .stateStoreContainerMismatch, .statementExecutorNotConfigured,
             .relationshipCatalogCorrupted:
            category = .internalFailure
            code = "MUTATION_RUNTIME_FAILURE"
        case .relationshipMutationConflict:
            category = .conflict
            code = "MUTATION_CONFLICT"
        case .relationshipTargetNotFound:
            category = .constraint
            code = "RELATIONSHIP_CONSTRAINT"
        default:
            category = .invalidRequest
            code = "INVALID_MUTATION"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseJobRuntimeError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .jobNotFound:
            category = .notFound
            code = "JOB_NOT_FOUND"
        case .resultNotReady:
            category = .conflict
            code = "JOB_RESULT_NOT_READY"
        case .invalidRetryPolicy, .requestPayloadTooLarge,
             .jobOperationMismatch,
             .invalidResultContinuation:
            category = .invalidRequest
            code = "INVALID_JOB_REQUEST"
        case .sliceExceededBudget, .responseTooLarge,
             .specificationTooLarge, .planTooLarge, .stateTooLarge,
             .unsuccessfulOutcomeExceedsLimits:
            category = .resourceLimit
            code = "JOB_RESOURCE_LIMIT"
        case .invalidConfiguration, .corruptedSpecification, .corruptedPlan,
             .corruptedState, .corruptedResult, .resultChunkMissing,
             .invalidStateTransition, .stateRevisionOverflow,
             .workUnitOverflow, .duplicateJobIdentifier,
             .commitModelMismatch:
            category = .internalFailure
            code = "JOB_RUNTIME_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseMaintenanceRuntimeError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .compactionRequiresJob:
            category = .invalidRequest
            code = "MAINTENANCE_JOB_REQUIRED"
        case .compactionUnavailable:
            category = .internalFailure
            code = "MAINTENANCE_CAPABILITY_UNAVAILABLE"
        case .migrationsNotResumable:
            category = .invalidRequest
            code = "MIGRATION_EXECUTION_UNAVAILABLE"
        case .invalidInvocation, .invalidContinuation, .invalidBatchSize,
             .exactPartitionRequired, .entityRequiredForPartitionFilter:
            category = .invalidRequest
            code = "INVALID_MAINTENANCE_REQUEST"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseIndexRebuildError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .entityNotFound, .indexNotFound:
            category = .notFound
            code = "INDEX_TARGET_NOT_FOUND"
        case .buildAlreadyActive:
            category = .conflict
            code = "INDEX_REBUILD_ACTIVE"
        case .uniquenessViolation:
            category = .constraint
            code = "INDEX_UNIQUENESS_VIOLATION"
        case .invalidWorkLimit:
            category = .resourceLimit
            code = "INDEX_REBUILD_RESOURCE_LIMIT"
        case .invalidContinuation:
            category = .invalidRequest
            code = "INVALID_INDEX_CONTINUATION"
        case .compiledTypeMissing, .corruptedRecord, .recordCountOverflow:
            category = .internalFailure
            code = "INDEX_REBUILD_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabasePartitionCatalogError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .invalidEntity, .invalidPartitions, .invalidContinuation,
             .invalidPageLimit:
            category = .invalidRequest
            code = "INVALID_PARTITION_CATALOG_REQUEST"
        case .digestCollision, .corruptedEntry:
            category = .internalFailure
            code = "PARTITION_CATALOG_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseStorageCompactionError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        let retryability: DatabaseRetryability
        switch error {
        case .invalidContinuation, .unsupportedContinuationVersion,
             .incompatibleContinuation:
            category = .invalidRequest
            code = "INVALID_COMPACTION_REQUEST"
            retryability = .never
        case .invalidMaximumWorkUnits:
            category = .resourceLimit
            code = "COMPACTION_RESOURCE_LIMIT"
            retryability = .never
        case .unsupportedConfiguration:
            category = .internalFailure
            code = "COMPACTION_CONFIGURATION_FAILURE"
            retryability = .never
        case .nestedTransaction:
            category = .internalFailure
            code = "COMPACTION_TRANSACTION_CONFLICT"
            retryability = .never
        case .backendMadeNoProgress:
            category = .internalFailure
            code = "COMPACTION_MADE_NO_PROGRESS"
            retryability = .never
        case .backendFailure:
            category = .unavailable
            code = "COMPACTION_BACKEND_FAILURE"
            retryability = .backoff
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: String(describing: error),
            retryability: retryability
        )
    }

    private static func map(
        _ error: DatabaseGraphQueryError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .continuationSnapshotChanged:
            category = .conflict
            code = "QUERY_SNAPSHOT_CHANGED"
        case .pageLimitExceedsMaximum,
             .pageLimitExceedsPlatformCapacity:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .invalidContinuation, .continuationDoesNotMatchRequest,
             .continuationOffsetOutOfRange:
            category = .invalidRequest
            code = "INVALID_QUERY_CONTINUATION"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseGraphAlgorithmError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .sourceIndexNotFound:
            category = .notFound
            code = "GRAPH_SOURCE_NOT_FOUND"
        case .continuationSnapshotChanged:
            category = .conflict
            code = "GRAPH_SNAPSHOT_CHANGED"
        case .edgeWeightMissing, .invalidEdgeWeight, .ambiguousEdgeWeight:
            category = .constraint
            code = "GRAPH_WEIGHT_INVALID"
        case .sourceIndexHasNoUniqueOwner, .inconsistentAlgorithmResult,
             .unsupportedAlgorithmLimit:
            category = .internalFailure
            code = "GRAPH_RUNTIME_FAILURE"
        case .invalidContinuation, .continuationDoesNotMatchRequest:
            category = .invalidRequest
            code = "INVALID_GRAPH_CONTINUATION"
        case .unsupportedSourceIndex, .expectedPropertyGraphIdentifier,
             .expectedRDFTerm, .invalidRDFPredicate, .invalidRDFGraphName,
             .rdfSourceDoesNotCoverDefaultGraph,
             .rdfSourceDoesNotCoverNamedGraph,
             .weightPropertyNotStored, .invalidInvocation,
             .numericLimitOutOfRange:
            category = .invalidRequest
            code = "INVALID_GRAPH_REQUEST"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseCommandRegistryError
    ) -> DatabaseRemoteError {
        switch error {
        case .commandNotFound:
            return DatabaseRemoteError(
                category: .notFound,
                code: "COMMAND_NOT_FOUND",
                message: error.description,
                retryability: .never
            )
        case .emptyIdentifier, .duplicate:
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "COMMAND_CONFIGURATION_INVALID",
                message: error.description,
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseRDFDocumentStoreError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .documentNotFound:
            category = .notFound
            code = "RDF_DOCUMENT_NOT_FOUND"
        case .revisionConflict:
            category = .conflict
            code = "RDF_DOCUMENT_REVISION_CONFLICT"
        case .emptyIdentifier, .invalidPage, .invalidContinuation:
            category = .invalidRequest
            code = "INVALID_RDF_DOCUMENT_REQUEST"
        case .revisionOverflow, .corruptedMetadata, .corruptedItemCount:
            category = .internalFailure
            code = "RDF_DOCUMENT_STORE_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseOntologyProcessingError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .ontologyNotFound, .importedOntologyNotFound, .resourceNotFound:
            category = .notFound
            code = "ONTOLOGY_RESOURCE_NOT_FOUND"
        case .ontologyInUse:
            category = .conflict
            code = "ONTOLOGY_IN_USE"
        case .workLimitExceeded:
            category = .resourceLimit
            code = "ONTOLOGY_WORK_LIMIT"
        case .invalidContinuation:
            category = .invalidRequest
            code = "INVALID_CONTINUATION"
        case .invalidDocument, .invalidReasoningTriple, .materialization,
             .ontologyIdentifierMismatch, .importsMismatch, .importCycle:
            category = .invalidRequest
            code = "INVALID_ONTOLOGY"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseSHACLValidationError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .shapesGraphNotFound:
            category = .notFound
            code = "SHACL_SHAPES_NOT_FOUND"
        case .workLimitExceeded:
            category = .resourceLimit
            code = "SHACL_WORK_LIMIT"
        case .invalidContinuation:
            category = .invalidRequest
            code = "INVALID_CONTINUATION"
        case .resolvedScopeMismatch, .resolvedGraphScopeMismatch,
             .resolvedEntailmentMismatch,
             .missingOWLReasoner, .invalidSnapshotFingerprint:
            category = .internalFailure
            code = "SHACL_RUNTIME_CONFIGURATION_INVALID"
        case .invalidShapesGraph:
            category = .invalidRequest
            code = "INVALID_SHACL_SHAPES"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(_ error: SHACLError) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .resourceLimitExceeded:
            category = .resourceLimit
            code = "SHACL_RESOURCE_LIMIT"
        case .invalidPattern, .invalidConstraint:
            category = .invalidRequest
            code = "INVALID_SHACL_SHAPE"
        case .shapesGraphNotFound:
            category = .notFound
            code = "SHACL_SHAPES_NOT_FOUND"
        case .shapeNotFound:
            category = .notFound
            code = "SHACL_SHAPE_NOT_FOUND"
        case .ontologyNotFound:
            category = .notFound
            code = "SHACL_ONTOLOGY_NOT_FOUND"
        case .graphIndexNotFound, .runtimeFailure,
             .resultBindingMissing, .resultBindingTypeMismatch:
            category = .internalFailure
            code = "SHACL_RUNTIME_FAILURE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseSHACLDataSourceError
    ) -> DatabaseRemoteError {
        let category: DatabaseErrorCategory
        let code: String
        switch error {
        case .entityNotFound, .indexNotFound, .recordNotFound:
            category = .notFound
            code = "SHACL_DATA_SOURCE_NOT_FOUND"
        case .unsupportedEntailment:
            category = .invalidRequest
            code = "SHACL_ENTAILMENT_UNSUPPORTED"
        case .indexIsNotRDFDataset, .graphNotCovered, .invalidGraphName,
             .invalidPartition, .recordEntityMismatch,
             .recordPartitionMismatch, .recordSubjectMissing:
            category = .invalidRequest
            code = "INVALID_SHACL_DATA_SOURCE"
        }
        return DatabaseRemoteError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(_ error: CanonicalReadError) -> DatabaseRemoteError {
        switch error {
        case .invalidContinuation:
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_CONTINUATION",
                message: "The query continuation is invalid",
                retryability: .never
            )
        case .unsupportedSelectQuery(let reason):
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "UNSUPPORTED_QUERY",
                message: reason,
                retryability: .never
            )
        case .unsupportedSource(let reason):
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "UNSUPPORTED_SOURCE",
                message: reason,
                retryability: .never
            )
        case .invalidPartition(let entity, let reason):
            return DatabaseRemoteError(
                category: .invalidRequest,
                code: "INVALID_PARTITION",
                message: "Invalid partition for '\(entity)': \(reason)",
                retryability: .never
            )
        default:
            return DatabaseRemoteError(
                category: .internalFailure,
                code: "QUERY_FAILURE",
                message: String(describing: error),
                retryability: .never
            )
        }
    }
}
