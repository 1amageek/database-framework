import DatabaseMaintenanceOperations
import DatabaseSchemaOperations
import DatabaseJobRuntime
import DatabaseGraphOperations
import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseCommandOperations
import DatabaseOperationCore
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
#if DATABASE_OPERATIONS_GRAPH_INDEXES
import GraphIndex
import OntologyIndex
#endif
import QueryAST
#if DATABASE_OPERATIONS_RELATIONSHIPS
import RelationshipIndex
#endif
import StorageKit

public protocol DatabaseErrorMapper: Sendable {
    func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> RemoteOperationError
}

extension DatabaseErrorMapper {
    public func remoteError(
        for error: any Error,
        context: DatabaseOperationContext
    ) -> RemoteOperationError {
        remoteError(for: error, context: context, limits: .default)
    }
}

public struct CanonicalDatabaseErrorMapper: DatabaseErrorMapper {
    public init() {}

    public func remoteError(
        for error: any Error,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits
    ) -> RemoteOperationError {
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
    ) -> RemoteOperationError {
        let mappingDepthLimit = max(0, limits.maximumNestingDepth / 2)
        guard mappingDepth <= mappingDepthLimit else {
            return RemoteOperationError(
                category: .internalFailure,
                code: "ERROR_MAPPING_DEPTH_EXCEEDED",
                message: "Nested error mapping exceeded the configured depth",
                retryability: .never
            )
        }
        if let remote = error as? RemoteOperationError {
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
            return RemoteOperationError(
                category: .resourceLimit,
                code: "RESPONSE_RESOURCE_LIMIT",
                message: responseError.description,
                retryability: .never
            )
        }
        if error is DatabaseWireError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_WIRE_PAYLOAD",
                message: "DatabaseWire payload is invalid",
                retryability: .never
            )
        }
        if error is PersistableDecodingError || error is QueryRowCodecError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_ENTITY",
                message: "Persisted entity payload is invalid",
                retryability: .never
            )
        }
        if let endpointError = error as? DatabaseOperationError {
            switch endpointError {
            case .missingHandler(let operation):
                return RemoteOperationError(
                    category: .unavailable,
                    code: "OPERATION_UNAVAILABLE",
                    message: "The requested operation is not available in this runtime",
                    retryability: .never,
                    details: Self.errorDetails([
                        (
                            key: "operation",
                            value: .uint64(UInt64(operation.rawValue))
                        ),
                    ])
                )
            case .migrationRequired:
                return RemoteOperationError(
                    category: .unavailable,
                    code: "MIGRATION_REQUIRED",
                    message: endpointError.description,
                    retryability: .never
                )
            case .targetKindNotAccepted:
                return RemoteOperationError(
                    category: .invalidRequest,
                    code: "TARGET_KIND_NOT_ACCEPTED",
                    message: endpointError.description,
                    retryability: .never
                )
            default:
                break
            }
        }
        if let limitError = error as? DatabaseOperationLimitError {
            if case .executionTimedOut(let timeoutMilliseconds) = limitError {
                return RemoteOperationError(
                    category: .resourceLimit,
                    code: "EXECUTION_TIMED_OUT",
                    message: limitError.description,
                    retryability: .never,
                    details: Self.errorDetails([
                        (key: "timeoutMilliseconds",
                            value: .uint64(UInt64(timeoutMilliseconds))
                        ),
                    ])
                )
            }
            return RemoteOperationError(
                category: .resourceLimit,
                code: "RESOURCE_LIMIT",
                message: limitError.description,
                retryability: .never
            )
        }
        if let deadlineError = error as? TransactionExecutionDeadlineExceeded {
            return RemoteOperationError(
                category: .resourceLimit,
                code: "EXECUTION_TIMED_OUT",
                message: deadlineError.description,
                retryability: .never,
                details: Self.errorDetails([
                    (key: "timeoutMilliseconds",
                        value: .uint64(deadlineError.timeoutMilliseconds)
                    ),
                ])
            )
        }
        if let workLimitError = error as? DatabaseWorkLimitError {
            return RemoteOperationError(
                category: .resourceLimit,
                code: "QUERY_RESOURCE_LIMIT",
                message: workLimitError.description,
                retryability: .never
            )
        }
        if let indexStateError = error as? IndexStateError {
            return Self.map(indexStateError)
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
        if let admissionError = error as? DatabaseStatementAdmissionError {
            return RemoteOperationError(
                category: .unavailable,
                code: "STATEMENT_FEATURE_UNAVAILABLE",
                message: admissionError.description,
                retryability: .never
            )
        }
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        if let datasetError = error as? RDFDatasetValidationError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_SPARQL_DATASET",
                message: datasetError.description,
                retryability: .never
            )
        }
        if error is GraphPatternConversionError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_GRAPH_PATTERN",
                message: "Graph pattern is invalid",
                retryability: .never
            )
        }
        if error is SPARQLSelectPlanCompilationError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_SPARQL_SELECT_PLAN",
                message: "SPARQL select plan is invalid",
                retryability: .never
            )
        }
        if let compilationError = error as? SPARQLExpressionCompilationError {
            let category: OperationErrorCategory
            let code: String
            switch compilationError {
            case .resourceLimitExceeded:
                category = .resourceLimit
                code = "QUERY_RESOURCE_LIMIT"
            default:
                category = .invalidRequest
                code = "INVALID_SPARQL_EXPRESSION"
            }
            return RemoteOperationError(
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
        if error is RDFDatasetScannerError {
            return RemoteOperationError(
                category: .internalFailure,
                code: "CORRUPTED_RDF_INDEX",
                message: "RDF index data is corrupted",
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
        #endif
        if let bindingError = error as? QueryParameterBindingError {
            if case .invalidStructure(let structuralError) = bindingError {
                return RemoteOperationError(
                    category: .resourceLimit,
                    code: "QUERY_RESOURCE_LIMIT",
                    message: structuralError.description,
                    retryability: .never
                )
            }
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_QUERY_PARAMETER",
                message: bindingError.description,
                retryability: .never
            )
        }
        if let structuralError = error as? QueryStructuralValidationError {
            return RemoteOperationError(
                category: .resourceLimit,
                code: "QUERY_RESOURCE_LIMIT",
                message: structuralError.description,
                retryability: .never
            )
        }
        if let semanticError = error as? SPARQLSemanticValidationError {
            let category: OperationErrorCategory
            let code: String
            switch semanticError {
            case .structural:
                category = .resourceLimit
                code = "QUERY_RESOURCE_LIMIT"
            default:
                category = .invalidRequest
                code = "INVALID_QUERY_SEMANTICS"
            }
            return RemoteOperationError(
                category: category,
                code: code,
                message: semanticError.description,
                retryability: .never
            )
        }
        if error is SQLParser.ParseError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_QUERY_SYNTAX",
                message: "Query syntax is invalid",
                retryability: .never
            )
        }
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        if error is SPARQLParser.ParseError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_QUERY_SYNTAX",
                message: "Query syntax is invalid",
                retryability: .never
            )
        }
        #endif
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        if let loadError = error as? SPARQLLoadSourceError {
            return Self.map(loadError)
        }
        if let updateError = error as? SPARQLUpdateError {
            return Self.map(updateError)
        }
        if let graphStoreError = error as? RDFGraphStoreError {
            return Self.map(graphStoreError)
        }
        #endif
        if let mutationError = error as? DatabaseMutationError {
            return Self.map(mutationError)
        }
        if let transactionError = error as? DatabaseTransactionError {
            return Self.map(transactionError)
        }
        if let contextError = error as? DatabaseContextError {
            return Self.map(contextError)
        }
        if let identityError = error as? EntityReferenceEncodingError {
            return Self.map(identityError)
        }
        if let projectionError = error as? PolymorphicProjectionError {
            return Self.map(projectionError)
        }
        if let grantError = error as? DatabaseGrantAuthorizationError {
            return Self.map(grantError, target: context.target)
        }
        #if DATABASE_OPERATIONS_MULTIPLE_BASES
        if let baseError = error as? DatabaseBaseExecutionError {
            return Self.map(baseError, target: context.target)
        }
        if let baseCatalogError = error as? DatabaseBaseCatalogError {
            return Self.map(baseCatalogError)
        }
        if let migrationError = error as? DatabaseLegacyLayoutMigrationError {
            return Self.map(migrationError)
        }
        if let compositionError = error as? DatabaseCompositionAccessError {
            return Self.map(compositionError)
        }
        if let compositionCatalogError = error as? DatabaseCompositionCatalogError {
            return Self.map(compositionCatalogError)
        }
        #endif
        if let administrationError = error as? DatabaseAdministrationError {
            return Self.map(administrationError)
        }
        if let authorizationError = error as? DatabaseJobAuthorizationError {
            return Self.map(authorizationError)
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
        if let schemaError = error as? DatabaseSchemaPublicationError {
            switch schemaError {
            case .fingerprintConflict:
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_FINGERPRINT_CONFLICT",
                    message: schemaError.description,
                    retryability: .never
                )
            case .idempotencyKeyReused:
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_IDEMPOTENCY_KEY_REUSED",
                    message: schemaError.description,
                    retryability: .never
                )
            case .transitionInProgress:
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_TRANSITION_IN_PROGRESS",
                    message: schemaError.description,
                    retryability: .backoff
                )
            case .invalidIdempotencyKey:
                return RemoteOperationError(
                    category: .invalidRequest,
                    code: "INVALID_SCHEMA_IDEMPOTENCY_KEY",
                    message: schemaError.description,
                    retryability: .never
                )
            case .persistentIndexBuildJobRequired:
                return RemoteOperationError(
                    category: .unavailable,
                    code: "SCHEMA_INDEX_BUILD_JOB_REQUIRED",
                    message: schemaError.description,
                    retryability: .never
                )
            case .generationOverflow, .corruptedState:
                return RemoteOperationError(
                    category: .internalFailure,
                    code: "SCHEMA_PUBLICATION_STATE_INVALID",
                    message: schemaError.description,
                    retryability: .never
                )
            }
        }
        if let schemaError = error as? DatabaseSchemaExecutionError {
            switch schemaError {
            case .migrationRequired:
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_MIGRATION_REQUIRED",
                    message: schemaError.description,
                    retryability: .never
                )
            case .runtimeUnavailable:
                return RemoteOperationError(
                    category: .unavailable,
                    code: "SCHEMA_RUNTIME_UNAVAILABLE",
                    message: schemaError.description,
                    retryability: .never
                )
            case .storageCapabilityUnavailable:
                return RemoteOperationError(
                    category: .unavailable,
                    code: "SCHEMA_STORAGE_CAPABILITY_UNAVAILABLE",
                    message: schemaError.description,
                    retryability: .never
                )
            case .persistentJobServiceUnavailable:
                return RemoteOperationError(
                    category: .unavailable,
                    code: "SCHEMA_PERSISTENT_JOB_SERVICE_UNAVAILABLE",
                    message: schemaError.description,
                    retryability: .never
                )
            }
        }
        if let schemaJobError = error as? DatabaseSchemaApplyJobError {
            if case .baseLifecycleTransitionInProgress = schemaJobError {
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_BASE_LIFECYCLE_IN_PROGRESS",
                    message: schemaJobError.description,
                    retryability: .backoff
                )
            }
            if case .baseGenerationChanged = schemaJobError {
                return RemoteOperationError(
                    category: .conflict,
                    code: "SCHEMA_BASE_GENERATION_CHANGED",
                    message: schemaJobError.description,
                    retryability: .never
                )
            }
            return RemoteOperationError(
                category: .internalFailure,
                code: "SCHEMA_INDEX_BUILD_FAILED",
                message: schemaJobError.description,
                retryability: .never
            )
        }
        if let registryError = error as? SchemaRegistryError {
            return RemoteOperationError(
                category: .conflict,
                code: "SCHEMA_INCOMPATIBLE",
                message: registryError.description,
                retryability: .never
            )
        }
        if let compactionError = error as? StorageCompactionError {
            return Self.map(compactionError)
        }
        if let registryError = error as? DatabaseResumableOperationRegistryError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "JOB_OPERATION_NOT_RESUMABLE",
                message: registryError.description,
                retryability: .never
            )
        }
        if let commandError = error as? DatabaseCommandRegistryError {
            return Self.map(commandError)
        }
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
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
        #endif
        if let expressionError = error as? DatabaseExpressionEvaluationError {
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_MUTATION_EXPRESSION",
                message: expressionError.description,
                retryability: .never
            )
        }
        if error is FieldSecurityError {
            return RemoteOperationError(
                category: .authorization,
                code: "FIELD_ACCESS_DENIED",
                message: "Field access was denied",
                retryability: .never
            )
        }
        if error is SecurityError {
            return RemoteOperationError(
                category: .authorization,
                code: "ACCESS_DENIED",
                message: "Access was denied",
                retryability: .never
            )
        }
        #if DATABASE_OPERATIONS_RELATIONSHIPS
        if let relationshipError = error as? RelationshipError {
            return Self.map(relationshipError)
        }
        if let referenceError = error as? RelationshipReferenceError {
            return Self.map(referenceError)
        }
        #endif
        if let storageError = error as? StorageError {
            let retryability: OperationRetryability
            switch storageError.retryDisposition {
            case .safe:
                retryability = .backoff
            case .requiresIdempotency:
                retryability = .immediate
            case .never:
                retryability = .never
            }
            let category: OperationErrorCategory
            switch storageError.code {
            case .transactionTooLarge, .keyTooLarge, .valueTooLarge:
                category = .resourceLimit
            case .backendFailure, .backendContractViolation, .dataCorruption:
                category = .internalFailure
            default:
                category = .unavailable
            }
            var detailFields: [(key: String, value: FieldValue)] = []
            if let backendCode = storageError.backendCode {
                detailFields.append((key: "backendCode",
                    value: .int64(Int64(backendCode))
                ))
            }
            if let byteLimitViolation = storageError.byteLimitViolation {
                detailFields.append((key: "observedByteCount",
                    value: .uint64(byteLimitViolation.observedByteCount)
                ))
                detailFields.append((key: "maximumByteCount",
                    value: .uint64(byteLimitViolation.maximumByteCount)
                ))
                detailFields.append((key: "resource",
                    value: .string(byteLimitViolation.resource.rawValue)
                ))
                detailFields.append((key: "measurement",
                    value: .string(byteLimitViolation.measurement.rawValue)
                ))
            }
            return RemoteOperationError(
                category: category,
                code: storageError.code.rawValue.uppercased(),
                message: storageError.description,
                retryability: retryability,
                details: Self.errorDetails(detailFields)
            )
        }
        if let readError = error as? CanonicalReadError {
            return Self.map(readError)
        }
        return RemoteOperationError(
            category: .internalFailure,
            code: "SERVER_FAILURE",
            message: "Database operation runtime failed",
            retryability: .never
        )
    }

    private func makeRemoteError(
        for error: StorageTransactionCleanupError,
        context: DatabaseOperationContext,
        limits: DatabaseWireLimits,
        mappingDepth: Int
    ) -> RemoteOperationError {
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
        var cancellationErrors: [RemoteOperationError] = []
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
        return RemoteOperationError(
            category: .internalFailure,
            code: "TRANSACTION_CLEANUP_FAILURE",
            message: "Transaction operation and cancellation both failed",
            retryability: .never,
            details: Self.errorDetails([
                (key: "operationError",
                    value: .object(Self.fields(for: operationError))
                ),
                (key: "cancellationErrors",
                    value: .array(
                        cancellationErrors.map {
                            .object(Self.fields(for: $0))
                        }
                    )
                ),
            ])
        )
    }

    private static func fields(
        for error: RemoteOperationError
    ) -> FieldObject {
        errorDetails([
            (key: "category",
                value: .uint64(UInt64(error.category.rawValue))
            ),
            (key: "code",
                value: .string(error.code)
            ),
            (key: "message",
                value: .string(error.message)
            ),
            (key: "retryability",
                value: .uint64(UInt64(error.retryability.rawValue))
            ),
            (key: "details",
                value: .object(error.details)
            ),
        ])
    }

    private static func errorDetails(
        _ fields: consuming [(key: String, value: FieldValue)]
    ) -> FieldObject {
        do {
            return try FieldObject(consume fields)
        } catch {
            preconditionFailure(
                "Database error detail keys must be unique: \(error)"
            )
        }
    }

    private static func map(
        _ error: IndexStateError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        let message: String
        let indexName: String

        switch error {
        case .invalidPersistedStateSize(let index, _),
             .unknownPersistedStateValue(let index, _):
            category = .internalFailure
            code = "CORRUPTED_INDEX_STATE"
            message = "Persisted index state is corrupted"
            indexName = index
        case .missingRequestedState(let index):
            category = .internalFailure
            code = "INDEX_STATE_INVARIANT_VIOLATION"
            message = "Index state resolution violated its contract"
            indexName = index
        case .invalidTransition(_, _, let index, _):
            category = .conflict
            code = "INVALID_INDEX_STATE_TRANSITION"
            message = "Index state transition is invalid"
            indexName = index
        case .missingStateForNonEmptyStore(let index):
            category = .internalFailure
            code = "MISSING_INDEX_STATE"
            message = "Persisted index state is missing for a non-empty store"
            indexName = index
        case .missingPersistedState(let index):
            category = .internalFailure
            code = "MISSING_INDEX_STATE"
            message = "Persisted index state is missing"
            indexName = index
        case .indexNotReady(let index, _):
            category = .conflict
            code = "INDEX_NOT_READY"
            message = "Index is not ready"
            indexName = index
        }

        return RemoteOperationError(
            category: category,
            code: code,
            message: message,
            retryability: .never,
            details: Self.errorDetails([
                (key: "index", value: .string(indexName))
            ])
        )
    }

    private static func map(
        _ error: TransactionMutationByteLimitError
    ) -> RemoteOperationError {
        switch error {
        case .exceeded(let actual, let maximum):
            guard let actualBytes = UInt64(exactly: actual),
                  let maximumBytes = UInt64(exactly: maximum) else {
                return RemoteOperationError(
                    category: .internalFailure,
                    code: "MUTATION_ADMISSION_INVARIANT_VIOLATION",
                    message: error.description,
                    retryability: .never
                )
            }
            return RemoteOperationError(
                category: .resourceLimit,
                code: "MUTATION_AGGREGATE_TOO_LARGE",
                message: error.description,
                retryability: .never,
                details: Self.errorDetails([
                    (key: "actualBytes",
                        value: .uint64(actualBytes)
                    ),
                    (key: "maximumBytes",
                        value: .uint64(maximumBytes)
                    ),
                ])
            )
        case .invalidMaximum:
            return RemoteOperationError(
                category: .internalFailure,
                code: "MUTATION_ADMISSION_CONFIGURATION_FAILURE",
                message: error.description,
                retryability: .never
            )
        case .alreadyConfigured, .configurationAfterAdmission:
            return RemoteOperationError(
                category: .internalFailure,
                code: "MUTATION_ADMISSION_STATE_FAILURE",
                message: error.description,
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseStorageLimitError
    ) -> RemoteOperationError {
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
            return RemoteOperationError(
                category: .internalFailure,
                code: "STORAGE_LIMIT_INVARIANT_VIOLATION",
                message: error.description,
                retryability: .never
            )
        }
        return RemoteOperationError(
            category: .resourceLimit,
            code: code,
            message: error.description,
            retryability: .never,
            details: Self.errorDetails([
                (key: "actualBytes",
                    value: .uint64(actualBytes)
                ),
                (key: "maximumBytes",
                    value: .uint64(maximumBytes)
                ),
            ])
        )
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private static func map(
        _ error: SPARQLLoadSourceError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        let retryability: OperationRetryability
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: retryability
        )
    }

    private static func map(
        _ error: SPARQLUpdateError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: RDFGraphStoreError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }
    #endif

    private static func map(
        _ error: DatabaseQueryExecutionError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .featureUnavailable, .querySnapshotUnavailable:
            category = .unavailable
            code = "QUERY_FEATURE_UNAVAILABLE"
        case .querySnapshotStale:
            category = .conflict
            code = "QUERY_SNAPSHOT_STALE"
        case .querySnapshotExpired:
            category = .notFound
            code = "QUERY_SNAPSHOT_EXPIRED"
        case .querySnapshotCorrupted:
            category = .internalFailure
            code = "QUERY_SNAPSHOT_CORRUPTED"
        case .querySnapshotLimitExceeded:
            category = .resourceLimit
            code = "QUERY_SNAPSHOT_LIMIT"
        case .compositionAggregateFailure:
            category = .constraint
            code = "COMPOSITION_AGGREGATE_FAILED"
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        case .rdfLiteralTooLarge:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        #endif
        case .invalidContinuation:
            category = .invalidRequest
            code = "INVALID_CONTINUATION"
        case .pageLimitMustBePositive, .solutionModifierMustBeNonNegative,
             .continuationNotSupported,
             .mutationRequiresMutationOperation,
             .compositionPlanUnsupported:
            category = .invalidRequest
            if case .compositionPlanUnsupported = error {
                code = "COMPOSITION_PLAN_UNSUPPORTED"
            } else {
                code = "INVALID_QUERY"
            }
        #if DATABASE_OPERATIONS_GRAPH_INDEXES
        case .unresolvedConstructTerm,
             .nonRDFBinding, .invalidRDFTermRole,
             .invalidRDFLiteralDatatype, .unsupportedRDFLiteral,
             .reifiedTripleRequiresTemplateContext,
             .describeVariableRequiresPattern, .invalidDescribeResource:
            category = .invalidRequest
            code = "INVALID_QUERY"
        #endif
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private static func map(
        _ error: SPARQLLiteralConversionError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }

    private static func map(
        _ error: SPARQLExpressionEvaluationError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: SPARQLQueryError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: XSDValidationFailure
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .resourceLimitExceeded:
            category = .resourceLimit
            code = "QUERY_RESOURCE_LIMIT"
        case .invalidLexicalForm, .unsupportedDatatype, .invalidRestriction:
            category = .invalidRequest
            code = "INVALID_XSD_VALUE"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }
    #endif

    private static func map(_ error: DatabaseMutationError) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .emptyMutation:
            category = .invalidRequest
            code = "EMPTY_MUTATION"
        case .mutationLimitExceeded, .preconditionLimitExceeded,
             .idempotencyKeyTooLarge:
            category = .resourceLimit
            code = "MUTATION_LIMIT"
        case .idempotencyKeyRequired:
            category = .invalidRequest
            code = "IDEMPOTENCY_KEY_REQUIRED"
        case .idempotencyKeyConflict:
            category = .conflict
            code = "IDEMPOTENCY_KEY_CONFLICT"
        case .idempotencyEntryCorrupted, .logicalVersionOverflow,
             .stateStoreContainerMismatch:
            category = .internalFailure
            code = "MUTATION_RUNTIME_FAILURE"
        case .unknownEntity:
            category = .invalidRequest
            code = "UNKNOWN_ENTITY"
        case .entityHasNoPersistableType:
            category = .invalidRequest
            code = "ENTITY_NOT_PERSISTABLE"
        case .invalidPersistableIdentifier, .identifierNotRepresentable:
            category = .invalidRequest
            code = "INVALID_ENTITY_IDENTIFIER"
        case .invalidPartition:
            category = .invalidRequest
            code = "INVALID_PARTITION"
        case .invalidGraphPartitions:
            category = .invalidRequest
            code = "INVALID_GRAPH_PARTITIONS"
        case .entityTypeMismatch, .persistableIdentityMismatch,
             .fieldNotRepresentable, .fieldValueNotRepresentable:
            category = .invalidRequest
            code = "INVALID_ENTITY"
        case .duplicateChange, .duplicatePrecondition,
             .incompatiblePreconditions:
            category = .invalidRequest
            code = "INVALID_MUTATION"
        case .entityAlreadyExists, .entityVersionMismatch:
            category = .conflict
            code = "MUTATION_CONFLICT"
        case .entityNotFound:
            category = .notFound
            code = "ENTITY_NOT_FOUND"
        case .invalidCompiledSchema:
            category = .internalFailure
            code = "MUTATION_SCHEMA_INVALID"
        case .featureUnavailable:
            category = .unavailable
            code = "MUTATION_FEATURE_UNAVAILABLE"
        case .unsupportedStatement:
            category = .invalidRequest
            code = "UNSUPPORTED_MUTATION_STATEMENT"
        case .fieldsRequired, .fieldsMustBeEmptyForDelete:
            category = .invalidRequest
            code = "INVALID_MUTATION_FIELDS"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseTransactionError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .concurrentOperation:
            category = .internalFailure
            code = "DATABASE_TRANSACTION_CONCURRENT_OPERATION"
        case .closed:
            category = .internalFailure
            code = "DATABASE_TRANSACTION_CLOSED"
        case .invalidOperationContext:
            category = .internalFailure
            code = "DATABASE_TRANSACTION_CONTEXT_INVALID"
        case .operationIdentifierExhausted:
            category = .resourceLimit
            code = "TRANSACTION_OPERATION_LIMIT"
        case .invalidLimit:
            category = .invalidRequest
            code = "INVALID_TRANSACTION_LIMIT"
        case .itemDisappearedDuringScan:
            category = .internalFailure
            code = "DATABASE_SCAN_INCONSISTENT"
        case .unknownEntity:
            category = .invalidRequest
            code = "UNKNOWN_ENTITY"
        case .entityHasNoPersistableType:
            category = .internalFailure
            code = "ENTITY_RUNTIME_NOT_COMPILED"
        case .invalidIdentity:
            category = .invalidRequest
            code = "INVALID_ENTITY_IDENTITY"
        case .persistedModelNotFound:
            category = .notFound
            code = "PERSISTED_MODEL_NOT_FOUND"
        case .duplicateMutation:
            category = .invalidRequest
            code = "DUPLICATE_MUTATION"
        case .conflictingDerivedMutation:
            category = .conflict
            code = "DERIVED_MUTATION_CONFLICT"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseContextError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        let retryability: OperationRetryability
        var details = FieldObject()
        switch error {
        case .concurrentSaveNotAllowed:
            category = .internalFailure
            code = "CONCURRENT_CONTEXT_SAVE"
            retryability = .never
        case .rollbackDuringSaveNotAllowed:
            category = .internalFailure
            code = "CONTEXT_ROLLBACK_DURING_SAVE"
            retryability = .never
        case .commitOutcomeUnknown:
            category = .unavailable
            code = "COMMIT_OUTCOME_UNKNOWN"
            retryability = .immediate
        case .saveIdentifierExhausted:
            category = .resourceLimit
            code = "CONTEXT_SAVE_LIMIT"
            retryability = .never
        case .invalidSaveState:
            category = .internalFailure
            code = "CONTEXT_SAVE_STATE_INVALID"
            retryability = .never
        case .preconditionFailed(let identity, _, _):
            category = .conflict
            code = "PRECONDITION_FAILED"
            retryability = .never
            details = Self.errorDetails([
                (key: "identity", value: .reference(identity)),
            ])
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: retryability,
            details: details
        )
    }

    private static func map(
        _ error: EntityReferenceEncodingError
    ) -> RemoteOperationError {
        switch error {
        case .invalidCompiledSchema:
            return RemoteOperationError(
                category: .internalFailure,
                code: "PERSISTABLE_SCHEMA_INVALID",
                message: "Persistable schema is invalid",
                retryability: .never
            )
        case .identifierNotRepresentable:
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_PERSISTED_IDENTITY",
                message: "Persisted entity identity is invalid",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: PolymorphicProjectionError
    ) -> RemoteOperationError {
        let code: String
        switch error {
        case .missingProjection:
            code = "POLYMORPHIC_PROJECTION_MISSING"
        case .unexpectedProjection:
            code = "POLYMORPHIC_PROJECTION_UNEXPECTED"
        }
        return RemoteOperationError(
            category: .internalFailure,
            code: code,
            message: code,
            retryability: .never
        )
    }

    #if DATABASE_OPERATIONS_RELATIONSHIPS
    private static func map(
        _ error: RelationshipError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .deleteRuleDenied:
            category = .constraint
            code = "RELATIONSHIP_DELETE_DENIED"
        case .mutationLimitExceeded:
            category = .resourceLimit
            code = "RELATIONSHIP_MUTATION_LIMIT"
        case .workLimitExceeded:
            category = .resourceLimit
            code = "RELATIONSHIP_WORK_LIMIT"
        case .catalogOwnerMissing:
            category = .internalFailure
            code = "RELATIONSHIP_CATALOG_CORRUPTED"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: RelationshipReferenceError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .unknownRelatedEntity:
            category = .invalidRequest
            code = "UNKNOWN_RELATIONSHIP_ENTITY"
        case .relatedEntityHasNoCompiledType:
            category = .internalFailure
            code = "RELATIONSHIP_ENTITY_NOT_COMPILED"
        case .missingRelationshipField:
            category = .internalFailure
            code = "RELATIONSHIP_FIELD_MISSING"
        case .invalidRelationshipValue:
            category = .invalidRequest
            code = "INVALID_RELATIONSHIP_VALUE"
        case .invalidReferenceEntity:
            category = .invalidRequest
            code = "INVALID_RELATIONSHIP_TARGET_ENTITY"
        case .invalidTargetPartition:
            category = .invalidRequest
            code = "INVALID_RELATIONSHIP_TARGET_PARTITION"
        case .invalidTargetIdentifier:
            category = .invalidRequest
            code = "INVALID_RELATIONSHIP_TARGET_IDENTIFIER"
        case .invalidOwnerIdentity:
            category = .internalFailure
            code = "RELATIONSHIP_OWNER_IDENTITY_INVALID"
        case .missingDescriptor:
            category = .internalFailure
            code = "RELATIONSHIP_DESCRIPTOR_MISSING"
        case .descriptorMismatch:
            category = .internalFailure
            code = "RELATIONSHIP_DESCRIPTOR_MISMATCH"
        case .loadedTypeMismatch:
            category = .internalFailure
            code = "RELATIONSHIP_STORED_TYPE_MISMATCH"
        case .targetEntityMissing:
            category = .constraint
            code = "RELATIONSHIP_TARGET_NOT_FOUND"
        case .corruptedCatalogEntry:
            category = .internalFailure
            code = "RELATIONSHIP_CATALOG_CORRUPTED"
        case .invalidScanLimit:
            category = .invalidRequest
            code = "INVALID_RELATIONSHIP_SCAN_LIMIT"
        case .nullifyRequiresOptionalField:
            category = .internalFailure
            code = "RELATIONSHIP_NULLIFY_FIELD_INVALID"
        case .entityDecodingFailed:
            category = .internalFailure
            code = "RELATIONSHIP_PROJECTION_DECODING_FAILED"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }
    #endif

    private static func map(
        _ error: DatabaseGrantAuthorizationError,
        target: DatabaseOperationTarget
    ) -> RemoteOperationError {
        switch error {
        case .unauthenticated:
            return RemoteOperationError(
                category: .authentication,
                code: "AUTHENTICATION_REQUIRED",
                message: "Authentication is required",
                retryability: .never
            )
        case .denied, .resourceMismatch:
            if case .base = target {
                return baseUnavailableError()
            }
            return RemoteOperationError(
                category: .authorization,
                code: "ACCESS_DENIED",
                message: "Access was denied",
                retryability: .never
            )
        case .invalidSubject, .invalidAccessBits:
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_GRANT",
                message: "The Grant is invalid",
                retryability: .never
            )
        case .lastAdministrator:
            return RemoteOperationError(
                category: .conflict,
                code: "LAST_ADMINISTRATOR",
                message: "At least one administering Grant must remain",
                retryability: .never
            )
        case .revisionConflict:
            return RemoteOperationError(
                category: .conflict,
                code: "GRANT_REVISION_CONFLICT",
                message: "The Grant revision does not match",
                retryability: .never
            )
        case .revisionOverflow, .corruptedGrant:
            return RemoteOperationError(
                category: .internalFailure,
                code: "GRANT_STORE_FAILURE",
                message: "The Grant store is inconsistent",
                retryability: .never
            )
        }
    }

    private static func baseUnavailableError() -> RemoteOperationError {
        RemoteOperationError(
            category: .authorization,
            code: "BASE_UNAVAILABLE",
            message: "The Base is unavailable",
            retryability: .never
        )
    }

    #if DATABASE_OPERATIONS_MULTIPLE_BASES
    private static func map(
        _ error: DatabaseBaseExecutionError,
        target: DatabaseOperationTarget
    ) -> RemoteOperationError {
        if case .base = target {
            switch error {
            case .baseNotFound, .baseUnavailable, .placementRootMissing:
                return baseUnavailableError()
            case .baseTargetRequired, .storageDomainUnavailable,
                 .leaseCountOverflow:
                break
            }
        }
        switch error {
        case .baseTargetRequired:
            return RemoteOperationError(
                category: .invalidRequest,
                code: "BASE_TARGET_REQUIRED",
                message: "A Base target is required",
                retryability: .never
            )
        case .baseNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "BASE_NOT_FOUND",
                message: "The Base was not found",
                retryability: .never
            )
        case .baseUnavailable:
            return RemoteOperationError(
                category: .conflict,
                code: "BASE_UNAVAILABLE",
                message: "The Base does not accept this operation",
                retryability: .never
            )
        case .storageDomainUnavailable:
            return RemoteOperationError(
                category: .unavailable,
                code: "BASE_STORAGE_UNAVAILABLE",
                message: "The Base storage domain is unavailable",
                retryability: .backoff
            )
        case .placementRootMissing, .leaseCountOverflow:
            return RemoteOperationError(
                category: .internalFailure,
                code: "BASE_RUNTIME_FAILURE",
                message: "The Base runtime state is inconsistent",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseBaseCatalogError
    ) -> RemoteOperationError {
        switch error {
        case .baseNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "BASE_NOT_FOUND",
                message: "The Base was not found",
                retryability: .never
            )
        case .baseAlreadyExists, .baseIdentifierRetired:
            return RemoteOperationError(
                category: .conflict,
                code: "BASE_IDENTITY_CONFLICT",
                message: "The Base identifier is unavailable",
                retryability: .never
            )
        case .revisionConflict:
            return RemoteOperationError(
                category: .conflict,
                code: "BASE_REVISION_CONFLICT",
                message: "The Base revision does not match",
                retryability: .never
            )
        case .invalidLifecycleTransition, .baseReferencedByComposition,
             .placementAlreadySelected, .placementDestinationMatchesSource,
             .placementDestinationClaimed, .placementDestinationNotEmpty,
             .baseDeletionClaimed:
            return RemoteOperationError(
                category: .conflict,
                code: "BASE_LIFECYCLE_CONFLICT",
                message: "The Base lifecycle transition is not currently allowed",
                retryability: .never
            )
        case .placementNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "PLACEMENT_NOT_FOUND",
                message: "The storage placement was not found",
                retryability: .never
            )
        case .catalogTooLarge:
            return RemoteOperationError(
                category: .resourceLimit,
                code: "BASE_CATALOG_RESOURCE_LIMIT",
                message: "The Base catalog exceeds the configured limit",
                retryability: .never
            )
        case .storageDomainNotFound, .placementDigestMismatch,
             .placementTransferOverflow, .invalidPlacementMoveOwner,
             .invalidDeletionOwner, .baseDeletionMarkerMissing,
             .corruptedRecord:
            return RemoteOperationError(
                category: .internalFailure,
                code: "BASE_CATALOG_FAILURE",
                message: "The Base catalog is inconsistent",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseLegacyLayoutMigrationError
    ) -> RemoteOperationError {
        switch error {
        case .layoutIsCurrent:
            return RemoteOperationError(
                category: .conflict,
                code: "LAYOUT_ALREADY_CURRENT",
                message: "The database already uses the current Base layout",
                retryability: .never
            )
        case .legacyJobsPresent:
            return RemoteOperationError(
                category: .conflict,
                code: "LEGACY_JOBS_PRESENT",
                message: "Legacy jobs must be completed or removed before migration",
                retryability: .never
            )
        case .destinationBaseExists:
            return RemoteOperationError(
                category: .conflict,
                code: "MIGRATION_BASE_CONFLICT",
                message: "The migration destination Base already exists",
                retryability: .never
            )
        case .fingerprintMismatch, .sourceChangedDuringMigration:
            return RemoteOperationError(
                category: .conflict,
                code: "LAYOUT_FINGERPRINT_CONFLICT",
                message: "The legacy layout changed after it was planned",
                retryability: .never
            )
        case .cleanupFailed:
            return RemoteOperationError(
                category: .unavailable,
                code: "LEGACY_CLEANUP_FAILED",
                message: "Legacy layout cleanup did not complete",
                retryability: .backoff
            )
        case .unknownPartitionEntity,
             .conflictingLegacyOntologyStores,
             .controlDomainOverlapsLegacyData,
             .destinationOverlapsLegacyData,
             .invalidInventory,
             .destinationDigestMismatch,
             .invalidTransferState,
             .transferOverflow:
            return RemoteOperationError(
                category: .internalFailure,
                code: "LEGACY_LAYOUT_INVALID",
                message: "The legacy layout cannot be migrated safely",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseCompositionAccessError
    ) -> RemoteOperationError {
        _ = error
        return RemoteOperationError(
            category: .notFound,
            code: "COMPOSITION_UNAVAILABLE",
            message: "The Composition is unavailable",
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseCompositionCatalogError
    ) -> RemoteOperationError {
        switch error {
        case .compositionNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "COMPOSITION_NOT_FOUND",
                message: "The Composition was not found",
                retryability: .never
            )
        case .compositionAlreadyExists:
            return RemoteOperationError(
                category: .conflict,
                code: "COMPOSITION_IDENTITY_CONFLICT",
                message: "The Composition identifier is unavailable",
                retryability: .never
            )
        case .revisionConflict:
            return RemoteOperationError(
                category: .conflict,
                code: "COMPOSITION_REVISION_CONFLICT",
                message: "The Composition revision does not match",
                retryability: .never
            )
        case .memberBaseNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "COMPOSITION_MEMBER_UNAVAILABLE",
                message: "A Composition member is unavailable",
                retryability: .never
            )
        case .memberBaseNotActive:
            return RemoteOperationError(
                category: .conflict,
                code: "COMPOSITION_MEMBER_UNAVAILABLE",
                message: "A Composition member does not accept reads",
                retryability: .never
            )
        case .catalogTooLarge:
            return RemoteOperationError(
                category: .resourceLimit,
                code: "COMPOSITION_CATALOG_RESOURCE_LIMIT",
                message: "The Composition catalog exceeds the configured limit",
                retryability: .never
            )
        case .corruptedRecord:
            return RemoteOperationError(
                category: .internalFailure,
                code: "COMPOSITION_CATALOG_FAILURE",
                message: "The Composition catalog is inconsistent",
                retryability: .never
            )
        }
    }
    #endif

    private static func map(
        _ error: DatabaseAdministrationError
    ) -> RemoteOperationError {
        switch error {
        case .targetMismatch, .grantResourceMismatch,
             .idempotencyKeyMismatch:
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_ADMINISTRATION_REQUEST",
                message: "The administration request does not match its target",
                retryability: .never
            )
        case .unsupportedLifecycleAction:
            return RemoteOperationError(
                category: .unavailable,
                code: "ADMINISTRATION_ACTION_UNAVAILABLE",
                message: "The administration action is unavailable",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseJobAuthorizationError
    ) -> RemoteOperationError {
        switch error {
        case .invalidReference, .referenceRequired:
            return RemoteOperationError(
                category: .authentication,
                code: "JOB_AUTHORIZATION_REFERENCE_REQUIRED",
                message: "Persistent jobs require a valid host authorization reference",
                retryability: .never
            )
        case .validatorUnavailable:
            return RemoteOperationError(
                category: .unavailable,
                code: "JOB_AUTHORIZATION_VALIDATOR_UNAVAILABLE",
                message: "The host authorization authority is unavailable",
                retryability: .backoff
            )
        case .principalChanged, .revalidationFailed:
            return RemoteOperationError(
                category: .authentication,
                code: "JOB_AUTHORIZATION_REVALIDATION_FAILED",
                message: "Persistent job authorization is no longer valid",
                retryability: .never
            )
        }
    }

    private static func map(
        _ error: DatabaseJobRuntimeError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .jobNotFound:
            category = .notFound
            code = "JOB_NOT_FOUND"
        case .resultNotReady:
            category = .conflict
            code = "JOB_RESULT_NOT_READY"
        case .invalidRetryPolicy, .invalidTarget, .requestPayloadTooLarge,
             .jobOperationMismatch,
             .invalidResultContinuation:
            category = .invalidRequest
            code = "INVALID_JOB_REQUEST"
        case .sliceExceededBudget, .responseTooLarge,
             .specificationTooLarge, .planTooLarge, .stateTooLarge,
             .unsuccessfulOutcomeExceedsLimits, .sliceMadeNoProgress:
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseMaintenanceRuntimeError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseIndexRebuildError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        case .compiledTypeMissing, .corruptedRebuildState, .entityCountOverflow:
            category = .internalFailure
            code = "INDEX_REBUILD_FAILURE"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabasePartitionCatalogError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: .never
        )
    }

    private static func map(
        _ error: StorageCompactionError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        let retryability: OperationRetryability
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: code,
            retryability: retryability
        )
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private static func map(
        _ error: DatabaseGraphQueryError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseGraphAlgorithmError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .sourceIndexNotFound:
            category = .notFound
            code = "GRAPH_SOURCE_NOT_FOUND"
        case .sourcePartitionNotFound:
            category = .notFound
            code = "GRAPH_SOURCE_PARTITION_NOT_FOUND"
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
             .propertyGraphSourceDoesNotCoverNamedGraph,
             .rdfSourceDoesNotCoverDefaultGraph,
             .rdfSourceDoesNotCoverNamedGraph,
             .weightPropertyNotStored, .invalidInvocation,
             .numericLimitOutOfRange:
            category = .invalidRequest
            code = "INVALID_GRAPH_REQUEST"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }
    #endif

    private static func map(
        _ error: DatabaseCommandRegistryError
    ) -> RemoteOperationError {
        switch error {
        case .commandNotFound:
            return RemoteOperationError(
                category: .notFound,
                code: "COMMAND_NOT_FOUND",
                message: error.description,
                retryability: .never
            )
        case .duplicate:
            return RemoteOperationError(
                category: .internalFailure,
                code: "COMMAND_CONFIGURATION_INVALID",
                message: error.description,
                retryability: .never
            )
        }
    }

    #if DATABASE_OPERATIONS_GRAPH_INDEXES
    private static func map(
        _ error: DatabaseRDFDocumentStoreError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseOntologyProcessingError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseSHACLValidationError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        case .resolvedDataSourceMismatch, .resolvedDataGraphMismatch,
             .resolvedEntailmentMismatch,
             .missingOWLEntailment, .invalidSnapshotFingerprint:
            category = .internalFailure
            code = "SHACL_RUNTIME_CONFIGURATION_INVALID"
        case .invalidShapesGraph:
            category = .invalidRequest
            code = "INVALID_SHACL_SHAPES"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(_ error: SHACLError) -> RemoteOperationError {
        let category: OperationErrorCategory
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
        case .ontologyIdentifierRequired:
            category = .invalidRequest
            code = "SHACL_ONTOLOGY_IDENTIFIER_REQUIRED"
        case .graphIndexNotFound, .runtimeFailure,
             .resultBindingMissing, .resultBindingTypeMismatch:
            category = .internalFailure
            code = "SHACL_RUNTIME_FAILURE"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }

    private static func map(
        _ error: DatabaseSHACLDataSourceError
    ) -> RemoteOperationError {
        let category: OperationErrorCategory
        let code: String
        switch error {
        case .schemaEntityNotFound, .indexNotFound, .focusEntityNotFound:
            category = .notFound
            code = "SHACL_DATA_SOURCE_NOT_FOUND"
        case .indexIsNotRDFDataset, .graphNotCovered, .invalidGraphName,
             .invalidPartition, .focusEntityMismatch,
             .focusPartitionMismatch, .focusSubjectMissing:
            category = .invalidRequest
            code = "INVALID_SHACL_DATA_SOURCE"
        }
        return RemoteOperationError(
            category: category,
            code: code,
            message: error.description,
            retryability: .never
        )
    }
    #endif

    private static func map(_ error: CanonicalReadError) -> RemoteOperationError {
        switch error {
        case .invalidContinuation:
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_CONTINUATION",
                message: "The query continuation is invalid",
                retryability: .never
            )
        case .unsupportedSelectQuery(let reason):
            return RemoteOperationError(
                category: .invalidRequest,
                code: "UNSUPPORTED_QUERY",
                message: reason,
                retryability: .never
            )
        case .unsupportedSource(let reason):
            return RemoteOperationError(
                category: .invalidRequest,
                code: "UNSUPPORTED_SOURCE",
                message: reason,
                retryability: .never
            )
        case .invalidPartition(let entity, let reason):
            return RemoteOperationError(
                category: .invalidRequest,
                code: "INVALID_PARTITION",
                message: "Invalid partition for '\(entity)': \(reason)",
                retryability: .never
            )
        default:
            return RemoteOperationError(
                category: .internalFailure,
                code: "QUERY_FAILURE",
                message: "Query execution failed",
                retryability: .never
            )
        }
    }
}
