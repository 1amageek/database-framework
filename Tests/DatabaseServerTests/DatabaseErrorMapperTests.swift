import Core
import DatabaseEngine
import DatabaseRuntime
import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import OntologyIndex
import QueryIR
import RelationshipIndex
import StorageKit
import Testing
@testable import DatabaseServer

@Suite("Database error mapper")
struct DatabaseErrorMapperTests {
    @Test("Empty mutations have a stable request error code")
    func emptyMutationFailure() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: DatabaseMutationError.emptyMutation,
            context: context
        )

        expect(
            remote,
            category: .invalidRequest,
            code: "EMPTY_MUTATION"
        )
        #expect(remote.retryability == .never)
    }

    @Test("Graph conversion failures are rejected as invalid requests")
    func graphPatternConversionFailure() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: GraphPatternConversionError.unsupportedGraphPattern("SERVICE"),
            context: context
        )

        expect(
            remote,
            category: .invalidRequest,
            code: "INVALID_GRAPH_PATTERN"
        )
    }

    @Test("Invalid SubSelect plans are rejected as invalid requests")
    func selectPlanCompilationFailure() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: SPARQLSelectPlanCompilationError.explicitDatasetInSubquery,
            context: context
        )

        expect(
            remote,
            category: .invalidRequest,
            code: "INVALID_SPARQL_SELECT_PLAN"
        )
    }

    @Test("SPARQL expression compilation separates invalid input and limits")
    func expressionCompilationFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let invalidExpression = mapper.remoteError(
            for: SPARQLExpressionCompilationError.aggregateNotAllowed,
            context: context
        )
        expect(
            invalidExpression,
            category: .invalidRequest,
            code: "INVALID_SPARQL_EXPRESSION"
        )

        let resourceLimit = mapper.remoteError(
            for: SPARQLExpressionCompilationError.resourceLimitExceeded(
                resource: "depth",
                actual: 65,
                maximum: 64
            ),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )
    }

    @Test("SPARQL literal limits are distinguished from malformed literals")
    func sparqlLiteralFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let resourceLimit = mapper.remoteError(
            for: SPARQLLiteralConversionError.literalTooLarge(
                requiredUTF8Count: 2_048,
                maximumUTF8Count: 1_024
            ),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )

        let invalidLiteral = mapper.remoteError(
            for: SPARQLLiteralConversionError.invalidLexicalForm(
                value: "not-a-number",
                datatype: "http://www.w3.org/2001/XMLSchema#integer"
            ),
            context: context
        )
        expect(
            invalidLiteral,
            category: .invalidRequest,
            code: "INVALID_RDF_LITERAL"
        )
    }

    @Test("SPARQL execution limits are distinguished from invalid queries")
    func sparqlQueryFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let resourceLimit = mapper.remoteError(
            for: SPARQLQueryError.propertyPathResultLimitExceeded(maximum: 128),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )

        let invalidQuery = mapper.remoteError(
            for: SPARQLQueryError.invalidPattern("predicate is missing"),
            context: context
        )
        expect(
            invalidQuery,
            category: .invalidRequest,
            code: "INVALID_SPARQL_QUERY"
        )
    }

    @Test("XSD resource limits are distinguished from invalid values")
    func xsdValidationFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let resourceLimit = mapper.remoteError(
            for: XSDValidationFailure.resourceLimitExceeded(
                resource: "decimalDigits",
                limit: 64,
                actual: 65
            ),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )

        let invalidValue = mapper.remoteError(
            for: XSDValidationFailure.unsupportedDatatype("urn:unsupported"),
            context: context
        )
        expect(
            invalidValue,
            category: .invalidRequest,
            code: "INVALID_XSD_VALUE"
        )
    }

    @Test("SHACL failures preserve invalid, resource, and runtime categories")
    func shaclFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let invalidPattern = mapper.remoteError(
            for: SHACLError.invalidPattern(regex: "[", reason: "unterminated"),
            context: context
        )
        expect(
            invalidPattern,
            category: .invalidRequest,
            code: "INVALID_SHACL_SHAPE"
        )

        let resourceLimit = mapper.remoteError(
            for: SHACLError.resourceLimitExceeded(
                resource: "regularExpression.activeTransitionWork",
                limit: 10,
                actual: 11
            ),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "SHACL_RESOURCE_LIMIT"
        )

        let runtimeFailure = mapper.remoteError(
            for: SHACLError.runtimeFailure(
                stage: "regular expression matching",
                reason: "invariant"
            ),
            context: context
        )
        expect(
            runtimeFailure,
            category: .internalFailure,
            code: "SHACL_RUNTIME_FAILURE"
        )
    }

    @Test("Database query limits are distinguished from invalid RDF bindings")
    func databaseQueryExecutionFailures() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let resourceLimit = mapper.remoteError(
            for: DatabaseQueryExecutionError.rdfLiteralTooLarge(
                requiredUTF8Count: 4_096,
                maximumUTF8Count: 1_024
            ),
            context: context
        )
        expect(
            resourceLimit,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )

        let invalidBinding = mapper.remoteError(
            for: DatabaseQueryExecutionError.nonRDFBinding("object"),
            context: context
        )
        expect(
            invalidBinding,
            category: .invalidRequest,
            code: "INVALID_QUERY"
        )
    }

    @Test("Mutation errors distinguish input, conflict, and schema failures")
    func mutationFailures() async throws {
        let context = try await makeContext()
        let identity = PersistableIdentity(
            entity: "Event",
            id: .string("event-1")
        )
        expectMappings(
            [
                (
                    DatabaseMutationError.idempotencyKeyRequired,
                    .invalidRequest,
                    "IDEMPOTENCY_KEY_REQUIRED"
                ),
                (
                    DatabaseMutationError.entityAlreadyExists(identity),
                    .conflict,
                    "MUTATION_CONFLICT"
                ),
                (
                    DatabaseMutationError.entityNotFound(identity),
                    .notFound,
                    "ENTITY_NOT_FOUND"
                ),
                (
                    DatabaseMutationError.invalidCompiledSchema(
                        entity: "Event",
                        reason: "missing field"
                    ),
                    .internalFailure,
                    "MUTATION_SCHEMA_INVALID"
                ),
            ],
            context: context
        )
    }

    @Test("Database transaction errors preserve their lifecycle contract")
    func databaseTransactionFailures() async throws {
        let context = try await makeContext()
        let identity = PersistableIdentity(
            entity: "Event",
            id: .string("event-1")
        )
        expectMappings(
            [
                (
                    DatabaseTransactionError.concurrentOperation,
                    .internalFailure,
                    "DATABASE_TRANSACTION_CONCURRENT_OPERATION"
                ),
                (
                    DatabaseTransactionError.closed,
                    .internalFailure,
                    "DATABASE_TRANSACTION_CLOSED"
                ),
                (
                    DatabaseTransactionError.invalidOperationContext,
                    .internalFailure,
                    "DATABASE_TRANSACTION_CONTEXT_INVALID"
                ),
                (
                    DatabaseTransactionError.operationIdentifierExhausted,
                    .resourceLimit,
                    "TRANSACTION_OPERATION_LIMIT"
                ),
                (
                    DatabaseTransactionError.invalidLimit(0),
                    .invalidRequest,
                    "INVALID_TRANSACTION_LIMIT"
                ),
                (
                    DatabaseTransactionError.itemDisappearedDuringScan,
                    .internalFailure,
                    "DATABASE_SCAN_INCONSISTENT"
                ),
                (
                    DatabaseTransactionError.unknownEntity("Event"),
                    .invalidRequest,
                    "UNKNOWN_ENTITY"
                ),
                (
                    DatabaseTransactionError.entityHasNoPersistableType("Event"),
                    .internalFailure,
                    "ENTITY_RUNTIME_NOT_COMPILED"
                ),
                (
                    DatabaseTransactionError.invalidIdentity(
                        entity: "Event",
                        reason: "invalid identifier"
                    ),
                    .invalidRequest,
                    "INVALID_ENTITY_IDENTITY"
                ),
                (
                    DatabaseTransactionError.persistedModelNotFound(identity),
                    .notFound,
                    "PERSISTED_MODEL_NOT_FOUND"
                ),
                (
                    DatabaseTransactionError.duplicateMutation(identity),
                    .invalidRequest,
                    "DUPLICATE_MUTATION"
                ),
                (
                    DatabaseTransactionError.conflictingDerivedMutation(identity),
                    .conflict,
                    "DERIVED_MUTATION_CONFLICT"
                ),
            ],
            context: context
        )
    }

    @Test("Context errors distinguish lifecycle, conflict, and size failures")
    func contextFailures() async throws {
        let context = try await makeContext()
        expectMappings(
            [
                (
                    DatabaseContextError.concurrentSaveNotAllowed,
                    .internalFailure,
                    "CONCURRENT_CONTEXT_SAVE"
                ),
                (
                    DatabaseContextError.rollbackDuringSaveNotAllowed,
                    .internalFailure,
                    "CONTEXT_ROLLBACK_DURING_SAVE"
                ),
                (
                    DatabaseContextError.saveIdentifierExhausted,
                    .resourceLimit,
                    "CONTEXT_SAVE_LIMIT"
                ),
                (
                    DatabaseContextError.invalidSaveState,
                    .internalFailure,
                    "CONTEXT_SAVE_STATE_INVALID"
                ),
                (
                    DatabaseContextError.preconditionFailed(
                        typeName: "Event",
                        idDescription: "event-1",
                        precondition: .exists,
                        reason: "row not found"
                    ),
                    .conflict,
                    "PRECONDITION_FAILED"
                ),
            ],
            context: context
        )

        let unknownCommit = CanonicalDatabaseErrorMapper().remoteError(
            for: DatabaseContextError.commitOutcomeUnknown,
            context: context
        )
        #expect(unknownCommit.category == .unavailable)
        #expect(unknownCommit.code == "COMMIT_OUTCOME_UNKNOWN")
        #expect(unknownCommit.retryability == .immediate)
    }

    @Test("Identity and projection errors retain schema and corruption semantics")
    func persistenceInvariantFailures() async throws {
        let context = try await makeContext()
        expectMappings(
            [
                (
                    PersistableIdentityEncodingError.invalidCompiledSchema(
                        entity: "Event",
                        reason: "missing partition field"
                    ),
                    .internalFailure,
                    "PERSISTABLE_SCHEMA_INVALID"
                ),
                (
                    PersistableIdentityEncodingError.identifierNotRepresentable(
                        entity: "Event"
                    ),
                    .invalidRequest,
                    "INVALID_PERSISTED_IDENTITY"
                ),
                (
                    PolymorphicProjectionError.missingProjection(
                        entity: "Event",
                        group: "TimelineItem"
                    ),
                    .internalFailure,
                    "POLYMORPHIC_PROJECTION_MISSING"
                ),
                (
                    PolymorphicProjectionError.unexpectedProjection(
                        entity: "Event",
                        group: "TimelineItem"
                    ),
                    .internalFailure,
                    "POLYMORPHIC_PROJECTION_UNEXPECTED"
                ),
            ],
            context: context
        )
    }

    @Test("Relationship errors distinguish constraints, limits, and corruption")
    func relationshipFailures() async throws {
        let context = try await makeContext()
        let identity = PersistableIdentity(
            entity: "Event",
            id: .string("event-1")
        )
        expectMappings(
            [
                (
                    RelationshipError.deleteRuleDenied(
                        itemType: "Calendar",
                        relationshipType: "Event",
                        propertyName: "calendar",
                        count: 1
                    ),
                    .constraint,
                    "RELATIONSHIP_DELETE_DENIED"
                ),
                (
                    RelationshipError.mutationLimitExceeded(
                        actual: 101,
                        maximum: 100
                    ),
                    .resourceLimit,
                    "RELATIONSHIP_MUTATION_LIMIT"
                ),
                (
                    RelationshipError.workLimitExceeded(maximum: 100),
                    .resourceLimit,
                    "RELATIONSHIP_WORK_LIMIT"
                ),
                (
                    RelationshipError.catalogOwnerMissing(identity),
                    .internalFailure,
                    "RELATIONSHIP_CATALOG_CORRUPTED"
                ),
            ],
            context: context
        )
    }

    @Test("Relationship reference errors preserve each boundary failure")
    func relationshipReferenceFailures() async throws {
        let context = try await makeContext()
        let identity = PersistableIdentity(
            entity: "Event",
            id: .string("event-1")
        )
        expectMappings(
            [
                (
                    RelationshipReferenceError.unknownRelatedEntity("Event"),
                    .invalidRequest,
                    "UNKNOWN_RELATIONSHIP_ENTITY"
                ),
                (
                    RelationshipReferenceError.relatedEntityHasNoCompiledType("Event"),
                    .internalFailure,
                    "RELATIONSHIP_ENTITY_NOT_COMPILED"
                ),
                (
                    RelationshipReferenceError.missingRelationshipField(
                        entity: "Event",
                        field: "calendar"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_FIELD_MISSING"
                ),
                (
                    RelationshipReferenceError.invalidRelationshipValue(
                        entity: "Event",
                        field: "calendar"
                    ),
                    .invalidRequest,
                    "INVALID_RELATIONSHIP_VALUE"
                ),
                (
                    RelationshipReferenceError.invalidReferenceEntity(
                        expected: "Calendar",
                        actual: "Event"
                    ),
                    .invalidRequest,
                    "INVALID_RELATIONSHIP_TARGET_ENTITY"
                ),
                (
                    RelationshipReferenceError.invalidTargetPartition(
                        entity: "Calendar",
                        reason: "tenant is missing"
                    ),
                    .invalidRequest,
                    "INVALID_RELATIONSHIP_TARGET_PARTITION"
                ),
                (
                    RelationshipReferenceError.invalidTargetIdentifier(
                        entity: "Calendar",
                        reason: .typeMismatch(expected: .string)
                    ),
                    .invalidRequest,
                    "INVALID_RELATIONSHIP_TARGET_IDENTIFIER"
                ),
                (
                    RelationshipReferenceError.invalidOwnerIdentity(
                        entity: "Event"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_OWNER_IDENTITY_INVALID"
                ),
                (
                    RelationshipReferenceError.missingDescriptor(
                        owner: "Event",
                        field: "calendar"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_DESCRIPTOR_MISSING"
                ),
                (
                    RelationshipReferenceError.descriptorMismatch(
                        owner: "Event",
                        field: "calendar"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_DESCRIPTOR_MISMATCH"
                ),
                (
                    RelationshipReferenceError.loadedTypeMismatch(
                        expected: "Event",
                        actual: "Calendar"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_STORED_TYPE_MISMATCH"
                ),
                (
                    RelationshipReferenceError.targetEntityMissing(identity),
                    .constraint,
                    "RELATIONSHIP_TARGET_NOT_FOUND"
                ),
                (
                    RelationshipReferenceError.corruptedCatalogEntry,
                    .internalFailure,
                    "RELATIONSHIP_CATALOG_CORRUPTED"
                ),
                (
                    RelationshipReferenceError.invalidScanLimit(0),
                    .invalidRequest,
                    "INVALID_RELATIONSHIP_SCAN_LIMIT"
                ),
                (
                    RelationshipReferenceError.nullifyRequiresOptionalField(
                        entity: "Event",
                        field: "calendar"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_NULLIFY_FIELD_INVALID"
                ),
                (
                    RelationshipReferenceError.entityDecodingFailed(
                        entity: "Event",
                        reason: "invalid projection"
                    ),
                    .internalFailure,
                    "RELATIONSHIP_PROJECTION_DECODING_FAILED"
                ),
            ],
            context: context
        )
    }

    @Test("Unknown failures remain explicit internal failures")
    func unknownFailure() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: UnknownFailure(),
            context: context
        )

        expect(
            remote,
            category: .internalFailure,
            code: "SERVER_FAILURE"
        )
    }

    @Test("RDF index corruption remains an explicit internal failure")
    func rdfIndexCorruption() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: RDFDatasetScannerError.physicalIndexFailure(
                source: "eventGraph",
                reason: .truncatedComponent(position: 2)
            ),
            context: context
        )

        expect(
            remote,
            category: .internalFailure,
            code: "CORRUPTED_RDF_INDEX"
        )
    }

    @Test("Invalid binary SPARQL datasets are rejected as invalid requests")
    func invalidSPARQLDataset() async throws {
        let context = try await makeContext()

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: RDFDatasetValidationError.invalidIRI("relative"),
            context: context
        )

        expect(
            remote,
            category: .invalidRequest,
            code: "INVALID_SPARQL_DATASET"
        )
    }

    @Test("An unknown commit result requires an idempotent replay")
    func commitUnknownRetryContract() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: StorageError(
                code: .commitUnknownResult,
                operation: .commit,
                message: "Commit outcome is unknown"
            ),
            context: context
        )

        #expect(remote.category == .unavailable)
        #expect(remote.code == "COMMIT_UNKNOWN_RESULT")
        #expect(remote.retryability == .immediate)
    }

    @Test("Backend storage size limits remain non-retryable resource errors")
    func nativeStorageSizeLimits() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()
        let cases: [(StorageError.Code, Int32)] = [
            (.transactionTooLarge, 2101),
            (.keyTooLarge, 2102),
            (.valueTooLarge, 2103),
        ]

        for (code, backendCode) in cases {
            let remote = mapper.remoteError(
                for: StorageError(
                    code: code,
                    operation: .commit,
                    backend: .foundationDB,
                    message: "FoundationDB size limit",
                    backendCode: backendCode
                ),
                context: context
            )
            #expect(remote.category == .resourceLimit)
            #expect(remote.code == code.rawValue.uppercased())
            #expect(remote.retryability == .never)
            #expect(
                remote.details == [
                    DatabaseObjectField(
                        number: 1,
                        name: "backendCode",
                        value: .int64(Int64(backendCode))
                    ),
                ]
            )
        }
    }

    @Test("Estimated commit-request limits preserve structured byte metadata")
    func estimatedCommitRequestLimit() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: StorageError(
                code: .transactionTooLarge,
                operation: .prepare,
                backend: .foundationDB,
                message: "FoundationDB commit request exceeds its configured limit",
                byteLimitViolation: StorageByteLimitViolation(
                    resource: .commitRequest,
                    observedByteCount: 10_000_001,
                    maximumByteCount: 10_000_000,
                    measurement: .estimated
                )
            ),
            context: context
        )

        #expect(remote.category == .resourceLimit)
        #expect(remote.code == "TRANSACTION_TOO_LARGE")
        #expect(remote.retryability == .never)
        #expect(remote.details == [
            DatabaseObjectField(
                number: 2,
                name: "observedByteCount",
                value: .uint64(10_000_001)
            ),
            DatabaseObjectField(
                number: 3,
                name: "maximumByteCount",
                value: .uint64(10_000_000)
            ),
            DatabaseObjectField(
                number: 4,
                name: "resource",
                value: .string("commit_request")
            ),
            DatabaseObjectField(
                number: 5,
                name: "measurement",
                value: .string("estimated")
            ),
        ])
    }

    @Test("Portable physical storage limits expose exact byte details")
    func portableStorageSizeLimits() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: DatabaseStorageLimitError.keyTooLarge(
                size: 10_001,
                limit: 10_000
            ),
            context: context
        )

        expect(remote, category: .resourceLimit, code: "KEY_TOO_LARGE")
        #expect(
            remote.details == [
                DatabaseObjectField(
                    number: 1,
                    name: "actualBytes",
                    value: .uint64(10_001)
                ),
                DatabaseObjectField(
                    number: 2,
                    name: "maximumBytes",
                    value: .uint64(10_000)
                ),
            ]
        )
    }

    @Test("Mutation aggregate overflow is a non-retryable resource limit")
    func mutationAggregateResourceLimit() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: TransactionMutationByteLimitError.exceeded(
                actual: 101,
                maximum: 100
            ),
            context: context
        )

        expect(
            remote,
            category: .resourceLimit,
            code: "MUTATION_AGGREGATE_TOO_LARGE"
        )
        #expect(
            remote.details == [
                DatabaseObjectField(
                    number: 1,
                    name: "actualBytes",
                    value: .uint64(101)
                ),
                DatabaseObjectField(
                    number: 2,
                    name: "maximumBytes",
                    value: .uint64(100)
                ),
            ]
        )
    }

    @Test("Portable transaction deadlines are distinct from backend timeouts")
    func portableTransactionDeadline() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: TransactionExecutionDeadlineExceeded(
                timeoutMilliseconds: 500,
                source: .inheritedOperation
            ),
            context: context
        )

        expect(
            remote,
            category: .resourceLimit,
            code: "EXECUTION_TIMED_OUT"
        )
        #expect(remote.retryability == .never)
        #expect(
            remote.details == [
                DatabaseObjectField(
                    number: 1,
                    name: "timeoutMilliseconds",
                    value: .uint64(500)
                ),
            ]
        )

        let backendTimeout = CanonicalDatabaseErrorMapper().remoteError(
            for: StorageError.transactionTimedOut,
            context: context
        )
        #expect(backendTimeout.category == .unavailable)
        #expect(backendTimeout.code == "TRANSACTION_TIMED_OUT")
        #expect(backendTimeout.retryability == .backoff)
    }

    @Test("Mutation admission configuration failures are server failures")
    func mutationAdmissionConfigurationFailure() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()

        let invalidMaximum = mapper.remoteError(
            for: TransactionMutationByteLimitError.invalidMaximum(0),
            context: context
        )
        expect(
            invalidMaximum,
            category: .internalFailure,
            code: "MUTATION_ADMISSION_CONFIGURATION_FAILURE"
        )

        let invalidState = mapper.remoteError(
            for: TransactionMutationByteLimitError.configurationAfterAdmission,
            context: context
        )
        expect(
            invalidState,
            category: .internalFailure,
            code: "MUTATION_ADMISSION_STATE_FAILURE"
        )
    }

    @Test("Invalid mutation admission counters do not trap at the boundary")
    func invalidMutationAdmissionCounters() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: TransactionMutationByteLimitError.exceeded(
                actual: -1,
                maximum: 100
            ),
            context: context
        )

        expect(
            remote,
            category: .internalFailure,
            code: "MUTATION_ADMISSION_INVARIANT_VIOLATION"
        )
        #expect(remote.details.isEmpty)
    }

    @Test("Transaction cleanup failures preserve both typed error paths")
    func transactionCleanupFailureDetails() async throws {
        let context = try await makeContext()
        let mapper = CanonicalDatabaseErrorMapper()
        let operationFailure = TransactionMutationByteLimitError.exceeded(
            actual: 101,
            maximum: 100
        )
        let cancellationFailure = StorageError(
            code: .backendFailure,
            operation: .cancel,
            message: "Cancellation failed"
        )
        let remote = mapper.remoteError(
            for: StorageTransactionCleanupError(
                operationError: operationFailure,
                cancellationError: cancellationFailure
            ),
            context: context
        )

        let mappedOperation = mapper.remoteError(
            for: operationFailure,
            context: context
        )
        let mappedCancellation = mapper.remoteError(
            for: cancellationFailure,
            context: context
        )
        expect(
            remote,
            category: .internalFailure,
            code: "TRANSACTION_CLEANUP_FAILURE"
        )
        #expect(
            remote.details == [
                DatabaseObjectField(
                    number: 1,
                    name: "operationError",
                    value: .object(remoteFields(mappedOperation))
                ),
                DatabaseObjectField(
                    number: 2,
                    name: "cancellationErrors",
                    value: .array([
                        .object(remoteFields(mappedCancellation)),
                    ])
                ),
            ]
        )
    }

    @Test("Transaction cleanup mapping obeys collection and object limits")
    func transactionCleanupMappingIsBounded() async throws {
        let context = try await makeContext()
        let cancellationFailure = StorageError(
            code: .backendFailure,
            operation: .cancel,
            message: "Cancellation failed"
        )
        var cleanup = StorageTransactionCleanupError(
            operationError: EndpointInvocationFailure.remoteFailure,
            cancellationError: cancellationFailure
        )
        for _ in 0..<10 {
            cleanup = cleanup.addingCancellationError(cancellationFailure)
        }
        let limits = try DatabaseWireLimits(
            maximumFrameBytes: 4_096,
            maximumStringBytes: 128,
            maximumByteStringBytes: 4_096,
            maximumCollectionCount: 2,
            maximumNestingDepth: 8,
            maximumObjectCount: 12
        )

        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: cleanup,
            context: context,
            limits: limits
        )

        guard remote.details.count == 2,
              case .array(let cancellations) = remote.details[1].value else {
            Issue.record("Expected bounded cancellation details")
            return
        }
        #expect(cancellations.count == 2)
    }

    @Test("Cleanup-wrapped deadlines retain execution-timeout semantics")
    func cleanupWrappedDeadlineRetainsTypedCause() async throws {
        let context = try await makeContext()
        let deadline = TransactionExecutionDeadlineExceeded(
            timeoutMilliseconds: 750,
            source: .inheritedOperation
        )
        let cleanup = StorageTransactionCleanupError(
            operationError: deadline,
            cancellationError: StorageError(
                code: .backendFailure,
                operation: .cancel,
                message: "Cancellation failed"
            )
        )
        let mapper = CanonicalDatabaseErrorMapper()

        let remote = mapper.remoteError(for: cleanup, context: context)
        let mappedDeadline = mapper.remoteError(for: deadline, context: context)

        guard let operationDetail = remote.details.first,
              case .object(let operationFields) = operationDetail.value else {
            Issue.record("Expected nested operation error details")
            return
        }
        #expect(operationFields == remoteFields(mappedDeadline))
        #expect(mappedDeadline.code == "EXECUTION_TIMED_OUT")
        #expect(mappedDeadline.retryability == .never)
    }

    @Test("SPARQL semantic violations are invalid requests")
    func semanticViolation() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: SPARQLSemanticValidationError
                .labelCrossesInsertDataOperations("shared"),
            context: context
        )

        expect(
            remote,
            category: .invalidRequest,
            code: "INVALID_SPARQL_SEMANTICS"
        )
    }

    @Test("SPARQL structural limits are resource failures")
    func semanticResourceLimit() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: SPARQLSemanticValidationError.structural(
                .resourceLimitExceeded(
                    resource: .nestingDepth,
                    actual: 65,
                    maximum: 64
                )
            ),
            context: context
        )

        expect(
            remote,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )
    }

    @Test("Canonical QueryIR structural limits are resource failures")
    func queryStructuralResourceLimit() async throws {
        let context = try await makeContext()
        let remote = CanonicalDatabaseErrorMapper().remoteError(
            for: QueryStructuralValidationError.resourceLimitExceeded(
                resource: .collectionElements,
                actual: 101,
                maximum: 100
            ),
            context: context
        )

        expect(
            remote,
            category: .resourceLimit,
            code: "QUERY_RESOURCE_LIMIT"
        )
    }

    private func makeContext() async throws -> DatabaseOperationContext {
        let container = try await DBContainer.open(
            for: Schema(
                [DatabaseEndpointEntity.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: DBConfiguration(backend: .custom(InMemoryEngine())),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
        return DatabaseOperationContext(
            container: container,
            requestID: 1,
            metadata: DatabaseRequestMetadata(),
            requestPayload: []
        )
    }

    private func remoteFields(
        _ error: DatabaseRemoteError
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

    private func expectMappings(
        _ mappings: [
            (
                error: any Error,
                category: DatabaseErrorCategory,
                code: String
            )
        ],
        context: DatabaseOperationContext
    ) {
        let mapper = CanonicalDatabaseErrorMapper()
        for mapping in mappings {
            let remote = mapper.remoteError(
                for: mapping.error,
                context: context
            )
            expect(
                remote,
                category: mapping.category,
                code: mapping.code
            )
        }
    }

    private func expect(
        _ remote: DatabaseRemoteError,
        category: DatabaseErrorCategory,
        code: String
    ) {
        #expect(remote.category == category)
        #expect(remote.code == code)
        #expect(remote.retryability == .never)
    }
}

private struct UnknownFailure: Error {}
