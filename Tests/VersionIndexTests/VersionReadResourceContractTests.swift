@_spi(DatabaseExecution) @_spi(PolymorphicRuntime) @testable import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing

@testable import VersionIndex

@Persistable(type: "VersionCanonicalDocument")
private struct VersionCanonicalDocument: SecurityPolicy {
    #Directory<VersionCanonicalDocument>("version-index-contract", "documents")
    #Index(
        .history(
            name: "VersionCanonicalDocument_history",
            version: \VersionCanonicalDocument.id,
            retention: .keepAll
        )
    )

    var id: String = ""
    var title: String = ""

    static func permitsRead(
        of resource: borrowing VersionCanonicalDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.roles.contains("version-reader") == true
    }

    static func permitsCreate(
        _ newResource: borrowing VersionCanonicalDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing VersionCanonicalDocument,
        to newResource: borrowing VersionCanonicalDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing VersionCanonicalDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Polymorphable(identifier: "VersionCanonicalDocumentGroup")
@PolymorphicDirectory("version-index-contract", "polymorphic")
@PolymorphicIndex(
    .history(
        name: "VersionCanonicalDocumentGroup_history",
        version: "id",
        retention: .keepAll
    )
)
private protocol VersionCanonicalDocumentGroup:
    Polymorphable<VersionCanonicalDocumentGroupPolymorphicGroup>
{
    var id: String { get }
    var title: String { get }
}

@Persistable(type: "VersionCanonicalArticle")
private struct VersionCanonicalArticle:
    VersionCanonicalDocumentGroup,
    SecurityPolicy
{
    #Directory<VersionCanonicalArticle>("version-index-contract", "articles")

    var id: String = ""
    var title: String = ""

    static func permitsRead(
        of resource: borrowing VersionCanonicalArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.roles.contains("version-reader") == true
    }

    static func permitsCreate(
        _ newResource: borrowing VersionCanonicalArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing VersionCanonicalArticle,
        to newResource: borrowing VersionCanonicalArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing VersionCanonicalArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Persistable(type: "VersionCanonicalReport")
private struct VersionCanonicalReport:
    VersionCanonicalDocumentGroup,
    SecurityPolicy
{
    #Directory<VersionCanonicalReport>("version-index-contract", "reports")

    var id: String = ""
    var title: String = ""

    static func permitsRead(
        of resource: borrowing VersionCanonicalReport,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.roles.contains("version-reader") == true
    }

    static func permitsCreate(
        _ newResource: borrowing VersionCanonicalReport,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing VersionCanonicalReport,
        to newResource: borrowing VersionCanonicalReport,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing VersionCanonicalReport,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

private struct VersionCancellingReadAccess: TransactionReadAccess {
    let base: any TransactionReadAccess
    let cancellation: VersionReadCancellation

    init(
        base: any TransactionReadAccess,
        failingAfterSuccessfulAdvances: Int
    ) {
        self.base = base
        self.cancellation = VersionReadCancellation(
            failingAfterSuccessfulAdvances: failingAfterSuccessfulAdvances
        )
    }

    var transactionDomain: StorageTransactionDomain {
        base.transactionDomain
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await base.getValue(for: key, snapshot: snapshot)
    }

    func getValue(
        for key: ByteString,
        snapshot: Bool,
        maximumByteCount: Int
    ) async throws -> ByteString? {
        try await base.getValue(
            for: key,
            snapshot: snapshot,
            maximumByteCount: maximumByteCount
        )
    }

    func getValue(for key: ByteString) async throws -> ByteString? {
        try await base.getValue(for: key)
    }

    func getKey(
        selector: KeySelector,
        snapshot: Bool
    ) async throws -> ByteString? {
        try await base.getKey(selector: selector, snapshot: snapshot)
    }

    func rangeCursor(
        from begin: KeySelector,
        to end: KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: StreamingMode
    ) -> KeyValueCursor {
        let cursor = base.rangeCursor(
            from: begin,
            to: end,
            limit: limit,
            reverse: reverse,
            snapshot: snapshot,
            streamingMode: streamingMode
        )
        return cursor.validatingBeforeAdvance {
            try cancellation.validateAdvance()
        }
    }
}

private struct VersionReadContractMaintainerProvider:
    IndexMaintainerProvider
{
    let indexType: IndexType = .history
    let runtimeRequirements: IndexRuntimeRequirements =
        .entityAndPolymorphicReads

    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        try VersionIndexMaintainerProvider().makeIndexMaintainer(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            configurations: configurations,
            wallClock: wallClock
        )
    }
}

private final class VersionReadCancellation: Sendable {
    private let successfulAdvances = Mutex(0)
    private let failingAfterSuccessfulAdvances: Int

    init(failingAfterSuccessfulAdvances: Int) {
        precondition(failingAfterSuccessfulAdvances >= 0)
        self.failingAfterSuccessfulAdvances = failingAfterSuccessfulAdvances
    }

    func validateAdvance() throws {
        let shouldCancel = successfulAdvances.withLock { count in
            guard count < failingAfterSuccessfulAdvances else {
                return true
            }
            count += 1
            return false
        }
        if shouldCancel {
            throw CancellationError()
        }
    }
}

@Suite("Version read resource contract")
struct VersionReadResourceContractTests {
    @Test("Regular canonical version execution is authorized before storage")
    func regularCanonicalExecutionHonorsAuthorization() async throws {
        let (container, control) = try await makeRegularCanonicalContainer()
        defer { await container.shutdown() }

        let authorized = container.testBaseContext(
            authorization: .authenticated(
                Principal(
                    identifier: "version-authorized",
                    roles: ["version-reader"]
                )
            )
        )
        let history = try await authorized.versions(
            VersionCanonicalDocument.self
        ).forItem("document-1").execute()
        #expect(history.map(\.item.title) == ["published", "draft"])
        #expect(history.allSatisfy { $0.version.bytes.count == 10 })

        let readsBeforeDeniedQuery = control.dataReadOperationCount
        let denied = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "version-denied")
            )
        )
        await #expect(throws: SecurityError.self) {
            _ = try await denied.versions(
                VersionCanonicalDocument.self
            ).forItem("document-1").execute()
        }
        #expect(control.dataReadOperationCount == readsBeforeDeniedQuery)
    }

    @Test("Polymorphic canonical version execution preserves type metadata")
    func polymorphicCanonicalExecutionHonorsAuthorization() async throws {
        let (container, control) = try await makePolymorphicCanonicalContainer()
        defer { await container.shutdown() }

        let authorized = container.testBaseContext(
            authorization: .authenticated(
                Principal(
                    identifier: "polymorphic-version-authorized",
                    roles: ["version-reader"]
                )
            )
        )
        let typeCode = VersionCanonicalArticle.typeCode(
            for: VersionCanonicalArticle.persistableType
        )
        let indexScan = IndexScanSource(
            indexName: "VersionCanonicalDocumentGroup_history",
            indexType: .history,
            parameters: [
                VersionReadParameter.primaryKey: .array([
                    try CanonicalTupleElementCodec.encode(typeCode),
                    try CanonicalTupleElementCodec.encode("article-1"),
                ])
            ]
        )
        let response = try await authorized.findPolymorphic(
            VersionCanonicalArticle.self
        ).query(accessPath: .index(indexScan))
        #expect(response.rows.count == 1)
        let row = try #require(response.rows.first)
        #expect(row.fields["title"] == .string("article"))
        #expect(
            row.annotations[PolymorphicRowAnnotation.typeName]
                == .string(VersionCanonicalArticle.persistableType)
        )
        #expect(
            row.annotations[PolymorphicRowAnnotation.typeCode]
                == .int64(typeCode)
        )
        #expect(row.annotations["version"]?.bytesValue?.count == 10)

        let readsBeforeDeniedQuery = control.dataReadOperationCount
        let denied = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "polymorphic-version-denied")
            )
        )
        await #expect(throws: SecurityError.self) {
            _ = try await denied.findPolymorphic(
                VersionCanonicalArticle.self
            ).query(accessPath: .index(indexScan))
        }
        #expect(control.dataReadOperationCount == readsBeforeDeniedQuery)
    }

    @Test("Retained history preserves newest-first ordering")
    func retainedHistoryPreservesOrdering() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }

        let subspace = Subspace(prefix: Tuple("version", "ordering").pack())
        let beginKey = subspace.pack(Tuple("document"))
        let olderPayload = ByteString(repeating: 1, count: 32)
        let newerPayload = ByteString(repeating: 2, count: 32)
        let timestamp = [UInt8](repeating: 0, count: 12)
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(timestamp + olderPayload.copyBytes()),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 1, count: 10)
                )
            )
            try transaction.setValue(
                ByteString(timestamp + newerPayload.copyBytes()),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 2, count: 10)
                )
            )
        }

        let meter = makeMeter()
        let history = try await engine.withTransaction { transaction in
            try await VersionIndexReader(subspace: subspace).retainedHistory(
                primaryKey: ["document"],
                transaction: transaction,
                snapshot: true,
                workMeter: meter
            )
        }

        #expect(history.count == 2)
        history.withEntry(at: 0) { entry in
            entry.withValues { _, data in
                #expect(data == newerPayload)
            }
        }
        history.withEntry(at: 1) { entry in
            entry.withValues { _, data in
                #expect(data == olderPayload)
            }
        }
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)
    }

    @Test("Retained history keeps payload admission until the final owner releases")
    func historyRetainsPayloadAdmissionUntilFinalOwnerRelease() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }

        let subspace = Subspace(prefix: Tuple("version", "release").pack())
        let beginKey = subspace.pack(Tuple("document"))
        let payload = ByteString(repeating: 7, count: 2_048)
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(
                    [UInt8](repeating: 0, count: 12)
                        + payload.copyBytes()
                ),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 1, count: 10)
                )
            )
        }

        let meter = makeMeter()
        var history: VersionRetainedHistory? = try await engine.withTransaction {
            transaction in
            try await VersionIndexReader(subspace: subspace).retainedHistory(
                primaryKey: ["document"],
                transaction: transaction,
                snapshot: true,
                workMeter: meter
            )
        }
        var escapedData: ByteString?
        history?.withEntry(at: 0) { entry in
            entry.withValues { _, data in
                escapedData = data
            }
        }

        #expect(escapedData == payload)
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)

        history = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes > 0)

        escapedData = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Malformed history releases admission")
    func malformedHistoryReleasesAdmission() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }

        let subspace = Subspace(prefix: Tuple("version", "malformed").pack())
        let beginKey = subspace.pack(Tuple("document"))
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(repeating: 0, count: 11),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 1, count: 10)
                )
            )
        }

        let meter = makeMeter()
        await #expect(
            throws: VersionIndexError.malformedVersionValue(byteCount: 11)
        ) {
            try await engine.withTransaction { transaction in
                _ = try await VersionIndexReader(
                    subspace: subspace
                ).retainedHistory(
                    primaryKey: ["document"],
                    transaction: transaction,
                    snapshot: true,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("History budget failure releases all partial admission")
    func budgetFailureReleasesAdmission() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }

        let subspace = Subspace(prefix: Tuple("version", "budget").pack())
        let beginKey = subspace.pack(Tuple("document"))
        let payload = ByteString(repeating: 0, count: 2_048)
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(
                    [UInt8](repeating: 0, count: 12)
                        + payload.copyBytes()
                ),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 1, count: 10)
                )
            )
            try transaction.setValue(
                ByteString(
                    [UInt8](repeating: 0, count: 12)
                        + payload.copyBytes()
                ),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 2, count: 10)
                )
            )
        }

        let meter = makeMeter(maximumIntermediateBytes: 3_000)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await engine.withTransaction { transaction in
                _ = try await VersionIndexReader(
                    subspace: subspace
                ).retainedHistory(
                    primaryKey: ["document"],
                    transaction: transaction,
                    snapshot: true,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("History cancellation releases all partial admission")
    func cancellationReleasesAdmission() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }

        let subspace = Subspace(prefix: Tuple("version", "cancel").pack())
        let beginKey = subspace.pack(Tuple("document"))
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                ByteString(repeating: 0, count: 12 + 32),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 1, count: 10)
                )
            )
            try transaction.setValue(
                ByteString(repeating: 0, count: 12 + 32),
                for: beginKey.appending(
                    contentsOf: ByteString(repeating: 2, count: 10)
                )
            )
        }

        let meter = makeMeter()
        await #expect(throws: CancellationError.self) {
            try await engine.withTransaction { transaction in
                let access = VersionCancellingReadAccess(
                    base: transaction,
                    failingAfterSuccessfulAdvances: 1
                )
                _ = try await VersionIndexReader(
                    subspace: subspace
                ).retainedHistory(
                    primaryKey: ["document"],
                    transaction: access,
                    snapshot: true,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

private func makeRegularCanonicalContainer() async throws -> (
    container: DBContainer,
    control: StorageTransactionControl
) {
    let storage = ControlledStorageEngine(base: InMemoryEngine())
    let provider = VersionReadContractMaintainerProvider()
    var runtime = try EntityRuntimeDefinition(VersionCanonicalDocument.self)
    try VersionReadExecutors.register(with: &runtime)
    try runtime.register(provider)
    let configuration = try DatabaseRuntimeConfiguration(
        executionIdentity: DatabaseExecutionRuntimeIdentity(
            identifier: "version-canonical-regular-tests",
            revision: 1
        ),
        indexMaintainerProviderDescriptors: [
            .init(describing: provider)
        ],
        entityRuntimes: [runtime.registration()],
        authorizationPolicies: [
            AuthorizationPolicyHandler(VersionCanonicalDocument.self)
        ]
    )
    let container = try await DBContainer.open(
        for: try Schema(
            entities: [try VersionCanonicalDocument.schemaEntity],
            version: Schema.Version(1, 0, 0)
        ),
        configuration: .testing(storageEngine: storage),
        runtimeConfiguration: configuration,
        security: .enabled()
    )
    #if MultiBase
    try await container.grantTestBaseAccess(
        to: .principalRole("version-reader"),
        access: .read
    )
    try await container.grantTestBaseAccess(
        to: .principal("version-denied"),
        access: .read
    )
    #endif
    let entitySubspace = try await container.testBaseDirectory(
        for: VersionCanonicalDocument.self
    )
    let indexSubspace = try IndexLifecycleStore(
        container: container,
        subspace: entitySubspace
    ).indexSubspace(for: "VersionCanonicalDocument_history")
    let first = VersionCanonicalDocument(id: "document-1", title: "draft")
    let second = VersionCanonicalDocument(
        id: "document-1",
        title: "published"
    )
    try await seedHistory(
        container: container,
        subspace: indexSubspace,
        primaryKey: [first.id],
        version: ByteString(repeating: 1, count: 10),
        model: first
    )
    try await seedHistory(
        container: container,
        subspace: indexSubspace,
        primaryKey: [second.id],
        version: ByteString(repeating: 2, count: 10),
        model: second
    )
    return (container, storage.control)
}

private func makePolymorphicCanonicalContainer() async throws -> (
    container: DBContainer,
    control: StorageTransactionControl
) {
    let storage = ControlledStorageEngine(base: InMemoryEngine())
    let provider = VersionReadContractMaintainerProvider()
    var articleRuntime = try EntityRuntimeDefinition(VersionCanonicalArticle.self)
    try VersionReadExecutors.register(with: &articleRuntime)
    try articleRuntime.register(provider)
    var reportRuntime = try EntityRuntimeDefinition(VersionCanonicalReport.self)
    try VersionReadExecutors.register(with: &reportRuntime)
    try reportRuntime.register(provider)
    let configuration = try DatabaseRuntimeConfiguration(
        executionIdentity: DatabaseExecutionRuntimeIdentity(
            identifier: "version-canonical-polymorphic-tests",
            revision: 1
        ),
        indexMaintainerProviderDescriptors: [
            .init(describing: provider)
        ],
        polymorphicIndexReadExecutors: [
            VersionReadExecutors.polymorphicIndexExecutor
        ],
        entityRuntimes: [
            articleRuntime.registration(),
            reportRuntime.registration(),
        ],
        authorizationPolicies: [
            AuthorizationPolicyHandler(VersionCanonicalArticle.self),
            AuthorizationPolicyHandler(VersionCanonicalReport.self),
        ]
    )
    let container = try await DBContainer.open(
        for: try Schema(
            entities: [
                try VersionCanonicalArticle.schemaEntity,
                try VersionCanonicalReport.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        ),
        configuration: .testing(storageEngine: storage),
        runtimeConfiguration: configuration,
        security: .enabled()
    )
    #if MultiBase
    try await container.grantTestBaseAccess(
        to: .principalRole("version-reader"),
        access: .read
    )
    try await container.grantTestBaseAccess(
        to: .principal("polymorphic-version-denied"),
        access: .read
    )
    #endif
    let group = try #require(
        container.schema.polymorphicGroup(
            identifier: VersionCanonicalArticle.polymorphableType
        )
    )
    let groupSubspace = try await container.testBasePolymorphicDirectory(
        for: group.identifier
    )
    let indexSubspace = try IndexLifecycleStore(
        container: container,
        subspace: groupSubspace
    ).indexSubspace(for: "VersionCanonicalDocumentGroup_history")
    let article = VersionCanonicalArticle(id: "article-1", title: "article")
    let typeCode = VersionCanonicalArticle.typeCode(
        for: VersionCanonicalArticle.persistableType
    )
    try await seedHistory(
        container: container,
        subspace: indexSubspace,
        primaryKey: [typeCode, article.id],
        version: ByteString(repeating: 1, count: 10),
        model: article
    )
    return (container, storage.control)
}

private func seedHistory<T: Persistable>(
    container: DBContainer,
    subspace: Subspace,
    primaryKey: [any TupleElement],
    version: ByteString,
    model: T
) async throws {
    let data = try DataAccess.serialize(model)
    let value = ByteString(
        [UInt8](repeating: 0, count: 12) + data.copyBytes()
    )
    let key = subspace.pack(Tuple(primaryKey)).appending(
        contentsOf: version
    )
    try await container.engine.withTransaction { transaction in
        try transaction.setValue(value, for: key)
    }
}

private func makeMeter(
    maximumIntermediateBytes: UInt64 = 64 * 1_024
) -> DatabaseWorkMeter {
    DatabaseWorkMeter(
        budget: ExecutionBudget(
            maximumWorkUnits: 10_000,
            maximumIntermediateRows: 16,
            maximumIntermediateBytes: maximumIntermediateBytes
        ),
        monotonicClock: TestProcessMonotonicClock()
    )
}
