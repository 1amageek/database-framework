import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import BitmapIndex

@Persistable(type: "BitmapAuthorizationDeniedItem")
private struct BitmapAuthorizationDeniedItem: SecurityPolicy {
    #Index(
        .bitmap(
            name: "bitmap_authorization_denied_status",
            field: \BitmapAuthorizationDeniedItem.status
        )
    )

    var id: String
    var status: String

    static func permitsRead(
        of resource: borrowing BitmapAuthorizationDeniedItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        false
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        false
    }

    static func permitsCreate(
        _ newResource: borrowing BitmapAuthorizationDeniedItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsUpdate(
        from resource: borrowing BitmapAuthorizationDeniedItem,
        to newResource: borrowing BitmapAuthorizationDeniedItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsDelete(
        _ resource: borrowing BitmapAuthorizationDeniedItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }
}

@Polymorphable(identifier: "BitmapAuthorizationPolymorphicDocument")
@PolymorphicDirectory("bitmap-authorization-polymorphic")
@PolymorphicIndex(
    .bitmap(
        name: "bitmap_authorization_polymorphic_status",
        field: "status"
    )
)
private protocol BitmapAuthorizationPolymorphicDocument:
    Polymorphable<BitmapAuthorizationPolymorphicDocumentPolymorphicGroup>
{
    var id: String { get }
    var status: String { get }
}

@Persistable(type: "BitmapAuthorizationPolymorphicItem")
private struct BitmapAuthorizationPolymorphicItem:
    BitmapAuthorizationPolymorphicDocument,
    SecurityPolicy
{
    #Directory<BitmapAuthorizationPolymorphicItem>(
        "bitmap-authorization-polymorphic-item"
    )

    var id: String
    var status: String

    static func permitsRead(
        of resource: borrowing BitmapAuthorizationPolymorphicItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier != "bitmap-polymorphic-denied"
    }

    static func permitsCreate(
        _ newResource: borrowing BitmapAuthorizationPolymorphicItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsUpdate(
        from resource: borrowing BitmapAuthorizationPolymorphicItem,
        to newResource: borrowing BitmapAuthorizationPolymorphicItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }

    static func permitsDelete(
        _ resource: borrowing BitmapAuthorizationPolymorphicItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        true
    }
}

@Suite("Bitmap canonical read execution", .serialized)
struct BitmapReadExecutionContractTests {
    @Test("regular and polymorphic readers register canonical executors")
    func readersRegisterCanonicalExecutors() throws {
        let provider = BitmapIndexMaintainerProvider()
        var definition = try EntityRuntimeDefinition(
            BitmapAuthorizationDeniedItem.self
        )
        try BitmapReadExecutors.register(with: &definition)
        try definition.register(provider)
        let registration = definition.registration()

        #expect(registration.hasIndexReader(for: .bitmap))
        #expect(registration.hasIndexProvider(for: .bitmap))

        let readRegistry = try ReadExecutorRegistry(
            polymorphicIndexExecutors: [
                BitmapReadExecutors.polymorphicIndexExecutor
            ]
        )
        #expect(
            readRegistry.polymorphicIndexExecutor(for: .bitmap) != nil
        )
    }

    @Test("regular direct execution denies before bounded storage reads")
    func regularAuthorizationDeniesBeforeStorage() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let provider = BitmapIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(
            BitmapAuthorizationDeniedItem.self
        )
        try BitmapReadExecutors.register(with: &runtime)
        try runtime.register(provider)
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [try BitmapAuthorizationDeniedItem.schemaEntity]
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "bitmap-authorization-denied",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                entityRuntimes: [runtime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        BitmapAuthorizationDeniedItem.self
                    )
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "bitmap-denied")
            )
        )
        let readsBefore = storage.control.dataReadOperationCount
        await #expect(throws: SecurityError.self) {
            _ = try await context.bitmap(
                BitmapAuthorizationDeniedItem.self
            )
            .field(BitmapAuthorizationDeniedItem.fields.status)
            .equals("active")
            .executeDirect()
        }
        #expect(storage.control.dataReadOperationCount == readsBefore)
    }

    @Test("polymorphic canonical execution uses the retained bitmap path")
    func polymorphicCanonicalExecutionUsesRetainedPath() async throws {
        let (container, storage) = try await makePolymorphicContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            BitmapAuthorizationPolymorphicItem(
                id: "poly-active",
                status: "active"
            )
        )
        try await context.save()

        let query = try bitmapPolymorphicQuery()
        let readsBefore = storage.control.dataReadOperationCount
        let response = try await context.query(query)
        #expect(response.rows.count == 1)
        #expect(response.rows[0].fields["id"] == .string("poly-active"))
        #expect(response.rows[0].fields["status"] == .string("active"))
        #expect(storage.control.dataReadOperationCount > readsBefore)
    }

    @Test("polymorphic authorization denies before bounded storage reads")
    func polymorphicAuthorizationDeniesBeforeStorage() async throws {
        let (container, storage) = try await makePolymorphicContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "bitmap-polymorphic-denied")
            )
        )
        let readsBefore = storage.control.dataReadOperationCount
        await #expect(throws: SecurityError.self) {
            _ = try await context.query(try bitmapPolymorphicQuery())
        }
        #expect(storage.control.dataReadOperationCount == readsBefore)
    }

    private func makePolymorphicContainer() async throws -> (
        DBContainer,
        ControlledStorageEngine<InMemoryEngine>
    ) {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let provider = BitmapIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(
            BitmapAuthorizationPolymorphicItem.self
        )
        try BitmapReadExecutors.register(with: &runtime)
        try runtime.register(provider)
        let container = try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try BitmapAuthorizationPolymorphicItem.schemaEntity
                ]
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "bitmap-polymorphic-authorization",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                polymorphicIndexReadExecutors: [
                    BitmapReadExecutors.polymorphicIndexExecutor
                ],
                entityRuntimes: [runtime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        BitmapAuthorizationPolymorphicItem.self
                    )
                ]
            ),
            security: .enabled()
        )
        return (container, storage)
    }

    private func bitmapPolymorphicQuery() throws -> SelectQuery {
        let groupIdentifier =
            BitmapAuthorizationPolymorphicItem.polymorphableType
        return SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: LogicalSourceKind.polymorphic,
                    identifier: groupIdentifier
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "bitmap_authorization_polymorphic_status",
                    indexType: .bitmap,
                    parameters: [
                        BitmapReadParameter.fieldName: .string("status"),
                        BitmapReadParameter.operation: .string(
                            BitmapReadParameter.equalsOperation
                        ),
                        BitmapReadParameter.values: .array([
                            .string("active")
                        ])
                    ]
                )
            )
        )
    }
}
