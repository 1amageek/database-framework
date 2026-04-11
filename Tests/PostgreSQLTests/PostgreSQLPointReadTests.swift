#if POSTGRESQL
// PostgreSQLPointReadTests.swift
// Point read behavior tests against PostgreSQL backend

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import Core
import TestSupport

@Persistable
private struct PGPointReadItem: Equatable {
    #Directory<PGPointReadItem>("test", "pg", "point-read")

    var id: String = UUID().uuidString
    var name: String = ""
    var value: Int = 0
}

@Persistable
private struct PGSecuredPointReadItem: Equatable, SecurityPolicy {
    #Directory<PGSecuredPointReadItem>("test", "pg", "secured-point-read")

    var id: String = UUID().uuidString
    var ownerID: String = ""
    var name: String = ""

    static func allowGet(resource: PGSecuredPointReadItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<PGSecuredPointReadItem>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: PGSecuredPointReadItem, auth: (any AuthContext)?) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(
        resource: PGSecuredPointReadItem,
        newResource: PGSecuredPointReadItem,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID && newResource.ownerID == auth?.userID
    }

    static func allowDelete(resource: PGSecuredPointReadItem, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }
}

private struct PGTestAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

@Suite("PostgreSQL Point Read Tests", .serialized, .heartbeat)
struct PostgreSQLPointReadTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupStaticContainer() async throws -> DBContainer {
        let schema = Schema([PGPointReadItem.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    private func setupPartitionedContainer() async throws -> DBContainer {
        let schema = Schema([TenantOrder.self], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLTestSetup.shared.makeContainer(schema: schema)
    }

    private func setupSecuredContainer() async throws -> DBContainer {
        let engine = try await PostgreSQLTestSetup.shared.engine
        let schema = Schema([PGSecuredPointReadItem.self], version: Schema.Version(1, 0, 0))
        return try await DBContainer(
            for: schema,
            configuration: .init(backend: .custom(engine)),
            security: .enabled(strict: true)
        )
    }

    @Test("DataStore.fetch(id:) returns item, supports tuple-wrapped ids, and returns nil for missing key")
    func staticPointRead() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupStaticContainer()
            let context = container.newContext()

            let itemID = uniqueID("point")
            var item = PGPointReadItem()
            item.id = itemID
            item.name = "stored"
            item.value = 42
            context.insert(item)
            try await context.save()

            let store = try await container.store(for: PGPointReadItem.self)
            let fetched = try await store.fetch(PGPointReadItem.self, id: itemID)
            let tupleFetched = try await store.fetch(
                PGPointReadItem.self,
                id: Tuple([itemID as any TupleElement])
            )
            let missing = try await store.fetch(PGPointReadItem.self, id: uniqueID("missing"))

            #expect(fetched?.id == itemID)
            #expect(fetched?.name == "stored")
            #expect(fetched?.value == 42)
            #expect(tupleFetched?.id == itemID)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.executeBatch single-item path preserves upsert and delete semantics")
    func dataStoreSingleItemExecuteBatch() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupStaticContainer()
            let store = try await container.store(for: PGPointReadItem.self)

            let itemID = uniqueID("batch")
            var item = PGPointReadItem()
            item.id = itemID
            item.name = "created"
            item.value = 1
            try await store.executeBatch(inserts: [item], deletes: [])

            var updated = item
            updated.name = "updated"
            updated.value = 2
            try await store.executeBatch(inserts: [updated], deletes: [])

            let fetched = try await store.fetch(PGPointReadItem.self, id: itemID)
            #expect(fetched?.name == "updated")
            #expect(fetched?.value == 2)

            try await store.executeBatch(inserts: [], deletes: [updated])
            let missing = try await store.fetch(PGPointReadItem.self, id: itemID)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.fetch(id:) respects resolved partition path")
    func partitionedPointRead() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupPartitionedContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")
            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 55.0)
            order.id = orderID
            context.insert(order)
            try await context.save()

            var path = DirectoryPath<TenantOrder>()
            path.set(\.tenantID, to: tenantID)
            let store = try await container.store(for: TenantOrder.self, path: path)
            let fetched = try await store.fetch(TenantOrder.self, id: orderID)

            var wrongPath = DirectoryPath<TenantOrder>()
            wrongPath.set(\.tenantID, to: uniqueID("other"))
            let wrongStore = try await container.store(for: TenantOrder.self, path: wrongPath)
            let missing = try await wrongStore.fetch(TenantOrder.self, id: orderID)

            #expect(fetched?.id == orderID)
            #expect(fetched?.tenantID == tenantID)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.fetch(id:) preserves GET security checks on point-read fast path")
    func securedPointRead() async throws {
        try await PostgreSQLTestSetup.shared.withSerializedAccess {
            let container = try await setupSecuredContainer()
            let itemID = uniqueID("secure")

            try await AuthContextKey.$current.withValue(PGTestAuth(userID: "owner")) {
                let context = container.newContext()
                var item = PGSecuredPointReadItem()
                item.id = itemID
                item.ownerID = "owner"
                item.name = "secret"
                context.insert(item)
                try await context.save()
            }

            let store = try await container.store(for: PGSecuredPointReadItem.self)

            let authorized = try await AuthContextKey.$current.withValue(PGTestAuth(userID: "owner")) {
                try await store.fetch(PGSecuredPointReadItem.self, id: itemID)
            }
            #expect(authorized?.id == itemID)

            await #expect(throws: SecurityError.self) {
                try await AuthContextKey.$current.withValue(PGTestAuth(userID: "intruder")) {
                    _ = try await store.fetch(PGSecuredPointReadItem.self, id: itemID)
                }
            }
        }
    }
}
#endif
