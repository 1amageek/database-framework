#if !os(WASI)
#if FOUNDATION_DB
// RoundTripDemoTests.swift
// Demonstrates Create -> Read -> Update -> Delete round-trip

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import Core

@Persistable
struct DemoItem: Equatable {
    #Directory<DemoItem>("round_trip_demo_tests")

    var id: String = UUID().uuidString
    var name: String = ""
    var value: Int = 0
    var tags: [String] = []
}

@Suite("Round Trip Demo", .serialized, .heartbeat)
struct RoundTripDemoTests {

    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = Schema([DemoItem.self], version: Schema.Version(1, 0, 0))
        return try await DBContainer(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled,
        )
    }

    @Test("Create -> Read -> Update -> Delete round-trip")
    func roundTrip() async throws {
        let container = try await setupContainer()
        let context = container.newContext()

        let itemId = "demo-\(UUID().uuidString.prefix(8))"

        // 1. CREATE
        try await context.withTransaction { tx in
            var item = DemoItem()
            item.id = itemId
            item.name = "テストアイテム"
            item.value = 42
            item.tags = ["swift", "fdb", "demo"]

            try await tx.save(item)
            print("✅ CREATE: id=\(itemId), name=\(item.name), value=\(item.value)")
        }

        // 2. READ
        try await context.withTransaction { tx in
            let fetched = try await tx.fetch(DemoItem.self, identifiedBy: itemId)
            #expect(fetched != nil)
            #expect(fetched?.name == "テストアイテム")
            #expect(fetched?.value == 42)
            #expect(fetched?.tags == ["swift", "fdb", "demo"])
            print("✅ READ: id=\(fetched!.id), name=\(fetched!.name), value=\(fetched!.value), tags=\(fetched!.tags)")
        }

        // 3. UPDATE
        try await context.withTransaction { tx in
            guard var updated = try await tx.fetch(DemoItem.self, identifiedBy: itemId) else {
                Issue.record("Item not found for update")
                return
            }
            updated.name = "更新されたアイテム"
            updated.value = 100
            updated.tags.append("updated")

            try await tx.save(updated)
            print("✅ UPDATE: name=\(updated.name), value=\(updated.value)")
        }

        // 3.1 READ after UPDATE
        try await context.withTransaction { tx in
            let fetched = try await tx.fetch(DemoItem.self, identifiedBy: itemId)
            #expect(fetched?.name == "更新されたアイテム")
            #expect(fetched?.value == 100)
            #expect(fetched?.tags.contains("updated") == true)
            print("✅ READ after UPDATE: name=\(fetched!.name), value=\(fetched!.value), tags=\(fetched!.tags)")
        }

        // 4. DELETE
        try await context.withTransaction { tx in
            try await tx.delete(DemoItem.self, identifiedBy: itemId)
            print("✅ DELETE: id=\(itemId)")
        }

        // 4.1 READ after DELETE (should be nil)
        try await context.withTransaction { tx in
            let fetched = try await tx.fetch(DemoItem.self, identifiedBy: itemId)
            #expect(fetched == nil)
            print("✅ READ after DELETE: nil (正常に削除されました)")
        }

        print("\n🎉 ラウンドトリップ完了!")
    }
}
#endif

#endif
