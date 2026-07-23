import Core
@testable import DatabaseEngine
import DatabaseRuntime
import DatabaseValue
import StorageKit
import Testing

@Suite("Database Partition Catalog Tests", .serialized)
struct DatabasePartitionCatalogTests {
    @Test("Dynamic partitions persist and page across container recreation")
    func persistsAndPagesPartitions() async throws {
        let engine = InMemoryEngine()
        let firstContainer = try await makeContainer(engine: engine)

        var firstPath = DirectoryPath<CatalogPartitionedRecord>()
        firstPath.set(\.tenantID, to: "tenant-a")
        _ = try await firstContainer.resolveDirectory(
            for: CatalogPartitionedRecord.self,
            path: firstPath
        )
        _ = try await firstContainer.resolveDirectory(
            for: CatalogPartitionedRecord.self,
            path: firstPath
        )

        var secondPath = DirectoryPath<CatalogPartitionedRecord>()
        secondPath.set(\.tenantID, to: "tenant-b")
        _ = try await firstContainer.resolveDirectory(
            for: CatalogPartitionedRecord.self,
            path: secondPath
        )

        let firstPage = try await firstContainer.partitionCatalogPage(
            entity: CatalogPartitionedRecord.persistableType,
            limit: 1
        )
        #expect(firstPage.entries.count == 1)
        let continuation = try #require(firstPage.continuation)

        let recreatedContainer = try await makeContainer(engine: engine)
        let secondPage = try await recreatedContainer.partitionCatalogPage(
            entity: CatalogPartitionedRecord.persistableType,
            continuation: continuation,
            limit: 1
        )
        #expect(secondPage.entries.count == 1)
        #expect(secondPage.continuation == nil)

        let values = Set(
            (firstPage.entries + secondPage.entries).compactMap {
                $0.partitions.first?.value
            }
        )
        #expect(values == [.string("tenant-a"), .string("tenant-b")])
    }

    @Test("Partition path components preserve DatabaseValue types")
    func preservesPartitionTypes() throws {
        let stringComponent = try CanonicalDirectoryPartitionCodec.encode(
            .string("1")
        )
        let integerComponent = try CanonicalDirectoryPartitionCodec.encode(
            .int64(1)
        )

        #expect(stringComponent != integerComponent)
        #expect(stringComponent.hasPrefix("dbp1-"))
        #expect(integerComponent.hasPrefix("dbp1-"))
    }

    @Test("Missing dynamic partitions are rejected before directory I/O")
    func rejectsMissingPartition() {
        #expect(throws: DirectoryPathError.self) {
            _ = try AnyDirectoryPath(for: CatalogPartitionedRecord.self)
        }
        #expect(throws: DirectoryPathError.self) {
            _ = try AnyDirectoryPath(
                DirectoryPath<CatalogPartitionedRecord>()
            )
        }
    }

    @Test("Catalog continuation is bound to its entity filter")
    func rejectsContinuationForDifferentEntity() async throws {
        let container = try await makeContainer(engine: InMemoryEngine())
        for tenant in ["tenant-a", "tenant-b"] {
            var path = DirectoryPath<CatalogPartitionedRecord>()
            path.set(\.tenantID, to: tenant)
            _ = try await container.resolveDirectory(
                for: CatalogPartitionedRecord.self,
                path: path
            )
        }
        let page = try await container.partitionCatalogPage(limit: 1)
        let continuation = try #require(page.continuation)

        await #expect(throws: DatabasePartitionCatalogError.self) {
            try await container.partitionCatalogPage(
                entity: CatalogPartitionedRecord.persistableType,
                continuation: continuation,
                limit: 1
            )
        }
    }

    private func makeContainer(engine: InMemoryEngine) async throws -> DBContainer {
        try await DBContainer.open(
            for: Schema(
                [CatalogPartitionedRecord.self],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .init(backend: .custom(engine)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(),
            security: .disabled
        )
    }
}
