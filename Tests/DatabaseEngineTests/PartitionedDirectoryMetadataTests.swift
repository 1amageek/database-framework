import DatabaseKit
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("Partitioned directory metadata")
struct PartitionedDirectoryMetadataTests {
    @Test("Persistable metadata distinguishes dynamic and static directories")
    func persistableDirectoryMetadata() {
        #expect(TenantOrder.hasDynamicDirectory)
        #expect(TenantOrder.directoryFieldNames == ["tenantID"])
        #expect(!Player.hasDynamicDirectory)
        #expect(Player.directoryFieldNames.isEmpty)
    }

    @Test("DirectoryPath rejects a missing partition value")
    func missingPartitionValueFailsValidation() {
        let path = DirectoryPath<TenantOrder>()

        #expect(throws: DirectoryPathError.self) {
            try path.validate()
        }
    }

    @Test("DirectoryPath accepts every required partition value")
    func completePartitionValuesPassValidation() throws {
        var path = DirectoryPath<TenantOrder>()
        path.set(TenantOrder.fields.tenantID, to: "tenant_123")

        try path.validate()
    }

    @Test("DirectoryPath extracts partition values from a model")
    func partitionValuesComeFromModel() throws {
        let order = TenantOrder.fixture(
            tenantID: "tenant_xyz",
            status: "pending",
            total: 50.0
        )
        let path = try DirectoryPath<TenantOrder>.from(order)

        #expect(
            try path.value(for: TenantOrder.fields.tenantID) == "tenant_xyz"
        )
    }
}
