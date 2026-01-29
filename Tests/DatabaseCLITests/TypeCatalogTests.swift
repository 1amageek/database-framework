import Testing
@testable import DatabaseEngine

@Suite("TypeCatalog directory resolution")
struct TypeCatalogTests {

    // MARK: - resolvedDirectoryPath

    @Test func staticOnly() throws {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath()
        #expect(path == ["app", "users"])
    }

    @Test func withDynamicField() throws {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("orders"),
            ],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath(partitionValues: ["tenantId": "t_123"])
        #expect(path == ["tenants", "t_123", "orders"])
    }

    @Test func multipleDynamicFields() throws {
        let catalog = TypeCatalog(
            typeName: "Message",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "accountId"),
                .staticPath("channels"),
                .dynamicField(fieldName: "channelId"),
                .staticPath("messages"),
            ],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath(partitionValues: [
            "accountId": "acc_1",
            "channelId": "ch_2",
        ])
        #expect(path == ["tenants", "acc_1", "channels", "ch_2", "messages"])
    }

    @Test func missingPartitionValueThrows() {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("orders"),
            ],
            indexes: []
        )
        #expect(throws: (any Error).self) {
            _ = try catalog.resolvedDirectoryPath()
        }
    }

    @Test func emptyComponents() throws {
        let catalog = TypeCatalog(
            typeName: "Simple",
            fields: [],
            directoryComponents: [],
            indexes: []
        )
        let path = try catalog.resolvedDirectoryPath()
        #expect(path.isEmpty)
    }

    // MARK: - hasDynamicDirectory

    @Test func staticOnlyIsNotDynamic() {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app"), .staticPath("users")],
            indexes: []
        )
        #expect(!catalog.hasDynamicDirectory)
    }

    @Test func withDynamicFieldIsDynamic() {
        let catalog = TypeCatalog(
            typeName: "Order",
            fields: [],
            directoryComponents: [.staticPath("tenants"), .dynamicField(fieldName: "tenantId")],
            indexes: []
        )
        #expect(catalog.hasDynamicDirectory)
    }

    // MARK: - dynamicFieldNames

    @Test func dynamicFieldNamesExtraction() {
        let catalog = TypeCatalog(
            typeName: "Message",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "accountId"),
                .staticPath("channels"),
                .dynamicField(fieldName: "channelId"),
            ],
            indexes: []
        )
        #expect(catalog.dynamicFieldNames == ["accountId", "channelId"])
    }

    @Test func noDynamicFieldNames() {
        let catalog = TypeCatalog(
            typeName: "User",
            fields: [],
            directoryComponents: [.staticPath("app")],
            indexes: []
        )
        #expect(catalog.dynamicFieldNames.isEmpty)
    }
}
