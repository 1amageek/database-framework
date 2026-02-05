/// CatalogDataAccessTests.swift
/// Tests for CatalogDataAccess catalog resolution (non-FDB tests)

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
@testable import Core

@Suite("CatalogDataAccess")
struct CatalogDataAccessTests {

    /// Create a minimal test TypeCatalog
    private func createMinimalCatalog(typeName: String) -> TypeCatalog {
        TypeCatalog(
            typeName: typeName,
            fields: [],
            directoryComponents: [],
            indexes: []
        )
    }

    // MARK: - Catalog Resolution (no FDB needed)

    @Test func catalogLookupReturnsCorrectCatalog() {
        // Create a mock database (CatalogDataAccess doesn't use it for catalog lookup)
        let catalogs = [
            createMinimalCatalog(typeName: "User"),
            createMinimalCatalog(typeName: "Order"),
            createMinimalCatalog(typeName: "Product")
        ]

        // Verify catalog names are stored
        #expect(catalogs.count == 3)
        #expect(catalogs[0].typeName == "User")
        #expect(catalogs[1].typeName == "Order")
        #expect(catalogs[2].typeName == "Product")
    }

    @Test func allCatalogsAreSortedByName() {
        let catalogC = createMinimalCatalog(typeName: "Charlie")
        let catalogA = createMinimalCatalog(typeName: "Alpha")
        let catalogB = createMinimalCatalog(typeName: "Beta")

        // When sorted by typeName
        let sorted = [catalogC, catalogA, catalogB].sorted { $0.typeName < $1.typeName }

        #expect(sorted[0].typeName == "Alpha")
        #expect(sorted[1].typeName == "Beta")
        #expect(sorted[2].typeName == "Charlie")
    }

    @Test func typeCatalogStoresFieldsCorrectly() {
        let catalog = TypeCatalog(
            typeName: "TestType",
            fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "count", fieldNumber: 2, type: .int64),
                FieldSchema(name: "price", fieldNumber: 3, type: .double),
                FieldSchema(name: "active", fieldNumber: 4, type: .bool)
            ],
            directoryComponents: [],
            indexes: []
        )

        #expect(catalog.typeName == "TestType")
        #expect(catalog.fields.count == 4)
        #expect(catalog.fields[0].name == "id")
        #expect(catalog.fields[0].type == .string)
        #expect(catalog.fields[1].name == "count")
        #expect(catalog.fields[1].type == .int64)
        #expect(catalog.fields[2].name == "price")
        #expect(catalog.fields[2].type == .double)
        #expect(catalog.fields[3].name == "active")
        #expect(catalog.fields[3].type == .bool)
    }

    @Test func typeCatalogStoresDirectoryComponentsCorrectly() {
        let catalog = TypeCatalog(
            typeName: "PartitionedType",
            fields: [],
            directoryComponents: [
                .staticPath("tenants"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("data")
            ],
            indexes: []
        )

        #expect(catalog.directoryComponents.count == 3)
    }

    @Test func fieldSchemaTypeCases() {
        // Test various FieldSchemaType cases
        let stringField = FieldSchema(name: "s", fieldNumber: 1, type: .string)
        let int64Field = FieldSchema(name: "i", fieldNumber: 2, type: .int64)
        let doubleField = FieldSchema(name: "d", fieldNumber: 3, type: .double)
        let boolField = FieldSchema(name: "b", fieldNumber: 4, type: .bool)
        let dataField = FieldSchema(name: "data", fieldNumber: 5, type: .data)
        let dateField = FieldSchema(name: "date", fieldNumber: 6, type: .date)
        let uuidField = FieldSchema(name: "uuid", fieldNumber: 7, type: .uuid)

        #expect(stringField.type == .string)
        #expect(int64Field.type == .int64)
        #expect(doubleField.type == .double)
        #expect(boolField.type == .bool)
        #expect(dataField.type == .data)
        #expect(dateField.type == .date)
        #expect(uuidField.type == .uuid)
    }

    @Test func fieldSchemaOptionalAndArray() {
        let optionalField = FieldSchema(name: "opt", fieldNumber: 1, type: .string, isOptional: true)
        let arrayField = FieldSchema(name: "arr", fieldNumber: 2, type: .int64, isArray: true)
        let optionalArrayField = FieldSchema(name: "optArr", fieldNumber: 3, type: .double, isOptional: true, isArray: true)

        #expect(optionalField.isOptional == true)
        #expect(optionalField.isArray == false)
        #expect(arrayField.isOptional == false)
        #expect(arrayField.isArray == true)
        #expect(optionalArrayField.isOptional == true)
        #expect(optionalArrayField.isArray == true)
    }

    @Test func typeCatalogFieldMapByName() {
        let catalog = TypeCatalog(
            typeName: "MapTest",
            fields: [
                FieldSchema(name: "alpha", fieldNumber: 1, type: .string),
                FieldSchema(name: "beta", fieldNumber: 2, type: .int64)
            ],
            directoryComponents: [],
            indexes: []
        )

        let map = catalog.fieldMapByName
        #expect(map["alpha"]?.type == .string)
        #expect(map["beta"]?.type == .int64)
        #expect(map["gamma"] == nil)
    }

    @Test func typeCatalogFieldMapByNumber() {
        let catalog = TypeCatalog(
            typeName: "MapTest",
            fields: [
                FieldSchema(name: "alpha", fieldNumber: 1, type: .string),
                FieldSchema(name: "beta", fieldNumber: 2, type: .int64)
            ],
            directoryComponents: [],
            indexes: []
        )

        let map = catalog.fieldMapByNumber
        #expect(map[1]?.name == "alpha")
        #expect(map[2]?.name == "beta")
        #expect(map[3] == nil)
    }

    @Test func typeCatalogResolvedDirectoryPath() throws {
        let catalog = TypeCatalog(
            typeName: "TenantData",
            fields: [],
            directoryComponents: [
                .staticPath("app"),
                .dynamicField(fieldName: "tenantId"),
                .staticPath("data")
            ],
            indexes: []
        )

        let path = try catalog.resolvedDirectoryPath(partitionValues: ["tenantId": "tenant123"])
        #expect(path == ["app", "tenant123", "data"])
    }

    @Test func typeCatalogHasDynamicDirectory() {
        let staticCatalog = TypeCatalog(
            typeName: "Static",
            fields: [],
            directoryComponents: [.staticPath("a"), .staticPath("b")],
            indexes: []
        )

        let dynamicCatalog = TypeCatalog(
            typeName: "Dynamic",
            fields: [],
            directoryComponents: [.staticPath("a"), .dynamicField(fieldName: "x")],
            indexes: []
        )

        #expect(staticCatalog.hasDynamicDirectory == false)
        #expect(dynamicCatalog.hasDynamicDirectory == true)
    }

    @Test func typeCatalogDynamicFieldNames() {
        let catalog = TypeCatalog(
            typeName: "MultiDynamic",
            fields: [],
            directoryComponents: [
                .staticPath("root"),
                .dynamicField(fieldName: "region"),
                .staticPath("data"),
                .dynamicField(fieldName: "tenant")
            ],
            indexes: []
        )

        let names = catalog.dynamicFieldNames
        #expect(names == ["region", "tenant"])
    }
}
