// ACORNFilteredSearchTests.swift
// Tests for ACORN filtered vector search functionality

import Testing
import Foundation
import FoundationDB
import Core
import Vector
import TestSupport
@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

struct ACORNTestProduct: Persistable {
    typealias ID = String

    var id: String
    var name: String
    var category: String
    var price: Int
    var embedding: [Float]

    init(
        id: String = UUID().uuidString,
        name: String,
        category: String,
        price: Int,
        embedding: [Float]
    ) {
        self.id = id
        self.name = name
        self.category = category
        self.price = price
        self.embedding = embedding
    }

    static var persistableType: String { "ACORNTestProduct" }
    static var allFields: [String] { ["id", "name", "category", "price", "embedding"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "name": return name
        case "category": return category
        case "price": return price
        case "embedding": return embedding
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<ACORNTestProduct, Value>) -> String {
        switch keyPath {
        case \ACORNTestProduct.id: return "id"
        case \ACORNTestProduct.name: return "name"
        case \ACORNTestProduct.category: return "category"
        case \ACORNTestProduct.price: return "price"
        case \ACORNTestProduct.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<ACORNTestProduct>) -> String {
        switch keyPath {
        case \ACORNTestProduct.id: return "id"
        case \ACORNTestProduct.name: return "name"
        case \ACORNTestProduct.category: return "category"
        case \ACORNTestProduct.price: return "price"
        case \ACORNTestProduct.embedding: return "embedding"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<ACORNTestProduct> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

// MARK: - Test Helper

private struct ACORNTestContext {
    nonisolated(unsafe) let database: any DatabaseProtocol
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: HNSWIndexMaintainer<ACORNTestProduct>
    let dimensions: Int
    let itemsSubspace: Subspace
    let blobsSubspace: Subspace

    init(dimensions: Int = 4, indexName: String = "ACORNTestProduct_embedding") throws {
        self.database = try FDBClient.openDatabase()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(prefix: Tuple("test", "acorn", String(testId)).pack())
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")
        self.blobsSubspace = subspace.subspace("B")

        let kind = VectorIndexKind<ACORNTestProduct>(
            embedding: \.embedding,
            dimensions: dimensions,
            metric: .cosine
        )

        let index = Index(
            name: indexName,
            kind: kind,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            subspaceKey: indexName,
            itemTypes: Set(["ACORNTestProduct"])
        )

        let hnswParams = VectorIndex.HNSWParameters(m: 8, efConstruction: 100, efSearch: 50)

        self.maintainer = HNSWIndexMaintainer<ACORNTestProduct>(
            index: index,
            dimensions: dimensions,
            metric: .cosine,
            subspace: indexSubspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            parameters: hnswParams
        )
    }

    func cleanup() async throws {
        try await database.withTransaction { transaction in
            let (begin, end) = subspace.range()
            transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func insertProduct(_ product: ACORNTestProduct) async throws {
        try await database.withTransaction { transaction in
            // Store the item using ItemStorage
            let itemKey = itemsSubspace.pack(Tuple(product.id))
            let encoder = JSONEncoder()
            let itemData = try encoder.encode(product)

            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            try await storage.write([UInt8](itemData), for: itemKey)

            // Index the vector
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: product,
                transaction: transaction
            )
        }
    }

    func insertProducts(_ products: [ACORNTestProduct]) async throws {
        for product in products {
            try await insertProduct(product)
        }
    }

    func fetchProduct(id: String) async throws -> ACORNTestProduct? {
        try await database.withTransaction { transaction in
            let itemKey = itemsSubspace.pack(Tuple(id))
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace)
            if let data = try await storage.read(for: itemKey) {
                let decoder = JSONDecoder()
                return try decoder.decode(ACORNTestProduct.self, from: Data(data))
            }
            return nil
        }
    }

    func searchWithFilter(
        query: [Float],
        k: Int,
        predicate: @escaping @Sendable (ACORNTestProduct) async throws -> Bool,
        acornParams: ACORNParameters = .default
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            // Create fetch function using ItemStorage for proper envelope handling
            let fetchItem: @Sendable (Tuple, any TransactionProtocol) async throws -> ACORNTestProduct? = { primaryKey, tx in
                guard let id = primaryKey[0] as? String else { return nil }
                let itemKey = self.itemsSubspace.pack(Tuple(id))
                let storage = ItemStorage(transaction: tx, blobsSubspace: self.blobsSubspace)
                if let data = try await storage.read(for: itemKey) {
                    let decoder = JSONDecoder()
                    return try decoder.decode(ACORNTestProduct.self, from: Data(data))
                }
                return nil
            }

            return try await maintainer.searchWithFilter(
                queryVector: query,
                k: k,
                predicate: predicate,
                fetchItem: fetchItem,
                acornParams: acornParams,
                transaction: transaction
            )
        }
    }

    func searchUnfiltered(query: [Float], k: Int) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            try await maintainer.search(queryVector: query, k: k, transaction: transaction)
        }
    }
}

// MARK: - ACORN Parameters Unit Tests

@Suite("ACORN Parameters Unit Tests")
struct ACORNParametersUnitTests {

    @Test("Default parameters")
    func testDefaultParameters() {
        let params = ACORNParameters.default

        #expect(params.expansionFactor == 2, "Default expansion factor should be 2")
        #expect(params.maxPredicateEvaluations == nil, "Default should have no evaluation limit")
    }

    @Test("Custom parameters")
    func testCustomParameters() {
        let params = ACORNParameters(expansionFactor: 5, maxPredicateEvaluations: 100)

        #expect(params.expansionFactor == 5)
        #expect(params.maxPredicateEvaluations == 100)
    }
}

// MARK: - ACORN Integration Tests

@Suite("ACORN Filtered Search Tests", .tags(.fdb), .serialized)
struct ACORNFilteredSearchTests {

    // Helper to create normalized unit vectors
    private func normalizedVector(_ components: [Float]) -> [Float] {
        let magnitude = sqrt(components.reduce(0) { $0 + $1 * $1 })
        return components.map { $0 / magnitude }
    }

    @Test("Basic filtered search")
    func testBasicFilteredSearch() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        // Create products with different categories
        let products = [
            ACORNTestProduct(
                id: "p1", name: "Laptop", category: "electronics", price: 1000,
                embedding: normalizedVector([1.0, 0.0, 0.0, 0.0])
            ),
            ACORNTestProduct(
                id: "p2", name: "Phone", category: "electronics", price: 500,
                embedding: normalizedVector([0.9, 0.1, 0.0, 0.0])
            ),
            ACORNTestProduct(
                id: "p3", name: "Chair", category: "furniture", price: 200,
                embedding: normalizedVector([0.8, 0.2, 0.0, 0.0])
            ),
            ACORNTestProduct(
                id: "p4", name: "Desk", category: "furniture", price: 300,
                embedding: normalizedVector([0.7, 0.3, 0.0, 0.0])
            )
        ]

        try await ctx.insertProducts(products)

        // Query vector close to p1
        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Search with category filter
        let results = try await ctx.searchWithFilter(
            query: queryVector,
            k: 10,
            predicate: { product in product.category == "electronics" }
        )

        // Should only return electronics products
        #expect(results.count == 2, "Should find exactly 2 electronics products")

        for result in results {
            let id = result.primaryKey.first as? String
            #expect(id == "p1" || id == "p2", "Result should be an electronics product")
        }

        try await ctx.cleanup()
    }

    @Test("Filtered search respects distance ordering")
    func testFilteredSearchRespectsDistanceOrdering() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        // Create electronics products at varying distances
        let products = [
            ACORNTestProduct(
                id: "close", name: "Close", category: "electronics", price: 100,
                embedding: normalizedVector([1.0, 0.0, 0.0, 0.0])
            ),
            ACORNTestProduct(
                id: "medium", name: "Medium", category: "electronics", price: 200,
                embedding: normalizedVector([0.7, 0.7, 0.0, 0.0])
            ),
            ACORNTestProduct(
                id: "far", name: "Far", category: "electronics", price: 300,
                embedding: normalizedVector([0.0, 1.0, 0.0, 0.0])
            ),
            // Furniture (should be filtered out even though close)
            ACORNTestProduct(
                id: "furniture", name: "Furniture", category: "furniture", price: 50,
                embedding: normalizedVector([0.99, 0.01, 0.0, 0.0])
            )
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        let results = try await ctx.searchWithFilter(
            query: queryVector,
            k: 10,
            predicate: { product in product.category == "electronics" }
        )

        #expect(results.count == 3, "Should find 3 electronics products")

        // Verify ordering by distance
        if results.count >= 2 {
            for i in 0..<(results.count - 1) {
                #expect(results[i].distance <= results[i + 1].distance,
                        "Results should be ordered by distance")
            }
        }

        // First result should be "close"
        if let firstId = results.first?.primaryKey.first as? String {
            #expect(firstId == "close", "Closest matching product should be first")
        }

        try await ctx.cleanup()
    }

    @Test("Complex predicate filter")
    func testComplexPredicateFilter() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        let products = [
            ACORNTestProduct(id: "p1", name: "Cheap Electronics", category: "electronics", price: 100,
                             embedding: normalizedVector([1.0, 0.0, 0.0, 0.0])),
            ACORNTestProduct(id: "p2", name: "Expensive Electronics", category: "electronics", price: 2000,
                             embedding: normalizedVector([0.9, 0.1, 0.0, 0.0])),
            ACORNTestProduct(id: "p3", name: "Cheap Furniture", category: "furniture", price: 50,
                             embedding: normalizedVector([0.8, 0.2, 0.0, 0.0])),
            ACORNTestProduct(id: "p4", name: "Mid Furniture", category: "furniture", price: 500,
                             embedding: normalizedVector([0.7, 0.3, 0.0, 0.0]))
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Complex filter: electronics under $1000 OR furniture under $100
        let results = try await ctx.searchWithFilter(
            query: queryVector,
            k: 10,
            predicate: { product in
                (product.category == "electronics" && product.price < 1000) ||
                (product.category == "furniture" && product.price < 100)
            }
        )

        #expect(results.count == 2, "Should find 2 products matching complex predicate")

        let ids = results.compactMap { $0.primaryKey.first as? String }
        #expect(ids.contains("p1"), "Should include cheap electronics")
        #expect(ids.contains("p3"), "Should include cheap furniture")

        try await ctx.cleanup()
    }

    @Test("Filter with k limit")
    func testFilterWithKLimit() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        // Create 10 electronics products
        var products: [ACORNTestProduct] = []
        for i in 0..<10 {
            let angle = Float(i) * 0.1
            products.append(ACORNTestProduct(
                id: "p\(i)", name: "Product \(i)", category: "electronics", price: i * 100,
                embedding: normalizedVector([cos(angle), sin(angle), 0.0, 0.0])
            ))
        }

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Request only k=3
        let results = try await ctx.searchWithFilter(
            query: queryVector,
            k: 3,
            predicate: { _ in true }  // Accept all
        )

        #expect(results.count == 3, "Should return exactly k=3 results")

        try await ctx.cleanup()
    }

    @Test("Filter that excludes all")
    func testFilterExcludesAll() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        let products = [
            ACORNTestProduct(id: "p1", name: "Product 1", category: "electronics", price: 100,
                             embedding: normalizedVector([1.0, 0.0, 0.0, 0.0])),
            ACORNTestProduct(id: "p2", name: "Product 2", category: "electronics", price: 200,
                             embedding: normalizedVector([0.9, 0.1, 0.0, 0.0]))
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Filter that matches nothing
        let results = try await ctx.searchWithFilter(
            query: queryVector,
            k: 10,
            predicate: { product in product.category == "nonexistent" }
        )

        #expect(results.isEmpty, "Should return empty results when filter excludes all")

        try await ctx.cleanup()
    }

    @Test("ACORN expansion factor affects results")
    func testACORNExpansionFactorAffectsResults() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        // Create a mix of products
        var products: [ACORNTestProduct] = []
        for i in 0..<20 {
            let category = i % 3 == 0 ? "target" : "other"
            let angle = Float(i) * 0.15
            products.append(ACORNTestProduct(
                id: "p\(i)", name: "Product \(i)", category: category, price: i * 50,
                embedding: normalizedVector([cos(angle), sin(angle), 0.0, 0.0])
            ))
        }

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Low expansion factor
        let resultsLow = try await ctx.searchWithFilter(
            query: queryVector,
            k: 5,
            predicate: { product in product.category == "target" },
            acornParams: ACORNParameters(expansionFactor: 1)
        )

        // High expansion factor
        let resultsHigh = try await ctx.searchWithFilter(
            query: queryVector,
            k: 5,
            predicate: { product in product.category == "target" },
            acornParams: ACORNParameters(expansionFactor: 5)
        )

        // Both should return results for "target" category
        #expect(!resultsLow.isEmpty, "Low expansion should find some results")
        #expect(!resultsHigh.isEmpty, "High expansion should find some results")

        // High expansion factor should generally find more or equal results
        // (better recall at cost of more evaluations)
        #expect(resultsHigh.count >= resultsLow.count,
                "Higher expansion factor should give equal or better recall")

        try await ctx.cleanup()
    }

    @Test("Comparison: filtered vs unfiltered search")
    func testComparisonFilteredVsUnfiltered() async throws {
        try await FDBTestSetup.shared.initialize()
        let ctx = try ACORNTestContext(dimensions: 4)

        let products = [
            ACORNTestProduct(id: "e1", name: "Electronics 1", category: "electronics", price: 100,
                             embedding: normalizedVector([1.0, 0.0, 0.0, 0.0])),
            ACORNTestProduct(id: "f1", name: "Furniture 1", category: "furniture", price: 200,
                             embedding: normalizedVector([0.95, 0.05, 0.0, 0.0])),
            ACORNTestProduct(id: "e2", name: "Electronics 2", category: "electronics", price: 300,
                             embedding: normalizedVector([0.9, 0.1, 0.0, 0.0])),
            ACORNTestProduct(id: "f2", name: "Furniture 2", category: "furniture", price: 400,
                             embedding: normalizedVector([0.85, 0.15, 0.0, 0.0]))
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Unfiltered search
        let unfilteredResults = try await ctx.searchUnfiltered(query: queryVector, k: 10)

        // Filtered search (only electronics)
        let filteredResults = try await ctx.searchWithFilter(
            query: queryVector,
            k: 10,
            predicate: { product in product.category == "electronics" }
        )

        #expect(unfilteredResults.count == 4, "Unfiltered should return all 4 products")
        #expect(filteredResults.count == 2, "Filtered should return only 2 electronics")

        // Filtered results should all be electronics
        for result in filteredResults {
            let id = result.primaryKey.first as? String
            #expect(id?.starts(with: "e") == true, "Filtered result should be electronics")
        }

        // First unfiltered result is e1, but f1 is closer than e2
        // With filter, e1 and e2 should be the top 2
        let filteredIds = Set(filteredResults.compactMap { $0.primaryKey.first as? String })
        #expect(filteredIds == Set(["e1", "e2"]), "Should find both electronics products")

        try await ctx.cleanup()
    }
}
