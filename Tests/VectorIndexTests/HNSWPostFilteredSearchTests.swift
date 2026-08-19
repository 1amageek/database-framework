// HNSWPostFilteredSearchTests.swift
// Tests for expanded HNSW candidate search followed by predicate evaluation

import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestHeartbeat
import Testing

@testable import DatabaseEngine
@testable import VectorIndex

// MARK: - Test Model

@Persistable
struct PostFilteredProduct {
    var id: String
    var name: String
    var category: String
    var price: Int64
    var embedding: Vector
}

// MARK: - Post-Filtered Search Context

private struct PostFilteredSearchContext {
    let database: any StorageEngine
    let subspace: Subspace
    let indexSubspace: Subspace
    let maintainer: HNSWIndexMaintainer<PostFilteredProduct>
    let dimensions: Int
    let itemsSubspace: Subspace
    let blobsSubspace: Subspace

    init(
        dimensions: Int = 4,
        indexName: String = "PostFilteredProduct_embedding"
    ) async throws {
        self.database = InMemoryEngine()
        self.dimensions = dimensions
        let testId = UUID().uuidString.prefix(8)
        self.subspace = Subspace(
            prefix: Tuple("test", "post-filter", String(testId)).pack()
        )
        self.indexSubspace = subspace.subspace("I").subspace(indexName)
        self.itemsSubspace = subspace.subspace("R")
        self.blobsSubspace = subspace.subspace("B")

        let definition = vectorIndexDefinition(
            fieldNumber: 5,
            dimensions: dimensions,
            metric: .cosine
        )

        let index = try ResolvedIndex(
            for: PostFilteredProduct.self,
            name: indexName,
            definition: definition,
            rootExpression: FieldKeyExpression(fieldName: "embedding"),
            itemTypes: Set(["PostFilteredProduct"])
        )

        let hnswParams = VectorIndex.HNSWParameters(m: 8, efConstruction: 100, efSearch: 50)

        self.maintainer = HNSWIndexMaintainer<PostFilteredProduct>(
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
            try transaction.clearRange(beginKey: begin, endKey: end)
        }
    }

    func insertProduct(_ product: PostFilteredProduct) async throws {
        try await database.withTransaction { transaction in
            // Store the item using ItemStorage
            let itemKey = itemsSubspace.pack(try product.persistableIdentifierTuple())
            let itemData = try PersistableStorageCodec.encode(product)

            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            try await storage.write(itemData, for: itemKey)

            // Index the vector
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: product,
                transaction: transaction
            )
        }
    }

    func insertProducts(_ products: [PostFilteredProduct]) async throws {
        for product in products {
            try await insertProduct(product)
        }
    }

    func fetchProduct(id: String) async throws -> PostFilteredProduct? {
        try await database.withTransaction { transaction in
            let identifier = try PersistableIdentifierKeyCodec.tuple(for: id)
            let itemKey = itemsSubspace.pack(identifier)
            let storage = ItemStorage(transaction: transaction, blobsSubspace: blobsSubspace, configuration: .v1)
            if let data = try await storage.read(for: itemKey) {
                return try PersistableStorageCodec.decode(
                    PostFilteredProduct.self,
                    from: data
                )
            }
            return nil
        }
    }

    func searchWithPostFilter(
        query: [Float],
        k: Int,
        predicate: @escaping @Sendable (PostFilteredProduct) async throws -> Bool,
        parameters: HNSWPostFilterParameters = .default
    ) async throws -> [(primaryKey: [any TupleElement], distance: Double)] {
        try await database.withTransaction { transaction in
            // Create fetch function using ItemStorage for proper envelope handling
            let fetchItem: @Sendable (
                Tuple,
                any TransactionAccess
            ) async throws -> PostFilteredProduct? = { primaryKey, tx in
                let itemKey = self.itemsSubspace.pack(primaryKey)
                let storage = ItemStorage(transaction: tx, blobsSubspace: self.blobsSubspace, configuration: .v1)
                if let data = try await storage.read(for: itemKey) {
                    return try PersistableStorageCodec.decode(
                        PostFilteredProduct.self,
                        from: data
                    )
                }
                return nil
            }

            return try await maintainer.searchWithPostFilter(
                queryVector: query,
                k: k,
                predicate: predicate,
                fetchItem: fetchItem,
                postFilterParameters: parameters,
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

// MARK: - Post-Filter Parameters Unit Tests

@Suite("HNSW Post-Filter Parameters Unit Tests", .heartbeat)
struct HNSWPostFilterParametersUnitTests {

    @Test("Default parameters")
    func testDefaultParameters() {
        let params = HNSWPostFilterParameters.default

        #expect(params.expansionFactor == 2, "Default expansion factor should be 2")
        #expect(params.maxPredicateEvaluations == nil, "Default should have no evaluation limit")
    }

    @Test("Custom parameters")
    func testCustomParameters() {
        let params = HNSWPostFilterParameters(
            expansionFactor: 5,
            maxPredicateEvaluations: 100
        )

        #expect(params.expansionFactor == 5)
        #expect(params.maxPredicateEvaluations == 100)
    }

    @Test("non-positive expansion fails explicitly")
    func rejectsNonPositiveExpansion() async throws {
        let context = try await PostFilteredSearchContext()

        await #expect(throws: VectorIndexError.self) {
            try await context.searchWithPostFilter(
                query: [1, 0, 0, 0],
                k: 1,
                predicate: { _ in true },
                parameters: HNSWPostFilterParameters(expansionFactor: 0)
            )
        }
    }

    @Test("candidate count overflow fails explicitly")
    func rejectsCandidateCountOverflow() async throws {
        let context = try await PostFilteredSearchContext()

        await #expect(throws: VectorIndexError.self) {
            try await context.searchWithPostFilter(
                query: [1, 0, 0, 0],
                k: Int.max,
                predicate: { _ in true },
                parameters: HNSWPostFilterParameters(
                    expansionFactor: Int.max
                )
            )
        }
    }

    @Test("negative predicate evaluation limit fails explicitly")
    func rejectsNegativePredicateEvaluationLimit() async throws {
        let context = try await PostFilteredSearchContext()

        await #expect(throws: VectorIndexError.self) {
            try await context.searchWithPostFilter(
                query: [1, 0, 0, 0],
                k: 1,
                predicate: { _ in true },
                parameters: HNSWPostFilterParameters(
                    maxPredicateEvaluations: -1
                )
            )
        }
    }
}

// MARK: - Post-Filtered Search Integration Tests

@Suite("HNSW Post-Filtered Search Tests", .serialized, .heartbeat)
struct HNSWPostFilteredSearchTests {

    // Creates normalized unit vectors.
    private func normalizedVector(_ components: [Float]) -> [Float] {
        let magnitude = sqrt(components.reduce(0) { $0 + $1 * $1 })
        return components.map { $0 / magnitude }
    }

    @Test("Basic filtered search")
    func testBasicFilteredSearch() async throws {
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        // Create products with different categories
        let products = [
            PostFilteredProduct(
                id: "p1", name: "Laptop", category: "electronics", price: 1000,
                embedding: try Vector(float32: normalizedVector([1.0, 0.0, 0.0, 0.0]))
            ),
            PostFilteredProduct(
                id: "p2", name: "Phone", category: "electronics", price: 500,
                embedding: try Vector(float32: normalizedVector([0.9, 0.1, 0.0, 0.0]))
            ),
            PostFilteredProduct(
                id: "p3", name: "Chair", category: "furniture", price: 200,
                embedding: try Vector(float32: normalizedVector([0.8, 0.2, 0.0, 0.0]))
            ),
            PostFilteredProduct(
                id: "p4", name: "Desk", category: "furniture", price: 300,
                embedding: try Vector(float32: normalizedVector([0.7, 0.3, 0.0, 0.0]))
            ),
        ]

        try await ctx.insertProducts(products)

        // Query vector close to p1
        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Search with category filter
        let results = try await ctx.searchWithPostFilter(
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
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        // Create electronics products at varying distances
        let products = [
            PostFilteredProduct(
                id: "close", name: "Close", category: "electronics", price: 100,
                embedding: try Vector(float32: normalizedVector([1.0, 0.0, 0.0, 0.0]))
            ),
            PostFilteredProduct(
                id: "medium", name: "Medium", category: "electronics", price: 200,
                embedding: try Vector(float32: normalizedVector([0.7, 0.7, 0.0, 0.0]))
            ),
            PostFilteredProduct(
                id: "far", name: "Far", category: "electronics", price: 300,
                embedding: try Vector(float32: normalizedVector([0.0, 1.0, 0.0, 0.0]))
            ),
            // Furniture (should be filtered out even though close)
            PostFilteredProduct(
                id: "furniture", name: "Furniture", category: "furniture", price: 50,
                embedding: try Vector(float32: normalizedVector([0.99, 0.01, 0.0, 0.0]))
            ),
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        let results = try await ctx.searchWithPostFilter(
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
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        let products = [
            PostFilteredProduct(id: "p1", name: "Cheap Electronics", category: "electronics", price: 100,
                             embedding: try Vector(float32: normalizedVector([1.0, 0.0, 0.0, 0.0]))),
            PostFilteredProduct(id: "p2", name: "Expensive Electronics", category: "electronics", price: 2000,
                             embedding: try Vector(float32: normalizedVector([0.9, 0.1, 0.0, 0.0]))),
            PostFilteredProduct(id: "p3", name: "Cheap Furniture", category: "furniture", price: 50,
                             embedding: try Vector(float32: normalizedVector([0.8, 0.2, 0.0, 0.0]))),
            PostFilteredProduct(id: "p4", name: "Mid Furniture", category: "furniture", price: 500,
                             embedding: try Vector(float32: normalizedVector([0.7, 0.3, 0.0, 0.0]))),
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Complex filter: electronics under $1000 OR furniture under $100
        let results = try await ctx.searchWithPostFilter(
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
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        // Create 10 electronics products
        var products: [PostFilteredProduct] = []
        for i in 0..<10 {
            let angle = Float(i) * 0.1
            products.append(PostFilteredProduct(
                id: "p\(i)", name: "Product \(i)", category: "electronics", price: Int64(i * 100),
                embedding: try Vector(float32: normalizedVector([cos(angle), sin(angle), 0.0, 0.0]))
            ))
        }

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Request only k=3
        let results = try await ctx.searchWithPostFilter(
            query: queryVector,
            k: 3,
            predicate: { _ in true }  // Accept all
        )

        #expect(results.count == 3, "Should return exactly k=3 results")

        try await ctx.cleanup()
    }

    @Test("Filter that excludes all")
    func testFilterExcludesAll() async throws {
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        let products = [
            PostFilteredProduct(id: "p1", name: "Product 1", category: "electronics", price: 100,
                             embedding: try Vector(float32: normalizedVector([1.0, 0.0, 0.0, 0.0]))),
            PostFilteredProduct(id: "p2", name: "Product 2", category: "electronics", price: 200,
                             embedding: try Vector(float32: normalizedVector([0.9, 0.1, 0.0, 0.0]))),
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Filter that matches nothing
        let results = try await ctx.searchWithPostFilter(
            query: queryVector,
            k: 10,
            predicate: { product in product.category == "nonexistent" }
        )

        #expect(results.isEmpty, "Should return empty results when filter excludes all")

        try await ctx.cleanup()
    }

    @Test("candidate expansion factor affects recall")
    func testCandidateExpansionFactorAffectsRecall() async throws {
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        // Create a mix of products
        var products: [PostFilteredProduct] = []
        for i in 0..<20 {
            let category = i % 3 == 0 ? "target" : "other"
            let angle = Float(i) * 0.15
            products.append(PostFilteredProduct(
                id: "p\(i)", name: "Product \(i)", category: category, price: Int64(i * 50),
                embedding: try Vector(float32: normalizedVector([cos(angle), sin(angle), 0.0, 0.0]))
            ))
        }

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Low expansion factor
        let resultsLow = try await ctx.searchWithPostFilter(
            query: queryVector,
            k: 5,
            predicate: { product in product.category == "target" },
            parameters: HNSWPostFilterParameters(expansionFactor: 1)
        )

        // High expansion factor
        let resultsHigh = try await ctx.searchWithPostFilter(
            query: queryVector,
            k: 5,
            predicate: { product in product.category == "target" },
            parameters: HNSWPostFilterParameters(expansionFactor: 5)
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
        let ctx = try await PostFilteredSearchContext(dimensions: 4)

        let products = [
            PostFilteredProduct(id: "e1", name: "Electronics 1", category: "electronics", price: 100,
                             embedding: try Vector(float32: normalizedVector([1.0, 0.0, 0.0, 0.0]))),
            PostFilteredProduct(id: "f1", name: "Furniture 1", category: "furniture", price: 200,
                             embedding: try Vector(float32: normalizedVector([0.95, 0.05, 0.0, 0.0]))),
            PostFilteredProduct(id: "e2", name: "Electronics 2", category: "electronics", price: 300,
                             embedding: try Vector(float32: normalizedVector([0.9, 0.1, 0.0, 0.0]))),
            PostFilteredProduct(id: "f2", name: "Furniture 2", category: "furniture", price: 400,
                             embedding: try Vector(float32: normalizedVector([0.85, 0.15, 0.0, 0.0]))),
        ]

        try await ctx.insertProducts(products)

        let queryVector = normalizedVector([1.0, 0.0, 0.0, 0.0])

        // Unfiltered search
        let unfilteredResults = try await ctx.searchUnfiltered(query: queryVector, k: 10)

        // Filtered search (only electronics)
        let filteredResults = try await ctx.searchWithPostFilter(
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
