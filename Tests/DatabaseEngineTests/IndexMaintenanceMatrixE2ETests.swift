#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import Core
import DatabaseValue
import Vector
import FullText
import Geospatial
import Rank
import Permuted
import Graph
import Relationship
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import ScalarIndex
@testable import VectorIndex
@testable import FullTextIndex
@testable import SpatialIndex
@testable import RankIndex
@testable import PermutedIndex
@testable import GraphIndex
@testable import AggregationIndex
@testable import VersionIndex
@testable import BitmapIndex
@testable import LeaderboardIndex
@testable import RelationshipIndex

private enum IndexMaintenanceMatrixError: Error, CustomStringConvertible {
    case descriptorNotFound(type: String, name: String)

    var description: String {
        switch self {
        case .descriptorNotFound(let type, let name):
            return "Missing index descriptor '\(name)' on \(type)"
        }
    }
}

// MARK: - Matrix Models

@Persistable
private struct MatrixScalarUser {
    #Directory<MatrixScalarUser>("test", "index_matrix", "scalar_users")

    var id: String = ULID().ulidString
    var email: String = ""
    var city: String = ""

    #Index(ScalarIndexKind<MatrixScalarUser>(fields: [\.email]), name: "matrix_scalar_email")
}

@Persistable
private struct MatrixVectorDocument {
    #Directory<MatrixVectorDocument>("test", "index_matrix", "vector_documents")

    var id: String = ULID().ulidString
    var title: String = ""
    var embedding: [Float] = []

    #Index(VectorIndexKind<MatrixVectorDocument>(embedding: \.embedding, dimensions: 3), name: "matrix_vector_embedding")
}

@Persistable
private struct MatrixFullTextArticle {
    #Directory<MatrixFullTextArticle>("test", "index_matrix", "fulltext_articles")

    var id: String = ULID().ulidString
    var title: String = ""
    var body: String = ""

    #Index(FullTextIndexKind<MatrixFullTextArticle>(fields: [\.body], tokenizer: .simple), name: "matrix_fulltext_body")
}

@Persistable
private struct MatrixGraphEdge {
    #Directory<MatrixGraphEdge>("test", "index_matrix", "graph_edges")

    var id: String = ULID().ulidString
    var source: String = ""
    var relation: String = ""
    var target: String = ""

    #Index(GraphIndexKind<MatrixGraphEdge>(
        from: \.source,
        edge: \.relation,
        to: \.target,
        strategy: .adjacency
    ), name: "matrix_graph_adjacency")
}

@Persistable
private struct MatrixSpatialPlace {
    #Directory<MatrixSpatialPlace>("test", "index_matrix", "spatial_places")

    var id: String = ULID().ulidString
    var name: String = ""
    var latitude: Double = 0
    var longitude: Double = 0
    var location: GeoPoint = GeoPoint(0, 0)

    #Index(SpatialIndexKind<MatrixSpatialPlace>(latitude: \.latitude, longitude: \.longitude), name: "matrix_spatial_lat_lon")
}

@Persistable
private struct MatrixRankPlayer {
    #Directory<MatrixRankPlayer>("test", "index_matrix", "rank_players")

    var id: String = ULID().ulidString
    var name: String = ""
    var score: Int64 = 0

    #Index(RankIndexKind<MatrixRankPlayer, Int64>(field: \.score), name: "matrix_rank_score")
}

@Persistable
private struct MatrixAggregationOrder {
    #Directory<MatrixAggregationOrder>("test", "index_matrix", "aggregation_orders")

    var id: String = ULID().ulidString
    var region: String = ""
    var category: String = ""
    var amount: Int64 = 0
    var latencyMs: Double = 0
    var customerID: String = ""
    var note: String? = nil

    #Index(CountIndexKind<MatrixAggregationOrder>(groupBy: [\.region]), name: "matrix_count_region")
    #Index(SumIndexKind<MatrixAggregationOrder, Int64>(groupBy: [\.region], value: \.amount), name: "matrix_sum_region_amount")
    #Index(MinIndexKind<MatrixAggregationOrder, Int64>(groupBy: [\.region], value: \.amount), name: "matrix_min_region_amount")
    #Index(MaxIndexKind<MatrixAggregationOrder, Int64>(groupBy: [\.region], value: \.amount), name: "matrix_max_region_amount")
    #Index(AverageIndexKind<MatrixAggregationOrder, Int64>(groupBy: [\.region], value: \.amount), name: "matrix_average_region_amount")
    #Index(CountUpdatesIndexKind<MatrixAggregationOrder>(field: \.id), name: "matrix_count_updates_id")
    #Index(CountNotNullIndexKind<MatrixAggregationOrder>(groupBy: [\.region], value: \.note), name: "matrix_count_not_null_region_note")
    #Index(DistinctIndexKind<MatrixAggregationOrder>(groupBy: [\.region], value: \.customerID), name: "matrix_distinct_region_customer")
    #Index(PercentileIndexKind<MatrixAggregationOrder, Double>(groupBy: [\.region], value: \.latencyMs), name: "matrix_percentile_region_latency")
}

@Persistable
private struct MatrixVersionDocument {
    #Directory<MatrixVersionDocument>("test", "index_matrix", "version_documents")

    var id: String = ULID().ulidString
    var title: String = ""
    var revision: Int = 0

    #Index(VersionIndexKind<MatrixVersionDocument>(field: \.id, strategy: .keepAll), name: "matrix_version_id")
}

@Persistable
private struct MatrixBitmapItem {
    #Directory<MatrixBitmapItem>("test", "index_matrix", "bitmap_items")

    var id: String = ULID().ulidString
    var status: String = ""
    var category: String = ""

    #Index(BitmapIndexKind<MatrixBitmapItem>(field: \.status), name: "matrix_bitmap_status")
}

@Persistable
private struct MatrixLeaderboardScore {
    #Directory<MatrixLeaderboardScore>("test", "index_matrix", "leaderboard_scores")

    var id: String = ULID().ulidString
    var player: String = ""
    var region: String = "global"
    var score: Int64 = 0

    #Index(TimeWindowLeaderboardIndexKind<MatrixLeaderboardScore>(
        scoreField: \.score,
        groupBy: [\.region],
        window: .daily,
        windowCount: 2
    ), name: "matrix_leaderboard_region_score")
}

@Persistable
private struct MatrixPermutedLocation {
    #Directory<MatrixPermutedLocation>("test", "index_matrix", "permuted_locations")

    var id: String = ULID().ulidString
    var country: String = ""
    var city: String = ""
    var name: String = ""

    #Index(PermutedIndexKind<MatrixPermutedLocation>(
        fields: [\.country, \.city, \.name],
        permutation: .swapping(0, 1, size: 3)
    ), name: "matrix_permuted_city_country_name")
}

@Persistable
private struct MatrixRelationshipCustomer {
    #Directory<MatrixRelationshipCustomer>("test", "index_matrix", "relationship_customers")

    var id: String = ULID().ulidString
    var name: String = ""
    var tier: String = ""
}

@Persistable
private struct MatrixRelationshipOrder {
    #Directory<MatrixRelationshipOrder>("test", "index_matrix", "relationship_orders")

    var id: String = ULID().ulidString
    @Relationship(deleteRule: .nullify)
    var customer: DatabaseReference<MatrixRelationshipCustomer>? = nil
    var total: Double = 0
}

// MARK: - Matrix Suite

@Suite("Index Maintenance Matrix E2E Tests", .serialized, .heartbeat)
struct IndexMaintenanceMatrixE2ETests {
    private let paths: [[String]] = [
        ["test", "index_matrix", "scalar_users"],
        ["test", "index_matrix", "vector_documents"],
        ["test", "index_matrix", "fulltext_articles"],
        ["test", "index_matrix", "graph_edges"],
        ["test", "index_matrix", "spatial_places"],
        ["test", "index_matrix", "rank_players"],
        ["test", "index_matrix", "aggregation_orders"],
        ["test", "index_matrix", "version_documents"],
        ["test", "index_matrix", "bitmap_items"],
        ["test", "index_matrix", "leaderboard_scores"],
        ["test", "index_matrix", "permuted_locations"],
        ["test", "index_matrix", "relationship_customers"],
        ["test", "index_matrix", "relationship_orders"],
    ]

    private func setupContainer(
        _ entities: [Schema.Entity]
    ) async throws -> DBContainer {
        try await FoundationDBScenarioCoordinator.shared.initialize()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: entities,
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [MatrixScalarUser.self, MatrixVectorDocument.self, MatrixFullTextArticle.self, MatrixGraphEdge.self, MatrixSpatialPlace.self, MatrixRankPlayer.self, MatrixAggregationOrder.self, MatrixVersionDocument.self, MatrixBitmapItem.self, MatrixLeaderboardScore.self, MatrixPermutedLocation.self, MatrixRelationshipCustomer.self, MatrixRelationshipOrder.self]),
            security: .disabled
        )
        try await cleanup(container: container)
        return container
    }

    private func cleanup(container: DBContainer) async throws {
        for path in paths {
            if try await container.engine.directoryExists(path: path) {
                try await container.engine.removeDirectory(path: path)
            }
        }
        try await container.ensureIndexesReady()
    }

    private func descriptor<T: Persistable>(
        for type: T.Type,
        named name: String
    ) throws -> IndexDescriptor {
        guard let descriptor = T.indexDescriptors.first(where: { $0.name == name }) else {
            throw IndexMaintenanceMatrixError.descriptorNotFound(type: T.persistableType, name: name)
        }
        return descriptor
    }

    private func entryCount<T: Persistable>(
        container: DBContainer,
        type: T.Type,
        indexName: String
    ) async throws -> Int {
        let typeSubspace = try await container.resolveDirectory(for: type)
        let subspace = typeSubspace
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
        return try await countEntries(container: container, subspace: subspace)
    }

    private func countEntries(container: DBContainer, subspace: Subspace) async throws -> Int {
        try await container.engine.withTransaction { transaction -> Int in
            let (begin, end) = subspace.range()
            var count = 0
            for try await _ in transaction.getRange(begin: begin, end: end, snapshot: true) {
                count += 1
            }
            return count
        }
    }

    private func assertMaintained<T: Persistable>(
        container: DBContainer,
        type: T.Type,
        indexName: String,
        minimumCount: Int = 1
    ) async throws {
        let descriptor = try descriptor(for: type, named: indexName)
        let count = try await entryCount(container: container, type: type, indexName: descriptor.name)
        #expect(
            count >= minimumCount,
            "\(T.persistableType).\(indexName) should have at least \(minimumCount) index entries, got \(count)"
        )
    }

    private func assertDescriptorShape<T: Persistable>(
        for type: T.Type,
        named name: String,
        kindIdentifier: String,
        descriptorFields: [String],
        kindFields: [String]? = nil
    ) throws {
        let descriptor = try descriptor(for: type, named: name)
        #expect(descriptor.kindIdentifier == kindIdentifier)
        #expect(descriptor.fieldNames == descriptorFields)
        #expect(descriptor.kind.fieldNames == (kindFields ?? descriptorFields))
    }

    @Test("Matrix descriptors use expected kind identifiers and KeyPath-derived fields")
    func matrixDescriptorShapes() throws {
        try assertDescriptorShape(
            for: MatrixScalarUser.self,
            named: "matrix_scalar_email",
            kindIdentifier: ScalarIndexKind<MatrixScalarUser>.identifier,
            descriptorFields: ["email"]
        )
        try assertDescriptorShape(
            for: MatrixVectorDocument.self,
            named: "matrix_vector_embedding",
            kindIdentifier: VectorIndexKind<MatrixVectorDocument>.identifier,
            descriptorFields: ["embedding"]
        )
        try assertDescriptorShape(
            for: MatrixFullTextArticle.self,
            named: "matrix_fulltext_body",
            kindIdentifier: FullTextIndexKind<MatrixFullTextArticle>.identifier,
            descriptorFields: ["body"]
        )
        try assertDescriptorShape(
            for: MatrixGraphEdge.self,
            named: "matrix_graph_adjacency",
            kindIdentifier: GraphIndexKind<MatrixGraphEdge>.identifier,
            descriptorFields: ["source", "relation", "target"]
        )
        try assertDescriptorShape(
            for: MatrixSpatialPlace.self,
            named: "matrix_spatial_lat_lon",
            kindIdentifier: SpatialIndexKind<MatrixSpatialPlace>.identifier,
            descriptorFields: ["latitude", "longitude"]
        )
        try assertDescriptorShape(
            for: MatrixRankPlayer.self,
            named: "matrix_rank_score",
            kindIdentifier: RankIndexKind<MatrixRankPlayer, Int64>.identifier,
            descriptorFields: ["score"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_count_region",
            kindIdentifier: CountIndexKind<MatrixAggregationOrder>.identifier,
            descriptorFields: ["region"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_sum_region_amount",
            kindIdentifier: SumIndexKind<MatrixAggregationOrder, Int64>.identifier,
            descriptorFields: ["region", "amount"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_min_region_amount",
            kindIdentifier: MinIndexKind<MatrixAggregationOrder, Int64>.identifier,
            descriptorFields: ["region", "amount"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_max_region_amount",
            kindIdentifier: MaxIndexKind<MatrixAggregationOrder, Int64>.identifier,
            descriptorFields: ["region", "amount"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_average_region_amount",
            kindIdentifier: AverageIndexKind<MatrixAggregationOrder, Int64>.identifier,
            descriptorFields: ["region", "amount"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_count_updates_id",
            kindIdentifier: CountUpdatesIndexKind<MatrixAggregationOrder>.identifier,
            descriptorFields: ["id"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_count_not_null_region_note",
            kindIdentifier: CountNotNullIndexKind<MatrixAggregationOrder>.identifier,
            descriptorFields: ["region", "note"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_distinct_region_customer",
            kindIdentifier: DistinctIndexKind<MatrixAggregationOrder>.identifier,
            descriptorFields: ["region", "customerID"]
        )
        try assertDescriptorShape(
            for: MatrixAggregationOrder.self,
            named: "matrix_percentile_region_latency",
            kindIdentifier: PercentileIndexKind<MatrixAggregationOrder, Double>.identifier,
            descriptorFields: ["region", "latencyMs"]
        )
        try assertDescriptorShape(
            for: MatrixVersionDocument.self,
            named: "matrix_version_id",
            kindIdentifier: VersionIndexKind<MatrixVersionDocument>.identifier,
            descriptorFields: ["id"]
        )
        try assertDescriptorShape(
            for: MatrixBitmapItem.self,
            named: "matrix_bitmap_status",
            kindIdentifier: BitmapIndexKind<MatrixBitmapItem>.identifier,
            descriptorFields: ["status"]
        )
        try assertDescriptorShape(
            for: MatrixLeaderboardScore.self,
            named: "matrix_leaderboard_region_score",
            kindIdentifier: TimeWindowLeaderboardIndexKind<MatrixLeaderboardScore>.identifier,
            descriptorFields: ["region", "score"]
        )
        try assertDescriptorShape(
            for: MatrixPermutedLocation.self,
            named: "matrix_permuted_city_country_name",
            kindIdentifier: PermutedIndexKind<MatrixPermutedLocation>.identifier,
            descriptorFields: ["country", "city", "name"]
        )
        try assertDescriptorShape(
            for: MatrixRelationshipOrder.self,
            named: "MatrixRelationshipOrder_customer",
            kindIdentifier: ScalarIndexKind<MatrixRelationshipOrder>.identifier,
            descriptorFields: ["customerID"]
        )
        let relationship = try #require(MatrixRelationshipOrder.relationshipDescriptors.first)
        #expect(relationship.name == "MatrixRelationshipOrder_customer")
        #expect(relationship.propertyName == "customerID")
        #expect(relationship.relatedTypeName == MatrixRelationshipCustomer.persistableType)
        #expect(relationship.deleteRule == .nullify)
    }

    @Test("Scalar index matrix path stores and queries by indexed field")
    func scalarIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixScalarUser.self])
            let context = container.newContext()

            let user = MatrixScalarUser(email: "matrix@example.com", city: "Tokyo")
            try context.insert(user)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixScalarUser.self, indexName: "matrix_scalar_email")

            let fetched = try await context.fetch(MatrixScalarUser.self)
                .where(\.email == "matrix@example.com")
                .first()
            #expect(fetched?.id == user.id)

            try await cleanup(container: container)
        }
    }

    @Test("Vector index matrix path stores and queries nearest vectors")
    func vectorIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixVectorDocument.self])
            let context = container.newContext()

            let close = MatrixVectorDocument(title: "close", embedding: [1, 0, 0])
            let far = MatrixVectorDocument(title: "far", embedding: [0, 1, 0])
            try context.insert(close)
            try context.insert(far)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixVectorDocument.self, indexName: "matrix_vector_embedding", minimumCount: 2)

            let results = try await context.findSimilar(MatrixVectorDocument.self)
                .vector(\.embedding, dimensions: 3)
                .query([1, 0, 0], k: 1)
                .execute()
            #expect(results.first?.item.id == close.id)

            try await cleanup(container: container)
        }
    }

    @Test("FullText index matrix path tokenizes and searches saved documents")
    func fullTextIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixFullTextArticle.self])
            let context = container.newContext()

            let article = MatrixFullTextArticle(title: "Matrix", body: "swift database indexing matrix")
            try context.insert(article)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixFullTextArticle.self, indexName: "matrix_fulltext_body", minimumCount: 3)

            let results = try await context.search(MatrixFullTextArticle.self)
                .fullText(\.body)
                .terms(["database"])
                .execute()
            #expect(results.map(\.id).contains(article.id))

            try await cleanup(container: container)
        }
    }

    @Test("Graph index matrix path maintains adjacency entries")
    func graphIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixGraphEdge.self])
            let context = container.newContext()

            let edge = MatrixGraphEdge(source: "alice", relation: "knows", target: "bob")
            try context.insert(edge)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixGraphEdge.self, indexName: "matrix_graph_adjacency", minimumCount: 2)

            try await cleanup(container: container)
        }
    }

    @Test("Spatial index matrix path stores coordinate entries")
    func spatialIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixSpatialPlace.self])
            let context = container.newContext()

            let station = MatrixSpatialPlace(
                name: "Tokyo Station",
                latitude: 35.6812,
                longitude: 139.7671,
                location: GeoPoint(35.6812, 139.7671)
            )
            let far = MatrixSpatialPlace(
                name: "Osaka",
                latitude: 34.6937,
                longitude: 135.5023,
                location: GeoPoint(34.6937, 135.5023)
            )
            try context.insert(station)
            try context.insert(far)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixSpatialPlace.self, indexName: "matrix_spatial_lat_lon", minimumCount: 2)

            try await cleanup(container: container)
        }
    }

    @Test("Rank index matrix path stores ranked scores")
    func rankIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixRankPlayer.self])
            let context = container.newContext()

            try context.insert(MatrixRankPlayer(name: "Alice", score: 100))
            try context.insert(MatrixRankPlayer(name: "Bob", score: 50))
            try await context.save()

            try await assertMaintained(container: container, type: MatrixRankPlayer.self, indexName: "matrix_rank_score", minimumCount: 2)

            try await cleanup(container: container)
        }
    }

    @Test("Aggregation index matrix path maintains every aggregation descriptor")
    func aggregationIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixAggregationOrder.self])
            let context = container.newContext()

            try context.insert(MatrixAggregationOrder(region: "apac", category: "software", amount: 100, latencyMs: 12.5, customerID: "c1", note: "paid"))
            try context.insert(MatrixAggregationOrder(region: "apac", category: "hardware", amount: 250, latencyMs: 40.0, customerID: "c2", note: nil))
            try context.insert(MatrixAggregationOrder(region: "emea", category: "software", amount: 75, latencyMs: 8.0, customerID: "c1", note: "paid"))
            try await context.save()

            for indexName in [
                "matrix_count_region",
                "matrix_sum_region_amount",
                "matrix_min_region_amount",
                "matrix_max_region_amount",
                "matrix_average_region_amount",
                "matrix_count_updates_id",
                "matrix_count_not_null_region_note",
                "matrix_distinct_region_customer",
                "matrix_percentile_region_latency",
            ] {
                try await assertMaintained(container: container, type: MatrixAggregationOrder.self, indexName: indexName)
            }

            try await cleanup(container: container)
        }
    }

    @Test("Version index matrix path stores version history")
    func versionIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixVersionDocument.self])
            let context = container.newContext()

            var document = MatrixVersionDocument(title: "draft", revision: 1)
            try context.insert(document)
            try await context.save()

            document.title = "published"
            document.revision = 2
            try context.insert(document)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixVersionDocument.self, indexName: "matrix_version_id", minimumCount: 2)

            let latest = try await context.versions(MatrixVersionDocument.self)
                .forItem(document.id)
                .latest()
            #expect(latest?.revision == 2)

            try await cleanup(container: container)
        }
    }

    @Test("Bitmap index matrix path stores low-cardinality membership")
    func bitmapIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixBitmapItem.self])
            let context = container.newContext()

            try context.insert(MatrixBitmapItem(status: "active", category: "a"))
            try context.insert(MatrixBitmapItem(status: "inactive", category: "b"))
            try await context.save()

            try await assertMaintained(container: container, type: MatrixBitmapItem.self, indexName: "matrix_bitmap_status", minimumCount: 2)

            try await cleanup(container: container)
        }
    }

    @Test("Leaderboard index matrix path stores and queries top scores")
    func leaderboardIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixLeaderboardScore.self])
            let context = container.newContext()

            let high = MatrixLeaderboardScore(player: "Alice", region: "apac", score: 900)
            let low = MatrixLeaderboardScore(player: "Bob", region: "apac", score: 100)
            try context.insert(high)
            try context.insert(low)
            try await context.save()

            try await assertMaintained(container: container, type: MatrixLeaderboardScore.self, indexName: "matrix_leaderboard_region_score")

            let top = try await context.leaderboard(MatrixLeaderboardScore.self)
                .index(\.score)
                .group(by: ["apac"])
                .top(1)
                .execute()
            #expect(top.first?.item.id == high.id)

            try await cleanup(container: container)
        }
    }

    @Test("Permuted index matrix path stores reordered compound keys")
    func permutedIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([MatrixPermutedLocation.self])
            let context = container.newContext()

            try context.insert(MatrixPermutedLocation(country: "JP", city: "Tokyo", name: "Station"))
            try context.insert(MatrixPermutedLocation(country: "US", city: "New York", name: "Terminal"))
            try await context.save()

            try await assertMaintained(container: container, type: MatrixPermutedLocation.self, indexName: "matrix_permuted_city_country_name", minimumCount: 2)

            try await cleanup(container: container)
        }
    }

    @Test("Relationship matrix path maintains the canonical catalog and delete rules")
    func relationshipIndexMatrixPath() async throws {
        try await FoundationDBScenarioCoordinator.shared.withSerializedAccess {
            let container = try await setupContainer([
                MatrixRelationshipCustomer.self,
                MatrixRelationshipOrder.self,
            ])
            let context = container.newContext()

            let customer = MatrixRelationshipCustomer(name: "Alice", tier: "gold")
            try context.insert(customer)
            try await context.save()

            let order = MatrixRelationshipOrder(
                customer: try context.reference(to: customer),
                total: 42
            )
            try context.insert(order)
            try await context.save()

            let related = try await context.related(order, \.customer)
            #expect(related?.id == customer.id)

            let inverse = try await context.inverseRelationshipResolver().referencedBy(
                try context.reference(to: customer),
                from: MatrixRelationshipOrder.self,
                via: \.customer,
                limit: 1
            )
            #expect(inverse.entities.map(\.id) == [order.id])

            try context.delete(customer)
            try await context.save()
            let reloadedOrder = try await context.fetch(MatrixRelationshipOrder.self)
                .where(\.id == order.id)
                .first()
            #expect(reloadedOrder?.customer == nil)

            try await cleanup(container: container)
        }
    }
}
#endif

#endif
