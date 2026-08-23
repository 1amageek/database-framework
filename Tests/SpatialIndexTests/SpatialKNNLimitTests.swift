import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import SpatialIndex
import StorageKit
import TestSupport
import Testing

@Persistable
private struct SpatialKNNLimitItem {
    var id: String
    var name: String
    var location: GeographicPoint

    #Index(
        .spatial(
            name: "SpatialKNNLimitItem_location",
            location: \SpatialKNNLimitItem.location,
            encoding: .s2,
            level: 10
        )
    )
}

@Persistable
private struct SpatialKNNMortonItem {
    var id: String
    var name: String
    var location: GeographicPoint

    #Index(
        .spatial(
            name: "SpatialKNNMortonItem_location",
            location: \SpatialKNNMortonItem.location,
            encoding: .morton,
            level: 10
        )
    )
}

@Suite("Spatial KNN limit semantics")
struct SpatialKNNLimitTests {
    @Test("Adaptive KNN processes the batch that exactly exhausts its key cap")
    func adaptiveKNNProcessesExactCapBatch() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "exact-adaptive",
                    name: "Exact adaptive",
                    location: center
                )
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(
                k: 1,
                from: center,
                initialRadiusKm: 1,
                maxRadiusKm: 1
            )
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 1,
                    maximumKeysPerIteration: 1,
                    maximumTotalKeys: 1
                )
            )
            .executeKNN()

        #expect(result.items.map(\.item.id) == ["exact-adaptive"])
    }

    @Test("Adaptive KNN reconsiders cell false positives after radius expansion")
    func adaptiveKNNReconsidersExpandedRadiusCandidate() async throws {
        let center = try GeographicPoint(latitude: 0.05, longitude: 0.05)
        let expandsIntoRadius = try GeographicPoint(
            latitude: 0.0635,
            longitude: 0.05
        )
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "expanded",
                    name: "Expanded",
                    location: expandsIntoRadius
                )
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(
                k: 1,
                from: center,
                initialRadiusKm: 1,
                maxRadiusKm: 2,
                expansionFactor: 2
            )
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 2,
                    maximumKeysPerIteration: 100,
                    maximumTotalKeys: 200
                )
            )
            .executeKNN()

        #expect(result.items.map(\.item.id) == ["expanded"])
    }

    @Test("Adaptive KNN reports its total candidate cap")
    func adaptiveKNNReportsTotalCandidateCap() async throws {
        let center = try GeographicPoint(latitude: 0.05, longitude: 0.05)
        let outsideInitialRadius = try GeographicPoint(
            latitude: 0.0635,
            longitude: 0.05
        )
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "outside-initial-radius",
                    name: "Outside initial radius",
                    location: outsideInitialRadius
                )
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(
                k: 1,
                from: center,
                initialRadiusKm: 1,
                maxRadiusKm: 2,
                expansionFactor: 2
            )
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 2,
                    maximumKeysPerIteration: 100,
                    maximumTotalKeys: 1
                )
            )
            .executeKNN()

        #expect(result.items.isEmpty)
        #expect(
            result.limitReason
                == .maxCandidatesReached(scanned: 1, limit: 1)
        )
    }

    @Test("Adaptive KNN reports its iteration cap")
    func adaptiveKNNReportsIterationCap() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(items: [])
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(k: 1, from: center)
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 1,
                    maximumKeysPerIteration: 100,
                    maximumTotalKeys: 100
                )
            )
            .executeKNN()

        #expect(result.items.isEmpty)
        #expect(
            result.limitReason
                == .maxIterationsReached(iterations: 1, limit: 1)
        )
    }

    @Test("Adaptive KNN reports its maximum radius")
    func adaptiveKNNReportsMaximumRadius() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(items: [])
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(
                k: 1,
                from: center,
                initialRadiusKm: 1,
                maxRadiusKm: 1
            )
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 10,
                    maximumKeysPerIteration: 100,
                    maximumTotalKeys: 100
                )
            )
            .executeKNN()

        #expect(result.items.isEmpty)
        #expect(
            result.limitReason
                == .maxRadiusReached(
                    radiusMeters: 1_000,
                    limitMeters: 1_000
                )
        )
    }

    @Test("True KNN processes the cell that exactly exhausts its point cap")
    func trueKNNProcessesExactCapCell() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "exact-true",
                    name: "Exact true",
                    location: center
                )
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(k: 1, from: center)
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 1,
                    maximumKeysPerIteration: 1,
                    maximumTotalKeys: 1
                )
            )
            .executeTrueKNN()

        #expect(result.items.map(\.item.id) == ["exact-true"])
        #expect(result.isComplete)
        #expect(result.limitReason == nil)
    }

    @Test("True KNN expands across the antimeridian")
    func trueKNNWrapsAntimeridianNeighbors() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 179.98)
        let acrossDateline = try GeographicPoint(
            latitude: 0,
            longitude: -179.99
        )
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "across-dateline",
                    name: "Across dateline",
                    location: acrossDateline
                )
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(k: 1, from: center)
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 1,
                    maximumKeysPerIteration: 1_000,
                    maximumTotalKeys: 10
                )
            )
            .executeTrueKNN()

        #expect(result.items.map(\.item.id) == ["across-dateline"])
        #expect(result.isComplete)
    }

    @Test("True KNN reports a candidate cap even when it returns k items")
    func trueKNNDoesNotClaimCompletenessAtCandidateCap() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(
            items: [
                SpatialKNNLimitItem(
                    id: "first",
                    name: "First",
                    location: try GeographicPoint(latitude: 20, longitude: 20)
                ),
                SpatialKNNLimitItem(
                    id: "closer",
                    name: "Closer",
                    location: try GeographicPoint(latitude: 0.001, longitude: 0.001)
                ),
            ]
        )
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNLimitItem.self)
            .location(SpatialKNNLimitItem.fields.location)
            .nearest(k: 1, from: center)
            .resourceLimits(
                SpatialKNNResourceLimits(
                    maximumIterations: 1,
                    maximumKeysPerIteration: 1,
                    maximumTotalKeys: 1
                )
            )
            .executeTrueKNN()

        #expect(result.count == 1)
        #expect(!result.isComplete)
        #expect(result.limitReason == .maxCandidatesReached(scanned: 1, limit: 1))
    }

    @Test("True KNN scans a Morton index using its physical encoding")
    func trueKNNReadsMortonIndex() async throws {
        let center = try GeographicPoint(latitude: 89.8, longitude: 179.8)
        let items = [
            SpatialKNNMortonItem(
                id: "nearest",
                name: "Nearest",
                location: try GeographicPoint(latitude: 89.81, longitude: -179.9)
            ),
            SpatialKNNMortonItem(
                id: "far",
                name: "Far",
                location: try GeographicPoint(latitude: -45, longitude: 20)
            ),
        ]
        let container = try await makeMortonContainer(items: items)
        defer { await container.shutdown() }

        let result = try await container.testBaseContext()
            .findNearby(SpatialKNNMortonItem.self)
            .location(SpatialKNNMortonItem.fields.location)
            .nearest(k: 1, from: center)
            .executeTrueKNN()

        #expect(result.items.map(\.item.id) == ["nearest"])
        #expect(result.isComplete)
    }

    @Test("KNN accepts unbounded key limits without sentinel overflow")
    func knnAcceptsUnboundedKeyLimits() async throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let container = try await makeContainer(items: [])
        defer { await container.shutdown() }

        let adaptive = try await container.testBaseContext()
                .findNearby(SpatialKNNLimitItem.self)
                .location(SpatialKNNLimitItem.fields.location)
                .nearest(k: 1, from: center)
                .resourceLimits(
                    SpatialKNNResourceLimits(
                        maximumIterations: 1,
                        maximumKeysPerIteration: Int.max,
                        maximumTotalKeys: 1
                    )
                )
                .executeKNN()
        #expect(adaptive.count == 0)

        let exact = try await container.testBaseContext()
                .findNearby(SpatialKNNLimitItem.self)
                .location(SpatialKNNLimitItem.fields.location)
                .nearest(k: 1, from: center)
                .resourceLimits(
                    SpatialKNNResourceLimits(
                        maximumIterations: 1,
                        maximumKeysPerIteration: 1,
                        maximumTotalKeys: Int.max
                    )
                )
                .executeTrueKNN()
        #expect(exact.isComplete)
    }

    private func makeContainer(
        items: [SpatialKNNLimitItem]
    ) async throws -> DBContainer {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try SpatialKNNLimitItem.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "spatial-knn-limit-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SpatialKNNLimitItem.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        for item in items {
            try context.insert(item)
        }
        try await context.save()
        return container
    }

    private func makeMortonContainer(
        items: [SpatialKNNMortonItem]
    ) async throws -> DBContainer {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try SpatialKNNMortonItem.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "spatial-knn-morton-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SpatialKNNMortonItem.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let context = container.testBaseContext()
        for item in items {
            try context.insert(item)
        }
        try await context.save()
        return container
    }
}
