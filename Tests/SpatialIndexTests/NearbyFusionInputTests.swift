import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import SpatialIndex

@Persistable
private struct NearbyFusionInputItem {
    var id: String
    var location: GeographicPoint
}

@Persistable
private struct NearbyFusionExecutionItem {
    #Index(
        .spatial(
            name: "nearby_fusion_s2",
            location: \NearbyFusionExecutionItem.location,
            encoding: .s2,
            level: 8
        )
    )
    #Index(
        .spatial(
            name: "nearby_fusion_morton",
            location: \NearbyFusionExecutionItem.location,
            encoding: .morton,
            level: 8
        )
    )

    var id: String
    var name: String
    var location: GeographicPoint
    var eligible: Bool
}

@Persistable
private struct SpatialPositionValueItem {
    #Index(
        .spatial(
            name: "spatial_position_value",
            location: \SpatialPositionValueItem.location,
            encoding: .morton,
            level: 8
        )
    )

    var id: String
    var location: GeographicPosition
}

@Suite("Spatial Fusion input")
struct NearbyFusionInputTests {
    @Test("Nearby lowers radius in meters and distance scoring")
    func lowersToCanonicalInput() throws {
        let center = try GeographicPoint(latitude: 35, longitude: 139)
        let input = try Nearby(NearbyFusionInputItem.fields.location)
            .within(radiusKm: 2.5, of: center)
            .limit(4)
            .fusionInput

        #expect(input.scoring == .annotation(
            name: "distance",
            order: .lowerIsBetter
        ))
        #expect(input.limit == 4)
        guard case .index(let source) = input.operation else {
            Issue.record("Nearby must lower to an index operation")
            return
        }
        #expect(source.selection == .matching(
            type: .spatial,
            fields: [NearbyFusionInputItem.fields.location.identity],
            fieldMatch: .exact
        ))
        #expect(source.referencedFields == [
            NearbyFusionInputItem.fields.location.identity,
        ])
        #expect(source.parameters[SpatialFusionReadParameter.radiusMeters]
            == .float64(2_500))
        #expect(source.parameters[SpatialFusionReadParameter.referencePoint]
            == .geographicPoint(center))
    }

    @Test("Nearby rejects non-finite radius")
    func rejectsNonFiniteRadius() throws {
        let center = try GeographicPoint(latitude: 35, longitude: 139)
        #expect {
            _ = try Nearby(NearbyFusionInputItem.fields.location)
                .within(radiusKm: .infinity, of: center)
        } throws: { error in
            error as? SpatialFusionInputError == .nonFiniteRadius
        }
    }

    @Test("Spatial values preserve exact points and antipodal distance")
    func spatialValueAndDistanceAreCanonical() throws {
        let point = try GeographicPoint(
            latitude: -33.8688,
            longitude: 151.2093
        )
        let storedPoint = SpatialIndexStoredCoordinate(
            point: point,
            height: nil
        )
        let encodedPoint = SpatialIndexValueCodec.encode(storedPoint)
        #expect(try SpatialIndexValueCodec.decode(encodedPoint) == storedPoint)
        let storedPosition = SpatialIndexStoredCoordinate(
            point: point,
            height: 42.5
        )
        let encodedPosition = SpatialIndexValueCodec.encode(storedPosition)
        #expect(
            try SpatialIndexValueCodec.decode(encodedPosition)
                == storedPosition
        )
        #expect {
            _ = try SpatialIndexValueCodec.decode(ByteString([0x01]))
        } throws: { error in
            error as? SpatialIndexValueCodecError == .invalidByteCount(1)
        }

        let antipode = try GeographicPoint(latitude: 0, longitude: 180)
        let distance = CellDistanceCalculator.haversineDistance(
            from: try GeographicPoint(latitude: 0, longitude: 0),
            to: antipode
        )
        #expect(distance.isFinite)
        #expect(distance > 20_000_000)
    }

    @Test("Spatial maintenance preserves geographic position height")
    func spatialMaintenancePreservesPositionHeight() async throws {
        let entity = try SpatialPositionValueItem.schemaEntity
        let provider = SpatialIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            SpatialPositionValueItem.self
        )
        try entityRuntime.register(provider)
        let container = try await DBContainer.open(
            testing: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "spatial-position-value-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let item = SpatialPositionValueItem(
            id: "position",
            location: try GeographicPosition(
                latitude: 35,
                longitude: 139,
                ellipsoidalHeightInMeters: 42.5
            )
        )
        let context = container.testBaseContext()
        try context.insert(item)
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "spatial_position_value",
                    indexType: .spatial,
                    for: SpatialPositionValueItem.self,
                    transaction: transaction
                )
            )
            #expect(
                readable.physicalLayout == (try IndexPhysicalLayout(
                    name: "spatial.exact-coordinate",
                    revision: 1
                ))
            )
            let coordinate = SpatialIndexStoredCoordinate(
                point: item.location.point,
                height: item.location.ellipsoidalHeightInMeters
            )
            let code = SpatialCodeEncoder.encode(
                coordinate,
                encoding: .morton,
                level: 8
            )
            let bytes = try #require(
                try await transaction.getValue(
                    for: readable.subspace.pack(Tuple(code, item.id))
                )
            )
            #expect(try SpatialIndexValueCodec.decode(bytes) == coordinate)
        }
    }

    @Test("Nearby validates its physical contract")
    func validatesPhysicalContract() throws {
        let descriptor = try #require(
            NearbyFusionExecutionItem.schemaEntity.indexes.first {
                $0.name == "nearby_fusion_s2"
            }
        )
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let input = try Nearby(NearbyFusionExecutionItem.fields.location)
            .index(named: "nearby_fusion_s2")
            .within(radiusKm: 1, of: center)
            .fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Nearby must lower to an index operation")
            return
        }
        let executor = SpatialFusionIndexReadExecutor()
        try executor.validate(
            FusionIndexValidationRequest(
                source: source,
                scoring: input.scoring,
                descriptor: descriptor
            )
        )

        var invalidReference = source.parameters
        invalidReference[SpatialFusionReadParameter.referencePoint] =
            .geographicPoint(
                try GeographicPoint(latitude: 1, longitude: 1)
            )
        #expect {
            try executor.validate(
                FusionIndexValidationRequest(
                    source: FusionIndexSource(
                        selection: source.selection,
                        referencedFields: source.referencedFields,
                        parameters: invalidReference
                    ),
                    scoring: input.scoring,
                    descriptor: descriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .spatial,
                parameter: SpatialFusionReadParameter.referencePoint
            )
        }
    }

    @Test("Nearby executes exact distance ordering for both encodings")
    func executesExactDistanceOrdering() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)

        for indexName in ["nearby_fusion_s2", "nearby_fusion_morton"] {
            let nearby = try Nearby(
                NearbyFusionExecutionItem.fields.location
            )
                .index(named: indexName)
                .within(
                    radiusKm: 2,
                    of: try GeographicPoint(latitude: 0, longitude: 0)
                )
                .limit(2)
            let result = try await context.execute(
                FusionQuery<NearbyFusionExecutionItem> {
                    nearby
                }
            )
            #expect(result.results.map(\.item.id) == ["center", "near"])
        }
    }

    @Test("Nearby applies exact bounding boxes")
    func appliesExactBoundingBoxes() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)
        let bounds = try BoundingBox(
            minLatitude: -0.001,
            minLongitude: -0.001,
            maxLatitude: 0.001,
            maxLongitude: 0.008
        )
        let nearby = try Nearby(NearbyFusionExecutionItem.fields.location)
            .index(named: "nearby_fusion_s2")
            .within(bounds: bounds)
            .limit(4)

        let result = try await context.execute(
            FusionQuery<NearbyFusionExecutionItem> {
                nearby
            }
        )
        #expect(result.results.map(\.item.id) == ["near", "center"])
    }

    @Test("Nearby restricts candidates before selecting top K")
    func restrictsCandidatesBeforeTopK() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try await insertFixtures(into: context)
        let eligible = try Filter(
            NearbyFusionExecutionItem.fields.eligible,
            equals: true
        )
        let nearby = try Nearby(NearbyFusionExecutionItem.fields.location)
            .index(named: "nearby_fusion_s2")
            .within(
                radiusKm: 2,
                of: try GeographicPoint(latitude: 0, longitude: 0)
            )
            .limit(2)

        let result = try await context.execute(
            FusionQuery<NearbyFusionExecutionItem> {
                eligible
                nearby
            }
        )
        #expect(result.results.map(\.item.id) == ["near", "farther"])
    }

    @Test("Malformed spatial values fail as corruption")
    func malformedValueFailsAsCorruption() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let item = NearbyFusionExecutionItem(
            id: "corrupted",
            name: "Corrupted",
            location: try GeographicPoint(latitude: 0, longitude: 0),
            eligible: true
        )
        try context.insert(item)
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "nearby_fusion_s2",
                    indexType: .spatial,
                    for: NearbyFusionExecutionItem.self,
                    transaction: transaction
                )
            )
            let code = S2Geometry.encode(
                latitude: item.location.latitude,
                longitude: item.location.longitude,
                level: 8
            )
            try transaction.setValue(
                ByteString([0x01]),
                for: readable.subspace.pack(Tuple(code, item.id))
            )
        }
        let nearby = try Nearby(NearbyFusionExecutionItem.fields.location)
            .index(named: "nearby_fusion_s2")
            .within(
                radiusKm: 1,
                of: try GeographicPoint(latitude: 0, longitude: 0)
            )

        await #expect {
            _ = try await context.execute(
                FusionQuery<NearbyFusionExecutionItem> {
                    nearby
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .corruptedIndex(.spatial)
        }
    }

    private func insertFixtures(
        into context: DatabaseContext
    ) async throws {
        for item in [
            NearbyFusionExecutionItem(
                id: "center",
                name: "Center",
                location: try GeographicPoint(latitude: 0, longitude: 0),
                eligible: false
            ),
            NearbyFusionExecutionItem(
                id: "near",
                name: "Near",
                location: try GeographicPoint(latitude: 0, longitude: 0.005),
                eligible: true
            ),
            NearbyFusionExecutionItem(
                id: "farther",
                name: "Farther",
                location: try GeographicPoint(latitude: 0, longitude: 0.01),
                eligible: true
            ),
            NearbyFusionExecutionItem(
                id: "outside",
                name: "Outside",
                location: try GeographicPoint(latitude: 0, longitude: 0.03),
                eligible: true
            ),
        ] {
            try context.insert(item)
        }
        try await context.save()
    }

    private func makeContainer() async throws -> DBContainer {
        let entity = try NearbyFusionExecutionItem.schemaEntity
        let provider = SpatialIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            NearbyFusionExecutionItem.self
        )
        try entityRuntime.register(provider)
        return try await DBContainer.open(
            testing: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "spatial-fusion-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    SpatialFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
    }
}
