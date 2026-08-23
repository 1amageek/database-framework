import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Foundation
import StorageKit
import TestSupport
import Testing

@testable import SpatialIndex

@Persistable
private struct SpatialPlannerItem {
    var id: String
    var location: GeographicPoint

    init(id: String, latitude: Double, longitude: Double) throws {
        self.id = id
        self.location = try GeographicPoint(
            latitude: latitude,
            longitude: longitude
        )
    }
}

@Persistable
private struct SpatialPlannerIntIDItem {
    var id: Int64
    var location: GeographicPoint
}

@Suite("Spatial scan planning")
struct SpatialScanPlannerTests {
    @Test("S2 constraints produce covering cells")
    func s2ConstraintProducesCells() throws {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinDistance(
                    center: try GeographicPoint(
                        latitude: 35.6812,
                        longitude: 139.7671
                    ),
                    radiusMeters: 1000
                )
            ),
            encoding: .s2,
            level: 12
        )

        guard case .cells(let cellPlan) = plan else {
            Issue.record("Expected S2 cell scan plan")
            return
        }
        #expect(!cellPlan.cells.isEmpty)
    }

    @Test("Narrow S2 coverings include the query-center cell")
    func narrowS2CoveringIncludesCenterCell() throws {
        let center = try GeographicPoint(latitude: 0, longitude: 0)
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinDistance(
                    center: center,
                    radiusMeters: 1_000
                )
            ),
            encoding: .s2,
            level: 10
        )

        guard case .cells(let cellPlan) = plan else {
            Issue.record("Expected S2 cell scan plan")
            return
        }
        #expect(
            cellPlan.cells.contains(
                S2Geometry.encode(
                    latitude: center.latitude,
                    longitude: center.longitude,
                    level: 10
                )
            )
        )
    }

    @Test("World coverings fail before materialization")
    func worldCoveringIsBounded() throws {
        do {
            _ = try SpatialScanPlanner.plan(
                for: SpatialConstraint(
                    type: .withinBounds(
                        minLat: -90,
                        minLon: -180,
                        maxLat: 90,
                        maxLon: 180
                    )
                ),
                encoding: .s2,
                level: 17
            )
            Issue.record("Expected a bounded covering failure")
        } catch SpatialScanPlanningError.coveringCellLimitExceeded(
            let required,
            let maximum
        ) {
            #expect(required > 10_000)
            #expect(maximum == 10_000)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }

    @Test("Cover planning releases a rejected reservation")
    func tightBudgetReleasesReservation() throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumIntermediateRows: 1,
                maximumIntermediateBytes: 64
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try SpatialScanPlanner.plan(
                for: SpatialConstraint(
                    type: .withinDistance(
                        center: try GeographicPoint(
                            latitude: 35.6812,
                            longitude: 139.7671
                        ),
                        radiusMeters: 1_000
                    )
                ),
                encoding: .s2,
                level: 12,
                workMeter: meter
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Cover planning observes cancellation before retaining cells")
    func cancellationReleasesReservation() async throws {
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
        let task = Task {
            try SpatialScanPlanner.plan(
                for: SpatialConstraint(
                    type: .withinBounds(
                        minLat: 35,
                        minLon: 139,
                        maxLat: 36,
                        maxLon: 140
                    )
                ),
                encoding: .s2,
                level: 12,
                workMeter: meter
            )
        }
        task.cancel()
        await #expect(throws: CancellationError.self) {
            _ = try await task.value
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Non-finite and negative radii fail before planning")
    func invalidRadiiAreTypedFailures() throws {
        let center = try GeographicPoint(latitude: 35, longitude: 139)
        for radius in [Double.nan, .infinity, -1] {
            #expect(throws: SpatialScanPlanningError.self) {
                _ = try SpatialScanPlanner.plan(
                    for: SpatialConstraint(
                        type: .withinDistance(
                            center: center,
                            radiusMeters: radius
                        )
                    ),
                    encoding: .s2,
                    level: 12
                )
            }
        }
    }

    @Test("Morton constraints produce code ranges")
    func mortonConstraintProducesCodeRange() throws {
        let plan = try SpatialScanPlanner.plan(
            for: SpatialConstraint(
                type: .withinBounds(
                    minLat: 35.0,
                    minLon: 139.0,
                    maxLat: 36.0,
                    maxLon: 140.0
                )
            ),
            encoding: .morton,
            level: 12
        )

        guard case .codeRanges(let ranges) = plan else {
            Issue.record("Expected Morton code-range scan plan")
            return
        }
        #expect(ranges.count == 1)
        #expect(ranges[0].min <= ranges[0].max)
    }

    @Test("Distance planning crosses the antimeridian without dropping cells")
    func distancePlanningCrossesAntimeridian() throws {
        let center = try GeographicPoint(latitude: 0, longitude: 179.9)
        let constraint = SpatialConstraint(
            type: .withinDistance(
                center: center,
                radiusMeters: 50_000
            )
        )

        let s2Plan = try SpatialScanPlanner.plan(
            for: constraint,
            encoding: .s2,
            level: 12
        )
        guard case .cells(let cellPlan) = s2Plan else {
            Issue.record("Expected S2 cell scan plan")
            return
        }
        #expect(
            cellPlan.cells.contains(
                S2Geometry.encode(
                    latitude: 0,
                    longitude: -179.9,
                    level: 12
                )
            )
        )

        let mortonPlan = try SpatialScanPlanner.plan(
            for: constraint,
            encoding: .morton,
            level: 12
        )
        guard case .codeRanges(let ranges) = mortonPlan else {
            Issue.record("Expected Morton code-range scan plan")
            return
        }
        let eastCode = SpatialScanPlanner.mortonCode(
            latitude: 0,
            longitude: 179.9,
            level: 12
        )
        let westCode = SpatialScanPlanner.mortonCode(
            latitude: 0,
            longitude: -179.9,
            level: 12
        )
        #expect(ranges.count == 2)
        #expect(ranges.contains { $0.min <= eastCode && eastCode <= $0.max })
        #expect(ranges.contains { $0.min <= westCode && westCode <= $0.max })
    }

    @Test("Morton write code matches scan planner coordinate contract")
    func mortonWriteCodeMatchesPlannerCoordinateContract() async throws {
        let index = try ResolvedIndex(
            for: SpatialPlannerItem.self,
            name: "location",
            definition: spatialIndexDefinition(
                fieldName: "location",
                fieldNumber: 2,
                encoding: .morton,
                level: 12
            ),
            rootExpression: FieldKeyExpression(fieldName: "location")
        )
        let maintainer = SpatialIndexMaintainer<SpatialPlannerItem>(
            index: index,
            encoding: .morton,
            level: 12,
            subspace: Subspace("spatial"),
            idExpression: FieldKeyExpression(fieldName: "id")
        )
        let item = try SpatialPlannerItem(
            id: "tokyo",
            latitude: 35.6812,
            longitude: 139.7671
        )
        let keys = try await maintainer.computeIndexKeys(for: item, id: Tuple("tokyo"))

        let keyTuple = try Subspace("spatial").unpack(keys[0])
        let storedElement = try #require(keyTuple[0])
        let storedCode = UInt64(bitPattern: try TypeConversion.int64(from: storedElement))
        let plannerCode = SpatialScanPlanner.mortonCode(
            latitude: item.location.latitude,
            longitude: item.location.longitude,
            level: 12
        )

        #expect(storedCode == plannerCode)
    }

    @Test("fetch limit derives bounded candidate scan budget")
    func fetchLimitDerivesCandidateScanBudget() {
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: nil) == nil)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 0) == nil)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 1) == 256)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 10) == 320)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 1_000) == 10_000)
        #expect(SpatialScanBudget.candidateLimit(forFetchLimit: 20_000) == 20_000)
    }

    @Test("range read limit tracks remaining candidate budget")
    func rangeReadLimitTracksRemainingCandidateBudget() {
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: nil, emittedCount: 10) == 0)
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: 100, emittedCount: 25) == 76)
        #expect(SpatialScanBudget.rangeReadLimit(totalLimit: 100, emittedCount: 100) == 1)
    }

    @Test("spatial primary-key extraction supports non-string IDs")
    func spatialPrimaryKeyExtractionSupportsNonStringIDs() throws {
        let item = SpatialPlannerIntIDItem(
            id: 42,
            location: try GeographicPoint(
                latitude: 35.6812,
                longitude: 139.7671
            )
        )

        let primaryKey = try SpatialPrimaryKey.tuple(for: item)

        #expect(Data(primaryKey.pack()) == Data(Tuple(Int64(42)).pack()))
    }
}
