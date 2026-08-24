import DatabaseKit
import DatabaseTypes
import Testing

@testable import SpatialIndex

@Persistable
private struct NearbyFusionInputItem {
    var id: String
    var location: GeographicPoint
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
}
