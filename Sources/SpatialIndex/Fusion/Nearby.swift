import DatabaseKit
import DatabaseTypes

/// Immutable geospatial input for a canonical Fusion plan.
public struct Nearby<Item: Persistable>: FusionQueryInput, Sendable {
    private enum Constraint: Sendable {
        case radius(center: GeographicPoint, meters: Double)
        case bounds(BoundingBox, center: GeographicPoint)
    }

    private let field: FieldIdentity
    private var constraint: Constraint?
    private var indexName: String?
    private var resultLimit: UInt64?

    public init(_ field: Field<Item, GeographicPoint>) {
        self.field = field.identity
    }

    public init(_ field: Field<Item, GeographicPoint?>) {
        self.field = field.identity
    }

    public func index(named name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    public func within(
        radiusKm: Double,
        of center: GeographicPoint
    ) throws(SpatialFusionInputError) -> Self {
        guard radiusKm.isFinite else { throw .nonFiniteRadius }
        guard radiusKm >= 0 else { throw .negativeRadius(radiusKm) }
        let radiusMeters = radiusKm * 1_000
        guard radiusMeters.isFinite else { throw .nonFiniteRadius }
        var copy = self
        copy.constraint = .radius(center: center, meters: radiusMeters)
        return copy
    }

    public func within(
        bounds: BoundingBox
    ) throws(BoundingBoxError) -> Self {
        var copy = self
        copy.constraint = .bounds(bounds, center: try bounds.center())
        return copy
    }

    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.resultLimit = count
        return copy
    }

    public var fusionInput: FusionInput {
        var parameters: [String: FieldValue] = [
            SpatialFusionReadParameter.fieldName: .string(field.name),
        ]
        switch constraint {
        case .radius(let center, let meters):
            parameters[SpatialFusionReadParameter.operation] = .string(
                SpatialFusionReadParameter.radiusOperation
            )
            parameters[SpatialFusionReadParameter.center] =
                .geographicPoint(center)
            parameters[SpatialFusionReadParameter.radiusMeters] =
                .float64(meters)
            parameters[SpatialFusionReadParameter.referencePoint] =
                .geographicPoint(center)
        case .bounds(let bounds, let center):
            parameters[SpatialFusionReadParameter.operation] = .string(
                SpatialFusionReadParameter.boundsOperation
            )
            parameters[SpatialFusionReadParameter.minimumLatitude] =
                .float64(bounds.southwest.latitude)
            parameters[SpatialFusionReadParameter.minimumLongitude] =
                .float64(bounds.southwest.longitude)
            parameters[SpatialFusionReadParameter.maximumLatitude] =
                .float64(bounds.northeast.latitude)
            parameters[SpatialFusionReadParameter.maximumLongitude] =
                .float64(bounds.northeast.longitude)
            parameters[SpatialFusionReadParameter.referencePoint] =
                .geographicPoint(center)
        case nil:
            break
        }
        let selection: FusionIndexSelection = if let indexName {
            .named(name: indexName, type: .spatial)
        } else {
            .matching(type: .spatial, fields: [field], fieldMatch: .exact)
        }
        return FusionInput(
            operation: .index(
                FusionIndexSource(
                    selection: selection,
                    referencedFields: [field],
                    parameters: parameters
                )
            ),
            scoring: .annotation(
                name: "distance",
                order: .lowerIsBetter
            ),
            limit: resultLimit
        )
    }
}
