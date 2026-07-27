import DatabaseTypes

/// A spatial predicate executed by the spatial index.
public struct SpatialConstraint: Sendable {
    public let type: SpatialConstraintType

    public init(type: SpatialConstraintType) {
        self.type = type
    }
}

public enum SpatialConstraintType: Sendable {
    case withinDistance(
        center: GeographicPoint,
        radiusMeters: Double
    )
    case withinBounds(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double
    )
    case withinPolygon(points: [GeographicPoint])
}
