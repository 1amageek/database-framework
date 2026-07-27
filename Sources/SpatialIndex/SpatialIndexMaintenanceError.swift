/// Failures that prevent deterministic spatial index maintenance.
public enum SpatialIndexMaintenanceError: Error, Sendable, Equatable {
    case invalidFieldExpression(indexName: String)
    case missingCoordinate(fieldName: String)
    case unsupportedCoordinateValue(fieldName: String)
}
