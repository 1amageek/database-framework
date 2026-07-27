import DatabaseTypes

public enum BoundingBoxError: Error, Sendable, Equatable {
    case invalidSouthwest(GeographicPointError)
    case invalidNortheast(GeographicPointError)
    case invertedLatitudeRange(minimum: Double, maximum: Double)
    case invertedLongitudeRange(minimum: Double, maximum: Double)
    case nonFiniteRadius
    case negativeRadius(Double)
    case invalidCenter(GeographicPointError)
}
