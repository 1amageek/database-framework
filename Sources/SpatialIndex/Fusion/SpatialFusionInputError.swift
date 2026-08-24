/// Invalid immutable spatial Fusion input.
public enum SpatialFusionInputError: Error, Sendable, Equatable {
    case nonFiniteRadius
    case negativeRadius(Double)
}
