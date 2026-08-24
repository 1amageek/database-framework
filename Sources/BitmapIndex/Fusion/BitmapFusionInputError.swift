import DatabaseTypes

public enum BitmapFusionInputError: Error, Sendable, Equatable {
    case unsupportedValue(FieldValue)
}
