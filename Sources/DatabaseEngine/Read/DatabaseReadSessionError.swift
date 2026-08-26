/// Invalid composition of one bounded database read session.
public enum DatabaseReadSessionError: Error, Sendable, Equatable {
    /// Execution and storage resources must charge the same request meter.
    case workMeterMismatch

    /// A prepared plan cannot cross the schema generation that produced it.
    case schemaGenerationMismatch

    /// Authorization evidence cannot cross its captured policy or principal.
    case authorizationMismatch
}
