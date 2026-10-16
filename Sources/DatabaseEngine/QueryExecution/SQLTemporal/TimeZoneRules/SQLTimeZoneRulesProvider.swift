/// An explicitly injected, immutable SQL named-zone capability.
public protocol SQLTimeZoneRulesProvider: Sendable {
    /// Resolves only supplied names and aliases, charging each lookup's work.
    func rules(named name: String, workMeter: DatabaseWorkMeter) throws -> SQLTimeZoneRules
}
