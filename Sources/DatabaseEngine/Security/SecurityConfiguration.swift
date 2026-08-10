/// Configuration for schema-driven entity and field policy evaluation.
///
/// Base-level operation access is always enforced by persisted Grants. This
/// value controls only the policy layer that runs after Grant authorization.
public struct SecurityConfiguration: Sendable {
    package enum PolicyEvaluation: Sendable {
        case enabled
        case disabledForTesting
    }

    package let policyEvaluation: PolicyEvaluation

    private init(policyEvaluation: PolicyEvaluation) {
        self.policyEvaluation = policyEvaluation
    }

    /// Enables registered entity and field policy evaluation.
    public static func enabled() -> SecurityConfiguration {
        SecurityConfiguration(policyEvaluation: .enabled)
    }

    /// Disables only schema-driven policy evaluation in isolated test
    /// runtimes. Persisted database and Base Grants remain mandatory.
    @_spi(Testing)
    public static let disabledForTesting = SecurityConfiguration(
        policyEvaluation: .disabledForTesting
    )
}
