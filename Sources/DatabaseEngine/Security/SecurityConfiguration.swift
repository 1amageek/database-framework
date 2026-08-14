/// Configuration for schema-driven entity and field policy evaluation.
///
/// This value controls the policy layer applied by the in-process engine.
/// When `MultipleBases` is enabled, persisted Grant authorization is an
/// additional, independent boundary evaluated before this policy.
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

    /// Disables schema-driven policy evaluation in isolated test runtimes.
    /// With `MultipleBases`, this does not disable persisted Grants.
    @_spi(Testing)
    public static let disabledForTesting = SecurityConfiguration(
        policyEvaluation: .disabledForTesting
    )
}
