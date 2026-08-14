#if DATABASE_MULTIPLE_BASES
import DatabaseKit

/// Union of the exact principal and role Grants contributing to one decision.
@_spi(DatabaseExecution)
public struct DatabaseEffectiveGrant: Sendable, Hashable {
    public let access: Security.Access
    public let contributors: [Security.Grant]

    public init(
        access: Security.Access,
        contributors: [Security.Grant]
    ) {
        self.access = access
        self.contributors = contributors
    }
}
#endif
