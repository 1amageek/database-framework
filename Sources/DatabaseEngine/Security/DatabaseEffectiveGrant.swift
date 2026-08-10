import DatabaseKit

/// Union of the exact principal and role Grants contributing to one decision.
package struct DatabaseEffectiveGrant: Sendable, Hashable {
    package let access: Security.Access
    package let contributors: [Security.Grant]

    package init(
        access: Security.Access,
        contributors: [Security.Grant]
    ) {
        self.access = access
        self.contributors = contributors
    }
}
