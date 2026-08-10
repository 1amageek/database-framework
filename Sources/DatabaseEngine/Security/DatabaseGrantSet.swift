import DatabaseKit

/// Direct Grants and revision stored for one exact Security resource.
package struct DatabaseGrantSet: Sendable, Hashable {
    package let revision: UInt64
    package let grants: [Security.Grant]
}
