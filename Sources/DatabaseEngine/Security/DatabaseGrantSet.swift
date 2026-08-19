#if DATABASE_MULTI_BASE
import DatabaseKit

/// Direct Grants and revision stored for one exact Security resource.
@_spi(DatabaseExecution)
public struct DatabaseGrantSet: Sendable, Hashable {
    public let revision: UInt64
    public let grants: [Security.Grant]

    public init(revision: UInt64, grants: [Security.Grant]) {
        self.revision = revision
        self.grants = grants
    }
}
#endif
