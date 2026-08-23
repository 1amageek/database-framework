import DatabaseKit

package enum DatabaseTransactionAccessMode: Sendable {
    case readOnly
    case readWrite

    package var allowsMutation: Bool {
        self == .readWrite
    }
}

/// The only authorization classes that may mint a mutation-capable storage
/// projection. Keeping this separate from `Security.Access` makes it
/// impossible to request a read Grant while receiving write authority.
package enum DatabaseMutationAuthorization: Sendable {
    case write
    case administer

    package var securityAccess: Security.Access {
        switch self {
        case .write:
            .write
        case .administer:
            .administer
        }
    }
}
