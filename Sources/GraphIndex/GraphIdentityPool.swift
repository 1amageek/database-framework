import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import Synchronization

/// Request-scoped canonical RDF identity interning.
///
/// The first physical key for a unique term owns the retained byte slice.
/// Repeated edge occurrences reuse that identity without retaining every key or
/// repeatedly materializing semantic RDF strings.
package final class GraphIdentityPool: Sendable {
    private struct Entry: Sendable {
        let fingerprint: RDFTermStorageFingerprint
        let identity: GraphIdentity
    }

    private let identities = Mutex(
        [UInt64: [Entry]]()
    )

    func internRDF(
        _ encoded: ByteString,
        role: GraphRDFComponentRole
    ) throws -> GraphIdentity {
        do throws(RDFTermStorageError) {
            return try RDFTermStorageFormat.withValidatedBytes(
                encoded,
                role: role.databaseRole
            ) { buffer, validation in
                identities.withLock { identities in
                    let bucketKey = Self.bucketKey(
                        for: validation.fingerprint
                    )
                    if let bucket = identities[bucketKey] {
                        for entry in bucket where
                            entry.fingerprint == validation.fingerprint
                                && entry.identity.canonicalRDFBytesEqual(buffer) {
                            return entry.identity
                        }
                    }

                    let identity = GraphIdentity
                        .retainingValidatedRDFBytes(
                            encoded,
                            fingerprint: validation.fingerprint
                        )
                    identities[bucketKey, default: []].append(
                        Entry(
                            fingerprint: validation.fingerprint,
                            identity: identity
                        )
                    )
                    return identity
                }
            }
        } catch let error {
            if case .invalidRole = error {
                throw invalidRoleError(for: role)
            }
            throw GraphIndexError.invalidRDFEncoding(error)
        }
    }

    private static func bucketKey(
        for fingerprint: RDFTermStorageFingerprint
    ) -> UInt64 {
        fingerprint.high
            ^ ((fingerprint.low << 29) | (fingerprint.low >> 35))
            ^ UInt64(truncatingIfNeeded: fingerprint.byteCount)
    }

    private func invalidRoleError(
        for role: GraphRDFComponentRole
    ) -> GraphIndexError {
        switch role {
        case .subject:
            .invalidRDFSubject
        case .predicate:
            .invalidRDFPredicate
        case .object:
            .invalidRDFObject
        case .graph:
            .invalidRDFGraphName
        }
    }
}

enum GraphRDFComponentRole: Sendable {
    case subject
    case predicate
    case object
    case graph

    var databaseRole: RDFTermRole {
        switch self {
        case .subject: .subject
        case .predicate: .predicate
        case .object: .object
        case .graph: .graphName
        }
    }
}
