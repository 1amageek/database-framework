import DatabaseValue
import Graph
import Synchronization

/// Request-scoped canonical RDF identity interning.
///
/// The first physical key for a unique term owns the retained byte slice.
/// Repeated edge occurrences reuse that identity without retaining every key or
/// repeatedly materializing semantic RDF strings.
package final class GraphIdentityPool: Sendable {
    private struct Entry: Sendable {
        let identity: GraphIdentity
    }

    private let identities = Mutex(
        [DatabaseRDFTermEncodingFingerprint: [Entry]]()
    )

    func internRDF(
        _ encoded: DatabaseBytes,
        role: GraphRDFComponentRole
    ) throws -> GraphIdentity {
        do {
            return try DatabaseRDFTermCodec.withValidatedBytes(
                encoded,
                role: role.databaseRole
            ) { buffer, validation in
                identities.withLock { identities in
                    if let bucket = identities[validation.fingerprint] {
                        for entry in bucket where
                            entry.identity.canonicalRDFBytesEqual(buffer) {
                            return entry.identity
                        }
                    }

                    let identity = GraphIdentity
                        .retainingValidatedRDFBytes(
                            encoded,
                            fingerprint: validation.fingerprint
                        )
                    identities[validation.fingerprint, default: []].append(
                        Entry(identity: identity)
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

    var databaseRole: DatabaseRDFTermRole {
        switch self {
        case .subject: .subject
        case .predicate: .predicate
        case .object: .object
        case .graph: .graphName
        }
    }
}
