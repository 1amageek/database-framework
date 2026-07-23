import DatabaseEngine
import DatabaseValue
import StorageKit

/// Canonical identity used by graph scanners and algorithms.
///
/// Property-graph identities retain their native string. RDF identities retain
/// only their validated canonical bytes. Algorithms compare and hash those
/// bytes directly; semantic RDF terms are materialized only at an output
/// boundary.
public struct GraphIdentity: Sendable, Hashable, Comparable {
    public enum Representation: Sendable, Hashable {
        case propertyGraph
        case rdf
    }

    private enum Storage: Sendable {
        case propertyGraph(String)
        case rdf(
            DatabaseBytes,
            fingerprint: DatabaseRDFTermEncodingFingerprint
        )
    }

    private let storage: Storage

    private init(storage: Storage) {
        self.storage = storage
    }

    public static func identifier(_ value: String) -> GraphIdentity {
        GraphIdentity(storage: .propertyGraph(value))
    }

    public static func rdf(
        _ term: DatabaseRDFTerm
    ) throws(DatabaseRDFTermCodecError) -> GraphIdentity {
        let encoded = try DatabaseRDFTermCodec.encode(term)
        return try DatabaseRDFTermCodec.withValidatedBytes(encoded) {
            _, validation in
            GraphIdentity(storage: .rdf(
                encoded,
                fingerprint: validation.fingerprint
            ))
        }
    }

    package static func retainingValidatedRDFBytes(
        _ encoded: DatabaseBytes,
        fingerprint: DatabaseRDFTermEncodingFingerprint
    ) -> GraphIdentity {
        GraphIdentity(storage: .rdf(
            encoded,
            fingerprint: fingerprint
        ))
    }

    public var representation: Representation {
        switch storage {
        case .propertyGraph:
            return .propertyGraph
        case .rdf:
            return .rdf
        }
    }

    public var identifier: String? {
        guard case .propertyGraph(let value) = storage else { return nil }
        return value
    }

    public func decodeRDFTerm(
    ) throws(DatabaseRDFTermCodecError) -> DatabaseRDFTerm? {
        guard case .rdf(let encoded, _) = storage else { return nil }
        return try DatabaseRDFTermCodec.decode(encoded)
    }

    package var tupleElement: any TupleElement {
        switch storage {
        case .propertyGraph(let value):
            return value
        case .rdf(let encoded, _):
            return Bytes(retaining: encoded)
        }
    }

    package var canonicalRDFBytes: DatabaseBytes? {
        guard case .rdf(let encoded, _) = storage else { return nil }
        return encoded
    }

    package func canonicalRDFBytesEqual(
        _ candidate: UnsafeRawBufferPointer
    ) -> Bool {
        guard case .rdf(let encoded, _) = storage,
              encoded.count == candidate.count else {
            return false
        }
        return encoded.withUnsafeBytes { existing in
            existing.elementsEqual(candidate)
        }
    }

    package func requirePropertyGraphIdentifier() throws -> String {
        guard case .propertyGraph(let value) = storage else {
            throw GraphIndexError.identityRepresentationMismatch(
                expected: .propertyGraph,
                actual: .rdf
            )
        }
        return value
    }

    public static func == (lhs: GraphIdentity, rhs: GraphIdentity) -> Bool {
        switch (lhs.storage, rhs.storage) {
        case (.propertyGraph(let left), .propertyGraph(let right)):
            return left == right
        case (.rdf(let left, _), .rdf(let right, _)):
            return left == right
        default:
            return false
        }
    }

    public func hash(into hasher: inout Hasher) {
        switch storage {
        case .propertyGraph(let value):
            hasher.combine(UInt8(0))
            hasher.combine(value)
        case .rdf(_, let fingerprint):
            hasher.combine(UInt8(1))
            hasher.combine(fingerprint.high)
            hasher.combine(fingerprint.low)
            hasher.combine(fingerprint.byteCount)
        }
    }

    public static func < (lhs: GraphIdentity, rhs: GraphIdentity) -> Bool {
        switch (lhs.storage, rhs.storage) {
        case (.propertyGraph(let left), .propertyGraph(let right)):
            return left < right
        case (.rdf(let left, _), .rdf(let right, _)):
            return left.lexicographicallyPrecedes(right)
        case (.propertyGraph, .rdf):
            return true
        case (.rdf, .propertyGraph):
            return false
        }
    }
}

extension GraphIdentity: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .identifier(value)
    }
}

extension GraphIdentity: CustomStringConvertible {
    public var description: String {
        switch storage {
        case .propertyGraph(let value):
            return value
        case .rdf(let encoded, _):
            return "rdf(\(encoded.count) canonical bytes)"
        }
    }
}
