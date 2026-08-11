#if DATABASE_WIRE_RUNTIME_MULTIPLE_BASES
import DatabaseTypes

public struct DatabaseLegacyLayoutMigrationJobState:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    enum Phase: UInt8, Sendable, Hashable {
        case verifySource = 0
        case prepareDestination = 1
        case copy = 2
        case reverifySource = 3
        case verifyDestination = 4
        case rebuildAndCutOver = 5
        case cleanup = 6
    }

    private static let formatVersion: UInt8 = 1
    private static let digestByteCount = 32

    let phase: Phase
    let entryIndex: UInt64
    let continuation: ByteString?
    let digest: ByteString
    let keyCount: UInt64
    let byteCount: UInt64
    let sourceDigest: ByteString?
    let sourceKeyCount: UInt64
    let sourceByteCount: UInt64

    init(
        phase: Phase,
        entryIndex: UInt64 = 0,
        continuation: ByteString? = nil,
        digest: ByteString = ByteString(repeating: 0, count: 32),
        keyCount: UInt64 = 0,
        byteCount: UInt64 = 0,
        sourceDigest: ByteString? = nil,
        sourceKeyCount: UInt64 = 0,
        sourceByteCount: UInt64 = 0
    ) {
        self.phase = phase
        self.entryIndex = entryIndex
        self.continuation = continuation
        self.digest = digest
        self.keyCount = keyCount
        self.byteCount = byteCount
        self.sourceDigest = sourceDigest
        self.sourceKeyCount = sourceKeyCount
        self.sourceByteCount = sourceByteCount
    }

    public func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "phase", value: .uint8(phase.rawValue)),
                (key: "entryIndex", value: .uint64(entryIndex)),
                (
                    key: "continuation",
                    value: continuation.map(FieldValue.bytes) ?? .null
                ),
                (key: "digest", value: .bytes(digest)),
                (key: "keyCount", value: .uint64(keyCount)),
                (key: "byteCount", value: .uint64(byteCount)),
                (
                    key: "sourceDigest",
                    value: sourceDigest.map(FieldValue.bytes) ?? .null
                ),
                (key: "sourceKeyCount", value: .uint64(sourceKeyCount)),
                (key: "sourceByteCount", value: .uint64(sourceByteCount)),
            ]))
        } catch {
            throw .invalidValue("Legacy migration state is not canonical")
        }
    }

    public init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 10,
              fields["version"]?.uint8Value == Self.formatVersion,
              let rawPhase = fields["phase"]?.uint8Value,
              let phase = Phase(rawValue: rawPhase),
              let entryIndex = fields["entryIndex"]?.uint64Value,
              let continuationValue = fields["continuation"],
              let digest = fields["digest"]?.bytesValue,
              digest.count == Self.digestByteCount,
              let keyCount = fields["keyCount"]?.uint64Value,
              let byteCount = fields["byteCount"]?.uint64Value,
              let sourceDigestValue = fields["sourceDigest"],
              let sourceKeyCount = fields["sourceKeyCount"]?.uint64Value,
              let sourceByteCount = fields["sourceByteCount"]?.uint64Value
        else {
            throw .invalidValue("Invalid legacy migration state")
        }
        let continuation: ByteString?
        if continuationValue.isNull {
            continuation = nil
        } else if let bytes = continuationValue.bytesValue, !bytes.isEmpty {
            continuation = bytes
        } else {
            throw .invalidValue("Invalid legacy migration continuation")
        }
        let sourceDigest: ByteString?
        if sourceDigestValue.isNull {
            sourceDigest = nil
        } else if let bytes = sourceDigestValue.bytesValue,
                  bytes.count == Self.digestByteCount {
            sourceDigest = bytes
        } else {
            throw .invalidValue("Invalid legacy migration source digest")
        }
        switch phase {
        case .verifySource:
            guard sourceDigest == nil else {
                throw .invalidValue("Initial verification has a source digest")
            }
        case .prepareDestination, .copy, .reverifySource,
             .verifyDestination, .rebuildAndCutOver, .cleanup:
            guard sourceDigest != nil else {
                throw .invalidValue("Legacy migration source is not fixed")
            }
        }
        self.phase = phase
        self.entryIndex = entryIndex
        self.continuation = continuation
        self.digest = digest
        self.keyCount = keyCount
        self.byteCount = byteCount
        self.sourceDigest = sourceDigest
        self.sourceKeyCount = sourceKeyCount
        self.sourceByteCount = sourceByteCount
    }

    func resetting(to phase: Phase) -> Self {
        Self(
            phase: phase,
            sourceDigest: sourceDigest,
            sourceKeyCount: sourceKeyCount,
            sourceByteCount: sourceByteCount
        )
    }
}

#endif
