import DatabaseTypes

struct DatabaseMutationStateManifest: Sendable, Hashable {
    // Version 2 generalizes the former server operation/digest manifest into
    // framework-owned discriminator and fingerprint fields.
    static let formatVersion: UInt16 = 2
    static let chunkByteCount: UInt32 = 90_000

    let discriminator: ByteString
    let requestFingerprint: ByteString
    let outcomeFingerprint: ByteString
    let totalOutcomeBytes: UInt64
    let chunkCount: UInt32

    func encode(
        limits: DatabaseMutationStateLimits
    ) throws -> ByteString {
        try validate(limits: limits)
        do {
            return try StorageFrameEncoder.encode {
                (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
                encoder.writeUInt16(Self.formatVersion)
                try encoder.writeBytes(discriminator)
                try encoder.writeBytes(requestFingerprint)
                try encoder.writeBytes(outcomeFingerprint)
                encoder.writeUInt64(totalOutcomeBytes)
                encoder.writeUInt32(Self.chunkByteCount)
                encoder.writeUInt32(chunkCount)
            }
        } catch {
            throw DatabaseMutationStateError.corruptedState
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: DatabaseMutationStateLimits
    ) throws -> Self {
        do {
            var decoder = try StorageFrameDecoder(bytes)
            guard try decoder.readUInt16() == formatVersion else {
                throw DatabaseMutationStateError.corruptedState
            }
            let discriminator = try decoder.readBytes()
            let requestFingerprint = try decoder.readBytes()
            let outcomeFingerprint = try decoder.readBytes()
            let totalOutcomeBytes = try decoder.readUInt64()
            let storedChunkByteCount = try decoder.readUInt32()
            guard storedChunkByteCount == chunkByteCount else {
                throw DatabaseMutationStateError.corruptedState
            }
            let manifest = Self(
                discriminator: discriminator,
                requestFingerprint: requestFingerprint,
                outcomeFingerprint: outcomeFingerprint,
                totalOutcomeBytes: totalOutcomeBytes,
                chunkCount: try decoder.readUInt32()
            )
            try decoder.ensureFullyRead()
            try manifest.validate(limits: limits)
            return manifest
        } catch {
            throw DatabaseMutationStateError.corruptedState
        }
    }

    func validate(limits: DatabaseMutationStateLimits) throws {
        guard !discriminator.isEmpty,
              discriminator.count <= limits.maximumDiscriminatorBytes else {
            throw DatabaseMutationStateError.invalidDiscriminator
        }
        guard !requestFingerprint.isEmpty,
              requestFingerprint.count <= limits.maximumFingerprintBytes,
              !outcomeFingerprint.isEmpty,
              outcomeFingerprint.count <= limits.maximumFingerprintBytes else {
            throw DatabaseMutationStateError.invalidFingerprint
        }
        guard totalOutcomeBytes <= UInt64(limits.maximumOutcomeBytes),
              UInt64(chunkCount) <= UInt64(limits.maximumChunkCount),
              chunkCount == Self.expectedChunkCount(
                  totalOutcomeBytes: totalOutcomeBytes
              ) else {
            throw DatabaseMutationStateError.corruptedState
        }
    }

    static func expectedChunkCount(totalOutcomeBytes: UInt64) -> UInt32? {
        guard totalOutcomeBytes > 0 else { return 0 }
        let count = ((totalOutcomeBytes - 1) / UInt64(chunkByteCount)) + 1
        return UInt32(exactly: count)
    }

    func expectedByteCount(forChunkAt index: UInt32) -> Int? {
        guard index < chunkCount else { return nil }
        let offset = UInt64(index) * UInt64(Self.chunkByteCount)
        guard offset < totalOutcomeBytes else { return nil }
        return Int(
            min(
                totalOutcomeBytes - offset,
                UInt64(Self.chunkByteCount)
            )
        )
    }
}
