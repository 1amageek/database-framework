/// Validated physical storage policy for database entities.
public struct ItemStorageConfiguration: Sendable, Equatable {
    public let encoding: ItemPayloadEncoding
    public let maximumPlainByteCount: Int
    public let maximumStoredByteCount: Int
    public let maximumInlineByteCount: Int
    public let chunkByteCount: Int

    /// Canonical v1 policy shared by every supported platform.
    public static let v1 = ItemStorageConfiguration(
        validatedEncoding: .identity,
        maximumPlainByteCount: 64 * 1_024 * 1_024,
        maximumStoredByteCount: 64 * 1_024 * 1_024,
        maximumInlineByteCount: 90_000,
        chunkByteCount: 90_000
    )

    public init(
        encoding: ItemPayloadEncoding,
        maximumPlainByteCount: Int,
        maximumStoredByteCount: Int,
        maximumInlineByteCount: Int,
        chunkByteCount: Int
    ) throws(ItemStorageConfigurationError) {
        guard maximumPlainByteCount > 0 else {
            throw .nonPositiveMaximumPlainByteCount
        }
        guard maximumStoredByteCount > 0 else {
            throw .nonPositiveMaximumStoredByteCount
        }
        guard maximumInlineByteCount > 0,
              maximumInlineByteCount <= maximumStoredByteCount else {
            throw .invalidMaximumInlineByteCount
        }
        guard chunkByteCount > 0,
              chunkByteCount <= maximumStoredByteCount,
              chunkByteCount <= Int(Int32.max) else {
            throw .invalidChunkByteCount
        }
        self.init(
            validatedEncoding: encoding,
            maximumPlainByteCount: maximumPlainByteCount,
            maximumStoredByteCount: maximumStoredByteCount,
            maximumInlineByteCount: maximumInlineByteCount,
            chunkByteCount: chunkByteCount
        )
    }

    private init(
        validatedEncoding: ItemPayloadEncoding,
        maximumPlainByteCount: Int,
        maximumStoredByteCount: Int,
        maximumInlineByteCount: Int,
        chunkByteCount: Int
    ) {
        self.encoding = validatedEncoding
        self.maximumPlainByteCount = maximumPlainByteCount
        self.maximumStoredByteCount = maximumStoredByteCount
        self.maximumInlineByteCount = maximumInlineByteCount
        self.chunkByteCount = chunkByteCount
    }
}

public enum ItemStorageConfigurationError: Error, Sendable, Equatable {
    case nonPositiveMaximumPlainByteCount
    case nonPositiveMaximumStoredByteCount
    case invalidMaximumInlineByteCount
    case invalidChunkByteCount
}
