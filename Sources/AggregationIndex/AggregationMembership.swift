import DatabaseEngine
import StorageKit

struct AggregationMembershipMetadata: Sendable, Equatable {
    static let encodedByteCount = 16

    let uniqueMemberCount: Int64
    let memberScanBytes: Int64
}

/// Increments an exact aggregate membership reference count and its group
/// metadata as one invariant-preserving mutation.
///
/// The Boolean result is true only when the logical member transitions from
/// absent to present. The fixed metadata frame stores both the number of unique
/// member keys and the exact bytes a future rebuild must scan. All reads and
/// limit checks complete before the first write to the transaction view.
func incrementAggregationMembership(
    key: Bytes,
    metadataKey: Bytes,
    maximumMembers: Int,
    maximumScanBytes: Int,
    transaction: any TransactionAccess
) async throws -> Bool {
    let storedMember = try await transaction.getValue(for: key)
    let storedMetadata = try await transaction.getValue(for: metadataKey)

    if let storedMember {
        let current = try decodeAggregationMembershipCount(storedMember)
        guard let storedMetadata else {
            throw AggregationStorageError.missingMembershipMetadata
        }
        _ = try decodeAggregationMembershipMetadata(
            storedMetadata,
            maximumMembers: maximumMembers,
            maximumScanBytes: maximumScanBytes
        )

        let (updated, overflow) = current.addingReportingOverflow(1)
        guard !overflow else {
            throw AggregationStorageError.integerOverflow
        }
        try transaction.setValue(
            ByteConversion.int64ToBytes(updated),
            for: key
        )
        return false
    }

    let currentMetadata: AggregationMembershipMetadata
    if let storedMetadata {
        currentMetadata = try decodeAggregationMembershipMetadata(
            storedMetadata,
            maximumMembers: maximumMembers,
            maximumScanBytes: maximumScanBytes
        )
    } else {
        currentMetadata = AggregationMembershipMetadata(
            uniqueMemberCount: 0,
            memberScanBytes: 0
        )
    }

    let memberScanBytes = try aggregationMemberScanByteCount(for: key)
    let (updatedCount, countOverflow) = currentMetadata.uniqueMemberCount
        .addingReportingOverflow(1)
    let (updatedBytes, byteOverflow) = currentMetadata.memberScanBytes
        .addingReportingOverflow(memberScanBytes)
    guard !countOverflow, !byteOverflow else {
        throw AggregationStorageError.integerOverflow
    }
    guard updatedCount <= Int64(maximumMembers) else {
        throw AggregationStorageError.membershipLimitExceeded(maximumMembers)
    }
    guard updatedBytes <= Int64(maximumScanBytes) else {
        throw AggregationStorageError.membershipByteLimitExceeded(
            maximumScanBytes
        )
    }

    let updatedMetadata = AggregationMembershipMetadata(
        uniqueMemberCount: updatedCount,
        memberScanBytes: updatedBytes
    )
    try transaction.setValue(ByteConversion.int64ToBytes(1), for: key)
    try transaction.setValue(
        try encodeAggregationMembershipMetadata(updatedMetadata),
        for: metadataKey
    )
    return true
}

/// Decrements an exact aggregate membership reference count and its group
/// metadata as one invariant-preserving mutation.
///
/// The Boolean result is true only when the logical member transitions from
/// present to absent. A missing member or metadata frame is corruption, not an
/// idempotent success. All validation completes before the first write.
func decrementAggregationMembership(
    key: Bytes,
    metadataKey: Bytes,
    maximumMembers: Int,
    maximumScanBytes: Int,
    transaction: any TransactionAccess
) async throws -> Bool {
    let storedMember = try await transaction.getValue(for: key)
    let storedMetadata = try await transaction.getValue(for: metadataKey)
    guard let storedMember else {
        throw AggregationStorageError.negativeCount(-1)
    }
    guard let storedMetadata else {
        throw AggregationStorageError.missingMembershipMetadata
    }

    let current = try decodeAggregationMembershipCount(storedMember)
    let metadata = try decodeAggregationMembershipMetadata(
        storedMetadata,
        maximumMembers: maximumMembers,
        maximumScanBytes: maximumScanBytes
    )
    if current == 1 {
        let memberScanBytes = try aggregationMemberScanByteCount(for: key)
        let updatedCount = metadata.uniqueMemberCount - 1
        let updatedBytes = metadata.memberScanBytes - memberScanBytes
        guard updatedCount >= 0, updatedBytes >= 0 else {
            throw AggregationStorageError.membershipMetadataUnderflow
        }
        guard (updatedCount == 0) == (updatedBytes == 0) else {
            throw AggregationStorageError.membershipMetadataMismatch(
                expectedScanBytes: metadata.memberScanBytes,
                actualScanBytes: memberScanBytes
            )
        }

        try transaction.clear(key: key)
        if updatedCount == 0 {
            try transaction.clear(key: metadataKey)
        } else {
            try transaction.setValue(
                try encodeAggregationMembershipMetadata(
                    AggregationMembershipMetadata(
                        uniqueMemberCount: updatedCount,
                        memberScanBytes: updatedBytes
                    )
                ),
                for: metadataKey
            )
        }
        return true
    }

    try transaction.setValue(
        ByteConversion.int64ToBytes(current - 1),
        for: key
    )
    return false
}

func decodeAggregationMembershipCount(_ bytes: Bytes) throws -> Int64 {
    let count = try ByteConversion.bytesToInt64(bytes)
    guard count > 0 else {
        throw AggregationStorageError.nonPositiveStoredCount(count)
    }
    return count
}

func encodeAggregationMembershipMetadata(
    _ metadata: AggregationMembershipMetadata
) throws -> Bytes {
    try validateAggregationMembershipMetadataShape(metadata)

    return Bytes.copying(
        count: AggregationMembershipMetadata.encodedByteCount
    ) { destination in
        guard let baseAddress = destination.baseAddress else {
            preconditionFailure("Membership metadata requires storage")
        }
        baseAddress.storeBytes(
            of: metadata.uniqueMemberCount.littleEndian,
            toByteOffset: 0,
            as: Int64.self
        )
        baseAddress.storeBytes(
            of: metadata.memberScanBytes.littleEndian,
            toByteOffset: MemoryLayout<Int64>.size,
            as: Int64.self
        )
    }
}

func decodeAggregationMembershipMetadata(
    _ bytes: Bytes,
    maximumMembers: Int,
    maximumScanBytes: Int
) throws -> AggregationMembershipMetadata {
    guard bytes.count == AggregationMembershipMetadata.encodedByteCount else {
        throw AggregationStorageError.invalidMembershipMetadataByteCount(
            bytes.count
        )
    }
    let metadata = bytes.withUnsafeBytes { buffer in
        AggregationMembershipMetadata(
            uniqueMemberCount: Int64(
                littleEndian: buffer.loadUnaligned(
                    fromByteOffset: 0,
                    as: Int64.self
                )
            ),
            memberScanBytes: Int64(
                littleEndian: buffer.loadUnaligned(
                    fromByteOffset: MemoryLayout<Int64>.size,
                    as: Int64.self
                )
            )
        )
    }
    guard metadata.uniqueMemberCount > 0,
          metadata.memberScanBytes > 0 else {
        throw AggregationStorageError.nonPositiveMembershipMetadata
    }
    guard metadata.uniqueMemberCount <= Int64(maximumMembers) else {
        throw AggregationStorageError.membershipLimitExceeded(maximumMembers)
    }
    guard metadata.memberScanBytes <= Int64(maximumScanBytes) else {
        throw AggregationStorageError.membershipByteLimitExceeded(
            maximumScanBytes
        )
    }
    try validateAggregationMembershipMetadataShape(metadata)
    return metadata
}

private func validateAggregationMembershipMetadataShape(
    _ metadata: AggregationMembershipMetadata
) throws {
    guard metadata.uniqueMemberCount > 0,
          metadata.memberScanBytes > 0 else {
        throw AggregationStorageError.nonPositiveMembershipMetadata
    }
    let (minimumScanBytes, overflow) = metadata.uniqueMemberCount
        .multipliedReportingOverflow(by: Int64(MemoryLayout<Int64>.size))
    guard !overflow else {
        throw AggregationStorageError.integerOverflow
    }
    guard metadata.memberScanBytes >= minimumScanBytes else {
        throw AggregationStorageError.membershipMetadataTooSmall(
            minimumScanBytes: minimumScanBytes,
            actualScanBytes: metadata.memberScanBytes
        )
    }
}

func aggregationMemberScanByteCount(for memberKey: Bytes) throws -> Int64 {
    let (byteCount, overflow) = memberKey.count.addingReportingOverflow(
        MemoryLayout<Int64>.size
    )
    guard !overflow, let result = Int64(exactly: byteCount) else {
        throw AggregationStorageError.integerOverflow
    }
    return result
}
