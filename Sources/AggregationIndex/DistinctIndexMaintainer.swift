import Core
import DatabaseEngine
import StorageKit

private struct DistinctIndexSubspaces: Sendable {
    let members: Subspace
    let summaries: Subspace
    let membershipMetadata: Subspace

    init(base: Subspace) {
        self.members = base.subspace(Int64(0))
        self.summaries = base.subspace(Int64(1))
        self.membershipMetadata = base.subspace(Int64(2))
    }
}

private struct DistinctIndexGroup: Sendable {
    let memberSubspace: Subspace
    let summaryKey: Bytes
    let membershipMetadataKey: Bytes
}

private struct DistinctIndexContribution: Sendable {
    let memberKey: Bytes
    let value: FieldValue
    let group: DistinctIndexGroup
}

private struct DistinctIndexBatchGroup: Sendable {
    let group: DistinctIndexGroup
    var values: [FieldValue]
}

private struct DistinctIndexReadGroup {
    let grouping: [any TupleElement]
    let membershipMetadata: AggregationMembershipMetadata
    var summarySeen: Bool
}

/// Maintains an approximate distinct count over exact, delete-capable membership.
///
/// Physical layout:
/// - `[base][0][group...][canonical value] -> positive Int64 refcount`
/// - `[base][1][group...] -> bounded binary HyperLogLog summary`
/// - `[base][2][group...] -> fixed 16-byte (unique count, scan bytes) metadata`
///
/// Inserts update the summary incrementally. A transition that removes the final
/// reference to a value rebuilds the affected summary from exact membership in
/// the same transaction. TypeScript and storage adapters therefore never own
/// DISTINCT semantics.
public struct DistinctIndexMaintainer<Item: Persistable>:
    SubspaceIndexMaintainer,
    GroupingKeySupport {
    public static var supportedPersistedPrecision: ClosedRange<Int> { 4...17 }

    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let precision: Int
    private let layers: DistinctIndexSubspaces

    private var maximumGroupsPerQuery: Int { 100_000 }
    private var maximumMembersPerGroup: Int { 100_000 }
    private var maximumItemsPerBatch: Int { 100_000 }
    private var maximumScannedBytes: Int { 16 * 1_024 * 1_024 }

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        precision: Int
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.precision = precision
        self.layers = DistinctIndexSubspaces(base: subspace)
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any Transaction
    ) async throws {
        try validateConfiguration()

        // Validate and encode both sides before the first transaction mutation.
        let oldContribution = try oldItem.flatMap {
            try contribution(for: $0)
        }
        let newContribution = try newItem.flatMap {
            try contribution(for: $0)
        }

        if oldContribution?.memberKey == newContribution?.memberKey {
            return
        }

        var affectedGroups: [DistinctIndexGroup] = []
        if let oldContribution {
            appendDistinctGroup(oldContribution.group, to: &affectedGroups)
        }
        if let newContribution {
            appendDistinctGroup(newContribution.group, to: &affectedGroups)
        }
        var groupWasPresent: [Bytes: Bool] = [:]
        groupWasPresent.reserveCapacity(affectedGroups.count)
        for group in affectedGroups {
            groupWasPresent[group.summaryKey] = try await validateStoredGroup(
                group,
                transaction: transaction
            )
        }
        if let oldContribution {
            guard groupWasPresent[oldContribution.group.summaryKey] == true else {
                throw DistinctIndexError.corruptedMembership
            }
        }

        var rebuildGroups: [DistinctIndexGroup] = []
        var additions: [DistinctIndexContribution] = []

        if let oldContribution {
            let becameAbsent = try await decrementAggregationMembership(
                key: oldContribution.memberKey,
                metadataKey: oldContribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )
            if becameAbsent {
                appendDistinctGroup(oldContribution.group, to: &rebuildGroups)
            }
        }

        if let newContribution {
            let becamePresent = try await incrementAggregationMembership(
                key: newContribution.memberKey,
                metadataKey: newContribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )
            if becamePresent {
                additions.append(newContribution)
            }
        }

        for group in rebuildGroups {
            try await rebuildSummary(for: group, transaction: transaction)
        }

        for addition in additions where !rebuildGroups.contains(
            where: { $0.summaryKey == addition.group.summaryKey }
        ) {
            try await addToSummary(
                values: CollectionOfOne(addition.value),
                group: addition.group,
                initializeIfAbsent:
                    groupWasPresent[addition.group.summaryKey] == false,
                transaction: transaction
            )
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any Transaction
    ) async throws {
        try validateConfiguration()
        guard let contribution = try contribution(for: item) else {
            return
        }
        let groupWasPresent = try await validateStoredGroup(
            contribution.group,
            transaction: transaction
        )
        let becamePresent = try await incrementAggregationMembership(
            key: contribution.memberKey,
            metadataKey: contribution.group.membershipMetadataKey,
            maximumMembers: maximumMembersPerGroup,
            maximumScanBytes: maximumScannedBytes,
            transaction: transaction
        )
        if becamePresent {
            try await addToSummary(
                values: CollectionOfOne(contribution.value),
                group: contribution.group,
                initializeIfAbsent: !groupWasPresent,
                transaction: transaction
            )
        }
    }

    /// Builds a batch with one summary read/write per affected group.
    public func scanItems(
        _ items: [(item: Item, id: Tuple)],
        transaction: any Transaction
    ) async throws {
        try validateConfiguration()
        guard items.count <= maximumItemsPerBatch else {
            throw DistinctIndexError.batchItemLimitExceeded(
                maximumItemsPerBatch
            )
        }

        // Extraction is intentionally completed before mutation so malformed
        // values cannot leave a partially built transaction view.
        var contributions: [DistinctIndexContribution] = []
        contributions.reserveCapacity(items.count)
        var ingestedBytes = 0
        for entry in items {
            if let contribution = try contribution(for: entry.item) {
                ingestedBytes = try checkedDistinctScannedBytes(
                    ingestedBytes,
                    adding: contribution.memberKey.count
                        + MemoryLayout<Int64>.size,
                    maximum: maximumScannedBytes
                )
                contributions.append(contribution)
            }
        }

        var groups: [DistinctIndexBatchGroup] = []
        var groupIndices: [Bytes: Int] = [:]
        var groupWasPresent: [Bytes: Bool] = [:]
        groupIndices.reserveCapacity(
            Swift.min(contributions.count, maximumGroupsPerQuery)
        )
        groupWasPresent.reserveCapacity(
            Swift.min(contributions.count, maximumGroupsPerQuery)
        )
        for contribution in contributions
        where groupIndices[contribution.group.summaryKey] == nil {
            guard groupIndices.count < maximumGroupsPerQuery else {
                throw DistinctIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            groupIndices[contribution.group.summaryKey] = -1
            groupWasPresent[contribution.group.summaryKey] =
                try await validateStoredGroup(
                    contribution.group,
                    transaction: transaction
                )
        }

        for contribution in contributions {
            let becamePresent = try await incrementAggregationMembership(
                key: contribution.memberKey,
                metadataKey: contribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )
            guard becamePresent else {
                continue
            }

            if let index = groupIndices[contribution.group.summaryKey],
               index >= 0 {
                groups[index].values.append(contribution.value)
            } else {
                groupIndices[contribution.group.summaryKey] = groups.count
                groups.append(
                    DistinctIndexBatchGroup(
                        group: contribution.group,
                        values: [contribution.value]
                    )
                )
            }
        }

        for group in groups {
            try await addToSummary(
                values: group.values,
                group: group.group,
                initializeIfAbsent:
                    groupWasPresent[group.group.summaryKey] == false,
                transaction: transaction
            )
        }
    }

    /// Returns the exact membership key expected for index scrubbing.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [Bytes] {
        try validateConfiguration()
        guard let contribution = try contribution(for: item) else {
            return []
        }
        return [contribution.memberKey]
    }

    public func getDistinctCount(
        groupingValues: [any TupleElement],
        transaction: any Transaction
    ) async throws -> (estimated: Int64, errorRate: Double) {
        try validateConfiguration()
        let group = try makeGroup(groupingValues: groupingValues)
        return try await storedDistinctCount(
            for: group,
            transaction: transaction,
            snapshot: true
        ) ?? (estimated: 0, errorRate: 0)
    }

    private func storedDistinctCount(
        for group: DistinctIndexGroup,
        transaction: any Transaction,
        snapshot: Bool
    ) async throws -> (estimated: Int64, errorRate: Double)? {
        let membershipMetadata = try await storedMembershipMetadata(
            for: group,
            transaction: transaction,
            snapshot: snapshot
        )
        let bytes = try await transaction.getValue(
            for: group.summaryKey,
            snapshot: snapshot
        )
        guard membershipMetadata != nil else {
            guard bytes == nil else {
                throw DistinctIndexError.corruptedMembership
            }
            return nil
        }
        guard let bytes else {
            throw DistinctIndexError.corruptedSummary
        }
        let estimator = try DistinctSummaryCodec.decode(
            bytes,
            expectedPrecision: precision
        )
        return (
            estimated: try estimatedCardinality(estimator),
            errorRate: estimator.estimatedRelativeError
        )
    }

    public func getAllDistinctCounts(
        transaction: any Transaction
    ) async throws -> [(
        grouping: [any TupleElement],
        estimated: Int64,
        errorRate: Double
    )] {
        try validateConfiguration()
        let membershipMetadataRange = layers.membershipMetadata.range()
        let summaryRange = layers.summaries.range()
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        if expectedGroupingCount == 0 {
            let group = try makeGroup(
                groupingValues: [] as [any TupleElement]
            )
            guard let result = try await storedDistinctCount(
                for: group,
                transaction: transaction,
                snapshot: true
            ) else {
                return []
            }
            return [(
                grouping: [],
                estimated: result.estimated,
                errorRate: result.errorRate
            )]
        }
        var results: [(
            grouping: [any TupleElement],
            estimated: Int64,
            errorRate: Double
        )] = []
        var groups: [Bytes: DistinctIndexReadGroup] = [:]
        var scannedMemberCountGroups = 0
        var scannedSummaryGroups = 0
        var scannedBytes = 0

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(membershipMetadataRange.begin),
            to: .firstGreaterOrEqual(membershipMetadataRange.end),
            limit: maximumGroupsPerQuery + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedMemberCountGroups += 1
            guard scannedMemberCountGroups <= maximumGroupsPerQuery else {
                throw DistinctIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            scannedBytes = try checkedDistinctScannedBytes(
                scannedBytes,
                adding: key.count + value.count,
                maximum: maximumScannedBytes
            )

            var cursor = try layers.membershipMetadata.tupleCursor(for: key)
            var grouping: [any TupleElement] = []
            grouping.reserveCapacity(expectedGroupingCount)
            for _ in 0..<expectedGroupingCount {
                grouping.append(try cursor.requireNext())
            }
            guard cursor.isAtEnd else {
                throw DistinctIndexError.corruptedMembership
            }
            let membershipMetadata: AggregationMembershipMetadata
            do {
                membershipMetadata = try decodeAggregationMembershipMetadata(
                    value,
                    maximumMembers: maximumMembersPerGroup,
                    maximumScanBytes: maximumScannedBytes
                )
            } catch {
                throw DistinctIndexError.corruptedMembership
            }
            let identity = key[
                layers.membershipMetadata.prefix.count..<key.count
            ]
            guard groups.updateValue(
                DistinctIndexReadGroup(
                    grouping: grouping,
                    membershipMetadata: membershipMetadata,
                    summarySeen: false
                ),
                forKey: identity
            ) == nil else {
                throw DistinctIndexError.corruptedMembership
            }
        }

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(summaryRange.begin),
            to: .firstGreaterOrEqual(summaryRange.end),
            limit: maximumGroupsPerQuery + 1,
            snapshot: true,
            streamingMode: .iterator
        ) { key, value in
            scannedSummaryGroups += 1
            guard scannedSummaryGroups <= maximumGroupsPerQuery else {
                throw DistinctIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            scannedBytes = try checkedDistinctScannedBytes(
                scannedBytes,
                adding: key.count + value.count,
                maximum: maximumScannedBytes
            )

            let identity = key[layers.summaries.prefix.count..<key.count]
            guard var group = groups[identity], !group.summarySeen else {
                throw DistinctIndexError.corruptedSummary
            }

            let estimator = try DistinctSummaryCodec.decode(
                value,
                expectedPrecision: precision
            )
            results.append((
                grouping: group.grouping,
                estimated: try estimatedCardinality(estimator),
                errorRate: estimator.estimatedRelativeError
            ))
            group.summarySeen = true
            groups[identity] = group
        }
        guard results.count == groups.count,
              groups.values.allSatisfy(\.summarySeen) else {
            throw DistinctIndexError.corruptedSummary
        }
        return results
    }

    private func validateConfiguration() throws {
        guard Self.supportedPersistedPrecision.contains(precision) else {
            throw DistinctIndexError.invalidPrecision(precision)
        }
        guard index.rootExpression.columnCount >= 1 else {
            throw IndexError.invalidStructure(
                "Distinct index '\(index.name)' requires one value field"
            )
        }
    }

    private func contribution(
        for item: Item
    ) throws -> DistinctIndexContribution? {
        guard let fields = try AggregationFieldExtractor.contribution(
            from: item,
            index: index
        ) else {
            return nil
        }

        let value = try canonicalDistinctValue(
            FieldValue(tupleElement: fields.value),
            fieldName: index.name
        )
        guard value != .null else {
            return nil
        }
        let memberElement = try value.toTupleElement()
        let group = try makeGroup(groupingValues: fields.grouping)
        let memberKey = layers.members.pack(
            elements: fields.grouping,
            appending: memberElement
        )
        try validateKeySize(memberKey)
        return DistinctIndexContribution(
            memberKey: memberKey,
            value: value,
            group: group
        )
    }

    private func makeGroup<Grouping: Collection>(
        groupingValues: Grouping
    ) throws -> DistinctIndexGroup
    where Grouping.Element == any TupleElement {
        let expectedCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedCount else {
            throw IndexError.invalidArgument(
                "Grouping value count does not match distinct index '\(index.name)'"
            )
        }
        let memberPrefix = layers.members.pack(elements: groupingValues)
        let summaryKey = layers.summaries.pack(elements: groupingValues)
        let membershipMetadataKey = layers.membershipMetadata.pack(
            elements: groupingValues
        )
        try validateKeySize(memberPrefix)
        try validateKeySize(summaryKey)
        try validateKeySize(membershipMetadataKey)
        return DistinctIndexGroup(
            memberSubspace: Subspace(prefix: memberPrefix),
            summaryKey: summaryKey,
            membershipMetadataKey: membershipMetadataKey
        )
    }

    private func addToSummary<Values: Sequence>(
        values: Values,
        group: DistinctIndexGroup,
        initializeIfAbsent: Bool,
        transaction: any Transaction
    ) async throws where Values.Element == FieldValue {
        let stored = try await transaction.getValue(
            for: group.summaryKey
        )
        var estimator: HyperLogLog
        if let stored {
            estimator = try DistinctSummaryCodec.decode(
                stored,
                expectedPrecision: precision
            )
        } else {
            guard initializeIfAbsent else {
                throw DistinctIndexError.corruptedSummary
            }
            estimator = try HyperLogLog(precision: precision)
        }

        var valueCount = 0
        for value in values {
            try estimator.add(value)
            valueCount += 1
        }
        guard valueCount > 0 else {
            throw IndexError.invalidArgument(
                "DISTINCT summary update requires at least one value"
            )
        }
        try transaction.setValue(
            try DistinctSummaryCodec.encode(estimator),
            for: group.summaryKey
        )
    }

    /// Validates the summary/metadata pair before membership is mutated.
    /// A group is either fully absent or has both a valid metadata frame and a
    /// decodable summary. No mutation path repairs an orphaned component.
    private func validateStoredGroup(
        _ group: DistinctIndexGroup,
        transaction: any Transaction
    ) async throws -> Bool {
        let membershipMetadata = try await storedMembershipMetadata(
            for: group,
            transaction: transaction,
            snapshot: false
        )
        let summary = try await transaction.getValue(for: group.summaryKey)
        switch (membershipMetadata, summary) {
        case (nil, nil):
            return false
        case (nil, .some(_)):
            throw DistinctIndexError.corruptedMembership
        case (.some(_), nil):
            throw DistinctIndexError.corruptedSummary
        case (.some(_), let summary?):
            let estimator = try DistinctSummaryCodec.decode(
                summary,
                expectedPrecision: precision
            )
            _ = try estimatedCardinality(estimator)
            return true
        }
    }

    private func storedMembershipMetadata(
        for group: DistinctIndexGroup,
        transaction: any Transaction,
        snapshot: Bool
    ) async throws -> AggregationMembershipMetadata? {
        guard let bytes = try await transaction.getValue(
            for: group.membershipMetadataKey,
            snapshot: snapshot
        ) else {
            return nil
        }
        do {
            return try decodeAggregationMembershipMetadata(
                bytes,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes
            )
        } catch {
            throw DistinctIndexError.corruptedMembership
        }
    }

    private func estimatedCardinality(
        _ estimator: HyperLogLog
    ) throws -> Int64 {
        do {
            return try estimator.cardinality()
        } catch {
            throw DistinctIndexError.corruptedSummary
        }
    }

    private func rebuildSummary(
        for group: DistinctIndexGroup,
        transaction: any Transaction
    ) async throws {
        var estimator = try HyperLogLog(precision: precision)
        var memberCount = 0
        var scannedBytes = 0
        let range = group.memberSubspace.range()
        let expectedMetadata = try await storedMembershipMetadata(
            for: group,
            transaction: transaction,
            snapshot: false
        )

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maximumMembersPerGroup + 1,
            snapshot: false,
            streamingMode: .iterator
        ) { key, value in
            memberCount += 1
            guard memberCount <= maximumMembersPerGroup else {
                throw DistinctIndexError.memberLimitExceeded(
                    maximumMembersPerGroup
                )
            }
            scannedBytes = try checkedDistinctScannedBytes(
                scannedBytes,
                adding: key.count + value.count,
                maximum: maximumScannedBytes
            )
            _ = try decodeAggregationMembershipCount(value)

            var cursor = try group.memberSubspace.tupleCursor(for: key)
            let element = try cursor.requireNext()
            guard cursor.isAtEnd else {
                throw DistinctIndexError.corruptedMembership
            }
            let fieldValue = try FieldValue(tupleElement: element)
            try estimator.add(fieldValue)
        }

        guard Int64(memberCount)
                == (expectedMetadata?.uniqueMemberCount ?? 0) else {
            throw AggregationStorageError.membershipCountMismatch(
                expected: expectedMetadata?.uniqueMemberCount ?? 0,
                actual: Int64(memberCount)
            )
        }
        guard Int64(scannedBytes)
                == (expectedMetadata?.memberScanBytes ?? 0) else {
            throw AggregationStorageError.membershipScanByteMismatch(
                expected: expectedMetadata?.memberScanBytes ?? 0,
                actual: Int64(scannedBytes)
            )
        }
        if memberCount == 0 {
            try transaction.clear(key: group.summaryKey)
        } else {
            try transaction.setValue(
                try DistinctSummaryCodec.encode(estimator),
                for: group.summaryKey
            )
        }
    }
}

public enum DistinctIndexError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case invalidGroupingValue(fieldName: String)
    case invalidDistinctValue(fieldName: String)
    case invalidPrecision(Int)
    case corruptedMembership
    case corruptedSummary
    case precisionMismatch(expected: Int, actual: Int)
    case memberLimitExceeded(Int)
    case groupLimitExceeded(Int)
    case batchItemLimitExceeded(Int)
    case byteLimitExceeded(Int)

    public var description: String {
        switch self {
        case .invalidGroupingValue(let fieldName):
            return "Invalid grouping value for field: \(fieldName)"
        case .invalidDistinctValue(let fieldName):
            return "Invalid distinct value for field: \(fieldName)"
        case .invalidPrecision(let precision):
            return "Unsupported persisted HyperLogLog precision: \(precision)"
        case .corruptedMembership:
            return "Corrupted DISTINCT membership"
        case .corruptedSummary:
            return "Corrupted DISTINCT summary"
        case .precisionMismatch(let expected, let actual):
            return "DISTINCT precision mismatch: expected \(expected), got \(actual)"
        case .memberLimitExceeded(let maximum):
            return "DISTINCT member scan exceeded \(maximum) entries"
        case .groupLimitExceeded(let maximum):
            return "DISTINCT group scan exceeded \(maximum) entries"
        case .batchItemLimitExceeded(let maximum):
            return "DISTINCT batch exceeded \(maximum) items"
        case .byteLimitExceeded(let maximum):
            return "DISTINCT scan exceeded \(maximum) bytes"
        }
    }
}

private enum DistinctSummaryCodec {
    private static let headerByteCount = 5
    private static let registerBitCount = 6

    static func encode(_ estimator: HyperLogLog) throws -> Bytes {
        guard DistinctIndexMaintainerCodecLimits.persistedPrecision.contains(
            estimator.precision
        ) else {
            throw DistinctIndexError.invalidPrecision(estimator.precision)
        }
        let registerCount = estimator.memorySizeInBytes
        let payloadByteCount = (registerCount * registerBitCount + 7) / 8
        let totalByteCount = headerByteCount + payloadByteCount

        return Bytes.copying(count: totalByteCount) { destination in
            destination[0] = 0x44
            destination[1] = 0x48
            destination[2] = 0x4C
            destination[3] = 0x01
            destination[4] = UInt8(estimator.precision)

            estimator.withUnsafeRegisters { registers in
                var accumulator: UInt16 = 0
                var accumulatedBits = 0
                var offset = headerByteCount
                for register in registers {
                    accumulator |= UInt16(register) << accumulatedBits
                    accumulatedBits += registerBitCount
                    while accumulatedBits >= 8 {
                        destination[offset] = UInt8(truncatingIfNeeded: accumulator)
                        offset += 1
                        accumulator >>= 8
                        accumulatedBits -= 8
                    }
                }
                if accumulatedBits > 0 {
                    destination[offset] = UInt8(truncatingIfNeeded: accumulator)
                    offset += 1
                }
                precondition(offset == totalByteCount)
            }
        }
    }

    static func decode(
        _ bytes: Bytes,
        expectedPrecision: Int
    ) throws -> HyperLogLog {
        guard bytes.count >= headerByteCount,
              bytes[0] == 0x44,
              bytes[1] == 0x48,
              bytes[2] == 0x4C,
              bytes[3] == 0x01 else {
            throw DistinctIndexError.corruptedSummary
        }
        let precision = Int(bytes[4])
        guard precision == expectedPrecision else {
            throw DistinctIndexError.precisionMismatch(
                expected: expectedPrecision,
                actual: precision
            )
        }
        guard DistinctIndexMaintainerCodecLimits.persistedPrecision.contains(
            precision
        ) else {
            throw DistinctIndexError.invalidPrecision(precision)
        }

        let registerCount = 1 << precision
        let payloadByteCount = (registerCount * registerBitCount + 7) / 8
        guard bytes.count == headerByteCount + payloadByteCount else {
            throw DistinctIndexError.corruptedSummary
        }

        var decodeError: DistinctIndexError?
        let registers = [UInt8](unsafeUninitializedCapacity: registerCount) {
            output, initializedCount in
            var accumulator: UInt16 = 0
            var accumulatedBits = 0
            var offset = headerByteCount
            for index in 0..<registerCount {
                while accumulatedBits < registerBitCount {
                    accumulator |= UInt16(bytes[offset]) << accumulatedBits
                    offset += 1
                    accumulatedBits += 8
                }
                let register = UInt8(accumulator & 0x3F)
                let maximum = UInt8(65 - precision)
                guard register <= maximum else {
                    decodeError = .corruptedSummary
                    initializedCount = index
                    return
                }
                output[index] = register
                initializedCount = index + 1
                accumulator >>= registerBitCount
                accumulatedBits -= registerBitCount
            }
            if accumulator != 0 || offset != bytes.count {
                decodeError = .corruptedSummary
            }
        }
        if let decodeError {
            throw decodeError
        }
        do {
            return try HyperLogLog(
                precision: precision,
                registers: registers
            )
        } catch {
            throw DistinctIndexError.corruptedSummary
        }
    }
}

private enum DistinctIndexMaintainerCodecLimits {
    static let persistedPrecision = 4...17
}

private func appendDistinctGroup(
    _ group: DistinctIndexGroup,
    to groups: inout [DistinctIndexGroup]
) {
    guard !groups.contains(where: { $0.summaryKey == group.summaryKey }) else {
        return
    }
    groups.append(group)
}

private func canonicalDistinctValue(
    _ value: FieldValue,
    fieldName: String
) throws -> FieldValue {
    try canonicalDistinctValueResult(
        value,
        fieldName: fieldName
    ).value
}

private struct CanonicalDistinctValueResult {
    let value: FieldValue
    let changedRepresentation: Bool
}

/// Canonicalizes only representations that compare as the same numeric value.
/// Arrays retain their original copy-on-write storage unless a nested value
/// actually changes representation.
private func canonicalDistinctValueResult(
    _ value: FieldValue,
    fieldName: String
) throws -> CanonicalDistinctValueResult {
    switch value {
    case .int64(let integer):
        return CanonicalDistinctValueResult(
            value: .int64(integer),
            changedRepresentation: false
        )
    case .uint64(let integer):
        if integer <= UInt64(Int64.max) {
            return CanonicalDistinctValueResult(
                value: .int64(Int64(integer)),
                changedRepresentation: true
            )
        }
        return CanonicalDistinctValueResult(
            value: .uint64(integer),
            changedRepresentation: false
        )
    case .double(let number):
        guard number.isFinite else {
            throw DistinctIndexError.invalidDistinctValue(
                fieldName: fieldName
            )
        }
        if let integer = Int64(exactly: number) {
            return CanonicalDistinctValueResult(
                value: .int64(integer),
                changedRepresentation: true
            )
        }
        if let integer = UInt64(exactly: number) {
            let canonical: FieldValue = integer <= UInt64(Int64.max)
                ? .int64(Int64(integer))
                : .uint64(integer)
            return CanonicalDistinctValueResult(
                value: canonical,
                changedRepresentation: true
            )
        }
        let normalized = number == 0 ? 0 : number
        return CanonicalDistinctValueResult(
            value: .double(normalized),
            changedRepresentation: normalized.bitPattern != number.bitPattern
        )
    case .array(let values):
        var canonical: [FieldValue]?
        for index in values.indices {
            let result = try canonicalDistinctValueResult(
                values[index],
                fieldName: fieldName
            )
            if result.changedRepresentation {
                if canonical == nil {
                    canonical = values
                }
                canonical?[index] = result.value
            }
        }
        guard let canonical else {
            return CanonicalDistinctValueResult(
                value: value,
                changedRepresentation: false
            )
        }
        return CanonicalDistinctValueResult(
            value: .array(canonical),
            changedRepresentation: true
        )
    case .null, .bool, .string, .data, .rdfTerm:
        return CanonicalDistinctValueResult(
            value: value,
            changedRepresentation: false
        )
    }
}

private func checkedDistinctScannedBytes(
    _ current: Int,
    adding additional: Int,
    maximum: Int
) throws -> Int {
    let (updated, overflow) = current.addingReportingOverflow(additional)
    guard !overflow, updated <= maximum else {
        throw DistinctIndexError.byteLimitExceeded(maximum)
    }
    return updated
}
