import DatabaseKit
import DatabaseEngine
import StorageKit

private struct PercentileIndexSubspaces: Sendable {
    let members: Subspace
    let summaries: Subspace
    let membershipMetadata: Subspace

    init(base: Subspace) {
        self.members = base.subspace(Int64(0))
        self.summaries = base.subspace(Int64(1))
        self.membershipMetadata = base.subspace(Int64(2))
    }
}

private struct PercentileIndexGroup: Sendable {
    let memberSubspace: Subspace
    let summaryKey: Bytes
    let membershipMetadataKey: Bytes
}

private struct PercentileIndexContribution: Sendable {
    let memberKey: Bytes
    let value: Double
    let group: PercentileIndexGroup
}

private struct PercentileIndexBatchGroup: Sendable {
    let group: PercentileIndexGroup
    var values: [Double]
}

private struct PercentileIndexReadGroup {
    let grouping: [any TupleElement]
    let membershipMetadata: AggregationMembershipMetadata
    var summarySeen: Bool
}

/// Maintains a t-digest over exact, delete-capable numeric membership.
///
/// Physical layout:
/// - `[base][0][group...][canonical Double] -> positive Int64 refcount`
/// - `[base][1][group...] -> strict bounded TDigest binary frame`
/// - `[base][2][group...] -> fixed 16-byte (unique count, scan bytes) metadata`
///
/// Inserts are applied incrementally. Deletes and updates rebuild every affected
/// summary from membership in the same transaction because a t-digest cannot
/// subtract weight without losing its error guarantees.
public struct PercentileIndexMaintainer<Item: Persistable>:
    SubspaceIndexMaintainer,
    GroupingKeySupport {
    public let index: Index
    public let subspace: Subspace
    public let idExpression: KeyExpression

    private let compression: Double
    private let layers: PercentileIndexSubspaces

    private var maximumGroupsPerQuery: Int { 100_000 }
    private var maximumMembersPerGroup: Int { 100_000 }
    private var maximumItemsPerBatch: Int { 100_000 }
    private var maximumPercentilesPerQuery: Int { 128 }
    private var maximumScannedBytes: Int { 16 * 1_024 * 1_024 }

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        compression: Double
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
        self.compression = compression
        self.layers = PercentileIndexSubspaces(base: subspace)
    }

    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        try validateConfiguration()

        // Both sides are validated before mutation. Invalid new values therefore
        // cannot remove a valid old contribution from the transaction view.
        let oldContribution = try oldItem.flatMap {
            try contribution(for: $0)
        }
        let newContribution = try newItem.flatMap {
            try contribution(for: $0)
        }
        if oldContribution?.memberKey == newContribution?.memberKey {
            return
        }

        var affectedGroups: [PercentileIndexGroup] = []
        if let oldContribution {
            appendPercentileGroup(oldContribution.group, to: &affectedGroups)
        }
        if let newContribution {
            appendPercentileGroup(newContribution.group, to: &affectedGroups)
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
                throw PercentileIndexError.corruptedMembership
            }
        }

        var rebuildGroups: [PercentileIndexGroup] = []
        if let oldContribution {
            _ = try await decrementAggregationMembership(
                key: oldContribution.memberKey,
                metadataKey: oldContribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )
            appendPercentileGroup(oldContribution.group, to: &rebuildGroups)
        }
        if let newContribution {
            _ = try await incrementAggregationMembership(
                key: newContribution.memberKey,
                metadataKey: newContribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )
        }

        for group in rebuildGroups {
            try await rebuildSummary(for: group, transaction: transaction)
        }
        if let newContribution,
           !rebuildGroups.contains(where: {
               $0.summaryKey == newContribution.group.summaryKey
           }) {
            try await addToSummary(
                values: CollectionOfOne(newContribution.value),
                group: newContribution.group,
                initializeIfAbsent:
                    groupWasPresent[newContribution.group.summaryKey] == false,
                transaction: transaction
            )
        }
    }

    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        try validateConfiguration()
        guard let contribution = try contribution(for: item) else {
            return
        }
        let groupWasPresent = try await validateStoredGroup(
            contribution.group,
            transaction: transaction
        )
        _ = try await incrementAggregationMembership(
            key: contribution.memberKey,
            metadataKey: contribution.group.membershipMetadataKey,
            maximumMembers: maximumMembersPerGroup,
            maximumScanBytes: maximumScannedBytes,
            transaction: transaction
        )
        try await addToSummary(
            values: CollectionOfOne(contribution.value),
            group: contribution.group,
            initializeIfAbsent: !groupWasPresent,
            transaction: transaction
        )
    }

    /// Builds a batch with one digest decode/encode per affected group.
    public func scanItems(
        _ items: [(item: Item, id: Tuple)],
        transaction: any TransactionAccess
    ) async throws {
        try validateConfiguration()
        guard items.count <= maximumItemsPerBatch else {
            throw PercentileIndexError.batchItemLimitExceeded(
                maximumItemsPerBatch
            )
        }

        // Extraction completes before the first mutation. Invalid values cannot
        // leave a partially constructed transaction view.
        var contributions: [PercentileIndexContribution] = []
        contributions.reserveCapacity(items.count)
        var ingestedBytes = 0
        for entry in items {
            if let contribution = try contribution(for: entry.item) {
                ingestedBytes = try checkedPercentileScannedBytes(
                    ingestedBytes,
                    adding: contribution.memberKey.count
                        + MemoryLayout<Int64>.size,
                    maximum: maximumScannedBytes
                )
                contributions.append(contribution)
            }
        }

        var groups: [PercentileIndexBatchGroup] = []
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
                throw PercentileIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            groupIndices[contribution.group.summaryKey] = groups.count
            groups.append(
                PercentileIndexBatchGroup(
                    group: contribution.group,
                    values: []
                )
            )
            groupWasPresent[contribution.group.summaryKey] =
                try await validateStoredGroup(
                    contribution.group,
                    transaction: transaction
                )
        }

        for contribution in contributions {
            _ = try await incrementAggregationMembership(
                key: contribution.memberKey,
                metadataKey: contribution.group.membershipMetadataKey,
                maximumMembers: maximumMembersPerGroup,
                maximumScanBytes: maximumScannedBytes,
                transaction: transaction
            )

            guard let index = groupIndices[contribution.group.summaryKey] else {
                throw PercentileIndexError.corruptedMembership
            }
            groups[index].values.append(contribution.value)
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

    public func getPercentile(
        percentile: Double,
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> Double? {
        try validatePercentiles(CollectionOfOne(percentile))
        guard var digest = try await digest(
            groupingValues: groupingValues,
            transaction: transaction
        ) else {
            return nil
        }
        return try digest.quantile(percentile)
    }

    public func getPercentiles(
        percentiles: [Double],
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> [Double: Double] {
        try validatePercentiles(percentiles)
        guard var digest = try await digest(
            groupingValues: groupingValues,
            transaction: transaction
        ) else {
            return [:]
        }
        return try digest.quantiles(percentiles)
    }

    public func getCDF(
        value: Double,
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> Double? {
        try validateConfiguration()
        guard value.isFinite else {
            throw PercentileIndexError.invalidNumericValue(
                fieldName: index.name
            )
        }
        guard var digest = try await digest(
            groupingValues: groupingValues,
            transaction: transaction
        ) else {
            return nil
        }
        return try digest.cdf(value)
    }

    public func getStatistics(
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> (
        count: Int64,
        min: Double,
        max: Double,
        median: Double
    )? {
        try validateConfiguration()
        guard var digest = try await digest(
            groupingValues: groupingValues,
            transaction: transaction
        ) else {
            return nil
        }
        return (
            count: digest.count,
            min: digest.min,
            max: digest.max,
            median: try digest.quantile(0.5)
        )
    }

    public func getAllPercentiles(
        percentiles: [Double],
        transaction: any TransactionAccess
    ) async throws -> [(
        grouping: [any TupleElement],
        values: [Double: Double]
    )] {
        try validatePercentiles(percentiles)
        let membershipMetadataRange = layers.membershipMetadata.range()
        let summaryRange = layers.summaries.range()
        let expectedGroupingCount = index.rootExpression.columnCount - 1
        if expectedGroupingCount == 0 {
            guard var digest = try await digest(
                groupingValues: [],
                transaction: transaction
            ) else {
                return []
            }
            return [(
                grouping: [],
                values: try digest.quantiles(percentiles)
            )]
        }
        var results: [(
            grouping: [any TupleElement],
            values: [Double: Double]
        )] = []
        var groups: [Bytes: PercentileIndexReadGroup] = [:]
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
                throw PercentileIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            scannedBytes = try checkedPercentileScannedBytes(
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
                throw PercentileIndexError.corruptedMembership
            }

            let membershipMetadata: AggregationMembershipMetadata
            do {
                membershipMetadata = try decodeAggregationMembershipMetadata(
                    value,
                    maximumMembers: maximumMembersPerGroup,
                    maximumScanBytes: maximumScannedBytes
                )
            } catch {
                throw PercentileIndexError.corruptedMembership
            }
            let identity = key[
                layers.membershipMetadata.prefix.count..<key.count
            ]
            guard groups.updateValue(
                PercentileIndexReadGroup(
                    grouping: grouping,
                    membershipMetadata: membershipMetadata,
                    summarySeen: false
                ),
                forKey: identity
            ) == nil else {
                throw PercentileIndexError.corruptedMembership
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
                throw PercentileIndexError.groupLimitExceeded(
                    maximumGroupsPerQuery
                )
            }
            scannedBytes = try checkedPercentileScannedBytes(
                scannedBytes,
                adding: key.count + value.count,
                maximum: maximumScannedBytes
            )

            let identity = key[layers.summaries.prefix.count..<key.count]
            guard var group = groups[identity], !group.summarySeen else {
                throw PercentileIndexError.corruptedSummary
            }

            var digest = try decodeDigest(value)
            guard digest.count >= group.membershipMetadata.uniqueMemberCount else {
                throw PercentileIndexError.corruptedSummary
            }
            results.append((
                grouping: group.grouping,
                values: try digest.quantiles(percentiles)
            ))
            group.summarySeen = true
            groups[identity] = group
        }
        guard results.count == groups.count,
              groups.values.allSatisfy(\.summarySeen) else {
            throw PercentileIndexError.corruptedSummary
        }
        return results
    }

    private func validateConfiguration() throws {
        guard compression.isFinite,
              TDigest.supportedCompression.contains(compression) else {
            throw PercentileIndexError.invalidCompression(compression)
        }
        guard index.rootExpression.columnCount >= 1 else {
            throw AggregationIndexError.invalidStructure(
                "Percentile index '\(index.name)' requires one value field"
            )
        }
    }

    private func validatePercentiles<Percentiles: Collection>(
        _ percentiles: Percentiles
    ) throws where Percentiles.Element == Double {
        try validateConfiguration()
        guard percentiles.count <= maximumPercentilesPerQuery else {
            throw PercentileIndexError.percentileLimitExceeded(
                maximumPercentilesPerQuery
            )
        }
        for percentile in percentiles {
            guard percentile.isFinite,
                  (0.0...1.0).contains(percentile) else {
                throw PercentileIndexError.invalidPercentile(percentile)
            }
        }
    }

    private func contribution(
        for item: Item
    ) throws -> PercentileIndexContribution? {
        guard let fields = try AggregationFieldExtractor.contribution(
            from: item,
            index: index
        ) else {
            return nil
        }

        let decoded: Double
        do {
            decoded = try TypeConversion.double(from: fields.value)
        } catch {
            throw PercentileIndexError.invalidNumericValue(
                fieldName: index.name
            )
        }
        guard decoded.isFinite else {
            throw PercentileIndexError.invalidNumericValue(
                fieldName: index.name
            )
        }
        let value = decoded == 0 ? 0 : decoded
        let group = try makeGroup(groupingValues: fields.grouping)
        let memberKey = layers.members.pack(
            elements: fields.grouping,
            appending: value
        )
        try validateKeySize(memberKey)
        return PercentileIndexContribution(
            memberKey: memberKey,
            value: value,
            group: group
        )
    }

    private func makeGroup<Grouping: Collection>(
        groupingValues: Grouping
    ) throws -> PercentileIndexGroup
    where Grouping.Element == any TupleElement {
        let expectedCount = index.rootExpression.columnCount - 1
        guard groupingValues.count == expectedCount else {
            throw AggregationIndexError.invalidArgument(
                "Grouping value count does not match percentile index '\(index.name)'"
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
        return PercentileIndexGroup(
            memberSubspace: Subspace(prefix: memberPrefix),
            summaryKey: summaryKey,
            membershipMetadataKey: membershipMetadataKey
        )
    }

    private func digest(
        groupingValues: [any TupleElement],
        transaction: any TransactionAccess
    ) async throws -> TDigest? {
        let group = try makeGroup(groupingValues: groupingValues)
        let membershipMetadata = try await storedMembershipMetadata(
            for: group,
            transaction: transaction,
            snapshot: true
        )
        let bytes = try await transaction.getValue(
            for: group.summaryKey,
            snapshot: true
        )
        guard let membershipMetadata else {
            guard bytes == nil else {
                throw PercentileIndexError.corruptedMembership
            }
            return nil
        }
        guard let bytes else {
            throw PercentileIndexError.corruptedSummary
        }
        let digest = try decodeDigest(bytes)
        guard digest.count >= membershipMetadata.uniqueMemberCount else {
            throw PercentileIndexError.corruptedSummary
        }
        return digest
    }

    private func storedMembershipMetadata(
        for group: PercentileIndexGroup,
        transaction: any TransactionAccess,
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
            throw PercentileIndexError.corruptedMembership
        }
    }

    private func decodeDigest(_ bytes: Bytes) throws -> TDigest {
        let digest: TDigest
        do {
            digest = try TDigest.decode(from: bytes)
        } catch {
            throw PercentileIndexError.corruptedSummary
        }
        guard digest.compression == compression, !digest.isEmpty else {
            throw PercentileIndexError.corruptedSummary
        }
        return digest
    }

    private func addToSummary<Values: Sequence>(
        values: Values,
        group: PercentileIndexGroup,
        initializeIfAbsent: Bool,
        transaction: any TransactionAccess
    ) async throws where Values.Element == Double {
        let stored = try await transaction.getValue(
            for: group.summaryKey
        )
        var digest: TDigest
        if let stored {
            digest = try decodeDigest(stored)
        } else {
            guard initializeIfAbsent else {
                throw PercentileIndexError.corruptedSummary
            }
            digest = try TDigest(compression: compression)
        }

        var valueCount = 0
        for value in values {
            try digest.add(value)
            valueCount += 1
        }
        guard valueCount > 0 else {
            throw AggregationIndexError.invalidArgument(
                "PERCENTILE summary update requires at least one value"
            )
        }
        try transaction.setValue(
            try digest.encodeBytes(),
            for: group.summaryKey
        )
    }

    /// Validates the summary/metadata pair before membership is mutated.
    /// A group is either fully absent or has both a valid metadata frame and a
    /// decodable summary. No mutation path repairs an orphaned component.
    private func validateStoredGroup(
        _ group: PercentileIndexGroup,
        transaction: any TransactionAccess
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
            throw PercentileIndexError.corruptedMembership
        case (.some(_), nil):
            throw PercentileIndexError.corruptedSummary
        case (let membershipMetadata?, let summary?):
            let digest = try decodeDigest(summary)
            guard digest.count >= membershipMetadata.uniqueMemberCount else {
                throw PercentileIndexError.corruptedSummary
            }
            return true
        }
    }

    private func rebuildSummary(
        for group: PercentileIndexGroup,
        transaction: any TransactionAccess
    ) async throws {
        var digest = try TDigest(compression: compression)
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
                throw PercentileIndexError.memberLimitExceeded(
                    maximumMembersPerGroup
                )
            }
            scannedBytes = try checkedPercentileScannedBytes(
                scannedBytes,
                adding: key.count + value.count,
                maximum: maximumScannedBytes
            )
            let weight = try decodeAggregationMembershipCount(value)

            var cursor = try group.memberSubspace.tupleCursor(for: key)
            let element = try cursor.requireNext()
            guard cursor.isAtEnd else {
                throw PercentileIndexError.corruptedMembership
            }
            let numericValue: Double
            do {
                numericValue = try TypeConversion.double(from: element)
            } catch {
                throw PercentileIndexError.corruptedMembership
            }
            guard numericValue.isFinite else {
                throw PercentileIndexError.corruptedMembership
            }
            try digest.add(numericValue, weight: weight)
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
                try digest.encodeBytes(),
                for: group.summaryKey
            )
        }
    }
}

public enum PercentileIndexError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case invalidGroupingValue(fieldName: String)
    case invalidNumericValue(fieldName: String)
    case invalidCompression(Double)
    case invalidPercentile(Double)
    case corruptedMembership
    case corruptedSummary
    case memberLimitExceeded(Int)
    case groupLimitExceeded(Int)
    case batchItemLimitExceeded(Int)
    case percentileLimitExceeded(Int)
    case byteLimitExceeded(Int)

    public var description: String {
        switch self {
        case .invalidGroupingValue(let fieldName):
            return "Invalid grouping value for field: \(fieldName)"
        case .invalidNumericValue(let fieldName):
            return "Invalid numeric value for field: \(fieldName)"
        case .invalidCompression(let compression):
            return "Unsupported t-digest compression: \(compression)"
        case .invalidPercentile(let percentile):
            return "Percentile must be finite and within 0...1: \(percentile)"
        case .corruptedMembership:
            return "Corrupted percentile membership"
        case .corruptedSummary:
            return "Corrupted percentile summary"
        case .memberLimitExceeded(let maximum):
            return "Percentile member scan exceeded \(maximum) entries"
        case .groupLimitExceeded(let maximum):
            return "Percentile group scan exceeded \(maximum) entries"
        case .batchItemLimitExceeded(let maximum):
            return "Percentile batch exceeded \(maximum) items"
        case .percentileLimitExceeded(let maximum):
            return "Percentile query exceeded \(maximum) requested values"
        case .byteLimitExceeded(let maximum):
            return "Percentile scan exceeded \(maximum) bytes"
        }
    }
}

private func appendPercentileGroup(
    _ group: PercentileIndexGroup,
    to groups: inout [PercentileIndexGroup]
) {
    guard !groups.contains(where: { $0.summaryKey == group.summaryKey }) else {
        return
    }
    groups.append(group)
}

private func checkedPercentileScannedBytes(
    _ current: Int,
    adding additional: Int,
    maximum: Int
) throws -> Int {
    let (updated, overflow) = current.addingReportingOverflow(additional)
    guard !overflow, updated <= maximum else {
        throw PercentileIndexError.byteLimitExceeded(maximum)
    }
    return updated
}
