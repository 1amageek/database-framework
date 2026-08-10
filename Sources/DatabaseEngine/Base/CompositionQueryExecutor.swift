import DatabaseKit
import StorageKit

/// Executes a type-safe, read-only query over every Base in one Composition.
///
/// Predicates and projections execute independently inside each Base. Global
/// ordering, offset, and limit are applied after the member-local reads while
/// preserving the origin of every returned value.
public struct CompositionQueryExecutor<Model: Persistable>: Sendable {
    private struct Candidate: Sendable {
        let baseID: Base.ID
        let memberOrdinal: Int
        let rowOrdinal: Int
        let value: Model
    }

    private let source: CompositionDataSource
    private var query: Query<Model>

    package init(
        source: CompositionDataSource,
        query: Query<Model>
    ) {
        self.source = source
        self.query = query
    }

    public func `where`(
        _ predicate: Predicate<Model>
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>,
        _ order: SortOrder
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field, order)
        return copy
    }

    public func limit(_ count: Int) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    public func offset(_ count: Int) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.offset(count)
        return copy
    }

    public func partition<Value: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<Model, Value>,
        equals value: Value
    ) -> CompositionQueryExecutor<Model> {
        var copy = self
        copy.query = query.partition(field, equals: value)
        return copy
    }

    /// Executes the query at one simultaneously held read snapshot per domain.
    public func execute() async throws -> [CompositionResult<Model>] {
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        let requestedQuery = query
        let resultLimit = requestedQuery.fetchLimit
        let resultOffset = requestedQuery.fetchOffset ?? 0
        let localLimit = Self.localReadLimit(
            offset: resultOffset,
            limit: resultLimit
        )

        return try await source.withReadSnapshot { snapshot in
            if resultLimit == 0 {
                return []
            }
            var candidates: [Candidate] = []
            if let localLimit {
                candidates.reserveCapacity(min(localLimit, 1_024))
            }

            if requestedQuery.sortDescriptors.isEmpty {
                var remainingOffset = resultOffset
                var remainingLimit = resultLimit
                for (memberOrdinal, member) in
                    snapshot.lease.members.enumerated()
                {
                    if remainingLimit == 0 { break }
                    var configuredQuery = requestedQuery
                    configuredQuery.fetchOffset = nil
                    configuredQuery.fetchLimit = Self.localReadLimit(
                        offset: remainingOffset,
                        limit: remainingLimit
                    )
                    let memberQuery = configuredQuery
                    let values = try await source.withMemberContext(
                        member,
                        in: snapshot
                    ) { context, transaction in
                        try await context.fetch(
                            memberQuery,
                            transaction: transaction
                        )
                    }
                    let start = min(remainingOffset, values.count)
                    remainingOffset -= start
                    let available = values.count - start
                    let accepted = min(available, remainingLimit ?? available)
                    if accepted > 0 {
                        candidates.reserveCapacity(candidates.count + accepted)
                        for rowOrdinal in start..<(start + accepted) {
                            candidates.append(
                                Candidate(
                                    baseID: member.baseID,
                                    memberOrdinal: memberOrdinal,
                                    rowOrdinal: rowOrdinal,
                                    value: values[rowOrdinal]
                                )
                            )
                        }
                    }
                    if let limit = remainingLimit {
                        remainingLimit = limit - accepted
                    }
                }
            } else {
                for (memberOrdinal, member) in
                    snapshot.lease.members.enumerated()
                {
                    var configuredQuery = requestedQuery
                    configuredQuery.fetchOffset = nil
                    configuredQuery.fetchLimit = localLimit
                    let memberQuery = configuredQuery
                    let values = try await source.withMemberContext(
                        member,
                        in: snapshot
                    ) { context, transaction in
                        try await context.fetch(
                            memberQuery,
                            transaction: transaction
                        )
                    }
                    var memberCandidates: [Candidate] = []
                    memberCandidates.reserveCapacity(values.count)
                    for rowOrdinal in values.indices {
                        memberCandidates.append(
                            Candidate(
                                baseID: member.baseID,
                                memberOrdinal: memberOrdinal,
                                rowOrdinal: rowOrdinal,
                                value: values[rowOrdinal]
                            )
                        )
                    }
                    candidates = try Self.merge(
                        candidates,
                        memberCandidates,
                        descriptors: requestedQuery.sortDescriptors,
                        maximumCount: localLimit
                    )
                }
                QueryResultWindow.apply(
                    to: &candidates,
                    limit: resultLimit,
                    offset: resultOffset
                )
            }

            let composition = snapshot.lease.record
            return candidates.map { candidate in
                CompositionResult(
                    compositionID: composition.composition.id,
                    generation: composition.generation,
                    origin: .source(candidate.baseID),
                    value: candidate.value
                )
            }
        }
    }

    public func count() async throws -> Int {
        try QueryResultWindow.validate(
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )
        let requestedQuery = query
        return try await source.withReadSnapshot { snapshot in
            var total = 0
            var normalizedQuery = requestedQuery
            normalizedQuery.fetchLimit = nil
            normalizedQuery.fetchOffset = nil
            let memberQuery = normalizedQuery
            for member in snapshot.lease.members {
                let partial = try await source.withMemberContext(
                    member,
                    in: snapshot
                ) { context, transaction in
                    try await context.fetchCount(
                        memberQuery,
                        transaction: transaction
                    )
                }
                let next = total.addingReportingOverflow(partial)
                guard !next.overflow else {
                    throw DatabaseCompositionQueryError.countOverflow
                }
                total = next.partialValue
            }
            return QueryResultWindow.resultCount(
                totalCount: total,
                limit: requestedQuery.fetchLimit,
                offset: requestedQuery.fetchOffset
            )
        }
    }

    public func first() async throws -> CompositionResult<Model>? {
        try await limit(1).execute().first
    }

    private static func localReadLimit(
        offset: Int,
        limit: Int?
    ) -> Int? {
        guard let limit else { return nil }
        let (value, overflow) = offset.addingReportingOverflow(limit)
        return overflow ? nil : value
    }

    private static func merge(
        _ left: consuming [Candidate],
        _ right: consuming [Candidate],
        descriptors: borrowing [SortDescriptor<Model>],
        maximumCount: Int?
    ) throws -> [Candidate] {
        var merged: [Candidate] = []
        let unboundedCount = left.count + right.count
        merged.reserveCapacity(min(maximumCount ?? unboundedCount, unboundedCount))
        var leftIndex = 0
        var rightIndex = 0
        while leftIndex < left.count || rightIndex < right.count {
            if let maximumCount, merged.count == maximumCount { break }
            if leftIndex == left.count {
                merged.append(right[rightIndex])
                rightIndex += 1
                continue
            }
            if rightIndex == right.count {
                merged.append(left[leftIndex])
                leftIndex += 1
                continue
            }
            if try isOrderedBefore(
                left[leftIndex],
                right[rightIndex],
                descriptors: descriptors
            ) {
                merged.append(left[leftIndex])
                leftIndex += 1
            } else {
                merged.append(right[rightIndex])
                rightIndex += 1
            }
        }
        return merged
    }

    private static func isOrderedBefore(
        _ left: borrowing Candidate,
        _ right: borrowing Candidate,
        descriptors: borrowing [SortDescriptor<Model>]
    ) throws -> Bool {
        for index in descriptors.indices {
            let descriptor = descriptors[index]
            switch try descriptor.orderedComparison(left.value, right.value) {
            case .lessThan:
                return true
            case .greaterThan:
                return false
            case .equal:
                continue
            }
        }
        if left.memberOrdinal != right.memberOrdinal {
            return left.memberOrdinal < right.memberOrdinal
        }
        return left.rowOrdinal < right.rowOrdinal
    }
}
