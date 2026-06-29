import Foundation
import Core
import QueryIR
import DatabaseClientProtocol

public struct CanonicalPageWindow<Item: Sendable>: Sendable {
    public let items: [Item]
    public let continuation: QueryContinuation?

    public init(items: [Item], continuation: QueryContinuation?) {
        self.items = items
        self.continuation = continuation
    }
}

public struct CanonicalPaginationContext: Sendable {
    public let continuationOffset: Int
    public let baseOffset: Int
    public let effectivePageSize: Int?
    public let isExhausted: Bool

    public init(
        continuationOffset: Int,
        baseOffset: Int,
        effectivePageSize: Int?,
        isExhausted: Bool
    ) {
        self.continuationOffset = continuationOffset
        self.baseOffset = baseOffset
        self.effectivePageSize = effectivePageSize
        self.isExhausted = isExhausted
    }
}

public enum CanonicalOffsetPagination {
    private struct Payload: Codable, Sendable {
        let offset: Int
    }

    public static func decode(_ continuation: QueryContinuation?) throws -> Int {
        guard let continuation else { return 0 }
        guard let data = Data(base64Encoded: continuation.token) else {
            throw CanonicalReadError.invalidContinuation
        }
        let payload = try JSONDecoder().decode(Payload.self, from: data)
        return payload.offset
    }

    public static func encode(offset: Int) throws -> QueryContinuation {
        let data = try JSONEncoder().encode(Payload(offset: offset))
        return QueryContinuation(data.base64EncodedString())
    }

    public static func window<Item: Sendable>(
        items: [Item],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> CanonicalPageWindow<Item> {
        try window(
            items: items,
            context: context(selectQuery: selectQuery, options: options)
        )
    }

    public static func context(
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> CanonicalPaginationContext {
        let continuationOffset = try decode(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return CanonicalPaginationContext(
                continuationOffset: continuationOffset,
                baseOffset: baseOffset,
                effectivePageSize: 0,
                isExhausted: true
            )
        }

        let requestedPageSize: Int? = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return nil
            }
        }()

        return CanonicalPaginationContext(
            continuationOffset: continuationOffset,
            baseOffset: baseOffset,
            effectivePageSize: requestedPageSize,
            isExhausted: false
        )
    }

    public static func window<Item: Sendable>(
        items: [Item],
        context: CanonicalPaginationContext,
        baseOffsetAlreadyApplied: Bool = false
    ) throws -> CanonicalPageWindow<Item> {
        if context.isExhausted {
            return CanonicalPageWindow(items: [], continuation: nil)
        }

        let offsetItems = baseOffsetAlreadyApplied ? items : Array(items.dropFirst(context.baseOffset))

        guard let effectivePageSize = context.effectivePageSize else {
            return CanonicalPageWindow(items: offsetItems, continuation: nil)
        }

        let window = Array(offsetItems.prefix(effectivePageSize + 1))
        let hasMore = window.count > effectivePageSize
        let visible = hasMore ? Array(window.prefix(effectivePageSize)) : window
        let continuation = hasMore ? try encode(offset: context.continuationOffset + visible.count) : nil
        return CanonicalPageWindow(items: visible, continuation: continuation)
    }
}

public enum CanonicalQueryPagination {
    private static let keysetSeedToken = "keyset:v1"
    private static let keysetTokenPrefix = "keyset:v1:"

    private struct KeysetPayload: Codable, Sendable {
        let returned: Int
        let values: [FieldValue]
    }

    private struct KeysetSortField: Sendable {
        let name: String
        let direction: SortDirection
        let nulls: NullOrdering?
    }

    public static func window(
        rows: [QueryRow],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> CanonicalPageWindow<QueryRow> {
        guard isKeysetContinuation(options.continuation) else {
            return try CanonicalOffsetPagination.window(
                items: rows,
                selectQuery: selectQuery,
                options: options
            )
        }

        return try keysetWindow(
            rows: rows,
            selectQuery: selectQuery,
            options: options
        )
    }

    private static func isKeysetContinuation(_ continuation: QueryContinuation?) -> Bool {
        guard let token = continuation?.token else { return false }
        return token == keysetSeedToken || token.hasPrefix(keysetTokenPrefix)
    }

    private static func keysetWindow(
        rows: [QueryRow],
        selectQuery: SelectQuery,
        options: ReadExecutionOptions
    ) throws -> CanonicalPageWindow<QueryRow> {
        let payload = try decodeKeysetPayload(options.continuation)
        guard !rows.isEmpty else {
            return CanonicalPageWindow(items: [], continuation: nil)
        }
        let sortFields = try keysetSortFields(
            orderBy: selectQuery.orderBy,
            rows: rows
        )
        let orderedRows = rows.sorted { lhs, rhs in
            compare(lhs, rhs, sortFields: sortFields) == .orderedAscending
        }

        let resumedRows: [QueryRow]
        if payload.values.isEmpty {
            resumedRows = Array(orderedRows.dropFirst(selectQuery.offset ?? 0))
        } else {
            resumedRows = orderedRows.filter { row in
                let comparison = compare(
                    values(for: row, sortFields: sortFields),
                    payload.values,
                    sortFields: sortFields
                )
                return comparison == .orderedDescending
            }
        }

        let remainingLimit = selectQuery.limit.map { max($0 - payload.returned, 0) }
        if remainingLimit == 0 {
            return CanonicalPageWindow(items: [], continuation: nil)
        }

        let effectivePageSize: Int? = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return nil
            }
        }()

        guard let effectivePageSize else {
            return CanonicalPageWindow(items: resumedRows, continuation: nil)
        }

        let window = Array(resumedRows.prefix(effectivePageSize + 1))
        let visible = Array(window.prefix(effectivePageSize))
        let returned = payload.returned + visible.count
        let canReturnMoreWithinLimit = selectQuery.limit.map { returned < $0 } ?? true
        let hasMore = window.count > effectivePageSize
            && canReturnMoreWithinLimit

        guard hasMore, let last = visible.last else {
            return CanonicalPageWindow(items: visible, continuation: nil)
        }

        return try CanonicalPageWindow(
            items: visible,
            continuation: encodeKeysetPayload(
                KeysetPayload(
                    returned: returned,
                    values: values(for: last, sortFields: sortFields)
                )
            )
        )
    }

    private static func keysetSortFields(
        orderBy: [SortKey]?,
        rows: [QueryRow]
    ) throws -> [KeysetSortField] {
        var fields: [KeysetSortField] = []
        for sortKey in orderBy ?? [] {
            guard case .column(let column) = sortKey.expression else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Keyset pagination requires column ORDER BY expressions"
                )
            }
            guard rows.contains(where: { $0.fields[column.column] != nil }) else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Keyset pagination requires ORDER BY column '\(column.column)' in projected rows"
                )
            }
            fields.append(
                KeysetSortField(
                    name: column.column,
                    direction: sortKey.direction,
                    nulls: sortKey.nulls
                )
            )
        }

        if !fields.contains(where: { $0.name == "id" }),
           rows.contains(where: { $0.fields["id"] != nil }) {
            fields.append(
                KeysetSortField(
                    name: "id",
                    direction: .ascending,
                    nulls: .last
                )
            )
        }

        guard !fields.isEmpty else {
            throw CanonicalReadError.unsupportedSelectQuery(
                "Keyset pagination requires ORDER BY columns or an id field"
            )
        }

        return fields
    }

    private static func decodeKeysetPayload(
        _ continuation: QueryContinuation?
    ) throws -> KeysetPayload {
        guard let token = continuation?.token, token != keysetSeedToken else {
            return KeysetPayload(returned: 0, values: [])
        }
        guard token.hasPrefix(keysetTokenPrefix) else {
            throw CanonicalReadError.invalidContinuation
        }
        let encoded = String(token.dropFirst(keysetTokenPrefix.count))
        guard let data = Data(base64Encoded: encoded) else {
            throw CanonicalReadError.invalidContinuation
        }
        do {
            return try JSONDecoder().decode(KeysetPayload.self, from: data)
        } catch {
            throw CanonicalReadError.invalidContinuation
        }
    }

    private static func encodeKeysetPayload(
        _ payload: KeysetPayload
    ) throws -> QueryContinuation {
        let data = try JSONEncoder().encode(payload)
        return QueryContinuation(keysetTokenPrefix + data.base64EncodedString())
    }

    private static func values(
        for row: QueryRow,
        sortFields: [KeysetSortField]
    ) -> [FieldValue] {
        sortFields.map { row.fields[$0.name] ?? .null }
    }

    private static func compare(
        _ lhs: QueryRow,
        _ rhs: QueryRow,
        sortFields: [KeysetSortField]
    ) -> ComparisonResult {
        compare(
            values(for: lhs, sortFields: sortFields),
            values(for: rhs, sortFields: sortFields),
            sortFields: sortFields
        )
    }

    private static func compare(
        _ lhsValues: [FieldValue],
        _ rhsValues: [FieldValue],
        sortFields: [KeysetSortField]
    ) -> ComparisonResult {
        for index in sortFields.indices {
            let sortField = sortFields[index]
            let comparison = compareField(
                lhsValues[index],
                rhsValues[index],
                nulls: sortField.nulls
            )
            guard comparison != .orderedSame else { continue }

            switch sortField.direction {
            case .ascending:
                return comparison
            case .descending:
                return reverse(comparison)
            }
        }
        return .orderedSame
    }

    private static func compareField(
        _ lhs: FieldValue,
        _ rhs: FieldValue,
        nulls: NullOrdering?
    ) -> ComparisonResult {
        switch (lhs, rhs) {
        case (.null, .null):
            return .orderedSame
        case (.null, _):
            return nulls == .last ? .orderedDescending : .orderedAscending
        case (_, .null):
            return nulls == .last ? .orderedAscending : .orderedDescending
        default:
            return lhs.compare(to: rhs) ?? .orderedSame
        }
    }

    private static func reverse(_ comparison: ComparisonResult) -> ComparisonResult {
        switch comparison {
        case .orderedAscending:
            return .orderedDescending
        case .orderedDescending:
            return .orderedAscending
        case .orderedSame:
            return .orderedSame
        }
    }
}
