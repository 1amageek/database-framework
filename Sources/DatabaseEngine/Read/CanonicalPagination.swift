import Foundation
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
