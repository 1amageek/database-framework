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
        let continuationOffset = try decode(options.continuation)
        let baseOffset = (selectQuery.offset ?? 0) + continuationOffset
        let remainingLimit = selectQuery.limit.map { max($0 - continuationOffset, 0) }
        if let remainingLimit, remainingLimit == 0 {
            return CanonicalPageWindow(items: [], continuation: nil)
        }

        let requestedPageSize: Int = {
            switch (options.pageSize, remainingLimit) {
            case let (.some(pageSize), .some(limit)):
                return min(pageSize, limit)
            case let (.some(pageSize), .none):
                return pageSize
            case let (.none, .some(limit)):
                return limit
            case (.none, .none):
                return items.count
            }
        }()

        let window = Array(items.dropFirst(baseOffset).prefix(requestedPageSize + 1))
        let hasMore = window.count > requestedPageSize
        let visible = hasMore ? Array(window.prefix(requestedPageSize)) : window
        let continuation = hasMore ? try encode(offset: continuationOffset + visible.count) : nil
        return CanonicalPageWindow(items: visible, continuation: continuation)
    }
}
