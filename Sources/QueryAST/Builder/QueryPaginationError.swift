public enum QueryPaginationError: Error, Sendable, Equatable {
    case offsetOverflow(pageIndex: UInt64, pageSize: UInt64)
}
