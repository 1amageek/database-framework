enum QueryResultWindow {
    static func validate(
        limit: Int?,
        offset: Int?
    ) throws(DatabaseQueryError) {
        if let limit, limit < 0 {
            throw .invalidLimit(limit)
        }
        if let offset, offset < 0 {
            throw .invalidOffset(offset)
        }
    }

    static func apply<Element>(
        to elements: inout [Element],
        limit: Int?,
        offset: Int?
    ) {
        let offset = offset ?? 0
        if offset >= elements.count {
            elements.removeAll(keepingCapacity: false)
            return
        }
        if offset > 0 {
            elements.removeFirst(offset)
        }
        if let limit, limit < elements.count {
            elements.removeSubrange(limit..<elements.count)
        }
    }

    static func resultCount(
        totalCount: Int,
        limit: Int?,
        offset: Int?
    ) -> Int {
        let remaining = max(0, totalCount - (offset ?? 0))
        guard let limit else { return remaining }
        return min(remaining, limit)
    }

    static func indexReadLimit(
        requestedLimit: Int?,
        offset: Int?,
        hasSort: Bool,
        requiresPostFilter: Bool,
        hasSecurityFilter: Bool
    ) -> Int? {
        guard offset == nil || offset == 0,
              !hasSort,
              !requiresPostFilter,
              !hasSecurityFilter else {
            return nil
        }
        return requestedLimit
    }
}
