// SortBridge.swift
// DatabaseEngine - Bridge between SortOrder and QueryIR.SortDirection

import QueryIR

// MARK: - SortOrder → SortDirection

extension SortOrder {
    /// Convert to QueryIR.SortDirection
    public var toSortDirection: QueryIR.SortDirection {
        switch self {
        case .ascending:
            return .ascending
        case .descending:
            return .descending
        }
    }
}

// MARK: - SortDirection → SortOrder

extension SortOrder {
    /// Create from QueryIR.SortDirection
    public init(_ direction: QueryIR.SortDirection) {
        switch direction {
        case .ascending:
            self = .ascending
        case .descending:
            self = .descending
        }
    }
}
