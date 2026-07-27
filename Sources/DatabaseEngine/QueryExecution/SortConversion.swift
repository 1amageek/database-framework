// SortConversion.swift
// DatabaseEngine - Conversion between SortOrder and SortDirection

import DatabaseKit

// MARK: - SortOrder → SortDirection

extension SortOrder {
    /// Convert to SortDirection
    public var toSortDirection: SortDirection {
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
    /// Create from SortDirection
    public init(_ direction: SortDirection) {
        switch direction {
        case .ascending:
            self = .ascending
        case .descending:
            self = .descending
        }
    }
}
