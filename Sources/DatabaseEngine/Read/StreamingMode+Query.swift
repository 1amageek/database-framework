import StorageKit

extension StreamingMode {
    /// Select a range-read streaming policy from the bounded query window.
    ///
    /// A small explicit limit favors a single response. Full scans favor
    /// prefetching, while an unknown result size retains adaptive iteration.
    public static func forQuery(
        estimatedRows: Int? = nil,
        limit: Int? = nil,
        isFullScan: Bool = false,
        isSingleClient: Bool = false
    ) -> StreamingMode {
        if let limit, limit <= 100 {
            return .exact
        }
        if isFullScan && isSingleClient {
            return .serial
        }
        if isFullScan {
            return .wantAll
        }
        if let estimatedRows {
            if estimatedRows > 10_000 {
                return .serial
            }
            if estimatedRows > 1_000 {
                return .large
            }
            if estimatedRows > 100 {
                return .medium
            }
            return .small
        }
        return .iterator
    }
}
