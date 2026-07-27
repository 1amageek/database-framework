#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import DatabaseKit
import StorageKit

extension SPARQLQueryExecutor {
    func applyRetainedSlice(
        _ source: consuming SPARQLRetainedBindings,
        slice: SPARQLSlice
    ) throws -> SPARQLRetainedBindings {
        precondition(slice.offset >= 0)
        precondition(slice.limit.map { $0 >= 0 } ?? true)
        guard source.isUnique else {
            return (consume source).applyingSlice(
                offset: slice.offset,
                limit: slice.limit
            )
        }

        let visibleRange: Range<Int>
        if slice.offset < source.count {
            let availableCount = source.count - slice.offset
            let visibleCount = min(
                slice.limit ?? availableCount,
                availableCount
            )
            visibleRange = slice.offset..<(slice.offset + visibleCount)
        } else {
            visibleRange = 0..<0
        }
        guard !visibleRange.isEmpty else { return .empty }
        guard visibleRange.count != source.count else {
            return consume source
        }

        let footprintMeter = try SPARQLBindingFootprintMeter.make(
            workMeter: try requiredWorkMeter(),
            stage: .projection
        )
        var releasedFootprint = DatabaseIntermediateFootprint()
        for index in 0..<source.count
        where !visibleRange.contains(index) {
            try source.withElement(at: index) { binding in
                releasedFootprint = try releasedFootprint.adding(
                    try footprintMeter.footprint(of: binding)
                )
            }
        }
        footprintMeter.shutdown()

        let sliced = (consume source).applyingSlice(
            offset: slice.offset,
            limit: slice.limit
        )
        return try (consume sliced).releasingRetainedFootprint(
            releasedFootprint
        )
    }

    /// Evaluate a Basic Graph Pattern (BGP)
    ///
    /// Optimizes join order based on selectivity and executes patterns
    /// using nested loop join with variable substitution.
}
