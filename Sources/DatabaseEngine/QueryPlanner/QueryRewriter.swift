// QueryRewriter.swift
// QueryPlanner - Predicate optimization and rewriting

import Foundation
import Core

/// Optimizes and rewrites predicates for better query performance
///
/// Applies a series of transformation rules to simplify predicates,
/// eliminate redundancies, and prepare for index matching.
///
/// **Optimizations**:
/// - Flatten nested AND/OR
/// - Remove duplicate predicates
/// - Fold constant expressions
/// - Eliminate contradictions (e.g., x > 5 AND x < 3)
/// - Merge overlapping ranges
/// - Convert to DNF for index matching (optional)
internal struct QueryRewriter<T: Persistable>: Sendable {

    // MARK: - Configuration

    /// Configuration for query rewriting
    internal struct Config: Sendable {
        /// Flatten nested AND/OR expressions
        internal let enableFlatten: Bool

        /// Remove duplicate predicates
        internal let enableDeduplication: Bool

        /// Fold constant expressions
        internal let enableConstantFolding: Bool

        /// Eliminate contradictory conditions
        internal let enableContradictionElimination: Bool

        /// Merge overlapping range conditions
        internal let enableRangeMerging: Bool

        /// Convert to DNF for index matching
        internal let enableDNFConversion: Bool

        /// DNF converter configuration
        internal let dnfConfig: DNFConverter<T>.Config

        internal init(
            enableFlatten: Bool = true,
            enableDeduplication: Bool = true,
            enableConstantFolding: Bool = true,
            enableContradictionElimination: Bool = true,
            enableRangeMerging: Bool = true,
            enableDNFConversion: Bool = false,
            dnfConfig: DNFConverter<T>.Config = .default
        ) {
            self.enableFlatten = enableFlatten
            self.enableDeduplication = enableDeduplication
            self.enableConstantFolding = enableConstantFolding
            self.enableContradictionElimination = enableContradictionElimination
            self.enableRangeMerging = enableRangeMerging
            self.enableDNFConversion = enableDNFConversion
            self.dnfConfig = dnfConfig
        }

        /// Default configuration
        internal static var `default`: Config { Config() }

        /// Full optimization with DNF conversion
        internal static var full: Config { Config(enableDNFConversion: true) }

        /// Minimal optimization (only flatten and dedupe)
        internal static var minimal: Config {
            Config(
                enableFlatten: true,
                enableDeduplication: true,
                enableConstantFolding: false,
                enableContradictionElimination: false,
                enableRangeMerging: false,
                enableDNFConversion: false
            )
        }
    }

    // MARK: - Properties

    private let config: Config

    // MARK: - Initialization

    internal init(config: Config = .default) {
        self.config = config
    }

    // MARK: - Rewriting

    /// Rewrite a predicate with all enabled optimizations
    internal func rewrite(_ predicate: Predicate<T>) -> Predicate<T> {
        var result = predicate

        // Apply transformations in order
        if config.enableFlatten {
            result = flatten(result)
        }

        if config.enableDeduplication {
            result = deduplicate(result)
        }

        if config.enableRangeMerging {
            result = mergeRanges(result)
        }

        if config.enableConstantFolding {
            result = foldConstants(result)
        }

        if config.enableContradictionElimination {
            result = eliminateContradictions(result)
        }

        if config.enableDNFConversion {
            let converter = DNFConverter<T>(config: config.dnfConfig)
            result = converter.tryConvert(result)
        }

        return result
    }

    /// Rewrite a query's predicates
    internal func rewrite(_ query: Query<T>) -> Query<T> {
        var result = query

        // Combine all predicates
        if !query.predicates.isEmpty {
            let combined: Predicate<T>
            if query.predicates.count == 1 {
                combined = query.predicates[0]
            } else {
                combined = .and(query.predicates)
            }

            // Rewrite the combined predicate
            let rewritten = rewrite(combined)

            // Extract top-level ANDs back to array
            result.predicates = extractTopLevelPredicates(rewritten)
        }

        return result
    }

    // MARK: - Flatten Transformation

    /// Flatten nested AND/OR expressions
    ///
    /// AND(AND(a, b), c) → AND(a, b, c)
    /// OR(OR(a, b), c) → OR(a, b, c)
    private func flatten(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .and(let children):
            var flattened: [Predicate<T>] = []
            for child in children {
                let flatChild = flatten(child)
                if case .and(let nested) = flatChild {
                    flattened.append(contentsOf: nested)
                } else {
                    flattened.append(flatChild)
                }
            }
            return flattened.count == 1 ? flattened[0] : .and(flattened)

        case .or(let children):
            var flattened: [Predicate<T>] = []
            for child in children {
                let flatChild = flatten(child)
                if case .or(let nested) = flatChild {
                    flattened.append(contentsOf: nested)
                } else {
                    flattened.append(flatChild)
                }
            }
            return flattened.count == 1 ? flattened[0] : .or(flattened)

        case .not(let inner):
            return .not(flatten(inner))

        case .comparison, .true, .false:
            return predicate
        }
    }

    // MARK: - Deduplication

    /// Remove duplicate predicates
    ///
    /// AND(a == 1, b == 2, a == 1) → AND(a == 1, b == 2)
    private func deduplicate(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .and(let children):
            var seen: Set<String> = []
            var unique: [Predicate<T>] = []
            for child in children {
                let deduped = deduplicate(child)
                let key = predicateKey(deduped)
                if !seen.contains(key) {
                    seen.insert(key)
                    unique.append(deduped)
                }
            }
            return unique.count == 1 ? unique[0] : .and(unique)

        case .or(let children):
            var seen: Set<String> = []
            var unique: [Predicate<T>] = []
            for child in children {
                let deduped = deduplicate(child)
                let key = predicateKey(deduped)
                if !seen.contains(key) {
                    seen.insert(key)
                    unique.append(deduped)
                }
            }
            return unique.count == 1 ? unique[0] : .or(unique)

        case .not(let inner):
            return .not(deduplicate(inner))

        case .comparison, .true, .false:
            return predicate
        }
    }

    // MARK: - Range Merging

    /// Merge overlapping range conditions on the same field
    ///
    /// AND(age > 18, age > 21) → age > 21
    /// AND(age > 18, age < 30) → 18 < age < 30 (kept as separate for now)
    private func mergeRanges(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .and(let children):
            // Group comparisons by field
            var fieldComparisons: [String: [FieldComparison<T>]] = [:]
            var others: [Predicate<T>] = []

            for child in children.map({ mergeRanges($0) }) {
                if case .comparison(let comp) = child {
                    fieldComparisons[comp.fieldName, default: []].append(comp)
                } else {
                    others.append(child)
                }
            }

            // Merge comparisons for each field
            var merged: [Predicate<T>] = others
            for (_, comparisons) in fieldComparisons {
                let mergedComparisons = mergeFieldComparisons(comparisons)
                merged.append(contentsOf: mergedComparisons.map { .comparison($0) })
            }

            return merged.count == 1 ? merged[0] : .and(merged)

        case .or(let children):
            return .or(children.map { mergeRanges($0) })

        case .not(let inner):
            return .not(mergeRanges(inner))

        case .comparison, .true, .false:
            return predicate
        }
    }

    /// Merge comparisons on the same field
    ///
    /// Keeps the strictest bounds:
    /// - For lower bounds: higher value is stricter, exclusive is stricter than inclusive at same value
    /// - For upper bounds: lower value is stricter, exclusive is stricter than inclusive at same value
    private func mergeFieldComparisons(_ comparisons: [FieldComparison<T>]) -> [FieldComparison<T>] {
        var result: [FieldComparison<T>] = []
        var lowerBound: (value: any Sendable, inclusive: Bool)?
        var upperBound: (value: any Sendable, inclusive: Bool)?
        var equalities: [FieldComparison<T>] = []
        var others: [FieldComparison<T>] = []

        for comp in comparisons {
            switch comp.op {
            case .greaterThan:
                lowerBound = updateLowerBound(existing: lowerBound, new: (comp.value, false))

            case .greaterThanOrEqual:
                lowerBound = updateLowerBound(existing: lowerBound, new: (comp.value, true))

            case .lessThan:
                upperBound = updateUpperBound(existing: upperBound, new: (comp.value, false))

            case .lessThanOrEqual:
                upperBound = updateUpperBound(existing: upperBound, new: (comp.value, true))

            case .equal:
                equalities.append(comp)

            default:
                others.append(comp)
            }
        }

        // Reconstruct bounds as comparisons
        if let lower = lowerBound {
            let op: ComparisonOperator = lower.inclusive ? .greaterThanOrEqual : .greaterThan
            result.append(FieldComparison(
                keyPath: comparisons[0].keyPath,
                op: op,
                value: lower.value
            ))
        }

        if let upper = upperBound {
            let op: ComparisonOperator = upper.inclusive ? .lessThanOrEqual : .lessThan
            result.append(FieldComparison(
                keyPath: comparisons[0].keyPath,
                op: op,
                value: upper.value
            ))
        }

        // Add back equalities and others
        result.append(contentsOf: equalities)
        result.append(contentsOf: others)

        return result
    }

    /// Update lower bound, keeping the stricter one
    /// For lower bounds: higher value is stricter, exclusive is stricter than inclusive at same value
    private func updateLowerBound(
        existing: (value: any Sendable, inclusive: Bool)?,
        new: (value: any Sendable, inclusive: Bool)
    ) -> (value: any Sendable, inclusive: Bool) {
        guard let existing = existing else { return new }

        let cmp = compareValues(new.value, existing.value)
        switch cmp {
        case .orderedDescending:
            // New value is higher → stricter
            return new
        case .orderedAscending:
            // Existing value is higher → keep existing
            return existing
        case .orderedSame:
            // Same value: exclusive (false) is stricter than inclusive (true)
            // a > 5 is stricter than a >= 5
            return existing.inclusive ? new : existing
        }
    }

    /// Update upper bound, keeping the stricter one
    /// For upper bounds: lower value is stricter, exclusive is stricter than inclusive at same value
    private func updateUpperBound(
        existing: (value: any Sendable, inclusive: Bool)?,
        new: (value: any Sendable, inclusive: Bool)
    ) -> (value: any Sendable, inclusive: Bool) {
        guard let existing = existing else { return new }

        let cmp = compareValues(new.value, existing.value)
        switch cmp {
        case .orderedAscending:
            // New value is lower → stricter
            return new
        case .orderedDescending:
            // Existing value is lower → keep existing
            return existing
        case .orderedSame:
            // Same value: exclusive (false) is stricter than inclusive (true)
            // a < 5 is stricter than a <= 5
            return existing.inclusive ? new : existing
        }
    }

    /// Compare two type-erased Sendable values
    private func compareValues(_ lhs: any Sendable, _ rhs: any Sendable) -> ComparisonResult {
        // Try different comparable types
        if let l = lhs as? Int, let r = rhs as? Int {
            return l < r ? .orderedAscending : (l > r ? .orderedDescending : .orderedSame)
        }
        if let l = lhs as? Double, let r = rhs as? Double {
            return l < r ? .orderedAscending : (l > r ? .orderedDescending : .orderedSame)
        }
        if let l = lhs as? String, let r = rhs as? String {
            return l.compare(r)
        }
        // Default to same (can't compare)
        return .orderedSame
    }

    // MARK: - Constant Folding

    /// Fold constant expressions
    ///
    /// AND(true, a == 1) → a == 1
    /// OR(true, a == 1) → true
    /// AND(false, a == 1) → false
    private func foldConstants(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .and(let children):
            var filtered: [Predicate<T>] = []
            for child in children {
                let folded = foldConstants(child)
                switch folded {
                case .true:
                    continue // Skip true in AND
                case .false:
                    return .false // AND with false is false
                default:
                    filtered.append(folded)
                }
            }
            if filtered.isEmpty { return .true }
            return filtered.count == 1 ? filtered[0] : .and(filtered)

        case .or(let children):
            var filtered: [Predicate<T>] = []
            for child in children {
                let folded = foldConstants(child)
                switch folded {
                case .false:
                    continue // Skip false in OR
                case .true:
                    return .true // OR with true is true
                default:
                    filtered.append(folded)
                }
            }
            if filtered.isEmpty { return .false }
            return filtered.count == 1 ? filtered[0] : .or(filtered)

        case .not(let inner):
            let folded = foldConstants(inner)
            switch folded {
            case .true: return .false
            case .false: return .true
            case .not(let doubleNeg): return doubleNeg
            default: return .not(folded)
            }

        case .comparison, .true, .false:
            return predicate
        }
    }

    // MARK: - Contradiction Elimination

    /// Eliminate contradictory conditions
    ///
    /// AND(a > 5, a < 3) → false
    /// AND(a == 1, a == 2) → false
    private func eliminateContradictions(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .and(let children):
            let processed = children.map { eliminateContradictions($0) }

            // Check for contradictions
            if hasContradiction(processed) {
                return .false
            }

            // Filter out false and check for true
            var filtered: [Predicate<T>] = []
            for child in processed {
                switch child {
                case .false:
                    return .false
                case .true:
                    continue
                default:
                    filtered.append(child)
                }
            }

            if filtered.isEmpty { return .true }
            return filtered.count == 1 ? filtered[0] : .and(filtered)

        case .or(let children):
            let processed = children.map { eliminateContradictions($0) }

            var filtered: [Predicate<T>] = []
            for child in processed {
                switch child {
                case .true:
                    return .true
                case .false:
                    continue
                default:
                    filtered.append(child)
                }
            }

            if filtered.isEmpty { return .false }
            return filtered.count == 1 ? filtered[0] : .or(filtered)

        case .not(let inner):
            return .not(eliminateContradictions(inner))

        case .comparison, .true, .false:
            return predicate
        }
    }

    /// Check if a list of AND predicates contains contradictions
    private func hasContradiction(_ predicates: [Predicate<T>]) -> Bool {
        // Group comparisons by field
        var fieldComparisons: [String: [FieldComparison<T>]] = [:]

        for pred in predicates {
            if case .comparison(let comp) = pred {
                fieldComparisons[comp.fieldName, default: []].append(comp)
            }
        }

        // Check each field for contradictions
        for (_, comparisons) in fieldComparisons {
            if fieldHasContradiction(comparisons) {
                return true
            }
        }

        return false
    }

    /// Check if comparisons on a single field are contradictory
    private func fieldHasContradiction(_ comparisons: [FieldComparison<T>]) -> Bool {
        var equalityValues: Set<String> = []
        var lowerBound: (value: any Sendable, inclusive: Bool)?
        var upperBound: (value: any Sendable, inclusive: Bool)?

        for comp in comparisons {
            switch comp.op {
            case .equal:
                let valueStr = "\(comp.value)"
                if !equalityValues.isEmpty && !equalityValues.contains(valueStr) {
                    // Multiple different equality values: a == 1 AND a == 2
                    return true
                }
                equalityValues.insert(valueStr)

                // Check against range bounds
                // For lower bound: equality value must be > lower (if exclusive) or >= lower (if inclusive)
                if let lower = lowerBound {
                    let cmp = compareValues(comp.value, lower.value)
                    if cmp == .orderedAscending {
                        // Equality value is less than lower bound
                        return true
                    }
                    if cmp == .orderedSame && !lower.inclusive {
                        // a > 5 AND a == 5 is a contradiction
                        return true
                    }
                }
                // For upper bound: equality value must be < upper (if exclusive) or <= upper (if inclusive)
                if let upper = upperBound {
                    let cmp = compareValues(comp.value, upper.value)
                    if cmp == .orderedDescending {
                        // Equality value is greater than upper bound
                        return true
                    }
                    if cmp == .orderedSame && !upper.inclusive {
                        // a < 5 AND a == 5 is a contradiction
                        return true
                    }
                }

            case .greaterThan:
                lowerBound = (comp.value, false)

            case .greaterThanOrEqual:
                lowerBound = (comp.value, true)

            case .lessThan:
                upperBound = (comp.value, false)

            case .lessThanOrEqual:
                upperBound = (comp.value, true)

            default:
                break
            }
        }

        // Check if range is impossible
        if let lower = lowerBound, let upper = upperBound {
            let cmp = compareValues(lower.value, upper.value)
            if cmp == .orderedDescending {
                // Lower bound > upper bound: always contradiction
                return true
            }
            if cmp == .orderedSame {
                // Lower == upper: contradiction unless both are inclusive
                // a >= 5 AND a <= 5 is NOT a contradiction (a == 5)
                // a > 5 AND a <= 5 IS a contradiction
                // a >= 5 AND a < 5 IS a contradiction
                // a > 5 AND a < 5 IS a contradiction
                if !lower.inclusive || !upper.inclusive {
                    return true
                }
            }
        }

        return false
    }

    // MARK: - Helpers

    /// Generate a unique key for a predicate (for deduplication)
    private func predicateKey(_ predicate: Predicate<T>) -> String {
        switch predicate {
        case .comparison(let comp):
            return "\(comp.fieldName):\(comp.op.rawValue):\(comp.value)"
        case .and(let children):
            let keys = children.map { predicateKey($0) }.sorted()
            return "AND(" + keys.joined(separator: ",") + ")"
        case .or(let children):
            let keys = children.map { predicateKey($0) }.sorted()
            return "OR(" + keys.joined(separator: ",") + ")"
        case .not(let inner):
            return "NOT(" + predicateKey(inner) + ")"
        case .true:
            return "TRUE"
        case .false:
            return "FALSE"
        }
    }

    /// Extract top-level predicates from a combined predicate
    private func extractTopLevelPredicates(_ predicate: Predicate<T>) -> [Predicate<T>] {
        if case .and(let children) = predicate {
            return children
        }
        return [predicate]
    }
}
