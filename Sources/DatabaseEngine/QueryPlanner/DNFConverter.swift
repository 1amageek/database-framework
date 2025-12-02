// DNFConverter.swift
// QueryPlanner - Disjunctive Normal Form conversion with explosion protection

import Foundation
import Core

/// Converts predicates to Disjunctive Normal Form (DNF)
///
/// DNF is a normalized form where the predicate is expressed as:
/// OR(AND(...), AND(...), ...)
///
/// This form is useful for index matching as each AND clause can be
/// independently evaluated against available indexes.
///
/// **Explosion Protection**: DNF conversion can cause exponential growth.
/// For example, (A OR B) AND (C OR D) AND (E OR F) produces 2^3 = 8 terms.
/// This converter limits the number of terms to prevent memory exhaustion.
internal struct DNFConverter<T: Persistable>: Sendable {

    // MARK: - Configuration

    /// Configuration for DNF conversion
    internal struct Config: Sendable {
        /// Maximum number of DNF terms before giving up
        internal let maxTerms: Int

        /// Maximum recursion depth
        internal let maxDepth: Int

        /// Whether to simplify after conversion
        internal let simplifyResult: Bool

        internal init(
            maxTerms: Int = 100,
            maxDepth: Int = 50,
            simplifyResult: Bool = true
        ) {
            self.maxTerms = maxTerms
            self.maxDepth = maxDepth
            self.simplifyResult = simplifyResult
        }

        /// Default configuration
        internal static var `default`: Config { Config() }

        /// Strict configuration for complex queries
        internal static var strict: Config { Config(maxTerms: 20, maxDepth: 20) }

        /// Relaxed configuration for simple queries
        internal static var relaxed: Config { Config(maxTerms: 500, maxDepth: 100) }
    }

    // MARK: - Properties

    private let config: Config

    // MARK: - Initialization

    internal init(config: Config = .default) {
        self.config = config
    }

    // MARK: - Public Interface

    /// Convert a predicate to DNF
    ///
    /// - Parameter predicate: The predicate to convert
    /// - Returns: DNF form of the predicate, or the original if conversion would explode
    /// - Throws: `DNFConversionError` if limits are exceeded
    internal func convert(_ predicate: Predicate<T>) throws -> Predicate<T> {
        // Step 1: Push NOT operators down to leaves (De Morgan's laws)
        let negationNormalized = pushNotDown(predicate)

        // Step 2: Convert to DNF with explosion protection
        let dnf = try toDNF(negationNormalized, depth: 0)

        // Step 3: Simplify if configured
        if config.simplifyResult {
            return simplify(dnf)
        }

        return dnf
    }

    /// Try to convert to DNF, returning original predicate if it would explode
    internal func tryConvert(_ predicate: Predicate<T>) -> Predicate<T> {
        do {
            return try convert(predicate)
        } catch {
            // Conversion would explode, return original
            return predicate
        }
    }

    /// Estimate the number of DNF terms without converting
    internal func estimateTermCount(_ predicate: Predicate<T>) -> Int {
        estimateTerms(pushNotDown(predicate))
    }

    // MARK: - NOT Pushdown (De Morgan's Laws)

    /// Push NOT operators down to leaf nodes
    private func pushNotDown(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .not(let inner):
            return pushNotDownInner(inner)

        case .and(let children):
            return .and(children.map { pushNotDown($0) })

        case .or(let children):
            return .or(children.map { pushNotDown($0) })

        case .comparison, .true, .false:
            return predicate
        }
    }

    /// Apply De Morgan's laws to push NOT inward
    private func pushNotDownInner(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .not(let inner):
            // Double negation: NOT(NOT(A)) = A
            return pushNotDown(inner)

        case .and(let children):
            // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
            return .or(children.map { pushNotDown(.not($0)) })

        case .or(let children):
            // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
            return .and(children.map { pushNotDown(.not($0)) })

        case .comparison(let comp):
            // Negate the comparison, or wrap in NOT if it can't be directly negated
            if let negated = negateComparison(comp) {
                return .comparison(negated)
            } else {
                // Complex operators (in, contains, etc.) remain wrapped in NOT
                return .not(.comparison(comp))
            }

        case .true:
            return .false

        case .false:
            return .true
        }
    }

    /// Negate a field comparison
    ///
    /// Returns the negated comparison, or nil if the operator cannot be directly negated
    /// (in which case the caller should wrap the original in NOT).
    private func negateComparison(_ comparison: FieldComparison<T>) -> FieldComparison<T>? {
        let newOp: ComparisonOperator
        switch comparison.op {
        case .equal:
            newOp = .notEqual
        case .notEqual:
            newOp = .equal
        case .lessThan:
            newOp = .greaterThanOrEqual
        case .lessThanOrEqual:
            newOp = .greaterThan
        case .greaterThan:
            newOp = .lessThanOrEqual
        case .greaterThanOrEqual:
            newOp = .lessThan
        case .isNil:
            newOp = .isNotNil
        case .isNotNil:
            newOp = .isNil
        default:
            // Complex operators (in, contains, etc.) cannot be directly negated
            // Return nil to signal that the caller should wrap in NOT
            return nil
        }

        return FieldComparison(
            keyPath: comparison.keyPath,
            op: newOp,
            value: comparison.value
        )
    }

    // MARK: - DNF Conversion

    /// Convert to DNF with depth tracking
    private func toDNF(_ predicate: Predicate<T>, depth: Int) throws -> Predicate<T> {
        guard depth < config.maxDepth else {
            throw DNFConversionError.maxDepthExceeded(depth: depth)
        }

        switch predicate {
        case .comparison, .true, .false:
            // Leaf nodes are already in DNF
            return predicate

        case .not:
            // Should have been pushed down
            return predicate

        case .or(let children):
            // OR at top level is valid for DNF
            let dnfChildren = try children.map { try toDNF($0, depth: depth + 1) }
            return flattenOr(.or(dnfChildren))

        case .and(let children):
            // AND needs distribution if any child is OR
            return try distributeAnd(children, depth: depth)
        }
    }

    /// Distribute AND over OR to create DNF
    ///
    /// (A OR B) AND C = (A AND C) OR (B AND C)
    private func distributeAnd(_ children: [Predicate<T>], depth: Int) throws -> Predicate<T> {
        // Convert each child to DNF first
        let dnfChildren = try children.map { try toDNF($0, depth: depth + 1) }

        // Extract OR terms from each child
        var termGroups: [[Predicate<T>]] = []
        for child in dnfChildren {
            termGroups.append(extractOrTerms(child))
        }

        // Compute Cartesian product
        var result: [[Predicate<T>]] = [[]]

        for terms in termGroups {
            var newResult: [[Predicate<T>]] = []

            for existing in result {
                for term in terms {
                    var combined = existing
                    combined.append(contentsOf: extractAndTerms(term))
                    newResult.append(combined)

                    // Check explosion limit
                    guard newResult.count <= config.maxTerms else {
                        throw DNFConversionError.termLimitExceeded(count: newResult.count)
                    }
                }
            }

            result = newResult
        }

        // Build result: OR of ANDs
        let orTerms = result.map { andTerms -> Predicate<T> in
            if andTerms.count == 1 {
                return andTerms[0]
            }
            return .and(andTerms)
        }

        if orTerms.count == 1 {
            return orTerms[0]
        }
        return .or(orTerms)
    }

    // MARK: - Term Extraction

    /// Extract top-level OR terms
    private func extractOrTerms(_ predicate: Predicate<T>) -> [Predicate<T>] {
        if case .or(let children) = predicate {
            return children.flatMap { extractOrTerms($0) }
        }
        return [predicate]
    }

    /// Extract top-level AND terms
    private func extractAndTerms(_ predicate: Predicate<T>) -> [Predicate<T>] {
        if case .and(let children) = predicate {
            return children.flatMap { extractAndTerms($0) }
        }
        return [predicate]
    }

    /// Flatten nested ORs
    private func flattenOr(_ predicate: Predicate<T>) -> Predicate<T> {
        guard case .or(let children) = predicate else {
            return predicate
        }

        var flattened: [Predicate<T>] = []
        for child in children {
            if case .or(let nested) = child {
                flattened.append(contentsOf: nested.map { flattenOr($0) })
            } else {
                flattened.append(child)
            }
        }

        if flattened.count == 1 {
            return flattened[0]
        }
        return .or(flattened)
    }

    // MARK: - Term Count Estimation

    /// Estimate the number of terms without actually converting
    private func estimateTerms(_ predicate: Predicate<T>) -> Int {
        switch predicate {
        case .comparison, .true, .false, .not:
            return 1

        case .or(let children):
            return children.map { estimateTerms($0) }.reduce(0, +)

        case .and(let children):
            return children.map { estimateTerms($0) }.reduce(1, *)
        }
    }

    // MARK: - Simplification

    /// Simplify a DNF predicate
    private func simplify(_ predicate: Predicate<T>) -> Predicate<T> {
        switch predicate {
        case .or(let children):
            // Remove duplicates and false
            var seen: Set<String> = []
            var simplified: [Predicate<T>] = []

            for child in children {
                if case .false = child { continue }
                if case .true = child { return .true }

                let key = predicateKey(child)
                if !seen.contains(key) {
                    seen.insert(key)
                    simplified.append(simplify(child))
                }
            }

            if simplified.isEmpty { return .false }
            if simplified.count == 1 { return simplified[0] }
            return .or(simplified)

        case .and(let children):
            // Remove duplicates and true
            var seen: Set<String> = []
            var simplified: [Predicate<T>] = []

            for child in children {
                if case .true = child { continue }
                if case .false = child { return .false }

                let key = predicateKey(child)
                if !seen.contains(key) {
                    seen.insert(key)
                    simplified.append(simplify(child))
                }
            }

            if simplified.isEmpty { return .true }
            if simplified.count == 1 { return simplified[0] }
            return .and(simplified)

        default:
            return predicate
        }
    }

    /// Generate a key for predicate deduplication
    private func predicateKey(_ predicate: Predicate<T>) -> String {
        switch predicate {
        case .comparison(let comp):
            return "\(comp.fieldName):\(comp.op.rawValue):\(comp.value.value)"
        case .and(let children):
            return "AND(" + children.map { predicateKey($0) }.sorted().joined(separator: ",") + ")"
        case .or(let children):
            return "OR(" + children.map { predicateKey($0) }.sorted().joined(separator: ",") + ")"
        case .not(let inner):
            return "NOT(" + predicateKey(inner) + ")"
        case .true:
            return "TRUE"
        case .false:
            return "FALSE"
        }
    }
}

// MARK: - Errors

/// Errors that can occur during DNF conversion
internal enum DNFConversionError: Error, CustomStringConvertible {
    /// Maximum recursion depth exceeded
    case maxDepthExceeded(depth: Int)

    /// Term limit exceeded (explosion protection)
    case termLimitExceeded(count: Int)

    internal var description: String {
        switch self {
        case .maxDepthExceeded(let depth):
            return "DNF conversion exceeded maximum depth: \(depth)"
        case .termLimitExceeded(let count):
            return "DNF conversion would produce \(count) terms, exceeding limit"
        }
    }
}

// MARK: - FieldComparison Extension

extension FieldComparison {
    /// Create a FieldComparison with explicit values
    internal init(keyPath: AnyKeyPath, op: ComparisonOperator, value: AnySendable) {
        self.keyPath = keyPath
        self.op = op
        self.value = value
    }
}
