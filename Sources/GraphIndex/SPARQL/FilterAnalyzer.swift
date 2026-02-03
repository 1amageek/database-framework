// FilterAnalyzer.swift
// GraphIndex - SPARQL filter analysis and pushdown optimization
//
// Analyzes FilterExpression to separate pushable filters (early evaluation)
// from complex filters (post-scan evaluation).

import Foundation
import Core
import Graph
import DatabaseEngine

/// Analyzes SPARQL FilterExpression to determine filter pushdown strategy
///
/// **Pushable filters** (evaluated during index scan):
/// - Simple comparisons: `.equals`, `.lessThan`, `.greaterThan`, etc.
/// - String operations: `.contains`, `.startsWith`, `.endsWith`
/// - AND combinations of pushable filters
///
/// **Complex filters** (evaluated post-scan):
/// - Logical operations: `.or`, `.not`
/// - Pattern matching: `.regex`
/// - Custom functions: `.custom`
/// - Variable comparisons: `.variableEquals`
///
/// **Design**: Filters on graph structure fields (?subject, ?predicate, ?object, ?graph)
/// are never pushed down, as they're handled by index key selection.
/// Only property fields stored in CoveringValue are candidates for pushdown.
struct FilterAnalyzer: Sendable {

    // MARK: - Properties

    /// Graph structure variable names (?subject, ?predicate, ?object, ?graph)
    private let structureVariables: Set<String>

    /// Property fields available in CoveringValue (from storedFieldNames)
    private let availablePropertyFields: Set<String>

    // MARK: - Initialization

    /// Initialize with graph pattern variables and available property fields
    ///
    /// - Parameters:
    ///   - subjectVar: Subject variable name (e.g., "?s")
    ///   - predicateVar: Predicate variable name (e.g., "?p")
    ///   - objectVar: Object variable name (e.g., "?o")
    ///   - graphVar: Graph variable name (e.g., "?g"), nil if not using named graphs
    ///   - availablePropertyFields: Field names stored in CoveringValue
    init(
        subjectVar: String?,
        predicateVar: String?,
        objectVar: String?,
        graphVar: String?,
        availablePropertyFields: Set<String>
    ) {
        var structureVars = Set<String>()
        if let v = subjectVar { structureVars.insert(v) }
        if let v = predicateVar { structureVars.insert(v) }
        if let v = objectVar { structureVars.insert(v) }
        if let v = graphVar { structureVars.insert(v) }

        self.structureVariables = structureVars
        self.availablePropertyFields = availablePropertyFields
    }

    // MARK: - Analysis

    /// Analyze FilterExpression and separate into pushable and remaining filters
    ///
    /// - Parameter expr: The filter expression to analyze
    /// - Returns: Tuple of (pushable filters for scan, remaining complex filters for post-scan)
    func analyze(_ expr: FilterExpression) -> (
        pushable: [PropertyFilter],
        remaining: FilterExpression?
    ) {
        switch expr {
        // Simple comparisons - try to push down
        case .equals(let variable, let value):
            return tryPush(variable, .equal, value, originalExpr: expr)

        case .notEquals(let variable, let value):
            return tryPush(variable, .notEqual, value, originalExpr: expr)

        case .lessThan(let variable, let value):
            return tryPush(variable, .lessThan, value, originalExpr: expr)

        case .lessThanOrEqual(let variable, let value):
            return tryPush(variable, .lessThanOrEqual, value, originalExpr: expr)

        case .greaterThan(let variable, let value):
            return tryPush(variable, .greaterThan, value, originalExpr: expr)

        case .greaterThanOrEqual(let variable, let value):
            return tryPush(variable, .greaterThanOrEqual, value, originalExpr: expr)

        // String operations - try to push down
        case .contains(let variable, let substring):
            return tryPush(variable, .contains, .string(substring), originalExpr: expr)

        case .startsWith(let variable, let prefix):
            return tryPush(variable, .hasPrefix, .string(prefix), originalExpr: expr)

        case .endsWith(let variable, let suffix):
            return tryPush(variable, .hasSuffix, .string(suffix), originalExpr: expr)

        // AND - recursively analyze both sides
        case .and(let left, let right):
            let (leftPush, leftRemain) = analyze(left)
            let (rightPush, rightRemain) = analyze(right)
            return (
                leftPush + rightPush,
                combineRemaining(leftRemain, rightRemain)
            )

        // Complex filters - cannot push down
        case .variableEquals, .variableNotEquals:
            // Variable-to-variable comparisons need both variables bound
            return ([], expr)

        case .bound, .notBound:
            // Existence checks - handled by structure matching
            return ([], expr)

        case .regex, .regexWithFlags:
            // Pattern matching - too complex for early filtering
            return ([], expr)

        case .or, .not:
            // Complex logical operations - cannot decompose
            return ([], expr)

        case .custom, .customWithVariables:
            // Arbitrary logic - cannot analyze
            return ([], expr)

        case .alwaysTrue:
            // No filtering needed
            return ([], nil)

        case .alwaysFalse:
            // Filter everything - keep as complex filter
            return ([], expr)
        }
    }

    // MARK: - Private Helpers

    /// Try to convert a simple comparison to a PropertyFilter
    ///
    /// Returns (filter, nil) if pushable, or ([], originalExpr) if not pushable
    private func tryPush(
        _ variable: String,
        _ op: ComparisonOperator,
        _ value: FieldValue,
        originalExpr: FilterExpression
    ) -> (pushable: [PropertyFilter], remaining: FilterExpression?) {
        // Remove "?" prefix from variable name to get field name
        let fieldName = variable.hasPrefix("?")
            ? String(variable.dropFirst())
            : variable

        // Structure fields (subject/predicate/object/graph) are handled by index key selection
        // They should not be pushed to property filters
        guard !structureVariables.contains(variable) else {
            return ([], originalExpr)
        }

        // Only push filters on fields that are stored in CoveringValue
        guard availablePropertyFields.contains(fieldName) else {
            return ([], originalExpr)
        }

        // Create PropertyFilter for pushdown
        let filter = PropertyFilter(
            fieldName: fieldName,
            op: op,
            value: value
        )
        return ([filter], nil)
    }

    /// Combine two optional filter expressions with AND
    private func combineRemaining(
        _ left: FilterExpression?,
        _ right: FilterExpression?
    ) -> FilterExpression? {
        // TODO: Implement in Phase 2
        switch (left, right) {
        case (nil, nil):
            return nil
        case (let expr?, nil):
            return expr
        case (nil, let expr?):
            return expr
        case (let l?, let r?):
            return .and(l, r)
        }
    }
}
