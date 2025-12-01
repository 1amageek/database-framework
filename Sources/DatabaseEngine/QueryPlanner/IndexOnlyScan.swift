// IndexOnlyScan.swift
// QueryPlanner - Index-only scan (covering index) optimization

import Foundation
import Core

/// Analyzer for index-only scan opportunities
///
/// **Index-Only Scan (Covering Index)**:
/// When an index contains all fields needed by a query, we can satisfy
/// the query entirely from the index without fetching the actual records.
///
/// **Benefits**:
/// - Eliminates record fetch I/O (often the most expensive operation)
/// - Reduces data transfer
/// - Better cache utilization
///
/// **Requirements**:
/// - All projected fields must be in the index
/// - All filter fields must be in the index
/// - All sort fields must be in the index (if sorting is required)
///
/// **Example**:
/// ```swift
/// // Index on (status, createdAt) storing (name)
/// // Query: SELECT name WHERE status = 'active' ORDER BY createdAt
/// // → Can be satisfied entirely from index (no record fetch)
/// ```
public struct IndexOnlyScanAnalyzer<T: Persistable> {

    public init() {}

    /// Analyze if a query can use index-only scan
    public func analyze(
        query: Query<T>,
        analysis: QueryAnalysis<T>,
        index: IndexDescriptor
    ) -> IndexOnlyScanResult {
        // Get fields in the index
        let indexFields = getIndexFields(index)

        // Get all required fields
        let requiredFields = getRequiredFields(query: query, analysis: analysis)

        // Check if all required fields are covered
        let coveredFields = requiredFields.intersection(indexFields)
        let uncoveredFields = requiredFields.subtracting(indexFields)

        let canUseIndexOnlyScan = uncoveredFields.isEmpty

        return IndexOnlyScanResult(
            canUseIndexOnlyScan: canUseIndexOnlyScan,
            index: index,
            coveredFields: coveredFields,
            uncoveredFields: uncoveredFields,
            estimatedSavings: canUseIndexOnlyScan ? estimateSavings(analysis: analysis) : 0
        )
    }

    /// Get all field names in an index
    private func getIndexFields(_ index: IndexDescriptor) -> Set<String> {
        var fields: Set<String> = []

        // Key fields
        for keyPath in index.keyPaths {
            fields.insert(T.fieldName(for: keyPath))
        }

        // Stored fields (if any)
        for storedPath in index.storedKeyPaths {
            fields.insert(T.fieldName(for: storedPath))
        }

        // Primary key is always available (for unique indexes)
        if index.isUnique {
            fields.insert("id")
        }

        return fields
    }

    /// Get all fields required by the query
    private func getRequiredFields(query: Query<T>, analysis: QueryAnalysis<T>) -> Set<String> {
        var required: Set<String> = []

        // Fields from projections (if any)
        if let projection = query.projectedFields {
            required.formUnion(projection)
        } else {
            // If no projection, assume all fields are needed
            // In practice, this should come from schema metadata
            required = analysis.referencedFields
            required.insert("id") // Primary key always needed
        }

        // Fields from conditions (for post-filtering)
        for condition in analysis.fieldConditions {
            required.insert(condition.field.fieldName)
        }

        // Fields from sorting
        for sortDesc in analysis.sortRequirements {
            required.insert(sortDesc.fieldName)
        }

        return required
    }

    /// Estimate cost savings from index-only scan
    private func estimateSavings(analysis: QueryAnalysis<T>) -> Double {
        // Savings = number of record fetches avoided * record fetch cost
        // Typically 80-90% reduction in I/O
        0.85
    }
}

// MARK: - Result

/// Result of index-only scan analysis
public struct IndexOnlyScanResult: Sendable {
    /// Whether index-only scan is possible
    public let canUseIndexOnlyScan: Bool

    /// The index being analyzed
    public let index: IndexDescriptor

    /// Fields covered by the index
    public let coveredFields: Set<String>

    /// Fields not covered (prevents index-only scan)
    public let uncoveredFields: Set<String>

    /// Estimated cost savings (0.0 - 1.0)
    public let estimatedSavings: Double
}

// MARK: - Index-Only Scan Operator

/// Operator for index-only scan execution
public struct IndexOnlyScanOperator<T: Persistable>: @unchecked Sendable {
    /// The index to scan
    public let index: IndexDescriptor

    /// Scan bounds
    public let bounds: IndexScanBounds

    /// Whether to scan in reverse
    public let reverse: Bool

    /// Fields to extract from index
    public let projectedFields: Set<String>

    /// Conditions satisfied by this scan
    public let satisfiedConditions: [FieldCondition<T>]

    /// Estimated matching entries
    public let estimatedEntries: Int

    public init(
        index: IndexDescriptor,
        bounds: IndexScanBounds,
        reverse: Bool = false,
        projectedFields: Set<String>,
        satisfiedConditions: [FieldCondition<T>] = [],
        estimatedEntries: Int
    ) {
        self.index = index
        self.bounds = bounds
        self.reverse = reverse
        self.projectedFields = projectedFields
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
    }
}

// MARK: - Index-Only Scan Plan Creation
//
// Note: Index-only scan plan creation should be integrated into PlanEnumerator directly
// rather than via extension, as it requires access to private members (strategyRegistry, statistics).
//
// Example integration in PlanEnumerator.enumerate():
//
// ```swift
// for index in indexes {
//     let analyzer = IndexOnlyScanAnalyzer<T>()
//     let result = analyzer.analyze(query: query, analysis: analysis, index: index)
//     if result.canUseIndexOnlyScan {
//         // Create IndexOnlyScanOperator with reduced fetch cost
//     }
// }
// ```

// MARK: - Covering Index Suggestion

/// Suggests covering indexes for queries
public struct CoveringIndexSuggester<T: Persistable> {

    public init() {}

    /// Suggest a covering index for a query
    public func suggest(
        query: Query<T>,
        analysis: QueryAnalysis<T>,
        existingIndexes: [IndexDescriptor]
    ) -> CoveringIndexSuggestion? {
        // Get required fields
        var requiredFields: Set<String> = analysis.referencedFields

        // Add sort fields
        for sortDesc in analysis.sortRequirements {
            requiredFields.insert(sortDesc.fieldName)
        }

        // Check if any existing index covers all fields
        for index in existingIndexes {
            let indexFields = getIndexFields(index)
            if indexFields.isSuperset(of: requiredFields) {
                // Already have a covering index
                return nil
            }
        }

        // Find the best index to extend
        var bestCandidate: (index: IndexDescriptor, missingFields: Set<String>)?

        for index in existingIndexes {
            let indexFields = getIndexFields(index)
            let missing = requiredFields.subtracting(indexFields)

            // Check if this index is usable for the query conditions
            let conditionFields = Set(analysis.fieldConditions.map { $0.field.fieldName })
            let indexKeyFields = Set(index.keyPaths.map { T.fieldName(for: $0) })

            // Index must have at least one condition field as key
            guard !conditionFields.isDisjoint(with: indexKeyFields) else { continue }

            if bestCandidate == nil || missing.count < bestCandidate!.missingFields.count {
                bestCandidate = (index, missing)
            }
        }

        guard let candidate = bestCandidate else {
            // Suggest a new index
            return CoveringIndexSuggestion(
                type: .newIndex,
                indexName: nil,
                keyFields: Array(Set(analysis.fieldConditions.map { $0.field.fieldName })),
                storedFields: Array(requiredFields),
                reason: "No existing index can be extended to cover query"
            )
        }

        if candidate.missingFields.isEmpty {
            return nil // Already covered
        }

        return CoveringIndexSuggestion(
            type: .extendExisting,
            indexName: candidate.index.name,
            keyFields: candidate.index.keyPaths.map { T.fieldName(for: $0) },
            storedFields: Array(candidate.missingFields),
            reason: "Add stored fields to make index covering: \(candidate.missingFields.joined(separator: ", "))"
        )
    }

    private func getIndexFields(_ index: IndexDescriptor) -> Set<String> {
        var fields: Set<String> = []
        for keyPath in index.keyPaths {
            fields.insert(T.fieldName(for: keyPath))
        }
        for storedPath in index.storedKeyPaths {
            fields.insert(T.fieldName(for: storedPath))
        }
        return fields
    }
}

/// Suggestion for a covering index
public struct CoveringIndexSuggestion: Sendable {
    public enum SuggestionType: Sendable {
        case newIndex
        case extendExisting
    }

    public let type: SuggestionType
    public let indexName: String?
    public let keyFields: [String]
    public let storedFields: [String]
    public let reason: String
}

// MARK: - IndexDescriptor Extension

extension IndexDescriptor {
    /// Fields stored in the index (for covering index support)
    public var storedKeyPaths: [AnyKeyPath] {
        // Default implementation: no stored fields
        // This would need to be extended in actual IndexDescriptor
        []
    }
}

// MARK: - Cost Model Extension

extension CostModel {
    /// Calculate cost savings from index-only scan
    public func indexOnlySavings(records: Double) -> Double {
        // Savings = avoided record fetches
        records * recordFetchWeight
    }
}

// MARK: - Query Extension

extension Query {
    /// Projected fields (nil means all fields)
    var projectedFields: Set<String>? {
        // Default: nil (all fields)
        // This would need to be added to Query type
        nil
    }
}
