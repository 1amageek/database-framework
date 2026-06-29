// QueryAnalyzer.swift
// QueryPlanner - Query analysis for planning

import Core

/// Analyzes a query to extract structured information for planning
public struct QueryAnalyzer<T: Persistable> {

    /// The predicate normalizer
    private let normalizer = PredicateNormalizer<T>()

    public init() {}

    /// Analyze a query and produce a QueryAnalysis
    public func analyze(_ query: Query<T>) throws -> QueryAnalysis<T> {
        // Combine all predicates
        let combinedPredicate = normalizer.combinePredicates(query.predicates)

        // Normalize to Conjunctive Normal Form for easier planning
        let normalized: QueryCondition<T>
        if let predicate = combinedPredicate {
            let converted = normalizer.convert(predicate)
            normalized = normalizer.toCNF(converted)
        } else {
            normalized = .alwaysTrue
        }

        // Extract all field conditions from the normalized tree
        let conditions = normalized.allFieldConditions

        // Identify field requirements
        let fieldRequirements = extractFieldRequirements(
            conditions: conditions,
            sortDescriptors: query.sortDescriptors
        )

        // Detect special query patterns
        let patterns = detectQueryPatterns(
            conditions: conditions,
            sortDescriptors: query.sortDescriptors,
            limit: query.fetchLimit,
            offset: query.fetchOffset
        )

        // Extract referenced fields
        let referencedFields = extractReferencedFields(
            conditions: conditions,
            sortDescriptors: query.sortDescriptors
        )

        return QueryAnalysis(
            originalPredicate: combinedPredicate,
            normalizedCondition: normalized,
            fieldConditions: conditions,
            fieldRequirements: fieldRequirements,
            sortRequirements: query.sortDescriptors,
            limit: query.fetchLimit,
            offset: query.fetchOffset,
            detectedPatterns: patterns,
            referencedFields: referencedFields
        )
    }

    // MARK: - Field Requirements

    /// Extract requirements for each field
    private func extractFieldRequirements(
        conditions: [any FieldConditionProtocol<T>],
        sortDescriptors: [SortDescriptor<T>]
    ) -> [String: FieldRequirement] {
        var requirements: [String: FieldRequirement] = [:]

        // Process conditions
        for condition in conditions {
            let fieldName = condition.fieldName
            let existing = requirements[fieldName] ?? FieldRequirement(
                fieldName: fieldName,
                accessTypes: [],
                usedInOrdering: false,
                orderDirection: nil
            )

            let accessType = accessTypeForCondition(condition)
            var accessTypes = existing.accessTypes
            accessTypes.insert(accessType)

            requirements[fieldName] = FieldRequirement(
                fieldName: fieldName,
                accessTypes: accessTypes,
                usedInOrdering: existing.usedInOrdering,
                orderDirection: existing.orderDirection
            )
        }

        // Process sort descriptors
        for descriptor in sortDescriptors {
            let fieldName = descriptor.fieldName
            let existing = requirements[fieldName] ?? FieldRequirement(
                fieldName: fieldName,
                accessTypes: [],
                usedInOrdering: false,
                orderDirection: nil
            )

            var accessTypes = existing.accessTypes
            accessTypes.insert(.ordering)

            requirements[fieldName] = FieldRequirement(
                fieldName: fieldName,
                accessTypes: accessTypes,
                usedInOrdering: true,
                orderDirection: descriptor.order
            )
        }

        return requirements
    }

    /// Determine the access type for a condition
    private func accessTypeForCondition(_ condition: any FieldConditionProtocol<T>) -> FieldAccessType {
        if condition.isEquality {
            return .equality
        } else if condition.isRange {
            return .range
        } else if condition.isIn {
            return .membership
        } else if condition.isNullCheck {
            return .equality
        } else if condition is TextSearchFieldCondition<T> {
            return .textSearch
        } else if condition is SpatialFieldCondition<T> {
            return .spatial
        } else if condition is VectorFieldCondition<T> {
            return .vector
        } else if condition is StringPatternFieldCondition<T> {
            return .pattern
        } else {
            return .equality  // Default
        }
    }

    // MARK: - Pattern Detection

    /// Detect special query patterns
    private func detectQueryPatterns(
        conditions: [any FieldConditionProtocol<T>],
        sortDescriptors: [SortDescriptor<T>],
        limit: Int?,
        offset: Int?
    ) -> Set<QueryPattern> {
        var patterns: Set<QueryPattern> = []

        // Point lookup (single equality)
        let equalityConditions = conditions.filter { $0.isEquality }
        if equalityConditions.count == 1 {
            patterns.insert(.pointLookup)
        }

        // Range query
        let rangeConditions = conditions.filter { $0.isRange }
        if !rangeConditions.isEmpty {
            patterns.insert(.rangeQuery)
        }

        // Multi-value lookup (IN)
        let inConditions = conditions.filter { $0.isIn }
        if !inConditions.isEmpty {
            patterns.insert(.multiValueLookup)
        }

        // Full-text search
        let textConditions = conditions.filter { $0 is TextSearchFieldCondition<T> }
        if !textConditions.isEmpty {
            patterns.insert(.fullTextSearch)
        }

        // Vector search
        let vectorConditions = conditions.filter { $0 is VectorFieldCondition<T> }
        if !vectorConditions.isEmpty {
            patterns.insert(.vectorSearch)
        }

        // Spatial query
        let spatialConditions = conditions.filter { $0 is SpatialFieldCondition<T> }
        if !spatialConditions.isEmpty {
            patterns.insert(.spatialQuery)
        }

        // Top-N (ORDER BY with LIMIT)
        if !sortDescriptors.isEmpty && limit != nil {
            patterns.insert(.topN)
        }

        // Pagination (OFFSET present)
        if offset != nil && offset! > 0 {
            patterns.insert(.pagination)
        }

        return patterns
    }

    // MARK: - Field Extraction

    /// Extract all referenced field names
    private func extractReferencedFields(
        conditions: [any FieldConditionProtocol<T>],
        sortDescriptors: [SortDescriptor<T>]
    ) -> Set<String> {
        var fields = Set<String>()

        for condition in conditions {
            fields.insert(condition.fieldName)
        }

        for descriptor in sortDescriptors {
            fields.insert(descriptor.fieldName)
        }

        return fields
    }
}

// MARK: - Query Analysis Result

/// Result of query analysis
public struct QueryAnalysis<T: Persistable>: @unchecked Sendable {
    /// Original combined predicate
    public let originalPredicate: Predicate<T>?

    /// Normalized condition tree
    public let normalizedCondition: QueryCondition<T>

    /// All field conditions (flattened)
    public let fieldConditions: [any FieldConditionProtocol<T>]

    /// Requirements per field
    public let fieldRequirements: [String: FieldRequirement]

    /// Sort requirements
    public let sortRequirements: [SortDescriptor<T>]

    /// Limit
    public let limit: Int?

    /// Offset
    public let offset: Int?

    /// Detected query patterns
    public let detectedPatterns: Set<QueryPattern>

    /// All referenced field names
    public let referencedFields: Set<String>

    public init(
        originalPredicate: Predicate<T>?,
        normalizedCondition: QueryCondition<T>,
        fieldConditions: [any FieldConditionProtocol<T>],
        fieldRequirements: [String: FieldRequirement],
        sortRequirements: [SortDescriptor<T>],
        limit: Int?,
        offset: Int?,
        detectedPatterns: Set<QueryPattern>,
        referencedFields: Set<String>
    ) {
        self.originalPredicate = originalPredicate
        self.normalizedCondition = normalizedCondition
        self.fieldConditions = fieldConditions
        self.fieldRequirements = fieldRequirements
        self.sortRequirements = sortRequirements
        self.limit = limit
        self.offset = offset
        self.detectedPatterns = detectedPatterns
        self.referencedFields = referencedFields
    }

    /// Check if query has any filter conditions
    public var hasFilterConditions: Bool {
        !fieldConditions.isEmpty
    }

    /// Check if query has sort requirements
    public var hasSortRequirements: Bool {
        !sortRequirements.isEmpty
    }

    /// Check if this is a simple point lookup
    public var isPointLookup: Bool {
        detectedPatterns.contains(.pointLookup)
    }

    /// Get equality conditions only
    public var equalityConditions: [any FieldConditionProtocol<T>] {
        fieldConditions.filter { $0.isEquality }
    }

    /// Get range conditions only
    public var rangeConditions: [any FieldConditionProtocol<T>] {
        fieldConditions.filter { $0.isRange }
    }

    /// Get IN conditions only
    public var inConditions: [any FieldConditionProtocol<T>] {
        fieldConditions.filter { $0.isIn }
    }
}

// MARK: - Query Pattern

/// Detected query patterns that may influence planning
public enum QueryPattern: Sendable, Hashable {
    /// Single equality condition (point lookup)
    case pointLookup

    /// Range query on ordered field
    case rangeQuery

    /// Multiple IN conditions (multi-seek)
    case multiValueLookup

    /// Full-text search present
    case fullTextSearch

    /// Vector similarity search present
    case vectorSearch

    /// Spatial query present
    case spatialQuery

    /// Aggregation query (COUNT, SUM, etc.)
    case aggregation(AggregationType)

    /// Top-N query (ORDER BY with LIMIT)
    case topN

    /// Pagination query (OFFSET present)
    case pagination

    public func hash(into hasher: inout Hasher) {
        switch self {
        case .pointLookup: hasher.combine(0)
        case .rangeQuery: hasher.combine(1)
        case .multiValueLookup: hasher.combine(2)
        case .fullTextSearch: hasher.combine(3)
        case .vectorSearch: hasher.combine(4)
        case .spatialQuery: hasher.combine(5)
        case .aggregation(let type):
            hasher.combine(6)
            hasher.combine(type)
        case .topN: hasher.combine(7)
        case .pagination: hasher.combine(8)
        }
    }

    public static func == (lhs: QueryPattern, rhs: QueryPattern) -> Bool {
        switch (lhs, rhs) {
        case (.pointLookup, .pointLookup),
             (.rangeQuery, .rangeQuery),
             (.multiValueLookup, .multiValueLookup),
             (.fullTextSearch, .fullTextSearch),
             (.vectorSearch, .vectorSearch),
             (.spatialQuery, .spatialQuery),
             (.topN, .topN),
             (.pagination, .pagination):
            return true
        case (.aggregation(let lhsType), .aggregation(let rhsType)):
            return lhsType == rhsType
        default:
            return false
        }
    }
}
