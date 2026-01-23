import Foundation

// MARK: - Index Kind

/// All supported index kinds in fdb-cli
public enum IndexKind: String, Codable, Sendable, CaseIterable {
    case scalar
    case vector
    case fulltext
    case spatial
    case rank
    case permuted
    case graph
    case aggregation
    case version
    case bitmap
    case leaderboard
    case relationship
}

// MARK: - Index Definition

/// Definition of an index for a dynamic schema
public struct IndexDefinition: Codable, Sendable, Equatable {
    public let name: String
    public let kind: IndexKind
    public let fields: [String]
    public let unique: Bool
    public let config: IndexConfig?

    public init(
        name: String,
        kind: IndexKind,
        fields: [String],
        unique: Bool = false,
        config: IndexConfig? = nil
    ) {
        self.name = name
        self.kind = kind
        self.fields = fields
        self.unique = unique
        self.config = config
    }

    /// Display string for CLI output
    public var displayString: String {
        var parts: [String] = [name]
        parts.append("(\(kind.rawValue)")
        if unique { parts.append(", unique") }
        if !fields.isEmpty { parts.append(", fields=\(fields.joined(separator: ","))") }
        if let config = config {
            parts.append(", \(config.displayString)")
        }
        return parts.joined() + ")"
    }
}

// MARK: - Index Config

/// Configuration union for different index types
public enum IndexConfig: Codable, Sendable, Equatable {
    case scalar(ScalarIndexConfig)
    case vector(VectorIndexConfig)
    case fulltext(FullTextIndexConfig)
    case spatial(SpatialIndexConfig)
    case rank(RankIndexConfig)
    case permuted(PermutedIndexConfig)
    case graph(GraphIndexConfig)
    case aggregation(AggregationIndexConfig)
    case version(VersionIndexConfig)
    case bitmap(BitmapIndexConfig)
    case leaderboard(LeaderboardIndexConfig)
    case relationship(RelationshipIndexConfig)

    /// Display string for configuration
    public var displayString: String {
        switch self {
        case .scalar(let c): return c.displayString
        case .vector(let c): return c.displayString
        case .fulltext(let c): return c.displayString
        case .spatial(let c): return c.displayString
        case .rank(let c): return c.displayString
        case .permuted(let c): return c.displayString
        case .graph(let c): return c.displayString
        case .aggregation(let c): return c.displayString
        case .version(let c): return c.displayString
        case .bitmap(let c): return c.displayString
        case .leaderboard(let c): return c.displayString
        case .relationship(let c): return c.displayString
        }
    }
}

// MARK: - Scalar Index Config

public struct ScalarIndexConfig: Codable, Sendable, Equatable {
    public let fields: [String]

    public init(fields: [String]) {
        self.fields = fields
    }

    public var displayString: String {
        "fields=\(fields.joined(separator: ","))"
    }
}

// MARK: - Vector Index Config

public enum VectorMetric: String, Codable, Sendable, CaseIterable {
    case cosine
    case euclidean
    case dotProduct
}

public enum VectorAlgorithm: String, Codable, Sendable, CaseIterable {
    case flat
    case hnsw
}

public struct VectorIndexConfig: Codable, Sendable, Equatable {
    public let dimensions: Int
    public let metric: VectorMetric
    public let algorithm: VectorAlgorithm
    public let hnswM: Int?
    public let hnswEfConstruction: Int?

    public init(
        dimensions: Int,
        metric: VectorMetric = .cosine,
        algorithm: VectorAlgorithm = .hnsw,
        hnswM: Int? = nil,
        hnswEfConstruction: Int? = nil
    ) {
        self.dimensions = dimensions
        self.metric = metric
        self.algorithm = algorithm
        self.hnswM = hnswM
        self.hnswEfConstruction = hnswEfConstruction
    }

    public var displayString: String {
        var parts = ["dim=\(dimensions)", "metric=\(metric.rawValue)", "algorithm=\(algorithm.rawValue)"]
        if let m = hnswM { parts.append("m=\(m)") }
        if let ef = hnswEfConstruction { parts.append("ef=\(ef)") }
        return parts.joined(separator: ", ")
    }
}

// MARK: - FullText Index Config

public enum Tokenizer: String, Codable, Sendable, CaseIterable {
    case simple
    case stem
    case ngram
    case keyword
}

public struct FullTextIndexConfig: Codable, Sendable, Equatable {
    public let tokenizer: Tokenizer
    public let ngramK: Int?
    public let storePositions: Bool

    public init(
        tokenizer: Tokenizer = .simple,
        ngramK: Int? = nil,
        storePositions: Bool = false
    ) {
        self.tokenizer = tokenizer
        self.ngramK = ngramK
        self.storePositions = storePositions
    }

    public var displayString: String {
        var parts = ["tokenizer=\(tokenizer.rawValue)"]
        if let k = ngramK { parts.append("k=\(k)") }
        if storePositions { parts.append("positions=true") }
        return parts.joined(separator: ", ")
    }
}

// MARK: - Spatial Index Config

public enum SpatialEncoding: String, Codable, Sendable, CaseIterable {
    case s2
    case morton
}

public struct SpatialIndexConfig: Codable, Sendable, Equatable {
    public let latField: String
    public let lonField: String
    public let altField: String?
    public let encoding: SpatialEncoding
    public let level: Int

    public init(
        latField: String,
        lonField: String,
        altField: String? = nil,
        encoding: SpatialEncoding = .s2,
        level: Int = 16
    ) {
        self.latField = latField
        self.lonField = lonField
        self.altField = altField
        self.encoding = encoding
        self.level = level
    }

    public var displayString: String {
        var parts = ["lat=\(latField)", "lon=\(lonField)"]
        if let alt = altField { parts.append("alt=\(alt)") }
        parts.append("encoding=\(encoding.rawValue)")
        parts.append("level=\(level)")
        return parts.joined(separator: ", ")
    }
}

// MARK: - Rank Index Config

public struct RankIndexConfig: Codable, Sendable, Equatable {
    public let scoreField: String
    public let descending: Bool

    public init(scoreField: String, descending: Bool = true) {
        self.scoreField = scoreField
        self.descending = descending
    }

    public var displayString: String {
        "score=\(scoreField), desc=\(descending)"
    }
}

// MARK: - Permuted Index Config

public struct PermutedIndexConfig: Codable, Sendable, Equatable {
    public let sourceIndex: String
    public let permutation: [Int]

    public init(sourceIndex: String, permutation: [Int]) {
        self.sourceIndex = sourceIndex
        self.permutation = permutation
    }

    public var displayString: String {
        "source=\(sourceIndex), order=\(permutation.map(String.init).joined(separator: ","))"
    }
}

// MARK: - Graph Index Config

public enum GraphStrategy: String, Codable, Sendable, CaseIterable {
    case adjacency
    case tripleStore
    case hexastore
    case knowledgeGraph
}

public struct GraphIndexConfig: Codable, Sendable, Equatable {
    public let fromField: String
    public let toField: String
    public let edgeLabelField: String?
    public let strategy: GraphStrategy

    public init(
        fromField: String,
        toField: String,
        edgeLabelField: String? = nil,
        strategy: GraphStrategy = .adjacency
    ) {
        self.fromField = fromField
        self.toField = toField
        self.edgeLabelField = edgeLabelField
        self.strategy = strategy
    }

    public var displayString: String {
        var parts = ["from=\(fromField)", "to=\(toField)"]
        if let label = edgeLabelField { parts.append("label=\(label)") }
        parts.append("strategy=\(strategy.rawValue)")
        return parts.joined(separator: ", ")
    }
}

// MARK: - Aggregation Index Config

public enum AggregationType: String, Codable, Sendable, CaseIterable {
    case count
    case sum
    case avg
    case min
    case max
    case minmax
    case distinct
    case percentile
}

public struct AggregationIndexConfig: Codable, Sendable, Equatable {
    public let aggregationType: AggregationType
    public let valueField: String?
    public let groupByFields: [String]
    public let percentile: Double?

    public init(
        aggregationType: AggregationType,
        valueField: String? = nil,
        groupByFields: [String],
        percentile: Double? = nil
    ) {
        self.aggregationType = aggregationType
        self.valueField = valueField
        self.groupByFields = groupByFields
        self.percentile = percentile
    }

    public var displayString: String {
        var parts = ["type=\(aggregationType.rawValue)"]
        if let field = valueField { parts.append("field=\(field)") }
        parts.append("by=\(groupByFields.joined(separator: ","))")
        if let p = percentile { parts.append("percentile=\(p)") }
        return parts.joined(separator: ", ")
    }
}

// MARK: - Version Index Config

public enum RetentionPolicy: Codable, Sendable, Equatable {
    case keepAll
    case keepLast(Int)
    case keepForDuration(seconds: Int)

    public var displayString: String {
        switch self {
        case .keepAll: return "keepAll"
        case .keepLast(let n): return "keepLast:\(n)"
        case .keepForDuration(let s): return "keepDays:\(s / 86400)"
        }
    }
}

public struct VersionIndexConfig: Codable, Sendable, Equatable {
    public let retention: RetentionPolicy

    public init(retention: RetentionPolicy = .keepAll) {
        self.retention = retention
    }

    public var displayString: String {
        "retention=\(retention.displayString)"
    }
}

// MARK: - Bitmap Index Config

public struct BitmapIndexConfig: Codable, Sendable, Equatable {
    public let field: String

    public init(field: String) {
        self.field = field
    }

    public var displayString: String {
        "field=\(field)"
    }
}

// MARK: - Leaderboard Index Config

public enum WindowType: String, Codable, Sendable, CaseIterable {
    case hourly
    case daily
    case weekly
    case monthly
}

public struct LeaderboardIndexConfig: Codable, Sendable, Equatable {
    public let scoreField: String
    public let windowType: WindowType
    public let windowCount: Int
    public let groupByFields: [String]

    public init(
        scoreField: String,
        windowType: WindowType = .daily,
        windowCount: Int = 7,
        groupByFields: [String] = []
    ) {
        self.scoreField = scoreField
        self.windowType = windowType
        self.windowCount = windowCount
        self.groupByFields = groupByFields
    }

    public var displayString: String {
        var parts = ["score=\(scoreField)", "window=\(windowType.rawValue)", "count=\(windowCount)"]
        if !groupByFields.isEmpty { parts.append("by=\(groupByFields.joined(separator: ","))") }
        return parts.joined(separator: ", ")
    }
}

// MARK: - Relationship Index Config

public enum DeleteRule: String, Codable, Sendable, CaseIterable {
    case cascade
    case nullify
    case deny
    case noAction
}

public struct RelationshipIndexConfig: Codable, Sendable, Equatable {
    public let foreignKeyField: String
    public let targetSchema: String
    public let deleteRule: DeleteRule

    public init(
        foreignKeyField: String,
        targetSchema: String,
        deleteRule: DeleteRule
    ) {
        self.foreignKeyField = foreignKeyField
        self.targetSchema = targetSchema
        self.deleteRule = deleteRule
    }

    public var displayString: String {
        "fk=\(foreignKeyField), target=\(targetSchema), rule=\(deleteRule.rawValue)"
    }
}
