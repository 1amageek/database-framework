import Foundation

/// Parser for CLI schema definition syntax
///
/// Syntax:
/// - Field: `<name>:<type>[#modifier][@relationship]`
/// - Modifiers: `#indexed`, `#unique`, `#vector(...)`, `#fulltext(...)`, `#bitmap`, `#rank`, `#leaderboard(...)`
/// - Relationship: `@relationship(Target,rule)`
/// - Options: `--spatial name(...)`, `--graph name(...)`, `--aggregate name(...)`, `--version name(...)`, `--composite name(...)`, `--permuted name(...)`
public struct SchemaParser {

    /// Result of parsing a schema definition
    public struct ParseResult {
        public let fields: [FieldDefinition]
        public let indexes: [IndexDefinition]
    }

    /// Parse a complete schema definition
    /// - Parameters:
    ///   - name: Schema name
    ///   - args: Field definitions and options
    /// - Returns: Parsed fields and indexes
    public static func parse(schemaName: String, args: [String]) throws -> ParseResult {
        var fields: [FieldDefinition] = []
        var indexes: [IndexDefinition] = []

        var i = 0
        while i < args.count {
            let arg = args[i]

            // Check for compound options (--spatial, --graph, etc.)
            if arg.hasPrefix("--") {
                let option = try parseOption(arg, args: args, index: &i)
                indexes.append(contentsOf: option)
            } else {
                // Parse field definition
                let (field, fieldIndexes) = try parseFieldDefinition(arg)
                fields.append(field)
                indexes.append(contentsOf: fieldIndexes)
            }
            i += 1
        }

        return ParseResult(fields: fields, indexes: indexes)
    }

    // MARK: - Field Parsing

    /// Parse a single field definition with modifiers
    /// Format: `name:type[#modifier...][@relationship]`
    private static func parseFieldDefinition(_ definition: String) throws -> (FieldDefinition, [IndexDefinition]) {
        var indexes: [IndexDefinition] = []

        // Split by @ first (for relationship)
        let atParts = definition.split(separator: "@", maxSplits: 1)
        let fieldPart = String(atParts[0])
        let relationshipPart = atParts.count > 1 ? String(atParts[1]) : nil

        // Split field part by # for modifiers
        let hashParts = fieldPart.split(separator: "#")
        guard !hashParts.isEmpty else {
            throw SchemaParserError.invalidFieldFormat(definition)
        }

        let nameTypePart = String(hashParts[0])
        let modifiers = hashParts.dropFirst().map(String.init)

        // Parse name:type
        let colonParts = nameTypePart.split(separator: ":", maxSplits: 1)
        guard colonParts.count == 2 else {
            throw SchemaParserError.invalidFieldFormat(definition)
        }

        let fieldName = String(colonParts[0])
        let typeString = String(colonParts[1])

        // Parse type (handles optional with ?)
        guard let (fieldType, isOptional) = FieldType.parse(typeString) else {
            throw SchemaParserError.unknownType(typeString)
        }

        let field = FieldDefinition(name: fieldName, type: fieldType, optional: isOptional)

        // Parse modifiers (#indexed, #unique, #vector(...), etc.)
        for modifier in modifiers {
            if let idx = try parseModifier(modifier, fieldName: fieldName, fieldType: fieldType) {
                indexes.append(idx)
            }
        }

        // Parse relationship (@relationship(Target,rule))
        if let rel = relationshipPart {
            let idx = try parseRelationship(rel, fieldName: fieldName)
            indexes.append(idx)
        }

        return (field, indexes)
    }

    // MARK: - Modifier Parsing

    /// Parse a field modifier (#indexed, #unique, #vector(...), etc.)
    private static func parseModifier(_ modifier: String, fieldName: String, fieldType: FieldType) throws -> IndexDefinition? {
        let lowercased = modifier.lowercased()

        // Simple modifiers
        if lowercased == "indexed" {
            return IndexDefinition(
                name: fieldName,
                kind: .scalar,
                fields: [fieldName],
                unique: false,
                config: .scalar(ScalarIndexConfig(fields: [fieldName]))
            )
        }

        if lowercased == "unique" {
            return IndexDefinition(
                name: fieldName,
                kind: .scalar,
                fields: [fieldName],
                unique: true,
                config: .scalar(ScalarIndexConfig(fields: [fieldName]))
            )
        }

        if lowercased == "bitmap" {
            return IndexDefinition(
                name: fieldName,
                kind: .bitmap,
                fields: [fieldName],
                config: .bitmap(BitmapIndexConfig(field: fieldName))
            )
        }

        if lowercased == "rank" {
            return IndexDefinition(
                name: fieldName,
                kind: .rank,
                fields: [fieldName],
                config: .rank(RankIndexConfig(scoreField: fieldName))
            )
        }

        // Parameterized modifiers
        if lowercased.hasPrefix("vector(") {
            return try parseVectorModifier(modifier, fieldName: fieldName)
        }

        if lowercased.hasPrefix("fulltext(") {
            return try parseFullTextModifier(modifier, fieldName: fieldName)
        }

        if lowercased.hasPrefix("leaderboard(") {
            return try parseLeaderboardModifier(modifier, fieldName: fieldName)
        }

        throw SchemaParserError.unknownModifier(modifier)
    }

    /// Parse #vector(dim=N,metric=M,algorithm=A,m=M,ef=E)
    private static func parseVectorModifier(_ modifier: String, fieldName: String) throws -> IndexDefinition {
        let params = try extractParameters(from: modifier)

        guard let dimStr = params["dim"], let dim = Int(dimStr) else {
            throw SchemaParserError.missingParameter("dim", in: modifier)
        }

        let metric: VectorMetric = {
            if let m = params["metric"], let v = VectorMetric(rawValue: m) {
                return v
            }
            return .cosine
        }()

        let algorithm: VectorAlgorithm = {
            if let a = params["algorithm"], let v = VectorAlgorithm(rawValue: a) {
                return v
            }
            return .hnsw
        }()

        let m = params["m"].flatMap(Int.init)
        let ef = params["ef"].flatMap(Int.init)

        return IndexDefinition(
            name: fieldName,
            kind: .vector,
            fields: [fieldName],
            config: .vector(VectorIndexConfig(
                dimensions: dim,
                metric: metric,
                algorithm: algorithm,
                hnswM: m,
                hnswEfConstruction: ef
            ))
        )
    }

    /// Parse #fulltext(tokenizer=T,positions=B,k=N)
    private static func parseFullTextModifier(_ modifier: String, fieldName: String) throws -> IndexDefinition {
        let params = try extractParameters(from: modifier)

        let tokenizer: Tokenizer = {
            if let t = params["tokenizer"], let v = Tokenizer(rawValue: t) {
                return v
            }
            return .simple
        }()

        let positions = params["positions"]?.lowercased() == "true"
        let k = params["k"].flatMap(Int.init)

        return IndexDefinition(
            name: "\(fieldName)_ft",
            kind: .fulltext,
            fields: [fieldName],
            config: .fulltext(FullTextIndexConfig(
                tokenizer: tokenizer,
                ngramK: k,
                storePositions: positions
            ))
        )
    }

    /// Parse #leaderboard(window=W,count=N,by=F)
    private static func parseLeaderboardModifier(_ modifier: String, fieldName: String) throws -> IndexDefinition {
        let params = try extractParameters(from: modifier)

        let window: WindowType = {
            if let w = params["window"], let v = WindowType(rawValue: w) {
                return v
            }
            return .daily
        }()

        let count = params["count"].flatMap(Int.init) ?? 7
        let groupBy = params["by"]?.split(separator: ",").map(String.init) ?? []

        return IndexDefinition(
            name: fieldName,
            kind: .leaderboard,
            fields: [fieldName],
            config: .leaderboard(LeaderboardIndexConfig(
                scoreField: fieldName,
                windowType: window,
                windowCount: count,
                groupByFields: groupBy
            ))
        )
    }

    // MARK: - Relationship Parsing

    /// Parse @relationship(Target,rule)
    private static func parseRelationship(_ rel: String, fieldName: String) throws -> IndexDefinition {
        guard rel.lowercased().hasPrefix("relationship(") else {
            throw SchemaParserError.invalidRelationship(rel)
        }

        // Extract content inside parentheses
        guard let start = rel.firstIndex(of: "("),
              let end = rel.lastIndex(of: ")") else {
            throw SchemaParserError.invalidRelationship(rel)
        }

        let content = String(rel[rel.index(after: start)..<end])
        let parts = content.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }

        guard parts.count >= 2 else {
            throw SchemaParserError.invalidRelationship(rel)
        }

        let targetSchema = parts[0]
        guard let deleteRule = DeleteRule(rawValue: parts[1]) else {
            throw SchemaParserError.unknownDeleteRule(parts[1])
        }

        return IndexDefinition(
            name: "\(fieldName)_rel",
            kind: .relationship,
            fields: [fieldName],
            config: .relationship(RelationshipIndexConfig(
                foreignKeyField: fieldName,
                targetSchema: targetSchema,
                deleteRule: deleteRule
            ))
        )
    }

    // MARK: - Option Parsing

    /// Parse compound options (--spatial, --graph, etc.)
    private static func parseOption(_ option: String, args: [String], index: inout Int) throws -> [IndexDefinition] {
        let lowercased = option.lowercased()

        // Extract option content (may span multiple args if not quoted)
        let optionContent: String
        if option.contains("(") && option.contains(")") {
            optionContent = option
        } else if option.contains("(") {
            // Content spans multiple args - collect until closing paren
            var content = option
            while !content.contains(")") && index + 1 < args.count {
                index += 1
                content += " " + args[index]
            }
            optionContent = content
        } else {
            optionContent = option
        }

        if lowercased.hasPrefix("--spatial") {
            return [try parseSpatialOption(optionContent)]
        }

        if lowercased.hasPrefix("--graph") {
            return [try parseGraphOption(optionContent)]
        }

        if lowercased.hasPrefix("--aggregate") {
            return [try parseAggregateOption(optionContent)]
        }

        if lowercased.hasPrefix("--version") {
            return [try parseVersionOption(optionContent)]
        }

        if lowercased.hasPrefix("--composite") {
            return [try parseCompositeOption(optionContent)]
        }

        if lowercased.hasPrefix("--permuted") {
            return [try parsePermutedOption(optionContent)]
        }

        throw SchemaParserError.unknownOption(option)
    }

    /// Parse --spatial name(fields=lat,lon,encoding=s2,level=16)
    private static func parseSpatialOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--spatial")

        guard let fieldsStr = params["fields"] else {
            throw SchemaParserError.missingParameter("fields", in: option)
        }

        let fieldParts = fieldsStr.split(separator: ",").map(String.init)
        guard fieldParts.count >= 2 else {
            throw SchemaParserError.invalidParameter("fields", in: option)
        }

        let latField = fieldParts[0]
        let lonField = fieldParts[1]
        let altField = fieldParts.count > 2 ? fieldParts[2] : nil

        let encoding: SpatialEncoding = {
            if let e = params["encoding"], let v = SpatialEncoding(rawValue: e) {
                return v
            }
            return .s2
        }()

        let level = params["level"].flatMap(Int.init) ?? 16

        return IndexDefinition(
            name: name,
            kind: .spatial,
            fields: [latField, lonField] + (altField.map { [$0] } ?? []),
            config: .spatial(SpatialIndexConfig(
                latField: latField,
                lonField: lonField,
                altField: altField,
                encoding: encoding,
                level: level
            ))
        )
    }

    /// Parse --graph name(from=F,to=T,label=L,strategy=S)
    private static func parseGraphOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--graph")

        guard let fromField = params["from"] else {
            throw SchemaParserError.missingParameter("from", in: option)
        }

        guard let toField = params["to"] else {
            throw SchemaParserError.missingParameter("to", in: option)
        }

        let labelField = params["label"]

        let strategy: GraphStrategy = {
            if let s = params["strategy"], let v = GraphStrategy(rawValue: s) {
                return v
            }
            return .adjacency
        }()

        return IndexDefinition(
            name: name,
            kind: .graph,
            fields: [fromField, toField] + (labelField.map { [$0] } ?? []),
            config: .graph(GraphIndexConfig(
                fromField: fromField,
                toField: toField,
                edgeLabelField: labelField,
                strategy: strategy
            ))
        )
    }

    /// Parse --aggregate name(type=T,field=F,by=B,percentile=P)
    private static func parseAggregateOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--aggregate")

        guard let typeStr = params["type"], let aggType = AggregationType(rawValue: typeStr) else {
            throw SchemaParserError.missingParameter("type", in: option)
        }

        let valueField = params["field"]
        guard let byStr = params["by"] else {
            throw SchemaParserError.missingParameter("by", in: option)
        }

        let groupByFields = byStr.split(separator: ",").map(String.init)
        let percentile = params["percentile"].flatMap(Double.init)

        var fields = groupByFields
        if let vf = valueField { fields.append(vf) }

        return IndexDefinition(
            name: name,
            kind: .aggregation,
            fields: fields,
            config: .aggregation(AggregationIndexConfig(
                aggregationType: aggType,
                valueField: valueField,
                groupByFields: groupByFields,
                percentile: percentile
            ))
        )
    }

    /// Parse --version name(retention=R)
    private static func parseVersionOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--version")

        let retention: RetentionPolicy = {
            guard let r = params["retention"] else { return .keepAll }

            if r == "keepAll" {
                return .keepAll
            }
            if r.hasPrefix("keepLast:"), let n = Int(r.dropFirst("keepLast:".count)) {
                return .keepLast(n)
            }
            if r.hasPrefix("keepDays:"), let n = Int(r.dropFirst("keepDays:".count)) {
                return .keepForDuration(seconds: n * 86400)
            }
            return .keepAll
        }()

        return IndexDefinition(
            name: name,
            kind: .version,
            fields: [],
            config: .version(VersionIndexConfig(retention: retention))
        )
    }

    /// Parse --composite name(fields=F1,F2,F3)
    private static func parseCompositeOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--composite")

        guard let fieldsStr = params["fields"] else {
            throw SchemaParserError.missingParameter("fields", in: option)
        }

        let fields = fieldsStr.split(separator: ",").map(String.init)

        return IndexDefinition(
            name: name,
            kind: .scalar,
            fields: fields,
            config: .scalar(ScalarIndexConfig(fields: fields))
        )
    }

    /// Parse --permuted name(source=S,order=0,1,2)
    private static func parsePermutedOption(_ option: String) throws -> IndexDefinition {
        let (name, params) = try extractNameAndParameters(from: option, prefix: "--permuted")

        guard let sourceIndex = params["source"] else {
            throw SchemaParserError.missingParameter("source", in: option)
        }

        guard let orderStr = params["order"] else {
            throw SchemaParserError.missingParameter("order", in: option)
        }

        let permutation = orderStr.split(separator: ",").compactMap { Int($0.trimmingCharacters(in: .whitespaces)) }

        return IndexDefinition(
            name: name,
            kind: .permuted,
            fields: [],
            config: .permuted(PermutedIndexConfig(
                sourceIndex: sourceIndex,
                permutation: permutation
            ))
        )
    }

    // MARK: - Utility Methods

    /// Extract key=value parameters from a modifier or option
    private static func extractParameters(from str: String) throws -> [String: String] {
        guard let start = str.firstIndex(of: "("),
              let end = str.lastIndex(of: ")") else {
            return [:]
        }

        let content = String(str[str.index(after: start)..<end])
        return parseKeyValuePairs(content)
    }

    /// Extract name and parameters from option format: --option name(params)
    private static func extractNameAndParameters(from str: String, prefix: String) throws -> (String, [String: String]) {
        // Remove prefix
        let remaining = str.dropFirst(prefix.count).trimmingCharacters(in: .whitespaces)

        // Extract name (before parenthesis)
        guard let parenStart = remaining.firstIndex(of: "(") else {
            throw SchemaParserError.invalidOptionFormat(str)
        }

        let name = String(remaining[..<parenStart]).trimmingCharacters(in: .whitespaces)

        // Extract parameters
        guard let parenEnd = remaining.lastIndex(of: ")") else {
            throw SchemaParserError.invalidOptionFormat(str)
        }

        let paramContent = String(remaining[remaining.index(after: parenStart)..<parenEnd])
        let params = parseKeyValuePairs(paramContent)

        return (name, params)
    }

    /// Parse comma-separated key=value pairs
    private static func parseKeyValuePairs(_ str: String) -> [String: String] {
        var result: [String: String] = [:]

        // Handle nested content (like order=1,2,3) carefully
        var pairs: [String] = []
        var current = ""
        var depth = 0

        for char in str {
            if char == "(" { depth += 1 }
            if char == ")" { depth -= 1 }

            if char == "," && depth == 0 {
                if !current.isEmpty {
                    pairs.append(current.trimmingCharacters(in: .whitespaces))
                }
                current = ""
            } else {
                current.append(char)
            }
        }

        if !current.isEmpty {
            pairs.append(current.trimmingCharacters(in: .whitespaces))
        }

        for pair in pairs {
            let parts = pair.split(separator: "=", maxSplits: 1)
            if parts.count == 2 {
                let key = String(parts[0]).trimmingCharacters(in: .whitespaces)
                let value = String(parts[1]).trimmingCharacters(in: .whitespaces)
                result[key] = value
            }
        }

        return result
    }
}

// MARK: - Errors

public enum SchemaParserError: Error, CustomStringConvertible {
    case invalidFieldFormat(String)
    case unknownType(String)
    case unknownModifier(String)
    case unknownOption(String)
    case unknownDeleteRule(String)
    case invalidRelationship(String)
    case missingParameter(String, in: String)
    case invalidParameter(String, in: String)
    case invalidOptionFormat(String)

    public var description: String {
        switch self {
        case .invalidFieldFormat(let field):
            return "Invalid field format: '\(field)'. Expected 'name:type[#modifier][@relationship]'."
        case .unknownType(let type):
            return "Unknown type: '\(type)'. Supported: string, int, double, bool, date, [string], [double]"
        case .unknownModifier(let modifier):
            return "Unknown modifier: '#\(modifier)'. Supported: indexed, unique, vector(...), fulltext(...), bitmap, rank, leaderboard(...)"
        case .unknownOption(let option):
            return "Unknown option: '\(option)'. Supported: --spatial, --graph, --aggregate, --version, --composite, --permuted"
        case .unknownDeleteRule(let rule):
            return "Unknown delete rule: '\(rule)'. Supported: cascade, nullify, deny, noAction"
        case .invalidRelationship(let rel):
            return "Invalid relationship: '\(rel)'. Expected @relationship(Target,rule)"
        case .missingParameter(let param, let context):
            return "Missing required parameter '\(param)' in '\(context)'"
        case .invalidParameter(let param, let context):
            return "Invalid parameter '\(param)' in '\(context)'"
        case .invalidOptionFormat(let option):
            return "Invalid option format: '\(option)'"
        }
    }
}
