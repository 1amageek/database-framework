/// SQLParser.swift
/// SQL Query Parser
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import DatabaseKit

/// SQL Parser for converting SQL strings to AST
public final class SQLParser {
    /// Parser errors
    public enum ParseError: Error, Sendable, Equatable {
        case unexpectedToken(expected: String, found: String, position: Int)
        case unexpectedEndOfInput(expected: String, position: Int)
        case invalidSyntax(message: String, position: Int)
        case unsupportedFeature(String)
        case literalError(message: String, position: Int)
    }

    /// Token types
    private enum Token: Sendable, Equatable {
        case keyword(String)
        case identifier(String)
        case string(String)
        case number(String)
        case parameter(QueryParameterReference)
        case invalidParameter(String)
        case symbol(String)
        case eof

        var isString: Bool {
            if case .string = self { return true }
            return false
        }
    }

    private enum PositionalParameterStyle: Sendable {
        case anonymous
        case explicit
    }

    private enum EdgeEndpointRole: String, Sendable {
        case source
        case destination

        var displayName: String {
            switch self {
            case .source: "Source"
            case .destination: "Destination"
            }
        }
    }

    private static let collectionOperators: [String: CollectionOperator] = [
        "@>": .contains, "<@": .containedBy, "&&": .overlaps,
        "->": .jsonField, "->>": .jsonFieldText, "#>": .jsonPath,
        "#>>": .jsonPathText, "?": .jsonHasKey, "?|": .jsonHasAnyKey,
        "?&": .jsonHasAllKeys, "#-": .jsonDeletePath,
    ]

    private static let keywords: Set<String> = [
        "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
        "IS", "NULL", "TRUE", "FALSE", "AS", "JOIN", "INNER", "LEFT", "RIGHT",
        "FULL", "CROSS", "ON", "USING", "GROUP", "BY", "HAVING", "ORDER", "ASC",
        "DESC", "LIMIT", "OFFSET", "DISTINCT", "ALL", "UNION", "INTERSECT",
        "EXCEPT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
        "CREATE", "DROP", "TABLE", "INDEX", "GRAPH", "PROPERTY", "MATCH",
        "WITH", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "COUNT",
        "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "GROUP_CONCAT",
        "EXISTS", "ANY", "SOME", "NULLS", "FIRST", "LAST", "OVER", "PARTITION",
        "ROWS", "RANGE", "GRAPH_TABLE", "COLUMNS", "WALK", "TRAIL", "ACYCLIC",
        "SIMPLE", "SHORTEST", "DEFAULT", "CONFLICT", "DO", "NOTHING", "RETURNING",
        "IF", "VERTEX", "TABLES", "EDGE", "KEY", "LABEL", "DESTINATION",
        "REFERENCES", "PROPERTIES", "NO", "LATERAL", "NATURAL",
    ]

    // PostgreSQL 18 lexical categories: src/include/parser/kwlist.h.
    // Grammar contexts distinguish ColId, type_function_name and ColLabel.
    private static let reservedWords: Set<String> = [
        "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS",
        "ASC", "ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK", "COLLATE",
        "COLUMN", "CONSTRAINT", "CREATE", "CURRENT_CATALOG", "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME",
        "CURRENT_TIMESTAMP", "CURRENT_USER", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO",
        "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN",
        "FROM", "GRANT", "GROUP", "HAVING", "IN", "INITIALLY", "INTERSECT",
        "INTO", "LATERAL", "LEADING", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "NOT",
        "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "PLACING",
        "PRIMARY", "REFERENCES", "RETURNING", "SELECT", "SESSION_USER", "SOME", "SYMMETRIC",
        "SYSTEM_USER", "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION",
        "UNIQUE", "USER", "USING", "VARIADIC", "WHEN", "WHERE", "WINDOW",
        "WITH",
    ]

    private static let typeFunctionWords: Set<String> = [
        "AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY", "CROSS", "CURRENT_SCHEMA", "FREEZE",
        "FULL", "ILIKE", "INNER", "IS", "ISNULL", "JOIN", "LEFT",
        "LIKE", "NATURAL", "NOTNULL", "OUTER", "OVERLAPS", "RIGHT", "SIMILAR",
        "TABLESAMPLE", "VERBOSE",
    ]

    private static let columnNameWords: Set<String> = [
        "BETWEEN", "BIGINT", "BIT", "BOOLEAN", "CHAR", "CHARACTER", "COALESCE",
        "DEC", "DECIMAL", "EXISTS", "EXTRACT", "FLOAT", "GREATEST", "GROUPING",
        "INOUT", "INT", "INTEGER", "INTERVAL", "JSON", "JSON_ARRAY", "JSON_ARRAYAGG",
        "JSON_EXISTS", "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE", "JSON_TABLE",
        "JSON_VALUE", "LEAST", "MERGE_ACTION", "NATIONAL", "NCHAR", "NONE", "NORMALIZE",
        "NULLIF", "NUMERIC", "OUT", "OVERLAY", "POSITION", "PRECISION", "REAL",
        "ROW", "SETOF", "SMALLINT", "SUBSTRING", "TIME", "TIMESTAMP", "TREAT",
        "TRIM", "VALUES", "VARCHAR", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT", "XMLEXISTS",
        "XMLFOREST", "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE", "XMLTABLE",
    ]

    private var position: Int
    private var sourceBytes: [UInt8] = []
    private var tokenStart = 0
    private var tokenEnd = 0
    private var previousTokenEnd = 0
    private var capturesSource = false
    private var preservesOutputReferenceCasts = false
    private var colonStartsSlice = false
    private var sourceCapture: SQLSourceCaptureCollector?
    private var sourceRoots: [QueryOccurrenceID] = []
    private var tokenIsQuoted = false
    private var currentToken: Token
    private var anonymousParameterPosition: UInt32
    private var positionalParameterStyle: PositionalParameterStyle?
    private let structuralLimits: QueryStructuralLimits
    private var structuralLedger: QueryStructuralResourceLedger
    private var structuralError: QueryStructuralValidationError?
    private var lexicalError: ParseError?

    public init(
        structuralLimits: QueryStructuralLimits = .default
    ) {
        self.position = 0
        self.currentToken = .eof
        self.anonymousParameterPosition = 0
        self.positionalParameterStyle = nil
        self.structuralLimits = structuralLimits
        self.structuralLedger = QueryStructuralResourceLedger(
            limits: structuralLimits
        )
        self.structuralError = nil
        self.lexicalError = nil
    }

    /// Parse a SQL SELECT query
    public func parseSelect(_ sql: String) throws -> SelectQuery {
        try resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            let query = try parseSelectQuery()
            try consumeStatementTerminator()
            try QueryStructuralValidator.validate(
                query,
                limits: structuralLimits
            )
            return query
        }
    }

    /// Parse any SQL statement
    public func parse(_ sql: String) throws -> QueryStatement {
        try resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            let statement = try parseStatement()
            try consumeStatementTerminator()
            try QueryStructuralValidator.validate(
                statement,
                limits: structuralLimits
            )
            return statement
        }
    }

    /// Parse a bounded, ordered collection of complete statements.
    public func parseStatements(_ sql: String) throws -> [QueryStatement] {
        try resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            var statements: [QueryStatement] = []
            while currentToken != .eof {
                if isSymbol(";") { advance(); continue }
                try admitCollectionElement()
                let statementStart = tokenStart
                let statement = try parseStatement()
                try QueryStructuralValidator.validate(statement, limits: structuralLimits)
                if sourceCapture != nil {
                    let root = try sourceCapture!.finishStatement(index: UInt32(statements.count), statement: statement,
                                                                 start: statementStart, end: previousTokenEnd,
                                                                 ledger: &structuralLedger)
                    sourceRoots.append(root)
                }
                statements.append(statement)
                guard currentToken == .eof || isSymbol(";") else {
                    throw ParseError.unexpectedToken(expected: "statement separator", found: tokenDescription(currentToken), position: tokenStart)
                }
            }
            return statements
        }
    }

    /// Parse SELECT with a bounded semantic occurrence map and UTF-8 source ranges.
    public func parseSelectWithSource(_ sql: String) throws -> SQLParsedSelect {
        capturesSource = true
        defer { capturesSource = false }
        let query = try parseSelect(sql)
        let root = try sourceCapture!.finishStatement(index: 0, ledger: &structuralLedger)
        let snapshot = sourceCapture!.snapshot
        return SQLParsedSelect(query: query, occurrences: snapshot.occurrences,
                               root: root, sourceMap: snapshot.sourceMap)
    }

    /// Parse one statement with source correspondence independent of parser reuse.
    public func parseWithSource(_ sql: String) throws -> SQLParsedStatement {
        capturesSource = true
        defer { capturesSource = false }
        let statement = try parse(sql)
        let root = try sourceCapture!.finishStatement(index: 0, statement: statement,
                                                     start: 0, end: previousTokenEnd,
                                                     ledger: &structuralLedger)
        let snapshot = sourceCapture!.snapshot
        return SQLParsedStatement(statement: statement, occurrences: snapshot.occurrences,
                                  root: root, sourceMap: snapshot.sourceMap)
    }

    /// Parse a batch while retaining source offsets relative to the complete input.
    public func parseStatementsWithSource(_ sql: String) throws -> SQLParsedStatements {
        capturesSource = true
        defer { capturesSource = false }
        let statements = try parseStatements(sql)
        let snapshot = sourceCapture!.snapshot
        return SQLParsedStatements(statements: statements, occurrences: snapshot.occurrences,
                                   roots: sourceRoots, sourceMap: snapshot.sourceMap)
    }

    private func resetParserState(for sql: String) throws {
        sourceBytes = []
        position = 0
        tokenStart = 0
        tokenEnd = 0
        previousTokenEnd = 0
        sourceCapture = capturesSource ? SQLSourceCaptureCollector(limits: structuralLimits) : nil
        sourceRoots = []
        preservesOutputReferenceCasts = false
        colonStartsSlice = false
        tokenIsQuoted = false
        currentToken = .eof
        anonymousParameterPosition = 0
        positionalParameterStyle = nil
        structuralLedger = QueryStructuralResourceLedger(limits: structuralLimits)
        structuralError = nil
        lexicalError = nil
        var sourceCount: UInt64 = 0
        for _ in sql.utf8 {
            sourceCount += 1
            guard sourceCount <= structuralLimits.maximumRetainedBytes else {
                throw QueryStructuralValidationError.resourceLimitExceeded(resource: .retainedBytes, actual: sourceCount, maximum: structuralLimits.maximumRetainedBytes)
            }
        }
        sourceBytes = Array(sql.utf8)
    }

    private func withStructuralErrorPrecedence<Result>(
        _ operation: () throws -> Result
    ) throws -> Result {
        do {
            let result = try operation()
            if let structuralError {
                throw structuralError
            }
            if let lexicalError {
                throw lexicalError
            }
            return result
        } catch {
            if let structuralError {
                throw structuralError
            }
            if let lexicalError {
                throw lexicalError
            }
            throw error
        }
    }

    private func enterStructuralNesting() throws {
        try structuralLedger.enterNesting()
    }

    private func leaveStructuralNesting() {
        do {
            try structuralLedger.leaveNesting()
        } catch {
            if structuralError == nil {
                structuralError = error
            }
        }
    }

    private func makeStructuralNode<Value>(
        _ value: @autoclosure () -> Value,
        at start: Int? = nil,
        identifierEnd: Int? = nil
    ) throws -> Value {
        try structuralLedger.consume(.totalNodes)
        return value()
    }

    private func makeStructuralNode(
        _ value: @autoclosure () -> Expression,
        at start: Int? = nil,
        identifierEnd: Int? = nil,
        end: Int? = nil
    ) throws -> Expression {
        try structuralLedger.consume(.totalNodes)
        let result = value()
        try sourceCapture?.record(result, start: start, end: end ?? previousTokenEnd,
                                  identifierEnd: identifierEnd, ledger: &structuralLedger)
        return result
    }

    private func makeStructuralNode(
        _ value: @autoclosure () -> SelectQuery,
        at start: Int? = nil,
        identifierEnd: Int? = nil
    ) throws -> SelectQuery {
        try structuralLedger.consume(.totalNodes)
        let result = value()
        try sourceCapture?.record(result, start: start, end: previousTokenEnd,
                                  identifierEnd: identifierEnd, ledger: &structuralLedger)
        return result
    }

    private func makeStructuralNode(
        _ value: @autoclosure () -> DataSource,
        at start: Int? = nil,
        identifierEnd: Int? = nil
    ) throws -> DataSource {
        try structuralLedger.consume(.totalNodes)
        let result = value()
        try sourceCapture?.record(result, start: start, end: previousTokenEnd,
                                  identifierEnd: identifierEnd, ledger: &structuralLedger)
        return result
    }

    private func makeStructuralNode(
        _ value: @autoclosure () -> DataType,
        at start: Int? = nil,
        identifierEnd: Int? = nil
    ) throws -> DataType {
        try structuralLedger.consume(.totalNodes)
        let result = value()
        try sourceCapture?.record(result, start: start, end: previousTokenEnd,
                                  identifierEnd: identifierEnd, ledger: &structuralLedger)
        return result
    }

    private func makeLiteralExpression(
        _ literal: Literal,
        at start: Int? = nil
    ) throws -> Expression {
        if case .queryValue(let value, let type) = literal {
            let footprint = try QueryValueValidator.measure(value)
            var nodes = footprint.nodeCount
            if case .primitive = value { nodes += 1 }
            if case .sql(let dataType) = type { nodes += 1 + dataTypeNodeCount(dataType) }
            try structuralLedger.consume(.totalNodes, amount: nodes)
            try structuralLedger.consume(.collectionElements, amount: footprint.collectionElementCount)
            try structuralLedger.consume(.retainedBytes, amount: footprint.retainedByteCount)
            let remainingDepth = structuralLedger.remainingLimits.maximumNestingDepth
            let currentDepth = structuralLimits.maximumNestingDepth - remainingDepth
            try structuralLedger.validateNestingDepth(currentDepth + 2 + footprint.maximumNestingDepth)
        }
        let admittedLiteral = try makeStructuralNode(literal)
        return try makeStructuralNode(.literal(admittedLiteral), at: start)
    }

    private func dataTypeNodeCount(_ type: DataType) -> UInt64 {
        var count: UInt64 = 1
        var current = type
        while case .array(let element) = current { count += 1; current = element }
        return count
    }

    private func constantLimits(reservingNodes reserved: UInt64) throws -> QueryStructuralLimits {
        let available = structuralLedger.remainingLimits
        return QueryStructuralLimits(
            maximumNestingDepth: available.maximumNestingDepth > 2 ? available.maximumNestingDepth - 2 : 0,
            maximumRetainedBytes: available.maximumRetainedBytes,
            maximumInputTokens: available.maximumInputTokens,
            maximumTotalNodes: available.maximumTotalNodes > reserved ? available.maximumTotalNodes - reserved : 0,
            maximumCollectionElements: available.maximumCollectionElements,
            maximumBasicGraphPatterns: available.maximumBasicGraphPatterns,
            maximumTriplePatterns: available.maximumTriplePatterns,
            maximumValuesRows: available.maximumValuesRows,
            maximumValuesVariables: available.maximumValuesVariables,
            maximumValuesCells: available.maximumValuesCells,
            maximumReifiedTripleExpansions: available.maximumReifiedTripleExpansions
        )
    }

    private func makeAggregateExpression(
        _ aggregate: AggregateFunction,
        at start: Int? = nil,
        identifierEnd: Int? = nil
    ) throws -> Expression {
        let admittedAggregate = try makeStructuralNode(aggregate)
        return try makeStructuralNode(.aggregate(admittedAggregate), at: start, identifierEnd: identifierEnd)
    }

    private func admitCollectionElement(
        amount: UInt64 = 1
    ) throws {
        try structuralLedger.consume(.collectionElements, amount: amount)
    }
}

// MARK: - Tokenizer

extension SQLParser {
    private func admitToken(_ token: Token, quoted: Bool = false) {
        guard structuralError == nil, lexicalError == nil else {
            currentToken = .eof
            return
        }
        currentToken = token
        tokenEnd = position
        tokenIsQuoted = quoted
    }

    private func asciiUppercase(_ value: String) -> String {
        String(decoding: value.utf8.map { (97...122).contains($0) ? $0 - 32 : $0 }, as: UTF8.self)
    }

    private func byte(_ offset: Int = 0) -> UInt8? {
        let index = position + offset
        return index < sourceBytes.count ? sourceBytes[index] : nil
    }

    private func isDigit(_ value: UInt8?) -> Bool {
        guard let value else { return false }
        return value >= 48 && value <= 57
    }

    private func isNameStart(_ value: UInt8?) -> Bool {
        guard let value else { return false }
        return value >= 128 || value == 95 || (65...90).contains(value) || (97...122).contains(value)
    }

    private func isNameContinuation(_ value: UInt8?) -> Bool {
        isNameStart(value) || isDigit(value) || value == 36
    }

    private func decoded(_ range: Range<Int>) throws -> String {
        try structuralLedger.consume(.retainedBytes, amount: UInt64(range.count))
        return String(decoding: sourceBytes[range], as: UTF8.self)
    }

    private func appendDecoded(_ bytes: some Collection<UInt8>, to output: inout [UInt8]) throws {
        try structuralLedger.consume(.retainedBytes, amount: UInt64(bytes.count))
        output.append(contentsOf: bytes)
    }

    private func appendScalar(_ value: UInt32, to output: inout [UInt8]) throws {
        guard value != 0, let scalar = Unicode.Scalar(value) else {
            throw ParseError.invalidSyntax(message: "Invalid Unicode escape", position: tokenStart)
        }
        try appendDecoded(String(scalar).utf8, to: &output)
    }

    private func advance() {
        previousTokenEnd = tokenEnd
        guard structuralError == nil, lexicalError == nil else { currentToken = .eof; return }
        do {
            _ = try skipWhitespace()
            tokenStart = position
            tokenEnd = position
            tokenIsQuoted = false
            guard let value = byte() else { currentToken = .eof; return }
            try structuralLedger.consume(.inputTokens)
            guard value != 0 else {
                throw ParseError.invalidSyntax(message: "U+0000 is not accepted in SQL input", position: tokenStart)
            }
            if value == 36 {
                let opening = position
                position += 1
                if isDigit(byte()) {
                    let start = position
                    while isDigit(byte()) { position += 1 }
                    let text = try decoded(start..<position)
                    guard !isNameContinuation(byte()), let number = UInt32(text), number > 0 else {
                        admitToken(.invalidParameter("Invalid positional parameter")); return
                    }
                    guard positionalParameterStyle != .anonymous else {
                        admitToken(.invalidParameter("Cannot mix '?' and '$n' positional parameters")); return
                    }
                    positionalParameterStyle = .explicit
                    admitToken(.parameter(.position(number))); return
                }
                if isNameStart(byte()) {
                    while isNameStart(byte()) || isDigit(byte()) { position += 1 }
                }
                guard byte() == 36 else {
                    throw ParseError.invalidSyntax(message: "Invalid dollar delimiter", position: opening)
                }
                position += 1
                let delimiter = sourceBytes[opening..<position]
                let body = position
                while position < sourceBytes.count {
                    var matched = 0
                    if byte() == 36 {
                        while matched < delimiter.count, position + matched < sourceBytes.count,
                              sourceBytes[position + matched] == delimiter[delimiter.startIndex + matched] { matched += 1 }
                    }
                    if matched == delimiter.count {
                        let text = try decoded(body..<position)
                        guard !text.utf8.contains(0) else {
                            throw ParseError.invalidSyntax(message: "U+0000 in dollar string", position: opening)
                        }
                        position += delimiter.count
                        admitToken(.string(text)); return
                    }
                    // A delimiter tag contains no '$', so its matched prefix can be skipped.
                    position += max(1, matched)
                }
                throw ParseError.invalidSyntax(message: "Unterminated dollar string", position: opening)
            }
            if value == 39 || value == 34 || ((value == 69 || value == 101) && byte(1) == 39)
                || ((value == 85 || value == 117) && byte(1) == 38 && (byte(2) == 39 || byte(2) == 34)) {
                let escapeString = (value == 69 || value == 101)
                let unicode = value == 85 || value == 117
                if escapeString { position += 1 }
                if unicode { position += 2 }
                let quote = byte()!
                var output: [UInt8] = []
                try scanQuoted(quote: quote, escapeString: escapeString, output: &output)
                var spanEnd = position
                if quote == 39 {
                    while true {
                        let hadNewline = try skipWhitespace(includeBlockComments: false)
                        guard hadNewline, byte() == 39 else { break }
                        try scanQuoted(quote: 39, escapeString: escapeString, output: &output)
                        spanEnd = position
                    }
                }
                if unicode {
                    let beforeEscape = position
                    let escape = try scanUnicodeEscapeCharacter()
                    var decodedOutput: [UInt8] = []
                    try decodeUnicode(output, escape: escape, output: &decodedOutput)
                    output = decodedOutput
                    if position != beforeEscape {
                        spanEnd = position
                    }
                }
                guard let text = String(validating: output, as: UTF8.self), !output.contains(0), quote != 34 || !output.isEmpty else {
                    throw ParseError.invalidSyntax(message: "Invalid UTF-8, empty identifier or U+0000 in quoted value", position: tokenStart)
                }
                admitToken(quote == 34 ? .identifier(text) : .string(text), quoted: quote == 34)
                tokenEnd = spanEnd
                return
            }
            if isNameStart(value) {
                let start = position
                while isNameContinuation(byte()) { position += 1 }
                let word = try decoded(start..<position)
                let upper = asciiUppercase(word)
                let folded = String(decoding: word.utf8.map { (65...90).contains($0) ? $0 + 32 : $0 }, as: UTF8.self)
                admitToken(Self.keywords.contains(upper) || Self.reservedWords.contains(upper) || Self.typeFunctionWords.contains(upper) ? .keyword(upper) : .identifier(folded)); return
            }
            if isDigit(value) || value == 46 && isDigit(byte(1)) {
                let start = position
                let radix = value == 48 && [66, 79, 88, 98, 111, 120].contains(byte(1) ?? 0)
                if radix {
                    position += 2
                    while let digit = byte(), isDigit(digit) || (65...70).contains(digit) || (97...102).contains(digit) || digit == 95 { position += 1 }
                } else {
                    while isDigit(byte()) || byte() == 95 { position += 1 }
                    if byte() == 46 {
                        position += 1
                        while isDigit(byte()) || byte() == 95 { position += 1 }
                    }
                    if byte() == 69 || byte() == 101 {
                        position += 1
                        if byte() == 43 || byte() == 45 { position += 1 }
                        while isDigit(byte()) || byte() == 95 { position += 1 }
                    }
                }
                guard !isNameStart(byte()) else {
                    throw ParseError.invalidSyntax(message: "Invalid numeric suffix", position: start)
                }
                admitToken(.number(try decoded(start..<position))); return
            }
            if (value == 33 && byte(1) == 126 && byte(2) == 42)
                || (value == 45 && byte(1) == 62 && byte(2) == 62)
                || (value == 35 && byte(1) == 62 && byte(2) == 62) {
                let symbol = String(decoding: sourceBytes[position..<position + 3], as: UTF8.self)
                position += 3; admitToken(.symbol(symbol)); return
            }
            if let next = byte(1) {
                let pair = String(decoding: [value, next], as: UTF8.self)
                if ["<=", ">=", "<>", "!=", "||", "&&", "->", "<-", "::", "~*", "!~", "=>", ":=", "@>", "<@", "#>", "?|", "?&", "#-"].contains(pair) {
                    position += 2; admitToken(.symbol(pair)); return
                }
            }
            position += 1
            admitToken(.symbol(String(decoding: [value], as: UTF8.self)))
        } catch let error as QueryStructuralValidationError {
            structuralError = error
            currentToken = .eof
        } catch let error as ParseError {
            lexicalError = error
            currentToken = .eof
        } catch {
            lexicalError = .invalidSyntax(message: "Invalid lexical input", position: tokenStart)
            currentToken = .eof
        }
    }

    private func scanQuoted(quote: UInt8, escapeString: Bool, output: inout [UInt8]) throws {
        let opening = tokenStart
        position += 1
        // All adjacent raw chunks are collected before Unicode UESCAPE decoding.
        var raw: [UInt8] = []
        while let value = byte() {
            if value == quote {
                position += 1
                if byte() == quote {
                    position += 1
                    try appendDecoded([quote], to: &raw)
                } else {
                    // Raw content was already admitted before growth.
                    output.append(contentsOf: raw)
                    return
                }
            } else if escapeString, value == 92 {
                position += 1
                guard let escaped = byte() else { break }
                position += 1
                switch escaped {
                case 98: try appendDecoded([8], to: &raw)
                case 102: try appendDecoded([12], to: &raw)
                case 110: try appendDecoded([10], to: &raw)
                case 114: try appendDecoded([13], to: &raw)
                case 116: try appendDecoded([9], to: &raw)
                case 48...55:
                    var number = UInt32(escaped - 48)
                    for _ in 0..<2 {
                        guard let digit = byte(), (48...55).contains(digit) else { break }
                        number = number * 8 + UInt32(digit - 48); position += 1
                    }
                    try appendDecoded([UInt8(truncatingIfNeeded: number)], to: &raw)
                case 120:
                    var number: UInt32 = 0
                    var count = 0
                    while count < 2, let digit = byte(), isHexDigit(digit) {
                        number = number * 16 + hexValue(digit); position += 1; count += 1
                    }
                    try appendDecoded([count == 0 ? escaped : UInt8(number)], to: &raw)
                case 117, 85:
                    var scalar = try scanHex(count: escaped == 117 ? 4 : 8)
                    if (0xD800...0xDBFF).contains(scalar) {
                        guard byte() == 92, byte(1) == 117 || byte(1) == 85 else {
                            throw ParseError.invalidSyntax(message: "Unpaired Unicode surrogate", position: opening)
                        }
                        let lowWidth = byte(1) == 117 ? 4 : 8
                        position += 2
                        let low = try scanHex(count: lowWidth)
                        guard (0xDC00...0xDFFF).contains(low) else {
                            throw ParseError.invalidSyntax(message: "Unpaired Unicode surrogate", position: opening)
                        }
                        scalar = 0x10000 + ((scalar - 0xD800) << 10) + low - 0xDC00
                    }
                    try appendScalar(scalar, to: &raw)
                default: try appendDecoded([escaped], to: &raw)
                }
            } else {
                guard value != 0 else { throw ParseError.invalidSyntax(message: "U+0000 in quoted value", position: opening) }
                position += 1
                try appendDecoded([value], to: &raw)
            }
        }
        throw ParseError.invalidSyntax(message: "Unterminated quoted value", position: opening)
    }

    private func scanUnicodeEscapeCharacter() throws -> UInt8 {
        let afterQuote = position
        _ = try skipWhitespace()
        guard sourceBytes.count - position >= 7,
              asciiUppercase(String(decoding: sourceBytes[position..<(position + 7)], as: UTF8.self)) == "UESCAPE",
              position + 7 == sourceBytes.count || !isNameContinuation(sourceBytes[position + 7]) else {
            position = afterQuote
            return 92
        }
        try structuralLedger.consume(.inputTokens, amount: 2)
        position += 7
        _ = try skipWhitespace()
        guard byte() == 39, let candidate = byte(1), byte(2) == 39,
              candidate < 128, candidate != 43, candidate != 34, candidate != 39,
              !isHexDigit(candidate), ![9, 10, 11, 12, 13, 32].contains(candidate) else {
            throw ParseError.invalidSyntax(message: "Invalid UESCAPE character", position: position)
        }
        position += 3
        return candidate
    }

    private func isHexDigit(_ value: UInt8) -> Bool {
        (48...57).contains(value) || (65...70).contains(value) || (97...102).contains(value)
    }

    private func hexValue(_ value: UInt8) -> UInt32 {
        UInt32(value <= 57 ? value - 48 : (value <= 70 ? value - 65 : value - 97) + 10)
    }

    private func scanHex(count: Int) throws -> UInt32 {
        var value: UInt32 = 0
        for _ in 0..<count {
            guard let digit = byte(), isHexDigit(digit) else {
                throw ParseError.invalidSyntax(message: "Invalid Unicode escape", position: tokenStart)
            }
            value = value * 16 + hexValue(digit)
            position += 1
        }
        return value
    }

    private func decodeUnicode(_ raw: [UInt8], escape: UInt8, output: inout [UInt8]) throws {
        var index = 0
        func codeUnit(_ count: Int, from offset: inout Int) throws -> UInt32 {
            var result: UInt32 = 0
            for _ in 0..<count {
                guard offset < raw.count, isHexDigit(raw[offset]) else {
                    throw ParseError.invalidSyntax(message: "Invalid Unicode escape", position: tokenStart)
                }
                result = result * 16 + hexValue(raw[offset]); offset += 1
            }
            return result
        }
        while index < raw.count {
            if raw[index] != escape {
                try appendDecoded([raw[index]], to: &output); index += 1; continue
            }
            index += 1
            guard index < raw.count else { throw ParseError.invalidSyntax(message: "Incomplete Unicode escape", position: tokenStart) }
            if raw[index] == escape {
                try appendDecoded([escape], to: &output); index += 1; continue
            }
            let long = raw[index] == 43
            if long { index += 1 }
            var scalar = try codeUnit(long ? 6 : 4, from: &index)
            if (0xD800...0xDBFF).contains(scalar) {
                guard index < raw.count, raw[index] == escape else {
                    throw ParseError.invalidSyntax(message: "Unpaired Unicode surrogate", position: tokenStart)
                }
                index += 1
                let lowLong = index < raw.count && raw[index] == 43
                if lowLong { index += 1 }
                let low = try codeUnit(lowLong ? 6 : 4, from: &index)
                guard (0xDC00...0xDFFF).contains(low) else { throw ParseError.invalidSyntax(message: "Unpaired Unicode surrogate", position: tokenStart) }
                scalar = 0x10000 + ((scalar - 0xD800) << 10) + low - 0xDC00
            }
            try appendScalar(scalar, to: &output)
        }
    }

    @discardableResult
    private func skipWhitespace(includeBlockComments: Bool = true) throws -> Bool {
        var newline = false
        while let value = byte() {
            if [9, 10, 11, 12, 13, 32].contains(value) {
                newline = newline || value == 10 || value == 13
                position += 1
            } else if value == 45, byte(1) == 45 {
                position += 2
                while let character = byte(), character != 10, character != 13 { position += 1 }
            } else if includeBlockComments, value == 47, byte(1) == 42 {
                let opening = position
                position += 2
                var depth: UInt64 = 1
                try structuralLedger.validateNestingDepth(depth)
                while depth > 0 {
                    guard let character = byte() else { throw ParseError.invalidSyntax(message: "Unterminated block comment", position: opening) }
                    newline = newline || character == 10 || character == 13
                    if character == 47, byte(1) == 42 {
                        depth += 1
                        try structuralLedger.validateNestingDepth(depth)
                        position += 2
                    } else if character == 42, byte(1) == 47 {
                        depth -= 1; position += 2
                    } else { position += 1 }
                }
            } else { break }
        }
        return newline
    }

    private func expect(_ tokenType: String) throws {
        switch currentToken {
        case .keyword(let kw) where kw == tokenType:
            advance()
        case .symbol(let s) where s == tokenType:
            advance()
        case .eof:
            throw ParseError.unexpectedEndOfInput(expected: tokenType, position: tokenStart)
        default:
            throw ParseError.unexpectedToken(
                expected: tokenType,
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
    }

    private var identifierText: String? {
        switch currentToken {
        case .identifier(let name): return name
        case .keyword(let word) where !Self.reservedWords.contains(word) && !Self.typeFunctionWords.contains(word): return word.lowercased()
        default: return nil
        }
    }

    private var labelText: String? {
        switch currentToken {
        case .identifier(let name): return name
        case .keyword(let word): return word.lowercased()
        default: return nil
        }
    }

    private func tokenDescription(_ token: Token) -> String {
        switch token {
        case .keyword(let k): return "keyword '\(k)'"
        case .identifier(let i): return "identifier '\(i)'"
        case .string(let s): return "string '\(s)'"
        case .number(let n): return "number '\(n)'"
        case .parameter(let parameter): return "parameter '\(parameter)'"
        case .invalidParameter(let reason): return "invalid parameter '\(reason)'"
        case .symbol(let s): return "symbol '\(s)'"
        case .eof: return "end of input"
        }
    }

    private func isSymbol(_ s: String) -> Bool {
        if case .symbol(let sym) = currentToken {
            return sym == s
        }
        return false
    }

    private func isKeyword(_ k: String) -> Bool {
        if case .keyword(let kw) = currentToken {
            return kw == k
        }
        return false
    }

    @discardableResult
    private func consumeContextualWord(_ word: String) -> Bool {
        switch currentToken {
        case .identifier(let value) where !tokenIsQuoted && asciiUppercase(value) == word:
            advance()
            return true
        case .keyword(let value) where value == word:
            advance()
            return true
        default:
            return false
        }
    }

    private func expectContextualWord(_ word: String) throws {
        guard consumeContextualWord(word) else {
            throw ParseError.unexpectedToken(
                expected: word,
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
    }

    private func consumeStatementTerminator() throws {
        if case .symbol(";") = currentToken {
            advance()
        }
        guard case .eof = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "end of input",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
    }
}

// MARK: - Statement Parsing

extension SQLParser {
    private func parseStatement() throws -> QueryStatement {
        switch currentToken {
        case .keyword("SELECT"), .keyword("WITH"):
            return .select(try parseSelectQuery())
        case .keyword("INSERT"):
            return .insert(try parseInsertQuery())
        case .keyword("UPDATE"):
            return .update(try parseUpdateQuery())
        case .keyword("DELETE"):
            return .delete(try parseDeleteQuery())
        case .keyword("CREATE"):
            advance()
            if case .keyword("PROPERTY") = currentToken {
                return .createGraph(try parseCreateGraph())
            }
            throw ParseError.unsupportedFeature("CREATE statement type")
        case .keyword("DROP"):
            advance()
            try expect("PROPERTY")
            try expect("GRAPH")
            guard let graphName = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: "property graph name",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            advance()
            return .dropGraph(graphName)
        default:
            throw ParseError.invalidSyntax(
                message: "Expected statement keyword",
                position: tokenStart
            )
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        let queryStart = tokenStart
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        // WITH clause (CTE) support
        var subqueries: [NamedSubquery]?
        if case .keyword("WITH") = currentToken {
            subqueries = try parseWithClause()
        }

        try expect("SELECT")

        // DISTINCT
        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        // Projection
        let projection = try parseProjection()

        // FROM
        let source: DataSource
        if case .keyword("FROM") = currentToken {
            advance()
            source = try parseDataSource()
        } else {
            // A SELECT without FROM evaluates once against the SQL singleton
            // relation. Represent it explicitly instead of routing an empty
            // table name into entity resolution.
            source = try makeStructuralNode(
                .values([[]], columnNames: [])
            )
        }

        // WHERE
        var filter: Expression?
        if case .keyword("WHERE") = currentToken {
            advance()
            filter = try parseExpression()
        }

        // GROUP BY
        var groupBy: [Expression]?
        if case .keyword("GROUP") = currentToken {
            advance()
            try expect("BY")
            groupBy = try parseExpressionList(outputReferences: true)
        }

        // HAVING
        var having: Expression?
        if case .keyword("HAVING") = currentToken {
            advance()
            having = try parseExpression()
        }

        // ORDER BY
        var orderBy: [SortKey]?
        if case .keyword("ORDER") = currentToken {
            advance()
            try expect("BY")
            orderBy = try parseOrderBy(outputReferences: true)
        }

        // LIMIT
        var limit: UInt64?
        if case .keyword("LIMIT") = currentToken {
            advance()
            guard case .number(let lexicalForm) = currentToken,
                  let value = UInt64(lexicalForm) else {
                throw ParseError.invalidSyntax(
                    message: "LIMIT requires a non-negative UInt64 integer",
                    position: tokenStart
                )
            }
            limit = value
            advance()
        }

        // OFFSET
        var offset: UInt64?
        if case .keyword("OFFSET") = currentToken {
            advance()
            guard case .number(let lexicalForm) = currentToken,
                  let value = UInt64(lexicalForm) else {
                throw ParseError.invalidSyntax(
                    message: "OFFSET requires a non-negative UInt64 integer",
                    position: tokenStart
                )
            }
            offset = value
            advance()
        }

        return try makeStructuralNode(
            SelectQuery(
                projection: projection,
                source: source,
                filter: filter,
                groupBy: groupBy,
                having: having,
                orderBy: orderBy,
                limit: limit,
                offset: offset,
                distinct: distinct,
                subqueries: subqueries
            ), at: queryStart
        )
    }

    private func parseWithClause() throws -> [NamedSubquery] {
        try expect("WITH")

        // RECURSIVE is contextual here so unrelated identifiers retain their
        // ordinary SQL name semantics.
        _ = consumeContextualWord("RECURSIVE")

        var subqueries: [NamedSubquery] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            guard let name = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: "CTE name",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            advance()

            // Optional column list: name(col1, col2)
            var columnList: [String]?
            if isSymbol("(") {
                advance()
                columnList = []
                var colFirst = true
                while colFirst || isSymbol(",") {
                    if !colFirst { advance() }
                    colFirst = false
                    if let col = identifierText {
                        try admitCollectionElement()
                        columnList?.append(col)
                        advance()
                    } else {
                        throw ParseError.unexpectedToken(expected: "CTE column name", found: tokenDescription(currentToken), position: tokenStart)
                    }
                }
                try expect(")")
            }

            try expect("AS")

            // Materialization hint (optional)
            var materialized: Materialization?
            if consumeContextualWord("MATERIALIZED") {
                materialized = .materialized
            } else if case .keyword("NOT") = currentToken {
                advance()
                // NOT must be followed by MATERIALIZED
                try expectContextualWord("MATERIALIZED")
                materialized = .notMaterialized
            }

            try expect("(")
            let query = try parseSelectQuery()
            try expect(")")

            try admitCollectionElement()
            subqueries.append(
                try makeStructuralNode(
                    NamedSubquery(
                        name: name,
                        columns: columnList,
                        query: query,
                        materialized: materialized
                    )
                )
            )
        }

        return subqueries
    }

    private func parseProjection() throws -> Projection {
        if case .symbol("*") = currentToken {
            advance()
            return .all
        }

        if let qualifier = identifierText, peekNextToken() == .symbol("."), peekNextToken(2) == .symbol("*") {
            let qualifierStart = tokenStart
            let qualifierEnd = tokenEnd
            advance()
            try expect(".")
            try expect("*")
            try sourceCapture?.recordProjectionQualifier(start: qualifierStart, end: previousTokenEnd,
                                                         identifierEnd: qualifierEnd, ledger: &structuralLedger)
            return .allFrom(qualifier)
        }

        var items: [ProjectionItem] = []
        var first = true
        while first || isSymbol(",") {
            if !first {
                advance()
            }
            first = false
            let expr = try parseExpression()
            var alias: String?
            if case .keyword("AS") = currentToken {
                advance()
                alias = try parseRequiredIdentifier("projection alias", allowsKeywordLabel: true)
            }
            try admitCollectionElement()
            items.append(
                try makeStructuralNode(
                    ProjectionItem(expr, alias: alias)
                )
            )
        }

        return .items(items)
    }

    private func parseDataSource() throws -> DataSource {
        let source = try parseTableRef()

        // Check for JOINs
        var result = source
        while case .keyword(let kw) = currentToken,
              ["INNER", "LEFT", "RIGHT", "FULL", "CROSS", "JOIN", "NATURAL"]
                .contains(kw) {
            var joinType = try parseJoinType()
            let crossJoin = joinType == .cross
            if case .keyword("LATERAL") = currentToken {
                switch joinType {
                case .inner, .cross:
                    joinType = .lateral
                case .left:
                    joinType = .leftLateral
                default:
                    throw ParseError.unsupportedFeature(
                        "LATERAL is supported only for INNER, CROSS, or LEFT joins"
                    )
                }
                advance()
            }
            let right = try parseTableRef()
            var condition: JoinCondition?

            if case .keyword("ON") = currentToken {
                advance()
                condition = .on(try parseExpression())
            } else if case .keyword("USING") = currentToken {
                advance()
                try expect("(")
                var cols: [String] = []
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    if let name = identifierText {
                        try admitCollectionElement()
                        cols.append(name)
                        advance()
                    } else {
                        throw ParseError.unexpectedToken(expected: "USING column", found: tokenDescription(currentToken), position: tokenStart)
                    }
                }
                try expect(")")
                guard !cols.isEmpty else {
                    throw ParseError.invalidSyntax(
                        message: "JOIN USING requires at least one column",
                        position: tokenStart
                    )
                }
                condition = .using(cols)
            }

            let naturalJoin: Bool
            switch joinType {
            case .natural, .naturalLeft, .naturalRight, .naturalFull:
                naturalJoin = true
            default:
                naturalJoin = false
            }
            if naturalJoin, condition != nil {
                throw ParseError.invalidSyntax(
                    message: "NATURAL JOIN cannot declare ON or USING",
                    position: tokenStart
                )
            }
            if crossJoin, condition != nil {
                throw ParseError.invalidSyntax(
                    message: "CROSS JOIN cannot declare ON or USING",
                    position: tokenStart
                )
            }

            if !naturalJoin, !crossJoin, condition == nil {
                throw ParseError.invalidSyntax(message: "JOIN requires ON or USING", position: tokenStart)
            }

            result = try makeStructuralNode(
                .join(
                    JoinClause(
                        type: joinType,
                        left: result,
                        right: right,
                        condition: condition
                    )
                )
            )
        }

        return result
    }

    private func parseTableRef() throws -> DataSource {
        let sourceStart = tokenStart
        let sourceIdentifierEnd = tokenEnd
        // FIXME(INCOMPLETE_IMPLEMENTATION): SQL FROM reaches row-returning function syntax here.
        // S10 must provide typed relation composition and real row execution before admission succeeds.
        if let name = labelText, peekNextToken() == .symbol("("), asciiUppercase(name) != "GRAPH_TABLE" {
            throw ParseError.unsupportedFeature("SQL table functions require S10 relation composition")
        }
        // Check for GRAPH_TABLE
        if case .keyword("GRAPH_TABLE") = currentToken, peekNextToken() == .symbol("(") {
            let savedCapture = sourceCapture
            sourceCapture = nil
            let source: DataSource
            do {
                source = try parseGraphTable()
            } catch {
                sourceCapture = savedCapture
                throw error
            }
            sourceCapture = savedCapture
            try sourceCapture?.record(source, start: sourceStart, end: previousTokenEnd,
                                      ledger: &structuralLedger)
            return source
        }

        // Check for subquery: (SELECT ...) or (WITH ...)
        if isSymbol("(") {
            try enterStructuralNesting()
            defer { leaveStructuralNesting() }
            advance()
            if !isKeyword("SELECT"), !isKeyword("WITH") {
                let grouped = try parseDataSource()
                try expect(")")
                if isKeyword("AS") || identifierText != nil {
                    throw ParseError.unsupportedFeature("Joined-table group alias")
                }
                return grouped
            }

            let subquery = try parseSelectQuery()
            try expect(")")

            // Alias is required for subqueries
            var alias: String?
            if case .keyword("AS") = currentToken {
                advance()
            }
            if let a = identifierText {
                alias = a
                advance()
            }

            guard let subqueryAlias = alias else {
                throw ParseError.invalidSyntax(
                    message: "Subquery in FROM clause requires an alias",
                    position: tokenStart
                )
            }

            return try makeStructuralNode(
                .subquery(subquery, alias: subqueryAlias)
            )
        }

        let table = try parseNamedTableReference()
        return try makeStructuralNode(
            .table(table), at: sourceStart, identifierEnd: sourceIdentifierEnd
        )
    }

    private func parseNamedTableReference(
        allowsImplicitAlias: Bool = true,
        implicitAliasTerminators: Set<String> = []
    ) throws -> TableRef {
        guard let firstName = identifierText else {
            throw ParseError.unexpectedToken(
                expected: "table name",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
        advance()

        let schema: String?
        let table: String
        if isSymbol(".") {
            advance()
            guard let tableName = labelText else {
                throw ParseError.unexpectedToken(
                    expected: "table name after schema qualifier",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            schema = firstName
            table = tableName
            advance()
        } else {
            schema = nil
            table = firstName
        }

        var alias: String?
        if isKeyword("AS") {
            advance()
            guard let name = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: "table alias",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            alias = name
            advance()
        } else if allowsImplicitAlias, let name = identifierText,
                  tokenIsQuoted || !implicitAliasTerminators.contains(asciiUppercase(name)) {
            alias = name
            advance()
        }

        return TableRef(schema: schema, table: table, alias: alias)
    }

    private func parseJoinType() throws -> JoinType {
        let isNatural: Bool
        if case .keyword("NATURAL") = currentToken {
            isNatural = true
            advance()
        } else {
            isNatural = false
        }
        var joinType: JoinType = .inner

        switch currentToken {
        case .keyword("LEFT"):
            joinType = .left
            advance()
        case .keyword("RIGHT"):
            joinType = .right
            advance()
        case .keyword("FULL"):
            joinType = .full
            advance()
        case .keyword("CROSS"):
            joinType = .cross
            advance()
        case .keyword("INNER"):
            advance()
        default:
            break
        }

        if joinType == .left || joinType == .right || joinType == .full {
            _ = consumeContextualWord("OUTER")
        }
        guard case .keyword("JOIN") = currentToken else {
            throw ParseError.unexpectedToken(
                expected: "JOIN",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
        advance()

        guard isNatural else { return joinType }
        switch joinType {
        case .inner: return .natural
        case .left: return .naturalLeft
        case .right: return .naturalRight
        case .full: return .naturalFull
        default:
            throw ParseError.unsupportedFeature(
                "NATURAL CROSS JOIN is not supported"
            )
        }
    }

    private func parseExpression() throws -> Expression {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        return try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .keyword("OR") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = try makeStructuralNode(.or(left, right))
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseNotExpression()
        while case .keyword("AND") = currentToken {
            advance()
            let right = try parseNotExpression()
            left = try makeStructuralNode(.and(left, right))
        }
        return left
    }

    private func parseNotExpression() throws -> Expression {
        var operatorCount = 0
        while case .keyword("NOT") = currentToken {
            try enterStructuralNesting()
            operatorCount += 1
            advance()
        }
        defer {
            for _ in 0..<operatorCount {
                leaveStructuralNesting()
            }
        }
        var expression = try parseComparisonExpression()
        for _ in 0..<operatorCount {
            expression = try makeStructuralNode(.not(expression))
        }
        return expression
    }

    private func parseComparisonExpression() throws -> Expression {
        var expression = try parsePredicateExpression()
        while isKeyword("IS") { expression = try parseIsPredicate(expression) }
        return expression
    }

    private func parsePredicateExpression() throws -> Expression {
        let left = try parseConcatenateExpression()

        let comparison: DatabaseKit.ComparisonOperator?
        switch currentToken {
        case .symbol("="): comparison = .equal
        case .symbol("<>"), .symbol("!="): comparison = .notEqual
        case .symbol("<"): comparison = .lessThan
        case .symbol("<="): comparison = .lessThanOrEqual
        case .symbol(">"): comparison = .greaterThan
        case .symbol(">="): comparison = .greaterThanOrEqual
        default: comparison = nil
        }
        if let comparison {
            advance()
            let expression: Expression
            if isKeyword("ANY") || isKeyword("SOME") || isKeyword("ALL") {
                let quantifier: ComparisonQuantifier = isKeyword("ALL") ? .all : .any
                advance()
                try expect("(")
                let source: QuantifiedComparisonSource
                if isKeyword("SELECT") || isKeyword("WITH") {
                    source = .subquery(try parseSelectQuery())
                } else {
                    source = .array(try parseExpression())
                }
                try expect(")")
                expression = try makeStructuralNode(.quantifiedComparison(left, comparison: comparison, quantifier: quantifier, source: source))
            } else {
                let right = try parseConcatenateExpression()
                switch comparison {
                case .equal: expression = try makeStructuralNode(.equal(left, right))
                case .notEqual: expression = try makeStructuralNode(.notEqual(left, right))
                case .lessThan: expression = try makeStructuralNode(.lessThan(left, right))
                case .lessThanOrEqual: expression = try makeStructuralNode(.lessThanOrEqual(left, right))
                case .greaterThan: expression = try makeStructuralNode(.greaterThan(left, right))
                case .greaterThanOrEqual: expression = try makeStructuralNode(.greaterThanOrEqual(left, right))
                }
            }
            return expression
        }

        switch currentToken {
        case .keyword("IS"):
            return try parseIsPredicate(left)
        case .keyword("LIKE"), .keyword("ILIKE"), .keyword("SIMILAR"):
            return try parsePatternPredicate(left, negated: false)
        case .keyword("IN"):
            advance()
            try expect("(")

            // Check if it's a subquery or value list
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(
                    .inSubquery(left, subquery: subquery)
                )
            }

            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(
                    .inSubquery(left, subquery: subquery)
                )
            }

            // Value list (existing logic)
            var values: [Expression] = []
            var first = true
            while first || isSymbol(",") {
                if !first { advance() }
                first = false
                try admitCollectionElement()
                values.append(try parseExpression())
            }
            try expect(")")
            return try makeStructuralNode(.inList(left, values: values))
        case .keyword("NOT"):
            advance()
            switch currentToken {
            case .keyword("IN"):
                advance()
                try expect("(")
                if case .keyword("SELECT") = currentToken {
                    let subquery = try parseSelectQuery()
                    try expect(")")
                    let membership = try makeStructuralNode(
                        Expression.inSubquery(left, subquery: subquery)
                    )
                    return try makeStructuralNode(.not(membership))
                }
                if case .keyword("WITH") = currentToken {
                    let subquery = try parseSelectQuery()
                    try expect(")")
                    let membership = try makeStructuralNode(
                        Expression.inSubquery(left, subquery: subquery)
                    )
                    return try makeStructuralNode(.not(membership))
                }
                var values: [Expression] = []
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    try admitCollectionElement()
                    values.append(try parseExpression())
                }
                try expect(")")
                return try makeStructuralNode(
                    .notInList(left, values: values)
                )
            case .keyword("BETWEEN"):
                advance()
                let low = try parseConcatenateExpression()
                try expect("AND")
                let high = try parseConcatenateExpression()
                let range = try makeStructuralNode(
                    Expression.between(left, low: low, high: high)
                )
                return try makeStructuralNode(.not(range))
            case .keyword("LIKE"), .keyword("ILIKE"), .keyword("SIMILAR"):
                return try parsePatternPredicate(left, negated: true)
            default:
                throw ParseError.unexpectedToken(
                    expected: "IN, BETWEEN, LIKE, ILIKE, or SIMILAR after NOT",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
        case .keyword("BETWEEN"):
            advance()
            let low = try parseConcatenateExpression()
            try expect("AND")
            let high = try parseConcatenateExpression()
            return try makeStructuralNode(
                .between(left, low: low, high: high)
            )
        default:
            return left
        }
    }

    private func parseIsPredicate(_ value: Expression) throws -> Expression {
        try expect("IS")
        let negated = isKeyword("NOT")
        if negated { advance() }
        if isKeyword("NULL") {
            advance()
            return try makeStructuralNode(negated ? .isNotNull(value) : .isNull(value))
        }
        if isKeyword("DISTINCT") {
            advance()
            try expect("FROM")
            let right = try parseConcatenateExpression()
            return try makeStructuralNode(.isDistinctFrom(value, right, negated: negated))
        }
        let truth: TruthValue
        if isKeyword("TRUE") { truth = .true }
        else if isKeyword("FALSE") { truth = .false }
        else if consumeContextualWord("UNKNOWN") {
            return try makeStructuralNode(.truthTest(value, value: .unknown, negated: negated))
        } else {
            throw ParseError.unexpectedToken(expected: "NULL, TRUE, FALSE, UNKNOWN, or DISTINCT FROM", found: tokenDescription(currentToken), position: tokenStart)
        }
        advance()
        return try makeStructuralNode(.truthTest(value, value: truth, negated: negated))
    }

    private func parseConcatenateExpression() throws -> Expression {
        var left = try parseAddExpression()
        while case .symbol(let symbol) = currentToken {
            guard symbol == "||" || Self.collectionOperators[symbol] != nil || ["~", "~*", "!~", "!~*"].contains(symbol) else { break }
            advance()
            let right = try parseAddExpression()
            if let operation = Self.collectionOperators[symbol] {
                left = try makeStructuralNode(.collectionOperation(operation, left: left, right: right))
            } else if symbol == "||" {
                left = try makeStructuralNode(.concatenate(left, right))
            } else {
                let kind: PatternMatchKind = symbol.hasSuffix("*") ? .caseInsensitiveRegularExpression : .regularExpression
                left = try makeStructuralNode(.patternMatch(subject: left, pattern: right, escape: nil, kind: kind))
                if symbol.hasPrefix("!") { left = try makeStructuralNode(.not(left)) }
            }
        }
        return left
    }

    private func parsePatternPredicate(_ subject: Expression, negated: Bool) throws -> Expression {
        let operatorStart = tokenStart
        let operatorEnd = tokenEnd
        let kind: PatternMatchKind
        var negate = negated
        switch currentToken {
        case .keyword("LIKE"): kind = .like
        case .keyword("ILIKE"): kind = .caseInsensitiveLike
        case .keyword("SIMILAR"): kind = .similarTo
        case .symbol("~"), .symbol("!~"):
            kind = .regularExpression
            if isSymbol("!~") { negate.toggle() }
        case .symbol("~*"), .symbol("!~*"):
            kind = .caseInsensitiveRegularExpression
            if isSymbol("!~*") { negate.toggle() }
        default: throw ParseError.invalidSyntax(message: "Expected SQL pattern operator", position: tokenStart)
        }
        advance()
        if kind == .similarTo { try expectContextualWord("TO") }
        let pattern = try parseConcatenateExpression()
        let escape: Expression?
        if kind.permitsEscape {
            if consumeContextualWord("ESCAPE") {
                let supplied = try parseConcatenateExpression()
                switch supplied {
                case .literal(.sqlUnknownString("")), .literal(.string("")),
                     .literal(.queryValue(.primitive(.string("")), _)):
                    _ = sourceCapture?.discardLastExpression()
                    escape = nil
                default: escape = supplied
                }
            } else {
                escape = try makeStructuralNode(.literal(.queryValue(.primitive(.string("\\")), type: .sql(.text))),
                    at: operatorStart, end: operatorEnd)
            }
        } else {
            if consumeContextualWord("ESCAPE") {
                throw ParseError.invalidSyntax(message: "ERE operators do not accept ESCAPE", position: operatorStart)
            }
            escape = nil
        }
        let match = try makeStructuralNode(.patternMatch(subject: subject, pattern: pattern, escape: escape, kind: kind))
        return try negate ? makeStructuralNode(.not(match)) : match
    }

    private func parseAddExpression() throws -> Expression {
        var left = try parseMulExpression()
        while case .symbol(let s) = currentToken, ["+", "-"].contains(s) {
            advance()
            let right = try parseMulExpression()
            if s == "+" {
                left = try makeStructuralNode(.add(left, right))
            } else {
                left = try makeStructuralNode(.subtract(left, right))
            }
        }
        return left
    }

    private func parseMulExpression() throws -> Expression {
        var left = try parseAtTimeZoneExpression()
        while case .symbol(let s) = currentToken, ["*", "/", "%"].contains(s) {
            advance()
            let right = try parseAtTimeZoneExpression()
            switch s {
            case "*": left = try makeStructuralNode(.multiply(left, right))
            case "/": left = try makeStructuralNode(.divide(left, right))
            case "%": left = try makeStructuralNode(.modulo(left, right))
            default: break
            }
        }
        return left
    }

    private func parseAtTimeZoneExpression() throws -> Expression {
        var value = try parseUnaryExpression()
        while isContextualWord("AT") {
            let start = tokenStart
            advance()
            try expectContextualWord("TIME")
            try expectContextualWord("ZONE")
            let zone = try parseUnaryExpression()
            try sourceCapture?.reorderLastExpressions([1, 0])
            value = try makeStructuralNode(.function(FunctionCall(name: "timezone", arguments: [zone, value])), at: start)
        }
        return value
    }

    private func parseUnaryExpression() throws -> Expression {
        let unaryStart = tokenStart
        var signs: [Bool] = []
        while isSymbol("-") || isSymbol("+") {
            try enterStructuralNesting()
            try admitCollectionElement()
            signs.append(isSymbol("-"))
            advance()
        }
        let signCount = signs.count
        defer { for _ in 0..<signCount { leaveStructuralNesting() } }
        var expression: Expression
        if case .number(let text) = currentToken, let negative = signs.popLast() {
            advance()
            expression = try numericExpression((negative ? "-" : "+") + text, at: unaryStart)
        } else {
            expression = try parsePrimaryExpression()
        }
        while isSymbol("::") || isSymbol("[") {
            if isSymbol("::") {
                let start = tokenStart
                advance()
                expression = try normalizedCast(expression, as: parseDataType(), at: start)
            } else {
                expression = try parseArrayAccess(expression)
            }
        }
        for negative in signs.reversed() {
            if negative { expression = try makeStructuralNode(.negate(expression), at: unaryStart) }
        }
        return expression
    }

    private func parseArrayAccess(_ base: Expression) throws -> Expression {
        var subscripts: [ArraySubscript] = []
        while isSymbol("[") {
            guard subscripts.count < 6 else {
                throw ParseError.invalidSyntax(message: "Array access has more than six dimensions", position: tokenStart)
            }
            try admitCollectionElement()
            advance()
            let lower: Expression?
            if isSymbol(":") { lower = nil }
            else { lower = try parseArrayBound() }
            if isSymbol(":") {
                advance()
                let upper = isSymbol("]") ? nil : try parseArrayBound()
                subscripts.append(.slice(lower: lower, upper: upper))
            } else if let lower {
                subscripts.append(.index(lower))
            } else {
                throw ParseError.invalidSyntax(message: "Expected array subscript", position: tokenStart)
            }
            try expect("]")
        }
        return try makeStructuralNode(.arrayAccess(base, subscripts: subscripts))
    }

    private func parseArrayBound() throws -> Expression {
        let previous = colonStartsSlice
        colonStartsSlice = true
        defer { colonStartsSlice = previous }
        return try parseExpression()
    }

    private func numericExpression(_ text: String, at start: Int) throws -> Expression {
        do {
            return try makeLiteralExpression(SQLConstantSyntax.numericLiteral(text, limits: constantLimits(reservingNodes: 4)), at: start)
        } catch let error as SQLConstantError {
            throw ParseError.literalError(message: String(describing: error), position: start)
        }
    }

    private func normalizedCast(_ expression: Expression, as type: DataType, at start: Int) throws -> Expression {
        do {
            if let literal = try SQLConstantSyntax.normalize(expression, as: type, limits: constantLimits(reservingNodes: 3 + dataTypeNodeCount(type))) {
                if preservesOutputReferenceCasts {
                    switch literal {
                    case .queryValue(.primitive(.int16), _),
                         .queryValue(.primitive(.int32), _),
                         .queryValue(.primitive(.int64), _):
                        return try makeStructuralNode(.cast(expression, targetType: type), at: start)
                    default: break
                    }
                }
                let originalRange = sourceCapture?.discardLastExpression()
                sourceCapture?.discardLastType()
                return try makeLiteralExpression(literal, at: min(start, originalRange?.start ?? start))
            }
            return try makeStructuralNode(.cast(expression, targetType: type), at: start)
        } catch let error as SQLConstantError {
            throw ParseError.literalError(message: String(describing: error), position: start)
        }
    }

    private func parsePrimaryExpression() throws -> Expression {
        let primaryStart = tokenStart
        if !tokenIsQuoted, let word = labelText {
            let upper = asciiUppercase(word)
            let next = peekNextToken()
            if let kind = Self.currentValueKind(upper) {
                let identifierEnd = tokenEnd
                advance()
                let precision: Int?
                if kind == .date { precision = nil }
                else { precision = try parseTemporalPrecision() }
                return try makeStructuralNode(.currentValue(kind: kind, precision: precision), at: primaryStart, identifierEnd: identifierEnd)
            }
            if upper == "INTERVAL", next.isString || next == .symbol("(") {
                return try parseIntervalLiteral(at: primaryStart)
            }
            if upper == "EXTRACT", next == .symbol("(") {
                return try parseExtractExpression(at: primaryStart)
            }
            if upper == "ARRAY", next == .symbol("(") {
                // FIXME(INCOMPLETE_IMPLEMENTATION): SQL ARRAY(subquery) reaches this gate;
                // S08 must provide relation carriage and executed cardinality/shape evidence.
                throw ParseError.unsupportedFeature("ARRAY(subquery) requires S08 relation composition")
            }
            if ["ARRAY", "ROW"].contains(upper), next == .symbol(upper == "ARRAY" ? "[" : "(") {
                advance()
                return try parseConstructor(array: upper == "ARRAY", at: primaryStart)
            }
            if Self.literalTypeWords.contains(upper),
               next.isString || next == .symbol("(") && (Self.columnNameWords.contains(upper) || isTypedLiteralPrefix()) || next == .keyword("WITH") || next == .identifier("without") || next == .identifier("precision") || next == .identifier("varying") {
                let start = tokenStart
                let type = try parseDataType()
                if case .array = type {
                    throw ParseError.invalidSyntax(message: "Array typed prefixes require CAST or ::", position: start)
                }
                guard case .string(let text) = currentToken else {
                    throw ParseError.unexpectedToken(expected: "typed string literal", found: tokenDescription(currentToken), position: tokenStart)
                }
                advance()
                let value = try makeLiteralExpression(.sqlUnknownString(text), at: primaryStart)
                return try normalizedCast(value, as: type, at: start)
            }
            let aggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "GROUP_CONCAT"]
            if aggregates.contains(upper), next == .symbol("(") { return try parseAggregate() }
            let syntaxExpression = ["EXISTS", "CASE", "CAST"].contains(upper)
            if !syntaxExpression || upper == "EXISTS" && next != .symbol("(") {
                if identifierText != nil || Self.typeFunctionWords.contains(upper) && next == .symbol("(") {
                    return try parseIdentifierExpression(word)
                }
            }
        }
        switch currentToken {
        case .symbol("("):
            let previousColonRole = colonStartsSlice
            colonStartsSlice = false
            defer { colonStartsSlice = previousColonRole }
            advance()
            // Disambiguate: subquery vs parenthesized expression
            if case .keyword("SELECT") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.subquery(subquery))
            }
            // Handle WITH clause (CTE) that starts a subquery
            if case .keyword("WITH") = currentToken {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.subquery(subquery))
            }
            let expr = try parseExpression()
            try expect(")")
            return expr

        case .keyword("EXISTS"):
            advance()
            try expect("(")
            let subquery = try parseSelectQuery()
            try expect(")")
            return try makeStructuralNode(.exists(subquery))

        case .number(let n):
            let start = tokenStart
            advance()
            return try numericExpression(n, at: start)

        case .string(let s):
            advance()
            return try makeLiteralExpression(.sqlUnknownString(s), at: primaryStart)

        case .symbol("?"):
            guard positionalParameterStyle != .explicit else {
                throw ParseError.invalidSyntax(message: "Cannot mix '?' and '$n' positional parameters", position: tokenStart)
            }
            guard anonymousParameterPosition < UInt32.max else {
                throw ParseError.invalidSyntax(message: "Anonymous parameter position overflow", position: tokenStart)
            }
            positionalParameterStyle = .anonymous
            anonymousParameterPosition += 1
            let reference = QueryParameterReference.position(anonymousParameterPosition)
            advance()
            return try makeStructuralNode(.parameter(reference), at: primaryStart)

        case .parameter(let reference):
            advance()
            return try makeStructuralNode(.parameter(reference), at: primaryStart)

        case .symbol(":"):
            guard !colonStartsSlice else {
                throw ParseError.invalidSyntax(message: "Named subscript operands require parentheses", position: tokenStart)
            }
            advance()
            let name: String
            switch currentToken {
            case .identifier(let value), .keyword(let value):
                name = tokenIsQuoted ? value : try decoded(tokenStart..<tokenEnd)
            default:
                throw ParseError.invalidSyntax(message: "Expected a named parameter identifier after ':'", position: tokenStart)
            }
            advance()
            return try makeStructuralNode(.parameter(.name(name)), at: primaryStart)

        case .invalidParameter(let reason):
            throw ParseError.invalidSyntax(
                message: reason,
                position: tokenStart
            )

        case .keyword("TRUE"):
            advance()
            return try makeLiteralExpression(.bool(true), at: primaryStart)

        case .keyword("FALSE"):
            advance()
            return try makeLiteralExpression(.bool(false), at: primaryStart)

        case .keyword("NULL"):
            advance()
            return try makeLiteralExpression(.null, at: primaryStart)

        case .keyword("COUNT"), .keyword("SUM"), .keyword("AVG"),
                .keyword("MIN"), .keyword("MAX"),
                .keyword("ARRAY_AGG"), .keyword("GROUP_CONCAT"):
            return try parseAggregate()

        case .keyword("CASE"):
            return try parseCaseExpression()

        case .keyword("CAST"):
            return try parseCastExpression()

        case .identifier(let name):
            return try parseIdentifierExpression(name)

        default:
            throw ParseError.unexpectedToken(
                expected: "expression",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
    }

    private static func currentValueKind(_ spelling: String) -> SQLCurrentValueKind? {
        switch spelling {
        case "CURRENT_DATE": return .date
        case "CURRENT_TIME": return .time
        case "CURRENT_TIMESTAMP": return .timestamp
        case "LOCALTIME": return .localTime
        case "LOCALTIMESTAMP": return .localTimestamp
        default: return nil
        }
    }

    private func parseExtractExpression(at start: Int) throws -> Expression {
        let identifierEnd = tokenEnd
        advance()
        try expect("(")
        let fieldStart = tokenStart, fieldEnd = tokenEnd
        let field: String
        if case .string(let value) = currentToken { field = value }
        else if let value = labelText { field = tokenIsQuoted ? value : value.lowercased() }
        else { throw ParseError.unexpectedToken(expected: "EXTRACT field", found: tokenDescription(currentToken), position: tokenStart) }
        advance()
        let fieldExpression = try makeStructuralNode(Expression.literal(.queryValue(.primitive(.string(field)), type: .sql(.text))), at: fieldStart, end: fieldEnd)
        try expect("FROM")
        let source = try parseExpression()
        try expect(")")
        return try makeStructuralNode(.function(FunctionCall(name: "extract", arguments: [fieldExpression, source])), at: start, identifierEnd: identifierEnd)
    }

    private func parseIntervalLiteral(at start: Int) throws -> Expression {
        advance()
        let precision = try parseTemporalPrecision()
        guard case .string(let text) = currentToken else {
            throw ParseError.unexpectedToken(expected: "interval string", found: tokenDescription(currentToken), position: tokenStart)
        }
        let literalStart = tokenStart
        advance()
        let value = try makeLiteralExpression(.sqlUnknownString(text), at: literalStart)
        if precision != nil, let word = labelText, !tokenIsQuoted,
           ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"].contains(asciiUppercase(word)) {
            throw ParseError.invalidSyntax(message: "Interval precision cannot precede restricted fields", position: tokenStart)
        }
        let fields = precision == nil ? try parseIntervalFields() : nil
        let fieldPrecision = fields == nil ? nil : try parseTemporalPrecision()
        let type = DataType.interval(fields: fields, precision: precision ?? fieldPrecision)
        guard QueryValueValidator.accepts(.primitive(.null), type: .sql(type)) else {
            throw ParseError.invalidSyntax(message: "Invalid interval field precision", position: start)
        }
        _ = try makeStructuralNode(type, at: start)
        return try normalizedCast(value, as: type, at: start)
    }

    private func parseIntervalFields() throws -> SQLIntervalFields? {
        guard let word = labelText, !tokenIsQuoted else { return nil }
        let first = asciiUppercase(word)
        guard ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"].contains(first) else { return nil }
        let start = tokenStart
        advance()
        var spelling = first
        if consumeContextualWord("TO") {
            guard let last = labelText, !tokenIsQuoted else {
                throw ParseError.unexpectedToken(expected: "interval terminal field", found: tokenDescription(currentToken), position: tokenStart)
            }
            spelling += " TO " + asciiUppercase(last)
            advance()
        }
        let field: SQLIntervalFields
        switch spelling {
        case "YEAR": field = .year
        case "MONTH": field = .month
        case "DAY": field = .day
        case "HOUR": field = .hour
        case "MINUTE": field = .minute
        case "SECOND": field = .second
        case "YEAR TO MONTH": field = .yearToMonth
        case "DAY TO HOUR": field = .dayToHour
        case "DAY TO MINUTE": field = .dayToMinute
        case "DAY TO SECOND": field = .dayToSecond
        case "HOUR TO MINUTE": field = .hourToMinute
        case "HOUR TO SECOND": field = .hourToSecond
        case "MINUTE TO SECOND": field = .minuteToSecond
        default: throw ParseError.invalidSyntax(message: "Invalid interval field range", position: start)
        }
        return field
    }

    private func parseMakeIntervalArguments(at start: Int, identifierEnd: Int) throws -> [Expression] {
        let names = ["years", "months", "weeks", "days", "hours", "mins", "secs"]
        var supplied: [Expression] = []
        var slots: [Int?] = Array(repeating: nil, count: names.count)
        var named = false
        if !isSymbol(")") {
            while true {
                try admitCollectionElement()
                let slot: Int
                if let name = labelText, peekNextToken() == .symbol("=>") || peekNextToken() == .symbol(":=") {
                    named = true
                    let exact = tokenIsQuoted ? name : name.lowercased()
                    guard let index = names.firstIndex(of: exact), slots[index] == nil else {
                        throw ParseError.invalidSyntax(message: "Unknown or duplicate MAKE_INTERVAL argument", position: tokenStart)
                    }
                    slot = index
                    advance(); advance()
                } else {
                    guard !named, supplied.count < names.count else {
                        throw ParseError.invalidSyntax(message: "Invalid MAKE_INTERVAL argument order or count", position: tokenStart)
                    }
                    slot = supplied.count
                }
                slots[slot] = supplied.count
                supplied.append(try parseExpression())
                guard isSymbol(",") else { break }
                advance()
            }
        }
        try expect(")")
        guard named else { return supplied }
        for index in slots.indices where slots[index] == nil {
            try admitCollectionElement()
            let literal: Literal = index == 6 ? .queryValue(.primitive(.float64(0)), type: .sql(.doublePrecision))
                : .queryValue(.primitive(.int32(0)), type: .sql(.integer))
            slots[index] = supplied.count
            supplied.append(try makeStructuralNode(Expression.literal(literal), at: start, end: identifierEnd))
        }
        var order: [Int] = []
        for slot in slots {
            guard let slot else { throw ParseError.invalidSyntax(message: "Missing MAKE_INTERVAL slot", position: start) }
            order.append(slot)
        }
        try sourceCapture?.reorderLastExpressions(order)
        return order.map { supplied[$0] }
    }

    private func parseIdentifierExpression(_ firstName: String) throws -> Expression {
        let firstQuoted = tokenIsQuoted
        let start = tokenStart
        var identifierEnd = tokenEnd
        advance()
        var name = firstName
        var schema: String?
        var quoted = firstQuoted
        if isSymbol(".") {
            advance()
            quoted = tokenIsQuoted
            name = try parseRequiredIdentifier("name after qualifier", allowsKeywordLabel: true)
            schema = firstName
            identifierEnd = previousTokenEnd
            guard !isSymbol(".") else {
                throw ParseError.unsupportedFeature("Multi-part function or column names")
            }
        }
        if isSymbol("(") {
            let previousColonRole = colonStartsSlice
            colonStartsSlice = false
            defer { colonStartsSlice = previousColonRole }
            let spelling = asciiUppercase(name)
            let expressionFunctions: Set<String> = ["COALESCE", "NULLIF", "GREATEST", "LEAST", "GROUPING", "JSON_ARRAY", "JSON_OBJECT", "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE", "JSON_VALUE", "JSON_EXISTS", "NORMALIZE", "SUBSTRING", "POSITION", "TRIM", "XMLCONCAT", "XMLFOREST"]
            guard schema != nil || quoted || !Self.columnNameWords.contains(spelling) || expressionFunctions.contains(spelling) else {
                throw ParseError.invalidSyntax(message: "Keyword is not a function name", position: start)
            }
            if schema == nil, !quoted, ["SUBSTRING", "POSITION", "TRIM"].contains(spelling) {
                return try parseStringFunction(spelling, start: start, identifierEnd: identifierEnd)
            }
            advance()
            let distinct = isKeyword("DISTINCT")
            if distinct {
                advance()
                guard !isSymbol(")") else {
                    throw ParseError.invalidSyntax(message: "DISTINCT function call requires arguments", position: tokenStart)
                }
            }
            if name == "make_interval", schema == nil || schema == "pg_catalog", !distinct {
                let args = try parseMakeIntervalArguments(at: start, identifierEnd: identifierEnd)
                return try makeStructuralNode(.function(FunctionCall(name: name, arguments: args, schema: schema)), at: start, identifierEnd: identifierEnd)
            }
            var args: [Expression] = []
            var finalArrayIsVariadic = false
            if !isSymbol(")") {
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    try admitCollectionElement()
                    if isKeyword("VARIADIC") {
                        finalArrayIsVariadic = true
                        advance()
                    }
                    args.append(try parseExpression())
                    if finalArrayIsVariadic && !isSymbol(")") {
                        throw ParseError.invalidSyntax(message: "VARIADIC must mark the final argument", position: tokenStart)
                    }
                }
            }
            try expect(")")
            if finalArrayIsVariadic, schema == nil, !quoted, ["COALESCE", "NULLIF", "GREATEST", "LEAST"].contains(spelling) {
                throw ParseError.invalidSyntax(message: "VARIADIC is not allowed in conditional expression syntax", position: start)
            }
            if schema == nil, !quoted, distinct, ["COALESCE", "NULLIF", "GREATEST", "LEAST"].contains(spelling) {
                throw ParseError.invalidSyntax(message: "DISTINCT is not allowed in conditional expression syntax", position: start)
            }
            if schema == nil, !quoted, spelling == "COALESCE" {
                guard !args.isEmpty else { throw ParseError.invalidSyntax(message: "COALESCE requires at least one argument", position: start) }
                return try makeStructuralNode(.coalesce(args), at: start, identifierEnd: identifierEnd)
            }
            if schema == nil, !quoted, spelling == "NULLIF" {
                guard args.count == 2 else { throw ParseError.invalidSyntax(message: "NULLIF requires exactly two arguments", position: start) }
                return try makeStructuralNode(.nullIf(args[0], args[1]), at: start, identifierEnd: identifierEnd)
            }
            if schema == nil, !quoted, spelling == "GREATEST" || spelling == "LEAST" {
                guard !args.isEmpty else { throw ParseError.invalidSyntax(message: "GREATEST/LEAST requires at least one argument", position: start) }
                return try makeStructuralNode(spelling == "GREATEST" ? .greatest(args) : .least(args), at: start, identifierEnd: identifierEnd)
            }
            return try makeStructuralNode(.function(FunctionCall(name: name, arguments: args, distinct: distinct, schema: schema, finalArrayIsVariadic: finalArrayIsVariadic)), at: start, identifierEnd: identifierEnd)
        }
        return try makeStructuralNode(.column(ColumnRef(table: schema, column: name)), at: start, identifierEnd: identifierEnd)
    }

    private func parseStringFunction(_ spelling: String, start: Int, identifierEnd: Int) throws -> Expression {
        try expect("(")
        var name = spelling.lowercased()
        var arguments: [Expression] = []
        if spelling == "POSITION" {
            let needle = try parseConcatenateExpression()
            try expect("IN")
            let source = try parseExpression()
            arguments = [source, needle]
            name = "strpos"
            try sourceCapture?.reorderLastExpressions([1, 0])
        } else if spelling == "TRIM" {
            name = "btrim"
            if consumeContextualWord("LEADING") { name = "ltrim" }
            else if consumeContextualWord("TRAILING") { name = "rtrim" }
            else { _ = consumeContextualWord("BOTH") }
            if consumeContextualWord("FROM") {
                arguments = [try parseExpression()]
            } else {
                let first = try parseExpression()
                if consumeContextualWord("FROM") {
                    let source = try parseExpression()
                    arguments = [source, first]
                    try sourceCapture?.reorderLastExpressions([1, 0])
                } else {
                    arguments = [first]
                    if isSymbol(",") { advance(); arguments.append(try parseExpression()) }
                }
            }
        } else {
            let source = try parseExpression()
            arguments = [source]
            if consumeContextualWord("FROM") {
                arguments.append(try parseExpression())
                if consumeContextualWord("FOR") { arguments.append(try parseExpression()) }
            } else if isContextualWord("FOR") {
                let clauseStart = tokenStart
                let clauseEnd = tokenEnd
                advance()
                let length = try parseExpression()
                let offset: Expression
                if consumeContextualWord("FROM") { offset = try parseExpression() }
                else {
                    offset = try makeStructuralNode(.literal(.queryValue(.primitive(.int32(1)), type: .sql(.integer))),
                        at: clauseStart, end: clauseEnd)
                }
                arguments = [source, offset, length]
                try sourceCapture?.reorderLastExpressions([0, 2, 1])
            } else {
                while isSymbol(",") { advance(); arguments.append(try parseExpression()) }
            }
        }
        for _ in arguments { try admitCollectionElement() }
        try expect(")")
        return try makeStructuralNode(.function(FunctionCall(name: name, arguments: arguments)), at: start, identifierEnd: identifierEnd)
    }

    private func isContextualWord(_ word: String) -> Bool {
        switch currentToken {
        case .keyword(let value): return value == word
        case .identifier(let value): return !tokenIsQuoted && asciiUppercase(value) == word
        default: return false
        }
    }

    private static let literalTypeWords: Set<String> = [
        "DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL", "UUID", "BYTEA",
        "JSON", "JSONB", "NUMERIC", "DECIMAL", "DEC", "BOOLEAN", "BOOL", "SMALLINT", "INT2",
        "INT", "INTEGER", "INT4", "BIGINT", "INT8", "REAL", "FLOAT4", "FLOAT8", "DOUBLE",
        "CHAR", "CHARACTER", "VARCHAR", "TEXT", "BINARY", "VARBINARY", "BLOB"
    ]

    private func parseConstructor(array: Bool, at start: Int? = nil) throws -> Expression {
        let previousColonRole = colonStartsSlice
        colonStartsSlice = false
        defer { colonStartsSlice = previousColonRole }
        let constructorStart = start ?? tokenStart
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        try expect(array ? "[" : "(")
        let end = array ? "]" : ")"
        var elements: [Expression] = []
        if !isSymbol(end) {
            while true {
                try admitCollectionElement()
                if array && isSymbol("[") {
                    elements.append(try parseConstructor(array: true))
                } else {
                    elements.append(try parseExpression())
                }
                guard isSymbol(",") else { break }
                advance()
            }
        }
        try expect(end)
        return try makeStructuralNode(array ? .array(elements) : .row(elements), at: constructorStart)
    }

    private func parseCastExpression() throws -> Expression {
        let previousColonRole = colonStartsSlice
        colonStartsSlice = false
        defer { colonStartsSlice = previousColonRole }
        let start = tokenStart
        try expect("CAST")
        try expect("(")
        let value = try parseExpression()
        try expect("AS")
        let targetType = try parseDataType()
        try expect(")")
        return try normalizedCast(value, as: targetType, at: start)
    }

    private func parseDataType() throws -> DataType {
        let typeStart = tokenStart
        var typeIdentifierEnd = tokenEnd
        let exactName: String
        let quoted = tokenIsQuoted
        switch currentToken {
        case .identifier(let name): exactName = name
        case .keyword(let name): exactName = name.lowercased()
        default: exactName = ""
        }
        if !quoted, case .keyword(let word) = currentToken,
           Self.reservedWords.contains(word) || Self.columnNameWords.contains(word) && !Self.literalTypeWords.contains(word) {
            throw ParseError.unexpectedToken(expected: "SQL data type", found: tokenDescription(currentToken), position: tokenStart)
        }
        guard let firstWord = consumeDataTypeWord() else {
            throw ParseError.unexpectedToken(
                expected: "SQL data type",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }

        let baseType: DataType
        switch quoted || isSymbol(".") ? "" : firstWord {
        case "BOOLEAN", "BOOL":
            baseType = .boolean
        case "SMALLINT", "INT2":
            baseType = .smallint
        case "INTEGER", "INT", "INT4":
            baseType = .integer
        case "BIGINT", "INT8":
            baseType = .bigint
        case "REAL", "FLOAT4":
            baseType = .real
        case "FLOAT8":
            baseType = .doublePrecision
        case "DOUBLE":
            try expectContextualWord("PRECISION")
            baseType = .doublePrecision
        case "DECIMAL", "DEC", "NUMERIC":
            baseType = try parseDecimalType()
        case "CHAR", "CHARACTER":
            if consumeContextualWord("VARYING") {
                baseType = .varchar(
                    length: try parseOptionalPositiveTypeParameter(
                        for: "CHARACTER VARYING"
                    )
                )
            } else {
                baseType = .char(
                    length: try parseOptionalPositiveTypeParameter(
                        for: firstWord
                    )
                )
            }
        case "VARCHAR":
            baseType = .varchar(
                length: try parseOptionalPositiveTypeParameter(
                    for: firstWord
                )
            )
        case "TEXT":
            baseType = .text
        case "DATE":
            baseType = .date
        case "TIME", "TIMETZ":
            let precision = try parseTemporalPrecision()
            let withTimeZone = firstWord == "TIMETZ" ? true : try parseOptionalTimeZoneQualifier()
            baseType = .time(withTimeZone: withTimeZone, precision: precision)
        case "TIMESTAMP", "TIMESTAMPTZ":
            let precision = try parseTemporalPrecision()
            let withTimeZone = firstWord == "TIMESTAMPTZ" ? true : try parseOptionalTimeZoneQualifier()
            baseType = .timestamp(withTimeZone: withTimeZone, precision: precision)
        case "INTERVAL":
            let fields = try parseIntervalFields()
            let precision = try parseTemporalPrecision()
            let interval = DataType.interval(fields: fields, precision: precision)
            guard QueryValueValidator.accepts(.primitive(.null), type: .sql(interval)) else {
                throw ParseError.invalidSyntax(message: "Invalid interval field precision", position: typeStart)
            }
            baseType = interval
        case "BINARY":
            baseType = .binary(
                length: try parseOptionalPositiveTypeParameter(
                    for: firstWord
                )
            )
        case "VARBINARY":
            baseType = .varbinary(
                length: try parseOptionalPositiveTypeParameter(
                    for: firstWord
                )
            )
        case "BLOB", "BYTEA":
            baseType = .blob
        case "JSON":
            baseType = .json
        case "JSONB":
            baseType = .jsonb
        case "UUID":
            baseType = .uuid
        default:
            if isSymbol(".") {
                advance()
                let name = try parseRequiredIdentifier("type name after schema", allowsKeywordLabel: true)
                guard !isSymbol(".") else {
                    throw ParseError.unsupportedFeature("Multi-part custom type name")
                }
                typeIdentifierEnd = previousTokenEnd
                baseType = .custom(name, schema: exactName)
            } else {
                baseType = .custom(exactName)
            }
        }

        var type = try makeStructuralNode(baseType, at: typeStart, identifierEnd: typeIdentifierEnd)
        while true {
            if consumeContextualWord("ARRAY") {
                if case .array = type {} else { type = try makeStructuralNode(.array(type), at: typeStart) }
                continue
            }
            if isSymbol("[") {
                advance()
                if case .number = currentToken { _ = try parseTypeParameter(named: "array dimension", allowsZero: false) }
                try expect("]")
                if case .array = type {} else { type = try makeStructuralNode(.array(type), at: typeStart) }
                continue
            }
            return type
        }
    }

    private func consumeDataTypeWord() -> String? {
        switch currentToken {
        case .identifier(let value), .keyword(let value):
            advance()
            return asciiUppercase(value)
        default:
            return nil
        }
    }

    private func parseDecimalType() throws -> DataType {
        guard isSymbol("(") else {
            return .decimal(precision: nil, scale: nil)
        }
        advance()
        let precisionPosition = tokenStart
        let precision = try parseTypeParameter(
            named: "DECIMAL precision",
            allowsZero: false
        )
        guard precision <= 38 else { throw ParseError.invalidSyntax(message: "NUMERIC precision must be in 1...38", position: precisionPosition) }
        var scale: Int?
        if isSymbol(",") {
            advance()
            let negative = isSymbol("-")
            if negative || isSymbol("+") { advance() }
            let scalePosition = tokenStart
            let magnitude = try parseTypeParameter(
                named: "DECIMAL scale",
                allowsZero: true
            )
            guard magnitude <= 1000 else { throw ParseError.invalidSyntax(message: "NUMERIC scale must be in -1000...1000", position: scalePosition) }
            scale = negative ? -magnitude : magnitude
        }
        try expect(")")
        return .decimal(precision: precision, scale: scale)
    }

    private func parseOptionalPositiveTypeParameter(
        for typeName: String
    ) throws -> Int? {
        guard isSymbol("(") else { return nil }
        advance()
        let value = try parseTypeParameter(
            named: "\(typeName) length",
            allowsZero: false
        )
        try expect(")")
        return value
    }

    private func parseTypeParameter(
        named name: String,
        allowsZero: Bool
    ) throws -> Int {
        guard case .number(let lexicalForm) = currentToken,
              let value = UInt64(lexicalForm),
              let result = Int(exactly: value),
              allowsZero || result > 0 else {
            throw ParseError.invalidSyntax(
                message: "\(name) must be \(allowsZero ? "a non-negative" : "a positive") integer",
                position: tokenStart
            )
        }
        advance()
        return result
    }

    private func parseTemporalPrecision() throws -> Int? {
        guard isSymbol("(") else { return nil }
        advance()
        let precisionPosition = tokenStart
        let value = try parseTypeParameter(named: "temporal precision", allowsZero: true)
        guard value <= 6 else { throw ParseError.invalidSyntax(message: "Temporal precision must be in 0...6", position: precisionPosition) }
        try expect(")")
        return value
    }

    private func parseOptionalTimeZoneQualifier() throws -> Bool {
        if consumeContextualWord("WITH") {
            try expectContextualWord("TIME")
            try expectContextualWord("ZONE")
            return true
        }
        if consumeContextualWord("WITHOUT") {
            try expectContextualWord("TIME")
            try expectContextualWord("ZONE")
        }
        return false
    }

    private func parseAggregate() throws -> Expression {
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        let aggregateStart = tokenStart
        let aggregateIdentifierEnd = tokenEnd
        let funcName = currentToken
        advance()
        try expect("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        var arg: Expression?
        if case .symbol("*") = currentToken {
            advance()
            arg = nil
        } else {
            arg = try parseExpression()
        }

        var separator: String?
        if funcName == .keyword("GROUP_CONCAT"), isSymbol(",") {
            advance()
            guard case .string(let value) = currentToken else {
                throw ParseError.invalidSyntax(
                    message: "GROUP_CONCAT separator must be a string literal",
                    position: tokenStart
                )
            }
            separator = value
            advance()
        }

        var aggregateOrderBy: [SortKey]?
        if funcName == .keyword("ARRAY_AGG"),
           case .keyword("ORDER") = currentToken {
            advance()
            try expect("BY")
            aggregateOrderBy = try parseOrderBy()
        }

        try expect(")")

        if arg == nil, funcName != .keyword("COUNT") {
            throw ParseError.invalidSyntax(
                message: "Only COUNT may use '*' as its aggregate argument",
                position: tokenStart
            )
        }
        if arg == nil, distinct {
            throw ParseError.invalidSyntax(
                message: "COUNT(DISTINCT *) is not valid",
                position: tokenStart
            )
        }

        switch funcName {
        case .keyword("COUNT"):
            return try makeAggregateExpression(
                .count(arg, distinct: distinct), at: aggregateStart, identifierEnd: aggregateIdentifierEnd
            )
        case .keyword("SUM"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .sum(operand, distinct: distinct), at: aggregateStart, identifierEnd: aggregateIdentifierEnd
            )
        case .keyword("AVG"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .avg(operand, distinct: distinct), at: aggregateStart, identifierEnd: aggregateIdentifierEnd
            )
        case .keyword("MIN"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.min(operand), at: aggregateStart, identifierEnd: aggregateIdentifierEnd)
        case .keyword("MAX"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(.max(operand), at: aggregateStart, identifierEnd: aggregateIdentifierEnd)
        case .keyword("ARRAY_AGG"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .arrayAgg(
                    operand,
                    orderBy: aggregateOrderBy,
                    distinct: distinct
                ), at: aggregateStart, identifierEnd: aggregateIdentifierEnd
            )
        case .keyword("GROUP_CONCAT"):
            let operand = try arg ?? makeLiteralExpression(.null)
            return try makeAggregateExpression(
                .groupConcat(
                    operand,
                    separator: separator,
                    distinct: distinct
                ), at: aggregateStart, identifierEnd: aggregateIdentifierEnd
            )
        default:
            throw ParseError.invalidSyntax(message: "Unknown aggregate function", position: tokenStart)
        }
    }

    private func parseCaseExpression() throws -> Expression {
        let caseStart = tokenStart
        try expect("CASE")

        let operand = isKeyword("WHEN") ? nil : try parseExpression()
        guard isKeyword("WHEN") else {
            throw ParseError.unexpectedToken(expected: "WHEN", found: tokenDescription(currentToken), position: tokenStart)
        }
        var cases: [CaseWhenPair] = []
        while case .keyword("WHEN") = currentToken {
            advance()
            let condition = try parseExpression()
            try expect("THEN")
            let result = try parseExpression()
            try admitCollectionElement()
            cases.append(
                try makeStructuralNode(
                    CaseWhenPair(condition: condition, result: result)
                )
            )
        }

        var elseResult: Expression?
        if case .keyword("ELSE") = currentToken {
            advance()
            elseResult = try parseExpression()
        }

        try expect("END")

        return try makeStructuralNode(
            .caseWhen(operand: operand, cases: cases, elseResult: elseResult), at: caseStart
        )
    }

    private func parseOutputReferenceExpression() throws -> Expression {
        let start = tokenStart
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = true
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        let expression = try parseExpression()
        var operand = expression
        var negative = false
        while case .negate(let child) = operand {
            negative.toggle()
            operand = child
        }
        let position: Int64
        switch operand {
        case .literal(.queryValue(.primitive(.int32(let value)), _)): position = Int64(value)
        case .literal(.queryValue(.primitive(.int64(let value)), _)): position = value
        default: return expression
        }
        let signed = negative ? Int64.zero.subtractingReportingOverflow(position) : (partialValue: position, overflow: false)
        guard !signed.overflow else { return expression }
        _ = sourceCapture?.discardLastExpression()
        return try makeLiteralExpression(.int(signed.partialValue), at: start)
    }

    private func parseExpressionList(outputReferences: Bool = false) throws -> [Expression] {
        var exprs: [Expression] = []
        var first = true
        while first || isSymbol(",") {
            if !first { advance() }
            first = false
            try admitCollectionElement()
            exprs.append(try outputReferences ? parseOutputReferenceExpression() : parseExpression())
        }
        return exprs
    }

    private func parseOrderBy(outputReferences: Bool = false) throws -> [SortKey] {
        var keys: [SortKey] = []
        var first = true
        while first || isSymbol(",") {
            if !first { advance() }
            first = false
            let expr = try outputReferences ? parseOutputReferenceExpression() : parseExpression()
            var direction: SortDirection = .ascending
            if case .keyword("DESC") = currentToken {
                direction = .descending
                advance()
            } else if case .keyword("ASC") = currentToken {
                advance()
            }
            var nulls: NullOrdering?
            if case .keyword("NULLS") = currentToken {
                advance()
                if case .keyword("FIRST") = currentToken {
                    nulls = .first
                    advance()
                } else if case .keyword("LAST") = currentToken {
                    nulls = .last
                    advance()
                } else {
                    throw ParseError.unexpectedToken(expected: "FIRST or LAST", found: tokenDescription(currentToken), position: tokenStart)
                }
            }
            try admitCollectionElement()
            keys.append(
                try makeStructuralNode(
                    SortKey(expr, direction: direction, nulls: nulls)
                )
            )
        }
        return keys
    }

    private func parseInsertQuery() throws -> InsertQuery {
        try expect("INSERT")
        try expect("INTO")
        let target = try parseNamedTableReference(allowsImplicitAlias: false)

        let columns: [String]?
        if isSymbol("(") {
            columns = try parseIdentifierList(
                openingConsumed: false,
                expected: "INSERT column"
            )
        } else {
            columns = nil
        }

        let source: InsertSource
        if isKeyword("VALUES") {
            advance()
            var rows: [[Expression]] = []
            repeat {
                try admitCollectionElement()
                try expect("(")
                var row: [Expression] = []
                guard !isSymbol(")") else {
                    throw ParseError.invalidSyntax(
                        message: "INSERT VALUES rows cannot be empty",
                        position: tokenStart
                    )
                }
                while true {
                    try admitCollectionElement()
                    row.append(try parseExpression())
                    guard isSymbol(",") else { break }
                    advance()
                }
                try expect(")")
                rows.append(row)
                guard isSymbol(",") else { break }
                advance()
            } while true
            let valuesSource = InsertSource.values(rows)
            source = try makeStructuralNode(valuesSource)
        } else if isKeyword("SELECT") || isKeyword("WITH") {
            let query = try parseSelectQuery()
            source = try makeStructuralNode(.select(query))
        } else if isKeyword("DEFAULT") {
            advance()
            try expect("VALUES")
            source = try makeStructuralNode(.defaultValues)
        } else {
            throw ParseError.unexpectedToken(
                expected: "VALUES, SELECT, WITH, or DEFAULT VALUES",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }

        var onConflict: OnConflictAction?
        if isKeyword("ON") {
            advance()
            try expect("CONFLICT")
            try expect("DO")
            if isKeyword("NOTHING") {
                advance()
                onConflict = try makeStructuralNode(.doNothing)
            } else if isKeyword("UPDATE") {
                advance()
                try expect("SET")
                let assignments = try parseAssignments()
                let predicate: Expression?
                if isKeyword("WHERE") {
                    advance()
                    predicate = try parseExpression()
                } else {
                    predicate = nil
                }
                onConflict = try makeStructuralNode(
                    .doUpdate(
                        assignments: assignments,
                        where: predicate
                    )
                )
            } else {
                throw ParseError.unexpectedToken(
                    expected: "NOTHING or UPDATE after ON CONFLICT DO",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            InsertQuery(
                target: target,
                columns: columns,
                source: source,
                onConflict: onConflict,
                returning: returning
            )
        )
    }

    private func parseUpdateQuery() throws -> UpdateQuery {
        try expect("UPDATE")
        let target = try parseNamedTableReference(implicitAliasTerminators: ["SET"])
        try expect("SET")
        let assignments = try parseAssignments()

        let source: DataSource?
        if isKeyword("FROM") {
            advance()
            source = try parseDataSource()
        } else {
            source = nil
        }

        let filter: Expression?
        if isKeyword("WHERE") {
            advance()
            filter = try parseExpression()
        } else {
            filter = nil
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            UpdateQuery(
                target: target,
                assignments: assignments,
                from: source,
                filter: filter,
                returning: returning
            )
        )
    }

    private func parseDeleteQuery() throws -> DeleteQuery {
        try expect("DELETE")
        try expect("FROM")
        let target = try parseNamedTableReference(implicitAliasTerminators: ["USING", "WHERE", "RETURNING"])

        let source: DataSource?
        if isKeyword("USING") {
            advance()
            source = try parseDataSource()
        } else {
            source = nil
        }

        let filter: Expression?
        if isKeyword("WHERE") {
            advance()
            filter = try parseExpression()
        } else {
            filter = nil
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            DeleteQuery(
                target: target,
                using: source,
                filter: filter,
                returning: returning
            )
        )
    }

    private func parseCreateGraph() throws -> CreateGraphStatement {
        try expect("PROPERTY")
        try expect("GRAPH")

        let ifNotExists: Bool
        if isKeyword("IF") {
            advance()
            try expect("NOT")
            try expect("EXISTS")
            ifNotExists = true
        } else {
            ifNotExists = false
        }

        guard let graphName = identifierText else {
            throw ParseError.unexpectedToken(
                expected: "property graph name",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
        advance()

        try expect("VERTEX")
        try expect("TABLES")
        try expect("(")
        var vertexTables: [VertexTableDefinition] = []
        guard !isSymbol(")") else {
            throw ParseError.invalidSyntax(
                message: "CREATE PROPERTY GRAPH requires at least one vertex table",
                position: tokenStart
            )
        }
        while true {
            try admitCollectionElement()
            vertexTables.append(try parseVertexTableDefinition())
            guard isSymbol(",") else { break }
            advance()
        }
        try expect(")")

        var edgeTables: [EdgeTableDefinition] = []
        if isKeyword("EDGE") {
            advance()
            try expect("TABLES")
            try expect("(")
            guard !isSymbol(")") else {
                throw ParseError.invalidSyntax(
                    message: "EDGE TABLES cannot be empty",
                    position: tokenStart
                )
            }
            while true {
                try admitCollectionElement()
                edgeTables.append(try parseEdgeTableDefinition())
                guard isSymbol(",") else { break }
                advance()
            }
            try expect(")")
        }

        return try makeStructuralNode(
            CreateGraphStatement(
                graphName: graphName,
                ifNotExists: ifNotExists,
                vertexTables: vertexTables,
                edgeTables: edgeTables
            )
        )
    }

    private func parseVertexTableDefinition() throws -> VertexTableDefinition {
        let tableName = try parseRequiredIdentifier("vertex table name")
        let alias = try parseOptionalAlias()
        try expect("KEY")
        let keyColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "vertex key column"
        )
        let label = try parseOptionalLabelExpression()
        let properties = try parseOptionalPropertiesSpec()
        return try makeStructuralNode(
            VertexTableDefinition(
                tableName: tableName,
                alias: alias,
                keyColumns: keyColumns,
                labelExpression: label,
                propertiesSpec: properties
            )
        )
    }

    private func parseEdgeTableDefinition() throws -> EdgeTableDefinition {
        let tableName = try parseRequiredIdentifier("edge table name")
        let alias = try parseOptionalAlias()
        try expect("KEY")
        let keyColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "edge key column"
        )
        try expectContextualWord("SOURCE")
        let source = try parseVertexReference(role: .source)
        try expect("DESTINATION")
        let destination = try parseVertexReference(role: .destination)
        let label = try parseOptionalLabelExpression()
        let properties = try parseOptionalPropertiesSpec()
        return try makeStructuralNode(
            EdgeTableDefinition(
                tableName: tableName,
                alias: alias,
                keyColumns: keyColumns,
                sourceVertex: source,
                destinationVertex: destination,
                labelExpression: label,
                propertiesSpec: properties
            )
        )
    }

    private func parseVertexReference(
        role: EdgeEndpointRole
    ) throws -> VertexReference {
        try expect("KEY")
        let sourceColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "\(role.rawValue) edge key column"
        )
        try expect("REFERENCES")
        let tableName = try parseRequiredIdentifier(
            "\(role.rawValue) referenced vertex table"
        )
        let targetColumns = try parseIdentifierList(
            openingConsumed: false,
            expected: "\(role.rawValue) referenced key column"
        )
        guard sourceColumns.count == targetColumns.count else {
            throw ParseError.invalidSyntax(
                message: "\(role.displayName) key and referenced key widths must match",
                position: tokenStart
            )
        }

        var mappings: [KeyColumnMapping] = []
        mappings.reserveCapacity(sourceColumns.count)
        for index in sourceColumns.indices {
            try admitCollectionElement()
            mappings.append(
                try makeStructuralNode(
                    KeyColumnMapping(
                        source: sourceColumns[index],
                        target: targetColumns[index]
                    )
                )
            )
        }
        return try makeStructuralNode(
            VertexReference(tableName: tableName, keyColumns: mappings)
        )
    }

    private func parseOptionalAlias() throws -> String? {
        guard isKeyword("AS") else { return nil }
        advance()
        return try parseRequiredIdentifier("table alias")
    }

    private func parseOptionalLabelExpression() throws -> LabelExpression? {
        guard isKeyword("LABEL") else { return nil }
        advance()
        return try parseLabelOrExpression()
    }

    private func parseLabelOrExpression() throws -> LabelExpression {
        let first = try parseLabelAndExpression()
        var expressions: [LabelExpression]?
        while isSymbol("|") {
            if expressions == nil {
                try admitCollectionElement(amount: 2)
                expressions = [first]
            } else {
                try admitCollectionElement()
            }
            advance()
            expressions?.append(try parseLabelAndExpression())
        }
        guard let expressions else { return first }
        return try makeStructuralNode(.or(expressions))
    }

    private func parseLabelAndExpression() throws -> LabelExpression {
        let first = try parseLabelPrimaryExpression()
        var expressions: [LabelExpression]?
        while isSymbol("&") {
            if expressions == nil {
                try admitCollectionElement(amount: 2)
                expressions = [first]
            } else {
                try admitCollectionElement()
            }
            advance()
            expressions?.append(try parseLabelPrimaryExpression())
        }
        guard let expressions else { return first }
        return try makeStructuralNode(.and(expressions))
    }

    private func parseLabelPrimaryExpression() throws -> LabelExpression {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        if isSymbol("(") {
            advance()
            if case .identifier(let column) = currentToken,
               peekNextToken() == .symbol(")") {
                advance()
                try expect(")")
                return try makeStructuralNode(.column(column))
            }
            let expression = try parseLabelOrExpression()
            try expect(")")
            return expression
        }

        let label = try parseRequiredIdentifier("label name")
        return try makeStructuralNode(.single(label))
    }

    private func parseOptionalPropertiesSpec() throws -> PropertiesSpec? {
        if isKeyword("NO") {
            advance()
            try expect("PROPERTIES")
            return try makeStructuralNode(PropertiesSpec.none)
        }
        guard isKeyword("PROPERTIES") else { return nil }
        advance()

        if isKeyword("ALL") {
            advance()
            try expect("COLUMNS")
            if isKeyword("EXCEPT") {
                advance()
                let columns = try parseIdentifierList(
                    openingConsumed: false,
                    expected: "excluded property column"
                )
                return try makeStructuralNode(.allExcept(columns))
            }
            return try makeStructuralNode(.all)
        }

        let columns = try parseIdentifierList(
            openingConsumed: false,
            expected: "property column"
        )
        return try makeStructuralNode(.columns(columns))
    }

    private func parseRequiredIdentifier(
        _ expected: String,
        allowsKeywordLabel: Bool = false
    ) throws -> String {
        guard let identifier = allowsKeywordLabel ? labelText : identifierText else {
            throw ParseError.unexpectedToken(
                expected: expected,
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
        advance()
        return identifier
    }

    private func parseAssignments() throws -> [Assignment] {
        var assignments: [Assignment] = []
        while true {
            guard let column = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: "assignment column",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            advance()
            try expect("=")
            let value = try parseExpression()
            try admitCollectionElement()
            assignments.append(
                try makeStructuralNode(
                    Assignment(column: column, value: value)
                )
            )
            guard isSymbol(",") else { break }
            advance()
        }
        return assignments
    }

    private func parseReturningClause() throws -> [ProjectionItem]? {
        guard isKeyword("RETURNING") else { return nil }
        advance()

        var items: [ProjectionItem] = []
        while true {
            let expression = try parseExpression()
            let alias: String?
            if isKeyword("AS") {
                advance()
                guard let name = labelText else {
                    throw ParseError.unexpectedToken(
                        expected: "RETURNING alias",
                        found: tokenDescription(currentToken),
                        position: tokenStart
                    )
                }
                alias = name
                advance()
            } else {
                alias = nil
            }
            try admitCollectionElement()
            items.append(
                try makeStructuralNode(
                    ProjectionItem(expression, alias: alias)
                )
            )
            guard isSymbol(",") else { break }
            advance()
        }
        return items
    }

    private func parseIdentifierList(
        openingConsumed: Bool,
        expected: String
    ) throws -> [String] {
        if !openingConsumed {
            try expect("(")
        }
        var identifiers: [String] = []
        while true {
            guard let identifier = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: expected,
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            try admitCollectionElement()
            identifiers.append(identifier)
            advance()
            guard isSymbol(",") else { break }
            advance()
        }
        try expect(")")
        return identifiers
    }
}

// MARK: - GRAPH_TABLE Parsing (ISO/IEC 9075-16:2023 SQL/PGQ)

extension SQLParser {
    /// Parse GRAPH_TABLE clause
    /// Syntax: GRAPH_TABLE(graphName, MATCH pattern [COLUMNS (...)])
    private func parseGraphTable() throws -> DataSource {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        try expect("GRAPH_TABLE")
        try expect("(")

        // Graph name
        guard let graphName = identifierText else {
            throw ParseError.unexpectedToken(
                expected: "graph name",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }
        advance()

        try expect(",")

        // MATCH pattern
        let matchPattern = try parseMatchPattern()

        // Optional COLUMNS clause
        var columns: [GraphTableColumn]?
        if case .keyword("COLUMNS") = currentToken {
            columns = try parseColumnsClause()
        }

        try expect(")")

        let alias: String?
        if case .keyword("AS") = currentToken {
            advance()
            guard let name = identifierText else {
                throw ParseError.unexpectedToken(
                    expected: "GRAPH_TABLE alias",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            alias = name
            advance()
        } else if let name = identifierText {
            alias = name
            advance()
        } else {
            alias = nil
        }

        let source = try makeStructuralNode(
            GraphTableSource(
                graphName: graphName,
                matchPattern: matchPattern,
                columns: columns,
                alias: alias
            )
        )

        return try makeStructuralNode(.graphTable(source))
    }

    /// Parse MATCH pattern
    /// Syntax: MATCH [mode] pathPattern [, pathPattern] [WHERE expr]
    private func parseMatchPattern() throws -> MatchPattern {
        try expect("MATCH")

        // Parse path patterns
        var paths: [PathPattern] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            let path = try parsePathPattern()
            try admitCollectionElement()
            paths.append(path)
        }

        // Optional WHERE clause
        var whereExpr: Expression?
        if case .keyword("WHERE") = currentToken {
            advance()
            whereExpr = try parseExpression()
        }

        return try makeStructuralNode(
            MatchPattern(paths: paths, where: whereExpr)
        )
    }

    /// Parse path pattern
    /// Syntax: [pathVar =] [mode] pathElement pathElement ...
    private func parsePathPattern() throws -> PathPattern {
        var pathVariable: String?
        var mode: PathMode?

        // Check for path variable: p = ...
        if let name = identifierText {
            let next = peekNextToken()
            if case .symbol("=") = next {
                pathVariable = name
                advance()  // identifier
                advance()  // =
            }
        }

        // Check for path mode
        if consumeContextualWord("ANY") {
            try expect("SHORTEST")
            _ = consumeContextualWord("PATH")
            mode = .anyShortest
        }
        if mode == nil, case .keyword(let kw) = currentToken {
            switch kw {
            case "WALK":
                mode = .walk
                advance()
            case "TRAIL":
                mode = .trail
                advance()
            case "ACYCLIC":
                mode = .acyclic
                advance()
            case "SIMPLE":
                mode = .simple
                advance()
            case "SHORTEST":
                // Check for ALL SHORTEST
                advance()
                if case .number = currentToken {
                    mode = .shortestK(try parseTypeParameter(named: "shortest path count", allowsZero: false))
                } else if consumeContextualWord("PATH") {
                    mode = .anyShortest
                } else if case .keyword("ALL") = currentToken {
                    advance()
                    try expectContextualWord("PATH")
                    mode = .allShortest
                } else {
                    mode = .anyShortest
                }
            case "ALL":
                advance()
                try expect("SHORTEST")
                // PATH is optional
                consumeContextualWord("PATH")
                mode = .allShortest
            default:
                break
            }
        }

        var elements: [PathElement] = []
        try admitCollectionElement()
        elements.append(try parsePathNodeOrGroup())
        while isSymbol("-") || isSymbol("<") || isSymbol("<-") || isSymbol("->") || isSymbol("(") {
            if isSymbol("(") {
                try admitCollectionElement()
                elements.append(try parsePathNodeOrGroup())
            } else {
                let edge = try parseEdgePattern()
                try admitCollectionElement()
                elements.append(try makeStructuralNode(.edge(edge)))
                try admitCollectionElement()
                elements.append(try parsePathNodeOrGroup())
            }
        }

        return try makeStructuralNode(
            PathPattern(
                pathVariable: pathVariable,
                elements: elements,
                mode: mode
            )
        )
    }

    private func parsePathNodeOrGroup() throws -> PathElement {
        guard isSymbol("(") else {
            throw ParseError.unexpectedToken(expected: "node or grouped path", found: tokenDescription(currentToken), position: tokenStart)
        }
        let next = peekNextToken()
        let grouped: Bool
        switch next {
        case .symbol("("): grouped = true
        case .keyword(let word) where ["WALK", "TRAIL", "ACYCLIC", "SIMPLE", "SHORTEST", "ALL", "ANY"].contains(word):
            let following = peekNextToken(2)
            grouped = following == .symbol("(") || following == .keyword("SHORTEST") || word == "SHORTEST" && { if case .number = following { return true }; return false }()
        case .identifier: grouped = peekNextToken(2) == .symbol("=")
        default: grouped = false
        }
        guard grouped else {
            let node = try parseNodePattern()
            return try makeStructuralNode(.node(node))
        }
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        advance()
        var alternatives: [PathPattern] = []
        try admitCollectionElement()
        alternatives.append(try parsePathPattern())
        while isSymbol("|") {
            advance()
            try admitCollectionElement()
            alternatives.append(try parsePathPattern())
        }
        try expect(")")
        let quantifier: PathQuantifier?
        if isSymbol("*") { advance(); quantifier = .zeroOrMore }
        else if isSymbol("+") { advance(); quantifier = .oneOrMore }
        else if isSymbol("?") { advance(); quantifier = .zeroOrOne }
        else if isSymbol("{") {
            advance()
            let minimum: Int?
            if isSymbol(",") { minimum = nil }
            else { minimum = try parseTypeParameter(named: "path quantifier", allowsZero: true) }
            if isSymbol(",") {
                advance()
                let maximum = isSymbol("}") ? nil : try parseTypeParameter(named: "path quantifier", allowsZero: true)
                guard minimum != nil || maximum != nil, minimum == nil || maximum == nil || minimum! <= maximum! else {
                    throw ParseError.invalidSyntax(message: "Invalid path quantifier range", position: tokenStart)
                }
                quantifier = .range(min: minimum, max: maximum)
            } else {
                guard let minimum else { throw ParseError.invalidSyntax(message: "Missing path quantifier", position: tokenStart) }
                quantifier = .exactly(minimum)
            }
            try expect("}")
        } else { quantifier = nil }
        let inner: PathPattern
        if alternatives.count == 1 { inner = alternatives[0] }
        else {
            let alternate = try makeStructuralNode(PathElement.alternation(alternatives))
            if quantifier == nil { return alternate }
            inner = try makeStructuralNode(PathPattern(elements: [alternate]))
        }
        guard let quantifier else {
            throw ParseError.invalidSyntax(message: "Grouped path requires a quantifier or alternation", position: tokenStart)
        }
        return try makeStructuralNode(.quantified(inner, quantifier: quantifier))
    }

    /// Parse node pattern
    /// Syntax: (var:Label {prop: val})
    private func parseNodePattern() throws -> NodePattern {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        try expect("(")

        var variable: String?
        var labels: [String]?
        var properties: [PropertyBinding]?

        if let name = identifierText {
            variable = name
            advance()
        }
        while isSymbol(":") {
            advance()
            let label = try parseRequiredIdentifier("graph label")
            try admitCollectionElement()
            if labels == nil { labels = [] }
            labels?.append(label)
        }

        // Parse properties: {prop: val, ...}
        if isSymbol("{") {
            advance()
            properties = []

            var first = true
            while first || isSymbol(",") {
                if !first { advance() }
                first = false

                guard let propName = identifierText else {
                    break
                }
                advance()

                try expect(":")

                let expr = try parseExpression()
                try admitCollectionElement()
                properties?.append(
                    try makeStructuralNode(
                        PropertyBinding(key: propName, value: expr)
                    )
                )
            }

            try expect("}")
        }

        try expect(")")

        return try makeStructuralNode(
            NodePattern(
                variable: variable,
                labels: labels,
                properties: properties
            )
        )
    }

    /// Parse edge pattern using state machine
    /// Grammar: EdgePattern ::= StartSymbol [EdgeDetails] EndSymbol
    /// Reference: ISO/IEC 9075-16:2023 SQL/PGQ
    private func parseEdgePattern() throws -> EdgePattern {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }

        // State 1: Parse start symbol
        enum StartSymbol {
            case hyphen       // -
            case leftArrow    // <-
            case leftAngle    // <
            case rightArrow   // -> (complete anonymous)
        }

        let start: StartSymbol
        if isSymbol("<-") {
            advance()
            start = .leftArrow
        } else if isSymbol("->") {
            advance()
            start = .rightArrow
        } else if isSymbol("<") {
            advance()
            // Check if next token is [ (immediate bracket) or - (explicit hyphen)
            if isSymbol("[") {
                // <[...] pattern: immediate bracket without explicit hyphen
                start = .leftAngle
            } else {
                // <-[...] or <- pattern: explicit hyphen
                try expect("-")
                start = .leftAngle
            }
        } else if isSymbol("-") {
            advance()
            start = .hyphen
        } else {
            throw ParseError.unexpectedToken(
                expected: "edge pattern start (-, <-, <, or ->)",
                found: tokenDescription(currentToken),
                position: tokenStart
            )
        }

        // State 2: Check for edge details [...]
        var variable: String?
        var labels: [String]?
        var properties: [PropertyBinding]?
        var hasDetails = false

        if isSymbol("[") {
            // Validate: only -> is a complete arrow that cannot have details
            if start == .rightArrow {
                throw ParseError.invalidSyntax(
                    message: "Edge brackets must come before arrow direction (use -[...]->, not ->[...])",
                    position: tokenStart
                )
            }

            hasDetails = true
            advance()

            if let name = identifierText {
                variable = name
                advance()
            }
            while isSymbol(":") {
                advance()
                let label = try parseRequiredIdentifier("graph label")
                try admitCollectionElement()
                if labels == nil { labels = [] }
                labels?.append(label)
            }

            // Parse properties: {prop: val, ...}
            if isSymbol("{") {
                advance()
                properties = []

                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false

                    guard let propName = identifierText else {
                        break
                    }
                    advance()

                    try expect(":")

                    let expr = try parseExpression()
                    try admitCollectionElement()
                    properties?.append(
                        try makeStructuralNode(
                            PropertyBinding(key: propName, value: expr)
                        )
                    )
                }

                try expect("}")
            }

            try expect("]")
        }

        // State 3: Parse end symbol (only if not a complete arrow)
        enum EndSymbol {
            case rightArrow   // ->
            case hyphen       // -
            case none         // ε (empty)
        }

        let end: EndSymbol

        if start == .rightArrow {
            // Complete anonymous arrow (->): no end symbol expected
            if hasDetails {
                // Defensive check (should have been caught above)
                throw ParseError.invalidSyntax(
                    message: "Internal error: -> with details",
                    position: tokenStart
                )
            }
            end = .none
        } else {
            // Expect end symbol for incomplete starts (-, <)
            if isSymbol("->") {
                advance()
                end = .rightArrow
            } else if isSymbol("-") {
                advance()
                if isSymbol(">") {
                    // -> split into two tokens
                    advance()
                    end = .rightArrow
                } else {
                    end = .hyphen
                }
            } else {
                end = .none
            }
        }

        // Resolve direction using type-safe pattern matching
        // Reference: ISO/IEC 9075-16:2023 direction resolution table
        let direction: EdgeDirection

        switch (start, end, hasDetails) {
        // Patterns with brackets
        case (.hyphen, .rightArrow, true):
            direction = .outgoing  // -[...]->
        case (.leftArrow, .hyphen, true), (.leftAngle, .hyphen, true):
            direction = .incoming  // <-[...]- or <[...]-
        case (.hyphen, .hyphen, true):
            direction = .undirected  // -[...]-
        case (.leftArrow, .rightArrow, true), (.leftAngle, .rightArrow, true):
            direction = .any  // <-[...]-> or <[...]->

        // Anonymous edges (no brackets)
        case (.rightArrow, .none, false):
            direction = .outgoing  // ->
        case (.leftArrow, .none, false):
            direction = .incoming  // <-
        case (.hyphen, .none, false):
            direction = .undirected  // - (anonymous)

        // Patterns without brackets but with end symbol (rare)
        case (.hyphen, .rightArrow, false):
            direction = .outgoing  // -->
        case (.leftArrow, .hyphen, false), (.leftAngle, .hyphen, false):
            direction = .incoming  // <--
        case (.hyphen, .hyphen, false):
            direction = .undirected  // --

        default:
            throw ParseError.invalidSyntax(
                message: "Invalid edge pattern combination",
                position: tokenStart
            )
        }

        return try makeStructuralNode(
            EdgePattern(
                variable: variable,
                labels: labels,
                properties: properties,
                direction: direction
            )
        )
    }

    /// Parse COLUMNS clause
    /// Syntax: COLUMNS (expr AS alias, ...)
    private func parseColumnsClause() throws -> [GraphTableColumn] {
        try expect("COLUMNS")
        try expect("(")

        var columns: [GraphTableColumn] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            let expr = try parseExpression()

            try expect("AS")

            guard let alias = labelText else {
                throw ParseError.unexpectedToken(
                    expected: "column alias",
                    found: tokenDescription(currentToken),
                    position: tokenStart
                )
            }
            advance()

            try admitCollectionElement()
            columns.append(
                try makeStructuralNode(
                    GraphTableColumn(expression: expr, alias: alias)
                )
            )
        }

        try expect(")")

        return columns
    }

    private func isTypedLiteralPrefix() -> Bool {
        do {
            return try withLookahead {
                _ = try parseDataType()
                return currentToken.isString
            }
        } catch {
            return false
        }
    }

    /// Peek at the next token without consuming current token.
    private func peekNextToken(_ distance: Int = 1) -> Token {
        withLookahead {
            for _ in 0..<distance { advance() }
            return currentToken
        }
    }

    private func withLookahead<Result>(_ operation: () throws -> Result) rethrows -> Result {
        let savedPosition = position
        let savedStart = tokenStart
        let savedEnd = tokenEnd
        let savedPreviousEnd = previousTokenEnd
        let savedQuoted = tokenIsQuoted
        let savedLexicalError = lexicalError
        let savedToken = currentToken
        let savedAnonymousParameterPosition = anonymousParameterPosition
        let savedPositionalParameterStyle = positionalParameterStyle
        let savedStructuralLedger = structuralLedger
        let savedSourceCapture = sourceCapture
        sourceCapture = nil
        let savedStructuralError = structuralError
        defer {
            position = savedPosition
            tokenStart = savedStart
            tokenEnd = savedEnd
            previousTokenEnd = savedPreviousEnd
            tokenIsQuoted = savedQuoted
            lexicalError = savedLexicalError
            currentToken = savedToken
            anonymousParameterPosition = savedAnonymousParameterPosition
            positionalParameterStyle = savedPositionalParameterStyle
            structuralLedger = savedStructuralLedger
            sourceCapture = savedSourceCapture
            structuralError = savedStructuralError
        }
        return try operation()
    }
}
