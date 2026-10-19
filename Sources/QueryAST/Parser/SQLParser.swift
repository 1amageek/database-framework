/// SQLParser.swift
/// SQL Query Parser
///
/// Reference:
/// - ISO/IEC 9075:2023 (SQL)
/// - ISO/IEC 9075-16:2023 (SQL/PGQ)

import DatabaseKit
import DatabaseTypes

/// SQL Parser for converting SQL strings to AST
public final class SQLParser {
    /// Parser errors
    public enum ParseError: Error, Sendable, Equatable {
        case unexpectedToken(expected: String, found: String, position: Int)
        case unexpectedEndOfInput(expected: String, position: Int)
        case invalidSyntax(message: String, position: Int)
        case unsupportedFeature(String)
        case literalError(message: String, position: Int, failure: SQLConstantFailure)
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

    /// Renders an identifier the way PostgreSQL's quote_ident does: bare when it
    /// is a lowercase regular identifier that is not a reserved, type/function or
    /// column-name keyword, otherwise double-quoted with embedded quotes doubled.
    public static func displayIdentifier(_ name: String) -> String {
        let scalars = name.unicodeScalars
        let regular = !scalars.isEmpty && scalars.allSatisfy {
            ("a"..."z").contains($0) || ("0"..."9").contains($0) || $0 == "_"
        } && !("0"..."9").contains(scalars.first!)
        let upper = name.uppercased()
        if regular, !reservedWords.contains(upper), !typeFunctionWords.contains(upper), !columnNameWords.contains(upper) {
            return name
        }
        return "\"" + name.map { $0 == "\"" ? "\"\"" : String($0) }.joined() + "\""
    }

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
        "COLLATE", "FETCH", "NEXT", "ONLY", "TIES", "ROW",
        "MERGE", "TRUNCATE", "MATCHED",
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
    private var implicitRowStart: Int?
    private let preservesCastOutputNames: Bool
    /// Source span and output name of the most recent cast folded to a literal.
    private var lastFoldedCast: (start: Int, end: Int, name: String)?
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

    /// - Parameter preservesCastOutputNames: Names an unaliased projection item
    ///   whose spanning cast folds to a constant after that cast, as SQL text
    ///   execution presents it. The canonical AST cannot distinguish such a
    ///   constant from one without cast syntax, so the default keeps the item
    ///   unaliased and canonical SQL round-trips unchanged.
    public init(
        structuralLimits: QueryStructuralLimits = .default,
        preservesCastOutputNames: Bool = false
    ) {
        self.preservesCastOutputNames = preservesCastOutputNames
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
        implicitRowStart = nil
        lastFoldedCast = nil
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

    /// A word naming an unknown option value is a value failure; any other token is syntax.
    private func unrecognizedOptionValue(_ option: String) -> ParseError {
        switch currentToken {
        case .identifier, .keyword:
            return .literalError(message: "Unrecognized \(option) value", position: tokenStart,
                failure: .invalidOptionValue)
        default:
            return .unexpectedToken(expected: option, found: tokenDescription(currentToken), position: tokenStart)
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
        case .keyword("WITH"):
            return try parseWithStatement()
        case .keyword("SELECT"), .keyword("VALUES"), .symbol("("):
            return .select(try parseSelectQuery())
        case .keyword("INSERT"):
            let start = tokenStart
            return try captureMutation(.insert(try parseInsertQuery()), start: start)
        case .keyword("UPDATE"):
            let start = tokenStart
            return try captureMutation(.update(try parseUpdateQuery()), start: start)
        case .keyword("DELETE"):
            let start = tokenStart
            return try captureMutation(.delete(try parseDeleteQuery()), start: start)
        case .keyword("MERGE"):
            let start = tokenStart
            return try captureMutation(.merge(try parseMergeQuery()), start: start)
        case .keyword("TRUNCATE"):
            let start = tokenStart
            return try captureMutation(.truncate(try parseTruncateQuery()), start: start)
        case .keyword("CREATE"):
            let start = tokenStart
            advance()
            if case .keyword("PROPERTY") = currentToken {
                return .createGraph(try parseCreateGraph())
            }
            return try captureDDL(parseCreateDDL(), start: start)
        case .keyword("DROP"):
            let start = tokenStart
            advance()
            if consumeContextualWord("PROPERTY") {
                try expect("GRAPH")
                return .dropGraph(try parseRequiredIdentifier("property graph name"))
            }
            return try captureDDL(parseDropDDL(), start: start)
        case .identifier(let word) where !tokenIsQuoted && ["alter", "refresh"].contains(word):
            let start = tokenStart
            let refresh = word == "refresh"
            advance()
            return try captureDDL(refresh ? parseRefreshDDL() : parseAlterDDL(), start: start)
        default:
            throw ParseError.invalidSyntax(
                message: "Expected statement keyword",
                position: tokenStart
            )
        }
    }

    private var startsQuery: Bool {
        withLookahead {
            var depth: UInt64 = 0
            while isSymbol("(") {
                guard depth < structuralLedger.remainingLimits.maximumNestingDepth else { return false }
                depth += 1
                advance()
            }
            return isKeyword("SELECT") || isKeyword("WITH") || isKeyword("VALUES")
        }
    }

    private func parseSelectQuery(startingWith initial: SelectQuery? = nil, at start: Int? = nil) throws -> SelectQuery {
        let enclosingOutputReferenceCasts = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosingOutputReferenceCasts }
        let queryStart = start ?? tokenStart
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        let ctes = initial == nil && isKeyword("WITH") ? try parseWithClause() : nil
        let parenthesized = initial == nil && isSymbol("(")
        var query = try parseSetQuery(minimumPrecedence: 0, startingWith: initial)
        let hasTail = isKeyword("ORDER") || isKeyword("LIMIT") || isKeyword("OFFSET") || isKeyword("FETCH")
        let preservesInnerTail = parenthesized && hasTail && (query.orderBy != nil || query.pagination != QueryPagination())
        if preservesInnerTail || ctes != nil && query.subqueries != nil {
            let source = try makeStructuralNode(DataSource.subquery(query))
            try sourceCapture?.recordSyntheticProjection(ledger: &structuralLedger)
            try sourceCapture?.swapLastPending()
            query = try makeStructuralNode(SelectQuery(projection: [.wildcard()], source: source))
        }
        if ctes == nil, !hasTail { return query }
        try sourceCapture?.unwrapLastQuery()
        var order: [SortKey]?
        if isKeyword("ORDER") {
            advance()
            try expect("BY")
            order = try parseOrderBy(outputReferences: true)
        }
        var limit: QueryLimit?
        var offset: QueryCount?
        var limitCapture: Int?
        while isKeyword("LIMIT") || isKeyword("OFFSET") || isKeyword("FETCH") {
            if isKeyword("OFFSET") {
                guard offset == nil else { throw ParseError.invalidSyntax(message: "Repeated OFFSET", position: tokenStart) }
                advance()
                offset = try parseQueryCount()
                if isKeyword("ROW") || isKeyword("ROWS") { advance() }
            } else if isKeyword("LIMIT") {
                guard limit == nil else { throw ParseError.invalidSyntax(message: "Repeated query limit", position: tokenStart) }
                advance()
                if isKeyword("ALL") { advance(); limit = .all }
                else {
                    limitCapture = sourceCapture?.pendingCheckpoint
                    limit = .count(try parseQueryCount(), withTies: false)
                }
            } else {
                guard limit == nil else { throw ParseError.invalidSyntax(message: "Repeated query limit", position: tokenStart) }
                advance()
                guard isKeyword("FIRST") || isKeyword("NEXT") else {
                    throw ParseError.unexpectedToken(expected: "FIRST or NEXT", found: tokenDescription(currentToken), position: tokenStart)
                }
                advance()
                let count: QueryCount
                if isKeyword("ROW") || isKeyword("ROWS") { count = .fixed(1) }
                else {
                    limitCapture = sourceCapture?.pendingCheckpoint
                    count = try parseQueryCount()
                }
                guard isKeyword("ROW") || isKeyword("ROWS") else {
                    throw ParseError.unexpectedToken(expected: "ROW or ROWS", found: tokenDescription(currentToken), position: tokenStart)
                }
                advance()
                let ties: Bool
                if isKeyword("ONLY") { advance(); ties = false }
                else { try expect("WITH"); try expect("TIES"); ties = true }
                limit = .count(count, withTies: ties)
            }
        }
        if let limitCapture, case .count(.expression, _) = limit {
            try sourceCapture?.movePendingToEnd(from: limitCapture, count: 1)
        }
        if let ctes { query = query.replacing(subqueries: ctes.definitions).replacing(withRecursive: ctes.recursive) }
        if let order { query = query.replacing(orderBy: order) }
        if limit != nil || offset != nil { query = query.replacing(pagination: QueryPagination(limit: limit, offset: offset)) }
        return try makeStructuralNode(query, at: queryStart)
    }

    private func parseQueryCount() throws -> QueryCount {
        if case .number(let text) = currentToken, let value = UInt64(text) {
            let next = peekNextToken()
            let boundary: Bool
            switch next {
            case .eof, .symbol(")"), .symbol(";"), .keyword("LIMIT"), .keyword("OFFSET"),
                 .keyword("FETCH"), .keyword("ROW"), .keyword("ROWS"), .keyword("UNION"),
                 .keyword("INTERSECT"), .keyword("EXCEPT"): boundary = true
            default: boundary = false
            }
            if boundary { advance(); return try makeStructuralNode(QueryCount.fixed(value)) }
        }
        return .expression(try parseExpression())
    }

    private func parseSetQuery(minimumPrecedence: Int, startingWith initial: SelectQuery? = nil) throws -> SelectQuery {
        var left: SelectQuery
        if let initial { left = initial }
        else { left = try parseQueryOperand() }
        while isKeyword("UNION") || isKeyword("INTERSECT") || isKeyword("EXCEPT") {
            let precedence = isKeyword("INTERSECT") ? 2 : 1
            guard precedence >= minimumPrecedence else { break }
            let operation: SetOperator = isKeyword("UNION") ? .union : isKeyword("INTERSECT") ? .intersect : .except
            advance()
            let quantifier: SetQuantifier
            if isKeyword("ALL") { advance(); quantifier = .all }
            else { if isKeyword("DISTINCT") { advance() }; quantifier = .distinct }
            let right = try parseSetQuery(minimumPrecedence: precedence + 1)
            let source = try makeStructuralNode(DataSource.setOperation(operator: operation, quantifier: quantifier, left: left, right: right))
            try sourceCapture?.recordSyntheticProjection(ledger: &structuralLedger)
            try sourceCapture?.swapLastPending()
            left = try makeStructuralNode(SelectQuery(projection: [.wildcard()], source: source))
        }
        return left
    }

    private func parseQueryOperand() throws -> SelectQuery {
        if isSymbol("(") {
            try enterStructuralNesting()
            defer { leaveStructuralNesting() }
            advance()
            let query = try parseSelectQuery()
            try expect(")")
            return query
        }
        if isKeyword("VALUES") {
            try sourceCapture?.recordSyntheticProjection(ledger: &structuralLedger)
            let source = try parseValuesSource()
            return try makeStructuralNode(SelectQuery(projection: [.wildcard()], source: source))
        }
        let start = tokenStart
        try expect("SELECT")
        let distinctCapture = sourceCapture?.pendingCheckpoint
        var distinctKeyCount = 0
        let quantifier: SelectQuantifier
        if isKeyword("DISTINCT") {
            advance()
            if isKeyword("ON") {
                advance(); try expect("(")
                let keys = try parseExpressionList()
                try expect(")")
                distinctKeyCount = keys.count
                quantifier = .distinctOn(keys)
            } else { quantifier = .distinct }
        } else {
            if isKeyword("ALL") { advance() }
            quantifier = .all
        }
        let projection = try parseProjection()
        let source: DataSource
        if isKeyword("FROM") { advance(); source = try parseDataSource() }
        else {
            try sourceCapture?.recordValuesRow(count: 0, start: nil, end: previousTokenEnd, ledger: &structuralLedger)
            source = try makeStructuralNode(.values([[]]))
        }
        var filter: Expression?
        if isKeyword("WHERE") { advance(); filter = try parseExpression() }
        var group: [GroupingElement]?
        if isKeyword("GROUP") { advance(); try expect("BY"); group = try parseGroupingList() }
        var having: Expression?
        if isKeyword("HAVING") { advance(); having = try parseExpression() }
        let windows = try parseNamedWindows()
        if let distinctCapture, distinctKeyCount > 0 {
            try sourceCapture?.movePendingToEnd(from: distinctCapture, count: distinctKeyCount)
        }
        return try makeStructuralNode(SelectQuery(projection: projection, source: source, filter: filter,
            groupBy: group, having: having, windows: windows, quantifier: quantifier), at: start)
    }

    private func parseValuesSource() throws -> DataSource {
        let start = tokenStart
        try expect("VALUES")
        var rows: [[Expression]] = []
        repeat {
            if !rows.isEmpty { advance() }
            try structuralLedger.consume(.valuesRows)
            try admitCollectionElement()
            let rowStart = tokenStart
            try expect("(")
            guard !isSymbol(")") else { throw ParseError.invalidSyntax(message: "SQL VALUES rows cannot be empty", position: tokenStart) }
            var row: [Expression] = []
            repeat {
                if !row.isEmpty { advance() }
                try structuralLedger.consume(.valuesCells)
                try admitCollectionElement()
                row.append(try parseExpression())
            } while isSymbol(",")
            try expect(")")
            try sourceCapture?.recordValuesRow(count: row.count, start: rowStart, end: previousTokenEnd, ledger: &structuralLedger)
            rows.append(row)
        } while isSymbol(",") && peekNextToken() == .symbol("(")
        return try makeStructuralNode(.values(rows), at: start)
    }

    private func parseWithClause() throws -> (definitions: [NamedSubquery], recursive: Bool) {
        let parsed = try parseWithDefinitions(statementBodies: false)
        var definitions: [NamedSubquery] = []
        for value in parsed.definitions {
            guard case .select(let query) = value.statement else { throw ParseError.unsupportedFeature("Writable WITH requires a statement boundary") }
            definitions.append(NamedSubquery(name: value.name, columns: value.columns, query: query,
                materialized: value.materialized, search: value.search, cycle: value.cycle))
        }
        return (definitions, parsed.recursive)
    }

    private func parseWithStatement() throws -> QueryStatement {
        let start = tokenStart
        try enterStructuralNesting(); defer { leaveStructuralNesting() }
        let definitions = try parseWithDefinitions(statementBodies: true)
        if definitions.definitions.allSatisfy({ if case .select = $0.statement { return true }; return false }), startsQuery {
            try sourceCapture?.normalizeSelectDefinitions(count: definitions.definitions.count, ledger: &structuralLedger)
            var query = try parseSelectQuery()
            if query.subqueries != nil {
                let source = try makeStructuralNode(DataSource.subquery(query))
                try sourceCapture?.recordSyntheticProjection(ledger: &structuralLedger)
                try sourceCapture?.swapLastPending()
                query = try makeStructuralNode(SelectQuery(projection: [.wildcard()], source: source))
            }
            try sourceCapture?.unwrapLastQuery()
            let ctes = definitions.definitions.map { value -> NamedSubquery in
                guard case .select(let query) = value.statement else { preconditionFailure("SELECT definitions already checked") }
                return NamedSubquery(name: value.name, columns: value.columns, query: query,
                    materialized: value.materialized, search: value.search, cycle: value.cycle)
            }
            query = query.replacing(subqueries: ctes).replacing(withRecursive: definitions.recursive)
            return .select(try makeStructuralNode(query, at: start))
        }
        let body = try parseStatement()
        guard isAdmittedWithBody(body) else { throw ParseError.unsupportedFeature("WITH main statement kind") }
        let statement = QueryStatement.with(SQLWithStatement(recursive: definitions.recursive,
            statements: definitions.definitions, body: body))
        try sourceCapture?.recordMutation(statement, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(statement)
    }

    private func isAdmittedWithBody(_ statement: QueryStatement) -> Bool {
        switch statement {
        case .select, .insert, .update, .delete, .merge: return true
        default: return false
        }
    }

    private func captureMutation(_ statement: QueryStatement, start: Int) throws -> QueryStatement {
        try sourceCapture?.recordMutation(statement, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return statement
    }

    private func parseWithDefinitions(statementBodies: Bool) throws -> (definitions: [SQLNamedStatement], recursive: Bool) {
        try expect("WITH")

        // RECURSIVE is contextual here so unrelated identifiers retain their
        // ordinary SQL name semantics.
        let recursive = consumeContextualWord("RECURSIVE")

        var subqueries: [SQLNamedStatement] = []
        var first = true

        while first || isSymbol(",") {
            if !first { advance() }
            first = false

            let definitionStart = tokenStart
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
            let statement: QueryStatement = statementBodies ? try parseStatement() : .select(try parseSelectQuery())
            guard isAdmittedWithBody(statement) else { throw ParseError.unsupportedFeature("WITH definition statement kind") }
            try expect(")")
            var search: CTESearchClause?
            if consumeContextualWord("SEARCH") {
                let order: CTESearchOrder
                if consumeContextualWord("DEPTH") { order = .depthFirst }
                else { try expectContextualWord("BREADTH"); order = .breadthFirst }
                try expectContextualWord("FIRST")
                try expect("BY")
                let keys = try parseCTEKeys()
                try expectContextualWord("SET")
                let sequence = try parseRequiredIdentifier("SEARCH sequence column")
                search = try makeStructuralNode(CTESearchClause(order: order, columns: keys, sequenceColumn: sequence))
            }
            var cycle: CTECycleClause?
            if consumeContextualWord("CYCLE") {
                let keys = try parseCTEKeys()
                try expectContextualWord("SET")
                let mark = try parseRequiredIdentifier("CYCLE mark column")
                var markValue: Literal?
                var defaultValue: Literal?
                if consumeContextualWord("TO") {
                    markValue = try parseCTECycleConstant()
                    try expect("DEFAULT")
                    defaultValue = try parseCTECycleConstant()
                }
                try expect("USING")
                let path = try parseRequiredIdentifier("CYCLE path column")
                cycle = try makeStructuralNode(CTECycleClause(columns: keys, markColumn: mark,
                    pathColumn: path, markValue: markValue, defaultValue: defaultValue))
            }

            try admitCollectionElement()
            subqueries.append(
                try makeStructuralNode(
                    SQLNamedStatement(
                        name: name,
                        columns: columnList,
                        statement: statement,
                        materialized: materialized,
                        search: search,
                        cycle: cycle
                    )
                )
            )
            if statementBodies, let value = subqueries.last { try sourceCapture?.recordNamedStatement(value, start: definitionStart, end: previousTokenEnd, ledger: &structuralLedger) }
        }

        return (subqueries, recursive)
    }

    private func parseCTEKeys() throws -> [String] {
        var keys: [String] = []
        repeat {
            if !keys.isEmpty { advance() }
            try admitCollectionElement()
            keys.append(try parseRequiredIdentifier("CTE key column"))
        } while isSymbol(",")
        return keys
    }

    private func parseCTECycleConstant() throws -> Literal {
        let start = tokenStart
        let accepted: Bool
        switch currentToken {
        case .number, .string, .keyword("TRUE"), .keyword("FALSE"), .keyword("NULL"): accepted = true
        default:
            accepted = !tokenIsQuoted && labelText.map { Self.literalTypeWords.contains(asciiUppercase($0)) } == true
        }
        guard accepted else {
            throw ParseError.invalidSyntax(message: "CYCLE mark and default require SQL constants", position: start)
        }
        let expression = try parsePrimaryExpression()
        let literal: Literal
        if case .literal(let value) = expression {
            literal = value
        } else if case .cast(.literal(.sqlUnknownString(let text)), targetType: .date) = expression {
            do {
                literal = try SQLConstantSyntax.cycleDate(text, limits: constantLimits(reservingNodes: 3))
            } catch let error as SQLConstantError {
                throw ParseError.literalError(message: error.description, position: start, failure: error.failure)
            }
            _ = sourceCapture?.discardLastExpression()
            _ = try makeLiteralExpression(literal, at: start)
        } else {
            throw ParseError.unsupportedFeature("CYCLE constant has no intrinsic literal representation")
        }
        try sourceCapture?.convertLastExpressionToLiteral()
        return literal
    }

    private func parseProjection() throws -> [ProjectionItem] {
        var items: [ProjectionItem] = []
        repeat {
            if !items.isEmpty { advance() }
            try admitCollectionElement()
            if isSymbol("*") {
                let start = tokenStart
                advance()
                try sourceCapture?.recordProjectionQualifier(start: start, end: previousTokenEnd, identifierEnd: nil, ledger: &structuralLedger)
                items.append(.wildcard())
            } else if let qualifier = identifierText, peekNextToken() == .symbol("."), peekNextToken(2) == .symbol("*") {
                let start = tokenStart, identifierEnd = tokenEnd
                advance(); try expect("."); try expect("*")
                try sourceCapture?.recordProjectionQualifier(start: start, end: previousTokenEnd, identifierEnd: identifierEnd, ledger: &structuralLedger)
                items.append(.wildcard(qualifier: qualifier))
            } else {
                let itemStart = tokenStart
                let expression = try parseExpression()
                let itemEnd = previousTokenEnd
                var alias: String?
                if isKeyword("AS") { advance(); alias = try parseRequiredIdentifier("projection alias", allowsKeywordLabel: true) }
                else if let name = identifierText { alias = name; advance() }
                // A folded cast keeps the output name of the cast it replaced.
                else if preservesCastOutputNames, case .literal = expression, let folded = lastFoldedCast,
                        folded.start == itemStart, folded.end == itemEnd {
                    alias = folded.name
                }
                items.append(try makeStructuralNode(ProjectionItem(expression, alias: alias)))
            }
        } while isSymbol(",")
        return items
    }

    private func parseDataSource() throws -> DataSource {
        var source = try parseJoinedSource()
        while isSymbol(",") {
            advance()
            let lateral = isKeyword("LATERAL")
            if lateral { advance() }
            let right = try parseJoinedSource()
            source = try makeStructuralNode(.join(JoinClause(type: lateral && !isFunctionSource(right) ? .lateral : .cross, left: source, right: right)))
        }
        return source
    }

    private func parseJoinedSource() throws -> DataSource {
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
            if isFunctionSource(right) {
                if joinType == .lateral { joinType = crossJoin ? .cross : .inner }
                if joinType == .leftLateral { joinType = .left }
            }
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
        let start = tokenStart
        var identifierEnd = tokenEnd
        let explicitLateral = isKeyword("LATERAL")
        if explicitLateral { advance(); identifierEnd = tokenEnd }
        let source: DataSource
        let functionName = labelText
        let functionCall = functionName != nil && (peekNextToken() == .symbol("(")
            || peekNextToken() == .symbol(".") && peekNextToken(3) == .symbol("("))
        if functionCall, let functionName, asciiUppercase(functionName) != "GRAPH_TABLE" {
            let functionStart = tokenStart
            let expression = try parseIdentifierExpression(functionName)
            guard case .function(let function) = expression else {
                throw ParseError.unsupportedFeature("FROM requires an ordinary function call")
            }
            let functionCapture = try sourceCapture?.unwrapLastExpression()
            let ordinality: Bool
            if isKeyword("WITH") { advance(); try expectContextualWord("ORDINALITY"); ordinality = true }
            else { ordinality = false }
            source = try makeStructuralNode(.tableFunction(TableFunctionSource(function: function,
                withOrdinality: ordinality)), at: functionCapture?.range.start ?? functionStart,
                identifierEnd: functionCapture?.identifierRange?.end ?? identifierEnd)
        } else if isKeyword("GRAPH_TABLE"), peekNextToken() == .symbol("(") {
            let savedCapture = sourceCapture
            sourceCapture = nil
            do { source = try parseGraphTable() }
            catch { sourceCapture = savedCapture; throw error }
            sourceCapture = savedCapture
            try sourceCapture?.record(source, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        } else if isSymbol("(") {
            try enterStructuralNesting()
            advance()
            do {
                if isKeyword("VALUES") {
                    let valuesStart = tokenStart
                    let values = try parseValuesSource()
                    if isSymbol(")") { source = values }
                    else {
                        try sourceCapture?.recordSyntheticProjection(ledger: &structuralLedger)
                        try sourceCapture?.swapLastPending()
                        let initial = try makeStructuralNode(SelectQuery(projection: [.wildcard()], source: values), at: valuesStart)
                        let query = try parseSelectQuery(startingWith: initial, at: valuesStart)
                        source = try makeStructuralNode(.subquery(query), at: start)
                    }
                    try expect(")")
                } else if startsQuery {
                    let query = try parseSelectQuery()
                    try expect(")")
                    source = try makeStructuralNode(.subquery(query), at: start)
                } else {
                    source = try parseDataSource()
                    try expect(")")
                }
            } catch { leaveStructuralNesting(); throw error }
            leaveStructuralNesting()
        } else {
            let table = try parseNamedTableReference(allowsImplicitAlias: false, allowsExplicitAlias: false)
            source = try makeStructuralNode(.table(table), at: start, identifierEnd: identifierEnd)
        }
        if explicitLateral, !isFunctionSource(source) {
            throw ParseError.unsupportedFeature("Leading LATERAL requires a function source")
        }
        let explicitAlias = isKeyword("AS")
        if explicitAlias { advance() }
        guard explicitAlias || identifierText != nil else { return source }
        let name = try parseRequiredIdentifier("source alias")
        var columns: [String]?
        if isSymbol("(") {
            advance()
            var names: [String] = []
            repeat {
                if !names.isEmpty { advance() }
                try admitCollectionElement()
                names.append(try parseRequiredIdentifier("source column alias"))
            } while isSymbol(",")
            try expect(")")
            columns = names
        }
        return try makeStructuralNode(.aliased(source, name: name, columns: columns), at: start)
    }

    private func isFunctionSource(_ source: DataSource) -> Bool {
        if case .tableFunction = source { return true }
        if case .aliased(let source, _, _) = source { return isFunctionSource(source) }
        return false
    }

    private func parseNamedTableReference(
        allowsImplicitAlias: Bool = true,
        allowsExplicitAlias: Bool = true,
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
        if allowsExplicitAlias, isKeyword("AS") {
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

    private func parseOrExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var left = try parseAndExpression(startingWith: initial)
        while case .keyword("OR") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = try makeStructuralNode(.or(left, right))
        }
        return left
    }

    private func parseAndExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var left = try parseNotExpression(startingWith: initial)
        while case .keyword("AND") = currentToken {
            advance()
            let right = try parseNotExpression()
            left = try makeStructuralNode(.and(left, right))
        }
        return left
    }

    private func parseNotExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var operatorCount = 0
        while initial == nil, case .keyword("NOT") = currentToken {
            try enterStructuralNesting()
            operatorCount += 1
            advance()
        }
        defer {
            for _ in 0..<operatorCount {
                leaveStructuralNesting()
            }
        }
        var expression = try parseComparisonExpression(startingWith: initial)
        for _ in 0..<operatorCount {
            expression = try makeStructuralNode(.not(expression))
        }
        return expression
    }

    private func parseComparisonExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var expression = try parsePredicateExpression(startingWith: initial)
        while isKeyword("IS") { expression = try parseIsPredicate(expression) }
        return expression
    }

    private func parsePredicateExpression(startingWith initial: Expression? = nil) throws -> Expression {
        let left = try parseConcatenateExpression(startingWith: initial)

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
                if startsQuery {
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
            if startsQuery {
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
                if startsQuery {
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

    private func parseConcatenateExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var left = try parseAddExpression(startingWith: initial)
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

    private func parseAddExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var left = try parseMulExpression(startingWith: initial)
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

    private func parseMulExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var left = try parseAtTimeZoneExpression(startingWith: initial)
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

    private func parseAtTimeZoneExpression(startingWith initial: Expression? = nil) throws -> Expression {
        var value = try parseUnaryExpression(startingWith: initial)
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

    private func parseUnaryExpression(startingWith initial: Expression? = nil) throws -> Expression {
        let unaryStart = tokenStart
        var signs: [Bool] = []
        while initial == nil && (isSymbol("-") || isSymbol("+")) {
            try enterStructuralNesting()
            try admitCollectionElement()
            signs.append(isSymbol("-"))
            advance()
        }
        let signCount = signs.count
        defer { for _ in 0..<signCount { leaveStructuralNesting() } }
        // A sign binds looser than `::`, so a signed cast never spans the operand.
        let operandStart: Int? = initial == nil ? tokenStart : nil
        var expression: Expression
        if let initial {
            expression = initial
        } else if case .number(let text) = currentToken, let negative = signs.popLast() {
            advance()
            expression = try numericExpression((negative ? "-" : "+") + text, at: unaryStart)
        } else {
            expression = try parsePrimaryExpression()
        }
        while isSymbol("::") || isSymbol("[") || isSymbol(".") || isKeyword("COLLATE") {
            if isSymbol("::") {
                let start = tokenStart
                advance()
                expression = try normalizedCast(expression, as: parseDataType(), at: start, spanStart: operandStart)
            } else if isSymbol("[") {
                expression = try parseArrayAccess(expression)
            } else if isSymbol(".") {
                advance()
                if isSymbol("*") { advance(); expression = try makeStructuralNode(.fieldExpansion(expression)) }
                else {
                    let name = try parseRequiredIdentifier("composite field", allowsKeywordLabel: true)
                    expression = try makeStructuralNode(.field(expression, name: name))
                }
            } else {
                advance()
                let position = tokenStart
                let name = try parseRequiredIdentifier("collation")
                let collation: SQLCollation
                switch name { case "C": collation = .c; case "POSIX": collation = .posix
                default: throw ParseError.invalidSyntax(message: "Unsupported collation", position: position) }
                expression = try makeStructuralNode(.collate(expression, collation: collation))
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
            throw ParseError.literalError(message: String(describing: error), position: start, failure: error.failure)
        }
    }

    /// `spanStart` is where the cast's source span begins for output naming,
    /// or `nil` when the operand's start is not known to this parser state.
    private func normalizedCast(_ expression: Expression, as type: DataType, at start: Int, spanStart: Int?) throws -> Expression {
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
                if let spanStart { lastFoldedCast = (spanStart, previousTokenEnd, SQLCastOutputName.name(for: type)) }
                return try makeLiteralExpression(literal, at: min(start, originalRange?.start ?? start))
            }
            return try makeStructuralNode(.cast(expression, targetType: type), at: start)
        } catch let error as SQLConstantError {
            throw ParseError.literalError(message: String(describing: error), position: start, failure: error.failure)
        }
    }

    private func parsePrimaryExpression() throws -> Expression {
        let primaryStart = tokenStart
        if !tokenIsQuoted, let word = labelText {
            let upper = asciiUppercase(word)
            let next = peekNextToken()
            if upper == "DEFAULT" {
                advance()
                return try makeStructuralNode(.defaultValue, at: primaryStart)
            }
            if ["CURRENT_USER", "SESSION_USER", "CURRENT_SCHEMA", "CURRENT_ROLE", "CURRENT_CATALOG"].contains(upper), next != .symbol("(") {
                let identifierEnd = tokenEnd
                advance()
                let name: String
                switch upper {
                case "CURRENT_ROLE": name = "current_user"
                case "CURRENT_CATALOG": name = "current_database"
                default: name = word.lowercased()
                }
                return try makeStructuralNode(.function(FunctionCall(name: name, arguments: [])), at: primaryStart, identifierEnd: identifierEnd)
            }
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
                advance(); try expect("(")
                let query = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.arraySubquery(query), at: primaryStart)
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
                return try normalizedCast(value, as: type, at: start, spanStart: start)
            }
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
            if startsQuery {
                let subquery = try parseSelectQuery()
                try expect(")")
                return try makeStructuralNode(.subquery(subquery))
            }
            let expressionStart = tokenStart
            let expr = try parseExpression()
            if isSymbol(",") {
                try admitCollectionElement()
                var fields = [expr]
                while isSymbol(",") { advance(); try admitCollectionElement(); fields.append(try parseExpression()) }
                try expect(")")
                let row = try makeStructuralNode(Expression.row(fields), at: primaryStart)
                implicitRowStart = primaryStart
                return row
            }
            let expressionEnd = previousTokenEnd
            try expect(")")
            if implicitRowStart == expressionStart { implicitRowStart = primaryStart }
            if let folded = lastFoldedCast, folded.start == expressionStart, folded.end == expressionEnd {
                lastFoldedCast = (primaryStart, previousTokenEnd, folded.name)
            }
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
            let nameStart = tokenEnd
            advance()
            guard tokenStart == nameStart else {
                throw ParseError.invalidSyntax(message: "Named parameter identifier must immediately follow ':'", position: primaryStart)
            }
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
            return try parseIdentifierExpression(labelText ?? "")

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
        return try normalizedCast(value, as: type, at: start, spanStart: start)
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
            if isSymbol("*") {
                let row = try makeStructuralNode(Expression.wholeRow(firstName), at: start, identifierEnd: identifierEnd, end: identifierEnd)
                advance()
                return try makeStructuralNode(.fieldExpansion(row), at: start)
            }
            quoted = tokenIsQuoted
            name = try parseRequiredIdentifier("name after qualifier", allowsKeywordLabel: true)
            schema = firstName
            identifierEnd = previousTokenEnd
        }
        if isSymbol("(") {
            let previousColonRole = colonStartsSlice
            colonStartsSlice = false
            defer { colonStartsSlice = previousColonRole }
            let spelling = asciiUppercase(name)
            let expressionFunctions: Set<String> = ["MERGE_ACTION", "COALESCE", "NULLIF", "GREATEST", "LEAST", "GROUPING", "JSON_ARRAY", "JSON_OBJECT", "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE", "JSON_VALUE", "JSON_EXISTS", "NORMALIZE", "SUBSTRING", "POSITION", "TRIM", "XMLCONCAT", "XMLFOREST"]
            guard schema != nil || quoted || !Self.columnNameWords.contains(spelling) || expressionFunctions.contains(spelling) else {
                throw ParseError.invalidSyntax(message: "Keyword is not a function name", position: start)
            }
            if schema == nil, !quoted, ["SUBSTRING", "POSITION", "TRIM"].contains(spelling) {
                return try parseStringFunction(spelling, start: start, identifierEnd: identifierEnd)
            }
            if let function = SQLAggregateFunction(rawValue: name) {
                return try parseSQLAggregate(function, schema: schema, start: start, identifierEnd: identifierEnd)
            }
            if let function = SQLWindowFunction(rawValue: name) {
                return try parseAnalyticWindow(function, schema: schema, start: start, identifierEnd: identifierEnd)
            }
            if schema == nil, !quoted, spelling == "GROUPING" {
                advance()
                let arguments = try parseExpressionList()
                try expect(")")
                return try makeStructuralNode(.grouping(arguments), at: start, identifierEnd: identifierEnd)
            }
            if schema == nil, !quoted, spelling == "GROUP_CONCAT" {
                return try parseGroupConcat(start: start, identifierEnd: identifierEnd)
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
        struct Frame {
            let array: Bool
            let start: Int
            let resumesExpression: Bool
            var elements: [Expression] = []
            var needsElement = true
            var closing: String { array ? "]" : ")" }
        }
        let previousColonRole = colonStartsSlice
        colonStartsSlice = false
        defer { colonStartsSlice = previousColonRole }
        var frames: [Frame] = []
        var enteredNesting = 0
        defer { for _ in 0..<enteredNesting { leaveStructuralNesting() } }
        func enter() throws {
            try enterStructuralNesting()
            enteredNesting += 1
        }
        func leave() {
            leaveStructuralNesting()
            enteredNesting -= 1
        }
        func push(array: Bool, start: Int, resumesExpression: Bool) throws {
            if resumesExpression { try enter() }
            try enter()
            try structuralLedger.consume(.retainedBytes, amount: UInt64(MemoryLayout<Frame>.stride))
            try expect(array ? "[" : "(")
            frames.append(Frame(array: array, start: start, resumesExpression: resumesExpression))
        }
        try push(array: array, start: start ?? tokenStart, resumesExpression: false)
        while !frames.isEmpty {
            let index = frames.count - 1
            let isArray = frames[index].array
            let closing = frames[index].closing
            let needsElement = frames[index].needsElement
            if needsElement && !(frames[index].elements.isEmpty && isSymbol(closing)) {
                try admitCollectionElement()
                if isArray && isSymbol("[") {
                    try push(array: true, start: tokenStart, resumesExpression: false)
                    continue
                }
                if !tokenIsQuoted, let word = labelText {
                    let upper = asciiUppercase(word)
                    if (upper == "ROW" && peekNextToken() == .symbol("("))
                        || (upper == "ARRAY" && peekNextToken() == .symbol("[")) {
                        let childStart = tokenStart
                        advance()
                        try push(array: upper == "ARRAY", start: childStart, resumesExpression: true)
                        continue
                    }
                }
                let element = try parseExpression()
                frames[index].elements.append(element)
                frames[index].needsElement = false
                continue
            }
            if !needsElement && isSymbol(",") {
                advance()
                frames[index].needsElement = true
                continue
            }
            try expect(closing)
            let frame = frames.removeLast()
            var completed = try makeStructuralNode(frame.array ? .array(frame.elements) : .row(frame.elements), at: frame.start)
            leave()
            guard !frames.isEmpty else { return completed }
            if frame.resumesExpression {
                // Resume postfix and binary syntax without re-entering a full
                // precedence ladder for every consecutive constructor child.
                completed = try parseOrExpression(startingWith: completed)
                leave()
            }
            frames[frames.count - 1].elements.append(completed)
            frames[frames.count - 1].needsElement = false
        }
        throw ParseError.invalidSyntax(message: "Missing constructor", position: tokenStart)
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
        return try normalizedCast(value, as: targetType, at: start, spanStart: start)
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
        guard precision <= 38 else { throw ParseError.literalError(message: "NUMERIC precision must be in 1...38", position: precisionPosition, failure: .invalidTypeModifier) }
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
            guard magnitude <= 1000 else { throw ParseError.literalError(message: "NUMERIC scale must be in -1000...1000", position: scalePosition, failure: .invalidTypeModifier) }
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
        guard case .number(let lexicalForm) = currentToken else {
            throw ParseError.invalidSyntax(
                message: "\(name) must be \(allowsZero ? "a non-negative" : "a positive") integer",
                position: tokenStart
            )
        }
        // A numeric argument outside the admitted range is a value failure, not syntax.
        guard let value = UInt64(lexicalForm), let result = Int(exactly: value),
              allowsZero || result > 0 else {
            throw ParseError.literalError(
                message: "\(name) must be \(allowsZero ? "a non-negative" : "a positive") integer",
                position: tokenStart, failure: .invalidTypeModifier
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
        guard value <= 6 else { throw ParseError.literalError(message: "Temporal precision must be in 0...6", position: precisionPosition, failure: .invalidTypeModifier) }
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
        let targetStart = tokenStart
        let target = try parseNamedTableReference(allowsImplicitAlias: false)
        try sourceCapture?.recordMutationTarget(start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)

        let columns: [String]?
        if isSymbol("(") {
            columns = try parseIdentifierList(
                openingConsumed: false,
                expected: "INSERT column"
            )
        } else {
            columns = nil
        }

        let overriding: SQLIdentityOverride?
        if consumeContextualWord("OVERRIDING") {
            if consumeContextualWord("SYSTEM") { overriding = .systemValue }
            else if consumeContextualWord("USER") { overriding = .userValue }
            else { throw ParseError.unexpectedToken(expected: "SYSTEM or USER", found: tokenDescription(currentToken), position: tokenStart) }
            try expectContextualWord("VALUE")
        } else { overriding = nil }

        let source: InsertSource
        if isKeyword("VALUES") {
            advance()
            var rows: [[Expression]] = []
            repeat {
                try admitCollectionElement()
                let rowStart = tokenStart
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
                try sourceCapture?.recordValuesRow(count: row.count, start: rowStart, end: previousTokenEnd, ledger: &structuralLedger)
                rows.append(row)
                guard isSymbol(",") else { break }
                advance()
            } while true
            let valuesSource = InsertSource.values(rows)
            source = try makeStructuralNode(valuesSource)
        } else if startsQuery {
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

        try sourceCapture?.recordInsertSource(source, end: previousTokenEnd, ledger: &structuralLedger)

        var onConflict: OnConflictAction?
        var conflictTarget: SQLConflictTarget?
        if isKeyword("ON") {
            advance()
            try expect("CONFLICT")
            if isSymbol("(") {
                let targetStart = tokenStart
                let columns = try parseIdentifierList(openingConsumed: false, expected: "conflict column")
                let predicate: Expression?
                if isKeyword("WHERE") { advance(); predicate = try parseExpression() } else { predicate = nil }
                conflictTarget = .columns(columns, where: predicate)
                try sourceCapture?.recordConflictTarget(conflictTarget!, start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)
            } else if isKeyword("ON") {
                let targetStart = tokenStart
                advance(); try expectContextualWord("CONSTRAINT")
                conflictTarget = .constraint(try parseRequiredIdentifier("constraint name"))
                try sourceCapture?.recordConflictTarget(conflictTarget!, start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)
            }
            let actionStart = tokenStart
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
            if let onConflict { try sourceCapture?.recordConflictAction(onConflict, start: actionStart, end: previousTokenEnd, ledger: &structuralLedger) }
        }

        let returning = try parseReturningClause()
        return try makeStructuralNode(
            InsertQuery(
                target: target,
                columns: columns,
                source: source,
                onConflict: onConflict,
                returning: returning.items,
                overriding: overriding,
                conflictTarget: conflictTarget,
                returningAliases: returning.aliases
            )
        )
    }

    private func parseUpdateQuery() throws -> UpdateQuery {
        try expect("UPDATE")
        let targetStart = tokenStart
        let target = try parseNamedTableReference(implicitAliasTerminators: ["SET"])
        try sourceCapture?.recordMutationTarget(start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)
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
                returning: returning.items,
                returningAliases: returning.aliases
            )
        )
    }

    private func parseDeleteQuery() throws -> DeleteQuery {
        try expect("DELETE")
        try expect("FROM")
        let targetStart = tokenStart
        let target = try parseNamedTableReference(implicitAliasTerminators: ["USING", "WHERE", "RETURNING"])
        try sourceCapture?.recordMutationTarget(start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)

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
                returning: returning.items,
                returningAliases: returning.aliases
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
            let start = tokenStart
            let columns: [String]
            if isSymbol("(") { columns = try parseIdentifierList(openingConsumed: false, expected: "assignment column") }
            else { columns = [try parseRequiredIdentifier("assignment column")] }
            try expect("=")
            let rhsStart = tokenStart
            let value = try parseExpression()
            if columns.count > 1 {
                switch value {
                case .row, .subquery: break
                default: throw ParseError.invalidSyntax(message: "Tuple assignment requires ROW or subquery", position: rhsStart)
                }
            }
            try admitCollectionElement()
            let assignment = try makeStructuralNode(Assignment(columns: columns, value: value))
            try sourceCapture?.recordAssignment(assignment, start: start, end: previousTokenEnd, ledger: &structuralLedger)
            assignments.append(assignment)
            guard isSymbol(",") else { break }
            advance()
        }
        return assignments
    }

    private func parseReturningClause() throws -> (items: [ProjectionItem]?, aliases: SQLReturningAliases?) {
        guard isKeyword("RETURNING") else { return (nil, nil) }
        advance()
        var aliases: SQLReturningAliases?
        if isKeyword("WITH") {
            let start = tokenStart
            advance(); try expect("(")
            var old: String?, new: String?
            repeat {
                if old != nil || new != nil { try expect(",") }
                let isOld: Bool
                if consumeContextualWord("OLD") { isOld = true }
                else { try expectContextualWord("NEW"); isOld = false }
                if isOld ? old != nil : new != nil { throw ParseError.invalidSyntax(message: "Duplicate RETURNING transition alias", position: tokenStart) }
                try expect("AS")
                let name = try parseRequiredIdentifier("RETURNING transition alias")
                if isOld { old = name } else { new = name }
            } while isSymbol(",")
            try expect(")")
            aliases = SQLReturningAliases(old: old, new: new)
            try sourceCapture?.recordReturningAliases(start: start, end: previousTokenEnd, ledger: &structuralLedger)
        }
        return (try parseProjection(), aliases)
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

        let source = try makeStructuralNode(
            GraphTableSource(
                graphName: graphName,
                matchPattern: matchPattern,
                columns: columns
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

// MARK: - Grouping, Aggregates, and Windows

extension SQLParser {
    private func parseGroupingList() throws -> [GroupingElement] {
        var elements: [GroupingElement] = []
        repeat {
            if !elements.isEmpty { advance() }
            try admitCollectionElement()
            elements.append(try parseGroupingElement())
        } while isSymbol(",")
        return elements
    }

    private var startsGroupingOperation: Bool {
        if (isContextualWord("ROLLUP") || isContextualWord("CUBE")), peekNextToken() == .symbol("(") { return true }
        return isContextualWord("GROUPING") && (peekNextToken() == .identifier("sets") || peekNextToken() == .keyword("SETS"))
    }

    /// Parse a complete scalar expression before interpreting implicit tuple grouping syntax.
    private func parseGroupingUnitExpressions() throws -> [Expression] {
        let start = tokenStart
        let enclosingRowStart = implicitRowStart
        implicitRowStart = nil
        defer { implicitRowStart = enclosingRowStart }
        let expression = try parseOutputReferenceExpression()
        if case .row(let fields) = expression, implicitRowStart == start {
            try sourceCapture?.unwrapLastRow(count: fields.count)
            return fields
        }
        return [expression]
    }

    private func parseGroupingElement() throws -> GroupingElement {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        let start = tokenStart
        let element: GroupingElement
        if (isContextualWord("ROLLUP") || isContextualWord("CUBE")), peekNextToken() == .symbol("(") {
            let rollup = isContextualWord("ROLLUP")
            advance(); try expect("(")
            var components: [[Expression]] = []
            repeat {
                if !components.isEmpty { advance() }
                try admitCollectionElement()
                let componentStart = tokenStart
                let expressions = try parseGroupingUnitExpressions()
                try sourceCapture?.recordGroupingComponent(count: expressions.count, start: componentStart,
                    end: previousTokenEnd, ledger: &structuralLedger)
                components.append(expressions)
            } while isSymbol(",")
            try expect(")")
            element = rollup ? .rollup(components) : .cube(components)
        } else if isContextualWord("GROUPING"), peekNextToken() == .identifier("sets") || peekNextToken() == .keyword("SETS") {
            advance(); try expectContextualWord("SETS"); try expect("(")
            var sets: [[GroupingElement]] = []
            repeat {
                if !sets.isEmpty { advance() }
                try admitCollectionElement()
                let setStart = tokenStart
                let elements: [GroupingElement]
                if isSymbol("("), peekNextToken() == .symbol(")") {
                    advance(); advance()
                    elements = []
                    try sourceCapture?.recordGroupingSet(count: 0, start: setStart, end: previousTokenEnd, ledger: &structuralLedger)
                } else if startsGroupingOperation {
                    elements = [try parseGroupingElement()]
                    try sourceCapture?.recordGroupingSet(count: 1, start: setStart, end: previousTokenEnd, ledger: &structuralLedger)
                } else {
                    let expressions = try parseGroupingUnitExpressions()
                    try structuralLedger.consume(.totalNodes, amount: UInt64(expressions.count))
                    elements = expressions.map(GroupingElement.expression)
                    try sourceCapture?.recordExpressionGroupingSet(count: expressions.count, start: setStart,
                        end: previousTokenEnd, ledger: &structuralLedger)
                }
                sets.append(elements)
            } while isSymbol(",")
            try expect(")")
            element = .groupingSets(sets)
        } else if isSymbol("("), peekNextToken() == .symbol(")") {
            advance(); advance()
            try sourceCapture?.recordGroupingSet(count: 0, start: start, end: previousTokenEnd, ledger: &structuralLedger)
            element = .groupingSets([[]])
        } else {
            let expressions = try parseGroupingUnitExpressions()
            if expressions.count == 1 {
                element = .expression(expressions[0])
            } else {
                try structuralLedger.consume(.totalNodes, amount: UInt64(expressions.count))
                try sourceCapture?.recordExpressionGroupingSet(count: expressions.count, start: start,
                    end: previousTokenEnd, ledger: &structuralLedger)
                element = .groupingSets([expressions.map(GroupingElement.expression)])
            }
        }
        try structuralLedger.consume(.totalNodes)
        try sourceCapture?.recordGrouping(element, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return element
    }

    private func parseSQLAggregate(_ function: SQLAggregateFunction, schema: String?, start: Int,
                                   identifierEnd: Int) throws -> Expression {
        let enclosing = preservesOutputReferenceCasts
        preservesOutputReferenceCasts = false
        defer { preservesOutputReferenceCasts = enclosing }
        try expect("(")
        let distinct = consumeContextualWord("DISTINCT")
        if !distinct { _ = consumeContextualWord("ALL") }
        var arguments: [Expression] = []
        let star = isSymbol("*")
        if star { advance() }
        else if !isSymbol(")") {
            repeat {
                if !arguments.isEmpty { advance() }
                try admitCollectionElement()
                arguments.append(try parseExpression())
            } while isSymbol(",")
        }
        var orderBy: [SortKey] = []
        if consumeContextualWord("ORDER") { try expect("BY"); orderBy = try parseOrderBy() }
        try expect(")")
        guard !star || function == .count && !distinct,
              function != .count || !arguments.isEmpty || star else {
            throw ParseError.invalidSyntax(message: "Invalid aggregate argument list", position: start)
        }
        var withinGroup: [SortKey]?
        if consumeContextualWord("WITHIN") {
            try expectContextualWord("GROUP"); try expect("("); try expect("ORDER"); try expect("BY")
            withinGroup = try parseOrderBy(); try expect(")")
        }
        var filter: Expression?
        if consumeContextualWord("FILTER") {
            try expect("("); try expect("WHERE"); filter = try parseExpression(); try expect(")")
        }
        let call = try makeStructuralNode(SQLAggregateCall(function: function, arguments: arguments,
            distinct: distinct, orderBy: orderBy, filter: filter, withinGroup: withinGroup, schema: schema))
        if isKeyword("OVER") {
            try sourceCapture?.recordWindowFunction(.aggregate(call), start: start, end: previousTokenEnd,
                identifierEnd: identifierEnd, ledger: &structuralLedger)
            advance()
            let reference = try parseWindowReference()
            let function = try makeStructuralNode(WindowFunction.aggregate(call))
            return try makeStructuralNode(.window(WindowCall(function: function, reference: reference)),
                at: start, identifierEnd: identifierEnd)
        }
        return try makeAggregateExpression(.sql(call), at: start, identifierEnd: identifierEnd)
    }

    private func parseAnalyticWindow(_ function: SQLWindowFunction, schema: String?, start: Int,
                                     identifierEnd: Int) throws -> Expression {
        // Analytic call metadata has no schema field; only the built-in catalog is representable.
        guard schema == nil || schema == "pg_catalog" else {
            throw ParseError.unsupportedFeature("Analytic function schema: " + (schema ?? ""))
        }
        try expect("(")
        let arguments = isSymbol(")") ? [] : try parseExpressionList()
        try expect(")")
        let call = try makeStructuralNode(WindowFunction.analytic(function, arguments: arguments))
        try sourceCapture?.recordWindowFunction(call, start: start, end: previousTokenEnd,
            identifierEnd: identifierEnd, ledger: &structuralLedger)
        try expect("OVER")
        let reference = try parseWindowReference()
        return try makeStructuralNode(.window(WindowCall(function: call, reference: reference)),
            at: start, identifierEnd: identifierEnd)
    }

    private func parseGroupConcat(start: Int, identifierEnd: Int) throws -> Expression {
        try expect("(")
        let distinct = consumeContextualWord("DISTINCT")
        let argument = try parseExpression()
        var separator: String?
        if isSymbol(",") {
            advance()
            guard case .string(let value) = currentToken else {
                throw ParseError.invalidSyntax(message: "GROUP_CONCAT separator must be a string literal", position: tokenStart)
            }
            separator = value; advance()
        }
        try expect(")")
        return try makeAggregateExpression(.groupConcat(argument, separator: separator, distinct: distinct),
            at: start, identifierEnd: identifierEnd)
    }

    private func parseWindowReference() throws -> WindowReference {
        if isSymbol("(") {
            let specification = try parseWindowSpecification()
            return try makeStructuralNode(WindowReference.specification(specification))
        }
        let start = tokenStart
        let name = try parseRequiredIdentifier("window name")
        try sourceCapture?.recordNamedWindowReference(start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(WindowReference.named(name))
    }

    private func parseWindowSpecification() throws -> WindowSpecification {
        try enterStructuralNesting()
        defer { leaveStructuralNesting() }
        let start = tokenStart
        try expect("(")
        var inheritedName: String?
        if !isSymbol(")"), !["PARTITION", "ORDER", "ROWS", "RANGE", "GROUPS"].contains(where: isContextualWord) {
            inheritedName = try parseRequiredIdentifier("inherited window name")
        }
        var partition: [Expression] = []
        if consumeContextualWord("PARTITION") { try expect("BY"); partition = try parseExpressionList() }
        var order: [SortKey] = []
        if consumeContextualWord("ORDER") { try expect("BY"); order = try parseOrderBy() }
        var frame: WindowFrame?
        if isContextualWord("ROWS") || isContextualWord("RANGE") || isContextualWord("GROUPS") {
            let mode: WindowFrameMode = isContextualWord("ROWS") ? .rows : isContextualWord("RANGE") ? .range : .groups
            advance()
            let between = consumeContextualWord("BETWEEN")
            let lower = try parseWindowBound()
            let upper: WindowFrameBound
            if between { try expect("AND"); upper = try parseWindowBound() }
            else { upper = .currentRow }
            var exclusion: WindowFrameExclusion = .noOthers
            if consumeContextualWord("EXCLUDE") {
                if consumeContextualWord("CURRENT") { try expect("ROW"); exclusion = .currentRow }
                else if consumeContextualWord("GROUP") { exclusion = .group }
                else if consumeContextualWord("TIES") { exclusion = .ties }
                else { try expect("NO"); try expectContextualWord("OTHERS") }
            }
            frame = try makeStructuralNode(WindowFrame(mode: mode, start: lower, end: upper, exclusion: exclusion))
        }
        try expect(")")
        let specification = try makeStructuralNode(WindowSpecification(inheritedName: inheritedName,
            partitionBy: partition, orderBy: order, frame: frame))
        try sourceCapture?.recordWindowSpecification(specification, start: start, end: previousTokenEnd,
            ledger: &structuralLedger)
        return specification
    }

    private func parseWindowBound() throws -> WindowFrameBound {
        if consumeContextualWord("UNBOUNDED") {
            if consumeContextualWord("PRECEDING") { return try makeStructuralNode(WindowFrameBound.unboundedPreceding) }
            try expectContextualWord("FOLLOWING"); return try makeStructuralNode(WindowFrameBound.unboundedFollowing)
        }
        if consumeContextualWord("CURRENT") { try expect("ROW"); return try makeStructuralNode(WindowFrameBound.currentRow) }
        let offset = try parseExpression()
        if consumeContextualWord("PRECEDING") { return try makeStructuralNode(WindowFrameBound.preceding(offset)) }
        try expectContextualWord("FOLLOWING"); return try makeStructuralNode(WindowFrameBound.following(offset))
    }

    private func parseNamedWindows() throws -> [NamedWindow] {
        guard consumeContextualWord("WINDOW") else { return [] }
        var windows: [NamedWindow] = []
        repeat {
            if !windows.isEmpty { advance() }
            try admitCollectionElement()
            let start = tokenStart
            let name = try parseRequiredIdentifier("window definition name")
            let identifierEnd = previousTokenEnd
            try expect("AS")
            let specification = try parseWindowSpecification()
            let window = try makeStructuralNode(NamedWindow(name: name, specification: specification))
            try sourceCapture?.recordNamedWindow(start: start, end: previousTokenEnd,
                identifierEnd: identifierEnd, ledger: &structuralLedger)
            windows.append(window)
        } while isSymbol(",")
        return windows
    }
}

extension SQLParser {
    private func parseMergeQuery() throws -> MergeQuery {
        try expect("MERGE")
        if isKeyword("INTO") { advance() }
        let targetStart = tokenStart
        let target = try parseNamedTableReference(implicitAliasTerminators: ["USING"])
        try sourceCapture?.recordMutationTarget(start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)
        try expect("USING")
        let source = try parseDataSource()
        try expect("ON")
        let condition = try parseExpression()
        var clauses: [SQLMergeClause] = []
        while isKeyword("WHEN") {
            let start = tokenStart
            advance()
            let match: SQLMergeMatch
            if isKeyword("NOT") {
                advance(); try expect("MATCHED")
                if isKeyword("BY") {
                    advance()
                    if consumeContextualWord("SOURCE") { match = .notMatchedBySource }
                    else { try expectContextualWord("TARGET"); match = .notMatchedByTarget }
                } else { match = .notMatchedByTarget }
            } else { try expect("MATCHED"); match = .matched }
            let predicate: Expression?
            if isKeyword("AND") { advance(); predicate = try parseExpression() } else { predicate = nil }
            try expect("THEN")
            let actionStart = tokenStart
            let action: SQLMergeAction
            if isKeyword("UPDATE") {
                advance(); try expect("SET"); action = .update(try parseAssignments())
            } else if isKeyword("DELETE") { advance(); action = .delete }
            else if isKeyword("DO") { advance(); try expect("NOTHING"); action = .doNothing }
            else {
                try expect("INSERT")
                let columns = isSymbol("(") ? try parseIdentifierList(openingConsumed: false, expected: "MERGE insert column") : nil
                var overriding: SQLIdentityOverride?
                if consumeContextualWord("OVERRIDING") {
                    if consumeContextualWord("SYSTEM") { overriding = .systemValue }
                    else { try expectContextualWord("USER"); overriding = .userValue }
                    try expectContextualWord("VALUE")
                }
                if isKeyword("DEFAULT") {
                    advance(); try expect("VALUES")
                    action = .insert(columns: columns, values: nil, overriding: overriding)
                } else {
                    try expect("VALUES"); try expect("(")
                    var values: [Expression] = []
                    repeat {
                        if !values.isEmpty { try expect(",") }
                        try admitCollectionElement()
                        values.append(try parseExpression())
                    } while isSymbol(",")
                    try expect(")")
                    action = .insert(columns: columns, values: values, overriding: overriding)
                }
            }
            try sourceCapture?.recordMergeAction(action, start: actionStart, end: previousTokenEnd, ledger: &structuralLedger)
            let clause = try makeStructuralNode(SQLMergeClause(match: match, condition: predicate, action: action))
            try sourceCapture?.recordMergeClause(clause, start: start, end: previousTokenEnd, ledger: &structuralLedger)
            try admitCollectionElement(); clauses.append(clause)
        }
        guard !clauses.isEmpty else { throw ParseError.invalidSyntax(message: "MERGE requires WHEN clauses", position: tokenStart) }
        let returning = try parseReturningClause()
        return try makeStructuralNode(MergeQuery(target: target, source: source, condition: condition,
            clauses: clauses, returning: returning.items, returningAliases: returning.aliases))
    }

    private func parseTruncateQuery() throws -> TruncateQuery {
        try expect("TRUNCATE")
        if isKeyword("TABLE") { advance() }
        var tables: [TableRef] = []
        repeat {
            if !tables.isEmpty { try expect(",") }
            let start = tokenStart
            let table = try parseNamedTableReference(allowsImplicitAlias: false)
            guard table.alias == nil else { throw ParseError.invalidSyntax(message: "TRUNCATE does not admit aliases", position: start) }
            try sourceCapture?.recordMutationTarget(start: start, end: previousTokenEnd, ledger: &structuralLedger)
            try admitCollectionElement(); tables.append(table)
        } while isSymbol(",")
        var identity = SQLTruncateIdentity.continue
        if consumeContextualWord("RESTART") { try expectContextualWord("IDENTITY"); identity = .restart }
        else if consumeContextualWord("CONTINUE") { try expectContextualWord("IDENTITY") }
        var references = SQLTruncateReferences.restrict
        if consumeContextualWord("CASCADE") { references = .cascade }
        else { _ = consumeContextualWord("RESTRICT") }
        return try makeStructuralNode(TruncateQuery(tables: tables, identity: identity, references: references))
    }
}

// MARK: - Schema Definition Parsing

extension SQLParser {
    private func captureDDL(_ ddl: SQLDDLStatement, start: Int) throws -> QueryStatement {
        let statement = QueryStatement.ddl(ddl)
        try sourceCapture?.recordDDL(ddl, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(statement)
    }

    private func ddlName() throws -> TableRef {
        let start = tokenStart
        let name = try parseNamedTableReference(allowsImplicitAlias: false, allowsExplicitAlias: false)
        try sourceCapture?.recordMutationTarget(start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return name
    }

    private func ddlIfExists(negated: Bool = false) throws -> Bool {
        guard consumeContextualWord("IF") else { return false }
        if negated { try expectContextualWord("NOT") }
        try expectContextualWord("EXISTS")
        return true
    }

    private func ddlDropBehavior() -> SQLDropBehavior {
        if consumeContextualWord("CASCADE") { return .cascade }
        _ = consumeContextualWord("RESTRICT")
        return .restrict
    }

    private func ddlNames() throws -> [TableRef] {
        var names: [TableRef] = []
        repeat {
            try admitCollectionElement()
            names.append(try ddlName())
            if !isSymbol(",") { break }
            advance()
        } while true
        return names
    }

    private func ddlColumns() throws -> [String] {
        try parseIdentifierList(openingConsumed: false, expected: "column name")
    }

    private func ddlWithData() throws -> Bool {
        guard consumeContextualWord("WITH") else { return true }
        let no = consumeContextualWord("NO")
        try expectContextualWord("DATA")
        return !no
    }

    private func parseCreateDDL() throws -> SQLDDLStatement {
        let replace: Bool
        if consumeContextualWord("OR") { try expectContextualWord("REPLACE"); replace = true }
        else { replace = false }
        let temporary = consumeContextualWord("TEMP") || consumeContextualWord("TEMPORARY")
        let unique = consumeContextualWord("UNIQUE")
        let materialized = consumeContextualWord("MATERIALIZED")
        if consumeContextualWord("SCHEMA") {
            guard !replace && !temporary && !unique && !materialized else { throw ddlSyntax("Invalid CREATE SCHEMA modifiers") }
            let conditional = try ddlIfExists(negated: true)
            let start = tokenStart
            let name = try parseRequiredIdentifier("schema name")
            try sourceCapture?.recordMutationTarget(start: start, end: previousTokenEnd, ledger: &structuralLedger)
            return .createSchema(name: name, ifNotExists: conditional)
        }
        if consumeContextualWord("TABLE") {
            guard !replace && !unique && !materialized else { throw ddlSyntax("Invalid CREATE TABLE modifiers") }
            let conditional = try ddlIfExists(negated: true)
            let name = try ddlName()
            let source: SQLTableDefinition
            if isSymbol("("), !withLookahead({ advance(); return startsQuery }) {
                let isNames = withLookahead { advance(); guard identifierText != nil else { return false }; advance(); return isSymbol(",") || isSymbol(")") }
                if isNames {
                    let columns = try ddlColumns()
                    try expectContextualWord("AS")
                    source = .query(try parseSelectQuery(), columns: columns, withData: try ddlWithData())
                } else {
                    advance()
                    var columns: [SQLColumnDefinition] = []
                    var constraints: [SQLTableConstraint] = []
                    if !isSymbol(")") {
                        repeat {
                            try admitCollectionElement()
                            if ddlStartsConstraint { constraints.append(try ddlConstraint(column: nil)) }
                            else { columns.append(try ddlColumn()) }
                            if !isSymbol(",") { break }; advance()
                        } while true
                    }
                    try expect(")")
                    source = .columns(columns, constraints: constraints)
                }
            } else {
                try expectContextualWord("AS")
                source = .query(try parseSelectQuery(), columns: nil, withData: try ddlWithData())
            }
            return .createTable(SQLCreateTable(name: name, source: source, temporary: temporary, ifNotExists: conditional))
        }
        if consumeContextualWord("INDEX") {
            guard !replace && !temporary && !materialized else { throw ddlSyntax("Invalid CREATE INDEX modifiers") }
            let conditional = try ddlIfExists(negated: true)
            let targetStart = tokenStart
            let name = TableRef(try parseRequiredIdentifier("index name"))
            try sourceCapture?.recordMutationTarget(start: targetStart, end: previousTokenEnd, ledger: &structuralLedger)
            try expectContextualWord("ON")
            let table = try ddlName()
            try expect("(")
            let keys = try parseOrderBy()
            try sourceCapture?.recordDDLIndexKeys(count: keys.count, ledger: &structuralLedger)
            try expect(")")
            let nulls = try ddlNulls()
            let predicate = consumeContextualWord("WHERE") ? try parseExpression() : nil
            return .createIndex(SQLCreateIndex(name: name, table: table, keys: keys, predicate: predicate,
                unique: unique, nulls: nulls, ifNotExists: conditional))
        }
        if consumeContextualWord("VIEW") {
            guard !temporary && !unique && !(replace && materialized) else { throw ddlSyntax("Invalid CREATE VIEW modifiers") }
            let conditional = materialized ? try ddlIfExists(negated: true) : false
            let name = try ddlName()
            let columns = isSymbol("(") ? try ddlColumns() : nil
            try expectContextualWord("AS")
            let query = try parseSelectQuery()
            if materialized {
                return .createMaterializedView(SQLCreateMaterializedView(name: name, columns: columns, query: query,
                    withData: try ddlWithData(), ifNotExists: conditional))
            }
            return .createView(SQLCreateView(name: name, columns: columns, query: query, orReplace: replace))
        }
        if consumeContextualWord("SEQUENCE") {
            guard !replace && !temporary && !unique && !materialized else { throw ddlSyntax("Invalid CREATE SEQUENCE modifiers") }
            let conditional = try ddlIfExists(negated: true)
            let name = try ddlName()
            let options = try ddlSequenceOptions(allowRestart: false)
            return .createSequence(SQLCreateSequence(name: name, options: options.options, ownedBy: options.ownedBy, ifNotExists: conditional))
        }
        throw ParseError.unsupportedFeature("CREATE statement type")
    }

    private func parseDropDDL() throws -> SQLDDLStatement {
        let materialized = consumeContextualWord("MATERIALIZED")
        let kind = try parseRequiredIdentifier("DROP object kind", allowsKeywordLabel: true)
        guard !materialized || kind == "view" else { throw ddlSyntax("MATERIALIZED requires VIEW") }
        let conditional = try ddlIfExists()
        if kind == "schema" {
            var names: [String] = []
            repeat {
                let start = tokenStart
                try admitCollectionElement(); names.append(try parseRequiredIdentifier("schema name"))
                try sourceCapture?.recordMutationTarget(start: start, end: previousTokenEnd, ledger: &structuralLedger)
                if !isSymbol(",") { break }; advance()
            } while true
            return .dropSchema(names: names, ifExists: conditional, behavior: ddlDropBehavior())
        }
        let names = try ddlNames()
        let behavior = ddlDropBehavior()
        switch kind {
        case "table": return .dropTable(names: names, ifExists: conditional, behavior: behavior)
        case "index": return .dropIndex(names: names, ifExists: conditional, behavior: behavior)
        case "view": return materialized ? .dropMaterializedView(names: names, ifExists: conditional, behavior: behavior) : .dropView(names: names, ifExists: conditional, behavior: behavior)
        case "sequence": return .dropSequence(names: names, ifExists: conditional, behavior: behavior)
        default: throw ParseError.unsupportedFeature("DROP \(kind)")
        }
    }

    private func parseRefreshDDL() throws -> SQLDDLStatement {
        try expectContextualWord("MATERIALIZED"); try expectContextualWord("VIEW")
        return .refreshMaterializedView(name: try ddlName(), withData: try ddlWithData())
    }

    private func parseAlterDDL() throws -> SQLDDLStatement {
        if consumeContextualWord("SCHEMA") {
            let start = tokenStart
            let name = try parseRequiredIdentifier("schema name")
            try sourceCapture?.recordMutationTarget(start: start, end: previousTokenEnd, ledger: &structuralLedger)
            try expectContextualWord("RENAME"); try expectContextualWord("TO")
            return .renameSchema(name: name, to: try parseRequiredIdentifier("new schema name"))
        }
        if consumeContextualWord("SEQUENCE") {
            let conditional = try ddlIfExists()
            let name = try ddlName()
            if consumeContextualWord("RENAME") {
                try expectContextualWord("TO")
                return .alterSequence(SQLAlterSequence(name: name, renameTo: try parseRequiredIdentifier("new sequence name"), ifExists: conditional))
            }
            let options = try ddlSequenceOptions(allowRestart: true)
            guard options.consumed else { throw ddlSyntax("ALTER SEQUENCE requires an action") }
            return .alterSequence(SQLAlterSequence(name: name, options: options.options, restart: options.restart,
                ownedBy: options.ownedBy, ifExists: conditional))
        }
        try expectContextualWord("TABLE")
        let conditional = try ddlIfExists()
        let name = try ddlName()
        var actions: [SQLAlterTableAction] = []
        repeat {
            try admitCollectionElement(); actions.append(try ddlAlterAction())
            if !isSymbol(",") { break }; advance()
        } while true
        return .alterTable(SQLAlterTable(name: name, actions: actions, ifExists: conditional))
    }

    private func ddlAlterAction() throws -> SQLAlterTableAction {
        let start = tokenStart
        let checkpoint = sourceCapture?.pendingCheckpoint ?? 0
        let action = try ddlAlterActionBody()
        try sourceCapture?.recordDDLAction(action, checkpoint: checkpoint, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(action)
    }

    private func ddlAlterActionBody() throws -> SQLAlterTableAction {
        if consumeContextualWord("ADD") {
            if ddlStartsConstraint { return .addConstraint(try ddlConstraint(column: nil)) }
            _ = consumeContextualWord("COLUMN")
            let conditional = try ddlIfExists(negated: true)
            return .addColumn(try ddlColumn(), ifNotExists: conditional)
        }
        if consumeContextualWord("DROP") {
            let constraint = consumeContextualWord("CONSTRAINT")
            if !constraint { _ = consumeContextualWord("COLUMN") }
            let conditional = try ddlIfExists()
            let name = try parseRequiredIdentifier(constraint ? "constraint name" : "column name")
            let behavior = ddlDropBehavior()
            return constraint ? .dropConstraint(name: name, ifExists: conditional, behavior: behavior) : .dropColumn(name: name, ifExists: conditional, behavior: behavior)
        }
        if consumeContextualWord("RENAME") {
            if consumeContextualWord("TO") { return .renameTable(to: try parseRequiredIdentifier("new table name")) }
            _ = consumeContextualWord("COLUMN")
            let name = try parseRequiredIdentifier("column name")
            try expectContextualWord("TO")
            return .renameColumn(name: name, to: try parseRequiredIdentifier("new column name"))
        }
        try expectContextualWord("ALTER"); _ = consumeContextualWord("COLUMN")
        let name = try parseRequiredIdentifier("column name")
        let setDataType = withLookahead { consumeContextualWord("SET") && consumeContextualWord("DATA") }
        if consumeContextualWord("TYPE") || setDataType {
            if setDataType { try expectContextualWord("SET"); try expectContextualWord("DATA"); try expectContextualWord("TYPE") }
            let type = try parseDataType()
            let using = consumeContextualWord("USING") ? try parseExpression() : nil
            return .alterColumnType(name: name, type: type, using: using)
        }
        if consumeContextualWord("SET") {
            if consumeContextualWord("DEFAULT") { return .setDefault(column: name, expression: try parseExpression()) }
            try expectContextualWord("NOT"); try expectContextualWord("NULL")
            return .setNotNull(column: name)
        }
        try expectContextualWord("DROP")
        if consumeContextualWord("DEFAULT") { return .dropDefault(column: name) }
        try expectContextualWord("NOT"); try expectContextualWord("NULL")
        return .dropNotNull(column: name)
    }

    private var ddlStartsConstraint: Bool {
        withLookahead { ["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"].contains { consumeContextualWord($0) } }
    }

    private func ddlColumn() throws -> SQLColumnDefinition {
        let start = tokenStart
        let checkpoint = sourceCapture?.pendingCheckpoint ?? 0
        let name = try parseRequiredIdentifier("column name")
        let serial: DataType? = withLookahead {
            if consumeContextualWord("SMALLSERIAL") || consumeContextualWord("SERIAL2") { return .smallint }
            if consumeContextualWord("SERIAL") || consumeContextualWord("SERIAL4") { return .integer }
            if consumeContextualWord("BIGSERIAL") || consumeContextualWord("SERIAL8") { return .bigint }
            return nil
        }
        let type: DataType
        var policy: SQLColumnDefinitionPolicy
        if let serial {
            type = serial
            let typeStart = tokenStart
            advance(); policy = .serial
            try sourceCapture?.record(serial, start: typeStart, end: previousTokenEnd, identifierEnd: previousTokenEnd, ledger: &structuralLedger)
        }
        else { type = try parseDataType(); policy = .input(defaultExpression: nil) }
        var hasPolicy = serial != nil
        var constraints: [SQLTableConstraint] = []
        while true {
            if consumeContextualWord("DEFAULT") {
                guard !hasPolicy else { throw ddlSyntax("Duplicate column generation policy") }
                policy = .input(defaultExpression: try parseExpression()); hasPolicy = true
            } else if consumeContextualWord("GENERATED") {
                guard !hasPolicy else { throw ddlSyntax("Duplicate column generation policy") }
                let generation: SQLIdentityGeneration
                if consumeContextualWord("ALWAYS") { generation = .always }
                else { try expectContextualWord("BY"); try expectContextualWord("DEFAULT"); generation = .byDefault }
                try expectContextualWord("AS")
                if consumeContextualWord("IDENTITY") {
                    let options: SQLSequenceOptions
                    if isSymbol("(") {
                        advance(); let parsed = try ddlSequenceOptions(allowRestart: false, allowOwnership: false)
                        options = parsed.options; try expect(")")
                    } else { options = SQLSequenceOptions() }
                    policy = .identity(generation: generation, options: options)
                } else {
                    guard generation == .always else { throw ddlSyntax("Generated expression requires ALWAYS") }
                    try expect("("); let expression = try parseExpression(); try expect(")")
                    let stored: Bool
                    if consumeContextualWord("STORED") { stored = true }
                    else { _ = consumeContextualWord("VIRTUAL"); stored = false }
                    policy = .generated(expression, stored: stored)
                }
                hasPolicy = true
            } else if consumeContextualWord("NULL") {
                // Explicit NULL is the default nullable column policy.
            } else if ddlStartsConstraint || withLookahead({ consumeContextualWord("NOT") || consumeContextualWord("REFERENCES") }) {
                try admitCollectionElement(); constraints.append(try ddlConstraint(column: name))
            } else { break }
        }
        let column = SQLColumnDefinition(name: name, type: type, policy: policy, constraints: constraints)
        try sourceCapture?.recordDDLColumn(column, checkpoint: checkpoint, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(column)
    }

    private func ddlConstraint(column: String?) throws -> SQLTableConstraint {
        let start = tokenStart
        let checkpoint = sourceCapture?.pendingCheckpoint ?? 0
        let name = consumeContextualWord("CONSTRAINT") ? try parseRequiredIdentifier("constraint name") : nil
        let kind: SQLTableConstraintKind
        if consumeContextualWord("PRIMARY") {
            try expectContextualWord("KEY")
            kind = .primaryKey(columns: try column.map { [$0] } ?? ddlColumns())
        } else if consumeContextualWord("UNIQUE") {
            let nulls = try ddlNulls()
            kind = .unique(columns: try column.map { [$0] } ?? ddlColumns(), nulls: nulls)
        } else if consumeContextualWord("NOT") {
            guard let column else { throw ddlSyntax("NOT NULL requires a column") }
            try expectContextualWord("NULL"); kind = .notNull(column: column)
        } else if consumeContextualWord("CHECK") {
            try expect("("); kind = .check(try parseExpression()); try expect(")")
        } else {
            let columns: [String]
            if let column { columns = [column] }
            else { try expectContextualWord("FOREIGN"); try expectContextualWord("KEY"); columns = try ddlColumns() }
            try expectContextualWord("REFERENCES")
            let table = try ddlName()
            let referenced = isSymbol("(") ? try ddlColumns() : nil
            var match = SQLForeignKeyMatch.simple
            if consumeContextualWord("MATCH") {
                if consumeContextualWord("FULL") { match = .full }
                else { try expectContextualWord("SIMPLE") }
            }
            var update = SQLReferentialAction.noAction
            var delete = SQLReferentialAction.noAction
            var deleteColumns: [String]?
            var seen: Set<String> = []
            while consumeContextualWord("ON") {
                let isDelete = consumeContextualWord("DELETE")
                if !isDelete { try expectContextualWord("UPDATE") }
                guard seen.insert(isDelete ? "delete" : "update").inserted else { throw ddlSyntax("Duplicate referential action") }
                let action = try ddlReferenceAction()
                if isDelete { delete = action; if isSymbol("(") { deleteColumns = try ddlColumns() } }
                else { update = action }
            }
            kind = .foreignKey(SQLForeignKeyDefinition(columns: columns, referencedTable: table,
                referencedColumns: referenced, match: match, onUpdate: update, onDelete: delete, onDeleteColumns: deleteColumns))
        }
        var deferrable: Bool?
        var deferred: Bool?
        while true {
            if consumeContextualWord("DEFERRABLE") {
                guard deferrable == nil else { throw ddlSyntax("Duplicate constraint timing") }; deferrable = true
            } else if withLookahead({ consumeContextualWord("NOT") && consumeContextualWord("DEFERRABLE") }) {
                advance(); advance()
                guard deferrable == nil else { throw ddlSyntax("Duplicate constraint timing") }; deferrable = false
            } else if consumeContextualWord("INITIALLY") {
                guard deferred == nil else { throw ddlSyntax("Duplicate initial timing") }
                if consumeContextualWord("DEFERRED") { deferred = true }
                else { try expectContextualWord("IMMEDIATE"); deferred = false }
            } else { break }
        }
        guard deferred != true || deferrable != false else { throw ddlSyntax("INITIALLY DEFERRED contradicts NOT DEFERRABLE") }
        if deferred == true { deferrable = true }
        let constraint = SQLTableConstraint(name: name, timing: deferrable == true ? .deferrable(initiallyDeferred: deferred ?? false) : .immediate, kind: kind)
        try sourceCapture?.recordDDLConstraint(constraint, checkpoint: checkpoint, start: start, end: previousTokenEnd, ledger: &structuralLedger)
        return try makeStructuralNode(constraint)
    }

    private func ddlNulls() throws -> SQLNullUniqueness {
        guard consumeContextualWord("NULLS") else { return .distinct }
        let not = consumeContextualWord("NOT")
        try expectContextualWord("DISTINCT")
        return not ? .notDistinct : .distinct
    }

    private func ddlReferenceAction() throws -> SQLReferentialAction {
        if consumeContextualWord("NO") { try expectContextualWord("ACTION"); return .noAction }
        if consumeContextualWord("RESTRICT") { return .restrict }
        if consumeContextualWord("CASCADE") { return .cascade }
        try expectContextualWord("SET")
        if consumeContextualWord("NULL") { return .setNull }
        try expectContextualWord("DEFAULT"); return .setDefault
    }

    private func ddlInteger() throws -> Int64 {
        let negative = isSymbol("-")
        if negative || isSymbol("+") { advance() }
        guard case .number(let number) = currentToken,
              let value = Int64((negative ? "-" : "") + number) else { throw ddlSyntax("Expected signed 64-bit integer") }
        advance(); return value
    }

    private func ddlSequenceOptions(allowRestart: Bool, allowOwnership: Bool = true) throws ->
        (options: SQLSequenceOptions, restart: SQLSequenceRestart?, ownedBy: SQLSequenceOwnership?, consumed: Bool) {
        var type: DataType?, start: Int64?, increment: Int64?, minimum: SQLSequenceBound?, maximum: SQLSequenceBound?, cycle: Bool?
        var restart: SQLSequenceRestart?, ownership: SQLSequenceOwnership?
        var seen: Set<String> = []
        func duplicate(_ key: String) throws {
            guard seen.insert(key).inserted else { throw ddlSyntax("Duplicate sequence option \(key)") }
        }
        while true {
            if consumeContextualWord("AS") { try duplicate("type"); type = try parseDataType() }
            else if consumeContextualWord("START") { try duplicate("start"); _ = consumeContextualWord("WITH"); start = try ddlInteger() }
            else if consumeContextualWord("INCREMENT") { try duplicate("increment"); _ = consumeContextualWord("BY"); increment = try ddlInteger() }
            else if consumeContextualWord("MINVALUE") { try duplicate("minimum"); minimum = .value(try ddlInteger()) }
            else if consumeContextualWord("MAXVALUE") { try duplicate("maximum"); maximum = .value(try ddlInteger()) }
            else if consumeContextualWord("CYCLE") { try duplicate("cycle"); cycle = true }
            else if consumeContextualWord("NO") {
                if consumeContextualWord("MINVALUE") { try duplicate("minimum"); minimum = SQLSequenceBound.none }
                else if consumeContextualWord("MAXVALUE") { try duplicate("maximum"); maximum = SQLSequenceBound.none }
                else { try expectContextualWord("CYCLE"); try duplicate("cycle"); cycle = false }
            } else if allowRestart, consumeContextualWord("RESTART") {
                try duplicate("restart")
                if consumeContextualWord("WITH") { restart = .value(try ddlInteger()) }
                else if isSymbol("-") || isSymbol("+") || { if case .number = currentToken { return true }; return false }() { restart = .value(try ddlInteger()) }
                else { restart = .start }
            } else if allowOwnership, consumeContextualWord("OWNED") {
                try duplicate("ownership"); try expectContextualWord("BY")
                if consumeContextualWord("NONE") { ownership = SQLSequenceOwnership.none }
                else {
                    let first = try parseRequiredIdentifier("owned column relation")
                    try expect(".")
                    let second = try parseRequiredIdentifier("owned column", allowsKeywordLabel: true)
                    if isSymbol(".") {
                        advance(); ownership = .column(table: TableRef(schema: first, table: second), column: try parseRequiredIdentifier("owned column", allowsKeywordLabel: true))
                    } else { ownership = .column(table: TableRef(first), column: second) }
                }
            } else { break }
        }
        return (SQLSequenceOptions(type: type, start: start, increment: increment, minimum: minimum, maximum: maximum, cycle: cycle), restart, ownership, !seen.isEmpty)
    }

    private func ddlSyntax(_ message: String) -> ParseError {
        .invalidSyntax(message: message, position: tokenStart)
    }
}

// MARK: - Request Block Parsing

extension SQLParser {
    /// Parses a complete operational request using one cumulative resource ledger.
    public func parseBlock(_ sql: String) throws -> SQLBlock {
        try parseOperationalBlock(sql).block
    }

    /// Source roots align with block steps; control-only steps have no statement root.
    public func parseBlockWithSource(_ sql: String) throws -> SQLParsedBlock {
        capturesSource = true
        defer { capturesSource = false }
        let parsed = try parseOperationalBlock(sql)
        let snapshot = sourceCapture!.snapshot
        return SQLParsedBlock(block: parsed.block, occurrences: snapshot.occurrences,
                              roots: parsed.roots, sourceMap: snapshot.sourceMap,
                              stepRanges: parsed.ranges, copyDataRanges: parsed.bodies)
    }

    private func parseOperationalBlock(_ sql: String) throws ->
        (block: SQLBlock, roots: [QueryOccurrenceID?], ranges: [SQLSourceRange], bodies: [SQLSourceRange?]) {
        try resetParserState(for: sql)
        return try withStructuralErrorPrecedence {
            advance()
            var steps: [SQLBlockStep] = []
            var roots: [QueryOccurrenceID?] = []
            var ranges: [SQLSourceRange] = []
            var bodies: [SQLSourceRange?] = []
            while currentToken != .eof {
                if isSymbol(";") { advance(); continue }
                try admitCollectionElement()
                let start = tokenStart
                let checkpoint = sourceCapture?.pendingCheckpoint ?? 0
                let step = try parseBlockStep()
                let commandEnd = previousTokenEnd
                var body: SQLSourceRange?
                let complete: SQLBlockStep
                if case .copy(let copy) = step, copy.direction == .from {
                    guard isSymbol(";") else { throw ddlSyntax("COPY FROM STDIN requires a semicolon and inline data") }
                    let payload = try parseCopyBody()
                    body = payload.range
                    complete = .copy(SQLCopyCommand(target: copy.target, direction: copy.direction,
                                                    options: copy.options, data: payload.data))
                } else {
                    complete = step
                    guard currentToken == .eof || isSymbol(";") else { throw ddlSyntax("Expected statement separator") }
                    if isSymbol(";") { advance() }
                }
                var statement: QueryStatement?
                switch complete {
                case .statement(let value): statement = value
                case .prepare(let value): statement = value.statement
                case .explain(let value): statement = value.statement
                case .copy(let value): if case .query(let query) = value.target { statement = .select(query) }
                default: break
                }
                var root: QueryOccurrenceID?
                if let statement, sourceCapture != nil {
                    guard let index = UInt32(exactly: steps.count) else { throw SQLSourceCaptureCollector.CaptureError.identifierOverflow }
                    root = try sourceCapture!.finishStatement(index: index, statement: statement,
                        start: start, end: commandEnd, ledger: &structuralLedger)
                } else {
                    try sourceCapture?.discardPending(after: checkpoint)
                }
                if capturesSource {
                    try structuralLedger.consume(.retainedBytes, amount: UInt64(MemoryLayout<SQLSourceRange>.stride * 2 + MemoryLayout<QueryOccurrenceID?>.stride))
                }
                steps.append(complete)
                roots.append(root)
                ranges.append(SQLSourceRange(start: start, end: commandEnd))
                bodies.append(body)
            }
            return (try SQLBlock(steps: steps, limits: structuralLimits), roots, ranges, bodies)
        }
    }

    private func parseBlockStep() throws -> SQLBlockStep {
        try structuralLedger.consume(.totalNodes)
        if consumeContextualWord("BEGIN") {
            _ = consumeContextualWord("WORK") || consumeContextualWord("TRANSACTION")
            return .transaction(.begin)
        }
        if consumeContextualWord("START") {
            try expectContextualWord("TRANSACTION"); return .transaction(.begin)
        }
        if consumeContextualWord("COMMIT") || consumeContextualWord("END") {
            _ = consumeContextualWord("WORK") || consumeContextualWord("TRANSACTION")
            return .transaction(.commit)
        }
        let rollback = consumeContextualWord("ROLLBACK")
        if rollback || consumeContextualWord("ABORT") {
            _ = consumeContextualWord("WORK") || consumeContextualWord("TRANSACTION")
            if rollback && consumeContextualWord("TO") {
                _ = consumeContextualWord("SAVEPOINT")
                return .rollbackTo(try parseRequiredIdentifier("savepoint name"))
            }
            return .transaction(.rollback)
        }
        if consumeContextualWord("SAVEPOINT") { return .savepoint(try parseRequiredIdentifier("savepoint name")) }
        if consumeContextualWord("RELEASE") {
            _ = consumeContextualWord("SAVEPOINT")
            return .release(try parseRequiredIdentifier("savepoint name"))
        }
        if consumeContextualWord("SET") {
            if consumeContextualWord("CONSTRAINTS") {
                let selection: SQLConstraintSelection
                if consumeContextualWord("ALL") { selection = .all }
                else {
                    var names: [String] = []
                    repeat {
                        try admitCollectionElement(); names.append(try parseRequiredIdentifier("constraint name"))
                        if !isSymbol(",") { break }; advance()
                    } while true
                    selection = .named(names)
                }
                let deferred = consumeContextualWord("DEFERRED")
                if !deferred { try expectContextualWord("IMMEDIATE") }
                return .setConstraints(try SQLSetConstraints(selection: selection, deferred: deferred))
            }
            let scope = blockSettingScope()
            let timeZoneSyntax = withLookahead { consumeContextualWord("TIME") && consumeContextualWord("ZONE") }
            let name = try blockSettingName()
            if !consumeContextualWord("TO"), !timeZoneSyntax { try expect("=") }
            if consumeContextualWord("DEFAULT") { return .setting(.reset(name, scope: scope)) }
            let value: Expression
            if name == .searchPath {
                let scalarString = currentToken.isString
                var names: [Expression] = []
                repeat {
                    try admitCollectionElement()
                    if case .string(let text) = currentToken { advance(); names.append(.literal(.sqlUnknownString(text))) }
                    else { names.append(.literal(.sqlUnknownString(try parseRequiredIdentifier("search path schema")))) }
                    if !isSymbol(",") { break }; advance()
                } while true
                value = scalarString && names.count == 1 ? names[0] : .array(names)
            } else if case .identifier(let text) = currentToken, !tokenIsQuoted {
                advance(); value = .literal(.sqlUnknownString(text))
            } else { value = try parseExpression() }
            guard blockSettingConstant(value) else { throw ddlSyntax("SQL settings require constant values") }
            return .setting(.set(name: name, value: value, scope: scope))
        }
        if consumeContextualWord("SHOW") {
            return .setting(.show(consumeContextualWord("ALL") ? nil : try blockSettingName()))
        }
        if consumeContextualWord("RESET") {
            let scope = blockSettingScope()
            return .setting(.reset(consumeContextualWord("ALL") ? nil : try blockSettingName(), scope: scope))
        }
        if consumeContextualWord("PREPARE") {
            let name = try parseRequiredIdentifier("prepared statement name")
            var types: [DataType] = []
            if isSymbol("(") {
                advance()
                repeat {
                    try admitCollectionElement(); types.append(try parseDataType())
                    if !isSymbol(",") { break }; advance()
                } while true
                try expect(")")
            }
            // Type declarations are not descendants of the prepared statement's root.
            try sourceCapture?.discardPending(after: 0)
            let oldStyle = positionalParameterStyle
            let oldPosition = anonymousParameterPosition
            positionalParameterStyle = nil; anonymousParameterPosition = 0
            defer { positionalParameterStyle = oldStyle; anonymousParameterPosition = oldPosition }
            try expectContextualWord("AS")
            return .prepare(SQLPrepare(name: name, parameterTypes: types, statement: try blockOrdinaryStatement()))
        }
        if consumeContextualWord("EXECUTE") {
            let name = try parseRequiredIdentifier("prepared statement name")
            var arguments: [Expression] = []
            if isSymbol("(") {
                advance()
                if !isSymbol(")") {
                    repeat {
                        try admitCollectionElement(); arguments.append(try parseExpression())
                        if !isSymbol(",") { break }; advance()
                    } while true
                }
                try expect(")")
            }
            return .execute(SQLExecute(name: name, arguments: arguments))
        }
        if consumeContextualWord("DEALLOCATE") {
            _ = consumeContextualWord("PREPARE")
            return .deallocate(consumeContextualWord("ALL") ? nil : try parseRequiredIdentifier("prepared statement name"))
        }
        if consumeContextualWord("EXPLAIN") { return .explain(try blockExplain()) }
        if consumeContextualWord("COPY") { return .copy(try blockCopy()) }
        return .statement(try parseStatement())
    }

    private func blockSettingScope() -> SQLSettingScope {
        if consumeContextualWord("LOCAL") { return .local }
        _ = consumeContextualWord("SESSION"); return .session
    }

    private func blockSettingName() throws -> SQLSettingName {
        if consumeContextualWord("SEARCH_PATH") { return .searchPath }
        if consumeContextualWord("TIMEZONE") { return .timeZone }
        if consumeContextualWord("TIME") { try expectContextualWord("ZONE"); return .timeZone }
        if consumeContextualWord("STATEMENT_TIMEOUT") { return .statementTimeout }
        throw ddlSyntax("Unsupported SQL setting")
    }

    private func blockSettingConstant(_ expression: Expression) -> Bool {
        switch expression {
        case .literal: return true
        case .cast(let value, _): return blockSettingConstant(value)
        case .array(let values): return values.allSatisfy(blockSettingConstant)
        default: return false
        }
    }

    private func blockOrdinaryStatement() throws -> QueryStatement {
        let statement = try parseStatement()
        switch statement {
        case .select, .insert, .update, .delete, .with, .merge: return statement
        default: throw ddlSyntax("Expected an ordinary SELECT or DML statement")
        }
    }

    private func blockBoolean() throws -> Bool {
        if consumeContextualWord("TRUE") || consumeContextualWord("ON") { return true }
        if consumeContextualWord("FALSE") || consumeContextualWord("OFF") { return false }
        if case .number(let number) = currentToken, number == "0" || number == "1" { advance(); return number == "1" }
        throw ddlSyntax("Expected boolean option")
    }

    private func blockExplain() throws -> SQLExplain {
        var analyze = false, verbose = false
        var format = SQLExplainFormat.text
        var seen: Set<String> = []
        let parenthesized = isSymbol("(")
        if parenthesized { advance() }
        while true {
            let key: String
            if consumeContextualWord("ANALYZE") || consumeContextualWord("ANALYSE") { key = "analyze" }
            else if consumeContextualWord("VERBOSE") { key = "verbose" }
            else if parenthesized, consumeContextualWord("FORMAT") { key = "format" }
            else { break }
            guard seen.insert(key).inserted else { throw ddlSyntax("Duplicate EXPLAIN option") }
            if key == "format" {
                if consumeContextualWord("JSON") { format = .json }
                else if consumeContextualWord("TEXT") { format = .text }
                else { throw unrecognizedOptionValue("EXPLAIN format") }
            } else {
                let explicit = withLookahead {
                    consumeContextualWord("TRUE") || consumeContextualWord("FALSE") || consumeContextualWord("ON") || consumeContextualWord("OFF") || { if case .number = currentToken { return true }; return false }()
                }
                let value = explicit ? try blockBoolean() : true
                if key == "analyze" { analyze = value } else { verbose = value }
            }
            if parenthesized {
                if !isSymbol(",") { break }; advance()
                guard !isSymbol(")") else { throw ddlSyntax("Expected EXPLAIN option") }
            }
        }
        if parenthesized { try expect(")") }
        return SQLExplain(statement: try blockOrdinaryStatement(), options: SQLExplainOptions(analyze: analyze, verbose: verbose, format: format))
    }

    private func blockCopy() throws -> SQLCopyCommand {
        let target: SQLCopyTarget
        if isSymbol("(") {
            advance(); let query = try parseSelectQuery(); try expect(")"); target = .query(query)
        } else {
            let table = try ddlName()
            target = .table(table, columns: isSymbol("(") ? try ddlColumns() : [])
        }
        let direction: SQLCopyDirection
        if consumeContextualWord("FROM") { direction = .from; try expectContextualWord("STDIN") }
        else { try expectContextualWord("TO"); direction = .to; try expectContextualWord("STDOUT") }
        let with = consumeContextualWord("WITH")
        let parenthesized = isSymbol("(")
        if parenthesized { advance() }
        var format = SQLCopyFormat.text
        var delimiter: String?, null: String?, quote: String?, escape: String?
        var header = false
        var seen: Set<String> = []
        while true {
            let key: String
            if consumeContextualWord("FORMAT") { key = "format" }
            else if consumeContextualWord("CSV") { key = "csv" }
            else if consumeContextualWord("DELIMITER") { key = "delimiter" }
            else if consumeContextualWord("NULL") { key = "null" }
            else if consumeContextualWord("HEADER") { key = "header" }
            else if consumeContextualWord("QUOTE") { key = "quote" }
            else if consumeContextualWord("ESCAPE") { key = "escape" }
            else { break }
            guard seen.insert(key == "csv" ? "format" : key).inserted else { throw ddlSyntax("Duplicate COPY option") }
            _ = consumeContextualWord("AS")
            switch key {
            case "csv": format = .csv
            case "format":
                if consumeContextualWord("CSV") { format = .csv }
                else if consumeContextualWord("TEXT") { format = .text }
                else { throw unrecognizedOptionValue("COPY format") }
            case "header":
                let explicit = withLookahead { consumeContextualWord("TRUE") || consumeContextualWord("FALSE") || consumeContextualWord("ON") || consumeContextualWord("OFF") }
                header = explicit ? try blockBoolean() : true
            default:
                guard case .string(let value) = currentToken else { throw ddlSyntax("COPY option requires a string") }
                advance()
                switch key { case "delimiter": delimiter = value; case "null": null = value; case "quote": quote = value; default: escape = value }
            }
            if parenthesized {
                if !isSymbol(",") { break }; advance()
                guard !isSymbol(")") else { throw ddlSyntax("Expected COPY option") }
            }
        }
        if parenthesized { try expect(")") }
        else if with && seen.isEmpty { throw ddlSyntax("WITH requires COPY options") }
        return SQLCopyCommand(target: target, direction: direction,
            options: try SQLCopyOptions(format: format, delimiter: delimiter, null: null, header: header, quote: quote, escape: escape))
    }

    private func parseCopyBody() throws -> (data: ByteString, range: SQLSourceRange) {
        // The lexer has consumed only the semicolon; payload bytes never become SQL tokens.
        var cursor = position
        while cursor < sourceBytes.count && (sourceBytes[cursor] == 32 || sourceBytes[cursor] == 9) { cursor += 1 }
        if cursor < sourceBytes.count && sourceBytes[cursor] == 13 { cursor += 1 }
        guard cursor < sourceBytes.count && sourceBytes[cursor] == 10 else { throw ddlSyntax("COPY data must start on the following line") }
        cursor += 1
        let start = cursor
        while cursor <= sourceBytes.count {
            let line = cursor
            while cursor < sourceBytes.count && sourceBytes[cursor] != 10 { cursor += 1 }
            let end = cursor > line && sourceBytes[cursor - 1] == 13 ? cursor - 1 : cursor
            if end - line == 2 && sourceBytes[line] == 92 && sourceBytes[line + 1] == 46 {
                try structuralLedger.consume(.retainedBytes, amount: UInt64(line - start))
                // Detach at the output boundary so COPY retains only its preadmitted, measurable payload.
                let data = ByteString(sourceBytes[start..<line]).detached()
                position = cursor < sourceBytes.count ? cursor + 1 : cursor
                advance()
                return (data, SQLSourceRange(start: start, end: line))
            }
            guard cursor < sourceBytes.count else { throw ParseError.unexpectedEndOfInput(expected: "COPY terminator \\.", position: cursor) }
            cursor += 1
        }
        throw ddlSyntax("Missing COPY terminator")
    }
}
