/// SPARQLParser.swift
/// SPARQL Query Parser
///
/// Reference:
/// - W3C SPARQL 1.1 Query Language
/// - W3C SPARQL 1.2 (Draft)

import Foundation

/// SPARQL Parser for converting SPARQL strings to AST
public final class SPARQLParser {
    /// Parser errors
    public enum ParseError: Error, Sendable, Equatable {
        case unexpectedToken(expected: String, found: String, position: Int)
        case unexpectedEndOfInput(expected: String)
        case invalidSyntax(message: String, position: Int)
        case invalidIRI(String)
        case unsupportedFeature(String)
    }

    /// Token types
    private enum Token: Sendable, Equatable {
        case keyword(String)
        case iri(String)
        case prefixedName(prefix: String, local: String)
        case variable(String)
        case string(String, language: String?, datatype: String?, direction: String?)
        case integer(String)
        case decimal(String)
        case double(String)
        case blankNode(String)
        case symbol(String)
        case eof
    }

    private var input: String
    private var position: String.Index
    private var currentToken: Token
    private var prefixes: [String: String]
    private var baseIRI: String?
    private var blankNodeCounter: Int = 0
    private var pendingTriples: [TriplePattern] = []
    private var sparqlVersion: String?

    public init() {
        self.input = ""
        self.position = "".startIndex
        self.currentToken = .eof
        self.prefixes = SPARQLTerm.commonPrefixes
        self.baseIRI = nil
        self.blankNodeCounter = 0
        self.pendingTriples = []
        self.sparqlVersion = nil
    }

    /// SPARQL 1.1 built-in function keywords [121] BuiltInCall + [127] Aggregate
    /// Used by parsePrimaryExpression() to route to parseBuiltInCall()
    private static let builtInFunctionKeywords: Set<String> = [
        // Aggregates [127]
        "COUNT", "SUM", "AVG", "MIN", "MAX", "SAMPLE", "GROUP_CONCAT",
        // Existing [121]
        "BOUND", "EXISTS", "NOT", "REGEX",
        // Conditional
        "IF", "COALESCE",
        // 0-arg
        "NOW", "RAND", "UUID", "STRUUID",
        // 0-1 arg
        "BNODE",
        // 1-arg: String
        "STR", "STRLEN", "UCASE", "LCASE", "ENCODE_FOR_URI",
        // 1-arg: Lang/Datatype
        "LANG", "DATATYPE",
        // SPARQL 1.2: Language direction functions
        "LANGDIR", "HASLANG", "HASLANGDIR", "STRLANGDIR",
        // 1-arg: IRI
        "IRI", "URI",
        // 1-arg: Numeric
        "ABS", "CEIL", "FLOOR", "ROUND",
        // 1-arg: Date/Time
        "YEAR", "MONTH", "DAY", "HOURS", "MINUTES", "SECONDS", "TIMEZONE", "TZ",
        // 1-arg: Hash
        "MD5", "SHA1", "SHA256", "SHA384", "SHA512",
        // 1-arg: Type checks
        "ISIRI", "ISURI", "ISBLANK", "ISLITERAL", "ISNUMERIC",
        // RDF-star (SPARQL-star): type check + accessors
        "ISTRIPLE", "TRIPLE", "SUBJECT", "PREDICATE", "OBJECT",
        // 2-arg
        "LANGMATCHES", "CONTAINS", "STRSTARTS", "STRENDS",
        "STRBEFORE", "STRAFTER", "SAMETERM", "STRDT", "STRLANG",
        // 2-3 arg [123]
        "SUBSTR",
        // 3-4 arg [124]
        "REPLACE",
        // variadic
        "CONCAT",
    ]

    /// Enable debug logging
    nonisolated(unsafe) private static var debugEnabled = false
    public static func enableDebug(_ enabled: Bool) {
        debugEnabled = enabled
    }
    private func log(_ message: String) {
        if Self.debugEnabled {
            print("[SPARQLParser] \(message)")
        }
    }

    /// Parse a SPARQL query
    public func parse(_ sparql: String) throws -> QueryStatement {
        log("parse() START: '\(sparql.prefix(50))...'")
        self.input = sparql
        self.position = input.startIndex
        self.prefixes = SPARQLTerm.commonPrefixes
        self.baseIRI = nil
        self.blankNodeCounter = 0
        self.pendingTriples = []
        self.sparqlVersion = nil
        advance()
        log("parse() initial token: \(currentToken)")

        // Parse prologue (PREFIX and BASE declarations)
        try parsePrologue()
        log("parse() after prologue, token: \(currentToken)")

        // Parse query form
        switch currentToken {
        case .keyword("SELECT"):
            return .select(try parseSelectQuery())
        case .keyword("CONSTRUCT"):
            return .construct(try parseConstructQuery())
        case .keyword("ASK"):
            return .ask(try parseAskQuery())
        case .keyword("DESCRIBE"):
            return .describe(try parseDescribeQuery())
        // SPARQL Update
        case .keyword("INSERT"):
            return try parseInsertOrModify()
        case .keyword("DELETE"):
            return try parseDeleteOrModify()
        case .keyword("LOAD"):
            return .load(try parseLoadQuery())
        case .keyword("CLEAR"):
            return .clear(try parseClearQuery())
        case .keyword("CREATE"):
            return try parseCreateGraph()
        case .keyword("DROP"):
            return try parseDropGraph()
        default:
            throw ParseError.invalidSyntax(
                message: "Expected query form (SELECT, CONSTRUCT, ASK, DESCRIBE, INSERT, DELETE, LOAD, CLEAR, CREATE, DROP)",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Parse a SPARQL SELECT query
    public func parseSelect(_ sparql: String) throws -> SelectQuery {
        self.input = sparql
        self.position = input.startIndex
        self.prefixes = SPARQLTerm.commonPrefixes
        self.baseIRI = nil
        self.blankNodeCounter = 0
        self.pendingTriples = []
        self.sparqlVersion = nil
        advance()

        try parsePrologue()
        return try parseSelectQuery()
    }
}

// MARK: - Tokenizer

extension SPARQLParser {
    private func advance() {
        skipWhitespace()

        guard position < input.endIndex else {
            currentToken = .eof
            return
        }

        let char = input[position]

        // Variables: ?var or $var (requires at least one varChar after ? or $)
        // Bare '?' without following varChar is a symbol (path modifier)
        if char == "?" || char == "$" {
            let nextIdx = input.index(after: position)
            if nextIdx < input.endIndex && isVarChar(input[nextIdx]) {
                position = nextIdx
                let start = position
                while position < input.endIndex && isVarChar(input[position]) {
                    position = input.index(after: position)
                }
                currentToken = .variable(String(input[start..<position]))
                return
            } else if char == "?" {
                // Bare '?' — property path ZeroOrOne modifier
                position = nextIdx
                currentToken = .symbol("?")
                return
            }
            // '$' without varChar — fall through to symbol handling
        }

        // Multi-character symbols (must be checked before IRI to distinguish << from <iri>)
        let twoChar = String(input[position...].prefix(2))
        if ["<=", ">=", "!=", "&&", "||", "^^", "<<", ">>", "{|", "|}"].contains(twoChar) {
            position = input.index(position, offsetBy: 2)
            currentToken = .symbol(twoChar)
            return
        }

        // IRIs: <...>
        if char == "<" {
            position = input.index(after: position)
            let start = position
            while position < input.endIndex && input[position] != ">" {
                position = input.index(after: position)
            }
            let iri = String(input[start..<position])
            if position < input.endIndex {
                position = input.index(after: position)
            }
            currentToken = .iri(iri)
            return
        }

        // Blank nodes: _:name
        if char == "_" && position < input.index(before: input.endIndex) && input[input.index(after: position)] == ":" {
            position = input.index(position, offsetBy: 2)
            let start = position
            while position < input.endIndex && isVarChar(input[position]) {
                position = input.index(after: position)
            }
            currentToken = .blankNode(String(input[start..<position]))
            return
        }

        // Strings
        if char == "\"" || char == "'" {
            let quote = char
            let tripleQuote = String(input[position...].prefix(3)) == String(repeating: quote, count: 3)

            if tripleQuote {
                position = input.index(position, offsetBy: 3)
                currentToken = parseLongString(quote: quote)
            } else {
                position = input.index(after: position)
                currentToken = parseShortString(quote: quote)
            }
            return
        }

        // Numbers
        if char.isNumber || (char == "." && position < input.index(before: input.endIndex) && input[input.index(after: position)].isNumber) {
            currentToken = parseNumber()
            return
        }

        // Keywords and prefixed names
        if char.isLetter || char == ":" {
            let start = position
            while position < input.endIndex {
                let c = input[position]
                if c.isLetter || c.isNumber || c == "_" || c == "-" || c == ":" || c == "." {
                    position = input.index(after: position)
                } else {
                    break
                }
            }

            // Trim trailing dots
            while position > start && input[input.index(before: position)] == "." {
                position = input.index(before: position)
            }

            let word = String(input[start..<position])

            // Check for prefixed name
            if let colonIndex = word.firstIndex(of: ":") {
                let prefix = String(word[..<colonIndex])
                let local = String(word[word.index(after: colonIndex)...])

                // Check if it's a keyword followed by colon (unlikely but handle it)
                let upper = word.uppercased()
                if !isKeywordString(upper) {
                    currentToken = .prefixedName(prefix: prefix, local: local)
                    return
                }
            }

            let upper = word.uppercased()
            if isKeywordString(upper) {
                currentToken = .keyword(upper)
            } else {
                // Bare word - treat as prefixed name with empty prefix
                currentToken = .prefixedName(prefix: "", local: word)
            }
            return
        }

        // Single character symbols
        position = input.index(after: position)
        currentToken = .symbol(String(char))
    }

    private func skipWhitespace() {
        while position < input.endIndex {
            let char = input[position]
            if char.isWhitespace {
                position = input.index(after: position)
            } else if char == "#" {
                // Comment
                while position < input.endIndex && input[position] != "\n" {
                    position = input.index(after: position)
                }
            } else {
                break
            }
        }
    }

    private func isVarChar(_ char: Character) -> Bool {
        char.isLetter || char.isNumber || char == "_"
    }

    private func isKeywordString(_ word: String) -> Bool {
        let keywords = ["SELECT", "CONSTRUCT", "DESCRIBE", "ASK", "WHERE", "FROM", "NAMED",
                       "PREFIX", "BASE", "OPTIONAL", "UNION", "FILTER", "GRAPH", "SERVICE",
                       "SILENT", "BIND", "AS", "VALUES", "MINUS", "GROUP", "BY", "HAVING",
                       "ORDER", "ASC", "DESC", "LIMIT", "OFFSET", "DISTINCT", "REDUCED",
                       "NOT", "IN", "EXISTS", "BOUND", "IF", "COALESCE", "REGEX", "STR",
                       "LANG", "LANGMATCHES", "DATATYPE", "IRI", "URI", "BNODE", "RAND",
                       "ABS", "CEIL", "FLOOR", "ROUND", "CONCAT", "STRLEN", "UCASE", "LCASE",
                       "ENCODE_FOR_URI", "CONTAINS", "STRSTARTS", "STRENDS", "STRBEFORE",
                       "STRAFTER", "YEAR", "MONTH", "DAY", "HOURS", "MINUTES", "SECONDS",
                       "TIMEZONE", "TZ", "NOW", "UUID", "STRUUID", "MD5", "SHA1", "SHA256",
                       "SHA384", "SHA512", "ISIRI", "ISURI", "ISBLANK", "ISLITERAL",
                       "ISNUMERIC", "SAMETERM", "TRUE", "FALSE", "COUNT", "SUM", "MIN",
                       "MAX", "AVG", "SAMPLE", "GROUP_CONCAT", "SEPARATOR", "A", "UNDEF",
                       "SUBSTR", "REPLACE", "STRDT", "STRLANG",
                       "ISTRIPLE", "TRIPLE", "SUBJECT", "PREDICATE", "OBJECT",
                       // SPARQL 1.2
                       "VERSION", "LATERAL", "LANGDIR", "HASLANG", "HASLANGDIR", "STRLANGDIR",
                       // SPARQL Update
                       "INSERT", "DELETE", "DATA", "INTO", "WITH", "USING",
                       "LOAD", "CLEAR", "CREATE", "DROP", "DEFAULT", "ALL", "COPY", "MOVE", "ADD",
                       "TO"]
        return keywords.contains(word)
    }

    private func parseShortString(quote: Character) -> Token {
        var value = ""
        while position < input.endIndex && input[position] != quote {
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }
        if position < input.endIndex {
            position = input.index(after: position)
        }

        let (language, datatype, direction) = parseLiteralSuffix()
        return .string(value, language: language, datatype: datatype, direction: direction)
    }

    private func parseLongString(quote: Character) -> Token {
        var value = ""
        let endQuote = String(repeating: quote, count: 3)

        while position < input.endIndex {
            if String(input[position...].prefix(3)) == endQuote {
                position = input.index(position, offsetBy: 3)
                break
            }
            if input[position] == "\\" {
                position = input.index(after: position)
                if position < input.endIndex {
                    value.append(parseEscape())
                }
            } else {
                value.append(input[position])
                position = input.index(after: position)
            }
        }

        let (language, datatype, direction) = parseLiteralSuffix()
        return .string(value, language: language, datatype: datatype, direction: direction)
    }

    /// Parse optional @language(--direction)? or ^^datatype suffix after a string literal
    private func parseLiteralSuffix() -> (language: String?, datatype: String?, direction: String?) {
        var language: String?
        var datatype: String?
        var direction: String?

        if position < input.endIndex && input[position] == "@" {
            position = input.index(after: position)
            let langStart = position
            while position < input.endIndex && (input[position].isLetter || input[position] == "-") {
                // Lookahead: stop at `--` (direction separator, SPARQL 1.2)
                if input[position] == "-" {
                    let next = input.index(after: position)
                    if next < input.endIndex && input[next] == "-" {
                        break
                    }
                }
                position = input.index(after: position)
            }
            language = String(input[langStart..<position])
            // Check for direction suffix: --ltr or --rtl (SPARQL 1.2)
            let dirRemaining = input[position...]
            if dirRemaining.prefix(2) == "--" {
                position = input.index(position, offsetBy: 2)
                let dirStart = position
                while position < input.endIndex && input[position].isLetter {
                    position = input.index(after: position)
                }
                direction = String(input[dirStart..<position])
            }
        } else if position < input.endIndex {
            let remaining = input[position...]
            if remaining.prefix(2) == "^^" {
                position = input.index(position, offsetBy: 2)
                advance()
                if case .iri(let iri) = currentToken {
                    datatype = iri
                } else if case .prefixedName(let prefix, let local) = currentToken {
                    datatype = resolvePrefixedName(prefix: prefix, local: local)
                }
            }
        }

        return (language, datatype, direction)
    }

    private func parseEscape() -> Character {
        let char = input[position]
        position = input.index(after: position)
        switch char {
        case "n": return "\n"
        case "r": return "\r"
        case "t": return "\t"
        case "\\": return "\\"
        case "\"": return "\""
        case "'": return "'"
        case "u":
            // \uXXXX — 4 hex digits
            if let scalar = parseHexScalar(digitCount: 4) {
                return Character(scalar)
            }
            return char
        case "U":
            // \UXXXXXXXX — 8 hex digits
            if let scalar = parseHexScalar(digitCount: 8) {
                return Character(scalar)
            }
            return char
        default: return char
        }
    }

    /// Parse `digitCount` hex characters and return the corresponding Unicode scalar
    private func parseHexScalar(digitCount: Int) -> Unicode.Scalar? {
        var hex = ""
        for _ in 0..<digitCount {
            guard position < input.endIndex else { return nil }
            let c = input[position]
            guard c.isHexDigit else { return nil }
            hex.append(c)
            position = input.index(after: position)
        }
        guard let codePoint = UInt32(hex, radix: 16),
              let scalar = Unicode.Scalar(codePoint) else {
            return nil
        }
        return scalar
    }

    private func parseNumber() -> Token {
        let start = position
        var hasDecimal = false
        var hasExponent = false

        while position < input.endIndex {
            let char = input[position]
            if char.isNumber {
                position = input.index(after: position)
            } else if char == "." && !hasDecimal {
                hasDecimal = true
                position = input.index(after: position)
            } else if (char == "e" || char == "E") && !hasExponent {
                hasExponent = true
                position = input.index(after: position)
                if position < input.endIndex && (input[position] == "+" || input[position] == "-") {
                    position = input.index(after: position)
                }
            } else {
                break
            }
        }

        let numStr = String(input[start..<position])
        if hasExponent {
            return .double(numStr)
        } else if hasDecimal {
            return .decimal(numStr)
        } else {
            return .integer(numStr)
        }
    }

    private func resolvePrefixedName(prefix: String, local: String) -> String {
        if let base = prefixes[prefix] {
            return base + local
        }
        return "\(prefix):\(local)"
    }

    private func expect(_ keyword: String) throws {
        guard case .keyword(let kw) = currentToken, kw == keyword else {
            throw ParseError.unexpectedToken(
                expected: keyword,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
    }

    private func expectSymbol(_ symbol: String) throws {
        guard case .symbol(let s) = currentToken, s == symbol else {
            throw ParseError.unexpectedToken(
                expected: symbol,
                found: tokenDescription(currentToken),
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
    }

    private func tokenDescription(_ token: Token) -> String {
        switch token {
        case .keyword(let k): return "keyword '\(k)'"
        case .iri(let i): return "IRI <\(i)>"
        case .prefixedName(let p, let l): return "prefixed name '\(p):\(l)'"
        case .variable(let v): return "variable ?\(v)"
        case .string(let s, _, _, _): return "string \"\(s)\""
        case .integer(let n): return "integer \(n)"
        case .decimal(let n): return "decimal \(n)"
        case .double(let n): return "double \(n)"
        case .blankNode(let b): return "blank node _:\(b)"
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

    private func isVariable() -> Bool {
        if case .variable = currentToken {
            return true
        }
        return false
    }

    private func isBuiltInFunctionKeyword() -> Bool {
        guard case .keyword(let kw) = currentToken else { return false }
        return Self.builtInFunctionKeywords.contains(kw)
    }

    private func isIRIOrPrefixedName() -> Bool {
        switch currentToken {
        case .iri: return true
        case .prefixedName: return true
        default: return false
        }
    }

    /// Generate a fresh blank node identifier
    private func freshBlankNode() -> String {
        let id = "_:anon_\(blankNodeCounter)"
        blankNodeCounter += 1
        return id
    }

    /// Parse SPARQL 1.2 annotation syntax: {| predicate object (;  predicate object)* |}
    /// The annotation subject is <<( s p o )>> (quoted triple of the annotated triple).
    private func parseAnnotation(subject: SPARQLTerm, predicate: SPARQLTerm, object: SPARQLTerm) throws -> [TriplePattern] {
        try expectSymbol("{|")
        let annotationSubject = SPARQLTerm.quotedTriple(subject: subject, predicate: predicate, object: object)
        var annotationTriples: [TriplePattern] = []

        var firstPred = true
        while !isSymbol("|}") {
            if !firstPred {
                guard isSymbol(";") else { break }
                advance()
                if isSymbol("|}") { break } // trailing semicolon
            }
            firstPred = false
            let annPred = try parseTerm()
            let annObj = try parseTerm()
            annotationTriples.append(TriplePattern(subject: annotationSubject, predicate: annPred, object: annObj))
            if !pendingTriples.isEmpty {
                annotationTriples.append(contentsOf: pendingTriples)
                pendingTriples.removeAll()
            }
            while isSymbol(",") {
                advance()
                let nextObj = try parseTerm()
                annotationTriples.append(TriplePattern(subject: annotationSubject, predicate: annPred, object: nextObj))
                if !pendingTriples.isEmpty {
                    annotationTriples.append(contentsOf: pendingTriples)
                    pendingTriples.removeAll()
                }
            }
        }
        try expectSymbol("|}")
        return annotationTriples
    }

    /// Resolve a relative IRI against the BASE IRI (RFC 3986 simplified)
    private func resolveIRI(_ iri: String) -> String {
        guard let base = baseIRI else { return iri }
        // Already absolute (has scheme)
        if iri.contains("://") { return iri }
        // Fragment-only reference
        if iri.hasPrefix("#") { return base + iri }
        // Relative path
        if iri.hasPrefix("/") {
            // Extract scheme + authority from base
            if let schemeEnd = base.range(of: "://") {
                let afterScheme = base[schemeEnd.upperBound...]
                if let authorityEnd = afterScheme.firstIndex(of: "/") {
                    return String(base[...authorityEnd]) + String(iri.dropFirst())
                }
            }
            return base + iri
        }
        // Remove last path component from base and append
        if let lastSlash = base.lastIndex(of: "/") {
            return String(base[...lastSlash]) + iri
        }
        return base + iri
    }

    // MARK: - Lookahead Helpers (LL(1) decision)

    /// Check if current token can start a TriplesSameSubjectPath
    /// SPARQL 1.1 Grammar [75] VarOrTerm, [97] RDFLiteral, etc.
    private func canStartTriple() -> Bool {
        switch currentToken {
        case .variable, .iri, .prefixedName, .blankNode,
             .string, .integer, .decimal, .double:
            return true
        case .symbol("<<"):  // RDF-star quoted triple
            return true
        case .symbol("["):  // Anonymous blank node
            return true
        case .symbol("("):  // RDF collection
            return true
        case .keyword("TRUE"), .keyword("FALSE"):
            return true
        default:
            return false
        }
    }

    /// Check if current token starts a GraphPatternNotTriples [57]
    private func canStartGraphPatternNotTriples() -> Bool {
        switch currentToken {
        case .symbol("{"):
            return true
        case .keyword("OPTIONAL"), .keyword("MINUS"), .keyword("GRAPH"),
             .keyword("SERVICE"), .keyword("FILTER"), .keyword("BIND"),
             .keyword("VALUES"), .keyword("LATERAL"):
            return true
        default:
            return false
        }
    }

    /// Check if current token can start a Verb or PropertyPath (predicate position)
    /// VerbPath = Path | Var  [SPARQL 1.1 Grammar [78]]
    private func canStartVerb() -> Bool {
        switch currentToken {
        case .variable, .iri, .prefixedName:
            return true
        case .keyword("A"):
            return true
        case .symbol("^"), .symbol("!"), .symbol("("):
            return true
        case .string:
            // Framework extension: allow string literals as predicates
            // e.g., ?s "knows" ?o — treated as IRI shorthand
            return true
        default:
            return false
        }
    }

    // MARK: - Property Path Parsing

    /// Check if current token is a path modifier (*, +, ?)
    private func isPathModifier() -> Bool {
        switch currentToken {
        case .symbol("*"), .symbol("+"), .symbol("?"):
            return true
        default:
            return false
        }
    }

    /// Check if current token is a path operator (/, |)
    private func isPathOperator() -> Bool {
        switch currentToken {
        case .symbol("/"), .symbol("|"):
            return true
        default:
            return false
        }
    }

    /// [88] Path ::= PathAlternative
    private func parsePropertyPath() throws -> PropertyPath {
        try parsePathAlternative()
    }

    /// [89] PathAlternative ::= PathSequence ( '|' PathSequence )*
    private func parsePathAlternative() throws -> PropertyPath {
        var path = try parsePathSequence()
        while isSymbol("|") {
            advance()
            let right = try parsePathSequence()
            path = .alternative(path, right)
        }
        return path
    }

    /// [90] PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
    private func parsePathSequence() throws -> PropertyPath {
        var path = try parsePathEltOrInverse()
        while isSymbol("/") {
            advance()
            let right = try parsePathEltOrInverse()
            path = .sequence(path, right)
        }
        return path
    }

    /// [92] PathEltOrInverse ::= PathElt | '^' PathElt
    private func parsePathEltOrInverse() throws -> PropertyPath {
        if isSymbol("^") {
            advance()
            let elt = try parsePathElt()
            return .inverse(elt)
        }
        return try parsePathElt()
    }

    /// [91] PathElt ::= PathPrimary PathMod?
    /// [94] PathMod ::= '*' | '?' | '+'
    private func parsePathElt() throws -> PropertyPath {
        var path = try parsePathPrimary()
        switch currentToken {
        case .symbol("*"):
            advance()
            path = .zeroOrMore(path)
        case .symbol("+"):
            advance()
            path = .oneOrMore(path)
        case .symbol("?"):
            advance()
            path = .zeroOrOne(path)
        default:
            break
        }
        return path
    }

    /// [95] PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
    private func parsePathPrimary() throws -> PropertyPath {
        switch currentToken {
        case .iri(let iri):
            advance()
            return .iri(resolveIRI(iri))
        case .prefixedName(let prefix, let local):
            advance()
            return .iri(resolvePrefixedName(prefix: prefix, local: local))
        case .keyword("A"):
            advance()
            return .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        case .symbol("!"):
            advance()
            return try parsePathNegatedPropertySet()
        case .symbol("("):
            advance()
            let path = try parsePropertyPath()
            try expectSymbol(")")
            return path
        default:
            throw ParseError.invalidSyntax(
                message: "Expected property path element (IRI, 'a', '!', or '(')",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [96] PathNegatedPropertySet ::= PathOneInPropertySet
    ///      | '(' ( PathOneInPropertySet ( '|' PathOneInPropertySet )* )? ')'
    /// [97] PathOneInPropertySet ::= iri | 'a' | '^' ( iri | 'a' )
    private func parsePathNegatedPropertySet() throws -> PropertyPath {
        if isSymbol("(") {
            advance()
            var iris: [String] = []
            if !isSymbol(")") {
                iris.append(try parsePathOneInPropertySetIRI())
                while isSymbol("|") {
                    advance()
                    iris.append(try parsePathOneInPropertySetIRI())
                }
            }
            try expectSymbol(")")
            return .negation(iris)
        } else {
            let iri = try parsePathOneInPropertySetIRI()
            return .negation([iri])
        }
    }

    /// Parse a single IRI from PathOneInPropertySet (iri | 'a' | '^'(iri|'a'))
    /// Note: '^' prefix for inverse negation returns the IRI as-is (negation handles semantics)
    private func parsePathOneInPropertySetIRI() throws -> String {
        // Skip inverse marker — negation handles both directions
        if isSymbol("^") {
            advance()
        }
        switch currentToken {
        case .iri(let iri):
            advance()
            return resolveIRI(iri)
        case .prefixedName(let prefix, let local):
            advance()
            return resolvePrefixedName(prefix: prefix, local: local)
        case .keyword("A"):
            advance()
            return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        default:
            throw ParseError.invalidSyntax(
                message: "Expected IRI or 'a' in negated property set",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Result of parsing a predicate position — either a simple term or a property path
    private enum VerbOrPath {
        case term(SPARQLTerm)
        case path(PropertyPath)
    }

    /// Parse predicate position: simple verb or property path
    /// If the predicate is a single IRI without path operators, returns .term
    /// If path operators follow, returns .path
    private func parseVerbOrPath() throws -> VerbOrPath {
        // Variable in predicate position is always a simple term
        if case .variable(let name) = currentToken {
            advance()
            return .term(.variable(name))
        }

        // For IRI/prefixedName/a, check if path operators follow
        // ^ and ! and ( always start a path
        if isSymbol("^") || isSymbol("!") || isSymbol("(") {
            return .path(try parsePropertyPath())
        }

        // Parse the IRI-like token
        let savedPos = position
        let savedTok = currentToken

        let iri: String
        switch currentToken {
        case .keyword("A"):
            advance()
            // Check if path operator follows 'a'
            if isPathModifier() || isPathOperator() || isSymbol("^") {
                position = savedPos
                currentToken = savedTok
                return .path(try parsePropertyPath())
            }
            return .term(.rdfType)
        case .iri(let i):
            iri = resolveIRI(i)
        case .prefixedName(let prefix, let local):
            iri = resolvePrefixedName(prefix: prefix, local: local)
            _ = iri  // suppress unused warning
        case .string(let str, _, _, _):
            // Framework extension: string literal in predicate position → treat as IRI
            advance()
            return .term(.iri(str))
        default:
            throw ParseError.invalidSyntax(
                message: "Expected verb or property path",
                position: input.distance(from: input.startIndex, to: position)
            )
        }

        // Consume the IRI token
        advance()

        // Check if path operators follow — if so, backtrack and parse as full path
        if isPathModifier() || isPathOperator() {
            position = savedPos
            currentToken = savedTok
            return .path(try parsePropertyPath())
        }

        // Simple IRI verb — use resolved iri
        switch savedTok {
        case .iri:
            return .term(.iri(iri))
        case .prefixedName:
            return .term(.iri(iri))
        default:
            return .term(.iri(iri))
        }
    }
}

// MARK: - Query Parsing

extension SPARQLParser {
    private func parsePrologue() throws {
        while true {
            switch currentToken {
            case .keyword("PREFIX"):
                advance()
                guard case .prefixedName(let prefix, _) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected prefix name", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                prefixes[prefix] = iri
                advance()

            case .keyword("BASE"):
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI", position: input.distance(from: input.startIndex, to: position))
                }
                self.baseIRI = iri
                advance()

            // SPARQL 1.2: VERSION declaration
            case .keyword("VERSION"):
                advance()
                guard case .string(let version, _, _, _) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected version string", position: input.distance(from: input.startIndex, to: position))
                }
                self.sparqlVersion = version
                advance()

            default:
                return
            }
        }
    }

    private func parseSelectQuery() throws -> SelectQuery {
        try expect("SELECT")

        // DISTINCT / REDUCED
        var distinct = false
        var reduced = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        } else if case .keyword("REDUCED") = currentToken {
            reduced = true
            advance()
        }

        // Projection
        let projection = try parseSelectClause()

        // Dataset clauses (FROM / FROM NAMED)
        var fromIRIs: [String] = []
        var fromNamedIRIs: [String] = []
        while case .keyword("FROM") = currentToken {
            advance()
            if case .keyword("NAMED") = currentToken {
                advance()
                if case .iri(let iri) = currentToken {
                    fromNamedIRIs.append(resolveIRI(iri))
                    advance()
                }
            } else if case .iri(let iri) = currentToken {
                fromIRIs.append(resolveIRI(iri))
                advance()
            }
        }

        // WHERE clause
        var pattern: GraphPattern = .basic([])
        if case .keyword("WHERE") = currentToken {
            advance()
        }
        if case .symbol("{") = currentToken {
            pattern = try parseGroupGraphPattern()
        }

        // Solution modifiers
        var groupBy: [Expression]?
        if case .keyword("GROUP") = currentToken {
            advance()
            try expect("BY")
            groupBy = try parseGroupCondition()
        }

        var having: Expression?
        if case .keyword("HAVING") = currentToken {
            advance()
            having = try parseConstraint()
        }

        var orderBy: [SortKey]?
        if case .keyword("ORDER") = currentToken {
            advance()
            try expect("BY")
            orderBy = try parseOrderCondition()
        }

        var limit: Int?
        if case .keyword("LIMIT") = currentToken {
            advance()
            if case .integer(let n) = currentToken {
                limit = Int(n)
                advance()
            }
        }

        var offset: Int?
        if case .keyword("OFFSET") = currentToken {
            advance()
            if case .integer(let n) = currentToken {
                offset = Int(n)
                advance()
            }
        }

        return SelectQuery(
            projection: projection,
            source: .graphPattern(pattern),
            filter: nil,
            groupBy: groupBy,
            having: having,
            orderBy: orderBy,
            limit: limit,
            offset: offset,
            distinct: distinct,
            reduced: reduced,
            from: fromIRIs.isEmpty ? nil : fromIRIs,
            fromNamed: fromNamedIRIs.isEmpty ? nil : fromNamedIRIs
        )
    }

    private func parseSelectClause() throws -> Projection {
        if case .symbol("*") = currentToken {
            advance()
            return .all
        }

        var items: [ProjectionItem] = []
        while true {
            switch currentToken {
            case .variable(let name):
                items.append(ProjectionItem(.variable(Variable(name))))
                advance()
            case .symbol("("):
                advance()
                let expr = try parseExpression()
                try expect("AS")
                guard case .variable(let alias) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected variable after AS", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                try expectSymbol(")")
                items.append(ProjectionItem(expr, alias: alias))
            default:
                break
            }

            // Check if next token starts a new select item
            if case .variable = currentToken { continue }
            if case .symbol("(") = currentToken { continue }
            break
        }

        return .items(items)
    }

    /// SPARQL 1.1 Grammar [54]
    /// GroupGraphPattern ::= '{' ( SubSelect | GroupGraphPatternSub ) '}'
    private func parseGroupGraphPattern() throws -> GraphPattern {
        try expectSymbol("{")
        let pattern: GraphPattern
        if isKeyword("SELECT") {
            // SubSelect: full SELECT query as subquery
            pattern = .subquery(try parseSelectQuery())
        } else {
            pattern = try parseGroupGraphPatternSub()
        }
        try expectSymbol("}")
        return pattern
    }

    /// SPARQL 1.1 Grammar [67]
    /// GroupOrUnionGraphPattern ::= GroupGraphPattern ( 'UNION' GroupGraphPattern )*
    private func parseGroupOrUnionGraphPattern() throws -> GraphPattern {
        var result = try parseGroupGraphPattern()
        while isKeyword("UNION") {
            advance() // consume 'UNION'
            let right = try parseGroupGraphPattern()
            result = .union(result, right)
        }
        return result
    }

    /// SPARQL 1.1 Grammar [55]
    /// GroupGraphPatternSub ::= TriplesBlock? ( GraphPatternNotTriples '.'? TriplesBlock? )*
    ///
    /// Algebra translation per W3C §18.2.2:
    /// - TriplesBlock        → Join(accumulated, BGP)
    /// - OPTIONAL            → LeftJoin(accumulated, opt)
    /// - MINUS               → Minus(accumulated, minus)
    /// - FILTER              → Filter(accumulated, expr)
    /// - BIND                → Extend(accumulated, var, expr)
    /// - GroupOrUnion/Graph/Service/Values → Join(accumulated, pattern)
    private func parseGroupGraphPatternSub() throws -> GraphPattern {
        log("parseGroupGraphPatternSub() START, token: \(currentToken)")

        var accumulated: GraphPattern? = nil

        func joinAccumulated(_ pattern: GraphPattern) {
            if let existing = accumulated {
                accumulated = .join(existing, pattern)
            } else {
                accumulated = pattern
            }
        }

        // 1. Optional leading TriplesBlock
        if canStartTriple() {
            let triplesBlock = try parseTriplesBlock()
            log("parseGroupGraphPatternSub() parsed leading TriplesBlock")
            joinAccumulated(triplesBlock)
        }

        // 2. ( GraphPatternNotTriples '.'? TriplesBlock? )*
        while canStartGraphPatternNotTriples() {
            log("parseGroupGraphPatternSub() GraphPatternNotTriples, token: \(currentToken)")

            // Parse GraphPatternNotTriples [57]
            switch currentToken {
            case .symbol("{"):
                // GroupOrUnionGraphPattern [67]
                let unionPattern = try parseGroupOrUnionGraphPattern()
                joinAccumulated(unionPattern)

            case .keyword("OPTIONAL"):
                advance()
                let optPattern = try parseGroupGraphPattern()
                accumulated = .optional(accumulated ?? .basic([]), optPattern)

            case .keyword("MINUS"):
                advance()
                let minusPattern = try parseGroupGraphPattern()
                accumulated = .minus(accumulated ?? .basic([]), minusPattern)

            case .keyword("LATERAL"):
                advance()
                let lateralPattern = try parseGroupGraphPattern()
                accumulated = .lateral(accumulated ?? .basic([]), lateralPattern)

            case .keyword("FILTER"):
                advance()
                let constraint = try parseConstraint()
                accumulated = .filter(accumulated ?? .basic([]), constraint)

            case .keyword("BIND"):
                advance()
                try expectSymbol("(")
                let expr = try parseExpression()
                try expect("AS")
                guard case .variable(let varName) = currentToken else {
                    throw ParseError.invalidSyntax(
                        message: "Expected variable after AS",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
                advance()
                try expectSymbol(")")
                accumulated = .bind(accumulated ?? .basic([]), variable: varName, expression: expr)

            case .keyword("VALUES"):
                let valuesPattern = try parseInlineData()
                joinAccumulated(valuesPattern)

            case .keyword("GRAPH"):
                advance()
                let graphName = try parseTerm()
                let graphPattern = try parseGroupGraphPattern()
                joinAccumulated(.graph(name: graphName, pattern: graphPattern))

            case .keyword("SERVICE"):
                advance()
                var silent = false
                if isKeyword("SILENT") {
                    silent = true
                    advance()
                }
                guard case .iri(let endpoint) = currentToken else {
                    throw ParseError.invalidSyntax(
                        message: "Expected IRI for SERVICE",
                        position: input.distance(from: input.startIndex, to: position)
                    )
                }
                advance()
                let servicePattern = try parseGroupGraphPattern()
                joinAccumulated(.service(endpoint: endpoint, pattern: servicePattern, silent: silent))

            default:
                break
            }

            // '.'? — Consume optional dot after GraphPatternNotTriples
            if isSymbol(".") {
                log("parseGroupGraphPatternSub() consuming optional dot after GraphPatternNotTriples")
                advance()
            }

            // TriplesBlock? — Optional TriplesBlock after GraphPatternNotTriples
            if canStartTriple() {
                let triplesBlock = try parseTriplesBlock()
                log("parseGroupGraphPatternSub() parsed TriplesBlock after GraphPatternNotTriples")
                joinAccumulated(triplesBlock)
            }
        }

        log("parseGroupGraphPatternSub() END, accumulated: \(accumulated != nil)")
        return accumulated ?? .basic([])
    }

    /// SPARQL 1.1 Grammar [56]
    /// TriplesBlock ::= TriplesSameSubjectPath ( '.' TriplesBlock? )?
    private func parseTriplesBlock() throws -> GraphPattern {
        log("parseTriplesBlock() START, token: \(currentToken)")
        var allTriples: [TriplePattern] = []
        var pathPatterns: [GraphPattern] = []

        func collectPattern(_ pattern: GraphPattern) {
            switch pattern {
            case .basic(let triples):
                allTriples.append(contentsOf: triples)
            case .join(let l, let r):
                collectPattern(l)
                collectPattern(r)
            default:
                pathPatterns.append(pattern)
            }
        }

        // Parse first TriplesSameSubjectPath
        collectPattern(try parseTriplesSameSubjectPath())

        // ( '.' TriplesBlock? )?  — recursive via loop
        while isSymbol(".") {
            advance() // consume '.'
            guard canStartTriple() else { break }
            collectPattern(try parseTriplesSameSubjectPath())
        }

        // Flatten: basic triples first (if any), then join path patterns
        var result: GraphPattern?
        if !allTriples.isEmpty {
            result = .basic(allTriples)
        }
        for pathPattern in pathPatterns {
            if let existing = result {
                result = .join(existing, pathPattern)
            } else {
                result = pathPattern
            }
        }

        log("parseTriplesBlock() END, next token: \(currentToken)")
        return result ?? .basic([])
    }

    /// Parse one subject with its predicate-object lists
    /// SPARQL 1.1 Grammar [75] TriplesSameSubjectPath ::= VarOrTerm PropertyListPathNotEmpty
    /// Returns GraphPattern — .basic for simple triples, .join with .propertyPath for paths
    private func parseTriplesSameSubjectPath() throws -> GraphPattern {
        var triples: [TriplePattern] = []
        var pathPatterns: [GraphPattern] = []
        let subject = try parseTerm()
        // Collect pending triples generated by blank node / collection parsing
        if !pendingTriples.isEmpty {
            triples.append(contentsOf: pendingTriples)
            pendingTriples.removeAll()
        }
        log("parseTriplesSameSubjectPath() subject: \(subject), next token: \(currentToken)")

        // PropertyListPathNotEmpty [77]:
        //   (VerbPath | VerbSimple) ObjectList ( ';' ( (VerbPath | VerbSimple) ObjectList )? )*
        //
        // Special case: blank node subjects like `[] :p :o` already have triples
        // from parseTerm(), but may also have a property list following.
        // Empty blank nodes `[]` used as subject will have a property list.
        // Blank nodes with inline properties `[ :p :o ]` may have no property list.
        var firstPredicate = true
        while true {
            if !firstPredicate {
                guard isSymbol(";") else { break }
                advance() // consume ';'
                if !canStartVerb() { break }
            }

            // If there's no verb following, this subject has no (more) property list
            if !canStartVerb() { break }
            firstPredicate = false

            let verbOrPath = try parseVerbOrPath()

            // ObjectList [79]: Object ( ',' Object )*
            switch verbOrPath {
            case .term(let predicate):
                let object = try parseTerm()
                triples.append(TriplePattern(subject: subject, predicate: predicate, object: object))
                // Collect pending triples from object-position blank nodes / collections
                if !pendingTriples.isEmpty {
                    triples.append(contentsOf: pendingTriples)
                    pendingTriples.removeAll()
                }
                // SPARQL 1.2: Annotation syntax {| annPred annObj |}
                if isSymbol("{|") {
                    let annotationTriples = try parseAnnotation(subject: subject, predicate: predicate, object: object)
                    triples.append(contentsOf: annotationTriples)
                }
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    triples.append(TriplePattern(subject: subject, predicate: predicate, object: nextObj))
                    if !pendingTriples.isEmpty {
                        triples.append(contentsOf: pendingTriples)
                        pendingTriples.removeAll()
                    }
                    // Check for annotation on comma-separated objects too
                    if isSymbol("{|") {
                        let annotationTriples = try parseAnnotation(subject: subject, predicate: predicate, object: nextObj)
                        triples.append(contentsOf: annotationTriples)
                    }
                }
            case .path(let path):
                let object = try parseTerm()
                pathPatterns.append(.propertyPath(subject: subject, path: path, object: object))
                if !pendingTriples.isEmpty {
                    triples.append(contentsOf: pendingTriples)
                    pendingTriples.removeAll()
                }
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    pathPatterns.append(.propertyPath(subject: subject, path: path, object: nextObj))
                    if !pendingTriples.isEmpty {
                        triples.append(contentsOf: pendingTriples)
                        pendingTriples.removeAll()
                    }
                }
            }
        }

        // Combine basic triples and property path patterns
        if pathPatterns.isEmpty {
            return .basic(triples)
        }

        var result: GraphPattern = triples.isEmpty ? pathPatterns[0] : .basic(triples)
        let startIdx = triples.isEmpty ? 1 : 0
        for i in startIdx..<pathPatterns.count {
            result = .join(result, pathPatterns[i])
        }
        return result
    }

    private func parseTerm() throws -> SPARQLTerm {
        switch currentToken {
        // RDF-star / SPARQL 1.2: << ... >>
        case .symbol("<<"):
            advance() // consume <<
            // SPARQL 1.2 triple term: <<( s p o )>>
            if isSymbol("(") {
                advance() // consume (
                let subject = try parseTerm()
                let predicate = try parseTerm()
                let object = try parseTerm()
                try expectSymbol(")")
                try expectSymbol(">>")
                return .quotedTriple(subject: subject, predicate: predicate, object: object)
            }
            // RDF-star quoted triple: << s p o >> or reified: << s p o ~?r >>
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            // Check for reifier ~
            if isSymbol("~") {
                advance() // consume ~
                let reifier = try parseTerm()
                try expectSymbol(">>")
                return .reifiedTriple(subject: subject, predicate: predicate, object: object, reifier: reifier)
            }
            try expectSymbol(">>")
            return .quotedTriple(subject: subject, predicate: predicate, object: object)

        // Anonymous blank node: [] or [ predicate object ; ... ]
        case .symbol("["):
            advance() // consume '['
            let bnId = freshBlankNode()
            if isSymbol("]") {
                advance() // empty blank node []
                return .blankNode(bnId)
            }
            // [ predicate-object list ]
            let bnTerm = SPARQLTerm.blankNode(bnId)
            var firstPred = true
            while true {
                if !firstPred {
                    guard isSymbol(";") else { break }
                    advance()
                    if isSymbol("]") { break }  // trailing semicolon
                }
                firstPred = false
                let predicate = try parseTerm()
                let object = try parseTerm()
                pendingTriples.append(TriplePattern(subject: bnTerm, predicate: predicate, object: object))
                while isSymbol(",") {
                    advance()
                    let nextObj = try parseTerm()
                    pendingTriples.append(TriplePattern(subject: bnTerm, predicate: predicate, object: nextObj))
                }
            }
            try expectSymbol("]")
            return bnTerm

        // RDF Collection: (term1 term2 ...)
        case .symbol("("):
            advance() // consume '('
            if isSymbol(")") {
                advance()
                return .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")
            }
            // Build rdf:first/rdf:rest chain
            let rdfFirst = SPARQLTerm.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#first")
            let rdfRest = SPARQLTerm.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#rest")
            let rdfNil = SPARQLTerm.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil")

            let headId = freshBlankNode()
            var currentBnId = headId
            var isFirst = true
            while !isSymbol(")") {
                if !isFirst {
                    let nextBnId = freshBlankNode()
                    pendingTriples.append(TriplePattern(
                        subject: .blankNode(currentBnId),
                        predicate: rdfRest,
                        object: .blankNode(nextBnId)
                    ))
                    currentBnId = nextBnId
                }
                isFirst = false
                let element = try parseTerm()
                pendingTriples.append(TriplePattern(
                    subject: .blankNode(currentBnId),
                    predicate: rdfFirst,
                    object: element
                ))
            }
            // Close the list
            pendingTriples.append(TriplePattern(
                subject: .blankNode(currentBnId),
                predicate: rdfRest,
                object: rdfNil
            ))
            try expectSymbol(")")
            return .blankNode(headId)

        case .variable(let name):
            advance()
            return .variable(name)

        case .iri(let iri):
            advance()
            return .iri(resolveIRI(iri))

        case .prefixedName(let prefix, let local):
            advance()
            return .prefixedName(prefix: prefix, local: local)

        case .string(let value, let language, let datatype, let direction):
            advance()
            if let lang = language, let dir = direction {
                return .literal(.dirLangLiteral(value: value, language: lang, direction: dir))
            } else if let lang = language {
                return .literal(.langLiteral(value: value, language: lang))
            } else if let dt = datatype {
                return .literal(.typedLiteral(value: value, datatype: dt))
            } else {
                return .literal(.string(value))
            }

        case .integer(let n):
            advance()
            return .literal(.int(Int64(n) ?? 0))

        case .decimal(let n), .double(let n):
            advance()
            return .literal(.double(Double(n) ?? 0))

        case .blankNode(let id):
            advance()
            return .blankNode(id)

        case .keyword("TRUE"):
            advance()
            return .literal(.bool(true))

        case .keyword("FALSE"):
            advance()
            return .literal(.bool(false))

        default:
            throw ParseError.invalidSyntax(
                message: "Expected term",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [69] Constraint ::= BrackettedExpression | BuiltInCall | FunctionCall
    private func parseConstraint() throws -> Expression {
        // BrackettedExpression
        if case .symbol("(") = currentToken {
            advance()
            let expr = try parseExpression()
            try expectSymbol(")")
            return expr
        }
        // BuiltInCall [121]
        if case .keyword(let kw) = currentToken, Self.builtInFunctionKeywords.contains(kw) {
            return try parseBuiltInCall()
        }
        // FunctionCall [70]: iri ArgList
        if case .iri(let iri) = currentToken {
            advance()
            return try parseIRIFunctionCall(iri: iri)
        }
        if case .prefixedName(let prefix, let local) = currentToken {
            advance()
            let resolved = resolvePrefixedName(prefix: prefix, local: local)
            return try parseIRIFunctionCall(iri: resolved)
        }
        // No valid Constraint production matched
        throw ParseError.invalidSyntax(
            message: "Expected constraint (bracketed expression, built-in call, or function call)",
            position: input.distance(from: input.startIndex, to: position)
        )
    }

    /// [121] BuiltInCall — All SPARQL 1.1 built-in functions
    private func parseBuiltInCall() throws -> Expression {
        switch currentToken {
        // BOUND [121]: 'BOUND' '(' Var ')'
        case .keyword("BOUND"):
            advance()
            try expectSymbol("(")
            guard case .variable(let varName) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected variable", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            try expectSymbol(")")
            return .bound(Variable(varName))

        // NotExistsFunc [126]: 'NOT' 'EXISTS' GroupGraphPattern
        case .keyword("NOT"):
            advance()
            if case .keyword("EXISTS") = currentToken {
                advance()
                let pattern = try parseGroupGraphPattern()
                return .not(.exists(SelectQuery(
                    projection: .all,
                    source: .graphPattern(pattern)
                )))
            }
            return .not(try parseConstraint())

        // ExistsFunc [125]: 'EXISTS' GroupGraphPattern
        case .keyword("EXISTS"):
            advance()
            let pattern = try parseGroupGraphPattern()
            return .exists(SelectQuery(
                projection: .all,
                source: .graphPattern(pattern)
            ))

        // RegexExpression [122]: 'REGEX' '(' Expression ',' Expression (',' Expression)? ')'
        case .keyword("REGEX"):
            advance()
            try expectSymbol("(")
            let text = try parseExpression()
            try expectSymbol(",")
            let patternExpr = try parseExpression()
            var flagsExpr: Expression?
            if isSymbol(",") {
                advance()
                flagsExpr = try parseExpression()
            }
            try expectSymbol(")")
            // If pattern is a string literal and flags (if present) is also a string literal,
            // use the dedicated .regex AST node. Otherwise fall through to generic function call.
            if case .literal(.string(let pattern)) = patternExpr {
                if let fExpr = flagsExpr {
                    if case .literal(.string(let f)) = fExpr {
                        return .regex(text, pattern: pattern, flags: f)
                    }
                    // flags is not a string literal — fall through to .function()
                } else {
                    return .regex(text, pattern: pattern, flags: nil)
                }
            }
            // Generic function call for non-literal pattern or non-literal flags
            var args: [Expression] = [text, patternExpr]
            if let f = flagsExpr { args.append(f) }
            return .function(FunctionCall(name: "REGEX", arguments: args))

        // Aggregate [127]: COUNT, SUM, AVG, MIN, MAX, SAMPLE
        case .keyword("COUNT"), .keyword("SUM"), .keyword("AVG"),
             .keyword("MIN"), .keyword("MAX"), .keyword("SAMPLE"):
            return try parseSPARQLAggregate()

        // Aggregate [127]: GROUP_CONCAT
        case .keyword("GROUP_CONCAT"):
            return try parseGroupConcat()

        // IF [121]: 'IF' '(' Expression ',' Expression ',' Expression ')'
        case .keyword("IF"):
            advance()
            try expectSymbol("(")
            let condition = try parseExpression()
            try expectSymbol(",")
            let thenExpr = try parseExpression()
            try expectSymbol(",")
            let elseExpr = try parseExpression()
            try expectSymbol(")")
            return .function(FunctionCall(name: "IF", arguments: [condition, thenExpr, elseExpr]))

        // COALESCE [121]: 'COALESCE' ExpressionList
        case .keyword("COALESCE"):
            advance()
            let args = try parseExpressionList()
            return .coalesce(args)

        // CONCAT [121]: 'CONCAT' ExpressionList
        case .keyword("CONCAT"):
            advance()
            let args = try parseExpressionList()
            return .function(FunctionCall(name: "CONCAT", arguments: args))

        // 0-arg functions [121]: NOW/RAND/UUID/STRUUID NIL
        case .keyword("NOW"), .keyword("RAND"), .keyword("UUID"), .keyword("STRUUID"):
            guard case .keyword(let name) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected function name", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            try expectSymbol("(")
            try expectSymbol(")")
            return .function(FunctionCall(name: name, arguments: []))

        // BNODE [121]: 'BNODE' ( '(' Expression ')' | NIL )
        case .keyword("BNODE"):
            advance()
            try expectSymbol("(")
            if isSymbol(")") {
                advance()
                return .function(FunctionCall(name: "BNODE", arguments: []))
            }
            let arg = try parseExpression()
            try expectSymbol(")")
            return .function(FunctionCall(name: "BNODE", arguments: [arg]))

        // Generic 1-arg functions [121]
        case .keyword("STR"), .keyword("LANG"), .keyword("DATATYPE"),
             .keyword("IRI"), .keyword("URI"),
             .keyword("ABS"), .keyword("CEIL"), .keyword("FLOOR"), .keyword("ROUND"),
             .keyword("STRLEN"), .keyword("UCASE"), .keyword("LCASE"),
             .keyword("ENCODE_FOR_URI"),
             .keyword("YEAR"), .keyword("MONTH"), .keyword("DAY"),
             .keyword("HOURS"), .keyword("MINUTES"), .keyword("SECONDS"),
             .keyword("TIMEZONE"), .keyword("TZ"),
             .keyword("MD5"), .keyword("SHA1"), .keyword("SHA256"),
             .keyword("SHA384"), .keyword("SHA512"),
             .keyword("ISIRI"), .keyword("ISURI"), .keyword("ISBLANK"),
             .keyword("ISLITERAL"), .keyword("ISNUMERIC"),
             // SPARQL 1.2: language direction functions (1-arg)
             .keyword("LANGDIR"), .keyword("HASLANG"), .keyword("HASLANGDIR"):
            return try parseGenericFunctionCall()

        // SPARQL 1.2: STRLANGDIR(string, lang, dir) — 3-arg function
        case .keyword("STRLANGDIR"):
            return try parseGenericFunctionCall()

        // Generic 2-arg functions [121]
        case .keyword("LANGMATCHES"), .keyword("CONTAINS"),
             .keyword("STRSTARTS"), .keyword("STRENDS"),
             .keyword("STRBEFORE"), .keyword("STRAFTER"),
             .keyword("SAMETERM"), .keyword("STRDT"), .keyword("STRLANG"):
            return try parseGenericFunctionCall()

        // SubstringExpression [123]: 'SUBSTR' '(' Expression ',' Expression (',' Expression)? ')'
        case .keyword("SUBSTR"):
            return try parseGenericFunctionCall()

        // StrReplaceExpression [124]: 'REPLACE' '(' Expression ',' Expression ',' Expression (',' Expression)? ')'
        case .keyword("REPLACE"):
            return try parseGenericFunctionCall()

        // RDF-star (SPARQL-star) built-in functions
        case .keyword("ISTRIPLE"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .isTriple(arg)

        case .keyword("TRIPLE"):
            advance()
            try expectSymbol("(")
            let s = try parseExpression()
            try expectSymbol(",")
            let p = try parseExpression()
            try expectSymbol(",")
            let o = try parseExpression()
            try expectSymbol(")")
            return .triple(subject: s, predicate: p, object: o)

        case .keyword("SUBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .subject(arg)

        case .keyword("PREDICATE"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .predicate(arg)

        case .keyword("OBJECT"):
            advance()
            try expectSymbol("(")
            let arg = try parseExpression()
            try expectSymbol(")")
            return .object(arg)

        default:
            throw ParseError.invalidSyntax(
                message: "Unknown built-in function",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// Generic function call parser for built-in functions with comma-separated arguments.
    /// Handles 1-arg, 2-arg, and variadic functions uniformly.
    /// keyword '(' Expression ( ',' Expression )* ')'
    private func parseGenericFunctionCall() throws -> Expression {
        guard case .keyword(let name) = currentToken else {
            throw ParseError.invalidSyntax(
                message: "Expected function name",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        try expectSymbol("(")
        var args: [Expression] = []
        if !isSymbol(")") {
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return .function(FunctionCall(name: name, arguments: args))
    }

    /// [127] Aggregate ::= 'COUNT' '(' 'DISTINCT'? ( '*' | Expression ) ')'
    ///                    | 'SUM'/'AVG'/'MIN'/'MAX'/'SAMPLE' '(' 'DISTINCT'? Expression ')'
    private func parseSPARQLAggregate() throws -> Expression {
        guard case .keyword(let funcName) = currentToken else {
            throw ParseError.invalidSyntax(
                message: "Expected aggregate function",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
        advance()
        try expectSymbol("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        var arg: Expression?
        if isSymbol("*") {
            advance()
            arg = nil
        } else {
            arg = try parseExpression()
        }

        try expectSymbol(")")

        switch funcName {
        case "COUNT":
            return .aggregate(.count(arg, distinct: distinct))
        case "SUM":
            return .aggregate(.sum(arg ?? .literal(.null), distinct: distinct))
        case "AVG":
            return .aggregate(.avg(arg ?? .literal(.null), distinct: distinct))
        case "MIN":
            return .aggregate(.min(arg ?? .literal(.null)))
        case "MAX":
            return .aggregate(.max(arg ?? .literal(.null)))
        case "SAMPLE":
            return .aggregate(.sample(arg ?? .literal(.null)))
        default:
            throw ParseError.invalidSyntax(
                message: "Unknown aggregate function: \(funcName)",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    /// [127] GROUP_CONCAT '(' 'DISTINCT'? Expression ( ';' 'SEPARATOR' '=' String )? ')'
    private func parseGroupConcat() throws -> Expression {
        advance() // consume GROUP_CONCAT
        try expectSymbol("(")

        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }

        let expr = try parseExpression()

        var separator: String?
        if isSymbol(";") {
            advance()
            try expect("SEPARATOR")
            try expectSymbol("=")
            guard case .string(let sep, _, _, _) = currentToken else {
                throw ParseError.invalidSyntax(
                    message: "Expected separator string",
                    position: input.distance(from: input.startIndex, to: position)
                )
            }
            separator = sep
            advance()
        }

        try expectSymbol(")")
        return .aggregate(.groupConcat(expr, separator: separator, distinct: distinct))
    }

    /// [120] ExpressionList ::= NIL | '(' Expression ( ',' Expression )* ')'
    private func parseExpressionList() throws -> [Expression] {
        try expectSymbol("(")
        if isSymbol(")") {
            advance()
            return []
        }
        var args: [Expression] = []
        args.append(try parseExpression())
        while isSymbol(",") {
            advance()
            args.append(try parseExpression())
        }
        try expectSymbol(")")
        return args
    }

    /// [70] FunctionCall ::= iri ArgList
    /// [71] ArgList ::= NIL | '(' 'DISTINCT'? Expression ( ',' Expression )* ')'
    /// [128] iriOrFunction ::= iri ArgList?
    private func parseIRIFunctionCall(iri: String) throws -> Expression {
        try expectSymbol("(")
        var distinct = false
        if case .keyword("DISTINCT") = currentToken {
            distinct = true
            advance()
        }
        var args: [Expression] = []
        if !isSymbol(")") {
            args.append(try parseExpression())
            while isSymbol(",") {
                advance()
                args.append(try parseExpression())
            }
        }
        try expectSymbol(")")
        return .function(FunctionCall(name: iri, arguments: args, distinct: distinct))
    }

    private func parseExpression() throws -> Expression {
        try parseOrExpression()
    }

    private func parseOrExpression() throws -> Expression {
        var left = try parseAndExpression()
        while case .symbol("||") = currentToken {
            advance()
            let right = try parseAndExpression()
            left = .or(left, right)
        }
        return left
    }

    private func parseAndExpression() throws -> Expression {
        var left = try parseRelationalExpression()
        while case .symbol("&&") = currentToken {
            advance()
            let right = try parseRelationalExpression()
            left = .and(left, right)
        }
        return left
    }

    private func parseRelationalExpression() throws -> Expression {
        let left = try parseAdditiveExpression()

        switch currentToken {
        case .symbol("="):
            advance()
            return .equal(left, try parseAdditiveExpression())
        case .symbol("!="):
            advance()
            return .notEqual(left, try parseAdditiveExpression())
        case .symbol("<"):
            advance()
            return .lessThan(left, try parseAdditiveExpression())
        case .symbol(">"):
            advance()
            return .greaterThan(left, try parseAdditiveExpression())
        case .symbol("<="):
            advance()
            return .lessThanOrEqual(left, try parseAdditiveExpression())
        case .symbol(">="):
            advance()
            return .greaterThanOrEqual(left, try parseAdditiveExpression())
        case .keyword("IN"):
            advance()
            try expectSymbol("(")
            var values: [Expression] = []
            if !isSymbol(")") {
                var first = true
                while first || isSymbol(",") {
                    if !first { advance() }
                    first = false
                    values.append(try parseExpression())
                }
            }
            try expectSymbol(")")
            return .inList(left, values: values)
        case .keyword("NOT"):
            let savedPos = position
            let savedTok = currentToken
            advance()
            if case .keyword("IN") = currentToken {
                advance()
                try expectSymbol("(")
                var values: [Expression] = []
                if !isSymbol(")") {
                    var first = true
                    while first || isSymbol(",") {
                        if !first { advance() }
                        first = false
                        values.append(try parseExpression())
                    }
                }
                try expectSymbol(")")
                return .notInList(left, values: values)
            } else {
                position = savedPos
                currentToken = savedTok
                return left
            }
        default:
            return left
        }
    }

    private func parseAdditiveExpression() throws -> Expression {
        var left = try parseMultiplicativeExpression()
        while case .symbol(let s) = currentToken, ["+", "-"].contains(s) {
            advance()
            let right = try parseMultiplicativeExpression()
            if s == "+" {
                left = .add(left, right)
            } else {
                left = .subtract(left, right)
            }
        }
        return left
    }

    private func parseMultiplicativeExpression() throws -> Expression {
        var left = try parseUnaryExpression()
        while case .symbol(let s) = currentToken, ["*", "/"].contains(s) {
            advance()
            let right = try parseUnaryExpression()
            if s == "*" {
                left = .multiply(left, right)
            } else {
                left = .divide(left, right)
            }
        }
        return left
    }

    private func parseUnaryExpression() throws -> Expression {
        switch currentToken {
        case .symbol("!"):
            advance()
            return .not(try parseUnaryExpression())
        case .symbol("-"):
            advance()
            return .negate(try parseUnaryExpression())
        case .symbol("+"):
            advance()
            return try parsePrimaryExpression()
        default:
            return try parsePrimaryExpression()
        }
    }

    /// [119] PrimaryExpression ::= BrackettedExpression | BuiltInCall | iriOrFunction
    ///                            | RDFLiteral | NumericLiteral | BooleanLiteral | Var
    private func parsePrimaryExpression() throws -> Expression {
        switch currentToken {
        // BrackettedExpression
        case .symbol("("):
            advance()
            let expr = try parseExpression()
            try expectSymbol(")")
            return expr

        // Var
        case .variable(let name):
            advance()
            return .variable(Variable(name))

        // iriOrFunction [128]: iri ArgList?
        case .iri(let iri):
            advance()
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: iri)
            }
            return .literal(.iri(iri))

        // iriOrFunction [128]: prefixedName ArgList?
        case .prefixedName(let prefix, let local):
            advance()
            let resolved = resolvePrefixedName(prefix: prefix, local: local)
            if isSymbol("(") {
                return try parseIRIFunctionCall(iri: resolved)
            }
            return .literal(.iri(resolved))

        // RDFLiteral
        case .string(let value, let language, let datatype, let direction):
            advance()
            if let lang = language, let dir = direction {
                return .literal(.dirLangLiteral(value: value, language: lang, direction: dir))
            } else if let lang = language {
                return .literal(.langLiteral(value: value, language: lang))
            } else if let dt = datatype {
                return .literal(.typedLiteral(value: value, datatype: dt))
            } else {
                return .literal(.string(value))
            }

        // NumericLiteral
        case .integer(let n):
            advance()
            return .literal(.int(Int64(n) ?? 0))

        case .decimal(let n), .double(let n):
            advance()
            return .literal(.double(Double(n) ?? 0))

        // BooleanLiteral
        case .keyword("TRUE"):
            advance()
            return .literal(.bool(true))

        case .keyword("FALSE"):
            advance()
            return .literal(.bool(false))

        // BuiltInCall [121]
        case .keyword(let kw) where Self.builtInFunctionKeywords.contains(kw):
            return try parseBuiltInCall()

        // RDF-star / SPARQL 1.2: << ... >> as expression
        case .symbol("<<"):
            advance()
            // SPARQL 1.2 triple term: <<( s p o )>>
            if isSymbol("(") {
                advance()
                let subject = try parseTerm()
                let predicate = try parseTerm()
                let object = try parseTerm()
                try expectSymbol(")")
                try expectSymbol(">>")
                let qt = SPARQLTerm.quotedTriple(subject: subject, predicate: predicate, object: object)
                return qt.toExpression()
            }
            // RDF-star quoted triple or reified triple
            let subject = try parseTerm()
            let predicate = try parseTerm()
            let object = try parseTerm()
            if isSymbol("~") {
                advance()
                let reifier = try parseTerm()
                try expectSymbol(">>")
                let rt = SPARQLTerm.reifiedTriple(subject: subject, predicate: predicate, object: object, reifier: reifier)
                return rt.toExpression()
            }
            try expectSymbol(">>")
            let qt = SPARQLTerm.quotedTriple(subject: subject, predicate: predicate, object: object)
            return qt.toExpression()

        default:
            throw ParseError.invalidSyntax(
                message: "Expected expression",
                position: input.distance(from: input.startIndex, to: position)
            )
        }
    }

    private func parseGroupCondition() throws -> [Expression] {
        var expressions: [Expression] = []

        while isVariable() || isSymbol("(") || isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
            switch currentToken {
            case .variable(let name):
                expressions.append(.variable(Variable(name)))
                advance()
            case .symbol("("):
                advance()
                let expr = try parseExpression()
                if isKeyword("AS") {
                    advance()
                    // Skip alias variable
                    if isVariable() { advance() }
                }
                try expectSymbol(")")
                expressions.append(expr)
            default:
                // Bare built-in function call or IRI function call
                let expr = try parseConstraint()
                expressions.append(expr)
            }
        }

        return expressions
    }

    private func parseOrderCondition() throws -> [SortKey] {
        var keys: [SortKey] = []

        while isVariable() || isKeyword("ASC") || isKeyword("DESC") || isSymbol("(")
              || isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
            var direction: SortDirection = .ascending
            if isKeyword("ASC") {
                advance()
            } else if isKeyword("DESC") {
                direction = .descending
                advance()
            }

            let expr: Expression
            if isSymbol("(") {
                advance()
                expr = try parseExpression()
                try expectSymbol(")")
            } else if case .variable(let name) = currentToken {
                expr = .variable(Variable(name))
                advance()
            } else if isBuiltInFunctionKeyword() || isIRIOrPrefixedName() {
                // Bare function call: STRLEN(?name), <iri>(?x), prefix:func(?x)
                expr = try parseConstraint()
            } else {
                break
            }

            keys.append(SortKey(expr, direction: direction))
        }

        return keys
    }

    private func parseInlineData() throws -> GraphPattern {
        try expect("VALUES")

        var variables: [String] = []

        // Parse variable list
        if case .symbol("(") = currentToken {
            advance()
            while case .variable(let name) = currentToken {
                variables.append(name)
                advance()
            }
            try expectSymbol(")")
        } else if case .variable(let name) = currentToken {
            variables.append(name)
            advance()
        }

        // Parse data block
        try expectSymbol("{")
        var bindings: [[Literal?]] = []

        while case .symbol("(") = currentToken {
            advance()
            var row: [Literal?] = []
            for _ in variables {
                if case .keyword("UNDEF") = currentToken {
                    row.append(nil)
                    advance()
                } else {
                    let term = try parseTerm()
                    row.append(term.literalValue)
                }
            }
            try expectSymbol(")")
            bindings.append(row)
        }

        try expectSymbol("}")

        return .values(variables: variables, bindings: bindings)
    }

    private func parseConstructQuery() throws -> ConstructQuery {
        log("parseConstructQuery() START")
        try expect("CONSTRUCT")

        var template: [TriplePattern] = []
        var pattern: GraphPattern = .basic([])

        // B10: CONSTRUCT WHERE shortcut — template is derived from WHERE pattern
        if isKeyword("WHERE") {
            advance()
            pattern = try parseGroupGraphPattern()
            // Extract BGP triples from pattern as template
            template = extractBGPTriples(from: pattern)
        } else {
            // Parse explicit template { ... }
            try expectSymbol("{")
            var loopCount = 0
            while currentToken != .symbol("}") {
                loopCount += 1
                log("parseConstructQuery() loop[\(loopCount)] token: \(currentToken)")
                if case .eof = currentToken {
                    log("parseConstructQuery() unexpected EOF")
                    throw ParseError.unexpectedEndOfInput(expected: "}")
                }
                if case .basic(let triples) = try parseTriplesBlock() {
                    template.append(contentsOf: triples)
                }
                if case .symbol(".") = currentToken { advance() }
            }
            log("parseConstructQuery() END, template count: \(template.count)")
            try expectSymbol("}")

            // Parse WHERE
            if isKeyword("WHERE") {
                advance()
                pattern = try parseGroupGraphPattern()
            }
        }

        // B11: CONSTRUCT solution modifiers (ORDER BY / LIMIT / OFFSET)
        var orderBy: [SortKey]?
        var limit: Int?
        var offset: Int?

        if isKeyword("ORDER") {
            advance()
            try expect("BY")
            orderBy = try parseOrderCondition()
        }
        if isKeyword("LIMIT") {
            advance()
            if case .integer(let n) = currentToken {
                limit = Int(n)
                advance()
            }
        }
        if isKeyword("OFFSET") {
            advance()
            if case .integer(let n) = currentToken {
                offset = Int(n)
                advance()
            }
        }

        return ConstructQuery(template: template, pattern: pattern, orderBy: orderBy, limit: limit, offset: offset)
    }

    /// Extract BGP triples from a graph pattern (for CONSTRUCT WHERE shortcut)
    private func extractBGPTriples(from pattern: GraphPattern) -> [TriplePattern] {
        switch pattern {
        case .basic(let triples):
            return triples
        case .join(let left, let right):
            return extractBGPTriples(from: left) + extractBGPTriples(from: right)
        case .filter(let inner, _):
            return extractBGPTriples(from: inner)
        default:
            return []
        }
    }

    private func parseAskQuery() throws -> AskQuery {
        try expect("ASK")
        let pattern = try parseGroupGraphPattern()
        return AskQuery(pattern: pattern)
    }

    private func parseDescribeQuery() throws -> DescribeQuery {
        log("parseDescribeQuery() START")
        try expect("DESCRIBE")

        var resources: [SPARQLTerm] = []
        var loopCount = 0

        if case .symbol("*") = currentToken {
            log("parseDescribeQuery() found '*'")
            advance()
        } else {
            while true {
                loopCount += 1
                log("parseDescribeQuery() loop[\(loopCount)] token: \(currentToken)")
                var madeProgress = false
                switch currentToken {
                case .variable(let name):
                    resources.append(.variable(name))
                    advance()
                    madeProgress = true
                case .iri(let iri):
                    resources.append(.iri(iri))
                    advance()
                    madeProgress = true
                case .prefixedName(let prefix, let local):
                    resources.append(.prefixedName(prefix: prefix, local: local))
                    advance()
                    madeProgress = true
                default:
                    log("parseDescribeQuery() default case")
                    break
                }
                if case .keyword("WHERE") = currentToken {
                    log("parseDescribeQuery() found WHERE, breaking")
                    break
                }
                if case .eof = currentToken {
                    log("parseDescribeQuery() found EOF, breaking")
                    break
                }
                if !madeProgress {
                    log("parseDescribeQuery() no progress, breaking")
                    break
                }
            }
        }
        log("parseDescribeQuery() resources count: \(resources.count)")

        var pattern: GraphPattern?
        if case .keyword("WHERE") = currentToken {
            advance()
            pattern = try parseGroupGraphPattern()
        }

        return DescribeQuery(resources: resources, pattern: pattern)
    }

    // MARK: - SPARQL Update Parsing (B12)

    /// Parse INSERT DATA or DELETE/INSERT WHERE
    private func parseInsertOrModify() throws -> QueryStatement {
        try expect("INSERT")
        if isKeyword("DATA") {
            advance()
            return .insertData(try parseInsertDataQuery())
        }
        // INSERT { ... } WHERE { ... }
        return try parseDeleteInsertQuery(deletePattern: nil)
    }

    /// Parse DELETE DATA or DELETE/INSERT WHERE
    private func parseDeleteOrModify() throws -> QueryStatement {
        try expect("DELETE")
        if isKeyword("DATA") {
            advance()
            return .deleteData(try parseDeleteDataQuery())
        }
        // DELETE { ... } [INSERT { ... }] WHERE { ... }
        let deleteQuads = try parseQuadData()
        var insertQuads: [Quad]?
        if isKeyword("INSERT") {
            advance()
            insertQuads = try parseQuadData()
        }
        return try parseDeleteInsertQuery(deletePattern: deleteQuads, insertPattern: insertQuads)
    }

    /// Parse INSERT DATA { quads }
    private func parseInsertDataQuery() throws -> InsertDataQuery {
        let quads = try parseQuadData()
        return InsertDataQuery(quads: quads)
    }

    /// Parse DELETE DATA { quads }
    private func parseDeleteDataQuery() throws -> DeleteDataQuery {
        let quads = try parseQuadData()
        return DeleteDataQuery(quads: quads)
    }

    /// Parse DELETE { } INSERT { } WHERE { }
    private func parseDeleteInsertQuery(
        deletePattern: [Quad]? = nil,
        insertPattern: [Quad]? = nil
    ) throws -> QueryStatement {
        var delPat = deletePattern
        var insPat = insertPattern

        // If we haven't parsed delete/insert blocks yet (INSERT-only form)
        if delPat == nil && insPat == nil {
            insPat = try parseQuadData()
        }

        // Parse USING clauses
        var using: [GraphRef]?
        while isKeyword("USING") {
            advance()
            if using == nil { using = [] }
            if isKeyword("NAMED") {
                advance()
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI after USING NAMED", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                using?.append(GraphRef(iri: iri, isNamed: true))
            } else {
                guard case .iri(let iri) = currentToken else {
                    throw ParseError.invalidSyntax(message: "Expected IRI after USING", position: input.distance(from: input.startIndex, to: position))
                }
                advance()
                using?.append(GraphRef(iri: iri, isNamed: false))
            }
        }

        // Parse WHERE
        try expect("WHERE")
        let wherePattern = try parseGroupGraphPattern()

        return .deleteInsert(DeleteInsertQuery(
            deletePattern: delPat,
            insertPattern: insPat,
            using: using,
            wherePattern: wherePattern
        ))
    }

    /// Parse quad data: { triples | GRAPH <iri> { triples } }
    private func parseQuadData() throws -> [Quad] {
        try expectSymbol("{")
        var quads: [Quad] = []

        while !isSymbol("}") {
            if case .eof = currentToken {
                throw ParseError.unexpectedEndOfInput(expected: "}")
            }

            if isKeyword("GRAPH") {
                advance()
                let graphIRI: String
                switch currentToken {
                case .iri(let iri):
                    graphIRI = iri
                    advance()
                case .prefixedName(let prefix, let local):
                    graphIRI = resolvePrefixedName(prefix: prefix, local: local)
                    advance()
                default:
                    throw ParseError.invalidSyntax(message: "Expected graph IRI", position: input.distance(from: input.startIndex, to: position))
                }
                try expectSymbol("{")
                let graphTerm = SPARQLTerm.iri(graphIRI)
                while !isSymbol("}") {
                    if case .eof = currentToken {
                        throw ParseError.unexpectedEndOfInput(expected: "}")
                    }
                    if case .basic(let triples) = try parseTriplesBlock() {
                        for triple in triples {
                            quads.append(Quad(graph: graphTerm, triple: triple))
                        }
                    }
                    if isSymbol(".") { advance() }
                }
                try expectSymbol("}")
            } else {
                if case .basic(let triples) = try parseTriplesBlock() {
                    for triple in triples {
                        quads.append(Quad(graph: nil, triple: triple))
                    }
                }
            }
            if isSymbol(".") { advance() }
        }
        try expectSymbol("}")
        return quads
    }

    /// Parse LOAD [SILENT] <iri> [INTO GRAPH <iri>]
    private func parseLoadQuery() throws -> LoadQuery {
        try expect("LOAD")
        let silent = parseSilent()

        guard case .iri(let source) = currentToken else {
            throw ParseError.invalidSyntax(message: "Expected source IRI", position: input.distance(from: input.startIndex, to: position))
        }
        advance()

        var destination: String?
        if isKeyword("INTO") {
            advance()
            if isKeyword("GRAPH") { advance() }
            guard case .iri(let dest) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected destination IRI", position: input.distance(from: input.startIndex, to: position))
            }
            destination = dest
            advance()
        }

        return LoadQuery(source: source, destination: destination, silent: silent)
    }

    /// Parse CLEAR [SILENT] (GRAPH <iri> | DEFAULT | NAMED | ALL)
    private func parseClearQuery() throws -> ClearQuery {
        try expect("CLEAR")
        let silent = parseSilent()

        let target: ClearTarget
        switch currentToken {
        case .keyword("GRAPH"):
            advance()
            guard case .iri(let iri) = currentToken else {
                throw ParseError.invalidSyntax(message: "Expected graph IRI", position: input.distance(from: input.startIndex, to: position))
            }
            advance()
            target = .graph(iri)
        case .keyword("DEFAULT"):
            advance()
            target = .default
        case .keyword("NAMED"):
            advance()
            target = .named
        case .keyword("ALL"):
            advance()
            target = .all
        default:
            target = .all
        }

        return ClearQuery(target: target, silent: silent)
    }

    /// Parse CREATE [SILENT] GRAPH <iri>
    private func parseCreateGraph() throws -> QueryStatement {
        try expect("CREATE")
        let silent = parseSilent()
        if isKeyword("GRAPH") { advance() }
        guard case .iri(let iri) = currentToken else {
            throw ParseError.invalidSyntax(message: "Expected graph IRI", position: input.distance(from: input.startIndex, to: position))
        }
        advance()
        return .createSPARQLGraph(iri, silent: silent)
    }

    /// Parse DROP [SILENT] GRAPH <iri>
    private func parseDropGraph() throws -> QueryStatement {
        try expect("DROP")
        let silent = parseSilent()
        if isKeyword("GRAPH") { advance() }
        guard case .iri(let iri) = currentToken else {
            throw ParseError.invalidSyntax(message: "Expected graph IRI", position: input.distance(from: input.startIndex, to: position))
        }
        advance()
        return .dropSPARQLGraph(iri, silent: silent)
    }

    /// Parse optional SILENT keyword, returns true if present
    private func parseSilent() -> Bool {
        if isKeyword("SILENT") {
            advance()
            return true
        }
        return false
    }
}
