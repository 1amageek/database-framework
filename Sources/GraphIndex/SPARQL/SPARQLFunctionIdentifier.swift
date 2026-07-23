import DatabaseValue

/// Canonical classification for every SPARQL function identifier.
///
/// Built-ins are case-insensitive keywords. Every other function name must be
/// a valid absolute RDF IRI; this includes schemes such as `mailto`, `tag`, and
/// `did`, not only identifiers containing `://`.
enum SPARQLFunctionIdentifier: Sendable, Hashable {
    enum BuiltIn: String, Sendable, Hashable {
        case str = "STR"
        case strlen = "STRLEN"
        case ucase = "UCASE"
        case lcase = "LCASE"
        case encodeForURI = "ENCODE_FOR_URI"
        case iri = "IRI"
        case uri = "URI"
        case abs = "ABS"
        case round = "ROUND"
        case ceil = "CEIL"
        case floor = "FLOOR"
        case isIRI = "ISIRI"
        case isURI = "ISURI"
        case isBlank = "ISBLANK"
        case isLiteral = "ISLITERAL"
        case isNumeric = "ISNUMERIC"
        case md5 = "MD5"
        case sha1 = "SHA1"
        case sha256 = "SHA256"
        case sha384 = "SHA384"
        case sha512 = "SHA512"
        case datatype = "DATATYPE"
        case lang = "LANG"
        case langDir = "LANGDIR"
        case hasLang = "HASLANG"
        case hasLangDir = "HASLANGDIR"
        case year = "YEAR"
        case month = "MONTH"
        case day = "DAY"
        case hours = "HOURS"
        case minutes = "MINUTES"
        case seconds = "SECONDS"
        case timezone = "TIMEZONE"
        case tz = "TZ"
        case contains = "CONTAINS"
        case strStarts = "STRSTARTS"
        case strEnds = "STRENDS"
        case strBefore = "STRBEFORE"
        case strAfter = "STRAFTER"
        case langMatches = "LANGMATCHES"
        case sameTerm = "SAMETERM"
        case strDT = "STRDT"
        case strLang = "STRLANG"
        case trigramSimilarity = "TRIGRAM_SIM"
        case substr = "SUBSTR"
        case regex = "REGEX"
        case replace = "REPLACE"
        case conditional = "IF"
        case strLangDir = "STRLANGDIR"
        case bound = "BOUND"
        case coalesce = "COALESCE"
        case now = "NOW"
        case rand = "RAND"
        case uuid = "UUID"
        case strUUID = "STRUUID"
        case blankNode = "BNODE"
        case concat = "CONCAT"

        var arity: ClosedRange<Int> {
            switch self {
            case .str, .strlen, .ucase, .lcase, .encodeForURI, .iri, .uri,
                 .abs, .round, .ceil, .floor, .isIRI, .isURI, .isBlank,
                 .isLiteral, .isNumeric, .md5, .sha1, .sha256, .sha384,
                 .sha512, .datatype, .lang, .langDir, .hasLang,
                 .hasLangDir, .year, .month, .day, .hours, .minutes,
                 .seconds, .timezone, .tz, .bound:
                return 1...1
            case .contains, .strStarts, .strEnds, .strBefore, .strAfter,
                 .langMatches, .sameTerm, .strDT, .strLang,
                 .trigramSimilarity:
                return 2...2
            case .substr, .regex:
                return 2...3
            case .replace:
                return 3...4
            case .conditional, .strLangDir:
                return 3...3
            case .coalesce:
                return 1...Int.max
            case .now, .rand, .uuid, .strUUID:
                return 0...0
            case .blankNode:
                return 0...1
            case .concat:
                return 0...Int.max
            }
        }

        var volatility: SPARQLExpressionPlan.Volatility {
            switch self {
            case .rand, .uuid, .strUUID, .blankNode:
                return .volatile
            case .now:
                return .queryStable
            default:
                return .immutable
            }
        }
    }

    case builtIn(BuiltIn)
    case datatypeConstructor(DatabaseRDFIRI)
    case extensionFunction(DatabaseRDFIRI)

    static let xsdNamespace = "http://www.w3.org/2001/XMLSchema#"

    static func resolve(
        _ rawIdentifier: String
    ) throws(SPARQLExpressionCompilationError) -> Self {
        if let builtIn = BuiltIn(rawValue: rawIdentifier.uppercased()) {
            return .builtIn(builtIn)
        }

        let iri: DatabaseRDFIRI
        do {
            iri = try DatabaseRDFIRI(rawIdentifier)
        } catch {
            throw .invalidFunctionIdentifier(rawIdentifier)
        }
        if Self.isSPARQLDatatypeConstructor(iri.rawValue) {
            return .datatypeConstructor(iri)
        }
        return .extensionFunction(iri)
    }

    private static func isSPARQLDatatypeConstructor(_ identifier: String) -> Bool {
        guard identifier.hasPrefix(xsdNamespace) else { return false }
        switch identifier.dropFirst(xsdNamespace.count) {
        case "string", "float", "double", "decimal", "integer",
             "dateTime", "boolean":
            return true
        default:
            return false
        }
    }
}
