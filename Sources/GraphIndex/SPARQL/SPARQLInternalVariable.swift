/// Names variables that participate in algebra evaluation but can never be
/// referenced or projected by SPARQL source text.
enum SPARQLInternalVariable {
    private static let rawPrefix = "\u{0}database-framework:"
    private static let prefix = "?\(rawPrefix)"

    static func blankNode(label: String, scope: UInt64) -> String {
        "\(prefix)blank:\(scope):\(label)"
    }

    static func aggregateRaw(_ identifier: UInt64) -> String {
        "\(rawPrefix)aggregate:\(identifier)"
    }

    static func groupKeyRaw(_ identifier: UInt64) -> String {
        "\(rawPrefix)group-key:\(identifier)"
    }

    static func executionName(forRawName name: String) -> String {
        "?\(name)"
    }

    static func isInternal(_ variable: String) -> Bool {
        variable.hasPrefix(prefix)
    }
}
