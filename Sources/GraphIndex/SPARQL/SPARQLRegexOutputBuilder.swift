/// Materializes only the final bounded REPLACE result.
struct SPARQLRegexOutputBuilder {
    private var storage = ""
    private var utf8Count = 0
    private let byteLimit: Int

    init(capacity: Int, byteLimit: Int) {
        self.byteLimit = byteLimit
        storage.reserveCapacity(capacity)
    }

    mutating func append(_ source: Substring) throws(SPARQLRegularExpression.Error) {
        let sourceByteCount = source.utf8.count
        utf8Count = try SPARQLRegularExpression.checkedAdd(
            utf8Count,
            sourceByteCount,
            name: "outputUTF8Bytes",
            limit: byteLimit
        )
        storage.append(contentsOf: source)
    }

    consuming func finish() -> String {
        storage
    }
}
