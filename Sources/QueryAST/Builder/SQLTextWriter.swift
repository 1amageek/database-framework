import DatabaseKit

/// Invocation-local text ownership. Every growth is admitted before mutation.
struct SQLTextWriter {
    let limits: QueryStructuralLimits
    private(set) var text = ""
    private(set) var byteCount: UInt64 = 0
    var nodes: UInt64 = 0
    var collectionElements: UInt64 = 0
    var retainedValueBytes: UInt64 = 0
    var depth: UInt64 = 0

    mutating func admit(_ count: UInt64) throws(SQLSerializationError) {
        let next = byteCount.addingReportingOverflow(count)
        guard !next.overflow, next.partialValue <= limits.maximumRetainedBytes,
              next.partialValue <= UInt64(Int.max) else {
            throw .outputLimit(actual: next.overflow ? UInt64.max : next.partialValue,
                               maximum: limits.maximumRetainedBytes)
        }
    }

    mutating func append(_ value: String) throws(SQLSerializationError) {
        let count = UInt64(value.utf8.count)
        try admit(count)
        text += value
        byteCount += count
    }

    mutating func repeated(_ character: String, count: Int) throws(SQLSerializationError) {
        guard count >= 0 else { throw .invalidStructure("Negative repetition") }
        let total = UInt64(count).multipliedReportingOverflow(by: UInt64(character.utf8.count))
        guard !total.overflow else { throw .outputLimit(actual: .max, maximum: limits.maximumRetainedBytes) }
        try admit(total.partialValue)
        text += String(repeating: character, count: count)
        byteCount += total.partialValue
    }

    mutating func enter() throws(SQLSerializationError) {
        guard depth < limits.maximumNestingDepth, nodes < limits.maximumTotalNodes else {
            throw .structuralLimit("SQL nesting depth or node budget exceeded")
        }
        depth += 1
        nodes += 1
    }

    mutating func collection(_ count: Int) throws(SQLSerializationError) {
        let total = collectionElements.addingReportingOverflow(UInt64(count))
        guard !total.overflow, total.partialValue <= limits.maximumCollectionElements else {
            throw .structuralLimit("SQL collection budget exceeded")
        }
        collectionElements = total.partialValue
    }

    mutating func identifier(_ value: String) throws(SQLSerializationError) {
        guard !value.isEmpty, !value.unicodeScalars.contains(where: { $0.value == 0 }) else {
            throw .invalidStructure("Empty or NUL SQL identifier")
        }
        try quoted(value, quote: "\"")
    }

    mutating func string(_ value: String) throws(SQLSerializationError) {
        guard !value.unicodeScalars.contains(where: { $0.value == 0 }) else {
            throw .unsupportedLiteral("SQL strings cannot contain U+0000")
        }
        try quoted(value, quote: "'")
    }

    private mutating func quoted(_ value: String, quote: Character) throws(SQLSerializationError) {
        let quoteScalar: UInt32 = quote == "\"" ? 34 : 39
        let escapes = value.unicodeScalars.reduce(into: UInt64(0)) { if $1.value == quoteScalar { $0 += 1 } }
        let total = UInt64(value.utf8.count).addingReportingOverflow(escapes + 2)
        guard !total.overflow else { throw .outputLimit(actual: .max, maximum: limits.maximumRetainedBytes) }
        try admit(total.partialValue)
        text.append(quote)
        for scalar in value.unicodeScalars {
            text.unicodeScalars.append(scalar)
            if scalar.value == quoteScalar { text.append(quote) }
        }
        text.append(quote)
        byteCount += total.partialValue
    }
}
