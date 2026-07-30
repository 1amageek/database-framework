/// Parsed `$1`...`$9` replacement template backed by source ranges.
struct SPARQLRegexReplacementTemplate {
    private enum Token {
        case literal(Range<String.Index>)
        case capture(Int)
    }

    private let source: String
    private let tokens: [Token]

    init(
        _ source: String,
        captureGroupCount: Int,
        tokenLimit: Int,
        byteLimit: Int
    ) throws(SPARQLRegularExpression.Error) {
        _ = try SPARQLRegularExpression.checkedUTF8ByteCount(
            source,
            name: "replacementUTF8Bytes",
            limit: byteLimit
        )
        self.source = source

        var parsedTokens: [Token] = []
        parsedTokens.reserveCapacity(min(source.unicodeScalars.count, tokenLimit))
        var index = source.unicodeScalars.startIndex
        var literalStart = index
        var scalarOffset = 0

        while index != source.unicodeScalars.endIndex {
            let scalar = source.unicodeScalars[index]
            guard scalar.value == 0x5C || scalar.value == 0x24 else {
                source.unicodeScalars.formIndex(after: &index)
                scalarOffset += 1
                continue
            }

            if literalStart != index {
                try Self.append(
                    .literal(literalStart..<index),
                    to: &parsedTokens,
                    limit: tokenLimit
                )
            }

            let markerOffset = scalarOffset
            source.unicodeScalars.formIndex(after: &index)
            scalarOffset += 1
            guard index != source.unicodeScalars.endIndex else {
                throw SPARQLRegularExpression.Error.invalidReplacement(
                    offset: markerOffset,
                    reason: scalar.value == 0x5C
                        ? "trailing escape"
                        : "missing capture number"
                )
            }

            let next = source.unicodeScalars[index]
            let nextEnd = source.unicodeScalars.index(after: index)
            if scalar.value == 0x5C {
                guard next.value == 0x5C || next.value == 0x24 else {
                    throw SPARQLRegularExpression.Error.invalidReplacement(
                        offset: markerOffset,
                        reason: "only '\\\\' and '\\$' may be escaped"
                    )
                }
                try Self.append(
                    .literal(index..<nextEnd),
                    to: &parsedTokens,
                    limit: tokenLimit
                )
            } else {
                guard next.value >= 0x31 && next.value <= 0x39 else {
                    throw SPARQLRegularExpression.Error.invalidReplacement(
                        offset: markerOffset,
                        reason: "capture reference must be $1 through $9"
                    )
                }
                let group = Int(next.value - 0x30)
                guard group <= captureGroupCount else {
                    throw SPARQLRegularExpression.Error.invalidReplacement(
                        offset: markerOffset,
                        reason: "capture group $\(group) does not exist"
                    )
                }
                try Self.append(
                    .capture(group),
                    to: &parsedTokens,
                    limit: tokenLimit
                )
            }

            index = nextEnd
            scalarOffset += 1
            literalStart = index
        }

        if literalStart != source.unicodeScalars.endIndex {
            try Self.append(
                .literal(literalStart..<source.unicodeScalars.endIndex),
                to: &parsedTokens,
                limit: tokenLimit
            )
        }
        tokens = parsedTokens
    }

    func append(
        match: SPARQLRegexNFA.Match,
        input: String,
        to output: inout SPARQLRegexOutputBuilder
    ) throws(SPARQLRegularExpression.Error) {
        for token in tokens {
            switch token {
            case .literal(let range):
                try output.append(source[range])
            case .capture(let group):
                if let range = match.captureRange(group) {
                    try output.append(input[range])
                }
            }
        }
    }

    private static func append(
        _ token: Token,
        to tokens: inout [Token],
        limit: Int
    ) throws(SPARQLRegularExpression.Error) {
        _ = try SPARQLRegularExpression.checkedIncrement(
            tokens.count,
            name: "replacementTokens",
            limit: limit
        )
        tokens.append(token)
    }
}
