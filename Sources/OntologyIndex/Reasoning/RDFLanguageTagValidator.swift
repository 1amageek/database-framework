/// Bounded BCP 47 language-tag validation for RDF literal value semantics.
enum RDFLanguageTagValidator {
    enum Validation: Sendable, Equatable {
        case valid
        case invalid
        case subtagLimit(limit: Int, actual: Int)
    }

    static func validate<Source: StringProtocol>(
        _ source: Source,
        maximumSubtags: Int
    ) -> Validation {
        guard !source.isEmpty else { return .invalid }
        var subtagCount = 1
        for byte in source.utf8 where byte == 45 {
            let (next, overflow) = subtagCount.addingReportingOverflow(1)
            let actual = overflow ? Int.max : next
            guard !overflow, actual <= maximumSubtags else {
                return .subtagLimit(
                    limit: maximumSubtags,
                    actual: actual
                )
            }
            subtagCount = actual
        }

        if isGrandfathered(source) { return .valid }
        let subtags = source.split(
            separator: "-",
            omittingEmptySubsequences: false
        )
        guard !subtags.isEmpty,
              !subtags.contains(where: { $0.isEmpty }) else {
            return .invalid
        }
        if isASCIIEqual(subtags[0], "x") {
            return validatePrivateUse(subtags[...]) ? .valid : .invalid
        }

        var index = 0
        let language = subtags[index]
        if language.count >= 2, language.count <= 3, allAlpha(language) {
            index += 1
            var extlangCount = 0
            while index < subtags.count,
                  subtags[index].count == 3,
                  allAlpha(subtags[index]),
                  extlangCount < 3 {
                index += 1
                extlangCount += 1
            }
        } else if language.count == 4, allAlpha(language) {
            index += 1
        } else if language.count >= 5,
                  language.count <= 8,
                  allAlpha(language) {
            index += 1
        } else {
            return .invalid
        }

        if index < subtags.count,
           subtags[index].count == 4,
           allAlpha(subtags[index]) {
            index += 1
        }
        if index < subtags.count,
           isRegion(subtags[index]) {
            index += 1
        }

        var variants: [Source.SubSequence] = []
        while index < subtags.count, isVariant(subtags[index]) {
            for variant in variants where
                asciiCaseInsensitiveEqual(variant, subtags[index]) {
                return .invalid
            }
            variants.append(subtags[index])
            index += 1
        }

        var singletonMask: UInt64 = 0
        while index < subtags.count,
              let singleton = extensionSingleton(subtags[index]) {
            let bit = UInt64(1) << singleton
            guard singletonMask & bit == 0 else { return .invalid }
            singletonMask |= bit
            index += 1
            let extensionStart = index
            while index < subtags.count,
                  subtags[index].count >= 2,
                  subtags[index].count <= 8,
                  allAlphanumeric(subtags[index]) {
                index += 1
            }
            guard index > extensionStart else { return .invalid }
        }

        if index < subtags.count, isASCIIEqual(subtags[index], "x") {
            guard validatePrivateUse(subtags[index...]) else {
                return .invalid
            }
            index = subtags.count
        }
        return index == subtags.count ? .valid : .invalid
    }

    private static func validatePrivateUse<Subtags: Collection>(
        _ subtags: Subtags
    ) -> Bool where Subtags.Element: StringProtocol {
        guard let first = subtags.first,
              isASCIIEqual(first, "x"),
              subtags.count >= 2 else {
            return false
        }
        for subtag in subtags.dropFirst() {
            guard subtag.count >= 1, subtag.count <= 8,
                  allAlphanumeric(subtag) else {
                return false
            }
        }
        return true
    }

    private static func isRegion<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        (source.count == 2 && allAlpha(source))
            || (source.count == 3 && allDigit(source))
    }

    private static func isVariant<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        if source.count >= 5, source.count <= 8 {
            return allAlphanumeric(source)
        }
        guard source.count == 4,
              let first = source.utf8.first,
              isDigit(first) else {
            return false
        }
        return allAlphanumeric(source)
    }

    private static func extensionSingleton<Source: StringProtocol>(
        _ source: Source
    ) -> UInt64? {
        guard source.utf8.count == 1,
              let byte = source.utf8.first else {
            return nil
        }
        let folded = fold(byte)
        guard isAlpha(folded) || isDigit(folded), folded != 120 else {
            return nil
        }
        if isDigit(folded) { return UInt64(folded - 48) }
        return UInt64(folded - 97 + 10)
    }

    private static func isGrandfathered<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        for tag in grandfathered where asciiCaseInsensitiveEqual(source, tag) {
            return true
        }
        return false
    }

    private static func allAlpha<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        source.utf8.allSatisfy(isAlpha)
    }

    private static func allDigit<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        source.utf8.allSatisfy(isDigit)
    }

    private static func allAlphanumeric<Source: StringProtocol>(
        _ source: Source
    ) -> Bool {
        source.utf8.allSatisfy { isAlpha($0) || isDigit($0) }
    }

    private static func isASCIIEqual<Source: StringProtocol>(
        _ source: Source,
        _ ascii: String
    ) -> Bool {
        asciiCaseInsensitiveEqual(source, ascii)
    }

    private static func asciiCaseInsensitiveEqual<
        Left: StringProtocol,
        Right: StringProtocol
    >(
        _ lhs: Left,
        _ rhs: Right
    ) -> Bool {
        var left = lhs.utf8.makeIterator()
        var right = rhs.utf8.makeIterator()
        while true {
            switch (left.next(), right.next()) {
            case (nil, nil): return true
            case (.some(let lhs), .some(let rhs)):
                guard fold(lhs) == fold(rhs) else { return false }
            default: return false
            }
        }
    }

    private static func fold(_ byte: UInt8) -> UInt8 {
        byte >= 65 && byte <= 90 ? byte + 32 : byte
    }

    private static func isAlpha(_ byte: UInt8) -> Bool {
        let folded = fold(byte)
        return folded >= 97 && folded <= 122
    }

    private static func isDigit(_ byte: UInt8) -> Bool {
        byte >= 48 && byte <= 57
    }

    private static let grandfathered = [
        "art-lojban", "cel-gaulish", "en-GB-oed", "i-ami", "i-bnn",
        "i-default", "i-enochian", "i-hak", "i-klingon", "i-lux",
        "i-mingo", "i-navajo", "i-pwn", "i-tao", "i-tay", "i-tsu",
        "no-bok", "no-nyn", "sgn-BE-FR", "sgn-BE-NL", "sgn-CH-DE",
        "zh-guoyu", "zh-hakka", "zh-min", "zh-min-nan", "zh-xiang",
    ]
}
