/// Compiled OWL 2 basic language range used by `rdf:langRange`.
struct RDFLanguageRange: Sendable {
    private let source: String
    private let wildcard: Bool

    init?(_ source: String) {
        if source == "*" {
            self.source = source
            wildcard = true
            return
        }

        var segmentLength = 0
        var firstSegment = true
        for byte in source.utf8 {
            if byte == 45 {
                guard segmentLength >= 1, segmentLength <= 8 else {
                    return nil
                }
                firstSegment = false
                segmentLength = 0
                continue
            }
            let isAlpha = (byte >= 65 && byte <= 90)
                || (byte >= 97 && byte <= 122)
            let isDigit = byte >= 48 && byte <= 57
            guard isAlpha || (!firstSegment && isDigit) else {
                return nil
            }
            segmentLength += 1
            guard segmentLength <= 8 else { return nil }
        }
        guard segmentLength >= 1 else { return nil }
        self.source = source
        wildcard = false
    }

    func matches(_ language: Substring?) -> Bool {
        guard let language else { return false }
        if wildcard { return true }

        var rangeIterator = source.utf8.makeIterator()
        var languageIterator = language.utf8.makeIterator()
        while let rangeByte = rangeIterator.next() {
            guard let languageByte = languageIterator.next(),
                  Self.foldedASCII(rangeByte)
                    == Self.foldedASCII(languageByte) else {
                return false
            }
        }
        guard let nextLanguageByte = languageIterator.next() else {
            return true
        }
        return nextLanguageByte == 45
    }

    private static func foldedASCII(_ byte: UInt8) -> UInt8 {
        byte >= 65 && byte <= 90 ? byte + 32 : byte
    }
}
