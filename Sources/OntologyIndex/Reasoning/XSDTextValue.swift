/// A zero-copy view of an XSD/RDF textual data value.
///
/// `Substring` retains the original literal storage. Plain-literal parsing can
/// therefore expose its string and language components without materializing
/// either component into another `String`.
package struct XSDTextValue: Sendable {
    package let kind: XSDDatatypeKind
    package let value: Substring
    package let language: Substring?

    package init(
        kind: XSDDatatypeKind,
        value: String,
        language: String? = nil
    ) {
        self.kind = kind
        self.value = value[...]
        self.language = language.map { $0[...] }
    }

    package init?(plainLiteral lexicalForm: String) {
        guard let separator = lexicalForm.lastIndex(of: "@") else {
            return nil
        }
        let stringPart = lexicalForm[..<separator]
        let languageStart = lexicalForm.index(after: separator)
        let languagePart = lexicalForm[languageStart...]
        guard languagePart.isEmpty
                || XSDUnicodeRules.isLanguage(languagePart) else {
            return nil
        }
        kind = .rdfPlainLiteral
        value = stringPart
        language = languagePart.isEmpty ? nil : languagePart
    }

    package var length: Int {
        value.unicodeScalars.count
    }

    package func isIdentical(to other: XSDTextValue) -> Bool {
        guard Self.exactlyEqual(value, other.value) else { return false }
        switch (language, other.language) {
        case (nil, nil):
            return true
        case (.some(let lhs), .some(let rhs)):
            return Self.asciiCaseInsensitiveEqual(lhs, rhs)
        default:
            return false
        }
    }

    private static func exactlyEqual(
        _ lhs: Substring,
        _ rhs: Substring
    ) -> Bool {
        lhs.utf8.elementsEqual(rhs.utf8)
    }

    private static func asciiCaseInsensitiveEqual(
        _ lhs: Substring,
        _ rhs: Substring
    ) -> Bool {
        var left = lhs.utf8.makeIterator()
        var right = rhs.utf8.makeIterator()
        while true {
            switch (left.next(), right.next()) {
            case (nil, nil):
                return true
            case (.some(let lhsByte), .some(let rhsByte)):
                guard foldedASCII(lhsByte) == foldedASCII(rhsByte) else {
                    return false
                }
            default:
                return false
            }
        }
    }

    private static func foldedASCII(_ byte: UInt8) -> UInt8 {
        byte >= 65 && byte <= 90 ? byte + 32 : byte
    }
}
