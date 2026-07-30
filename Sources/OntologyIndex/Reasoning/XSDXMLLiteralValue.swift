/// Bounded, Foundation-free representation of an `rdf:XMLLiteral` value.
///
/// Token payloads are `Substring` views into the owned lexical `String`.
/// Parsing validates XML 1.0 content and namespace well-formedness without
/// copying element names, attribute values, or character data. Equality follows
/// normalized DOM node equality, including unordered attributes.
package struct XSDXMLLiteralValue: Sendable {
    package enum ParseFailure: Error, Sendable, Equatable {
        case invalid(reason: String)
        case resource(name: String, limit: Int, actual: Int)
    }

    package enum ComparisonFailure: Error, Sendable, Equatable {
        case workLimit(limit: Int, actual: Int)
    }

    private let nodes: [Node]
    private let comparisonWorkLimit: Int

    package init(
        lexicalForm: String,
        limits: XSDValidationLimits
    ) throws(ParseFailure) {
        var parser = Parser(source: lexicalForm, limits: limits)
        nodes = try parser.parse()
        comparisonWorkLimit = limits.maxXMLComparisonWork
    }

    package func isIdentical(
        to other: XSDXMLLiteralValue
    ) -> Result<Bool, ComparisonFailure> {
        var budget = ComparisonBudget(
            limit: min(comparisonWorkLimit, other.comparisonWorkLimit)
        )
        do {
            guard nodes.count == other.nodes.count else {
                return .success(false)
            }
            for index in nodes.indices {
                guard try nodes[index].isEqual(
                    to: other.nodes[index],
                    budget: &budget
                ) else {
                    return .success(false)
                }
            }
            return .success(true)
        } catch let failure as ComparisonFailure {
            return .failure(failure)
        } catch {
            preconditionFailure(
                "Validated XML token comparison failed unexpectedly"
            )
        }
    }

    private enum Node: Sendable {
        case startElement(Element)
        case endElement
        case text(TextView)
        case cdata(TextView)
        case comment(TextView)
        case processingInstruction(target: Substring, data: TextView)

        func isEqual(
            to other: Node,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            try budget.consume(1)
            switch (self, other) {
            case (.startElement(let lhs), .startElement(let rhs)):
                return try lhs.isEqual(to: rhs, budget: &budget)
            case (.endElement, .endElement):
                return true
            case (.text(let lhs), .text(let rhs)),
                 (.cdata(let lhs), .cdata(let rhs)),
                 (.comment(let lhs), .comment(let rhs)):
                return try lhs.isEqual(to: rhs, budget: &budget)
            case (
                .processingInstruction(let lhsTarget, let lhsData),
                .processingInstruction(let rhsTarget, let rhsData)
            ):
                return try TextView.exactlyEqual(
                    lhsTarget,
                    rhsTarget,
                    budget: &budget
                ) && lhsData.isEqual(to: rhsData, budget: &budget)
            default:
                return false
            }
        }
    }

    private struct Element: Sendable {
        let name: ResolvedName
        let attributes: [Attribute]

        func isEqual(
            to other: Element,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            guard try name.isEqual(
                to: other.name,
                comparesPrefix: true,
                budget: &budget
            ), attributes.count == other.attributes.count else {
                return false
            }
            for attribute in attributes {
                var found = false
                for candidate in other.attributes {
                    try budget.consume(1)
                    if try attribute.isEqual(
                        to: candidate,
                        budget: &budget
                    ) {
                        found = true
                        break
                    }
                }
                guard found else { return false }
            }
            return true
        }
    }

    private struct Attribute: Sendable {
        let name: ResolvedName
        let value: TextView

        func isEqual(
            to other: Attribute,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            try name.isEqual(
                to: other.name,
                comparesPrefix: false,
                budget: &budget
            ) && value.isEqual(to: other.value, budget: &budget)
        }
    }

    private struct ResolvedName: Sendable {
        let namespace: TextView?
        let prefix: Substring?
        let localName: Substring

        func isEqual(
            to other: ResolvedName,
            comparesPrefix: Bool,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            guard try TextView.optionalEqual(
                namespace,
                other.namespace,
                budget: &budget
            ), try TextView.exactlyEqual(
                localName,
                other.localName,
                budget: &budget
            ) else {
                return false
            }
            guard comparesPrefix else { return true }
            return try TextView.optionalExactEqual(
                prefix,
                other.prefix,
                budget: &budget
            )
        }
    }

    private struct QualifiedName: Sendable {
        let prefix: Substring?
        let localName: Substring

        func isLexicallyEqual(to other: QualifiedName) -> Bool {
            TextView.optionalExactlyEqual(prefix, other.prefix)
                && TextView.exactlyEqual(localName, other.localName)
        }
    }

    private struct TextView: Sendable {
        enum Mode: Sendable, Equatable {
            case characterData
            case attribute
            case raw
        }

        let source: Substring
        let mode: Mode

        func validate() throws(ParseFailure) {
            var decoder = Decoder(source: source, mode: mode)
            while try decoder.next() != nil {}
        }

        func isEqual(
            to other: TextView,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            var lhs = Decoder(source: source, mode: mode)
            var rhs = Decoder(source: other.source, mode: other.mode)
            while true {
                let lhsScalar = try lhs.next()
                let rhsScalar = try rhs.next()
                try budget.consume(1)
                switch (lhsScalar, rhsScalar) {
                case (nil, nil):
                    return true
                case (.some(let left), .some(let right)) where left == right:
                    continue
                default:
                    return false
                }
            }
        }

        func isEqual(to constant: String) throws(ParseFailure) -> Bool {
            var lhs = Decoder(source: source, mode: mode)
            var rhs = constant.unicodeScalars.makeIterator()
            while true {
                switch (try lhs.next(), rhs.next()) {
                case (nil, nil): return true
                case (.some(let left), .some(let right)) where left == right:
                    continue
                default: return false
                }
            }
        }

        func isEqualForParsing(
            to other: TextView
        ) throws(ParseFailure) -> Bool {
            var lhs = Decoder(source: source, mode: mode)
            var rhs = Decoder(source: other.source, mode: other.mode)
            while true {
                switch (try lhs.next(), try rhs.next()) {
                case (nil, nil): return true
                case (.some(let left), .some(let right)) where left == right:
                    continue
                default: return false
                }
            }
        }

        func isEmpty() throws(ParseFailure) -> Bool {
            var decoder = Decoder(source: source, mode: mode)
            return try decoder.next() == nil
        }

        static func optionalEqual(
            _ lhs: TextView?,
            _ rhs: TextView?,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            switch (lhs, rhs) {
            case (nil, nil): return true
            case (.some(let lhs), .some(let rhs)):
                return try lhs.isEqual(to: rhs, budget: &budget)
            default: return false
            }
        }

        static func optionalExactEqual(
            _ lhs: Substring?,
            _ rhs: Substring?,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            switch (lhs, rhs) {
            case (nil, nil): return true
            case (.some(let lhs), .some(let rhs)):
                return try exactlyEqual(lhs, rhs, budget: &budget)
            default: return false
            }
        }

        static func optionalExactlyEqual(
            _ lhs: Substring?,
            _ rhs: Substring?
        ) -> Bool {
            switch (lhs, rhs) {
            case (nil, nil): return true
            case (.some(let lhs), .some(let rhs)):
                return exactlyEqual(lhs, rhs)
            default: return false
            }
        }

        static func exactlyEqual(
            _ lhs: Substring,
            _ rhs: Substring
        ) -> Bool {
            lhs.utf8.elementsEqual(rhs.utf8)
        }

        static func exactlyEqual(
            _ lhs: Substring,
            _ rhs: Substring,
            budget: inout ComparisonBudget
        ) throws -> Bool {
            var left = lhs.unicodeScalars.makeIterator()
            var right = rhs.unicodeScalars.makeIterator()
            while true {
                try budget.consume(1)
                switch (left.next(), right.next()) {
                case (nil, nil): return true
                case (.some(let lhs), .some(let rhs)) where lhs == rhs:
                    continue
                default: return false
                }
            }
        }

        private struct Decoder {
            private let scalars: Substring.UnicodeScalarView
            private let mode: Mode
            private var index: Substring.UnicodeScalarView.Index

            init(source: Substring, mode: Mode) {
                scalars = source.unicodeScalars
                self.mode = mode
                index = scalars.startIndex
            }

            mutating func next() throws(ParseFailure) -> Unicode.Scalar? {
                guard index != scalars.endIndex else { return nil }
                let scalar = scalars[index]
                scalars.formIndex(after: &index)

                if scalar.value == 0x26, mode != .raw {
                    return try decodeReference()
                }
                if scalar.value == 0x0D {
                    if index != scalars.endIndex,
                       scalars[index].value == 0x0A {
                        scalars.formIndex(after: &index)
                    }
                    return mode == .attribute ? " " : "\n"
                }
                if mode == .attribute,
                   scalar.value == 0x09 || scalar.value == 0x0A {
                    return " "
                }
                return scalar
            }

            private mutating func decodeReference() throws(ParseFailure) -> Unicode.Scalar {
                guard index != scalars.endIndex else {
                    throw ParseFailure.invalid(reason: "unterminated XML reference")
                }
                if scalars[index].value == 0x23 {
                    scalars.formIndex(after: &index)
                    return try decodeCharacterReference()
                }

                let start = index
                while index != scalars.endIndex,
                      scalars[index].value != 0x3B {
                    scalars.formIndex(after: &index)
                }
                guard index != scalars.endIndex else {
                    throw ParseFailure.invalid(reason: "unterminated XML entity reference")
                }
                let name = scalars[start..<index]
                scalars.formIndex(after: &index)
                if name.elementsEqual("amp".unicodeScalars) { return "&" }
                if name.elementsEqual("lt".unicodeScalars) { return "<" }
                if name.elementsEqual("gt".unicodeScalars) { return ">" }
                if name.elementsEqual("apos".unicodeScalars) { return "'" }
                if name.elementsEqual("quot".unicodeScalars) { return "\"" }
                throw ParseFailure.invalid(
                    reason: "undeclared XML entity reference"
                )
            }

            private mutating func decodeCharacterReference() throws(ParseFailure) -> Unicode.Scalar {
                var radix: UInt32 = 10
                if index != scalars.endIndex,
                   scalars[index].value == 0x78 {
                    radix = 16
                    scalars.formIndex(after: &index)
                }
                var value: UInt32 = 0
                var digitCount = 0
                while index != scalars.endIndex,
                      scalars[index].value != 0x3B {
                    let scalar = scalars[index].value
                    let digit: UInt32
                    switch scalar {
                    case 0x30...0x39: digit = scalar - 0x30
                    case 0x41...0x46 where radix == 16: digit = scalar - 0x41 + 10
                    case 0x61...0x66 where radix == 16: digit = scalar - 0x61 + 10
                    default:
                        throw ParseFailure.invalid(
                            reason: "invalid XML character reference"
                        )
                    }
                    guard digit < radix,
                          value <= (UInt32.max - digit) / radix else {
                        throw ParseFailure.invalid(
                            reason: "overflowing XML character reference"
                        )
                    }
                    value = value * radix + digit
                    digitCount += 1
                    scalars.formIndex(after: &index)
                }
                guard digitCount > 0, index != scalars.endIndex,
                      let result = Unicode.Scalar(value),
                      XSDUnicodeRules.isXMLCharacter(result) else {
                    throw ParseFailure.invalid(
                        reason: "invalid XML character reference"
                    )
                }
                scalars.formIndex(after: &index)
                return result
            }
        }
    }

    private struct RawAttribute: Sendable {
        let name: QualifiedName
        let value: TextView
    }

    private struct NamespaceBinding: Sendable {
        let prefix: Substring?
        let namespace: TextView
    }

    private struct ElementFrame: Sendable {
        let name: QualifiedName
        let namespaceBindingCount: Int
    }

    private struct Parser {
        private static let xmlNamespace = "http://www.w3.org/XML/1998/namespace"
        private static let xmlnsNamespace = "http://www.w3.org/2000/xmlns/"

        let source: String
        let limits: XSDValidationLimits
        var index: String.Index
        var nodes: [Node] = []
        var stack: [ElementFrame] = []
        var namespaceBindings: [NamespaceBinding] = []
        var parsingWork = 0

        init(source: String, limits: XSDValidationLimits) {
            self.source = source
            self.limits = limits
            index = source.startIndex
        }

        mutating func parse() throws(ParseFailure) -> [Node] {
            guard XSDUnicodeRules.allXMLCharacters(source) else {
                throw ParseFailure.invalid(
                    reason: "XML literal contains a character outside XML Char"
                )
            }
            while index != source.endIndex {
                if source[index...].hasPrefix("<!--") {
                    try parseComment()
                } else if source[index...].hasPrefix("<![CDATA[") {
                    try parseCDATA()
                } else if source[index...].hasPrefix("<?") {
                    try parseProcessingInstruction()
                } else if source[index...].hasPrefix("</") {
                    try parseEndElement()
                } else if source[index] == "<" {
                    try parseStartElement()
                } else {
                    try parseCharacterData()
                }
            }
            guard stack.isEmpty else {
                throw ParseFailure.invalid(reason: "unclosed XML element")
            }
            return nodes
        }

        private mutating func parseCharacterData() throws(ParseFailure) {
            let start = index
            while index != source.endIndex, source[index] != "<" {
                if source[index...].hasPrefix("]]>") {
                    throw ParseFailure.invalid(
                        reason: "]]>' is not permitted in XML character data"
                    )
                }
                source.formIndex(after: &index)
            }
            let view = TextView(
                source: source[start..<index],
                mode: .characterData
            )
            try view.validate()
            if !view.source.isEmpty {
                try append(.text(view))
            }
        }

        private mutating func parseComment() throws(ParseFailure) {
            advanceASCII("<!--")
            let start = index
            while index != source.endIndex,
                  !source[index...].hasPrefix("-->") {
                if source[index...].hasPrefix("--") {
                    throw ParseFailure.invalid(
                        reason: "XML comment contains --"
                    )
                }
                source.formIndex(after: &index)
            }
            guard index != source.endIndex else {
                throw ParseFailure.invalid(reason: "unterminated XML comment")
            }
            let view = TextView(source: source[start..<index], mode: .raw)
            try view.validate()
            advanceASCII("-->")
            try append(.comment(view))
        }

        private mutating func parseCDATA() throws(ParseFailure) {
            advanceASCII("<![CDATA[")
            let start = index
            while index != source.endIndex,
                  !source[index...].hasPrefix("]]>") {
                source.formIndex(after: &index)
            }
            guard index != source.endIndex else {
                throw ParseFailure.invalid(reason: "unterminated CDATA section")
            }
            let view = TextView(source: source[start..<index], mode: .raw)
            try view.validate()
            advanceASCII("]]>")
            try append(.cdata(view))
        }

        private mutating func parseProcessingInstruction() throws(ParseFailure) {
            advanceASCII("<?")
            let target = try parseName(allowsColon: true)
            if asciiCaseInsensitiveEqual(target, "xml") {
                throw ParseFailure.invalid(
                    reason: "XML processing-instruction target is reserved"
                )
            }
            let hadWhitespace = skipWhitespace()
            guard hadWhitespace || source[index...].hasPrefix("?>") else {
                throw ParseFailure.invalid(
                    reason: "processing-instruction target must be separated from data"
                )
            }
            let start = index
            while index != source.endIndex,
                  !source[index...].hasPrefix("?>") {
                source.formIndex(after: &index)
            }
            guard index != source.endIndex else {
                throw ParseFailure.invalid(
                    reason: "unterminated processing instruction"
                )
            }
            let data = TextView(source: source[start..<index], mode: .raw)
            try data.validate()
            advanceASCII("?>")
            try append(.processingInstruction(target: target, data: data))
        }

        private mutating func parseStartElement() throws(ParseFailure) {
            advanceASCII("<")
            let name = try parseQualifiedName()
            var rawAttributes: [RawAttribute] = []
            var selfClosing = false

            while true {
                let hadWhitespace = skipWhitespace()
                if source[index...].hasPrefix("/>") {
                    advanceASCII("/>")
                    selfClosing = true
                    break
                }
                if source[index...].hasPrefix(">") {
                    advanceASCII(">")
                    break
                }
                guard hadWhitespace else {
                    throw ParseFailure.invalid(
                        reason: "XML attributes require separating whitespace"
                    )
                }
                guard rawAttributes.count < limits.maxXMLAttributesPerElement else {
                    throw ParseFailure.resource(
                        name: "xmlAttributesPerElement",
                        limit: limits.maxXMLAttributesPerElement,
                        actual: rawAttributes.count + 1
                    )
                }
                let attributeName = try parseQualifiedName()
                for attribute in rawAttributes {
                    if try qualifiedNamesAreEqual(
                        attribute.name,
                        attributeName
                    ) {
                        throw ParseFailure.invalid(
                            reason: "duplicate XML attribute name"
                        )
                    }
                }
                _ = skipWhitespace()
                guard consume("=") else {
                    throw ParseFailure.invalid(reason: "XML attribute requires =")
                }
                _ = skipWhitespace()
                let value = try parseAttributeValue()
                rawAttributes.append(RawAttribute(
                    name: attributeName,
                    value: value
                ))
            }

            guard stack.count + 1 <= limits.maxXMLDepth else {
                throw ParseFailure.resource(
                    name: "xmlDepth",
                    limit: limits.maxXMLDepth,
                    actual: stack.count + 1
                )
            }
            let bindingCount = namespaceBindings.count
            try installNamespaceDeclarations(rawAttributes)
            let elementName = try resolveElementName(name)
            let attributes = try resolveAttributes(rawAttributes)
            try append(.startElement(Element(
                name: elementName,
                attributes: attributes
            )))
            if selfClosing {
                try append(.endElement)
                namespaceBindings.removeLast(
                    namespaceBindings.count - bindingCount
                )
            } else {
                stack.append(ElementFrame(
                    name: name,
                    namespaceBindingCount: bindingCount
                ))
            }
        }

        private mutating func parseEndElement() throws(ParseFailure) {
            advanceASCII("</")
            let name = try parseQualifiedName()
            _ = skipWhitespace()
            guard consume(">"), let frame = stack.last,
                  frame.name.isLexicallyEqual(to: name) else {
                throw ParseFailure.invalid(
                    reason: "XML end tag does not match the open element"
                )
            }
            stack.removeLast()
            namespaceBindings.removeLast(
                namespaceBindings.count - frame.namespaceBindingCount
            )
            try append(.endElement)
        }

        private mutating func parseAttributeValue() throws(ParseFailure) -> TextView {
            guard index != source.endIndex,
                  source[index] == "\"" || source[index] == "'" else {
                throw ParseFailure.invalid(
                    reason: "XML attribute value must be quoted"
                )
            }
            let quote = source[index]
            source.formIndex(after: &index)
            let start = index
            while index != source.endIndex, source[index] != quote {
                guard source[index] != "<" else {
                    throw ParseFailure.invalid(
                        reason: "< is not permitted in an XML attribute value"
                    )
                }
                source.formIndex(after: &index)
            }
            guard index != source.endIndex else {
                throw ParseFailure.invalid(
                    reason: "unterminated XML attribute value"
                )
            }
            let view = TextView(source: source[start..<index], mode: .attribute)
            try view.validate()
            source.formIndex(after: &index)
            return view
        }

        private mutating func installNamespaceDeclarations(
            _ attributes: [RawAttribute]
        ) throws(ParseFailure) {
            let initialBindingCount = namespaceBindings.count
            for attribute in attributes {
                let declarationPrefix: Substring?
                if attribute.name.prefix == nil,
                   TextView.exactlyEqual(attribute.name.localName, "xmlns"[...]) {
                    declarationPrefix = nil
                } else if let prefix = attribute.name.prefix,
                          TextView.exactlyEqual(prefix, "xmlns"[...]) {
                    declarationPrefix = attribute.name.localName
                } else {
                    continue
                }

                for binding in namespaceBindings.suffix(
                    from: initialBindingCount
                ) {
                    if try optionalNamesAreEqual(
                        binding.prefix,
                        declarationPrefix
                    ) {
                        throw ParseFailure.invalid(
                            reason: "duplicate XML namespace declaration"
                        )
                    }
                }
                try validateNamespaceDeclaration(
                    prefix: declarationPrefix,
                    namespace: attribute.value
                )
                guard namespaceBindings.count < limits.maxXMLNamespaceBindings else {
                    throw ParseFailure.resource(
                        name: "xmlNamespaceBindings",
                        limit: limits.maxXMLNamespaceBindings,
                        actual: namespaceBindings.count + 1
                    )
                }
                namespaceBindings.append(NamespaceBinding(
                    prefix: declarationPrefix,
                    namespace: attribute.value
                ))
            }
        }

        private mutating func validateNamespaceDeclaration(
            prefix: Substring?,
            namespace: TextView
        ) throws(ParseFailure) {
            if let prefix,
               TextView.exactlyEqual(prefix, "xmlns"[...]) {
                throw ParseFailure.invalid(
                    reason: "the xmlns prefix cannot be rebound"
                )
            }
            try consumeParsingWork(
                namespace.source.utf8.count + Self.xmlnsNamespace.utf8.count
            )
            if try namespace.isEqual(to: Self.xmlnsNamespace) {
                throw ParseFailure.invalid(
                    reason: "the XMLNS namespace cannot be used as a namespace name"
                )
            }
            let isXMLPrefix = prefix.map {
                TextView.exactlyEqual($0, "xml"[...])
            } ?? false
            try consumeParsingWork(
                namespace.source.utf8.count + Self.xmlNamespace.utf8.count
            )
            let isXMLNamespace = try namespace.isEqual(to: Self.xmlNamespace)
            guard isXMLPrefix == isXMLNamespace else {
                throw ParseFailure.invalid(
                    reason: "the xml prefix must map only to the XML namespace"
                )
            }
            try consumeParsingWork(namespace.source.utf8.count)
            if prefix != nil, try namespace.isEmpty() {
                throw ParseFailure.invalid(
                    reason: "a prefixed XML namespace declaration cannot be empty"
                )
            }
        }

        private mutating func resolveElementName(
            _ name: QualifiedName
        ) throws(ParseFailure) -> ResolvedName {
            let resolvedNamespace: TextView?
            if let prefix = name.prefix {
                resolvedNamespace = try requiredNamespace(for: prefix)
            } else {
                resolvedNamespace = try namespace(for: nil)
            }
            return ResolvedName(
                namespace: resolvedNamespace,
                prefix: name.prefix,
                localName: name.localName
            )
        }

        private mutating func resolveAttributes(
            _ rawAttributes: [RawAttribute]
        ) throws(ParseFailure) -> [Attribute] {
            var result: [Attribute] = []
            result.reserveCapacity(rawAttributes.count)
            for raw in rawAttributes {
                let resolved: ResolvedName
                if raw.name.prefix == nil,
                   TextView.exactlyEqual(raw.name.localName, "xmlns"[...]) {
                    resolved = ResolvedName(
                        namespace: TextView(
                            source: Self.xmlnsNamespace[...],
                            mode: .raw
                        ),
                        prefix: nil,
                        localName: raw.name.localName
                    )
                } else if let prefix = raw.name.prefix,
                          TextView.exactlyEqual(prefix, "xmlns"[...]) {
                    resolved = ResolvedName(
                        namespace: TextView(
                            source: Self.xmlnsNamespace[...],
                            mode: .raw
                        ),
                        prefix: prefix,
                        localName: raw.name.localName
                    )
                } else if let prefix = raw.name.prefix {
                    resolved = ResolvedName(
                        namespace: try requiredNamespace(for: prefix),
                        prefix: prefix,
                        localName: raw.name.localName
                    )
                } else {
                    resolved = ResolvedName(
                        namespace: nil,
                        prefix: nil,
                        localName: raw.name.localName
                    )
                }
                for existing in result {
                    if try expandedNamesAreEqual(existing.name, resolved) {
                        throw ParseFailure.invalid(
                            reason: "duplicate expanded XML attribute name"
                        )
                    }
                }
                result.append(Attribute(name: resolved, value: raw.value))
            }
            return result
        }

        private mutating func requiredNamespace(
            for prefix: Substring
        ) throws(ParseFailure) -> TextView {
            if TextView.exactlyEqual(prefix, "xml"[...]) {
                return TextView(source: Self.xmlNamespace[...], mode: .raw)
            }
            guard let namespace = try namespace(for: prefix) else {
                throw ParseFailure.invalid(
                    reason: "XML namespace prefix is not declared"
                )
            }
            return namespace
        }

        private mutating func namespace(
            for prefix: Substring?
        ) throws(ParseFailure) -> TextView? {
            for binding in namespaceBindings.reversed() {
                if try optionalNamesAreEqual(binding.prefix, prefix) {
                    try consumeParsingWork(
                        binding.namespace.source.utf8.count
                    )
                    if try binding.namespace.isEmpty() {
                        return nil
                    }
                    return binding.namespace
                }
            }
            return nil
        }

        private mutating func qualifiedNamesAreEqual(
            _ lhs: QualifiedName,
            _ rhs: QualifiedName
        ) throws(ParseFailure) -> Bool {
            guard try optionalNamesAreEqual(lhs.prefix, rhs.prefix) else {
                return false
            }
            return try namesAreEqual(lhs.localName, rhs.localName)
        }

        private mutating func expandedNamesAreEqual(
            _ lhs: ResolvedName,
            _ rhs: ResolvedName
        ) throws(ParseFailure) -> Bool {
            guard try namesAreEqual(lhs.localName, rhs.localName) else {
                return false
            }
            switch (lhs.namespace, rhs.namespace) {
            case (nil, nil):
                return true
            case (.some(let lhs), .some(let rhs)):
                try consumeParsingWork(
                    lhs.source.utf8.count + rhs.source.utf8.count
                )
                return try lhs.isEqualForParsing(to: rhs)
            default:
                return false
            }
        }

        private mutating func optionalNamesAreEqual(
            _ lhs: Substring?,
            _ rhs: Substring?
        ) throws(ParseFailure) -> Bool {
            switch (lhs, rhs) {
            case (nil, nil):
                return true
            case (.some(let lhs), .some(let rhs)):
                return try namesAreEqual(lhs, rhs)
            default:
                return false
            }
        }

        private mutating func namesAreEqual(
            _ lhs: Substring,
            _ rhs: Substring
        ) throws(ParseFailure) -> Bool {
            try consumeParsingWork(lhs.utf8.count + rhs.utf8.count)
            return TextView.exactlyEqual(lhs, rhs)
        }

        private mutating func consumeParsingWork(_ amount: Int) throws(ParseFailure) {
            let (actual, overflow) = parsingWork.addingReportingOverflow(amount)
            guard !overflow, actual <= limits.maxXMLParsingWork else {
                throw ParseFailure.resource(
                    name: "xmlParsingWork",
                    limit: limits.maxXMLParsingWork,
                    actual: overflow ? Int.max : actual
                )
            }
            parsingWork = actual
        }

        private mutating func parseQualifiedName() throws(ParseFailure) -> QualifiedName {
            let name = try parseName(allowsColon: true)
            guard let separator = name.firstIndex(of: ":") else {
                return QualifiedName(prefix: nil, localName: name)
            }
            let localStart = name.index(after: separator)
            let prefix = name[..<separator]
            let localName = name[localStart...]
            guard !prefix.isEmpty, !localName.isEmpty,
                  localName.firstIndex(of: ":") == nil,
                  XSDUnicodeRules.isName(prefix, allowsColon: false),
                  XSDUnicodeRules.isName(localName, allowsColon: false) else {
                throw ParseFailure.invalid(reason: "invalid XML qualified name")
            }
            return QualifiedName(prefix: prefix, localName: localName)
        }

        private mutating func parseName(
            allowsColon: Bool
        ) throws(ParseFailure) -> Substring {
            let start = index
            let scalars = source.unicodeScalars
            guard index != source.endIndex,
                  XSDUnicodeRules.isNameStart(scalars[index]),
                  allowsColon || scalars[index].value != 0x3A else {
                throw ParseFailure.invalid(reason: "expected XML name")
            }
            scalars.formIndex(after: &index)
            while index != source.endIndex,
                  XSDUnicodeRules.isNameCharacter(scalars[index]),
                  allowsColon || scalars[index].value != 0x3A {
                scalars.formIndex(after: &index)
            }
            return source[start..<index]
        }

        @discardableResult
        private mutating func skipWhitespace() -> Bool {
            let start = index
            while index != source.endIndex {
                switch source[index] {
                case " ", "\t", "\n", "\r":
                    source.formIndex(after: &index)
                default:
                    return index != start
                }
            }
            return index != start
        }

        private mutating func append(_ node: Node) throws(ParseFailure) {
            guard nodes.count < limits.maxXMLNodes else {
                throw ParseFailure.resource(
                    name: "xmlNodes",
                    limit: limits.maxXMLNodes,
                    actual: nodes.count + 1
                )
            }
            nodes.append(node)
        }

        private mutating func consume(_ token: Character) -> Bool {
            guard index != source.endIndex, source[index] == token else {
                return false
            }
            source.formIndex(after: &index)
            return true
        }

        private mutating func advanceASCII(_ token: String) {
            precondition(source[index...].hasPrefix(token))
            for _ in token {
                source.formIndex(after: &index)
            }
        }

        private func asciiCaseInsensitiveEqual(
            _ lhs: Substring,
            _ rhs: String
        ) -> Bool {
            var left = lhs.utf8.makeIterator()
            var right = rhs.utf8.makeIterator()
            while true {
                switch (left.next(), right.next()) {
                case (nil, nil): return true
                case (.some(let lhs), .some(let rhs)):
                    let foldedLeft = lhs >= 65 && lhs <= 90 ? lhs + 32 : lhs
                    let foldedRight = rhs >= 65 && rhs <= 90 ? rhs + 32 : rhs
                    guard foldedLeft == foldedRight else { return false }
                default: return false
                }
            }
        }
    }

    private struct ComparisonBudget {
        let limit: Int
        var consumed = 0

        mutating func consume(_ amount: Int) throws {
            let (actual, overflow) = consumed.addingReportingOverflow(amount)
            guard !overflow, actual <= limit else {
                throw ComparisonFailure.workLimit(
                    limit: limit,
                    actual: overflow ? Int.max : actual
                )
            }
            consumed = actual
        }
    }
}
