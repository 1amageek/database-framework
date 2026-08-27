import DatabaseKit
import DatabaseTypes

struct RDFTermStorageValidator {
    let bytes: UnsafeRawBufferPointer
    let limits: RDFTermStorageLimits
    private(set) var offset = 0
    private(set) var objectCount = 0
    private(set) var maximumDepth = 0
    private(set) var literalCount = 0
    private(set) var stringCount = 0
    private(set) var decodedStringByteCount = 0
    private(set) var blankNodeCount = 0
    private(set) var blankNodeStringByteCount = 0

    var isAtEnd: Bool { offset == bytes.count }

    mutating func validateTerm(
        depth: Int
    ) throws(RDFTermStorageError) -> RDFTermKind {
        try registerTerm(at: depth)
        let kind: RDFTermKind
        switch try readByte() {
        case 1:
            let identifier = try readStringRange()
            let decodedByteCount = try validateUTF8(identifier)
            guard !identifier.isEmpty else {
                throw .invalidBlankNodeIdentifier
            }
            try registerString(
                decodedByteCount: decodedByteCount,
                isBlankNode: true
            )
            kind = .blankNode
        case 2:
            let iri = try readStringRange()
            let decodedByteCount = try validateIRI(iri)
            try registerString(decodedByteCount: decodedByteCount)
            kind = .iri
        case 3:
            try increment(&literalCount)
            try validateLiteral()
            kind = .literal
        case 4:
            let nestedDepth = depth + 1
            let subject = try validateTerm(depth: nestedDepth)
            guard subject == .iri || subject == .blankNode else {
                throw .invalidTripleSubject
            }
            let predicate = try validateTerm(depth: nestedDepth)
            guard predicate == .iri else {
                throw .invalidTriplePredicate
            }
            _ = try validateTerm(depth: nestedDepth)
            kind = .tripleTerm
        case let tag:
            throw .unknownTag(tag)
        }
        return kind
    }

    private mutating func validateLiteral(
    ) throws(RDFTermStorageError) {
        let lexicalForm = try readStringRange()
        let lexicalByteCount = try validateUTF8(lexicalForm)
        try registerString(decodedByteCount: lexicalByteCount)
        switch try readByte() {
        case 1:
            let datatype = try readStringRange()
            let datatypeByteCount: Int
            do {
                datatypeByteCount = try validateIRI(datatype)
                guard !matchesASCII(
                    datatype,
                    RDFIRI.rdfLanguageString.rawValue
                ), !matchesASCII(
                    datatype,
                    RDFIRI.rdfDirectionalLanguageString.rawValue
                ) else {
                    throw RDFTermStorageError.invalidDatatypeIRI
                }
            } catch {
                throw .invalidDatatypeIRI
            }
            try registerString(decodedByteCount: datatypeByteCount)
        case 2:
            let language = try readStringRange()
            let languageByteCount = try validateLanguageTag(language)
            try registerString(decodedByteCount: languageByteCount)
        case 3:
            let language = try readStringRange()
            let languageByteCount = try validateLanguageTag(language)
            try registerString(decodedByteCount: languageByteCount)
            switch try readByte() {
            case 1, 2:
                break
            case let tag:
                throw .invalidDirection(tag)
            }
        case let tag:
            throw .invalidLiteralAnnotation(tag)
        }
    }

    private mutating func readStringRange(
    ) throws(RDFTermStorageError) -> Range<Int> {
        let storedByteCount = try readCount()
        guard storedByteCount > 0 else {
            throw .nonCanonicalStringEncoding
        }
        let encodedByteCount = storedByteCount - 1
        guard encodedByteCount <= bytes.count - offset else {
            throw .truncated
        }
        let range = offset..<(offset + encodedByteCount)
        offset += encodedByteCount
        return range
    }

    private mutating func readCount(
    ) throws(RDFTermStorageError) -> Int {
        var value: UInt64 = 0
        for byteIndex in 0..<10 {
            let byte = try readByte()
            if byteIndex == 9, byte & 0xFE != 0 {
                throw .nonCanonicalVarint
            }
            value |= UInt64(byte & 0x7F) << UInt64(byteIndex * 7)
            if byte & 0x80 == 0 {
                if byteIndex > 0, byte == 0 {
                    throw .nonCanonicalVarint
                }
                guard let count = Int(exactly: value) else {
                    throw .byteCountOverflow
                }
                return count
            }
        }
        throw .nonCanonicalVarint
    }

    private mutating func readByte(
    ) throws(RDFTermStorageError) -> UInt8 {
        guard offset < bytes.count else { throw .truncated }
        let byte = bytes[offset]
        offset += 1
        return byte
    }

    private mutating func registerTerm(
        at depth: Int
    ) throws(RDFTermStorageError) {
        guard depth <= limits.maximumDepth else {
            throw .maximumDepthExceeded(
                actual: depth,
                maximum: limits.maximumDepth
            )
        }
        let (nextCount, overflow) = objectCount.addingReportingOverflow(1)
        guard !overflow else { throw .byteCountOverflow }
        guard nextCount <= limits.maximumObjectCount else {
            throw .maximumObjectCountExceeded(
                actual: nextCount,
                maximum: limits.maximumObjectCount
            )
        }
        objectCount = nextCount
        maximumDepth = max(maximumDepth, depth)
    }

    private mutating func registerString(
        decodedByteCount: Int,
        isBlankNode: Bool = false
    ) throws(RDFTermStorageError) {
        try increment(&stringCount)
        try add(decodedByteCount, to: &decodedStringByteCount)
        if isBlankNode {
            try increment(&blankNodeCount)
            try add(decodedByteCount, to: &blankNodeStringByteCount)
        }
    }

    private func validateUTF8(
        _ range: Range<Int>
    ) throws(RDFTermStorageError) -> Int {
        var count = 0
        var index = range.lowerBound
        while index < range.upperBound {
            let decoded = try readScalar(
                at: index,
                end: range.upperBound
            )
            let increment = decoded.scalar == 0
                ? 1
                : decoded.nextIndex - index
            let (next, overflow) = count.addingReportingOverflow(increment)
            guard !overflow else { throw .byteCountOverflow }
            count = next
            index = decoded.nextIndex
        }
        return count
    }

    private func increment(
        _ value: inout Int
    ) throws(RDFTermStorageError) {
        let (next, overflow) = value.addingReportingOverflow(1)
        guard !overflow else { throw .byteCountOverflow }
        value = next
    }

    private func add(
        _ amount: Int,
        to value: inout Int
    ) throws(RDFTermStorageError) {
        let (next, overflow) = value.addingReportingOverflow(amount)
        guard !overflow else { throw .byteCountOverflow }
        value = next
    }

    private func readScalar(
        at index: Int,
        end: Int
    ) throws(RDFTermStorageError) -> (scalar: UInt32, nextIndex: Int) {
        let first = bytes[index]
        if first == 0 {
            throw .nonCanonicalStringEncoding
        }
        if first < 0x80 {
            return (UInt32(first), index + 1)
        }
        if first == 0xC0 {
            guard index + 1 < end, bytes[index + 1] == 0x80 else {
                throw .invalidUTF8
            }
            return (0, index + 2)
        }
        if first >= 0xC2, first <= 0xDF {
            guard index + 1 < end else { throw .invalidUTF8 }
            let second = bytes[index + 1]
            guard isContinuation(second) else { throw .invalidUTF8 }
            return (
                UInt32(first & 0x1F) << 6 | UInt32(second & 0x3F),
                index + 2
            )
        }
        if first >= 0xE0, first <= 0xEF {
            guard index + 2 < end else { throw .invalidUTF8 }
            let second = bytes[index + 1]
            let third = bytes[index + 2]
            guard isContinuation(second), isContinuation(third) else {
                throw .invalidUTF8
            }
            guard first != 0xE0 || second >= 0xA0,
                  first != 0xED || second < 0xA0 else {
                throw .invalidUTF8
            }
            return (
                UInt32(first & 0x0F) << 12
                    | UInt32(second & 0x3F) << 6
                    | UInt32(third & 0x3F),
                index + 3
            )
        }
        if first >= 0xF0, first <= 0xF4 {
            guard index + 3 < end else { throw .invalidUTF8 }
            let second = bytes[index + 1]
            let third = bytes[index + 2]
            let fourth = bytes[index + 3]
            guard isContinuation(second), isContinuation(third),
                  isContinuation(fourth) else {
                throw .invalidUTF8
            }
            guard first != 0xF0 || second >= 0x90,
                  first != 0xF4 || second <= 0x8F else {
                throw .invalidUTF8
            }
            return (
                UInt32(first & 0x07) << 18
                    | UInt32(second & 0x3F) << 12
                    | UInt32(third & 0x3F) << 6
                    | UInt32(fourth & 0x3F),
                index + 4
            )
        }
        throw .invalidUTF8
    }

    private func validateIRI(
        _ range: Range<Int>
    ) throws(RDFTermStorageError) -> Int {
        let decodedByteCount = try validateUTF8(range)
        guard let colon = firstIndex(of: 0x3A, in: range) else {
            throw .invalidIRI(.missingScheme)
        }
        try validateIRIScheme(range.lowerBound..<colon, sourceStart: range.lowerBound)

        let hierarchyStart = colon + 1
        let fragmentStart = firstIndex(
            of: 0x23,
            in: hierarchyStart..<range.upperBound
        )
        let beforeFragmentEnd = fragmentStart ?? range.upperBound
        if let fragmentStart {
            try validateIRICharacters(
                (fragmentStart + 1)..<range.upperBound,
                component: .fragment,
                sourceStart: range.lowerBound
            )
        }

        let queryStart = firstIndex(
            of: 0x3F,
            in: hierarchyStart..<beforeFragmentEnd
        )
        let hierarchyEnd = queryStart ?? beforeFragmentEnd
        if let queryStart {
            try validateIRICharacters(
                (queryStart + 1)..<beforeFragmentEnd,
                component: .query,
                sourceStart: range.lowerBound
            )
        }

        let hierarchy = hierarchyStart..<hierarchyEnd
        if hierarchy.count >= 2,
           bytes[hierarchy.lowerBound] == 0x2F,
           bytes[hierarchy.lowerBound + 1] == 0x2F {
            let authorityStart = hierarchy.lowerBound + 2
            let slash = firstIndex(of: 0x2F, in: authorityStart..<hierarchy.upperBound)
            let authorityEnd = slash ?? hierarchy.upperBound
            try validateIRIAuthority(
                authorityStart..<authorityEnd,
                sourceStart: range.lowerBound
            )
            try validateIRICharacters(
                authorityEnd..<hierarchy.upperBound,
                component: .path,
                sourceStart: range.lowerBound
            )
        } else {
            try validateIRICharacters(
                hierarchy,
                component: .path,
                sourceStart: range.lowerBound
            )
        }
        return decodedByteCount
    }

    private func validateIRIScheme(
        _ range: Range<Int>,
        sourceStart: Int
    ) throws(RDFTermStorageError) {
        guard let first = range.first else {
            throw .invalidIRI(.missingScheme)
        }
        guard isASCIIAlpha(bytes[first]) else {
            throw .invalidIRI(.invalidScheme(byteOffset: 0))
        }
        var index = first + 1
        while index < range.upperBound {
            let byte = bytes[index]
            guard isASCIIAlpha(byte) || isASCIIDigit(byte)
                    || byte == 43 || byte == 45 || byte == 46 else {
                throw .invalidIRI(
                    .invalidScheme(byteOffset: index - sourceStart)
                )
            }
            index += 1
        }
    }

    private func validateIRIAuthority(
        _ range: Range<Int>,
        sourceStart: Int
    ) throws(RDFTermStorageError) {
        let at = lastIndex(of: 0x40, in: range)
        let hostPortStart: Int
        if let at {
            try validateIRICharacters(
                range.lowerBound..<at,
                component: .userinfo,
                sourceStart: sourceStart
            )
            hostPortStart = at + 1
        } else {
            hostPortStart = range.lowerBound
        }
        let hostPort = hostPortStart..<range.upperBound

        if hostPort.first.map({ bytes[$0] == 0x5B }) == true {
            guard let closingBracket = firstIndex(of: 0x5D, in: hostPort) else {
                throw .invalidIRI(.invalidAuthority)
            }
            let literal = (hostPort.lowerBound + 1)..<closingBracket
            guard !literal.isEmpty else {
                throw .invalidIRI(.invalidIPLiteral)
            }
            try validateIPLiteral(literal)
            let suffix = (closingBracket + 1)..<hostPort.upperBound
            if !suffix.isEmpty {
                guard bytes[suffix.lowerBound] == 0x3A else {
                    throw .invalidIRI(.invalidAuthority)
                }
                try validatePort((suffix.lowerBound + 1)..<suffix.upperBound)
            }
            return
        }

        let portSeparator = lastIndex(of: 0x3A, in: hostPort)
        let hostEnd = portSeparator ?? hostPort.upperBound
        try validateIRICharacters(
            hostPort.lowerBound..<hostEnd,
            component: .registeredName,
            sourceStart: sourceStart
        )
        if let portSeparator {
            try validatePort((portSeparator + 1)..<hostPort.upperBound)
        }
    }

    private func validatePort(
        _ range: Range<Int>
    ) throws(RDFTermStorageError) {
        var index = range.lowerBound
        while index < range.upperBound {
            guard isASCIIDigit(bytes[index]) else {
                throw .invalidIRI(.invalidAuthority)
            }
            index += 1
        }
    }

    private func validateIRICharacters(
        _ range: Range<Int>,
        component: IRIComponent,
        sourceStart: Int
    ) throws(RDFTermStorageError) {
        var index = range.lowerBound
        while index < range.upperBound {
            let start = index
            let decoded = try readScalar(at: index, end: range.upperBound)
            let scalar = decoded.scalar
            if scalar == 0x25 {
                guard start + 2 < range.upperBound,
                      isASCIIHex(bytes[start + 1]),
                      isASCIIHex(bytes[start + 2]) else {
                    throw .invalidIRI(
                        .invalidPercentEncoding(byteOffset: start - sourceStart)
                    )
                }
                index = start + 3
                continue
            }
            guard component.permits(scalar) else {
                throw .invalidIRI(
                    .invalidCharacter(byteOffset: start - sourceStart)
                )
            }
            index = decoded.nextIndex
        }
    }

    private func validateIPLiteral(
        _ range: Range<Int>
    ) throws(RDFTermStorageError) {
        if let first = range.first, bytes[first] == 86 || bytes[first] == 118 {
            guard validateIPvFuture(range) else {
                throw .invalidIRI(.invalidIPLiteral)
            }
            return
        }
        guard validateIPv6(range) else {
            throw .invalidIRI(.invalidIPLiteral)
        }
    }

    private func validateIPvFuture(_ range: Range<Int>) -> Bool {
        var index = range.lowerBound + 1
        var hexCount = 0
        while index < range.upperBound, isASCIIHex(bytes[index]) {
            hexCount += 1
            index += 1
        }
        guard hexCount > 0, index < range.upperBound, bytes[index] == 46 else {
            return false
        }
        index += 1
        var valueCount = 0
        while index < range.upperBound {
            let byte = bytes[index]
            guard isASCIIUnreserved(byte) || isSubDelimiter(byte)
                    || byte == 58 else {
                return false
            }
            valueCount += 1
            index += 1
        }
        return valueCount > 0
    }

    private func validateIPv6(_ range: Range<Int>) -> Bool {
        var index = range.lowerBound
        while index < range.upperBound {
            let byte = bytes[index]
            guard isASCIIHex(byte) || byte == 58 || byte == 46 else {
                return false
            }
            index += 1
        }

        var compression: (first: Int, after: Int)?
        index = range.lowerBound
        var previousColon: Int?
        while index < range.upperBound {
            if bytes[index] == 58 {
                if let previousColon, previousColon + 1 == index {
                    guard compression == nil else { return false }
                    compression = (previousColon, index + 1)
                }
                previousColon = index
            } else {
                previousColon = nil
            }
            index += 1
        }

        if let compression {
            guard let left = ipv6UnitCount(
                range.lowerBound..<compression.first
            ), let right = ipv6UnitCount(
                compression.after..<range.upperBound
            ), left + right < 8 else {
                return false
            }
            return true
        }
        return ipv6UnitCount(range) == 8
    }

    private func ipv6UnitCount(_ range: Range<Int>) -> Int? {
        if range.isEmpty { return 0 }
        var count = 0
        var segmentStart = range.lowerBound
        while true {
            let separator = firstIndex(
                of: 58,
                in: segmentStart..<range.upperBound
            )
            let segmentEnd = separator ?? range.upperBound
            let segment = segmentStart..<segmentEnd
            guard !segment.isEmpty else { return nil }
            if firstIndex(of: 46, in: segment) != nil {
                guard separator == nil, validateIPv4(segment) else { return nil }
                count += 2
            } else {
                guard segment.count <= 4 else { return nil }
                var index = segment.lowerBound
                while index < segment.upperBound {
                    guard isASCIIHex(bytes[index]) else { return nil }
                    index += 1
                }
                count += 1
            }
            guard count <= 8 else { return nil }
            guard let separator else { return count }
            segmentStart = separator + 1
        }
    }

    private func validateIPv4(_ range: Range<Int>) -> Bool {
        var componentCount = 0
        var componentStart = range.lowerBound
        while true {
            let separator = firstIndex(
                of: 46,
                in: componentStart..<range.upperBound
            )
            let componentEnd = separator ?? range.upperBound
            let component = componentStart..<componentEnd
            guard !component.isEmpty, component.count <= 3,
                  component.count == 1 || bytes[component.lowerBound] != 48 else {
                return false
            }
            var number = 0
            var index = component.lowerBound
            while index < component.upperBound {
                let byte = bytes[index]
                guard isASCIIDigit(byte) else { return false }
                number = number * 10 + Int(byte - 48)
                index += 1
            }
            guard number <= 255 else { return false }
            componentCount += 1
            guard let separator else { return componentCount == 4 }
            componentStart = separator + 1
        }
    }

    private func validateLanguageTag(
        _ range: Range<Int>
    ) throws(RDFTermStorageError) -> Int {
        let decodedByteCount = try validateUTF8(range)
        guard RDFLanguageTag.isValid(utf8: bytes, in: range) else {
            throw .invalidLanguageTag
        }
        var index = range.lowerBound
        while index < range.upperBound {
            let byte = bytes[index]
            guard byte < 0x80 else { throw .invalidLanguageTag }
            if byte >= 65, byte <= 90 {
                throw .nonCanonicalLanguageTag
            }
            index += 1
        }
        return decodedByteCount
    }

    private func matchesASCII(
        _ range: Range<Int>,
        _ value: String
    ) -> Bool {
        guard range.count == value.utf8.count else { return false }
        var index = range.lowerBound
        for byte in value.utf8 {
            guard bytes[index] == byte else { return false }
            index += 1
        }
        return true
    }

    private func firstIndex(of byte: UInt8, in range: Range<Int>) -> Int? {
        var index = range.lowerBound
        while index < range.upperBound {
            if bytes[index] == byte { return index }
            index += 1
        }
        return nil
    }

    private func lastIndex(of byte: UInt8, in range: Range<Int>) -> Int? {
        var index = range.upperBound
        while index > range.lowerBound {
            index -= 1
            if bytes[index] == byte { return index }
        }
        return nil
    }

    private func isContinuation(_ byte: UInt8) -> Bool {
        byte >= 0x80 && byte <= 0xBF
    }

    private func isASCIIAlpha(_ byte: UInt8) -> Bool {
        (byte >= 65 && byte <= 90) || (byte >= 97 && byte <= 122)
    }

    private func isASCIIDigit(_ byte: UInt8) -> Bool {
        byte >= 48 && byte <= 57
    }

    private func isASCIIHex(_ byte: UInt8) -> Bool {
        isASCIIDigit(byte) || (byte >= 65 && byte <= 70)
            || (byte >= 97 && byte <= 102)
    }

    private func isASCIIUnreserved(_ byte: UInt8) -> Bool {
        isASCIIAlpha(byte) || isASCIIDigit(byte)
            || byte == 45 || byte == 46 || byte == 95 || byte == 126
    }

    private func isSubDelimiter(_ byte: UInt8) -> Bool {
        switch byte {
        case 33, 36, 38, 39, 40, 41, 42, 43, 44, 59, 61:
            return true
        default:
            return false
        }
    }

    private func isIUnreserved(_ scalar: UInt32) -> Bool {
        if scalar <= 0x7F {
            return isASCIIUnreserved(UInt8(scalar))
        }
        switch scalar {
        case 0xA0...0xD7FF, 0xF900...0xFDCF, 0xFDF0...0xFFEF,
             0x10000...0x1FFFD, 0x20000...0x2FFFD,
             0x30000...0x3FFFD, 0x40000...0x4FFFD,
             0x50000...0x5FFFD, 0x60000...0x6FFFD,
             0x70000...0x7FFFD, 0x80000...0x8FFFD,
             0x90000...0x9FFFD, 0xA0000...0xAFFFD,
             0xB0000...0xBFFFD, 0xC0000...0xCFFFD,
             0xD0000...0xDFFFD, 0xE1000...0xEFFFD:
            return true
        default:
            return false
        }
    }

    private func isIPrivate(_ scalar: UInt32) -> Bool {
        switch scalar {
        case 0xE000...0xF8FF, 0xF0000...0xFFFFD,
             0x100000...0x10FFFD:
            return true
        default:
            return false
        }
    }

    private enum IRIComponent {
        case userinfo
        case registeredName
        case path
        case query
        case fragment

        func permits(_ scalar: UInt32) -> Bool {
            if scalar <= 0x7F {
                let byte = UInt8(scalar)
                if (byte >= 65 && byte <= 90)
                    || (byte >= 97 && byte <= 122)
                    || (byte >= 48 && byte <= 57)
                    || byte == 45 || byte == 46 || byte == 95 || byte == 126
                    || Self.isSubDelimiter(byte) {
                    return true
                }
            } else if Self.isIUnreserved(scalar) {
                return true
            }
            switch self {
            case .userinfo:
                return scalar == 0x3A
            case .registeredName:
                return false
            case .path:
                return scalar == 0x3A || scalar == 0x40 || scalar == 0x2F
            case .query:
                return scalar == 0x3A || scalar == 0x40 || scalar == 0x2F
                    || scalar == 0x3F || Self.isIPrivate(scalar)
            case .fragment:
                return scalar == 0x3A || scalar == 0x40 || scalar == 0x2F
                    || scalar == 0x3F
            }
        }

        private static func isSubDelimiter(_ byte: UInt8) -> Bool {
            switch byte {
            case 33, 36, 38, 39, 40, 41, 42, 43, 44, 59, 61:
                return true
            default:
                return false
            }
        }

        private static func isIUnreserved(_ scalar: UInt32) -> Bool {
            switch scalar {
            case 0xA0...0xD7FF, 0xF900...0xFDCF, 0xFDF0...0xFFEF,
                 0x10000...0x1FFFD, 0x20000...0x2FFFD,
                 0x30000...0x3FFFD, 0x40000...0x4FFFD,
                 0x50000...0x5FFFD, 0x60000...0x6FFFD,
                 0x70000...0x7FFFD, 0x80000...0x8FFFD,
                 0x90000...0x9FFFD, 0xA0000...0xAFFFD,
                 0xB0000...0xBFFFD, 0xC0000...0xCFFFD,
                 0xD0000...0xDFFFD, 0xE1000...0xEFFFD:
                return true
            default:
                return false
            }
        }

        private static func isIPrivate(_ scalar: UInt32) -> Bool {
            switch scalar {
            case 0xE000...0xF8FF, 0xF0000...0xFFFFD,
                 0x100000...0x10FFFD:
                return true
            default:
                return false
            }
        }
    }
}
