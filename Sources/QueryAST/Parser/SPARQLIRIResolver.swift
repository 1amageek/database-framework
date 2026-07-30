import DatabaseTypes

/// Resolves SPARQL IRI references while preserving their Unicode spelling.
enum SPARQLIRIResolver {
    enum ResolutionError: Error, Equatable {
        case invalidReference
        case invalidBase
    }

    /// Borrowed component views keep parsing allocation-free. Each view retains
    /// the source string storage for exactly as long as this value is alive.
    private struct Components {
        let scheme: Substring?
        let authority: Substring?
        let path: Substring
        let query: Substring?
        let fragment: Substring?

        init(_ value: String) throws {
            let fragmentStart = value.firstIndex(of: "#")
            let beforeFragment = fragmentStart.map { value[..<$0] }
                ?? value[...]
            if let fragmentStart {
                fragment = value[value.index(after: fragmentStart)...]
            } else {
                fragment = nil
            }

            let queryStart = beforeFragment.firstIndex(of: "?")
            let hierarchy = queryStart.map { beforeFragment[..<$0] }
                ?? beforeFragment[...]
            if let queryStart {
                query = beforeFragment[beforeFragment.index(after: queryStart)...]
            } else {
                query = nil
            }

            let parsedScheme = try Self.scheme(in: hierarchy)
            scheme = parsedScheme.map { hierarchy[..<$0] }
            let hierarchyBody: Substring
            if let parsedScheme {
                hierarchyBody = hierarchy[hierarchy.index(after: parsedScheme)...]
            } else {
                hierarchyBody = hierarchy
            }

            if hierarchyBody.hasPrefix("//") {
                let authorityStart = hierarchyBody.index(
                    hierarchyBody.startIndex,
                    offsetBy: 2
                )
                let authorityEnd = hierarchyBody[authorityStart...]
                    .firstIndex(of: "/") ?? hierarchyBody.endIndex
                authority = hierarchyBody[authorityStart..<authorityEnd]
                path = hierarchyBody[authorityEnd...]
            } else {
                authority = nil
                path = hierarchyBody
                if parsedScheme == nil,
                   !path.hasPrefix("/"),
                   path.prefix(while: { $0 != "/" }).contains(":") {
                    throw ResolutionError.invalidReference
                }
            }
        }

        private static func scheme(
            in hierarchy: Substring
        ) throws -> Substring.Index? {
            guard let colon = hierarchy.firstIndex(of: ":") else {
                return nil
            }
            if let slash = hierarchy.firstIndex(of: "/"), slash < colon {
                return nil
            }

            let candidate = hierarchy[..<colon]
            guard let first = candidate.utf8.first,
                  isASCIIAlpha(first) else {
                throw ResolutionError.invalidReference
            }
            for byte in candidate.utf8.dropFirst() {
                guard isASCIIAlpha(byte) || isASCIIDigit(byte)
                        || byte == 0x2B || byte == 0x2D || byte == 0x2E else {
                    throw ResolutionError.invalidReference
                }
            }
            return colon
        }

        private static func isASCIIAlpha(_ byte: UInt8) -> Bool {
            (byte >= 0x41 && byte <= 0x5A)
                || (byte >= 0x61 && byte <= 0x7A)
        }

        private static func isASCIIDigit(_ byte: UInt8) -> Bool {
            byte >= 0x30 && byte <= 0x39
        }
    }

    /// A resolved path remains a view whenever resolution does not change its
    /// structure. Joined views avoid a temporary merged path allocation.
    private enum ResolvedPath {
        case borrowed(Substring)
        case joined(prefix: Substring, suffix: Substring)
        case rooted(Substring)
        case normalized(String)

        var utf8Count: Int {
            switch self {
            case .borrowed(let value), .rooted(let value):
                return value.utf8.count + (isRooted ? 1 : 0)
            case .joined(let prefix, let suffix):
                return prefix.utf8.count + suffix.utf8.count
            case .normalized(let value):
                return value.utf8.count
            }
        }

        func append(to result: inout String) {
            switch self {
            case .borrowed(let value):
                result.append(contentsOf: value)
            case .joined(let prefix, let suffix):
                result.append(contentsOf: prefix)
                result.append(contentsOf: suffix)
            case .rooted(let value):
                result.append("/")
                result.append(contentsOf: value)
            case .normalized(let value):
                result.append(contentsOf: value)
            }
        }

        private var isRooted: Bool {
            if case .rooted = self { return true }
            return false
        }
    }

    static func resolve(
        _ reference: String,
        against base: String?
    ) throws -> String {
        if RDFIRISyntax.isAbsolute(reference) {
            return reference
        }
        guard let base else { return reference }
        guard RDFIRISyntax.isAbsolute(base) else {
            throw ResolutionError.invalidBase
        }

        let baseComponents = try Components(base)
        let referenceComponents = try Components(reference)
        guard let baseScheme = baseComponents.scheme else {
            throw ResolutionError.invalidBase
        }

        let targetScheme: Substring
        let targetAuthority: Substring?
        let targetPath: ResolvedPath
        let targetQuery: Substring?
        let targetFragment = referenceComponents.fragment

        if let referenceScheme = referenceComponents.scheme {
            targetScheme = referenceScheme
            targetAuthority = referenceComponents.authority
            targetPath = normalized(referenceComponents.path)
            targetQuery = referenceComponents.query
        } else if let referenceAuthority = referenceComponents.authority {
            targetScheme = baseScheme
            targetAuthority = referenceAuthority
            targetPath = normalized(referenceComponents.path)
            targetQuery = referenceComponents.query
        } else if referenceComponents.path.isEmpty {
            targetScheme = baseScheme
            targetAuthority = baseComponents.authority
            targetPath = .borrowed(baseComponents.path)
            targetQuery = referenceComponents.query ?? baseComponents.query
        } else {
            targetScheme = baseScheme
            targetAuthority = baseComponents.authority
            if referenceComponents.path.hasPrefix("/") {
                targetPath = normalized(referenceComponents.path)
            } else {
                targetPath = mergeAndNormalize(
                    basePath: baseComponents.path,
                    baseHasAuthority: baseComponents.authority != nil,
                    referencePath: referenceComponents.path
                )
            }
            targetQuery = referenceComponents.query
        }

        let result = try serialize(
            scheme: targetScheme,
            authority: targetAuthority,
            path: targetPath,
            query: targetQuery,
            fragment: targetFragment
        )
        guard RDFIRISyntax.isAbsolute(result) else {
            throw ResolutionError.invalidReference
        }
        return result
    }

    private static func mergeAndNormalize(
        basePath: Substring,
        baseHasAuthority: Bool,
        referencePath: Substring
    ) -> ResolvedPath {
        if baseHasAuthority, basePath.isEmpty {
            guard containsDotSegment(referencePath) else {
                return .rooted(referencePath)
            }
            var merged = "/"
            merged.append(contentsOf: referencePath)
            return .normalized(removeDotSegments(merged[...]))
        }

        guard let lastSlash = basePath.lastIndex(of: "/") else {
            return normalized(referencePath)
        }
        let prefix = basePath[...lastSlash]
        guard containsDotSegment(prefix)
                || containsDotSegment(referencePath) else {
            return .joined(prefix: prefix, suffix: referencePath)
        }

        // Dot-segment removal mutates path structure, so an owned scratch path
        // is required only for this normalization branch.
        var merged = String(prefix)
        merged.append(contentsOf: referencePath)
        return .normalized(removeDotSegments(merged[...]))
    }

    private static func normalized(_ path: Substring) -> ResolvedPath {
        guard containsDotSegment(path) else { return .borrowed(path) }
        return .normalized(removeDotSegments(path))
    }

    private static func containsDotSegment(_ path: Substring) -> Bool {
        path.split(separator: "/", omittingEmptySubsequences: false).contains {
            $0 == "." || $0 == ".."
        }
    }

    private static func serialize(
        scheme: Substring,
        authority: Substring?,
        path: ResolvedPath,
        query: Substring?,
        fragment: Substring?
    ) throws -> String {
        var capacity = scheme.utf8.count
        try addCapacity(1, to: &capacity)
        if let authority {
            try addCapacity(2, to: &capacity)
            try addCapacity(authority.utf8.count, to: &capacity)
        }
        try addCapacity(path.utf8Count, to: &capacity)
        if let query {
            try addCapacity(1, to: &capacity)
            try addCapacity(query.utf8.count, to: &capacity)
        }
        if let fragment {
            try addCapacity(1, to: &capacity)
            try addCapacity(fragment.utf8.count, to: &capacity)
        }

        var result = String()
        result.reserveCapacity(capacity)
        result.append(contentsOf: scheme)
        result.append(":")
        if let authority {
            result.append("//")
            result.append(contentsOf: authority)
        }
        path.append(to: &result)
        if let query {
            result.append("?")
            result.append(contentsOf: query)
        }
        if let fragment {
            result.append("#")
            result.append(contentsOf: fragment)
        }
        return result
    }

    private static func addCapacity(
        _ amount: Int,
        to capacity: inout Int
    ) throws {
        let (sum, overflow) = capacity.addingReportingOverflow(amount)
        guard !overflow else { throw ResolutionError.invalidReference }
        capacity = sum
    }

    private static func removeDotSegments(_ path: Substring) -> String {
        var input = path
        var output = String()
        output.reserveCapacity(path.utf8.count)

        while !input.isEmpty {
            if input.hasPrefix("../") {
                input.removeFirst(3)
            } else if input.hasPrefix("./") {
                input.removeFirst(2)
            } else if input.hasPrefix("/./") {
                input.removeFirst(2)
            } else if input == "/." {
                input = "/"[...]
            } else if input.hasPrefix("/../") {
                input.removeFirst(3)
                removeLastSegment(from: &output)
            } else if input == "/.." {
                input = "/"[...]
                removeLastSegment(from: &output)
            } else if input == "." || input == ".." {
                input = ""[...]
            } else {
                let segmentEnd: Substring.Index
                if input.first == "/" {
                    let afterSlash = input.index(after: input.startIndex)
                    segmentEnd = input[afterSlash...].firstIndex(of: "/")
                        ?? input.endIndex
                } else {
                    segmentEnd = input.firstIndex(of: "/")
                        ?? input.endIndex
                }
                output.append(contentsOf: input[..<segmentEnd])
                input = input[segmentEnd...]
            }
        }
        return output
    }

    private static func removeLastSegment(from output: inout String) {
        guard let slash = output.lastIndex(of: "/") else {
            output.removeAll(keepingCapacity: true)
            return
        }
        output.removeSubrange(slash...)
    }
}
