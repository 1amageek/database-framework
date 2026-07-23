/// A SHACL pattern backed by the bounded SPARQL regular-expression engine.
///
/// SHACL defines `sh:pattern` in terms of SPARQL REGEX semantics. This adapter
/// keeps invalid shape definitions, resource exhaustion, and runtime failures
/// distinct so validation never turns an execution failure into a violation.
struct SHACLRegularExpression: Sendable {
    private let expression: SPARQLRegularExpression

    init(pattern: String, flags: String?) throws {
        do {
            expression = try SPARQLRegularExpression(
                pattern: pattern,
                flags: flags
            )
        } catch let error as SPARQLRegularExpression.Error {
            throw Self.mapCompilationError(error, pattern: pattern)
        }
    }

    func matches(_ input: String) throws -> Bool {
        try matches(input) { _ in }
    }

    func matches(
        _ input: String,
        onValidatedInput: (Int) throws -> Void
    ) throws -> Bool {
        do {
            return try expression.matches(
                input,
                onValidatedInput: onValidatedInput
            )
        } catch let error as SPARQLRegularExpression.Error {
            throw Self.mapExecutionError(error)
        }
    }

    private static func mapCompilationError(
        _ error: SPARQLRegularExpression.Error,
        pattern: String
    ) -> SHACLError {
        switch error {
        case .invalidFlags(let offset, let flag):
            return .invalidPattern(
                regex: pattern,
                reason: "unsupported flag '\(flag)' at scalar offset \(offset)"
            )
        case .invalidSyntax(let offset, let reason):
            return .invalidPattern(
                regex: pattern,
                reason: "\(reason) at scalar offset \(offset)"
            )
        case .invalidReplacement(let offset, let reason):
            return .runtimeFailure(
                stage: "regular expression compilation",
                reason: "unexpected replacement error at offset \(offset): \(reason)"
            )
        case .resourceLimit(let name, let limit, let actual):
            return resourceLimit(name: name, limit: limit, actual: actual)
        }
    }

    private static func mapExecutionError(
        _ error: SPARQLRegularExpression.Error
    ) -> SHACLError {
        switch error {
        case .resourceLimit(let name, let limit, let actual):
            return resourceLimit(name: name, limit: limit, actual: actual)
        case .invalidFlags(let offset, let flag):
            return .runtimeFailure(
                stage: "regular expression matching",
                reason: "compiled expression produced flag '\(flag)' at offset \(offset)"
            )
        case .invalidSyntax(let offset, let reason):
            return .runtimeFailure(
                stage: "regular expression matching",
                reason: "compiled expression produced syntax error at offset \(offset): \(reason)"
            )
        case .invalidReplacement(let offset, let reason):
            return .runtimeFailure(
                stage: "regular expression matching",
                reason: "compiled expression produced replacement error at offset \(offset): \(reason)"
            )
        }
    }

    private static func resourceLimit(
        name: String,
        limit: Int,
        actual: Int
    ) -> SHACLError {
        .resourceLimitExceeded(
            resource: "regularExpression.\(name)",
            limit: limit,
            actual: actual
        )
    }
}
