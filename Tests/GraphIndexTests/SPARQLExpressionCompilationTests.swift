import DatabaseKit
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL expression compilation", .heartbeat)
struct SPARQLExpressionCompilationTests {
    @Test("Every valid absolute IRI scheme can identify an extension function")
    func extensionFunctionIRIClassification() throws {
        for identifier in [
            "mailto:function@example.com",
            "tag:example.com,2026:function",
            "did:example:function",
            "urn:example:function",
            "https://example.com/function",
        ] {
            let resolved = try SPARQLFunctionIdentifier.resolve(identifier)
            guard case .extensionFunction(let iri) = resolved else {
                Issue.record("Expected an extension function for \(identifier)")
                continue
            }
            #expect(iri.rawValue == identifier)
        }
    }

    @Test("XSD constructor IRIs are not extension functions")
    func datatypeConstructorClassification() throws {
        let identifier =
            "http://www.w3.org/2001/XMLSchema#integer"
        let resolved = try SPARQLFunctionIdentifier.resolve(identifier)

        guard case .datatypeConstructor(let iri) = resolved else {
            Issue.record("Expected an XSD datatype constructor")
            return
        }
        #expect(iri.rawValue == identifier)
    }

    @Test("A non-IRI unknown function fails before execution")
    func invalidFunctionIdentifierFails() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .function(
                    FunctionCall(
                        name: "not a function",
                        arguments: []
                    )
                )
            )
        }
    }

    @Test("BOUND requires a variable operand")
    func boundRequiresVariable() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .function(
                    FunctionCall(
                        name: "BOUND",
                        arguments: [.literal(.bool(true))]
                    )
                )
            )
        }
    }

    @Test("Directly constructed expressions obey the compilation depth limit")
    func compilationDepthIsBounded() {
        let expression = Expression.not(
            .not(.not(.literal(.bool(true))))
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    maximumDepth: 3
                )
            )
        }
    }

    @Test("Function argument collections are bounded after decode")
    func functionArgumentsAreBounded() {
        let expression = Expression.function(
            FunctionCall(
                name: "CONCAT",
                arguments: [
                    .literal(.string("a")),
                    .literal(.string("b")),
                    .literal(.string("c")),
                ]
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                expression,
                limits: SPARQLExpressionCompilationLimits(
                    maximumFunctionArguments: 2
                )
            )
        }
    }

    @Test("Directly constructed literal strings obey the UTF-8 limit")
    func literalStringsAreBounded() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            try SPARQLExpressionValidator.validate(
                .literal(.string("four")),
                limits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("Expression plans cannot bypass bounded compilation")
    func expressionPlanUsesCompilerLimits() {
        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(
                .literal(.string("four")),
                limits: SPARQLExpressionCompilationLimits(
                    maximumStringUTF8Count: 3
                )
            )
        }
    }

    @Test("EXISTS recursively validates its graph pattern")
    func existsGraphPatternIsValidated() {
        let expression = Expression.exists(
            SelectQuery(
                projection: .all,
                source: .graphPattern(
                    .service(
                        endpoint: "https://example.com/sparql",
                        pattern: .basic([]),
                        silent: false
                    )
                )
            )
        )

        #expect(throws: SPARQLExpressionCompilationError.self) {
            _ = try SPARQLExpressionPlan(expression)
        }
    }
}
