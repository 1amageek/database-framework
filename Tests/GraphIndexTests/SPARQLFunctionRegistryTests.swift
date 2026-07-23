import Core
import DatabaseValue
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("SPARQL function registry", .heartbeat)
struct SPARQLFunctionRegistryTests {
    private enum SPARQLFunctionEvaluationFailure: Error {
        case expected
    }

    private struct ConfiguredSPARQLFunction: SPARQLFunction {
        enum Behavior: Sendable {
            case returnValue(FieldValue)
            case fail
        }

        let identifier: DatabaseRDFIRI
        let behavior: Behavior

        func evaluate(arguments: [FieldValue]) throws -> FieldValue {
            switch behavior {
            case .returnValue(let value):
                return value
            case .fail:
                throw SPARQLFunctionEvaluationFailure.expected
            }
        }
    }

    @Test("An exact canonical IRI resolves to its injected function")
    func exactCanonicalLookup() throws {
        let identifier = try DatabaseRDFIRI("https://example.com/function/value")
        let expected = FieldValue.rdfTerm(.iri("https://example.com/result"))
        let registry = try SPARQLFunctionRegistry([
            ConfiguredSPARQLFunction(
                identifier: identifier,
                behavior: .returnValue(expected)
            )
        ])

        #expect(
            try registry.evaluate(
                identifier: identifier.rawValue,
                arguments: []
            ) == expected
        )
    }

    @Test("Duplicate extension identifiers fail registry construction")
    func duplicateIdentifierFails() throws {
        let identifier = try DatabaseRDFIRI("https://example.com/function/value")
        let function = ConfiguredSPARQLFunction(
            identifier: identifier,
            behavior: .returnValue(.rdfTerm(.iri("https://example.com/result")))
        )

        #expect(throws: SPARQLFunctionRegistryError.self) {
            _ = try SPARQLFunctionRegistry([function, function])
        }
    }

    @Test("Unknown extension IRIs remain typed failures")
    func unknownFunctionFails() {
        #expect(throws: SPARQLFunctionRegistryError.self) {
            _ = try SPARQLFunctionRegistry.empty.evaluate(
                identifier: "https://example.com/function/missing",
                arguments: []
            )
        }
    }

    @Test("Extension functions cannot return non-canonical scalar values")
    func nonCanonicalResultFails() throws {
        let identifier = try DatabaseRDFIRI("https://example.com/function/scalar")
        let registry = try SPARQLFunctionRegistry([
            ConfiguredSPARQLFunction(
                identifier: identifier,
                behavior: .returnValue(.string("not-an-rdf-term"))
            )
        ])

        #expect(throws: SPARQLFunctionRegistryError.self) {
            _ = try registry.evaluate(
                identifier: identifier.rawValue,
                arguments: []
            )
        }
    }

    @Test("Implementation failures preserve the function identity")
    func extensionFunctionFailurePreservesIdentifier() throws {
        let identifier = try DatabaseRDFIRI("https://example.com/function/failing")
        let registry = try SPARQLFunctionRegistry([
            ConfiguredSPARQLFunction(identifier: identifier, behavior: .fail)
        ])

        do {
            _ = try registry.evaluate(
                identifier: identifier.rawValue,
                arguments: []
            )
            Issue.record("Expected the extension function to fail")
        } catch let error as SPARQLFunctionRegistryError {
            guard case .functionFailed(let failedIdentifier, _) = error else {
                Issue.record("Expected a functionFailed error, received \(error)")
                return
            }
            #expect(failedIdentifier == identifier.rawValue)
        }
    }
}
