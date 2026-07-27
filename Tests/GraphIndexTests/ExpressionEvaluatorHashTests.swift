// ExpressionEvaluatorHashTests.swift
// GraphIndexTests - SPARQL hash function behavior

import Testing
import DatabaseKit
import DatabaseTypes
@testable import GraphIndex

@Suite("ExpressionEvaluator SPARQL Hash Functions", .heartbeat)
struct ExpressionEvaluatorHashTests {
    @Test("Standard digest vectors are lowercase hexadecimal")
    func standardDigestVectors() throws {
        let vectors = [
            ("MD5", "900150983cd24fb0d6963f7d28e17f72"),
            ("SHA1", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("SHA256", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
            ("SHA384", "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"),
            ("SHA512", "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f")
        ]

        for (name, expected) in vectors {
            let result = try evaluateHash(name, input: "abc")
            #expect(
                result
                    == .rdfTerm(
                        .literal(
                            RDFLiteral(
                                lexicalForm: expected,
                                datatype: .xsdString
                            )
                        )
                    ),
                "Unexpected \(name) digest"
            )
        }
    }

    @Test("Hash functions consume the exact UTF-8 representation")
    func unicodeInputUsesUTF8() throws {
        let result = try evaluateHash("SHA256", input: "🌍")

        #expect(
            result
                == .rdfTerm(
                    .literal(
                        RDFLiteral(
                            lexicalForm: "8cba3282fe37fa6054ce64531ce17410a1404e2bc4afbf113824097037b1e498",
                            datatype: .xsdString
                        )
                    )
                )
        )
    }

    @Test("Hash functions reject invalid argument counts")
    func invalidArgumentCountIsEvaluationFailure() {
        let expression = Expression.function(
            FunctionCall(name: "SHA256", arguments: [])
        )

        #expect(
            throws: SPARQLExpressionEvaluationError
                .invalidFunctionArguments("SHA256")
        ) {
            try ExpressionEvaluator.evaluate(
                expression,
                binding: VariableBinding()
            )
        }
    }

    private func evaluateHash(
        _ name: String,
        input: String
    ) throws -> FieldValue {
        let expression = Expression.function(
            FunctionCall(
                name: name,
                arguments: [.literal(.string(input))]
            )
        )
        return try ExpressionEvaluator.evaluate(
            expression,
            binding: VariableBinding()
        )
    }
}
