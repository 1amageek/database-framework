#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseMath
import DatabaseEngine
import DatabaseTypes
import Synchronization

/// Query-scoped state for SPARQL functions whose result cannot be evaluated as
/// a compile-time scalar. A new instance is created for every query execution.
final class SPARQLQueryExpressionContext: Sendable {
    private struct BlankNodeKey: Sendable, Hashable {
        let solutionScope: UInt64
        let label: String
    }

    private struct BlankNodeState: Sendable {
        var nextSolutionScope: UInt64 = 1
        var identifiersByKey: [BlankNodeKey: String] = [:]
    }

    private let nowTimestamp: Timestamp
    private let blankNodes = Mutex(BlankNodeState())
    private let functionRegistry: SPARQLFunctionRegistry
    private let workMeter: DatabaseWorkMeter

    init(
        now: Timestamp? = nil,
        functionRegistry: SPARQLFunctionRegistry,
        workMeter: DatabaseWorkMeter
    ) throws {
        self.nowTimestamp = try now ?? Self.currentTimestamp()
        self.functionRegistry = functionRegistry
        self.workMeter = workMeter
    }

    func bindingWithExpressionScope(
        _ binding: VariableBinding
    ) throws -> VariableBinding {
        guard binding.expressionScopeIdentifier == nil else { return binding }
        let identifiers = try reserveExpressionScopes(count: 1)
        return binding.assigningExpressionScope(identifiers.lowerBound)
    }

    /// Reserves stable solution identifiers before a retained operator stores
    /// row metadata. The returned range is query-local and never reused.
    func reserveExpressionScopes(count: Int) throws -> Range<UInt64> {
        guard count >= 0, let count = UInt64(exactly: count) else {
            throw SPARQLExpressionEvaluationError.resourceLimitExceeded(
                stage: "BNODE solution scope",
                required: nil,
                maximum: UInt64.max
            )
        }
        try workMeter.consume(count, at: .resultMaterialization)
        return try blankNodes.withLock { state in
            let identifier = state.nextSolutionScope
            let (next, overflow) = identifier.addingReportingOverflow(count)
            guard !overflow else {
                throw SPARQLExpressionEvaluationError.resourceLimitExceeded(
                    stage: "BNODE solution scope",
                    required: nil,
                    maximum: UInt64.max
                )
            }
            state.nextSolutionScope = next
            return identifier..<next
        }
    }

    func bindingWithExpressionScope(
        _ binding: borrowing VariableBinding,
        reservedIdentifier: UInt64
    ) -> VariableBinding {
        binding.assigningExpressionScope(reservedIdentifier)
    }

    func evaluateFunction(
        name: String,
        arguments: [FieldValue],
        binding: VariableBinding
    ) throws -> FieldValue {
        switch name.uppercased() {
        case "NOW":
            guard arguments.isEmpty else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            return try ExpressionEvaluator.evaluate(
                .literal(.timestamp(nowTimestamp)),
                binding: VariableBinding()
            )

        case "RAND":
            guard arguments.isEmpty else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            return try ExpressionEvaluator.evaluate(
                .literal(.double(Double.random(in: 0..<1))),
                binding: VariableBinding()
            )

        case "UUID":
            guard arguments.isEmpty else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            return .rdfTerm(
                .iri(try RDFIRI("urn:uuid:\(Self.randomUUID())"))
            )

        case "STRUUID":
            guard arguments.isEmpty else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            return try ExpressionEvaluator.evaluate(
                .literal(.string(Self.randomUUID().description)),
                binding: VariableBinding()
            )

        case "BNODE":
            guard arguments.count <= 1 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            if arguments.isEmpty {
                try workMeter.consume(at: .resultMaterialization)
                return .rdfTerm(
                    .blankNode(
                        try RDFBlankNodeIdentifier(
                            Self.randomUUID().description
                        )
                    )
                )
            }
            guard let label = Self.simpleString(arguments[0]) else {
                throw SPARQLExpressionEvaluationError.typeError(
                    "BNODE requires a simple string literal"
                )
            }
            guard let solutionScope = binding.expressionScopeIdentifier else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "BNODE(label) requires a solution-scoped binding"
                )
            }
            try workMeter.consume(
                UInt64(label.utf8.count) + 1,
                at: .resultMaterialization
            )
            let key = BlankNodeKey(
                solutionScope: solutionScope,
                label: label
            )
            let identifier = blankNodes.withLock { state in
                if let existing = state.identifiersByKey[key] {
                    return existing
                }
                let generated = Self.randomUUID().description
                state.identifiersByKey[key] = generated
                return generated
            }
            return .rdfTerm(
                .blankNode(try RDFBlankNodeIdentifier(identifier))
            )

        default:
            do {
                return try functionRegistry.evaluate(
                    identifier: name,
                    arguments: arguments
                )
            } catch SPARQLFunctionRegistryError.unknownFunction(_) {
                throw SPARQLExpressionEvaluationError.unsupportedExpression(
                    "function \(name)"
                )
            } catch let error as SPARQLExpressionEvaluationError {
                throw error
            } catch {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    String(describing: error)
                )
            }
        }
    }

    private static func currentTimestamp() throws -> Timestamp {
        let interval = Date().timeIntervalSince1970
        let seconds = DatabaseMath.floor(interval)
        let fractional = max(0, min(interval - seconds, 0.999_999_999))
        return try Timestamp(
            secondsSinceUnixEpoch: Int64(seconds),
            nanoseconds: UInt32(fractional * 1_000_000_000)
        )
    }

    private static func randomUUID() -> DatabaseTypes.UUID {
        var high = UInt64.random(in: UInt64.min...UInt64.max)
        var low = UInt64.random(in: UInt64.min...UInt64.max)
        high = (high & 0xffff_ffff_ffff_0fff) | 0x0000_0000_0000_4000
        low = (low & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000
        return DatabaseTypes.UUID(high: high, low: low)
    }

    private static func simpleString(_ value: FieldValue) -> String? {
        switch value {
        case .rdfTerm(.literal(let literal))
            where literal.datatypeIRI == .xsdString:
            return literal.lexicalForm
        case .string(let value):
            return value
        default:
            return nil
        }
    }
}
