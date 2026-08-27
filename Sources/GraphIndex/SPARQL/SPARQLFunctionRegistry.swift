import DatabaseKit
import DatabaseTypes

/// Immutable, container-scoped registry for SPARQL extension functions.
public struct SPARQLFunctionRegistry: Sendable {
    private let functions: [RDFIRI: any SPARQLFunction]

    public static let empty = SPARQLFunctionRegistry(functions: [:])

    public init(_ functions: [any SPARQLFunction]) throws {
        var registered: [RDFIRI: any SPARQLFunction] = [:]
        registered.reserveCapacity(functions.count)
        for function in functions {
            guard registered[function.identifier] == nil else {
                throw SPARQLFunctionRegistryError.duplicateFunction(
                    function.identifier.rawValue
                )
            }
            registered[function.identifier] = function
        }
        self.functions = registered
    }

    private init(functions: [RDFIRI: any SPARQLFunction]) {
        self.functions = functions
    }

    func maximumResultByteCount(
        identifier: String
    ) throws(SPARQLFunctionRegistryError) -> UInt64 {
        let iri: RDFIRI
        do {
            iri = try RDFIRI(identifier)
        } catch {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        guard let function = functions[iri] else {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        return function.maximumResultByteCount
    }

    func evaluate(
        identifier: String,
        arguments: [FieldValue]
    ) throws(SPARQLFunctionRegistryError) -> FieldValue {
        let iri: RDFIRI
        do {
            iri = try RDFIRI(identifier)
        } catch {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        guard let function = functions[iri] else {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        let value: FieldValue
        do {
            value = try function.evaluate(arguments: arguments)
        } catch let error {
            throw .evaluation(error)
        }
        guard case .rdfTerm = value else {
            throw SPARQLFunctionRegistryError.nonCanonicalResult(identifier)
        }
        return value
    }
}
