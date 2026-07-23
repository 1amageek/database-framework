import Core
import DatabaseValue

/// Immutable, container-scoped registry for SPARQL extension functions.
public struct SPARQLFunctionRegistry: Sendable {
    private let functions: [DatabaseRDFIRI: any SPARQLFunction]

    public static let empty = SPARQLFunctionRegistry(functions: [:])

    public init(_ functions: [any SPARQLFunction]) throws {
        var registered: [DatabaseRDFIRI: any SPARQLFunction] = [:]
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

    private init(functions: [DatabaseRDFIRI: any SPARQLFunction]) {
        self.functions = functions
    }

    func evaluate(
        identifier: String,
        arguments: [FieldValue]
    ) throws -> FieldValue {
        let iri: DatabaseRDFIRI
        do {
            iri = try DatabaseRDFIRI(identifier)
        } catch {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        guard let function = functions[iri] else {
            throw SPARQLFunctionRegistryError.unknownFunction(identifier)
        }
        let value: FieldValue
        do {
            value = try function.evaluate(arguments: arguments)
        } catch let error as SPARQLExpressionEvaluationError {
            throw error
        } catch {
            throw SPARQLFunctionRegistryError.functionFailed(
                identifier: identifier,
                detail: String(describing: error)
            )
        }
        guard case .rdfTerm = value else {
            throw SPARQLFunctionRegistryError.nonCanonicalResult(identifier)
        }
        return value
    }
}
