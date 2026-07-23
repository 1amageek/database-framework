#if FOUNDATION_DB
@testable import QueryAST

enum SPARQLUpdateAssertionError: Error {
    case expectedSingleOperation
}

func requireSingleSPARQLUpdateOperation(
    _ statement: QueryStatement
) throws -> SPARQLUpdateOperation {
    guard case .sparqlUpdate(let request) = statement,
          request.count == 1 else {
        throw SPARQLUpdateAssertionError.expectedSingleOperation
    }
    return request.firstOperation
}
#endif
