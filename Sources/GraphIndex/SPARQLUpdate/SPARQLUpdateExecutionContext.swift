import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine

@_spi(DatabaseExecution)
public protocol SPARQLUpdateExecutionContext: Sendable {
    var idempotencyKey: String? { get }

    func makeSPARQLQueryExecutor(
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode,
        dataset: SPARQLExecutionDataset,
        functionRegistry: SPARQLFunctionRegistry
    ) throws -> SPARQLQueryExecutor
}
