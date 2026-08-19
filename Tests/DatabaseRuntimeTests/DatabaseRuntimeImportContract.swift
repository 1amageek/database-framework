import DatabaseRuntime

private let databaseRuntimeCoreImportContract: (
    ByteString.Type,
    Schema.Type,
    DBContainer.Type,
    DatabaseRuntimeConfiguration.Type,
    DatabaseExecutionRuntimeIdentity.Type
) = (
    ByteString.self,
    Schema.self,
    DBContainer.self,
    DatabaseRuntimeConfiguration.self,
    DatabaseExecutionRuntimeIdentity.self
)

#if DATABASE_RUNTIME_TEST_GRAPH_INDEXES
private let databaseRuntimeOntologyImportContract: ReasoningTriple.Type =
    ReasoningTriple.self
#endif
