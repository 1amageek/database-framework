import DatabaseRuntime

private let databaseRuntimeCoreImportContract: (
    ByteString.Type,
    Schema.Type,
    DBContainer.Type,
    DatabaseRuntimeConfiguration.Type
) = (
    ByteString.self,
    Schema.self,
    DBContainer.self,
    DatabaseRuntimeConfiguration.self
)
