enum DatabaseSchemaExecutionScope {
    @TaskLocal static var lease: DatabaseSchemaLease?
}
