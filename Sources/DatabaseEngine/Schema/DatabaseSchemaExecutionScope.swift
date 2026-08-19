enum DatabaseSchemaExecutionScope {
    @TaskLocal static var container: DBContainer?
    @TaskLocal static var lease: DatabaseSchemaLease?
    @TaskLocal static var dataAdmissionLease: DatabaseDataOperationAdmissionLease?
    @TaskLocal static var migrationMaintenanceGate: DatabaseMigrationAdmissionGate?
}
