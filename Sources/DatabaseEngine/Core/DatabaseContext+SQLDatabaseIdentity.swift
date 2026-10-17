extension DatabaseContext {
    package var inheritedSQLDatabaseName: String? {
        container.configuration.sqlDatabaseName
    }
}
