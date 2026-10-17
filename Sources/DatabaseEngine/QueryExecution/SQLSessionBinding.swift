import DatabaseKit

/// Retains the exact admitted namespace and principal for one scalar scope.
package struct SQLSessionBinding: Sendable {
    package let resolution: SQLReadResolution
    package let databaseName: String?

    package init(resolution: SQLReadResolution, databaseName: String?) {
        self.resolution = resolution
        self.databaseName = databaseName
    }
}
