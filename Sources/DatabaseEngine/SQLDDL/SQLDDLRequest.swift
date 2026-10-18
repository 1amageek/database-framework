import DatabaseKit

/// Immutable request identity shared by every closed-and-replayed data attempt.
struct SQLDDLRequest: Sendable {
    let identity: String
    var temporaryNamespace: String { "sql_temp_" + identity }

    init() {
        var random = SystemRandomNumberGenerator()
        identity = String(random.next(), radix: 16) + String(random.next(), radix: 16)
    }

    func objectID(step: Int, role: String) -> String {
        "sql:" + identity + ":" + String(step) + ":" + role
    }

    func searchPath(_ requested: SQLSearchPath, catalog: SQLCatalog) throws -> SQLSearchPath {
        guard catalog.namespaces.contains(where: { $0.id.utf8.elementsEqual(temporaryNamespace.utf8) }) else { return requested }
        return try SQLSearchPath(entries: [.schema(temporaryNamespace)] + requested.entries)
    }
}
