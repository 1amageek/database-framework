#if FOUNDATION_DB && !MultipleBases
import Database
import FDBStorage
import Testing

@Suite("FoundationDB database configuration")
struct FDBDatabaseConfigurationTests {
    @Test("A FoundationDB database requires a Directory path")
    func rejectsEmptyDirectoryPath() {
        #expect(
            throws: FDBDatabaseConfigurationError.emptyDirectoryPath
        ) {
            _ = try FDBDatabaseConfiguration(
                storage: FDBStorageEngine.Configuration(),
                directoryPath: []
            )
        }
    }

    @Test("Every FoundationDB Directory component must be nonempty")
    func rejectsEmptyDirectoryComponent() {
        #expect(
            throws: FDBDatabaseConfigurationError
                .emptyDirectoryComponent(index: 1)
        ) {
            _ = try FDBDatabaseConfiguration(
                storage: FDBStorageEngine.Configuration(),
                directoryPath: ["applications", ""]
            )
        }
    }
}
#endif
