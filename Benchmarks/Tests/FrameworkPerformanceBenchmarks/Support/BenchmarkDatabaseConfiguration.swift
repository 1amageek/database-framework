@_spi(Testing) import DatabaseEngine
import StorageKit

extension DBConfiguration {
    static func benchmarking(
        name: String? = nil,
        databaseIdentifier: String? = nil,
        storageEngine: any StorageEngine
    ) throws(DatabaseDirectoryLayoutError) -> DBConfiguration {
        try DBConfiguration(
            name: name,
            storageEngine: storageEngine,
            databaseRootPath: databaseIdentifier.map {
                ["benchmark-database", $0]
            } ?? [],
            monotonicClock: BenchmarkProcessMonotonicClock(),
            wallClock: FixedBenchmarkWallClock()
        )
    }
}

extension SecurityConfiguration {
    static var benchmarkingDisabled: SecurityConfiguration {
        .disabledForTesting
    }
}

extension DBContainer {
    func benchmarkContext(
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        newContext(
            authorization: .anonymous,
            autosaveEnabled: autosaveEnabled
        )
    }
}
