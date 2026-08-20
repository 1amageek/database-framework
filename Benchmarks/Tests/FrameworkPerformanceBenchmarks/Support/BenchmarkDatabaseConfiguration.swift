@_spi(Testing) import DatabaseEngine
import StorageKit

extension DBConfiguration {
    static func benchmarking(
        name: String? = nil,
        databaseIdentifier: String? = nil,
        storageEngine: any StorageEngine
    ) -> DBConfiguration {
        DBConfiguration(
            name: name,
            storageEngine: storageEngine,
            databaseRoot: databaseIdentifier.map {
                Subspace("benchmark-database", $0)
            } ?? Subspace(),
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
