// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "database-framework-benchmarks",
    platforms: [.macOS(.v26)],
    dependencies: [
        .package(
            path: "..",
            traits: ["FoundationDB", "AllRuntimeFeatures"]
        ),
        .package(path: "../../database-framework-benchmark"),
        .package(
            url: "https://github.com/1amageek/database-kit.git",
            revision: "cca9c9da970c7cbf2d41e8f7471632df0aa92ff2"
        ),
        .package(
            url: "https://github.com/1amageek/database-types.git",
            revision: "d075edcb37f8a7be845c5be4d6a7a226d899505b"
        ),
        .package(
            url: "https://github.com/1amageek/storage-kit.git",
            revision: "5408f55259f3ae883c2f6ba31b2fbd230856019d"
        ),
        .package(
            url: "https://github.com/1amageek/swift-testing-heartbeat.git",
            revision: "74b9288fd7f21f575c06fd8b3b4bb7416321e5b0"
        ),
    ],
    targets: [
        .testTarget(
            name: "SQLMutationPerformanceBenchmarks",
            dependencies: [
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "QueryAST", package: "database-framework"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        .testTarget(
            name: "FrameworkPerformanceBenchmarks",
            dependencies: [
                .product(
                    name: "BenchmarkFramework",
                    package: "database-framework-benchmark"
                ),
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "DatabaseRuntime", package: "database-framework"),
                .product(name: "QueryAST", package: "database-framework"),
                .product(name: "ScalarIndex", package: "database-framework"),
                .product(name: "VectorIndex", package: "database-framework"),
                .product(name: "FullTextIndex", package: "database-framework"),
                .product(name: "SpatialIndex", package: "database-framework"),
                .product(name: "RankIndex", package: "database-framework"),
                .product(name: "GraphIndex", package: "database-framework"),
                .product(name: "AggregationIndex", package: "database-framework"),
                .product(name: "VersionIndex", package: "database-framework"),
                .product(name: "BitmapIndex", package: "database-framework"),
                .product(name: "LeaderboardIndex", package: "database-framework"),
                .product(name: "OntologyIndex", package: "database-framework"),
                .product(name: "RelationshipIndex", package: "database-framework"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitSystemClock", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [.define("FOUNDATION_DB")],
            linkerSettings: [
                .unsafeFlags([
                    "-Xlinker", "-rpath",
                    "-Xlinker", "/usr/local/lib",
                ])
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
