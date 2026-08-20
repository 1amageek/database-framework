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
            from: "26.0819.0"
        ),
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0730.0"
        ),
        .package(
            url: "https://github.com/1amageek/storage-kit.git",
            from: "26.0807.0"
        ),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            from: "0.3.3"
        ),
        .package(
            url: "https://github.com/1amageek/swift-testing-heartbeat.git",
            from: "0.1.0"
        ),
    ],
    targets: [
        .testTarget(
            name: "FrameworkPerformanceBenchmarks",
            dependencies: [
                .product(
                    name: "BenchmarkFramework",
                    package: "database-framework-benchmark"
                ),
                .product(name: "DatabaseEngine", package: "database-framework"),
                .product(name: "DatabaseRuntime", package: "database-framework"),
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
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
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
