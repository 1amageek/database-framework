// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "Database",
    platforms: [
        .macOS(.v15)
    ],
    products: [
        .library(name: "DatabaseEngine", targets: ["DatabaseEngine"]),
        .library(name: "ScalarIndex", targets: ["ScalarIndex"]),
        .library(name: "VectorIndex", targets: ["VectorIndex"]),
        .library(name: "FullTextIndex", targets: ["FullTextIndex"]),
        .library(name: "SpatialIndex", targets: ["SpatialIndex"]),
        .library(name: "RankIndex", targets: ["RankIndex"]),
        .library(name: "PermutedIndex", targets: ["PermutedIndex"]),
        .library(name: "GraphIndex", targets: ["GraphIndex"]),
        .library(name: "AggregationIndex", targets: ["AggregationIndex"]),
        .library(name: "VersionIndex", targets: ["VersionIndex"]),
        .library(name: "BitmapIndex", targets: ["BitmapIndex"]),
        .library(name: "LeaderboardIndex", targets: ["LeaderboardIndex"]),
        .library(name: "RelationshipIndex", targets: ["RelationshipIndex"]),
        // QueryIR is provided by database-kit
        .library(name: "QueryAST", targets: ["QueryAST"]),
        .library(name: "Database", targets: ["Database"]),
        .library(name: "BenchmarkFramework", targets: ["BenchmarkFramework"]),
        .library(name: "DatabaseCLICore", targets: ["DatabaseCLICore"]),
        .library(name: "DatabaseServer", targets: ["DatabaseServer"]),
        .executable(name: "database", targets: ["DatabaseCLI"]),
    ],
    dependencies: [
        .package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),
        .package(url: "https://github.com/1amageek/swift-hnsw.git", from: "0.2.1"),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            branch: "feature/directory-layer"
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.7.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "4.2.0"),
        .package(url: "https://github.com/apple/swift-configuration.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
        .package(url: "https://github.com/jpsim/Yams.git", from: "5.1.0"),
    ],
    targets: [
        .target(
            name: "DatabaseEngine",
            dependencies: [
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseClientProtocol", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Configuration", package: "swift-configuration"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "ScalarIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "VectorIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Vector", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "SwiftHNSW", package: "swift-hnsw"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "FullTextIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FullText", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "SpatialIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Spatial", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "RankIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "PermutedIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Permuted", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "GraphIndex",
            dependencies: [
                .product(name: "QueryIR", package: "database-kit"),
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "AggregationIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "VersionIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "BitmapIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "LeaderboardIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "RelationshipIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        // QueryIR is now provided by database-kit
        .target(
            name: "QueryAST",
            dependencies: [
                .product(name: "QueryIR", package: "database-kit"),
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "Database",
            dependencies: [
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                "DatabaseEngine",
                "ScalarIndex",
                "VectorIndex",
                "FullTextIndex",
                "SpatialIndex",
                "RankIndex",
                "PermutedIndex",
                "GraphIndex",
                "AggregationIndex",
                "VersionIndex",
                "BitmapIndex",
                "LeaderboardIndex",
                "RelationshipIndex",
                .product(name: "QueryIR", package: "database-kit"),
                "QueryAST",
            ],
            exclude: ["README.md"]
        ),
        // BenchmarkFramework - Performance benchmarking infrastructure
        .target(
            name: "BenchmarkFramework",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        // DatabaseCLICore - Embeddable CLI library with REPL, commands, and catalog access
        .target(
            name: "DatabaseCLICore",
            dependencies: [
                "DatabaseEngine",
                "GraphIndex",
                "Database",
                "QueryAST",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
                .product(name: "Yams", package: "Yams"),
            ],
            exclude: ["README.md"]
        ),
        // DatabaseServer - Remote client endpoint library
        .target(
            name: "DatabaseServer",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "DatabaseClientProtocol", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        // DatabaseCLI - Standalone executable entry point
        .executableTarget(
            name: "DatabaseCLI",
            dependencies: [
                "DatabaseCLICore",
                "DatabaseEngine",
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            exclude: ["README.md"],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // Test Support (shared test utilities)
        .target(
            name: "TestSupport",
            dependencies: [
                "DatabaseEngine",
                "ScalarIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ],
            path: "Tests/Shared"
        ),
        // Core engine tests
        .testTarget(
            name: "DatabaseEngineTests",
            dependencies: [
                "DatabaseEngine",
                "ScalarIndex",
                "FullTextIndex",
                "AggregationIndex",
                "RelationshipIndex",
                "BitmapIndex",
                "LeaderboardIndex",
                "GraphIndex",
                "TestSupport",
                .product(name: "FullText", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "Logging", package: "swift-log"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // ScalarIndex tests
        .testTarget(
            name: "ScalarIndexTests",
            dependencies: [
                "ScalarIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // VectorIndex tests
        .testTarget(
            name: "VectorIndexTests",
            dependencies: [
                "VectorIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Vector", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // GraphIndex tests
        .testTarget(
            name: "GraphIndexTests",
            dependencies: [
                "GraphIndex",
                "QueryAST",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // AggregationIndex tests (Count, Sum, Min, Max)
        .testTarget(
            name: "AggregationIndexTests",
            dependencies: [
                "AggregationIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // VersionIndex tests
        .testTarget(
            name: "VersionIndexTests",
            dependencies: [
                "VersionIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // SpatialIndex tests
        .testTarget(
            name: "SpatialIndexTests",
            dependencies: [
                "SpatialIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Spatial", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // RankIndex tests
        .testTarget(
            name: "RankIndexTests",
            dependencies: [
                "RankIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // FullTextIndex tests
        .testTarget(
            name: "FullTextIndexTests",
            dependencies: [
                "FullTextIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FullText", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // PermutedIndex tests
        .testTarget(
            name: "PermutedIndexTests",
            dependencies: [
                "PermutedIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Permuted", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // BitmapIndex tests
        .testTarget(
            name: "BitmapIndexTests",
            dependencies: [
                "BitmapIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // LeaderboardIndex tests
        .testTarget(
            name: "LeaderboardIndexTests",
            dependencies: [
                "LeaderboardIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // CLI tests
        .testTarget(
            name: "DatabaseCLITests",
            dependencies: [
                "DatabaseCLICore",
                "Database",
                "TestSupport",
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // QueryAST tests
        .testTarget(
            name: "QueryASTTests",
            dependencies: [
                "QueryAST",
                "DatabaseEngine",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // QueryIR tests (SQL/SPARQL escape, Expression operators)
        .testTarget(
            name: "QueryIRTests",
            dependencies: [
                .product(name: "QueryIR", package: "database-kit"),
            ]
        ),
        // Database integration tests (SPARQL() function, etc.)
        .testTarget(
            name: "DatabaseTests",
            dependencies: [
                "Database",
                "DatabaseEngine",
                "GraphIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // BenchmarkFramework tests
        .testTarget(
            name: "BenchmarkFrameworkTests",
            dependencies: [
                "BenchmarkFramework",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // Performance Benchmarks
        .testTarget(
            name: "PerformanceBenchmarks",
            dependencies: [
                "BenchmarkFramework",
                "TestSupport",
                "ScalarIndex",
                "RankIndex",
                "AggregationIndex",
                "BitmapIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
            ],
            path: "Benchmarks",
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
