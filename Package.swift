// swift-tools-version: 6.4
import PackageDescription

let hostPlatforms: [Platform] = [
    .macOS,
    .iOS,
    .linux,
]

let package = Package(
    name: "database-framework",
    platforms: [
        .macOS(.v26),
        .iOS(.v26),
    ],
    products: [
        .library(name: "DatabaseEngine", targets: ["DatabaseEngine"]),
        .library(
            name: "DatabaseEngineFoundation",
            targets: ["DatabaseEngineFoundation"]
        ),
        .library(name: "DatabaseRuntime", targets: ["DatabaseRuntime"]),
        .library(name: "ScalarIndex", targets: ["ScalarIndex"]),
        .library(name: "VectorIndex", targets: ["VectorIndex"]),
        .library(name: "FullTextIndex", targets: ["FullTextIndex"]),
        .library(
            name: "FullTextIndexFoundation",
            targets: ["FullTextIndexFoundation"]
        ),
        .library(name: "SpatialIndex", targets: ["SpatialIndex"]),
        .library(name: "RankIndex", targets: ["RankIndex"]),
        .library(name: "PermutedIndex", targets: ["PermutedIndex"]),
        .library(name: "GraphIndex", targets: ["GraphIndex"]),
        .library(name: "AggregationIndex", targets: ["AggregationIndex"]),
        .library(name: "VersionIndex", targets: ["VersionIndex"]),
        .library(name: "BitmapIndex", targets: ["BitmapIndex"]),
        .library(name: "LeaderboardIndex", targets: ["LeaderboardIndex"]),
        .library(name: "OntologyIndex", targets: ["OntologyIndex"]),
        .library(name: "RelationshipIndex", targets: ["RelationshipIndex"]),
        // QueryIR is provided by database-kit
        .library(name: "QueryAST", targets: ["QueryAST"]),
        .library(name: "Database", targets: ["Database"]),
        .library(name: "BenchmarkFramework", targets: ["BenchmarkFramework"]),
        .library(name: "DatabaseCLICore", targets: ["DatabaseCLICore"]),
        .library(name: "DatabaseServer", targets: ["DatabaseServer"]),
        .executable(name: "database", targets: ["DatabaseCLI"]),
    ],
    traits: [
        .default(enabledTraits: ["FoundationDB"]),
        .trait(name: "FoundationDB"),
        .trait(name: "SQLite"),
        .trait(name: "PostgreSQL"),
    ],
    dependencies: [
        .package(path: "../database-kit"),
        .package(path: "../swift-hnsw"),
        .package(
            path: "../storage-kit",
            traits: [
                .trait(name: "FoundationDB", condition: .when(traits: ["FoundationDB"])),
                .trait(name: "SQLite", condition: .when(traits: ["SQLite"])),
                .trait(name: "PostgreSQL", condition: .when(traits: ["PostgreSQL"])),
            ]
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.7.0"),
        .package(path: "../../networking/swift-crypto"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.5.0"),
        .package(url: "https://github.com/1amageek/swift-testing-heartbeat.git", from: "0.1.0"),
    ],
    targets: [
        .target(name: "DatabaseMath"),
        .target(
            name: "DatabaseEngine",
            dependencies: [
                "DatabaseMath",
                .product(name: "DatabaseDigest", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit",
                         condition: .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
            ],
            exclude: ["README.md"],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ]
        ),
        .target(
            name: "ScalarIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "DatabaseEngineFoundation",
            dependencies: ["DatabaseEngine"]
        ),
        .target(
            name: "VectorIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "Vector", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "SwiftHNSW", package: "swift-hnsw"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "FullTextIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "FullText", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "FullTextIndexFoundation",
            dependencies: ["FullTextIndex"]
        ),
        .target(
            name: "DatabaseRuntime",
            dependencies: [
                "DatabaseEngine",
                "ScalarIndex",
                "VectorIndex",
                "FullTextIndex",
                "SpatialIndex",
                "RankIndex",
                "BitmapIndex",
                "VersionIndex",
                "PermutedIndex",
                "GraphIndex",
                "AggregationIndex",
                "LeaderboardIndex",
                "RelationshipIndex",
            ]
        ),
        .target(
            name: "SpatialIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Geospatial", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "RankIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "PermutedIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "Permuted", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "GraphIndex",
            dependencies: [
                "DatabaseMath",
                .product(name: "DatabaseDigest", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                "DatabaseEngine",
                "OntologyIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "Crypto", package: "swift-crypto"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "AggregationIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "VersionIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "BitmapIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "LeaderboardIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "OntologyIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        .target(
            name: "RelationshipIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        // QueryIR is now provided by database-kit
        .target(
            name: "QueryAST",
            dependencies: [
                "DatabaseMath",
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
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
                "DatabaseRuntime",
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
                "OntologyIndex",
                .product(name: "QueryIR", package: "database-kit"),
                "QueryAST",
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit",
                         condition: .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
                .product(name: "SQLiteStorage", package: "storage-kit",
                         condition: .when(platforms: hostPlatforms, traits: ["SQLite"])),
                .product(name: "PostgreSQLStorage", package: "storage-kit",
                         condition: .when(platforms: hostPlatforms, traits: ["PostgreSQL"])),
            ],
            exclude: ["README.md"],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
                .define("SQLITE", .when(platforms: hostPlatforms, traits: ["SQLite"])),
                .define("POSTGRESQL", .when(platforms: hostPlatforms, traits: ["PostgreSQL"])),
            ]
        ),
        // BenchmarkFramework - Performance benchmarking infrastructure
        .target(
            name: "BenchmarkFramework",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        // DatabaseCLICore - Embeddable CLI library with REPL, commands, and catalog access
        .target(
            name: "DatabaseCLICore",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit",
                         condition: .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ],
            exclude: ["README.md"],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ]
        ),
        // DatabaseServer - Remote client endpoint library
        .target(
            name: "DatabaseServer",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                "GraphIndex",
                "OntologyIndex",
                "RelationshipIndex",
                "QueryAST",
                .product(name: "DatabaseDigest", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "Core", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        // DatabaseCLI - Standalone executable entry point
        .executableTarget(
            name: "DatabaseCLI",
            dependencies: [
                "DatabaseCLICore",
                "DatabaseEngine",
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            exclude: ["README.md"],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"], .when(platforms: hostPlatforms)),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"], .when(platforms: hostPlatforms))
            ]
        ),
        // Test Support (shared test utilities)
        .target(
            name: "TestSupport",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                "ScalarIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit",
                         condition: .when(traits: ["FoundationDB"])),
                .product(name: "PostgreSQLStorage", package: "storage-kit",
                         condition: .when(traits: ["PostgreSQL"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            path: "Tests/Shared",
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
                .define("POSTGRESQL", .when(traits: ["PostgreSQL"])),
            ]
        ),
        // Core engine tests
        .testTarget(
            name: "DatabaseEngineTests",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitEmbeddedCore", package: "storage-kit"),
                .target(name: "ScalarIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "VectorIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "FullTextIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "SpatialIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "RankIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "PermutedIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "AggregationIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "VersionIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "RelationshipIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "BitmapIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "LeaderboardIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "GraphIndex", condition: .when(platforms: hostPlatforms)),
                .target(name: "TestSupport", condition: .when(platforms: hostPlatforms)),
                .product(name: "Vector", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "FullText", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Geospatial", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Rank", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Permuted", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Graph", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Relationship", package: "database-kit", condition: .when(platforms: hostPlatforms)),
                .product(name: "Logging", package: "swift-log", condition: .when(platforms: hostPlatforms)),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat", condition: .when(platforms: hostPlatforms)),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"], .when(platforms: hostPlatforms)),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"], .when(platforms: hostPlatforms))
            ]
        ),
        .testTarget(
            name: "DatabaseEngineTransactionTests",
            dependencies: [
                "DatabaseEngine",
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"], .when(platforms: hostPlatforms)),
                .unsafeFlags(
                    ["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"],
                    .when(platforms: hostPlatforms)
                ),
            ]
        ),
        .testTarget(
            name: "DatabaseRuntimeTests",
            dependencies: [
                "DatabaseRuntime",
                "DatabaseEngine",
                "ScalarIndex",
                "VectorIndex",
                "RelationshipIndex",
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: hostPlatforms, traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"], .when(platforms: hostPlatforms)),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"], .when(platforms: hostPlatforms))
            ]
        ),
        // ScalarIndex tests
        .testTarget(
            name: "ScalarIndexTests",
            dependencies: [
                "ScalarIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Vector", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                "DatabaseRuntime",
                "OntologyIndex",
                "QueryAST",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseValueCodable", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "Geospatial", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // RelationshipIndex tests
        .testTarget(
            name: "RelationshipIndexTests",
            dependencies: [
                "RelationshipIndex",
                "DatabaseRuntime",
                "DatabaseEngine",
                "ScalarIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "FDBStorage", package: "storage-kit",
                         condition: .when(traits: ["FoundationDB"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // DatabaseServer tests
        .testTarget(
            name: "DatabaseServerTests",
            dependencies: [
                "DatabaseServer",
                "DatabaseRuntime",
                "DatabaseEngine",
                "GraphIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseValueCodable", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ]
        ),
        // Database integration tests (SPARQL() function, etc.)
        .testTarget(
            name: "DatabaseTests",
            dependencies: [
                "Database",
                "DatabaseRuntime",
                "DatabaseEngine",
                "GraphIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseValueCodable", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
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
                "DatabaseEngine",
                "DatabaseRuntime",
                "TestSupport",
                "ScalarIndex",
                "RankIndex",
                "AggregationIndex",
                "BitmapIndex",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            path: "Benchmarks",
            swiftSettings: [
                .define("FOUNDATION_DB", .when(traits: ["FoundationDB"])),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        // PostgreSQL backend tests (requires running PostgreSQL, no libfdb_c required)
        .testTarget(
            name: "PostgreSQLTests",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                "ScalarIndex",
                "GraphIndex",
                "TestSupport",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "PostgreSQLStorage", package: "storage-kit",
                         condition: .when(traits: ["PostgreSQL"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("POSTGRESQL", .when(traits: ["PostgreSQL"])),
            ]
        ),
        // SQLite backend tests (no libfdb_c required)
        .testTarget(
            name: "FDBiteTests",
            dependencies: [
                "Database",
                "DatabaseRuntime",
                .product(name: "Core", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("SQLITE", .when(traits: ["SQLite"])),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
