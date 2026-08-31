// swift-tools-version: 6.4
import PackageDescription

let databaseKitTraits: Set<Package.Dependency.Trait> = [
    .trait(
        name: "MultiBase",
        condition: .when(traits: ["MultiBase"])
    )
]

let nativeRuntimePlatforms: [Platform] = [
    .macOS,
    .iOS,
    .linux,
]

let foundationDBClientPlatforms: [Platform] = [
    .macOS,
    .linux,
]

let foundationDBClientLinkerSettings: [LinkerSetting] = [
    .unsafeFlags(
        ["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"],
        .when(platforms: [.macOS], traits: ["FoundationDB"])
    )
]

let package = Package(
    name: "database-framework",
    platforms: [
        .macOS(.v26),
        .iOS(.v26),
    ],
    products: [
        .library(name: "DatabaseMath", targets: ["DatabaseMath"]),
        .library(name: "DatabaseEngine", targets: ["DatabaseEngine"]),
        .library(
            name: "SwiftLogDatabaseLogging",
            targets: ["SwiftLogDatabaseLogging"]
        ),
        .library(
            name: "SwiftMetricsDatabaseMetrics",
            targets: ["SwiftMetricsDatabaseMetrics"]
        ),
        .library(name: "DatabaseRuntime", targets: ["DatabaseRuntime"]),
        .library(name: "ScalarIndex", targets: ["ScalarIndex"]),
        .library(name: "VectorIndex", targets: ["VectorIndex"]),
        .library(name: "FullTextIndex", targets: ["FullTextIndex"]),
        .library(name: "SpatialIndex", targets: ["SpatialIndex"]),
        .library(name: "RankIndex", targets: ["RankIndex"]),
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
    ],
    traits: [
        .trait(name: "FoundationDB"),
        .trait(name: "SQLite"),
        .trait(name: "PostgreSQL"),
        .trait(name: "MultiBase"),
        .trait(
            name: "AllRuntimeFeatures",
            enabledTraits: [
                "ScalarIndexes",
                "VectorIndexes",
                "FullTextIndexes",
                "SpatialIndexes",
                "RankIndexes",
                "BitmapIndexes",
                "VersionIndexes",
                "GraphIndexes",
                "AggregationIndexes",
                "LeaderboardIndexes",
                "Relationships",
            ]
        ),
        .trait(name: "ScalarIndexes"),
        .trait(name: "VectorIndexes"),
        .trait(name: "FullTextIndexes"),
        .trait(name: "SpatialIndexes"),
        .trait(name: "RankIndexes"),
        .trait(name: "BitmapIndexes"),
        .trait(name: "VersionIndexes"),
        .trait(name: "GraphIndexes", enabledTraits: ["ScalarIndexes"]),
        .trait(name: "AggregationIndexes"),
        .trait(name: "LeaderboardIndexes"),
        .trait(name: "Relationships"),
    ],
    dependencies: [
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0831.0"
        ),
        .package(
            url: "https://github.com/1amageek/database-kit.git",
            from: "26.0831.0",
            traits: databaseKitTraits
        ),
        .package(
            url: "https://github.com/1amageek/swift-hnsw.git",
            from: "1.2.0"
        ),
        .package(
            url: "https://github.com/1amageek/storage-kit.git",
            from: "26.0831.0"
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.7.0"),
        .package(
            url: "https://github.com/1amageek/swift-testing-heartbeat.git",
            revision: "74b9288fd7f21f575c06fd8b3b4bb7416321e5b0"
        ),
    ],
    targets: [
        .target(name: "DatabaseMath"),
        .target(
            name: "DatabaseEngine",
            dependencies: [
                "DatabaseMath",
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: [
                "README.md",
                "DESIGN.md",
                "Core/DESIGN.md",
                "Directory/DESIGN.md",
                "QueryExecution/DESIGN.md",
                "Read/DESIGN.md",
                "Serialization/DESIGN.md",
                "Serialization/RDF/DESIGN.md",
            ],
            swiftSettings: [
                .define(
                    "DATABASE_MULTI_BASE",
                    .when(traits: ["MultiBase"])
                )
            ]
        ),
        .target(
            name: "SwiftLogDatabaseLogging",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .target(
            name: "SwiftMetricsDatabaseMetrics",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Metrics", package: "swift-metrics"),
            ]
        ),
        .target(
            name: "ScalarIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "VectorIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "SwiftHNSW", package: "swift-hnsw"),
            ],
            exclude: [
                "README.md",
                "DESIGN.md",
                "IVF/DESIGN.md",
                "PQ/DESIGN.md",
            ]
        ),
        .target(
            name: "FullTextIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: [
                "README.md",
                "DESIGN.md",
                "Autocomplete/DESIGN.md",
                "Facet/DESIGN.md",
            ]
        ),
        .target(
            name: "DatabaseRuntime",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .target(
                    name: "ScalarIndex",
                    condition: .when(traits: ["ScalarIndexes"])
                ),
                .target(
                    name: "VectorIndex",
                    condition: .when(traits: ["VectorIndexes"])
                ),
                .target(
                    name: "FullTextIndex",
                    condition: .when(traits: ["FullTextIndexes"])
                ),
                .target(
                    name: "SpatialIndex",
                    condition: .when(traits: ["SpatialIndexes"])
                ),
                .target(
                    name: "RankIndex",
                    condition: .when(traits: ["RankIndexes"])
                ),
                .target(
                    name: "BitmapIndex",
                    condition: .when(traits: ["BitmapIndexes"])
                ),
                .target(
                    name: "VersionIndex",
                    condition: .when(traits: ["VersionIndexes"])
                ),
                .target(
                    name: "GraphIndex",
                    condition: .when(traits: ["GraphIndexes"])
                ),
                .target(
                    name: "OntologyIndex",
                    condition: .when(traits: ["GraphIndexes"])
                ),
                .target(
                    name: "AggregationIndex",
                    condition: .when(traits: ["AggregationIndexes"])
                ),
                .target(
                    name: "LeaderboardIndex",
                    condition: .when(traits: ["LeaderboardIndexes"])
                ),
                .target(
                    name: "RelationshipIndex",
                    condition: .when(traits: ["Relationships"])
                ),
            ],
            swiftSettings: [
                .define(
                    "DATABASE_RUNTIME_SCALAR_INDEXES",
                    .when(traits: ["ScalarIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_VECTOR_INDEXES",
                    .when(traits: ["VectorIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_FULL_TEXT_INDEXES",
                    .when(traits: ["FullTextIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_SPATIAL_INDEXES",
                    .when(traits: ["SpatialIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_RANK_INDEXES",
                    .when(traits: ["RankIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_BITMAP_INDEXES",
                    .when(traits: ["BitmapIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_VERSION_INDEXES",
                    .when(traits: ["VersionIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_GRAPH_INDEXES",
                    .when(traits: ["GraphIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_AGGREGATION_INDEXES",
                    .when(traits: ["AggregationIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_LEADERBOARD_INDEXES",
                    .when(traits: ["LeaderboardIndexes"])
                ),
                .define(
                    "DATABASE_RUNTIME_RELATIONSHIPS",
                    .when(traits: ["Relationships"])
                ),
            ]
        ),
        .target(
            name: "SpatialIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "RankIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitSystemClock", package: "storage-kit"),
            ],
            exclude: ["README.md", "DESIGN.md"]
        ),
        .target(
            name: "GraphIndex",
            dependencies: [
                "DatabaseMath",
                "ScalarIndex",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                "DatabaseEngine",
                "OntologyIndex",
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md", "DESIGN.md"],
            swiftSettings: [
                .define(
                    "DATABASE_MULTI_BASE",
                    .when(traits: ["MultiBase"])
                )
            ]
        ),
        .target(
            name: "AggregationIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "VersionIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md", "DESIGN.md"]
        ),
        .target(
            name: "BitmapIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md", "DESIGN.md"]
        ),
        .target(
            name: "LeaderboardIndex",
            dependencies: [
                "DatabaseMath",
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "OntologyIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
            ]
        ),
        .target(
            name: "RelationshipIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
            ],
            exclude: ["README.md"]
        ),
        // QueryIR is now provided by database-kit
        .target(
            name: "QueryAST",
            dependencies: [
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseKit", package: "database-kit"),
            ],
            exclude: ["README.md"]
        ),
        .target(
            name: "Database",
            dependencies: [
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                "DatabaseEngine",
                "DatabaseRuntime",
                .target(
                    name: "ScalarIndex",
                    condition: .when(traits: ["ScalarIndexes"])
                ),
                .target(
                    name: "VectorIndex",
                    condition: .when(traits: ["VectorIndexes"])
                ),
                .target(
                    name: "FullTextIndex",
                    condition: .when(traits: ["FullTextIndexes"])
                ),
                .target(
                    name: "SpatialIndex",
                    condition: .when(traits: ["SpatialIndexes"])
                ),
                .target(
                    name: "RankIndex",
                    condition: .when(traits: ["RankIndexes"])
                ),
                .target(
                    name: "GraphIndex",
                    condition: .when(traits: ["GraphIndexes"])
                ),
                .target(
                    name: "AggregationIndex",
                    condition: .when(traits: ["AggregationIndexes"])
                ),
                .target(
                    name: "VersionIndex",
                    condition: .when(traits: ["VersionIndexes"])
                ),
                .target(
                    name: "BitmapIndex",
                    condition: .when(traits: ["BitmapIndexes"])
                ),
                .target(
                    name: "LeaderboardIndex",
                    condition: .when(traits: ["LeaderboardIndexes"])
                ),
                .target(
                    name: "RelationshipIndex",
                    condition: .when(traits: ["Relationships"])
                ),
                .target(
                    name: "OntologyIndex",
                    condition: .when(traits: ["GraphIndexes"])
                ),
                "QueryAST",
                .product(name: "StorageKit", package: "storage-kit"),
                .product(
                    name: "FDBStorage", package: "storage-kit",
                    condition: .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .product(
                    name: "SQLiteStorage", package: "storage-kit",
                    condition: .when(platforms: nativeRuntimePlatforms, traits: ["SQLite"])),
                .product(
                    name: "PostgreSQLStorage", package: "storage-kit",
                    condition: .when(platforms: nativeRuntimePlatforms, traits: ["PostgreSQL"])),
            ],
            exclude: ["README.md", "DESIGN.md"],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .define("SQLITE", .when(platforms: nativeRuntimePlatforms, traits: ["SQLite"])),
                .define("POSTGRESQL", .when(platforms: nativeRuntimePlatforms, traits: ["PostgreSQL"])),
                .define(
                    "DATABASE_SCALAR_INDEXES",
                    .when(traits: ["ScalarIndexes"])
                ),
                .define(
                    "DATABASE_VECTOR_INDEXES",
                    .when(traits: ["VectorIndexes"])
                ),
                .define(
                    "DATABASE_FULL_TEXT_INDEXES",
                    .when(traits: ["FullTextIndexes"])
                ),
                .define(
                    "DATABASE_SPATIAL_INDEXES",
                    .when(traits: ["SpatialIndexes"])
                ),
                .define(
                    "DATABASE_RANK_INDEXES",
                    .when(traits: ["RankIndexes"])
                ),
                .define(
                    "DATABASE_GRAPH_INDEXES",
                    .when(traits: ["GraphIndexes"])
                ),
                .define(
                    "DATABASE_AGGREGATION_INDEXES",
                    .when(traits: ["AggregationIndexes"])
                ),
                .define(
                    "DATABASE_VERSION_INDEXES",
                    .when(traits: ["VersionIndexes"])
                ),
                .define(
                    "DATABASE_BITMAP_INDEXES",
                    .when(traits: ["BitmapIndexes"])
                ),
                .define(
                    "DATABASE_LEADERBOARD_INDEXES",
                    .when(traits: ["LeaderboardIndexes"])
                ),
                .define(
                    "DATABASE_RELATIONSHIPS",
                    .when(traits: ["Relationships"])
                ),
                .define(
                    "DATABASE_MULTI_BASE",
                    .when(traits: ["MultiBase"])
                ),
            ]
        ),
        // Test Support (shared test utilities)
        .target(
            name: "TestSupport",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                "ScalarIndex",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(
                    name: "FDBStorage", package: "storage-kit",
                    condition: .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .product(
                    name: "PostgreSQLStorage", package: "storage-kit",
                    condition: .when(traits: ["PostgreSQL"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            path: "Tests/Shared",
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .define("POSTGRESQL", .when(traits: ["PostgreSQL"])),
            ]
        ),
        // Core engine tests
        .testTarget(
            name: "DatabaseEngineTests",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                .target(
                    name: "Database",
                    condition: .when(
                        platforms: foundationDBClientPlatforms,
                        traits: ["FoundationDB"]
                    )
                ),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(
                    name: "DatabaseKitFoundation",
                    package: "database-kit",
                    condition: .when(platforms: nativeRuntimePlatforms)
                ),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitSystemClock", package: "storage-kit"),
                .target(name: "ScalarIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "VectorIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "FullTextIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "SpatialIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "RankIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "AggregationIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "VersionIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "RelationshipIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "BitmapIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "LeaderboardIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "GraphIndex", condition: .when(platforms: nativeRuntimePlatforms)),
                .target(name: "TestSupport", condition: .when(platforms: nativeRuntimePlatforms)),
                .product(
                    name: "DatabaseKit", package: "database-kit", condition: .when(platforms: nativeRuntimePlatforms)),
                .product(name: "Logging", package: "swift-log", condition: .when(platforms: nativeRuntimePlatforms)),
                .product(
                    name: "TestHeartbeat", package: "swift-testing-heartbeat",
                    condition: .when(platforms: nativeRuntimePlatforms)),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        .testTarget(
            name: "DatabaseEngineTransactionTests",
            dependencies: [
                "DatabaseEngine",
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "StorageKitSystemClock", package: "storage-kit"),
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        .testTarget(
            name: "DatabaseRuntimeTests",
            dependencies: [
                "DatabaseRuntime",
                "DatabaseEngine",
                "ScalarIndex",
                "VectorIndex",
                "RelationshipIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .define(
                    "DATABASE_RUNTIME_TEST_GRAPH_INDEXES",
                    .when(traits: ["GraphIndexes"])
                ),
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // ScalarIndex tests
        .testTarget(
            name: "ScalarIndexTests",
            dependencies: [
                "ScalarIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // VectorIndex tests
        .testTarget(
            name: "VectorIndexTests",
            dependencies: [
                "VectorIndex",
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseKitFoundation", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
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
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseKitFoundation", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .define(
                    "DATABASE_RUNTIME_TEST_GRAPH_INDEXES",
                    .when(traits: ["GraphIndexes"])
                ),
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // AggregationIndex tests (Count, Sum, Min, Max)
        .testTarget(
            name: "AggregationIndexTests",
            dependencies: [
                "AggregationIndex",
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(
                    name: "DatabaseKitFoundation",
                    package: "database-kit"
                ),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // VersionIndex tests
        .testTarget(
            name: "VersionIndexTests",
            dependencies: [
                "VersionIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // SpatialIndex tests
        .testTarget(
            name: "SpatialIndexTests",
            dependencies: [
                "SpatialIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // RankIndex tests
        .testTarget(
            name: "RankIndexTests",
            dependencies: [
                "RankIndex",
                "DatabaseRuntime",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // FullTextIndex tests
        .testTarget(
            name: "FullTextIndexTests",
            dependencies: [
                "FullTextIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // BitmapIndex tests
        .testTarget(
            name: "BitmapIndexTests",
            dependencies: [
                "BitmapIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // LeaderboardIndex tests
        .testTarget(
            name: "LeaderboardIndexTests",
            dependencies: [
                "LeaderboardIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
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
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(
                    name: "FDBStorage", package: "storage-kit",
                    condition: .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // QueryAST tests
        .testTarget(
            name: "QueryASTTests",
            dependencies: [
                "QueryAST",
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ]
        ),
        // QueryIR tests (SQL/SPARQL escape, Expression operators)
        .testTarget(
            name: "QueryIRTests",
            dependencies: [
                .product(name: "DatabaseKit", package: "database-kit"),
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
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "DatabaseTypes", package: "database-types"),
                .product(name: "DatabaseKitFoundation", package: "database-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("FOUNDATION_DB", .when(platforms: foundationDBClientPlatforms, traits: ["FoundationDB"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
        // PostgreSQL backend tests (requires running PostgreSQL, no libfdb_c required)
        .testTarget(
            name: "PostgreSQLTests",
            dependencies: [
                "DatabaseEngine",
                "DatabaseRuntime",
                "FullTextIndex",
                "ScalarIndex",
                "GraphIndex",
                "TestSupport",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(
                    name: "PostgreSQLStorage", package: "storage-kit",
                    condition: .when(traits: ["PostgreSQL"])),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
            ],
            swiftSettings: [
                .define("POSTGRESQL", .when(traits: ["PostgreSQL"]))
            ]
        ),
        // SQLite backend tests. The harness enables the SQLite trait in an
        // isolated package copy; the package itself has no default backend.
        .testTarget(
            name: "SQLiteTests",
            dependencies: [
                "Database",
                "DatabaseEngine",
                "DatabaseRuntime",
                .product(name: "DatabaseKit", package: "database-kit"),
                .product(
                    name: "DatabaseKitFoundation",
                    package: "database-kit"
                ),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "StorageKit", package: "storage-kit"),
                .product(name: "TestHeartbeat", package: "swift-testing-heartbeat"),
                "TestSupport",
            ],
            swiftSettings: [
                .define("SQLITE", .when(traits: ["SQLite"]))
            ],
            linkerSettings: foundationDBClientLinkerSettings
        ),
    ],
    swiftLanguageModes: [.v6]
)
