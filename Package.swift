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
        .library(name: "Database", targets: ["Database"]),
    ],
    dependencies: [
        .package(url: "https://github.com/1amageek/database-kit.git", branch: "main"),
        .package(url: "https://github.com/1amageek/swift-hnsw.git", branch: "main"),
        .package(
            url: "https://github.com/1amageek/fdb-swift-bindings.git",
            branch: "feature/directory-layer"
        ),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.7.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", from: "2.7.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "4.2.0"),
        .package(url: "https://github.com/apple/swift-configuration.git", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "DatabaseEngine",
            dependencies: [
                .product(name: "Core", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),  // For joining() support
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "Configuration", package: "swift-configuration"),
            ]
        ),
        .target(
            name: "ScalarIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
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
            ]
        ),
        .target(
            name: "FullTextIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FullText", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "SpatialIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Spatial", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "RankIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Rank", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "PermutedIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Permuted", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "GraphIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "AggregationIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "VersionIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "BitmapIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "LeaderboardIndex",
            dependencies: [
                "DatabaseEngine",
                .product(name: "Core", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "RelationshipIndex",
            dependencies: [
                "DatabaseEngine",
                "ScalarIndex",  // For FK index lookups
                .product(name: "Core", package: "database-kit"),
                .product(name: "Relationship", package: "database-kit"),
                .product(name: "FoundationDB", package: "fdb-swift-bindings"),
            ]
        ),
        .target(
            name: "Database",
            dependencies: [
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
                "RelationshipIndex",
                "BitmapIndex",
                "LeaderboardIndex",
                "GraphIndex",
                "TestSupport",
                .product(name: "Graph", package: "database-kit"),
            ],
            exclude: ["IndexTestDesign.md"],
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
    ],
    swiftLanguageModes: [.v6]
)
