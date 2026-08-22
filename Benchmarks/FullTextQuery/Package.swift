// swift-tools-version: 6.4

import PackageDescription

let package = Package(
    name: "database-framework-fulltext-benchmarks",
    platforms: [.macOS(.v26)],
    dependencies: [
        .package(
            path: "../..",
            traits: ["SQLite", "FullTextIndexes"]
        )
    ],
    targets: [
        .testTarget(
            name: "FullTextQueryPerformanceBenchmarks",
            dependencies: [
                .product(
                    name: "Database",
                    package: "database-framework"
                )
            ]
        )
    ],
    swiftLanguageModes: [.v6]
)
