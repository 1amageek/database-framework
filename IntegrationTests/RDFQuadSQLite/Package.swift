// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "RDFQuadSQLiteIntegration",
    platforms: [
        .macOS(.v26),
    ],
    dependencies: [
        .package(
            path: "../..",
            traits: []
        ),
        .package(path: "../../../database-kit"),
        .package(
            path: "../../../storage-kit",
            traits: ["SQLite"]
        ),
    ],
    targets: [
        .testTarget(
            name: "RDFQuadSQLiteIntegrationTests",
            dependencies: [
                .product(
                    name: "DatabaseEngine",
                    package: "database-framework"
                ),
                .product(
                    name: "GraphIndex",
                    package: "database-framework"
                ),
                .product(name: "Core", package: "database-kit"),
                .product(name: "DatabaseValue", package: "database-kit"),
                .product(name: "DatabaseWire", package: "database-kit"),
                .product(name: "Graph", package: "database-kit"),
                .product(name: "QueryIR", package: "database-kit"),
                .product(name: "SQLiteStorage", package: "storage-kit"),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
