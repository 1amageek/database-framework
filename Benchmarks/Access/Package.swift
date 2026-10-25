// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "database-framework-access-measurements",
    platforms: [.macOS("27.0")],
    products: [.executable(name: "DatabaseAccessPerformanceContract", targets: ["DatabaseAccessPerformanceContract"])],
    dependencies: [
        .package(path: "../..", traits: ["SQLite", "ScalarIndexes"]),
        .package(url: "https://github.com/1amageek/database-client.git", from: "26.1026.0"),
        .package(url: "https://github.com/1amageek/database-kit.git", from: "26.1026.0"),
        .package(url: "https://github.com/1amageek/database-types.git", from: "26.1021.0"),
        .package(url: "https://github.com/1amageek/storage-kit.git", from: "26.1026.0"),
    ],
    targets: [
        .target(name: "AccessPerformanceProbe",
            exclude: ["DESIGN.md", "AllocationObservation.c", "SelfCheck.c"],
            publicHeadersPath: "include", cSettings: [.unsafeFlags(["-fno-builtin"])]),
        .executableTarget(name: "DatabaseAccessPerformanceContract", dependencies: [
            "AccessPerformanceProbe",
            .product(name: "Database", package: "database-framework"),
            .product(name: "DatabaseEmbedded", package: "database-framework"),
            .product(name: "DatabaseEngine", package: "database-framework"),
            .product(name: "DatabaseRuntime", package: "database-framework"),
            .product(name: "QueryAST", package: "database-framework"),
            .product(name: "DatabaseAccess", package: "database-client"),
            .product(name: "DatabaseClient", package: "database-client"),
            .product(name: "DatabaseKit", package: "database-kit"),
            .product(name: "DatabaseWire", package: "database-kit"),
            .product(name: "DatabaseTypes", package: "database-types"),
            .product(name: "StorageKit", package: "storage-kit"),
            .product(name: "StorageKitSystemClock", package: "storage-kit"),
            .product(name: "SQLiteStorage", package: "storage-kit"),
        ], exclude: ["DESIGN.md"], swiftSettings: [.define("SQLITE")]),
    ],
    swiftLanguageModes: [.v6]
)
