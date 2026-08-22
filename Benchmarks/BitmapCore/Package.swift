// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "database-framework-bitmap-benchmarks",
    platforms: [.macOS(.v26)],
    dependencies: [
        .package(path: "../..", traits: ["BitmapIndexes"]),
    ],
    targets: [
        .testTarget(
            name: "BitmapCorePerformanceBenchmarks",
            dependencies: [
                .product(name: "BitmapIndex", package: "database-framework"),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
