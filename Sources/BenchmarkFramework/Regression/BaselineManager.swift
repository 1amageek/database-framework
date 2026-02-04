import Foundation

public struct BaselineManager: Sendable {
    public init() {}

    /// Load baseline from file
    public static func loadBaseline(from path: String) throws -> ComparisonResult {
        try JSONReporter.readComparison(from: path)
    }

    /// Save baseline to file
    public static func saveBaseline(_ result: ComparisonResult, to path: String) throws {
        try JSONReporter.write(result, to: path)
    }

    /// Download baseline from GitHub Release
    /// Note: Requires `gh` CLI to be installed and authenticated
    public static func downloadFromGitHub(
        owner: String = "1amageek",
        repo: String = "database-framework",
        tag: String = "baseline",
        pattern: String = "*.json",
        destination: String = "."
    ) async throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "gh", "release", "download", tag,
            "--repo", "\(owner)/\(repo)",
            "--pattern", pattern,
            "--dir", destination
        ]

        try process.run()
        process.waitUntilExit()

        guard process.terminationStatus == 0 else {
            throw BaselineError.downloadFailed(
                "Failed to download baseline: exit code \(process.terminationStatus)"
            )
        }
    }

    /// Upload baseline to GitHub Release
    /// Note: Requires `gh` CLI to be installed and authenticated
    public static func uploadToGitHub(
        files: [String],
        owner: String = "1amageek",
        repo: String = "database-framework",
        tag: String = "baseline",
        clobber: Bool = true
    ) async throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")

        var args = [
            "gh", "release", "upload", tag
        ]
        args.append(contentsOf: files)
        args.append("--repo")
        args.append("\(owner)/\(repo)")
        if clobber {
            args.append("--clobber")
        }

        process.arguments = args

        try process.run()
        process.waitUntilExit()

        guard process.terminationStatus == 0 else {
            throw BaselineError.uploadFailed(
                "Failed to upload baseline: exit code \(process.terminationStatus)"
            )
        }
    }

    /// Create GitHub Release if it doesn't exist
    /// Note: Requires `gh` CLI to be installed and authenticated
    public static func ensureReleaseExists(
        owner: String = "1amageek",
        repo: String = "database-framework",
        tag: String = "baseline"
    ) async throws {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [
            "gh", "release", "create", tag,
            "--repo", "\(owner)/\(repo)",
            "--title", "Benchmark Baselines",
            "--notes", "Automatically managed baseline results for performance regression testing"
        ]

        try process.run()
        process.waitUntilExit()

        // Exit code 1 means release already exists, which is fine
        guard process.terminationStatus == 0 || process.terminationStatus == 1 else {
            throw BaselineError.releaseCreationFailed(
                "Failed to create release: exit code \(process.terminationStatus)"
            )
        }
    }
}

public enum BaselineError: Error, LocalizedError {
    case downloadFailed(String)
    case uploadFailed(String)
    case releaseCreationFailed(String)

    public var errorDescription: String? {
        switch self {
        case .downloadFailed(let message):
            return "Baseline download failed: \(message)"
        case .uploadFailed(let message):
            return "Baseline upload failed: \(message)"
        case .releaseCreationFailed(let message):
            return "Release creation failed: \(message)"
        }
    }
}
