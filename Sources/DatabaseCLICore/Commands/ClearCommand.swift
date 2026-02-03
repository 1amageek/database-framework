/// ClearCommand - Clear all data for a type or all types
///
/// Destructive operation with two-phase safety:
/// 1. Interactive confirmation prompt (y/N)
/// 2. 10-second countdown before execution (Ctrl+C to abort)
///
/// Use `--force` to skip both safeguards.

import Foundation
import DatabaseEngine

public struct ClearCommand {

    private let dataAccess: CatalogDataAccess
    private let output: OutputFormatter

    public init(dataAccess: CatalogDataAccess, output: OutputFormatter) {
        self.dataAccess = dataAccess
        self.output = output
    }

    /// Usage: clear <TypeName> [--force] | clear --all [--force]
    public func execute(_ args: [String]) async throws {
        let force = args.contains("--force")
        let cleanArgs = args.filter { $0 != "--force" }

        if cleanArgs.first == "--all" {
            let catalogs = dataAccess.allCatalogs
            if catalogs.isEmpty {
                output.info("No registered types found.")
                return
            }
            output.info("The following types will be cleared:")
            for catalog in catalogs {
                let count = try await recordCount(for: catalog.typeName)
                output.info("  \(catalog.typeName): \(count) records")
            }
            guard try await confirmExecution(description: "ALL types", force: force) else {
                return
            }
            for catalog in catalogs {
                try await dataAccess.clearAll(typeName: catalog.typeName)
            }
            output.success("Cleared all types")
        } else {
            guard let typeName = cleanArgs.first else {
                throw CLIError.invalidArguments("Usage: clear <TypeName> [--force] | clear --all [--force]")
            }
            let count = try await recordCount(for: typeName)
            output.info("  \(typeName): \(count) records")
            guard try await confirmExecution(description: typeName, force: force) else {
                return
            }
            try await dataAccess.clearAll(typeName: typeName)
            output.success("Cleared all data for '\(typeName)'")
        }
    }

    // MARK: - Private

    /// Two-phase safety gate. Returns `true` if execution should proceed.
    private func confirmExecution(description: String, force: Bool) async throws -> Bool {
        if force { return true }

        // Phase 1: Interactive confirmation
        print("Delete ALL data for \(description)? (y/N): ", terminator: "")
        fflush(stdout)
        guard let answer = readLine()?.trimmingCharacters(in: .whitespaces).lowercased(),
              answer == "y" || answer == "yes" else {
            output.info("Aborted.")
            return false
        }

        // Phase 2: 10-second countdown
        output.info("Executing in 10 seconds... (Ctrl+C to abort)")
        for i in stride(from: 10, through: 1, by: -1) {
            print("\r  \(i)...", terminator: "")
            fflush(stdout)
            try await Task.sleep(for: .seconds(1))
        }
        print()

        return true
    }

    private func recordCount(for typeName: String) async throws -> Int {
        let records = try await dataAccess.findAll(typeName: typeName, limit: nil)
        return records.count
    }
}

// MARK: - Help

extension ClearCommand {
    static var helpText: String {
        """
        Clear Command (destructive):
          clear <TypeName>            Clear all data for a type
          clear --all                 Clear all data for all registered types
          clear <TypeName> --force    Skip confirmation and countdown
          clear --all --force         Skip confirmation and countdown

        Safety (without --force):
          1. Shows record counts and asks for confirmation (y/N)
          2. 10-second countdown before execution (Ctrl+C to abort)

        Examples:
          clear RDFTriple
          clear --all
          clear RDFTriple --force
        """
    }
}
