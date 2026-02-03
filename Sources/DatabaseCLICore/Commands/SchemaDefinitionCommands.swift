// SchemaDefinitionCommands.swift
// Schema definition management commands

import Foundation
import DatabaseEngine
import Core
import FoundationDB

public struct SchemaDefinitionCommands {
    private let database: any DatabaseProtocol
    private let output: OutputFormatter

    public init(database: any DatabaseProtocol, output: OutputFormatter) {
        self.database = database
        self.output = output
    }

    // MARK: - Apply (Register Schema)

    public func apply(fileOrDirectory path: String) async throws {
        let url = URL(fileURLWithPath: path)
        var isDirectory: ObjCBool = false

        guard FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory) else {
            throw CLIError.invalidArguments("Path does not exist: \(path)")
        }

        if isDirectory.boolValue {
            try await applyDirectory(url)
        } else {
            try await applyFile(url)
        }
    }

    private func applyFile(_ fileURL: URL) async throws {
        output.info("Applying schema from: \(fileURL.lastPathComponent)")

        let catalog = try SchemaFileParser.parseYAML(from: fileURL)
        let registry = SchemaRegistry(database: database)

        try await registry.persist(catalog)

        output.success("✓ Schema '\(catalog.typeName)' registered successfully")
        output.info("  - \(catalog.fields.count) fields")
        output.info("  - \(catalog.indexes.count) indexes")

        let directoryPath = catalog.directoryComponents.map { component in
            switch component {
            case .staticPath(let path): return path
            case .dynamicField(let fieldName): return "{\(fieldName)}"
            }
        }.joined(separator: "/")
        output.info("  - Directory: /\(directoryPath)")
    }

    private func applyDirectory(_ directoryURL: URL) async throws {
        let fileManager = FileManager.default
        let files = try fileManager.contentsOfDirectory(at: directoryURL, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "yaml" || $0.pathExtension == "yml" }

        if files.isEmpty {
            throw CLIError.invalidArguments("No YAML files found in directory: \(directoryURL.path)")
        }

        output.info("Applying schemas from: \(directoryURL.lastPathComponent)")

        for fileURL in files.sorted(by: { $0.lastPathComponent < $1.lastPathComponent }) {
            do {
                try await applyFile(fileURL)
            } catch {
                output.error("Failed to apply \(fileURL.lastPathComponent): \(error)")
            }
        }
    }

    // MARK: - Export

    public func export(typeName: String, outputPath: String?) async throws {
        let registry = SchemaRegistry(database: database)

        guard let catalog = try await registry.load(typeName: typeName) else {
            throw CLIError.invalidArguments("Type '\(typeName)' not found in catalog")
        }

        let yaml = try SchemaFileExporter.toYAML(catalog)

        if let outputPath = outputPath {
            let url = URL(fileURLWithPath: outputPath)
            try yaml.write(to: url, atomically: true, encoding: .utf8)
            output.success("✓ Exported '\(typeName)' to \(outputPath)")
        } else {
            print(yaml)
        }
    }

    public func exportAll(outputDirectory: String?) async throws {
        let registry = SchemaRegistry(database: database)
        let catalogs = try await registry.loadAll()

        if catalogs.isEmpty {
            output.info("No schemas found in catalog")
            return
        }

        if let outputDirectory = outputDirectory {
            let dirURL = URL(fileURLWithPath: outputDirectory)
            try FileManager.default.createDirectory(at: dirURL, withIntermediateDirectories: true)

            for catalog in catalogs {
                let yaml = try SchemaFileExporter.toYAML(catalog)
                let fileURL = dirURL.appendingPathComponent("\(catalog.typeName).yaml")
                try yaml.write(to: fileURL, atomically: true, encoding: .utf8)
                output.info("Exported '\(catalog.typeName)'")
            }

            output.success("✓ Exported \(catalogs.count) schemas to \(outputDirectory)")
        } else {
            for catalog in catalogs {
                let yaml = try SchemaFileExporter.toYAML(catalog)
                output.info("--- \(catalog.typeName) ---")
                print(yaml)
            }
        }
    }

    // MARK: - Validate

    /// Validate schema file without database connection (static method)
    public static func validate(filePath: String, output: OutputFormatter) throws {
        let url = URL(fileURLWithPath: filePath)

        do {
            let catalog = try SchemaFileParser.parseYAML(from: url)

            output.success("✓ Schema '\(catalog.typeName)' is valid")
            output.info("  - \(catalog.fields.count) fields defined")
            output.info("  - \(catalog.indexes.count) indexes defined")

            if !catalog.directoryComponents.isEmpty {
                let path = catalog.directoryComponents.map { component in
                    switch component {
                    case .staticPath(let path): return path
                    case .dynamicField(let fieldName): return "{\(fieldName)}"
                    }
                }.joined(separator: "/")
                output.info("  - Directory: /\(path)")
            }

            if catalog.hasDynamicDirectory {
                output.info("  - Uses dynamic directory (partition-aware)")
            }

        } catch {
            output.error("✗ Schema validation failed:")
            output.error("  \(error)")
            throw error
        }
    }

    // MARK: - Drop

    public func drop(typeName: String, force: Bool) async throws {
        if !force {
            output.info("This will permanently delete the schema for '\(typeName)'")
            output.info("Use --force to confirm")
            return
        }

        let registry = SchemaRegistry(database: database)
        try await registry.delete(typeName: typeName)

        output.success("✓ Schema '\(typeName)' dropped")
    }

    public func dropAll(force: Bool) async throws {
        if !force {
            output.info("This will permanently delete ALL schemas")
            output.info("Use --force to confirm")
            return
        }

        let registry = SchemaRegistry(database: database)
        let catalogs = try await registry.loadAll()

        for catalog in catalogs {
            try await registry.delete(typeName: catalog.typeName)
            output.info("Dropped '\(catalog.typeName)'")
        }

        output.success("✓ Dropped \(catalogs.count) schemas")
    }
}
