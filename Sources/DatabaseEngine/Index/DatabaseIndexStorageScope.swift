import DatabaseKit
import DatabaseTypes

/// Locates the physical store that owns one index generation.
///
/// The scope retains the source directory contract so cleanup remains
/// executable after the declaring entity or polymorphic group is removed from
/// the published schema.
@_spi(DatabaseExecution)
public enum DatabaseIndexStorageScope: Sendable, Hashable {
    case entity(
        name: String,
        directoryComponents: [DirectoryPathComponent]
    )
    case polymorphicGroup(
        identifier: String,
        directoryPath: [String]
    )

    public var usesDynamicDirectory: Bool {
        guard case .entity(_, let components) = self else {
            return false
        }
        return components.contains { component in
            if case .dynamicField = component { return true }
            return false
        }
    }

    /// Whether a partition catalog entry belongs to this directory contract.
    public func accepts(partitions: FieldObject) -> Bool {
        switch self {
        case .entity(_, let components):
            let required = Set(components.compactMap { component in
                if case .dynamicField(let name) = component { return name }
                return nil
            })
            return Set(partitions.fields.map { $0.key }) == required
        case .polymorphicGroup:
            return partitions.isEmpty
        }
    }

    /// Stable structural ordering used by transition plans and durable host
    /// checkpoints. Length framing prevents component-boundary collisions.
    public var stableOrderingKey: String {
        func frame(_ value: String) -> String {
            "\(value.utf8.count):\(value)"
        }
        switch self {
        case .entity(let name, let components):
            var key = "0\(frame(name))"
            for component in components {
                switch component {
                case .staticPath(let value):
                    key += "0\(frame(value))"
                case .dynamicField(let name):
                    key += "1\(frame(name))"
                }
            }
            return key
        case .polymorphicGroup(let identifier, let path):
            return "1\(frame(identifier))"
                + path.map { "0\(frame($0))" }.joined()
        }
    }

    package func validate() throws(DatabaseIndexStorageScopeError) {
        switch self {
        case .entity(let name, let components):
            guard !name.isEmpty else {
                throw DatabaseIndexStorageScopeError.emptyIdentifier
            }
            var dynamicNames = Set<String>()
            for component in components {
                switch component {
                case .staticPath(let value):
                    guard !value.isEmpty else {
                        throw DatabaseIndexStorageScopeError
                            .emptyDirectoryComponent
                    }
                case .dynamicField(let name):
                    guard !name.isEmpty else {
                        throw DatabaseIndexStorageScopeError
                            .emptyDirectoryComponent
                    }
                    guard dynamicNames.insert(name).inserted else {
                        throw DatabaseIndexStorageScopeError
                            .duplicateDynamicDirectoryField(name)
                    }
                }
            }
        case .polymorphicGroup(let identifier, let path):
            guard !identifier.isEmpty else {
                throw DatabaseIndexStorageScopeError.emptyIdentifier
            }
            guard path.allSatisfy({ !$0.isEmpty }) else {
                throw DatabaseIndexStorageScopeError.emptyDirectoryComponent
            }
        }
    }
}

@_spi(DatabaseExecution)
public enum DatabaseIndexStorageScopeError: Error, Sendable, Equatable {
    case emptyIdentifier
    case emptyDirectoryComponent
    case duplicateDynamicDirectoryField(String)
}
