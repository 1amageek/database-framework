import DatabaseKit
import StorageKit

/// Binds a resolved application `#Directory` path to a Directory node below the
/// `data` Directory of one Tenant Partition.
///
/// Section 12.1 makes the two directions asymmetric, so they are separate
/// operations rather than one walk with a creation flag. A read never creates a
/// node and reports absence as an absent keyspace, which is what lets a query
/// against a directory that was never written return nothing instead of
/// failing. A write creates what is missing, and the Directory metadata it
/// records commits with the mutation that needed it.
///
/// Layers come from the schema rather than from the path. A position that some
/// declaration resolves as a Partition leaf is a Partition for every
/// declaration passing through it, so `DirectoryLayerTagMap` derives the layer
/// of a position once per schema generation. Both directions pass the expected
/// layer: without it a node stored under the wrong layer would be accepted as
/// the addressed one, and its content would be read from the wrong prefix,
/// because a Partition offsets its data root and a plain Directory does not.
package enum DatabaseDirectoryBinding {

    /// Opens the node `path` addresses below `data`, or `nil` when any
    /// component of it has never been created.
    package static func open(
        _ path: [String],
        layers: [DirectoryLayer],
        below data: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        precondition(
            layers.count == path.count,
            "A directory path is typed component by component"
        )
        var current = data
        for index in path.indices {
            guard let child = try await access.open(
                path[index],
                expecting: tag(layers[index]),
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = child
        }
        return current
    }

    /// Opens the node `path` addresses below `data` without verifying any
    /// layer, or `nil` when a component of it does not exist.
    ///
    /// Retirement walks a path recorded by a past schema generation. When the
    /// current schema no longer declares that path there is no declared layer
    /// to verify against, so this walk verifies nothing rather than asserting a
    /// layer it cannot derive. Addressing stays correct because the returned
    /// `Directory` computes its root from the layer the catalog stored.
    package static func openUnverified(
        _ path: [String],
        below data: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        var current = data
        for component in path {
            guard let child = try await access.open(
                component,
                expecting: nil,
                in: current,
                transaction: transaction
            ) else {
                return nil
            }
            current = child
        }
        return current
    }

    /// Opens the node `path` addresses below `data`, creating what is absent.
    package static func openOrCreate(
        _ path: [String],
        layers: [DirectoryLayer],
        below data: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        precondition(
            layers.count == path.count,
            "A directory path is typed component by component"
        )
        var current = data
        for index in path.indices {
            current = try await access.openOrCreate(
                path[index],
                layer: tag(layers[index]),
                in: current,
                transaction: transaction
            )
        }
        return current
    }

    /// Storage tag of a declared layer.
    ///
    /// The switch is exhaustive so a new declared layer cannot be mapped by
    /// omission onto the plain tag.
    package static func tag(_ layer: DirectoryLayer) -> LayerTag {
        switch layer {
        case .default: .default
        case .partition: .partition
        }
    }
}
