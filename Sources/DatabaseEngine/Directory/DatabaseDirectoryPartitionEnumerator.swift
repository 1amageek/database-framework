import DatabaseKit
import DatabaseTypes
import StorageKit

/// Enumerates the directory partitions of one entity from the Directory
/// catalog.
///
/// Section 12.3 makes StorageKit's transactional Directory catalog the sole
/// authority for which nodes exist, so the set of partitions an entity has is
/// not recorded a second time: it is the set of nodes that its `#Directory`
/// declaration resolves to below `data`. Every static component is one fixed
/// child and every dynamic component is every child the catalog lists at that
/// position, so the enumeration is a bounded depth-first walk of that shape.
///
/// An entity whose declaration has no dynamic component has no partitions to
/// enumerate: its single directory is addressed by the declaration itself.
package enum DatabaseDirectoryPartitionEnumerator {
    /// Children requested from the catalog per round trip.
    private static let listChunk = 64

    /// Largest page a caller may request.
    package static let maximumPageSize = 256

    /// One page of the partitions `entity` currently has.
    ///
    /// A page ends after `limit` partitions or when the walk is finished, and
    /// its continuation is the path of the last partition it reported. Resuming
    /// re-descends that path, so a node removed between two pages resumes from
    /// its position rather than restarting the walk.
    package static func page(
        entity: Schema.Entity,
        layers: [DirectoryLayer],
        below data: Directory,
        access: any DirectoryAccess,
        continuation: ByteString?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> DatabasePartitionCatalogPage {
        guard limit > 0, limit <= maximumPageSize else {
            throw DatabasePartitionCatalogError.invalidPageLimit(
                actual: limit,
                maximum: maximumPageSize
            )
        }
        let components = entity.directoryComponents
        precondition(
            layers.count == components.count,
            "A directory path is typed component by component"
        )
        guard components.contains(where: { component in
            if case .dynamicField = component { return true }
            return false
        }) else {
            return DatabasePartitionCatalogPage(entries: [], continuation: nil)
        }

        let declaration = declarationBinding(
            entity: entity.name,
            components: components
        )
        var stack = try await descend(
            try decodeCursor(
                continuation,
                declaration: declaration,
                componentCount: components.count
            ),
            components: components,
            layers: layers,
            below: data,
            access: access,
            transaction: transaction
        )
        var entries: [DatabasePartitionCatalogItem] = []
        entries.reserveCapacity(limit)

        while !stack.isEmpty {
            let index = stack[stack.count - 1].index
            guard let step = try await advance(
                &stack[stack.count - 1],
                component: components[index],
                layer: layers[index],
                access: access,
                transaction: transaction
            ) else {
                stack.removeLast()
                continue
            }
            guard index + 1 < components.count else {
                entries.append(
                    DatabasePartitionCatalogItem(
                        entity: entity.name,
                        partitions: try partitions(
                            of: stack,
                            components: components,
                            entity: entity.name
                        )
                    )
                )
                guard entries.count < limit else {
                    return DatabasePartitionCatalogPage(
                        entries: entries,
                        continuation: encodeCursor(
                            stack.map(\.name),
                            declaration: declaration
                        )
                    )
                }
                continue
            }
            stack.append(Frame(parent: step, index: index + 1))
        }
        return DatabasePartitionCatalogPage(entries: entries, continuation: nil)
    }

    // MARK: - Walk state

    /// One level of the walk: the node whose children this level visits.
    ///
    /// `name` doubles as the catalog cursor of the level. For a dynamic level it
    /// is the last child visited, so listing resumes after it; for a static
    /// level it records that the single child it has was already visited.
    private struct Frame {
        let parent: Directory
        let index: Int
        var name: String?
        var buffer: [DirectoryEntry] = []
        var bufferIndex: Int = 0
        var exhausted: Bool = false
    }

    /// Visits the next child of `frame`, or `nil` once the level is finished.
    private static func advance(
        _ frame: inout Frame,
        component: DirectoryPathComponent,
        layer: DirectoryLayer,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> Directory? {
        let expected = DatabaseDirectoryBinding.tag(layer)
        switch component {
        case .staticPath(let value):
            guard frame.name == nil else { return nil }
            frame.name = value
            return try await access.open(
                value,
                expecting: expected,
                in: frame.parent,
                transaction: transaction
            )

        case .dynamicField:
            while true {
                if frame.bufferIndex < frame.buffer.count {
                    let entry = frame.buffer[frame.bufferIndex]
                    frame.bufferIndex += 1
                    frame.name = entry.name
                    // The catalog listed this child, so a `nil` here means it
                    // was removed after the listing. A child stored under
                    // another layer is reported by `open` rather than skipped.
                    guard let directory = try await access.open(
                        entry.name,
                        expecting: expected,
                        in: frame.parent,
                        transaction: transaction
                    ) else {
                        continue
                    }
                    return directory
                }
                guard !frame.exhausted else { return nil }
                let page = try await access.listChildren(
                    in: frame.parent,
                    after: frame.name,
                    limit: listChunk,
                    transaction: transaction
                )
                frame.exhausted = page.count < listChunk
                frame.buffer = page
                frame.bufferIndex = 0
                guard !page.isEmpty else { return nil }
            }
        }
    }

    /// Rebuilds the walk state a continuation describes.
    ///
    /// Every level of the recorded path is pushed with that path's component as
    /// its cursor, so the walk continues after it. Descending stops at the first
    /// component that no longer exists: that level still holds a valid cursor,
    /// and the levels below it describe a subtree that is gone.
    private static func descend(
        _ cursor: [String],
        components: [DirectoryPathComponent],
        layers: [DirectoryLayer],
        below data: Directory,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> [Frame] {
        guard !cursor.isEmpty else {
            return [Frame(parent: data, index: 0)]
        }
        var stack: [Frame] = []
        stack.reserveCapacity(cursor.count)
        var parent = data
        for index in cursor.indices {
            var frame = Frame(parent: parent, index: index)
            frame.name = cursor[index]
            stack.append(frame)
            guard let child = try await access.open(
                cursor[index],
                expecting: DatabaseDirectoryBinding.tag(layers[index]),
                in: parent,
                transaction: transaction
            ) else {
                return stack
            }
            parent = child
        }
        return stack
    }

    /// The dynamic component values the current walk position resolves.
    private static func partitions(
        of stack: [Frame],
        components: [DirectoryPathComponent],
        entity: String
    ) throws -> FieldObject {
        var fields: [(key: String, value: FieldValue)] = []
        for index in components.indices {
            guard case .dynamicField(let field) = components[index] else {
                continue
            }
            guard let name = stack[index].name else {
                throw DatabasePartitionCatalogError.invalidPartitions(
                    entity: entity,
                    reason: "the walk reported a leaf with an unresolved component"
                )
            }
            do {
                fields.append((key: field, value: try DirectoryComponentCodec.decode(name)))
            } catch {
                throw DatabasePartitionCatalogError.invalidPartitions(
                    entity: entity,
                    reason: "component '\(name)' is not a canonical field value"
                )
            }
        }
        do {
            return try FieldObject(fields)
        } catch {
            throw DatabasePartitionCatalogError.invalidPartitions(
                entity: entity,
                reason: "the declaration repeats a dynamic field"
            )
        }
    }

    // MARK: - Continuation

    /// Marker every cursor this walk produces carries.
    ///
    /// A continuation issued by a different encoding could decode into names of
    /// the right shape and resume the walk at a position it never described,
    /// silently skipping partitions. The marker makes such a value fail to
    /// decode instead.
    private static let cursorMarker = "directory-partition-v1"

    /// The declaration a cursor was issued for.
    ///
    /// The marker alone types the encoding, not the walk: two entities whose
    /// declarations have the same number of components accept each other's
    /// cursors, and a declaration whose components changed between two pages
    /// accepts a cursor describing a shape it no longer has. Either case
    /// resumes at a position the current walk never describes and reports the
    /// remainder of a different tree as this entity's partitions.
    ///
    /// The binding is the entity name and the component shape, packed as a
    /// tuple so a name cannot be confused with a component and a static value
    /// cannot be confused with a dynamic field of the same text.
    private static func declarationBinding(
        entity: String,
        components: [DirectoryPathComponent]
    ) -> ByteString {
        var elements: [any TupleElement] = [entity]
        elements.reserveCapacity(components.count * 2 + 1)
        for component in components {
            switch component {
            case .staticPath(let value):
                elements.append("s")
                elements.append(value)
            case .dynamicField(let name):
                elements.append("d")
                elements.append(name)
            }
        }
        return Tuple(elements).pack()
    }

    private static func encodeCursor(
        _ path: [String?],
        declaration: ByteString
    ) -> ByteString {
        var elements: [any TupleElement] = [cursorMarker, declaration]
        elements.reserveCapacity(path.count + 2)
        for name in path {
            elements.append(name ?? "")
        }
        return Tuple(elements).pack()
    }

    private static func decodeCursor(
        _ bytes: ByteString?,
        declaration: ByteString,
        componentCount: Int
    ) throws -> [String] {
        guard let bytes else { return [] }
        do {
            let tuple = try Tuple(packed: bytes)
            guard tuple.count == componentCount + 2,
                  case .string(let marker) = try tuple.value(at: 0),
                  marker == cursorMarker,
                  case .bytes(let issuer) = try tuple.value(at: 1),
                  issuer == declaration
            else {
                throw DatabasePartitionCatalogError.invalidContinuation
            }
            var path: [String] = []
            path.reserveCapacity(componentCount)
            for index in 2..<tuple.count {
                guard case .string(let name) = try tuple.value(at: index) else {
                    throw DatabasePartitionCatalogError.invalidContinuation
                }
                path.append(name)
            }
            return path
        } catch {
            throw DatabasePartitionCatalogError.invalidContinuation
        }
    }
}
