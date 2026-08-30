#if DATABASE_MULTI_BASE
import DatabaseTypes
import StorageKit

/// Bounded pre-order walk over the content of one Tenant Partition.
///
/// A placement move copies bytes between two storage domains, and the
/// Directory layout rules out copying one contiguous range and substituting its
/// leading prefix: a child edge is stored under a key that embeds its parent's
/// absolute prefix as a packed element, and its value embeds the child's own
/// absolute prefix. Rewriting the leading bytes of a key rewrites neither, so
/// the destination would inherit edges addressing the source. Content is
/// therefore copied node by node while the destination structure is created
/// through the catalog, which allocates and records its own prefixes.
///
/// The unit of the walk is one node's own content, which ``Directory/root``
/// already isolates. A plain node's children are allocated as siblings in
/// prefix space, and a Partition offsets its data root by one reserved byte
/// that sits below its nested layer's node subspace and above every Tuple type
/// code. For both kinds `root.range()` covers that node's content and nothing
/// else, so no caller has to know which kind it walked.
///
/// The walk assumes the tree does not change while it runs. A placement move
/// publishes the `moving` generation and drains every lease before the first
/// page, so the source is frozen for the whole transfer.
package enum DatabaseBaseTenantTransfer {
    /// One key-value pair of a node, keyed below that node's data root.
    package struct Row: Sendable {
        package let suffix: ByteString
        package let value: ByteString

        package init(suffix: ByteString, value: ByteString) {
            self.suffix = suffix
            self.value = value
        }
    }

    /// One visited node and the rows this page read from it.
    ///
    /// A node with no rows is still reported: an empty Directory is state, and
    /// dropping it would lose the structure the destination has to mirror.
    package struct Node: Sendable {
        /// Path below the Tenant Partition root. Empty addresses the Partition.
        package let path: [String]
        /// Layer tag of each component of ``path``, in the same order.
        package let layers: [LayerTag]
        package let rows: [Row]
        /// True when an earlier page already reported part of this node.
        ///
        /// A consumer that folds one record per node uses this to fold that
        /// record exactly once across the pages the node spans.
        package let resumed: Bool

        package init(
            path: [String],
            layers: [LayerTag],
            rows: [Row],
            resumed: Bool
        ) {
            self.path = path
            self.layers = layers
            self.rows = rows
            self.resumed = resumed
        }
    }

    /// One bounded page. A `nil` continuation means the walk is finished.
    package struct Page: Sendable {
        package let nodes: [Node]
        package let continuation: ByteString?

        package init(nodes: [Node], continuation: ByteString?) {
            self.nodes = nodes
            self.continuation = continuation
        }
    }

    // MARK: - Walking

    /// Reads at most `limit` rows and at most `limit` nodes, in pre-order.
    ///
    /// `continuation` resumes an earlier page exactly: a node it stops inside
    /// is re-reported with only the rows that remain, so no row is read twice
    /// and no node is skipped.
    package static func page(
        tenant: DatabaseTenantDirectories,
        access: any DirectoryAccess,
        continuation: ByteString?,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> Page {
        precondition(limit > 0, "A transfer page must admit at least one row")
        var position = try decodeCursor(continuation)
        var resumed = !position.suffix.isEmpty
        var nodes: [Node] = []
        var remainingRows = limit
        while true {
            let resolved = try await resolve(
                position.path,
                in: tenant,
                access: access,
                transaction: transaction
            )
            let read = try await readRows(
                from: resolved.directory,
                after: position.suffix,
                limit: remainingRows,
                transaction: transaction
            )
            nodes.append(
                Node(
                    path: position.path,
                    layers: resolved.layers,
                    rows: read.rows,
                    resumed: resumed
                )
            )
            remainingRows -= read.rows.count
            if let stopped = read.nextSuffix {
                return Page(
                    nodes: nodes,
                    continuation: encodeCursor(
                        path: position.path,
                        suffix: stopped
                    )
                )
            }
            guard let next = try await successor(
                of: position.path,
                directory: resolved.directory,
                in: tenant,
                access: access,
                transaction: transaction
            ) else {
                return Page(nodes: nodes, continuation: nil)
            }
            guard remainingRows > 0, nodes.count < limit else {
                return Page(
                    nodes: nodes,
                    continuation: encodeCursor(path: next, suffix: ByteString())
                )
            }
            position = Cursor(path: next, suffix: ByteString())
            resumed = false
        }
    }

    /// Opens the mirror of `path` below `tenant`, creating what is absent.
    ///
    /// `layers` types each created component, so a Partition leaf declared by
    /// the source is recreated as a Partition rather than a plain Directory.
    package static func resolveOrCreate(
        _ path: [String],
        layers: [LayerTag],
        in tenant: DatabaseTenantDirectories,
        access: any DirectoryAccess,
        transaction: any TransactionAccess
    ) async throws -> Directory {
        guard path.count == layers.count else {
            throw DatabaseBaseCatalogError.corruptedRecord(nil)
        }
        var current = tenant.partition.root
        for (name, layer) in zip(path, layers) {
            current = try await access.openOrCreate(
                name,
                layer: layer,
                in: current,
                transaction: transaction
            )
        }
        return current
    }

    // MARK: - Node content

    private static func readRows(
        from directory: Directory,
        after suffix: ByteString,
        limit: Int,
        transaction: any TransactionReadAccess
    ) async throws -> (rows: [Row], nextSuffix: ByteString?) {
        let root = directory.root
        let range = root.range()
        let begin: KeySelector = suffix.isEmpty
            ? .firstGreaterOrEqual(range.begin)
            : .firstGreaterThan(root.prefix.appending(contentsOf: suffix))
        let collected = try await TransactionRangeCollection.collect(
            using: transaction,
            from: begin,
            to: .firstGreaterOrEqual(range.end),
            limit: limit + 1,
            reverse: false,
            snapshot: false,
            streamingMode: .iterator
        )
        var rows: [Row] = []
        rows.reserveCapacity(min(collected.count, limit))
        for (key, value) in collected.prefix(limit) {
            guard root.contains(key) else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            let bounds = (key.startIndex + root.prefix.count)..<key.endIndex
            rows.append(Row(suffix: key[bounds], value: value))
        }
        let nextSuffix = collected.count > limit ? rows.last?.suffix : nil
        return (rows, nextSuffix)
    }

    // MARK: - Structure

    private static func resolve(
        _ path: [String],
        in tenant: DatabaseTenantDirectories,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> (directory: Directory, layers: [LayerTag]) {
        var current = tenant.partition.root
        var layers: [LayerTag] = []
        layers.reserveCapacity(path.count)
        for name in path {
            guard let next = try await access.open(
                name,
                expecting: nil,
                in: current,
                transaction: transaction
            ) else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            layers.append(next.layer)
            current = next
        }
        return (current, layers)
    }

    /// Pre-order successor: the first child, otherwise the nearest following
    /// sibling of the node or of one of its ancestors.
    private static func successor(
        of path: [String],
        directory: Directory,
        in tenant: DatabaseTenantDirectories,
        access: any DirectoryAccess,
        transaction: any TransactionReadAccess
    ) async throws -> [String]? {
        if let first = try await access.listChildren(
            in: directory,
            after: nil,
            limit: 1,
            transaction: transaction
        ).first {
            return path + [first.name]
        }
        var ancestors = path
        while let name = ancestors.last {
            ancestors.removeLast()
            let parent = try await resolve(
                ancestors,
                in: tenant,
                access: access,
                transaction: transaction
            )
            if let next = try await access.listChildren(
                in: parent.directory,
                after: name,
                limit: 1,
                transaction: transaction
            ).first {
                return ancestors + [next.name]
            }
        }
        return nil
    }

    // MARK: - Cursor

    private struct Cursor {
        var path: [String]
        /// Suffix of the last row already read from `path`; empty starts it.
        var suffix: ByteString
    }

    private static func encodeCursor(
        path: [String],
        suffix: ByteString
    ) -> ByteString {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(path.count + 1)
        for name in path {
            elements.append(name)
        }
        elements.append(suffix)
        return Tuple(elements).pack()
    }

    /// A cursor is Framework-written state, so any shape this cannot read is
    /// corruption rather than an alternative encoding to tolerate.
    private static func decodeCursor(
        _ bytes: ByteString?
    ) throws(DatabaseBaseCatalogError) -> Cursor {
        guard let bytes else {
            return Cursor(path: [], suffix: ByteString())
        }
        do {
            let tuple = try Tuple(packed: bytes)
            guard tuple.count >= 1 else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            var path: [String] = []
            path.reserveCapacity(tuple.count - 1)
            for index in 0..<(tuple.count - 1) {
                guard case .string(let name) = try tuple.value(at: index) else {
                    throw DatabaseBaseCatalogError.corruptedRecord(nil)
                }
                path.append(name)
            }
            guard case .bytes(let suffix) = try tuple.value(
                at: tuple.count - 1
            ) else {
                throw DatabaseBaseCatalogError.corruptedRecord(nil)
            }
            return Cursor(path: path, suffix: suffix)
        } catch {
            throw DatabaseBaseCatalogError.corruptedRecord(nil)
        }
    }
}

#endif
