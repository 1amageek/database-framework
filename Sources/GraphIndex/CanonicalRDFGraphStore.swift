import DatabaseEngine
import DatabaseValue
import Graph
import StorageKit

/// Persistent authoritative RDF graph store backed by six canonical quad
/// indexes and an explicit empty-named-graph catalog.
public struct CanonicalRDFGraphStore: RDFGraphMutationStore {
    /// Database-wide v1 RDF graph-store namespace. This tuple prefix is part
    /// of the physical database format and therefore does not require an
    /// asynchronous directory lookup inside a caller-owned transaction.
    public static let defaultRootSubspace = Subspace(
        prefix: Tuple(["_database-framework", "rdf-graph-store", Int64(1)]).pack()
    )

    private enum ScanControl: Error {
        case logicalLimitReached
    }

    private enum CatalogRemoval {
        case none
        case key(Bytes)
        case range(begin: Bytes, end: Bytes)

        var requiresWrite: Bool {
            switch self {
            case .none: false
            case .key, .range: true
            }
        }
    }

    private static let emptyValue = Bytes()

    private let physicalCodec: RDFQuadIndexPhysicalCodec
    private let scanner: IndexedRDFDatasetScanner
    private let catalogCodec: RDFGraphCatalogCodec

    public let datasetSource: RDFDatasetSource

    public init() {
        self.init(rootSubspace: Self.defaultRootSubspace)
    }

    public init(rootSubspace: Subspace) {
        let quadSubspace = rootSubspace.subspace(Int64(1))
        let source = RDFDatasetSource(
            entityName: "$canonical-rdf-store",
            indexName: "$quads",
            indexSubspace: quadSubspace,
            coverage: .dataset
        )
        self.physicalCodec = RDFQuadIndexPhysicalCodec(
            baseSubspace: quadSubspace
        )
        self.datasetSource = source
        self.scanner = IndexedRDFDatasetScanner(sources: [source])
        self.catalogCodec = RDFGraphCatalogCodec(
            subspace: rootSubspace.subspace(Int64(2))
        )
    }

    public func scan(
        subject: DatabaseRDFTerm?,
        predicate: DatabaseRDFTerm?,
        object: DatabaseRDFTerm?,
        graphScope: RDFGraphScanScope,
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFDatasetScanResult {
        try await scanner.scan(
            subject: subject,
            predicate: predicate,
            object: object,
            graphScope: graphScope,
            limit: limit,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    public func namedGraphs(
        limit: Int?,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> [RDFGraphName] {
        if let limit, limit <= 0 { return [] }

        var graphs: [RDFGraphName] = []
        let range = catalogCodec.range
        let storageLimit = try workMeter.storageReadLimitWithSentinel()
        do {
            try await transaction.forEachInRange(
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                limit: storageLimit,
                snapshot: readMode.usesSnapshotReads,
                streamingMode: .iterator
            ) { key, value in
                try workMeter.consume(at: .storageRow)
                try catalogCodec.validateMarker(value)
                graphs.append(try catalogCodec.decodeGraph(from: key))
                if let limit, graphs.count >= limit {
                    throw ScanControl.logicalLimitReached
                }
            }
        } catch ScanControl.logicalLimitReached {
            // The requested logical page is complete.
        }
        graphs.sort()
        return graphs
    }

    public func containsGraph(
        _ graph: RDFGraphName,
        readMode: RDFDatasetReadMode,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let key = try catalogCodec.key(for: graph)
        try workMeter.consume(at: .storageRow)
        guard let marker = try await transaction.getValue(
            for: key,
            snapshot: readMode.usesSnapshotReads
        ) else {
            return false
        }
        try catalogCodec.validateMarker(marker)
        return true
    }

    public func createGraph(
        _ graph: RDFGraphName,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws {
        let key = try catalogCodec.key(for: graph)
        try workMeter.consume(at: .storageRow)
        if let marker = try await transaction.getValue(
            for: key,
            snapshot: false
        ) {
            try catalogCodec.validateMarker(marker)
            throw RDFGraphStoreError.graphAlreadyExists(graph)
        }
        if try await containsPhysicalQuad(
            in: graph,
            transaction: transaction,
            workMeter: workMeter
        ) {
            throw RDFGraphStoreError.missingCatalogForStoredQuad(graph)
        }
        try workMeter.consume(at: .storageWrite)
        try transaction.setValue(RDFGraphCatalogCodec.marker, for: key)
    }

    @discardableResult
    public func insert(
        _ quad: RDFQuad,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> RDFGraphInsertResult {
        let plan = try validatedPlan(for: quad, workMeter: workMeter)
        try validateKeySizes(plan)
        let primaryKey = try encode(plan.primaryEntry)
        try workMeter.consume(at: .storageRow)
        let alreadyExists = try await transaction.getValue(
            for: primaryKey,
            snapshot: false
        ) != nil

        var catalogKeyToInsert: Bytes?
        if let graphTerm = quad.graph {
            let graph = try RDFGraphName(graphTerm)
            catalogKeyToInsert = try await missingCatalogKeyAfterIntegrityCheck(
                for: graph,
                transaction: transaction,
                workMeter: workMeter
            )
            if alreadyExists, catalogKeyToInsert != nil {
                throw RDFGraphStoreError.missingCatalogForStoredQuad(graph)
            }
        }
        guard !alreadyExists else {
            return RDFGraphInsertResult(
                quadInserted: false,
                graphCreated: false
            )
        }

        let writeCount: UInt64 = catalogKeyToInsert == nil ? 6 : 7
        try workMeter.consume(writeCount, at: .storageWrite)
        if let catalogKeyToInsert {
            try transaction.setValue(
                RDFGraphCatalogCodec.marker,
                for: catalogKeyToInsert
            )
        }
        try write(
            plan,
            reusingPrimaryKey: primaryKey,
            transaction: transaction
        )
        return RDFGraphInsertResult(
            quadInserted: true,
            graphCreated: catalogKeyToInsert != nil
        )
    }

    @discardableResult
    public func delete(
        _ quad: RDFQuad,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let plan = try validatedPlan(for: quad, workMeter: workMeter)
        try validateKeySizes(plan)
        let primaryKey = try encode(plan.primaryEntry)
        try workMeter.consume(at: .storageRow)
        let quadExists = try await transaction.getValue(
            for: primaryKey,
            snapshot: false
        ) != nil

        if let graphTerm = quad.graph {
            let graph = try RDFGraphName(graphTerm)
            let missingCatalogKey = try await missingCatalogKeyAfterIntegrityCheck(
                for: graph,
                transaction: transaction,
                workMeter: workMeter
            )
            if quadExists, missingCatalogKey != nil {
                throw RDFGraphStoreError.missingCatalogForStoredQuad(graph)
            }
        }
        guard quadExists else { return false }

        try workMeter.consume(6, at: .storageWrite)
        try clear(
            plan,
            reusingPrimaryKey: primaryKey,
            transaction: transaction
        )
        return true
    }

    @discardableResult
    public func clear(
        _ scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        try await validateNamedGraphExists(
            for: scope,
            transaction: transaction,
            workMeter: workMeter
        )
        return try await clearPhysicalQuads(
            in: scope,
            transaction: transaction,
            workMeter: workMeter
        )
    }

    @discardableResult
    public func drop(
        _ scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        let catalogRemoval = try await catalogRemoval(
            for: scope,
            transaction: transaction,
            workMeter: workMeter
        )
        if catalogRemoval.requiresWrite {
            try workMeter.consume(at: .storageWrite)
        }
        let removed = try await clearPhysicalQuads(
            in: scope,
            transaction: transaction,
            workMeter: workMeter
        )
        switch catalogRemoval {
        case .none:
            break
        case .key(let key):
            try transaction.clear(key: key)
        case .range(let begin, let end):
            try transaction.clearRange(
                beginKey: begin,
                endKey: end
            )
        }
        return removed
    }

    private func validatedPlan(
        for quad: RDFQuad,
        workMeter: DatabaseWorkMeter
    ) throws -> RDFQuadIndexWritePlan {
        try workMeter.consume(at: .validation)
        do {
            try quad.validate()
        } catch let error as RDFDatasetValidationError {
            throw RDFGraphStoreError.invalidQuad(error)
        }
        do {
            return try RDFQuadIndexWritePlan(quad: quad)
        } catch let error {
            throw RDFGraphStoreError.invalidTermEncoding(error)
        }
    }

    private func missingCatalogKeyAfterIntegrityCheck(
        for graph: RDFGraphName,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bytes? {
        let key = try catalogCodec.key(for: graph)
        try workMeter.consume(at: .storageRow)
        if let marker = try await transaction.getValue(
            for: key,
            snapshot: false
        ) {
            try catalogCodec.validateMarker(marker)
            return nil
        }
        if try await containsPhysicalQuad(
            in: graph,
            transaction: transaction,
            workMeter: workMeter
        ) {
            throw RDFGraphStoreError.missingCatalogForStoredQuad(graph)
        }
        return key
    }

    /// Probes the authoritative graph-first index with a conflict-tracked,
    /// single-row range read. A missing catalog can only be treated as a new
    /// graph when this exact protected range is empty.
    private func containsPhysicalQuad(
        in graph: RDFGraphName,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        try workMeter.consume(at: .indexScan)
        let range = try physicalRange(for: .named(graph), ordering: .gspo)
        var found = false
        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: 1,
            snapshot: false,
            streamingMode: .exact
        ) { _, _ in
            try workMeter.consume(at: .storageRow)
            found = true
        }
        return found
    }

    private func validateNamedGraphExists(
        for scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws {
        guard case .named(let graph) = scope else { return }
        guard try await missingCatalogKeyAfterIntegrityCheck(
            for: graph,
            transaction: transaction,
            workMeter: workMeter
        ) == nil else {
            throw RDFGraphStoreError.graphNotFound(graph)
        }
    }

    private func catalogRemoval(
        for scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> CatalogRemoval {
        switch scope {
        case .defaultGraph:
            return .none
        case .named(let graph):
            guard try await missingCatalogKeyAfterIntegrityCheck(
                for: graph,
                transaction: transaction,
                workMeter: workMeter
            ) == nil else {
                throw RDFGraphStoreError.graphNotFound(graph)
            }
            return .key(try catalogCodec.key(for: graph))
        case .allNamedGraphs, .allGraphs:
            let range = catalogCodec.range
            return .range(begin: range.begin, end: range.end)
        }
    }

    private func write(
        _ plan: RDFQuadIndexWritePlan,
        reusingPrimaryKey primaryKey: Bytes,
        transaction: any Transaction
    ) throws {
        try plan.forEachEntry { entry in
            let key = entry.ordering == .spo
                ? primaryKey
                : try encode(entry)
            try transaction.setValue(Self.emptyValue, for: key)
        }
    }

    private func clear(
        _ plan: RDFQuadIndexWritePlan,
        reusingPrimaryKey primaryKey: Bytes,
        transaction: any Transaction
    ) throws {
        try plan.forEachEntry { entry in
            let key = entry.ordering == .spo
                ? primaryKey
                : try encode(entry)
            try transaction.clear(key: key)
        }
    }

    private func clearPhysicalQuads(
        in scope: RDFGraphMutationScope,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) async throws -> UInt64 {
        let scanRange = try physicalRange(for: scope, ordering: .gspo)
        let storageLimit = try workMeter.storageReadLimitWithSentinel()
        var removed: UInt64 = 0

        if scope == .allGraphs {
            try await transaction.forEachInRange(
                from: .firstGreaterOrEqual(scanRange.begin),
                to: .firstGreaterOrEqual(scanRange.end),
                limit: storageLimit,
                snapshot: false,
                streamingMode: .iterator
            ) { _, _ in
                try workMeter.consume(at: .storageRow)
                removed = try incremented(removed)
            }
            try clearPhysicalRanges(
                for: scope,
                includingTermFirstIndexes: true,
                transaction: transaction,
                workMeter: workMeter
            )
            return removed
        }

        try await transaction.forEachInRange(
            from: .firstGreaterOrEqual(scanRange.begin),
            to: .firstGreaterOrEqual(scanRange.end),
            limit: storageLimit,
            snapshot: false,
            streamingMode: .iterator
        ) { key, _ in
            try workMeter.consume(at: .storageRow)
            let encoded: RDFQuadIndexEncodedQuad
            do {
                encoded = try physicalCodec.decodeEncodedQuad(
                    key: key,
                    ordering: .gspo
                )
            } catch let error as RDFQuadIndexPhysicalCodecError {
                throw RDFGraphStoreError.invalidPhysicalIndex(error)
            }
            if scope == .allNamedGraphs, encoded.graph == nil {
                throw RDFGraphStoreError.invalidPhysicalIndex(
                    .invalidComponentCount(expected: 4...4, actual: 3)
                )
            }
            let plan: RDFQuadIndexWritePlan
            do {
                plan = try RDFQuadIndexWritePlan(encodedQuad: encoded)
            } catch let error as DatabaseRDFTermCodecError {
                throw RDFGraphStoreError.invalidTermEncoding(error)
            }
            try validateKeySizes(plan)
            try workMeter.consume(3, at: .storageWrite)
            try plan.forEachEntry { entry in
                guard !entry.ordering.isGraphFirst else { return }
                let termFirstKey = try encode(entry)
                try transaction.clear(key: termFirstKey)
            }
            removed = try incremented(removed)
        }

        try clearPhysicalRanges(
            for: scope,
            includingTermFirstIndexes: false,
            transaction: transaction,
            workMeter: workMeter
        )
        return removed
    }

    private func clearPhysicalRanges(
        for scope: RDFGraphMutationScope,
        includingTermFirstIndexes: Bool,
        transaction: any Transaction,
        workMeter: DatabaseWorkMeter
    ) throws {
        try workMeter.consume(
            includingTermFirstIndexes ? 6 : 3,
            at: .storageWrite
        )
        if includingTermFirstIndexes {
            try clearPhysicalRange(
                for: scope,
                ordering: .spo,
                transaction: transaction
            )
            try clearPhysicalRange(
                for: scope,
                ordering: .pos,
                transaction: transaction
            )
            try clearPhysicalRange(
                for: scope,
                ordering: .osp,
                transaction: transaction
            )
        }
        try clearPhysicalRange(
            for: scope,
            ordering: .gspo,
            transaction: transaction
        )
        try clearPhysicalRange(
            for: scope,
            ordering: .gpos,
            transaction: transaction
        )
        try clearPhysicalRange(
            for: scope,
            ordering: .gosp,
            transaction: transaction
        )
    }

    private func clearPhysicalRange(
        for scope: RDFGraphMutationScope,
        ordering: GraphIndexOrdering,
        transaction: any Transaction
    ) throws {
        let range = try physicalRange(for: scope, ordering: ordering)
        try transaction.clearRange(
            beginKey: range.begin,
            endKey: range.end
        )
    }

    private func physicalRange(
        for scope: RDFGraphMutationScope,
        ordering: GraphIndexOrdering
    ) throws -> (begin: Bytes, end: Bytes) {
        switch scope {
        case .allGraphs:
            do {
                return try physicalCodec.subspace(for: ordering).range()
            } catch let error {
                throw RDFGraphStoreError.invalidPhysicalIndex(error)
            }

        case .defaultGraph:
            return try graphPrefixRange(
                component: .defaultGraph,
                ordering: ordering
            )

        case .named(let graph):
            let component: RDFQuadIndexComponentWritePlan
            do {
                component = try RDFQuadIndexComponentWritePlan(
                    term: graph.term,
                    role: .graphName
                )
            } catch let error {
                throw RDFGraphStoreError.invalidTermEncoding(error)
            }
            return try graphPrefixRange(
                component: component,
                ordering: ordering
            )

        case .allNamedGraphs:
            let subspace: Subspace
            do {
                subspace = try physicalCodec.subspace(for: ordering)
            } catch let error {
                throw RDFGraphStoreError.invalidPhysicalIndex(error)
            }
            let defaultRange = try graphPrefixRange(
                component: .defaultGraph,
                ordering: ordering
            )
            return (begin: subspace.range().begin, end: defaultRange.begin)
        }
    }

    private func graphPrefixRange(
        component: RDFQuadIndexComponentWritePlan,
        ordering: GraphIndexOrdering
    ) throws -> (begin: Bytes, end: Bytes) {
        guard ordering.isGraphFirst else {
            do {
                return try physicalCodec.subspace(for: ordering).range()
            } catch let error {
                throw RDFGraphStoreError.invalidPhysicalIndex(error)
            }
        }
        var prefix = RDFQuadIndexPrefixWritePlan()
        do {
            try prefix.append(component)
            return try physicalCodec.range(prefix: prefix, ordering: ordering)
        } catch let error {
            throw RDFGraphStoreError.invalidPhysicalIndex(error)
        }
    }

    private func encode(
        _ entry: RDFQuadIndexEntryWritePlan
    ) throws -> Bytes {
        let keyByteCount: Int
        do {
            let tupleByteCount = try entry.encodedTupleByteCount()
            let subspace = try physicalCodec.subspace(for: entry.ordering)
            let (count, overflow) = subspace.prefix.count
                .addingReportingOverflow(tupleByteCount)
            guard !overflow else {
                throw RDFGraphStoreError.keyTooLarge(
                    actual: Int.max,
                    maximum: databaseMaximumKeySize
                )
            }
            keyByteCount = count
        } catch let error as RDFGraphStoreError {
            throw error
        } catch let error as RDFQuadIndexPhysicalCodecError {
            throw RDFGraphStoreError.invalidPhysicalIndex(error)
        }
        guard keyByteCount <= databaseMaximumKeySize else {
            throw RDFGraphStoreError.keyTooLarge(
                actual: keyByteCount,
                maximum: databaseMaximumKeySize
            )
        }
        let key: Bytes
        do {
            key = try physicalCodec.encode(entry)
        } catch let error {
            throw RDFGraphStoreError.invalidPhysicalIndex(error)
        }
        guard key.count <= databaseMaximumKeySize else {
            throw RDFGraphStoreError.keyTooLarge(
                actual: key.count,
                maximum: databaseMaximumKeySize
            )
        }
        assert(key.count == keyByteCount)
        return key
    }

    private func validateKeySizes(
        _ plan: RDFQuadIndexWritePlan
    ) throws {
        try plan.forEachEntry { entry in
            let tupleByteCount: Int
            let subspace: Subspace
            do {
                tupleByteCount = try entry.encodedTupleByteCount()
                subspace = try physicalCodec.subspace(for: entry.ordering)
            } catch let error as RDFQuadIndexPhysicalCodecError {
                throw RDFGraphStoreError.invalidPhysicalIndex(error)
            }
            let (keyByteCount, overflow) = subspace.prefix.count
                .addingReportingOverflow(tupleByteCount)
            guard !overflow, keyByteCount <= databaseMaximumKeySize else {
                throw RDFGraphStoreError.keyTooLarge(
                    actual: overflow ? Int.max : keyByteCount,
                    maximum: databaseMaximumKeySize
                )
            }
        }
    }

    private func incremented(_ value: UInt64) throws -> UInt64 {
        let (next, overflow) = value.addingReportingOverflow(1)
        guard !overflow else { throw RDFGraphStoreError.quadCountOverflow }
        return next
    }
}
