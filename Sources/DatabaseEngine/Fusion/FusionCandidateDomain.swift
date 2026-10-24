import DatabaseKit
import DatabaseTypes
import StorageKit

/// Supplies candidate rows through a bounded borrow without coupling
/// candidate construction to one physical row owner.
private protocol FusionCandidateRowInput {
    var count: Int { get }
    var columns: [QueryColumn] { get }
    var payloadLeases: [DatabaseQueryPayloadLease] { get }
    var schemaLeases: [DatabaseIntermediateReservation] { get }

    func forEachRow(
        _ body: (borrowing ExecutionQueryRow) throws -> Void
    ) throws
}

private struct FusionCandidateCollectionRowInput<Rows: Collection>:
    FusionCandidateRowInput
where Rows.Element == ExecutionQueryRow {
    let rows: Rows
    let columns: [QueryColumn]
    var payloadLeases: [DatabaseQueryPayloadLease] { [] }
    var schemaLeases: [DatabaseIntermediateReservation] { [] }

    var count: Int { rows.count }

    func forEachRow(
        _ body: (borrowing ExecutionQueryRow) throws -> Void
    ) throws {
        for row in rows {
            try body(row)
        }
    }
}

private struct FusionCandidateScopedRowInput: FusionCandidateRowInput {
    let rows: CanonicalRetainedQueryRowView
    var columns: [QueryColumn] { rows.columns }
    var payloadLeases: [DatabaseQueryPayloadLease] { rows.originPayloadLeases }
    var schemaLeases: [DatabaseIntermediateReservation] { rows.schemaLeases }

    var count: Int { rows.count }

    func forEachRow(
        _ body: (borrowing ExecutionQueryRow) throws -> Void
    ) throws {
        for position in 0..<rows.count {
            try rows.withElement(at: position) { row in
                try body(row)
            }
        }
    }
}

/// Engine-owned immutable candidate rows for one Fusion stage.
///
/// Feature modules can inspect only canonical primary keys. Materialized rows,
/// identity values, and retained-storage ownership remain inside DatabaseEngine.
package struct FusionCandidateDomain: Sendable {
    struct Entry: Sendable {
        let identity: FieldValue
        let packedPrimaryKey: ByteString
        let row: ExecutionQueryRow
        let rowFootprint: DatabaseIntermediateFootprint
        let retainedFootprint: DatabaseIntermediateFootprint
        let payloadLeases: [DatabaseQueryPayloadLease]
    }

    private let entries: DatabaseSharedRetainedArray<Entry>
    let columns: [QueryColumn]
    private let entity: Schema.Entity
    private let payloadLeases: [DatabaseQueryPayloadLease]
    private let schemaLeases: [DatabaseIntermediateReservation]

    private init(
        entries: DatabaseSharedRetainedArray<Entry>, columns: [QueryColumn], entity: Schema.Entity,
        payloadLeases: [DatabaseQueryPayloadLease], schemaLeases: [DatabaseIntermediateReservation]
    ) {
        self.entries = entries
        self.columns = columns
        self.entity = entity
        self.payloadLeases = payloadLeases
        self.schemaLeases = schemaLeases
    }

    private init(entries: DatabaseSharedRetainedArray<Entry>, source: FusionCandidateDomain) {
        self.init(entries: entries, columns: source.columns, entity: source.entity,
                  payloadLeases: source.payloadLeases, schemaLeases: source.schemaLeases)
    }

    package var count: Int { entries.count }
    package var isEmpty: Bool { entries.isEmpty }

    var originPayloadLeases: [DatabaseQueryPayloadLease] {
        payloadLeases + [entries.payloadLease] + schemaLeases.map { DatabaseQueryPayloadLease(retaining: $0) }
    }

    package func primaryKey(at index: Int) -> ByteString {
        entries[index].packedPrimaryKey
    }

    func withEntry<Result>(
        at index: Int,
        _ body: (borrowing Entry) throws -> Result
    ) rethrows -> Result {
        try entries.withElement(at: index, body)
    }

    package func contains(
        primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try insertionIndex(
            for: primaryKey,
            workMeter: workMeter
        ).match != nil
    }

    func entry(
        for primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Entry? {
        guard let index = try insertionIndex(
            for: primaryKey,
            workMeter: workMeter
        ).match else {
            return nil
        }
        return entries[index]
    }

    func index(
        forPrimaryKey primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Int? {
        try insertionIndex(for: primaryKey, workMeter: workMeter).match
    }

    func forEachEntry(
        _ body: (borrowing Entry) throws -> Void
    ) rethrows {
        for index in entries.indices {
            try entries.withElement(at: index) { entry in
                try body(entry)
            }
        }
    }

    static func make<Rows: Collection>(
        rows: Rows,
        columns: [QueryColumn],
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain where Rows.Element == ExecutionQueryRow {
        try make(
            input: FusionCandidateCollectionRowInput(rows: rows, columns: columns),
            entity: entity,
            workMeter: workMeter,
            stage: stage
        )
    }

    static func make(
        rows: CanonicalRetainedQueryRowView,
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        return try make(
            input: FusionCandidateScopedRowInput(rows: rows),
            entity: entity,
            workMeter: workMeter,
            stage: stage
        )
    }

    private static func make<Input: FusionCandidateRowInput>(
        input: Input,
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> FusionCandidateDomain {
        let columns = input.columns
        let schemaLeases = input.schemaLeases.isEmpty
            ? [try admitColumns(columns, workMeter: workMeter, stage: stage)] : input.schemaLeases
        try validateColumns(columns)
        let identitySlot = try identitySlot(columns: columns)
        let payloadLeases = input.payloadLeases
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: input.count
        )
        try input.forEachRow { row in
            try workMeter.consume(at: stage)
            try row.validate(columns: columns)
            let identity = try identity(in: row, slot: identitySlot)
            let primaryKey = try PersistableIdentifierKeyCodec
                .tuple(forPersistedIdentifier: identity)
            _ = try PersistableIdentifierKeyCodec.value(
                from: primaryKey,
                expectedType: entity.identifierType
            )
            let packedPrimaryKeyByteCount = primaryKey.packedByteCount
            let rowFootprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter
            )
            let footprint = try rowFootprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(packedPrimaryKeyByteCount) + 64
                )
            )
            try builder.append(footprint: footprint, at: stage) {
                let packedPrimaryKey = primaryKey.pack()
                precondition(
                    packedPrimaryKey.count == packedPrimaryKeyByteCount
                )
                return Entry(
                    identity: identity,
                    packedPrimaryKey: packedPrimaryKey,
                    row: row,
                    rowFootprint: rowFootprint,
                    retainedFootprint: footprint,
                    payloadLeases: []
                )
            }
        }

        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        try sorted.withSpan { entries in
            guard entries.count > 1 else { return }
            for index in 1..<entries.count {
                try workMeter.consume(at: stage)
                let previous = entries[index - 1]
                let current = entries[index]
                guard try primaryKeyOrder(
                    previous.packedPrimaryKey,
                    current.packedPrimaryKey,
                    workMeter: workMeter,
                    stage: stage
                ) != 0 else {
                    if previous.identity == current.identity {
                        throw FusionExecutionContractError.duplicateIdentity(
                            current.identity
                        )
                    }
                    throw FusionExecutionContractError.duplicatePrimaryKey(
                        current.packedPrimaryKey
                    )
                }
            }
        }
        return FusionCandidateDomain(
            entries: try sorted.moveToSharedOwnership(at: stage), columns: columns, entity: entity,
            payloadLeases: payloadLeases, schemaLeases: schemaLeases
        )
    }

    static func make(
        selecting primaryKeys: some Collection<ByteString>,
        from source: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain {
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: primaryKeys.count
        )
        for primaryKey in primaryKeys {
            try workMeter.consume(at: stage)
            guard let entry = try source.entry(
                for: primaryKey,
                workMeter: workMeter
            ) else {
                throw FusionExecutionContractError.missingCandidateRow(primaryKey)
            }
            try builder.append(
                footprint: entry.retainedFootprint,
                at: stage,
                make: { entry }
            )
        }
        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        return FusionCandidateDomain(entries: try sorted.moveToSharedOwnership(
            at: stage
        ), source: source)
    }

    /// Relational SQL responses may carry an internal identity column absent
    /// from the admitted candidate projection. Select original entries so
    /// routing metadata never changes the domain's column contract.
    static func make(
        selecting rows: CanonicalRetainedQueryRowView,
        from source: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let identitySlot = try identitySlot(columns: rows.columns)
        var primaryKeys = try DatabaseRetainedArrayBuilder<ByteString>(workMeter: workMeter, stage: stage,
            layout: DatabaseRetainedArrayLayout.forElement(ByteString.self), expectedCount: rows.count)
        for position in 0..<rows.count {
            try workMeter.consume(at: stage)
            try rows.withElement(at: position) { row in
                try row.validate(columns: rows.columns)
                let identity = try identity(in: row, slot: identitySlot)
                let key = try PersistableIdentifierKeyCodec.tuple(forPersistedIdentifier: identity)
                let footprint = DatabaseIntermediateFootprint(bytes: UInt64(key.packedByteCount))
                try primaryKeys.append(footprint: footprint) { key.pack() }
            }
        }
        let selected = try primaryKeys.finish().moveToSharedOwnership(at: stage)
        return try make(selecting: selected, from: source, workMeter: workMeter, stage: stage)
    }

    static func make(
        models: DatabaseRetainedPersistedModels,
        primaryKeys: any DatabaseRetainedPrimaryKeyCollection,
        entity: Schema.Entity,
        admittedFieldNames: Set<String>? = nil,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        guard models.count == primaryKeys.count else {
            throw FusionExecutionContractError.entityReadCountMismatch(
                expected: primaryKeys.count,
                actual: models.count
            )
        }
        guard models.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        if let admittedFieldNames, !admittedFieldNames.allSatisfy({ entity.fieldMapByName[$0] != nil }) {
            throw FusionExecutionContractError.executionContractViolation
        }
        let declaredFields = entity.fields.lazy.filter { admittedFieldNames?.contains($0.name) ?? true }
        let schemaFootprint = try CanonicalRelationalFootprintMeter.sourceLayoutFootprint(
            fieldNames: declaredFields.map { $0.name }, sourceName: nil,
            workMeter: workMeter, stage: .storageRow
        )
        let columnBytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumn>.stride))
            .multiplied(by: UInt64(declaredFields.count))
        let schemaReservation = try workMeter.reserveIntermediate(
            bytes: schemaFootprint.adding(columnBytes).bytes, at: .storageRow
        )
        let columns = try declaredFields.enumerated().map { index, field in
            try QueryColumn(number: UInt32(index + 1), name: field.name,
                            type: field.queryType ?? .primitive(field.type, isArray: field.isArray),
                            isNullable: field.isOptional)
        }
        let layout = CanonicalSourceLayout(
            slots: declaredFields.map { .init(name: $0.name, scope: nil) }, reservation: schemaReservation,
            types: columns.map { $0.type }, nullability: columns.map { $0.isNullable }
        )
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: models.count
        )
        for index in 0..<primaryKeys.count {
            try workMeter.consume(at: .storageRow)
            try primaryKeys.withRetainedPrimaryKey(at: index) { primaryKey in
                let packedPrimaryKeyByteCount = primaryKey.packedByteCount
                try models.withEntry(at: index) { retained in
                    guard let retained else {
                        throw FusionExecutionContractError.missingCandidateRow(
                            primaryKey.pack()
                        )
                    }
                    var rowFootprint = DatabaseIntermediateFootprint()
                    try retained.withModel { model in
                        rowFootprint = try CanonicalRelationalFootprintMeter.sourceRowFootprint(
                            of: model, entity: entity, layout: layout, workMeter: workMeter, stage: .storageRow
                        )
                    }
                    let footprint = try rowFootprint.adding(DatabaseIntermediateFootprint(
                        bytes: UInt64(packedPrimaryKeyByteCount) + 64
                    ))
                    try builder.append(footprint: footprint) {
                        let packedPrimaryKey = primaryKey.pack()
                        precondition(
                            packedPrimaryKey.count == packedPrimaryKeyByteCount
                        )
                        var candidate: Entry?
                        try retained.withModel { model in
                            let values = try EntityRuntimeDefinition.canonicalSourceValues(
                                model, entity: entity, layout: layout, workMeter: workMeter, stage: .storageRow
                            )
                            let row = ExecutionQueryRow(validatedValues: values,
                                               version: try PersistableVersionTokenCodec.token(for: model.fields))
                            // Internal candidate routing does not require an
                            // identifier slot in the authorized projection.
                            guard let identity = model.value(forFieldNamed: "id"),
                                  identity != .null else {
                                throw FusionExecutionContractError
                                    .missingIdentity(field: "id")
                            }
                            let actualPrimaryKey = try
                                PersistableIdentifierKeyCodec
                                .tuple(forPersistedIdentifier: identity)
                            _ = try PersistableIdentifierKeyCodec.value(
                                from: actualPrimaryKey,
                                expectedType: entity.identifierType
                            )
                            // Structural comparison avoids materializing a
                            // second packed identifier while the retained key
                            // is alive.
                            guard actualPrimaryKey == primaryKey else {
                                throw FusionExecutionContractError
                                    .inconsistentPayload(packedPrimaryKey)
                            }
                            candidate = Entry(
                                identity: identity,
                                packedPrimaryKey: packedPrimaryKey,
                                row: row,
                                rowFootprint: rowFootprint,
                                retainedFootprint: footprint,
                                payloadLeases: [try retained.canonicalPayloadLease(workMeter: workMeter)]
                            )
                        }
                        guard let candidate else {
                            preconditionFailure(
                                "Scoped retained model did not produce a candidate"
                            )
                        }
                        return candidate
                    }
                }
            }
        }
        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        return FusionCandidateDomain(entries: try sorted.moveToSharedOwnership(
            at: .storageRow
        ), columns: columns, entity: entity, payloadLeases: [], schemaLeases: [schemaReservation])
    }

    func union(
        _ other: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        guard entity.name == other.entity.name, columns == other.columns else {
            throw FusionExecutionContractError.executionContractViolation
        }
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: try checkedCombinedCount(other)
        )
        var left = entries.startIndex
        var right = other.entries.startIndex
        while left < entries.endIndex || right < other.entries.endIndex {
            try workMeter.consume(at: .bindingCandidate)
            let selected: Entry
            if left == entries.endIndex {
                selected = other.entries[right]
                right += 1
            } else if right == other.entries.endIndex {
                selected = entries[left]
                left += 1
            } else {
                let lhs = entries[left]
                let rhs = other.entries[right]
                let order = try Self.primaryKeyOrder(
                    lhs.packedPrimaryKey,
                    rhs.packedPrimaryKey,
                    workMeter: workMeter,
                    stage: .bindingCandidate
                )
                if order == 0 {
                    try Self.requireEquivalentPayload(
                        lhs,
                        rhs,
                        workMeter: workMeter
                    )
                    selected = lhs
                    left += 1
                    right += 1
                } else if order < 0 {
                    selected = lhs
                    left += 1
                } else {
                    selected = rhs
                    right += 1
                }
            }
            try builder.append(
                footprint: selected.retainedFootprint,
                make: { selected }
            )
        }
        return FusionCandidateDomain(
            entries: try builder.finish().moveToSharedOwnership(
                at: .bindingCandidate
            ), columns: columns, entity: entity,
            payloadLeases: payloadLeases + other.payloadLeases + [entries.payloadLease, other.entries.payloadLease],
            schemaLeases: schemaLeases + other.schemaLeases
        )
    }

    func intersection(
        _ other: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        guard entity.name == other.entity.name, columns == other.columns else {
            throw FusionExecutionContractError.executionContractViolation
        }
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: min(count, other.count)
        )
        var left = entries.startIndex
        var right = other.entries.startIndex
        while left < entries.endIndex, right < other.entries.endIndex {
            try workMeter.consume(at: .bindingCandidate)
            let lhs = entries[left]
            let rhs = other.entries[right]
            let order = try Self.primaryKeyOrder(
                lhs.packedPrimaryKey,
                rhs.packedPrimaryKey,
                workMeter: workMeter,
                stage: .bindingCandidate
            )
            if order == 0 {
                try Self.requireEquivalentPayload(
                    lhs,
                    rhs,
                    workMeter: workMeter
                )
                try builder.append(
                    footprint: lhs.retainedFootprint,
                    make: { lhs }
                )
                left += 1
                right += 1
            } else if order < 0 {
                left += 1
            } else {
                right += 1
            }
        }
        return FusionCandidateDomain(
            entries: try builder.finish().moveToSharedOwnership(
                at: .bindingCandidate
            ), columns: columns, entity: entity,
            payloadLeases: payloadLeases + other.payloadLeases + [entries.payloadLease, other.entries.payloadLease],
            schemaLeases: schemaLeases + other.schemaLeases
        )
    }

    static func identitySlot(columns: [QueryColumn]) throws -> Int {
        guard let slot = columns.firstIndex(where: { $0.name == "id" }) else {
            throw FusionExecutionContractError.missingIdentity(field: "id")
        }
        guard columns.dropFirst(slot + 1).allSatisfy({ $0.name != "id" }) else {
            throw QueryRowCodecError.duplicateColumn("id")
        }
        return slot
    }

    static func identity(in row: borrowing ExecutionQueryRow, slot: Int) throws -> FieldValue {
        guard row.values.indices.contains(slot), case .primitive(let identity) = row.values[slot], identity != .null else {
            throw FusionExecutionContractError.missingIdentity(field: "id")
        }
        return identity
    }

    /// This is the explicit primitive persistence destination for a partial
    /// Fusion projection; fields outside its admitted columns are not added.
    func indexFields(for row: borrowing ExecutionQueryRow) throws -> [String: FieldValue] {
        try row.validate(columns: columns)
        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(columns.count)
        for (column, value) in zip(columns, row.values) {
            let descriptor = try persistenceDescriptor(for: column)
            fields[column.name] = try QueryFieldStorageAdapter.encode(
                value, type: descriptor,
                limits: QueryFieldStorageAdapter.inferredLimits(for: value, type: descriptor)
            )
        }
        return fields
    }

    func indexRowFootprint(for row: borrowing ExecutionQueryRow, workMeter: DatabaseWorkMeter) throws -> DatabaseIntermediateFootprint {
        var footprint = CanonicalRelationalFootprintMeter.queryRowBaseFootprint()
        for (column, value) in zip(columns, row.values) {
            let descriptor = try persistenceDescriptor(for: column)
            let counts = try QueryFieldStorageAdapter.measure(value, type: descriptor)
            let retained = try QueryValueValidator.measure(value)
            let nodes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<FieldValue>.stride + 32))
                .multiplied(by: counts.valueNodes)
            let objectEntries = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<FieldValue>.stride + 64))
                .multiplied(by: counts.objectEntries).multiplied(by: 2)
            let arrayEntries = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<FieldValue>.stride + 64))
                .multiplied(by: counts.arrayElements).multiplied(by: 2)
            footprint = try footprint.adding(nodes).adding(objectEntries).adding(arrayEntries)
                .adding(DatabaseIntermediateFootprint(bytes: 32 + UInt64(column.name.utf8.count)))
                .adding(DatabaseIntermediateFootprint(bytes: counts.newUTF8Bytes))
                .adding(DatabaseIntermediateFootprint(bytes: retained.retainedByteCount))
            try workMeter.consume(counts.valueNodes, at: .projection)
        }
        for (name, value) in row.annotations {
            footprint = try footprint.adding(CanonicalRelationalFootprintMeter.fieldEntryFootprint(
                nameUTF8Count: name.utf8.count, value: value, workMeter: workMeter, stage: .projection
            ))
        }
        if let version = row.version {
            footprint = try footprint.adding(DatabaseIntermediateFootprint(bytes: UInt64(version.value.utf8.count)))
        }
        return footprint
    }

    private func persistenceDescriptor(for column: QueryColumn) throws -> QueryColumnType {
        guard let field = entity.fieldMapByName[column.name] else {
            throw QueryRowCodecError.unknownField(type: entity.name, field: column.name)
        }
        if let descriptor = field.queryType {
            guard descriptor == column.type else {
                throw QueryRowCodecError.incompatibleField(type: entity.name, field: field.name)
            }
            return descriptor
        }
        try QueryFieldStorageAdapter.validateDescriptor(column.type, fieldType: field.type, isArray: field.isArray)
        return .primitive(field.type, isArray: field.isArray)
    }

    private static func validateColumns(_ columns: [QueryColumn]) throws {
        try QueryColumn.validate(columns)
        var names = Set<String>()
        for column in columns {
            guard names.insert(column.name).inserted else { throw QueryRowCodecError.duplicateColumn(column.name) }
        }
    }

    private static func admitColumns(_ columns: [QueryColumn], workMeter: DatabaseWorkMeter,
                                     stage: DatabaseWorkStage) throws -> DatabaseIntermediateReservation {
        var bytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryColumn>.stride))
            .multiplied(by: UInt64(columns.count))
            .adding(DatabaseIntermediateFootprint(bytes: 64 + UInt64(MemoryLayout<[QueryColumn]>.stride)))
        for column in columns { bytes = try bytes.adding(DatabaseIntermediateFootprint(bytes: UInt64(column.name.utf8.count))) }
        return try workMeter.reserveIntermediate(bytes: bytes.bytes, at: stage)
    }

    private func checkedCombinedCount(
        _ other: FusionCandidateDomain
    ) throws -> Int {
        let (combined, overflow) = count.addingReportingOverflow(other.count)
        guard !overflow else {
            throw FusionExecutionContractError.candidateCountOverflow
        }
        return combined
    }

    private func insertionIndex(
        for primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> (index: Int, match: Int?) {
        var lower = entries.startIndex
        var upper = entries.endIndex
        while lower < upper {
            try workMeter.consume(at: .bindingCandidate)
            let middle = lower + (upper - lower) / 2
            let candidate = entries[middle].packedPrimaryKey
            let order = try Self.primaryKeyOrder(
                candidate,
                primaryKey,
                workMeter: workMeter,
                stage: .bindingCandidate
            )
            if order == 0 {
                return (middle, middle)
            }
            if order < 0 {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        return (lower, nil)
    }

    /// Compares retained primary-key bytes without materializing either side.
    /// The full possible scan is charged before the synchronous borrow.
    static func primaryKeyOrder(
        _ lhs: ByteString,
        _ rhs: ByteString,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> Int {
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(lhs.count, rhs.count),
            workMeter: workMeter,
            stage: stage
        )
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                let sharedCount = min(lhsBytes.count, rhsBytes.count)
                for index in 0..<sharedCount {
                    if lhsBytes[index] < rhsBytes[index] { return -1 }
                    if lhsBytes[index] > rhsBytes[index] { return 1 }
                }
                if lhsBytes.count < rhsBytes.count { return -1 }
                if lhsBytes.count > rhsBytes.count { return 1 }
                return 0
            }
        }
    }

    private static func requireEquivalentPayload(
        _ lhs: borrowing Entry,
        _ rhs: borrowing Entry,
        workMeter: DatabaseWorkMeter
    ) throws {
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(
                lhs.rowFootprint.bytes,
                rhs.rowFootprint.bytes
            ),
            workMeter: workMeter,
            stage: .bindingCandidate
        )
        guard lhs.identity == rhs.identity,
              lhs.row.values == rhs.row.values,
              lhs.row.annotations == rhs.row.annotations,
              lhs.row.version == rhs.row.version else {
            throw FusionExecutionContractError.inconsistentPayload(
                lhs.packedPrimaryKey
            )
        }
    }
}
