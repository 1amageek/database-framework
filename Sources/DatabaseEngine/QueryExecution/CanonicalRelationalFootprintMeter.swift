import DatabaseTypes
import DatabaseKit

package enum CanonicalRelationalFootprintMeter {
    private static let rowHeaderByteCount: UInt64 = 64
    private static let collectionEntryByteCount: UInt64 = 32

    package static func emptySourceRowFootprint(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        try positionalRowFootprint(values: [], annotations: [:], version: nil,
                                   workMeter: workMeter, stage: stage)
    }

    package static func sourceScopeBaseFootprint(
        nameUTF8Count: Int,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        let bytes = try adding(collectionEntryByteCount, UInt64(nameUTF8Count))
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
        return DatabaseIntermediateFootprint(bytes: bytes)
    }

    /// Shared source labels, slot storage and wildcard indices are admitted
    /// once, before the source layout is allocated.
    static func sourceLayoutFootprint<Names: Collection>(
        fieldNames: Names,
        sourceName: String?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint where Names.Element == String {
        var bytes = try arrayStorageByteCount(elementCount: fieldNames.count,
                                               elementStride: MemoryLayout<CanonicalSourceLayout.Slot>.stride)
        bytes = try adding(bytes, arrayStorageByteCount(elementCount: fieldNames.count,
                                                        elementStride: MemoryLayout<Int>.stride))
        bytes = try adding(bytes, arrayStorageByteCount(elementCount: fieldNames.count,
                                                        elementStride: MemoryLayout<QueryColumnType?>.stride))
        bytes = try adding(bytes, arrayStorageByteCount(elementCount: fieldNames.count,
                                                        elementStride: MemoryLayout<Bool>.stride))
        bytes = try adding(bytes, 64)
        for name in fieldNames { bytes = try adding(bytes, UInt64(name.utf8.count)) }
        if let sourceName {
            bytes = try adding(bytes, UInt64(sourceName.utf8.count))
            bytes = try adding(bytes, arrayStorageByteCount(elementCount: 1, elementStride: MemoryLayout<String>.stride))
        }
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
        return DatabaseIntermediateFootprint(bytes: bytes)
    }

    static func sourceAliasFootprint(
        of row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        let (count, overflow) = row.payloadLeases.count.addingReportingOverflow(1)
        guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        return try positionalValueArrayFootprint(count: row.values.count,
            valueAt: { _ in .primitive(.null) }, annotations: row.annotations,
            version: row.version, workMeter: workMeter, stage: stage)
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: count,
                element: DatabaseQueryPayloadLease.self))
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: row.sourcePresence.count, element: Bool.self))
    }

    static func footprint(
        of row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try positionalRowFootprint(values: row.values, annotations: row.annotations,
                                   version: row.version, workMeter: workMeter, stage: .projection)
            .adding(DatabaseIntermediateCollectionMeter.arrayFootprint(count: row.sourcePresence.count, element: Bool.self))
    }

    package static func footprint(
        of row: ExecutionQueryRow,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        try positionalRowFootprint(values: row.values, annotations: row.annotations,
                                   version: row.version, workMeter: workMeter, stage: stage)
    }

    package static func queryRowAliasFootprint(
        of row: borrowing ExecutionQueryRow,
        prefixingRDFBlankNodeIdentifiersWith prefix: String? = nil,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        try positionalValueAliasArrayFootprint(count: row.values.count,
            valueAt: { row.values[$0] }, prefixingRDFBlankNodeIdentifiersWith: prefix,
            annotations: row.annotations, version: row.version, workMeter: workMeter, stage: stage)
    }

    /// Known producer roots cover existing payloads. Qualification allocates
    /// new primitive containers and complete blank-node identifier strings.
    package static func positionalValueAliasArrayFootprint(
        count: Int,
        valueAt: (Int) throws -> QueryValue,
        prefixingRDFBlankNodeIdentifiersWith prefix: String? = nil,
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        var bytes = try adding(rowHeaderByteCount,
            arrayStorageByteCount(elementCount: count, elementStride: MemoryLayout<QueryValue>.stride))
        bytes = try adding(bytes, UInt64(MemoryLayout<[String: FieldValue]>.stride))
        for name in annotations.keys {
            bytes = try adding(bytes, try adding(collectionEntryByteCount, UInt64(name.utf8.count)))
        }
        if let version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        if let prefix {
            let prefixBytes = UInt64(prefix.utf8.count)
            for index in 0..<count {
                if case .primitive(let value) = try valueAt(index) {
                    let shape = try QueryValueValidator.measure(.primitive(value))
                    bytes = try adding(bytes, structureByteCount(nodes: shape.nodeCount,
                                                                 elements: shape.collectionElementCount))
                    bytes = try adding(bytes, newBlankNodeTextByteCount(value,
                        prefixByteCount: prefixBytes, workMeter: workMeter, stage: stage))
                }
            }
            for value in annotations.values {
                let shape = try QueryValueValidator.measure(.primitive(value))
                bytes = try adding(bytes, structureByteCount(nodes: shape.nodeCount,
                                                             elements: shape.collectionElementCount))
                bytes = try adding(bytes, newBlankNodeTextByteCount(value,
                    prefixByteCount: prefixBytes, workMeter: workMeter, stage: stage))
            }
        }
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
        return DatabaseIntermediateFootprint(rows: 1, bytes: bytes)
    }

    private enum PrimitiveAliasVisit {
        case field(FieldValue)
        case array([FieldValue], index: Int)
        case object([(key: String, value: FieldValue)], index: Int)
    }

    private static func newBlankNodeTextByteCount(
        _ value: FieldValue,
        prefixByteCount: UInt64,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> UInt64 {
        let layout = try DatabaseRetainedArrayLayout.forElement(PrimitiveAliasVisit.self)
        let claim = try workMeter.reserveIntermediate(at: stage)
        defer { claim.release() }
        var stack: [PrimitiveAliasVisit] = []
        var capacity = 0
        var bytes: UInt64 = 0
        func push(_ visit: PrimitiveAliasVisit) throws {
            let (needed, overflow) = stack.count.addingReportingOverflow(1)
            guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let growth = try layout.growth(from: capacity, toFit: needed)
            if growth.additionalByteCount > 0 {
                try claim.reserveAdditional(bytes: growth.additionalByteCount, at: stage)
                stack.reserveCapacity(growth.capacity)
                capacity = growth.capacity
            }
            stack.append(visit)
        }
        try push(.field(value))
        while let visit = stack.popLast() {
            try workMeter.consume(at: stage)
            switch visit {
            case .array(let values, let index):
                if index + 1 < values.count { try push(.array(values, index: index + 1)) }
                try push(.field(values[index]))
            case .object(let fields, let index):
                if index + 1 < fields.count { try push(.object(fields, index: index + 1)) }
                try push(.field(fields[index].value))
            case .field(let value):
                switch value {
                case .array(let values):
                    if !values.isEmpty { try push(.array(values, index: 0)) }
                case .object(let object):
                    let fields = object.fields
                    if !fields.isEmpty { try push(.object(fields, index: 0)) }
                case .rdfTerm(let first):
                    var term = first
                    var complete = false
                    while !complete {
                        try workMeter.consume(at: stage)
                        switch term {
                        case .blankNode(let identifier):
                            bytes = try adding(bytes, adding(prefixByteCount, UInt64(identifier.rawValue.utf8.count)))
                            complete = true
                        case .tripleTerm(let subject, _, let object):
                            if case .blankNode(let identifier) = subject {
                                bytes = try adding(bytes, adding(prefixByteCount, UInt64(identifier.rawValue.utf8.count)))
                            }
                            term = object
                        case .iri, .literal: complete = true
                        }
                    }
                default: break
                }
            }
        }
        return bytes
    }

    private enum DecodedStructureVisit {
        case value(QueryValue, ownerAdmitted: Bool)
        case arrayElements(QueryValueElements, index: Int)
    }

    /// Observes complete newly constructed owners before publishing a decoded row.
    /// Primitive fields and lexical JSON share the already-admitted source root.
    /// String UTF-8 is a content allowance, not an allocator capacity claim.
    static func decodedRowStructureFootprint(
        values: [QueryValue],
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        payloadLeaseCount: Int,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        var bytes = try adding(rowHeaderByteCount,
            DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryValue>.stride))
                .multiplied(by: UInt64(values.count)).bytes)
        bytes = try adding(bytes, UInt64(MemoryLayout<[String: FieldValue]>.stride))
        bytes = try adding(bytes, DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<String>.stride + MemoryLayout<FieldValue>.stride + 32))
            .multiplied(by: UInt64(annotations.count)).bytes)
        for name in annotations.keys { bytes = try adding(bytes, UInt64(name.utf8.count)) }
        bytes = try adding(bytes, DatabaseIntermediateCollectionMeter.arrayFootprint(
            count: payloadLeaseCount, element: DatabaseQueryPayloadLease.self).bytes)
        if let version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        let layout = try DatabaseRetainedArrayLayout.forElement(DecodedStructureVisit.self)
        let scratch = try workMeter.reserveIntermediate(at: stage)
        defer { scratch.release() }
        var capacity = 0
        var stack: [DecodedStructureVisit] = []
        func push(_ visit: DecodedStructureVisit) throws {
            let (needed, overflow) = stack.count.addingReportingOverflow(1)
            guard !overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let growth = try layout.growth(from: capacity, toFit: needed)
            if growth.additionalByteCount > 0 {
                try scratch.reserveAdditional(bytes: growth.additionalByteCount, at: stage)
                stack.reserveCapacity(growth.capacity)
                capacity = growth.capacity
            }
            stack.append(visit)
        }
        for value in values {
            try push(.value(value, ownerAdmitted: false))
            while let visit = stack.popLast() {
                switch visit {
                case .arrayElements(let elements, let index):
                    if index + 1 < elements.count { try push(.arrayElements(elements, index: index + 1)) }
                    try push(.value(elements[index], ownerAdmitted: true))
                case .value(let value, let ownerAdmitted):
                    try workMeter.consume(at: stage)
                    switch value {
                    case .jsonb(let json):
                        let owner = json.root.storageFootprint
                        bytes = try adding(bytes, owner.retainedStorageByteCount)
                        bytes = try adding(bytes, owner.utf8ByteCount)
                    case .array(let array):
                        if !ownerAdmitted { bytes = try adding(bytes, flatStructureByteCount(array.storageFootprint)) }
                        if !array.elements.isEmpty { try push(.arrayElements(array.elements, index: 0)) }
                    case .composite(let record):
                        if !ownerAdmitted { bytes = try adding(bytes, flatStructureByteCount(record.storageFootprint)) }
                        if !record.fields.isEmpty { try push(.arrayElements(record.fields, index: 0)) }
                    // Fixed temporal payloads are inline in the already charged QueryValue slots.
                    case .primitive, .numeric, .json, .interval, .time, .timeWithTimeZone,
                         .dateInfinity, .timestampInfinity, .intervalInfinity:
                        break
                    }
                }
            }
        }
        try workMeter.checkpoint(at: stage)
        return DatabaseIntermediateFootprint(rows: 1, bytes: bytes)
    }

    /// A view retains the complete flat structural owner, independently of its logical subtree.
    private static func flatStructureByteCount(_ footprint: QueryValueStorageFootprint) throws -> UInt64 {
        try adding(footprint.retainedStorageByteCount,
            adding(footprint.typeStorageFootprint.retainedStorageByteCount, footprint.typeStorageFootprint.utf8ByteCount))
    }

    /// The persisted array decoder selects a finite growth allowance before allocation.
    package static func decodedQueryValueStructureAllowance(
        _ decoded: QueryFieldDecodedFootprint
    ) throws -> UInt64 {
        let minimum = try decoded.minimumQueryValueStorageByteCount
        return try adding(minimum, minimum)
    }

    /// Capacity selection is Framework policy; Kit reports logical requirements.
    /// Actual arena capacities are reconciled after bounded construction.
    private static func decodedStructureAllowance(
        _ decoded: QueryFieldDecodedFootprint
    ) throws -> UInt64 {
        func capacity(_ count: UInt64) throws -> UInt64 {
            guard let count = Int(exactly: count) else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let layout = try DatabaseRetainedArrayLayout.forElement(QueryValue.self)
            return UInt64(try layout.growth(from: 0, toFit: count).capacity)
        }
        let flat = try QueryJSONBStorageFootprint.storageByteCount(
            nodeCapacity: capacity(decoded.jsonbNodeCount),
            elementIndexCapacity: capacity(decoded.jsonbElementIndexCount),
            memberCapacity: capacity(decoded.jsonbMemberCount))
        return try adding(adding(adding(structureByteCount(nodes: decoded.nodeCount,
            elements: decoded.collectionElementCount), flat), decoded.utf8ByteCount),
            decodedQueryValueStructureAllowance(decoded))
    }

    /// Only primitive RDF terms change; declared rich SQL values remain intact.
    package static func footprint(
        of row: borrowing ExecutionQueryRow,
        prefixingRDFBlankNodeIdentifiersWith prefix: String,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        var bytes = try arrayStorageByteCount(elementCount: row.values.count,
                                               elementStride: MemoryLayout<QueryValue>.stride)
        for value in row.values {
            bytes = try adding(bytes, queryValueByteCount(
                value, prefixingRDFBlankNodeIdentifiersWith: prefix, workMeter: workMeter, stage: stage
            ))
        }
        bytes = try adding(bytes, retainedByteCount(fields: row.annotations,
                                                    prefixingRDFBlankNodeIdentifiersWith: prefix,
                                                    workMeter: workMeter, stage: stage))
        if let version = row.version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        return DatabaseIntermediateFootprint(rows: 1, bytes: try adding(rowHeaderByteCount, bytes))
    }

    package static func queryRowBaseFootprint() -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(rows: 1, bytes: rowHeaderByteCount
            + UInt64(MemoryLayout<[QueryValue]>.stride)
            + UInt64(MemoryLayout<[String: FieldValue]>.stride))
    }

    /// Prospective primitive adapter row. Dictionary inputs are consumed only
    /// at this explicit adapter boundary; source row storage is positional.
    package static func sourceRowFootprint(
        fields: [String: FieldValue],
        sourceName: String?,
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        var bytes = try arrayStorageByteCount(elementCount: fields.count,
                                               elementStride: MemoryLayout<QueryValue>.stride)
        for value in fields.values {
            bytes = try adding(bytes, queryValueByteCount(.primitive(value), workMeter: workMeter, stage: stage))
        }
        bytes = try adding(bytes, retainedByteCount(fields: annotations, workMeter: workMeter, stage: stage))
        if let version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        return DatabaseIntermediateFootprint(rows: 1, bytes: try adding(rowHeaderByteCount, bytes))
    }

    package static func sourceRowFootprint(
        sourceFields: [String: FieldValue],
        sourceColumns: [String],
        targetColumns: [String],
        sourceName: String,
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> DatabaseIntermediateFootprint {
        guard sourceColumns.count == targetColumns.count else {
            throw CanonicalReadError.unsupportedSelectQuery("Subquery column arity does not match its destination")
        }
        var bytes = try arrayStorageByteCount(elementCount: sourceColumns.count,
                                               elementStride: MemoryLayout<QueryValue>.stride)
        for sourceColumn in sourceColumns {
            guard let value = sourceFields[sourceColumn] else {
                throw CanonicalReadError.unsupportedSelectQuery("Subquery output column '\(sourceColumn)' is missing")
            }
            bytes = try adding(bytes, queryValueByteCount(.primitive(value), workMeter: workMeter, stage: stage))
        }
        bytes = try adding(bytes, retainedByteCount(fields: annotations, workMeter: workMeter, stage: stage))
        if let version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        return DatabaseIntermediateFootprint(rows: 1, bytes: try adding(rowHeaderByteCount, bytes))
    }

    /// Admits new destination arrays and rich-value structure before decode.
    /// Persisted byte roots remain charged through the source entry's lease;
    /// envelope traversal counts bound new structure, not unique allocation.
    static func sourceRowFootprint(
        of model: borrowing PersistedModel,
        entity: Schema.Entity,
        layout: CanonicalSourceLayout,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        try EntityRuntimeDefinition.validateCanonicalSourceModel(model, entity: entity)
        var bytes = try arrayStorageByteCount(elementCount: layout.slots.count,
                                               elementStride: MemoryLayout<QueryValue>.stride)
        bytes = try adding(bytes, UInt64(MemoryLayout<[String: FieldValue]>.stride))
        // Version hashing sorts fields into a temporary array after admission.
        bytes = try adding(bytes, arrayStorageByteCount(elementCount: model.fields.count,
                                                        elementStride: MemoryLayout<PersistableField>.stride))
        bytes = try adding(bytes, arrayStorageByteCount(elementCount: 1,
                                                        elementStride: MemoryLayout<DatabaseQueryPayloadLease>.stride))
        for slot in layout.slots {
            guard let schema = entity.fieldMapByName[slot.name] else { continue }
            let payload = try EntityRuntimeDefinition.canonicalSourceField(model, schema: schema)
            if let descriptor = schema.queryType {
                try QueryFieldStorageAdapter.validateDescriptor(descriptor, fieldType: schema.type, isArray: schema.isArray)
                let decoded = try QueryFieldStorageAdapter.measureDecoded(payload, type: descriptor)
                bytes = try adding(bytes, decodedStructureAllowance(decoded))
                try workMeter.consume(decoded.nodeCount, at: stage)
            } else {
                guard QueryValueValidator.accepts(.primitive(payload), type: .primitive(schema.type, isArray: schema.isArray)) else {
                    throw SchemaDrivenEntityRuntimeError.invalidFieldValue(entity: entity.name, field: schema.name, expected: schema.type)
                }
            }
        }
        // The version's lowercase SHA-256 text is newly owned by the row.
        bytes = try adding(bytes, 64)
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
        return DatabaseIntermediateFootprint(rows: 1, bytes: try adding(rowHeaderByteCount, bytes))
    }

    package static func positionalRowFootprint(
        values: [QueryValue], annotations: [String: FieldValue], version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        try positionalValueArrayFootprint(
            count: values.count, valueAt: { values[$0] }, annotations: annotations,
            version: version, workMeter: workMeter, stage: stage
        )
    }

    /// Measures a positional destination before its array is allocated.
    /// Column labels are owned by the independently admitted shared schema.
    package static func positionalValueArrayFootprint(
        count: Int,
        valueAt: (Int) throws -> QueryValue,
        prefixingRDFBlankNodeIdentifiersWith prefix: String? = nil,
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        var bytes = try arrayStorageByteCount(elementCount: count,
                                               elementStride: MemoryLayout<QueryValue>.stride)
        for index in 0..<count {
            bytes = try adding(bytes, queryValueByteCount(try valueAt(index), prefixingRDFBlankNodeIdentifiersWith: prefix,
                                                            workMeter: workMeter, stage: stage))
        }
        bytes = try adding(bytes, retainedByteCount(fields: annotations, workMeter: workMeter, stage: stage))
        if let version { bytes = try adding(bytes, UInt64(version.value.utf8.count)) }
        return DatabaseIntermediateFootprint(rows: 1, bytes: try adding(rowHeaderByteCount, bytes))
    }

    private static func queryValueByteCount(
        _ value: QueryValue,
        prefixingRDFBlankNodeIdentifiersWith prefix: String? = nil,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> UInt64 {
        // Logical traversal is charged for the selected subtree; retainedByteCount
        // includes its complete flat value/type owner exactly once, not per cell.
        let counts = try QueryValueValidator.measure(value)
        var bytes = try adding(structureByteCount(nodes: counts.nodeCount, elements: counts.collectionElementCount),
                               counts.retainedByteCount)
        if let prefix, case .primitive(let primitive) = value {
            // Prefixing changes only RDF lexical text, preserving all value
            // nodes, container capacities and unrelated retained byte roots.
            bytes = try adding(bytes, rdfPrefixByteIncrease(primitive, prefixByteCount: UInt64(prefix.utf8.count)))
        }
        try workMeter.consume(counts.nodeCount, at: stage)
        try DatabaseByteProcessingMeter.consume(byteCount: bytes, workMeter: workMeter, stage: stage)
        return bytes
    }

    private static func rdfPrefixByteIncrease(_ value: FieldValue, prefixByteCount: UInt64) throws -> UInt64 {
        switch value {
        case .rdfTerm(let term): return try rdfPrefixByteIncrease(term, prefixByteCount: prefixByteCount)
        case .array(let values):
            var total: UInt64 = 0
            for value in values { total = try adding(total, rdfPrefixByteIncrease(value, prefixByteCount: prefixByteCount)) }
            return total
        case .object(let object):
            var total: UInt64 = 0
            for field in object.fields { total = try adding(total, rdfPrefixByteIncrease(field.value, prefixByteCount: prefixByteCount)) }
            return total
        case .reference(let reference):
            var total: UInt64 = 0
            for field in reference.partitions.fields {
                total = try adding(total, rdfPrefixByteIncrease(field.value, prefixByteCount: prefixByteCount))
            }
            return total
        default: return 0
        }
    }

    private static func rdfPrefixByteIncrease(_ term: RDFTerm, prefixByteCount: UInt64) throws -> UInt64 {
        switch term {
        case .blankNode: return prefixByteCount
        case .tripleTerm(let subject, _, let object):
            let subjectBytes: UInt64
            if case .blankNode = subject { subjectBytes = prefixByteCount } else { subjectBytes = 0 }
            return try adding(subjectBytes, rdfPrefixByteIncrease(object, prefixByteCount: prefixByteCount))
        case .iri, .literal: return 0
        }
    }

    /// Logical producer bounds use the same structural allowances as actual
    /// retained values; callers separately bound newly produced UTF-8 bytes.
    package static func prospectiveValueFootprint(
        nodeCount: UInt64 = 1,
        collectionElementCount: UInt64 = 0,
        retainedByteCount: UInt64 = 0
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(bytes: try adding(
            structureByteCount(nodes: nodeCount, elements: collectionElementCount), retainedByteCount
        ))
    }

    private static func structureByteCount(nodes: UInt64, elements: UInt64) throws -> UInt64 {
        let nodeStride = UInt64(max(MemoryLayout<QueryValue>.stride, MemoryLayout<QueryJSONBMember>.stride) + 32)
        let nodeBytes = try DatabaseIntermediateFootprint(bytes: nodeStride).multiplied(by: nodes)
        // Twice the logical count bounds the deterministic geometric capacity.
        let entryBytes = try DatabaseIntermediateFootprint(bytes: nodeStride).multiplied(by: elements).multiplied(by: 2)
        return try nodeBytes.adding(entryBytes).bytes
    }

    private static func arrayStorageByteCount(elementCount: Int, elementStride: Int) throws -> UInt64 {
        let layout = try DatabaseRetainedArrayLayout.validated(
            containerByteCount: UInt64(MemoryLayout<[QueryValue]>.stride),
            elementCapacitySlotByteCount: UInt64(max(1, elementStride)),
            sharedOwnerByteCount: 32, appendAdmissionByteCount: 16)
        let growth = try layout.growth(from: 0, toFit: elementCount)
        return try adding(layout.containerByteCount, growth.additionalByteCount)
    }

    /// Returns the retained claim for one prospective ExecutionQueryRow field.
    ///
    /// Supplying the UTF-8 count lets a feature executor reserve memory before
    /// allocating a composed field-name String.
    package static func fieldEntryFootprint(
        nameUTF8Count: Int,
        value: borrowing FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: try retainedEntryByteCount(
                nameUTF8Count: nameUTF8Count,
                value: value,
                workMeter: workMeter,
                stage: stage
            )
        )
    }

    package static func fieldEntryFootprint(
        nameUTF8Count: Int,
        value: borrowing FieldValue,
        prefixingRDFBlankNodeIdentifiersWith prefix: String,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        let valueBytes = try queryValueByteCount(.primitive(copy value),
            prefixingRDFBlankNodeIdentifiersWith: prefix, workMeter: workMeter, stage: stage)
        let retainedBytes = try adding(
            collectionEntryByteCount,
            try adding(UInt64(nameUTF8Count), valueBytes)
        )
        try DatabaseByteProcessingMeter.consume(
            byteCount: retainedBytes,
            workMeter: workMeter,
            stage: stage
        )
        return DatabaseIntermediateFootprint(bytes: retainedBytes)
    }

    /// Measures one retained field-value payload without creating a temporary
    /// row. Destination container storage remains the destination owner's
    /// responsibility.
    package static func valueFootprint(
        of value: borrowing FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(bytes: try queryValueByteCount(
            .primitive(copy value), workMeter: workMeter, stage: stage))
    }

    package static func valueFootprint(
        of value: borrowing QueryValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(bytes: try queryValueByteCount(copy value, workMeter: workMeter, stage: stage))
    }

    /// Conservatively bounds the retained canonical FieldValue footprint of
    /// one RDF term before the term is produced. The UTF-8 and term-count
    /// limits are semantic producer contracts; the structural allowance
    /// covers canonical tags, lengths, and triple-term framing.
    package static func maximumRDFTermValueFootprint(
        maximumUTF8ByteCount: UInt64,
        maximumTermCount: UInt64 = 1
    ) throws -> DatabaseIntermediateFootprint {
        let structuralBytes = try DatabaseIntermediateFootprint(bytes: 64)
            .multiplied(by: maximumTermCount)
        let encodedBytes = try DatabaseIntermediateFootprint(
            bytes: maximumUTF8ByteCount
        )
            .adding(structuralBytes)
            .adding(DatabaseIntermediateFootprint(bytes: 64))
        let retainedPayload = try encodedBytes.multiplied(by: 4)
        return try retainedPayload.adding(
            DatabaseIntermediateFootprint(
                bytes: UInt64(MemoryLayout<FieldValue>.stride) + 160
            )
        )
    }

    package static func footprint(
        of model: PersistedModel,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var retainedBytes = try retainedByteCount(
            fields: model.fields,
            workMeter: workMeter
        )
        retainedBytes = try adding(
            retainedBytes,
            UInt64(MemoryLayout<[String: FieldValue]>.stride)
        )
        retainedBytes = try adding(retainedBytes, 64)
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(rowHeaderByteCount, retainedBytes)
        )
    }

    package static func footprint(
        of row: ExecutionQueryRow,
        appendingAnnotationNamed name: String,
        value: FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        guard row.annotations[name] != nil else {
            return try footprint(
                try footprint(of: row, workMeter: workMeter),
                appendingAnnotationNamed: name,
                value: value,
                workMeter: workMeter
            )
        }
        var retainedBytes = try arrayStorageByteCount(elementCount: row.values.count,
                                                        elementStride: MemoryLayout<QueryValue>.stride)
        for cell in row.values {
            retainedBytes = try adding(retainedBytes, queryValueByteCount(cell, workMeter: workMeter, stage: .projection))
        }
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: row.annotations,
                replacingName: name,
                value: value,
                workMeter: workMeter
            )
        )
        if let version = row.version {
            retainedBytes = try adding(
                retainedBytes,
                UInt64(version.value.utf8.count)
            )
        }
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(rowHeaderByteCount, retainedBytes)
        )
    }

    package static func footprint(
        _ existing: DatabaseIntermediateFootprint,
        appendingAnnotationNamed name: String,
        value: FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try existing.adding(
            DatabaseIntermediateFootprint(
                bytes: try retainedEntryByteCount(
                    name: name,
                    value: value,
                    workMeter: workMeter
                )
            )
        )
    }

    /// Extends an independently measured footprint without allocating a
    /// dynamic annotation-name `String` before destination admission.
    package static func footprint(
        _ existing: DatabaseIntermediateFootprint,
        appendingAnnotationNamed name: StaticString,
        value: FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try existing.adding(
            DatabaseIntermediateFootprint(
                bytes: try retainedEntryByteCount(
                    nameUTF8Count: name.utf8CodeUnitCount,
                    value: value,
                    workMeter: workMeter,
                    stage: .projection
                )
            )
        )
    }

    static func footprint(
        of rows: [CanonicalSourceRow],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var total = DatabaseIntermediateFootprint()
        for row in rows {
            total = try total.adding(
                footprint(of: row, workMeter: workMeter)
            )
        }
        return total
    }

    package static func footprint(
        of rows: [ExecutionQueryRow],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var total = DatabaseIntermediateFootprint()
        for row in rows {
            total = try total.adding(
                footprint(of: row, workMeter: workMeter)
            )
        }
        return total
    }

    private static func retainedByteCount(
        fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> UInt64 {
        var total = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        for (name, value) in fields {
            total = try adding(
                total,
                retainedEntryByteCount(
                    name: name,
                    value: value,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        }
        try workMeter.checkpoint(at: stage)
        return total
    }

    private static func retainedByteCount(
        fields: [String: FieldValue],
        prefixingRDFBlankNodeIdentifiersWith prefix: String,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> UInt64 {
        var total = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        for (name, value) in fields {
            let valueBytes = try queryValueByteCount(.primitive(value),
                prefixingRDFBlankNodeIdentifiersWith: prefix, workMeter: workMeter, stage: stage)
            let retainedBytes = try adding(
                collectionEntryByteCount,
                try adding(UInt64(name.utf8.count), valueBytes)
            )
            try DatabaseByteProcessingMeter.consume(
                byteCount: retainedBytes,
                workMeter: workMeter,
                stage: stage
            )
            total = try adding(total, retainedBytes)
        }
        try workMeter.checkpoint(at: stage)
        return total
    }

    private static func retainedByteCount(
        fields: [PersistableField],
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        var total = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        for field in fields {
            total = try adding(
                total,
                retainedEntryByteCount(
                    name: field.name,
                    value: field.value,
                    workMeter: workMeter
                )
            )
        }
        try workMeter.checkpoint(at: .projection)
        return total
    }

    private static func retainedByteCount(
        fields: [String: FieldValue],
        replacingName name: String,
        value: FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        var total = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        for (existingName, existingValue) in fields where existingName != name {
            total = try adding(
                total,
                retainedEntryByteCount(
                    name: existingName,
                    value: existingValue,
                    workMeter: workMeter
                )
            )
        }
        total = try adding(
            total,
            retainedEntryByteCount(
                name: name,
                value: value,
                workMeter: workMeter
            )
        )
        try workMeter.checkpoint(at: .projection)
        return total
    }

    private static func retainedEntryByteCount(
        name: String,
        value: FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> UInt64 {
        try retainedEntryByteCount(
            nameUTF8Count: name.utf8.count,
            value: value,
            workMeter: workMeter,
            stage: stage
        )
    }

    private static func retainedEntryByteCount(
        nameUTF8Count: Int,
        value: borrowing FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> UInt64 {
        let valueBytes = try queryValueByteCount(.primitive(copy value), workMeter: workMeter, stage: stage)
        let retainedBytes = try adding(
            collectionEntryByteCount,
            try adding(UInt64(nameUTF8Count), valueBytes)
        )
        try DatabaseByteProcessingMeter.consume(
            byteCount: retainedBytes,
            workMeter: workMeter,
            stage: stage
        )
        return retainedBytes
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: lhs)
            .adding(DatabaseIntermediateFootprint(bytes: rhs))
            .bytes
    }
}
