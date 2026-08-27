import DatabaseTypes
import DatabaseKit

package enum CanonicalRelationalFootprintMeter {
    private static let rowHeaderByteCount: UInt64 = 64
    private static let collectionEntryByteCount: UInt64 = 32

    package static func emptySourceRowFootprint(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        try sourceRowFootprint(
            fields: [:],
            sourceName: nil,
            annotations: [:],
            version: nil,
            workMeter: workMeter,
            stage: stage
        )
    }

    package static func sourceScopeBaseFootprint(
        nameUTF8Count: Int,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        let bytes = try adding(
            try adding(collectionEntryByteCount, UInt64(nameUTF8Count)),
            UInt64(MemoryLayout<[String: FieldValue]>.stride)
        )
        try DatabaseByteProcessingMeter.consume(
            byteCount: bytes,
            workMeter: workMeter,
            stage: stage
        )
        return DatabaseIntermediateFootprint(bytes: bytes)
    }

    static func footprint(
        of row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var retainedBytes = try retainedByteCount(
            fields: row.fields,
            workMeter: workMeter
        )
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: row.annotations,
                workMeter: workMeter
            )
        )
        for (scope, fields) in row.scopedFields {
            let scopeBytes = try adding(
                collectionEntryByteCount,
                UInt64(scope.utf8.count)
            )
            try DatabaseByteProcessingMeter.consume(
                byteCount: scopeBytes,
                workMeter: workMeter,
                stage: .projection
            )
            retainedBytes = try adding(retainedBytes, scopeBytes)
            retainedBytes = try adding(
                retainedBytes,
                retainedByteCount(fields: fields, workMeter: workMeter)
            )
        }
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
        of row: QueryRow,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        var retainedBytes = try retainedByteCount(
            fields: row.fields,
            workMeter: workMeter,
            stage: stage
        )
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: row.annotations,
                workMeter: workMeter,
                stage: stage
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

    /// Measures the exact destination row after RDF blank-node identifiers
    /// receive a shared prefix, without constructing the qualified row.
    package static func footprint(
        of row: borrowing QueryRow,
        prefixingRDFBlankNodeIdentifiersWith prefix: String,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseIntermediateFootprint {
        var retainedBytes = try retainedByteCount(
            fields: row.fields,
            prefixingRDFBlankNodeIdentifiersWith: prefix,
            workMeter: workMeter,
            stage: stage
        )
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: row.annotations,
                prefixingRDFBlankNodeIdentifiersWith: prefix,
                workMeter: workMeter,
                stage: stage
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

    /// Returns the fixed retained shape of a QueryRow before its field and
    /// annotation dictionaries are populated.
    package static func queryRowBaseFootprint()
        -> DatabaseIntermediateFootprint
    {
        DatabaseIntermediateFootprint(
            rows: 1,
            bytes: rowHeaderByteCount
                + UInt64(MemoryLayout<[String: FieldValue]>.stride) * 2
        )
    }

    /// Measures the exact retained shape created by
    /// `CanonicalSourceRow.fromBaseFields` before that shape allocates its
    /// flattened and scoped dictionaries.
    package static func sourceRowFootprint(
        fields: [String: FieldValue],
        sourceName: String?,
        annotations: [String: FieldValue],
        version: PersistableVersionToken?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        var retainedBytes: UInt64
        if let sourceName {
            retainedBytes = UInt64(
                MemoryLayout<[String: FieldValue]>.stride
            )
            for (name, value) in fields {
                retainedBytes = try adding(
                    retainedBytes,
                    retainedEntryByteCount(
                        nameUTF8Count: name.utf8.count,
                        value: value,
                        workMeter: workMeter,
                        stage: stage
                    )
                )
                retainedBytes = try adding(
                    retainedBytes,
                    retainedEntryByteCount(
                        nameUTF8Count: sourceName.utf8.count
                            + 1
                            + name.utf8.count,
                        value: value,
                        workMeter: workMeter,
                        stage: stage
                    )
                )
            }
            retainedBytes = try adding(
                retainedBytes,
                try adding(
                    collectionEntryByteCount,
                    UInt64(sourceName.utf8.count)
                )
            )
            retainedBytes = try adding(
                retainedBytes,
                retainedByteCount(
                    fields: fields,
                    workMeter: workMeter,
                    stage: stage
                )
            )
        } else {
            retainedBytes = try retainedByteCount(
                fields: fields,
                workMeter: workMeter,
                stage: stage
            )
        }
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: annotations,
                workMeter: workMeter,
                stage: stage
            )
        )
        if let version {
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

    /// Measures a renamed subquery row before allocating its destination
    /// dictionaries. Source and target columns are positionally paired.
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
        precondition(sourceColumns.count == targetColumns.count)
        var flattenedBytes = UInt64(
            MemoryLayout<[String: FieldValue]>.stride
        )
        var scopedBytes = UInt64(
            MemoryLayout<[String: FieldValue]>.stride
        )
        for (sourceColumn, targetColumn) in zip(
            sourceColumns,
            targetColumns
        ) {
            guard let value = sourceFields[sourceColumn] else {
                throw CanonicalReadError.unsupportedSelectQuery(
                    "Subquery output column '\(sourceColumn)' is missing"
                )
            }
            let base = try retainedEntryByteCount(
                nameUTF8Count: targetColumn.utf8.count,
                value: value,
                workMeter: workMeter,
                stage: stage
            )
            flattenedBytes = try adding(flattenedBytes, base)
            flattenedBytes = try adding(
                flattenedBytes,
                retainedEntryByteCount(
                    nameUTF8Count: sourceName.utf8.count
                        + 1
                        + targetColumn.utf8.count,
                    value: value,
                    workMeter: workMeter,
                    stage: stage
                )
            )
            scopedBytes = try adding(scopedBytes, base)
        }
        var retainedBytes = try adding(flattenedBytes, scopedBytes)
        retainedBytes = try adding(
            retainedBytes,
            try adding(
                collectionEntryByteCount,
                UInt64(sourceName.utf8.count)
            )
        )
        retainedBytes = try adding(
            retainedBytes,
            retainedByteCount(
                fields: annotations,
                workMeter: workMeter,
                stage: stage
            )
        )
        if let version {
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

    /// Persisted-model variant of the prospective source-row measurement. It
    /// avoids allocating the QueryRow field dictionary before admission.
    package static func sourceRowFootprint(
        of model: borrowing PersistedModel,
        sourceName: String?,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .projection
    ) throws -> DatabaseIntermediateFootprint {
        var flattenedBytes = UInt64(
            MemoryLayout<[String: FieldValue]>.stride
        )
        var scopedBytes = UInt64(
            MemoryLayout<[String: FieldValue]>.stride
        )
        for field in model.fields {
            let base = try retainedEntryByteCount(
                nameUTF8Count: field.name.utf8.count,
                value: field.value,
                workMeter: workMeter,
                stage: stage
            )
            flattenedBytes = try adding(flattenedBytes, base)
            if let sourceName {
                flattenedBytes = try adding(
                    flattenedBytes,
                    retainedEntryByteCount(
                        nameUTF8Count: sourceName.utf8.count
                            + 1
                            + field.name.utf8.count,
                        value: field.value,
                        workMeter: workMeter,
                        stage: stage
                    )
                )
                scopedBytes = try adding(scopedBytes, base)
            }
        }
        var retainedBytes = flattenedBytes
        if let sourceName {
            retainedBytes = try adding(
                retainedBytes,
                try adding(
                    collectionEntryByteCount,
                    UInt64(sourceName.utf8.count)
                )
            )
            retainedBytes = try adding(retainedBytes, scopedBytes)
        }
        retainedBytes = try adding(
            retainedBytes,
            UInt64(MemoryLayout<[String: FieldValue]>.stride)
        )
        // QueryRowCodec emits a lowercase SHA-256 token for every persisted
        // model. Its retained textual representation is always 64 UTF-8 bytes.
        retainedBytes = try adding(retainedBytes, 64)
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(rowHeaderByteCount, retainedBytes)
        )
    }

    /// Returns the retained claim for one prospective QueryRow field.
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
        let valueBytes = try StorageValueDecoder.retainedFootprint(
            of: copy value,
            prefixingRDFBlankNodeIdentifiersWith: prefix
        )
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
        DatabaseIntermediateFootprint(
            bytes: try StorageValueDecoder.retainedFootprint(
                of: copy value,
                workMeter: workMeter,
                stage: stage
            )
        )
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
        of row: QueryRow,
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
        var retainedBytes = try retainedByteCount(
            fields: row.fields,
            workMeter: workMeter
        )
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
        of rows: [QueryRow],
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
            let valueBytes = try StorageValueDecoder.retainedFootprint(
                of: value,
                prefixingRDFBlankNodeIdentifiersWith: prefix
            )
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
        let valueBytes = try StorageValueDecoder.retainedFootprint(of: value)
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
