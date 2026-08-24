import DatabaseTypes
import DatabaseKit

package enum CanonicalRelationalFootprintMeter {
    private static let rowHeaderByteCount: UInt64 = 64
    private static let collectionEntryByteCount: UInt64 = 32

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
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        var total = UInt64(MemoryLayout<[String: FieldValue]>.stride)
        for (name, value) in fields {
            total = try adding(
                total,
                retainedEntryByteCount(
                    name: name,
                    value: value,
                    workMeter: workMeter
                )
            )
        }
        try workMeter.checkpoint(at: .projection)
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
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        let valueBytes = try StorageValueDecoder.retainedFootprint(of: value)
        let retainedBytes = try adding(
            collectionEntryByteCount,
            try adding(UInt64(name.utf8.count), valueBytes)
        )
        try DatabaseByteProcessingMeter.consume(
            byteCount: retainedBytes,
            workMeter: workMeter,
            stage: .projection
        )
        return retainedBytes
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: lhs)
            .adding(DatabaseIntermediateFootprint(bytes: rhs))
            .bytes
    }
}
