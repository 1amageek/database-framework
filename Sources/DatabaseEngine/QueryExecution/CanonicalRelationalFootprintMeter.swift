import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire

package enum CanonicalRelationalFootprintMeter {
    private static let rowHeaderByteCount: UInt64 = 64
    private static let collectionEntryByteCount: UInt64 = 32

    static func footprint(
        of row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var encodedBytes = try encodedByteCount(
            fields: row.fields,
            workMeter: workMeter
        )
        encodedBytes = try adding(
            encodedBytes,
            encodedByteCount(
                fields: row.annotations,
                workMeter: workMeter
            )
        )
        for (scope, fields) in row.scopedFields {
            encodedBytes = try adding(encodedBytes, UInt64(scope.utf8.count))
            encodedBytes = try adding(
                encodedBytes,
                encodedByteCount(fields: fields, workMeter: workMeter)
            )
        }
        if let version = row.version {
            encodedBytes = try adding(
                encodedBytes,
                UInt64(version.value.utf8.count)
            )
        }
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(rowHeaderByteCount, encodedBytes)
        )
    }

    package static func footprint(
        of row: QueryRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var encodedBytes = try encodedByteCount(
            fields: row.fields,
            workMeter: workMeter
        )
        encodedBytes = try adding(
            encodedBytes,
            encodedByteCount(
                fields: row.annotations,
                workMeter: workMeter
            )
        )
        if let version = row.version {
            encodedBytes = try adding(
                encodedBytes,
                UInt64(version.value.utf8.count)
            )
        }
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(rowHeaderByteCount, encodedBytes)
        )
    }

    /// Measures one standalone retained field value without materializing a
    /// temporary QueryRow dictionary.
    package static func footprint(
        of value: borrowing FieldValue,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        _ = workMeter
        return DatabaseIntermediateFootprint(
            bytes: try adding(
                collectionEntryByteCount,
                adding(
                    encodedFieldByteCount(name: "value", value: copy value),
                    retainedOwnerSurplus(of: value)
                )
            )
        )
    }

    /// Measures a compiled model without first materializing a PersistedModel
    /// field Array or QueryRow dictionary. Each field is borrowed and counted
    /// independently so admission can precede retained collection growth.
    package static func footprint<Model: Persistable>(
        of model: borrowing Model,
        annotations: [String: FieldValue] = [:],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        var encodedBytes: UInt64
        do {
            encodedBytes = try encodedCollectionCount(Model.allFields.count)
            for name in Model.allFields {
                let value = try model.persistedValue(forFieldNamed: name)
                    ?? .null
                encodedBytes = try adding(
                    encodedBytes,
                    encodedFieldByteCount(name: name, value: value)
                )
                encodedBytes = try adding(
                    encodedBytes,
                    retainedOwnerSurplus(of: value)
                )
            }
        } catch {
            throw DatabaseIntermediateFootprintError
                .canonicalValueByteCountUnavailable
        }
        encodedBytes = try adding(
            encodedBytes,
            encodedByteCount(fields: annotations, workMeter: workMeter)
        )
        let entries = try DatabaseIntermediateFootprint(
            bytes: collectionEntryByteCount
        ).multiplied(by: UInt64(Model.allFields.count)).bytes
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(
                rowHeaderByteCount,
                adding(encodedBytes, entries)
            )
        )
    }

    /// Measures an already-compiled model without rebuilding a QueryRow
    /// dictionary, allowing destination admission to precede typed decoding.
    package static func footprint(
        of model: borrowing PersistedModel,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        _ = workMeter
        var encodedBytes = try encodedCollectionCount(model.fields.count)
        for field in model.fields {
            encodedBytes = try adding(
                encodedBytes,
                encodedFieldByteCount(name: field.name, value: field.value)
            )
            encodedBytes = try adding(
                encodedBytes,
                retainedOwnerSurplus(of: field.value)
            )
        }
        let entries = try DatabaseIntermediateFootprint(
            bytes: collectionEntryByteCount
        ).multiplied(by: UInt64(model.fields.count)).bytes
        return DatabaseIntermediateFootprint(
            rows: 1,
            bytes: try adding(
                rowHeaderByteCount,
                adding(encodedBytes, entries)
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

    package static func retainedArrayLayout<Element>(
        for type: Element.Type
    ) throws -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: UInt64(MemoryLayout<[Element]>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<Element>.stride)
            ),
            sharedOwnerByteCount: 32,
            appendAdmissionByteCount: 16
        )
    }

    private static func encodedByteCount(
        fields: [String: FieldValue],
        workMeter: DatabaseWorkMeter
    ) throws -> UInt64 {
        _ = workMeter
        let count: Int
        do {
            let limits = try DatabaseWireLimits(
                maximumFrameBytes: Int.max,
                maximumStringBytes: Int.max,
                maximumByteStringBytes: Int.max,
                maximumCollectionCount:
                    DatabaseWireLimits.default.maximumCollectionCount,
                maximumNestingDepth:
                    DatabaseWireLimits.maximumSupportedNestingDepth,
                maximumObjectCount:
                    DatabaseWireLimits.default.maximumObjectCount
            )
            count = try DatabaseWireWriter.encodedByteCount(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                try writer.writeCount(fields.count)
                for (key, value) in fields {
                    try writer.writeString(key)
                    try value.encode(into: &writer)
                }
            }
        } catch {
            throw DatabaseIntermediateFootprintError
                .canonicalValueByteCountUnavailable
        }
        let entries = try DatabaseIntermediateFootprint(
            bytes: collectionEntryByteCount
        ).multiplied(by: UInt64(fields.count)).bytes
        var retainedSurplus: UInt64 = 0
        for value in fields.values {
            retainedSurplus = try adding(
                retainedSurplus,
                retainedOwnerSurplus(of: value)
            )
        }
        return try adding(adding(UInt64(count), entries), retainedSurplus)
    }

    private static func encodedCollectionCount(_ count: Int) throws -> UInt64 {
        let byteCount = try DatabaseWireWriter.encodedByteCount(
            limits: countingLimits()
        ) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try writer.writeCount(count)
        }
        return UInt64(byteCount)
    }

    private static func encodedFieldByteCount(
        name: String,
        value: FieldValue
    ) throws -> UInt64 {
        let byteCount = try DatabaseWireWriter.encodedByteCount(
            limits: countingLimits()
        ) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try writer.writeString(name)
            try value.encode(into: &writer)
        }
        return UInt64(byteCount)
    }

    private static func countingLimits() throws -> DatabaseWireLimits {
        try DatabaseWireLimits(
            maximumFrameBytes: Int.max,
            maximumStringBytes: Int.max,
            maximumByteStringBytes: Int.max,
            maximumCollectionCount:
                DatabaseWireLimits.default.maximumCollectionCount,
            maximumNestingDepth:
                DatabaseWireLimits.maximumSupportedNestingDepth,
            maximumObjectCount:
                DatabaseWireLimits.default.maximumObjectCount
        )
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: lhs)
            .adding(DatabaseIntermediateFootprint(bytes: rhs))
            .bytes
    }

    /// Encoded byte counts already include every visible byte. This adds only
    /// backing-owner capacity that remains retained beyond those visible bytes.
    private static func retainedOwnerSurplus(
        of value: borrowing FieldValue
    ) throws -> UInt64 {
        switch value {
        case .bytes(let bytes):
            guard let retained = bytes.retainedByteCount else {
                throw DatabaseIntermediateFootprintError
                    .canonicalValueByteCountUnavailable
            }
            return UInt64(retained - bytes.count)
        case .vector(let vector):
            guard let retained = vector.retainedByteCount else {
                throw DatabaseIntermediateFootprintError
                    .canonicalValueByteCountUnavailable
            }
            let visible = vector.count * vector.elementType.byteCount
            return UInt64(retained - visible)
        case .array(let values):
            var result: UInt64 = 0
            for child in values {
                result = try adding(result, retainedOwnerSurplus(of: child))
            }
            return result
        case .object(let object):
            return try retainedOwnerSurplus(of: object)
        case .reference(let reference):
            return try adding(
                retainedOwnerSurplus(of: reference.id),
                retainedOwnerSurplus(of: reference.partitions)
            )
        case .null, .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .float32, .float64,
             .decimal, .string, .date, .time, .dateTime, .timestamp,
             .timeSpan, .calendarPeriod, .geographicPoint,
             .geographicPosition, .uuid, .rdfTerm:
            return 0
        }
    }

    private static func retainedOwnerSurplus(
        of object: borrowing FieldObject
    ) throws -> UInt64 {
        var result: UInt64 = 0
        for field in object.fields {
            result = try adding(
                result,
                retainedOwnerSurplus(of: field.value)
            )
        }
        return result
    }

    private static func retainedOwnerSurplus(
        of identifier: borrowing ReferenceIdentifier
    ) throws -> UInt64 {
        switch identifier {
        case .bytes(let bytes):
            guard let retained = bytes.retainedByteCount else {
                throw DatabaseIntermediateFootprintError
                    .canonicalValueByteCountUnavailable
            }
            return UInt64(retained - bytes.count)
        case .composite(let components):
            var result: UInt64 = 0
            for component in components {
                result = try adding(
                    result,
                    retainedOwnerSurplus(of: component)
                )
            }
            return result
        case .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .string, .uuid:
            return 0
        }
    }
}
