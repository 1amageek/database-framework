import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

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
        let count = try DatabaseWireWriter.encodedByteCount(limits: limits) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            try writer.writeCount(fields.count)
            for (key, value) in fields {
                try writer.writeString(key)
                try value.encode(into: &writer)
            }
        }
        let entries = try DatabaseIntermediateFootprint(
            bytes: collectionEntryByteCount
        ).multiplied(by: UInt64(fields.count)).bytes
        return try adding(UInt64(count), entries)
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: lhs)
            .adding(DatabaseIntermediateFootprint(bytes: rhs))
            .bytes
    }
}
