import DatabaseKit
import DatabaseTypes

/// Reconstructs primitive field values from bounded engine storage frames.
package enum StorageValueDecoder {
    private enum RetainedFootprintWork {
        case value(FieldValue)
        case object(FieldObject)
        case identifier(ReferenceIdentifier)
    }

    /// Measures an already-decoded canonical value with the same retained
    /// ownership model used by the bounded frame preflight.
    package static func retainedFootprint(
        of value: FieldValue
    ) throws -> UInt64 {
        let base = UInt64(MemoryLayout<FieldValue>.stride + 32)
        switch value {
        case .null, .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .float32, .float64,
             .decimal, .date, .time, .dateTime, .timestamp, .timeSpan,
             .calendarPeriod, .geographicPoint, .geographicPosition, .uuid:
            return base
        case .string(let string):
            return try addingFootprints(base, UInt64(string.utf8.count))
        case .bytes(let bytes):
            return try addingFootprints(base, UInt64(bytes.count))
        case .vector(let vector):
            let elementByteCount: UInt64
            switch vector.elementType {
            case .int8, .uint8: elementByteCount = 1
            case .int16, .uint16: elementByteCount = 2
            case .int32, .uint32, .float32: elementByteCount = 4
            case .int64, .uint64, .float64: elementByteCount = 8
            }
            let values = try DatabaseIntermediateFootprint(
                bytes: elementByteCount
            ).multiplied(by: UInt64(vector.count))
            return try DatabaseIntermediateFootprint(bytes: base + 32)
                .adding(values).bytes
        case .array(let values):
            var total = try DatabaseIntermediateFootprint(
                bytes: base + UInt64(MemoryLayout<[FieldValue]>.stride)
            ).adding(
                try DatabaseIntermediateFootprint(
                    bytes: UInt64(MemoryLayout<FieldValue>.stride + 16)
                ).multiplied(by: UInt64(values.count))
            )
            for child in values {
                total = try total.adding(
                    DatabaseIntermediateFootprint(
                        bytes: try retainedFootprint(of: child)
                    )
                )
            }
            return total.bytes
        case .object(let object):
            return try addingFootprints(
                base,
                retainedFootprint(of: object)
            )
        case .reference(let reference):
            var total = try DatabaseIntermediateFootprint(
                bytes: base + 64 + UInt64(reference.entity.utf8.count)
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: try retainedFootprint(of: reference.id)
                )
            )
            total = try total.adding(
                DatabaseIntermediateFootprint(
                    bytes: try retainedFootprint(of: reference.partitions)
                )
            )
            return total.bytes
        case .rdfTerm(let term):
            let encodedBytes = UInt64(
                try RDFTermStorageFormat.encodedByteCount(term)
            )
            return try DatabaseIntermediateFootprint(bytes: encodedBytes)
                .multiplied(by: 4)
                .adding(DatabaseIntermediateFootprint(bytes: base + 128))
                .bytes
        }
    }

    /// Iteratively measures an owned value while charging traversal work and
    /// admitting the explicit traversal stack before it grows.
    package static func retainedFootprint(
        of value: FieldValue,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> UInt64 {
        let layout = try DatabaseRetainedArrayLayout.forElement(
            RetainedFootprintWork.self
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: layout.containerByteCount,
            at: stage
        )
        defer { reservation.release() }
        var stack: [RetainedFootprintWork] = []
        var accountedCapacity = 0

        func append(_ work: RetainedFootprintWork) throws {
            let required = stack.count + 1
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: required
            )
            if growth.additionalByteCount > 0 {
                try reservation.reserveAdditional(
                    bytes: growth.additionalByteCount,
                    at: stage
                )
                stack.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            stack.append(work)
        }

        try append(.value(value))
        var total: UInt64 = 0
        while let work = stack.popLast() {
            try workMeter.consume(at: stage)
            switch work {
            case .value(let current):
                let base = UInt64(MemoryLayout<FieldValue>.stride + 32)
                total = try addingFootprints(total, base)
                switch current {
                case .null, .bool, .int8, .int16, .int32, .int64,
                     .uint8, .uint16, .uint32, .uint64, .float32, .float64,
                     .decimal, .date, .time, .dateTime, .timestamp, .timeSpan,
                     .calendarPeriod, .geographicPoint,
                     .geographicPosition, .uuid:
                    break
                case .string(let string):
                    let count = UInt64(string.utf8.count)
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: count,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, count)
                case .bytes(let bytes):
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: bytes.count,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, UInt64(bytes.count))
                case .vector(let vector):
                    let elementByteCount: UInt64
                    switch vector.elementType {
                    case .int8, .uint8: elementByteCount = 1
                    case .int16, .uint16: elementByteCount = 2
                    case .int32, .uint32, .float32: elementByteCount = 4
                    case .int64, .uint64, .float64: elementByteCount = 8
                    }
                    let values = try DatabaseIntermediateFootprint(
                        bytes: elementByteCount
                    ).multiplied(by: UInt64(vector.count)).bytes
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: values,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, 32)
                    total = try addingFootprints(total, values)
                case .array(let values):
                    total = try addingFootprints(
                        total,
                        UInt64(MemoryLayout<[FieldValue]>.stride)
                    )
                    total = try addingFootprints(
                        total,
                        try DatabaseIntermediateFootprint(
                            bytes: UInt64(MemoryLayout<FieldValue>.stride + 16)
                        ).multiplied(by: UInt64(values.count)).bytes
                    )
                    for child in values.reversed() {
                        try append(.value(child))
                    }
                case .object(let object):
                    try append(.object(object))
                case .reference(let reference):
                    let entityBytes = UInt64(reference.entity.utf8.count)
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: entityBytes,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, 64)
                    total = try addingFootprints(total, entityBytes)
                    try append(.object(reference.partitions))
                    try append(.identifier(reference.id))
                case .rdfTerm(let term):
                    let encodedBytes = UInt64(
                        try RDFTermStorageFormat.encodedByteCount(term)
                    )
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: encodedBytes,
                        passes: 4,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, 128)
                    total = try addingFootprints(
                        total,
                        try DatabaseIntermediateFootprint(bytes: encodedBytes)
                            .multiplied(by: 4).bytes
                    )
                }
            case .object(let object):
                total = try addingFootprints(
                    total,
                    UInt64(MemoryLayout<FieldObject>.stride + 64)
                )
                total = try addingFootprints(
                    total,
                    try DatabaseIntermediateFootprint(bytes: 64)
                        .multiplied(by: UInt64(object.count)).bytes
                )
                for field in object.fields.reversed() {
                    let keyBytes = UInt64(field.key.utf8.count)
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: keyBytes,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, keyBytes)
                    try append(.value(field.value))
                }
            case .identifier(let identifier):
                let base: UInt64 = 64
                total = try addingFootprints(total, base)
                switch identifier {
                case .bool, .int8, .int16, .int32, .int64,
                     .uint8, .uint16, .uint32, .uint64, .uuid:
                    break
                case .string(let string):
                    let count = UInt64(string.utf8.count)
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: count,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, count)
                case .bytes(let bytes):
                    try DatabaseByteProcessingMeter.consume(
                        byteCount: bytes.count,
                        workMeter: workMeter,
                        stage: stage
                    )
                    total = try addingFootprints(total, UInt64(bytes.count))
                case .composite(let components):
                    total = try addingFootprints(
                        total,
                        UInt64(MemoryLayout<[ReferenceIdentifier]>.stride)
                    )
                    total = try addingFootprints(
                        total,
                        try DatabaseIntermediateFootprint(bytes: 64)
                            .multiplied(by: UInt64(components.count)).bytes
                    )
                    for component in components.reversed() {
                        try append(.identifier(component))
                    }
                }
            }
        }
        return total
    }

    /// Measures the retained value tree without materializing it. The caller
    /// reserves the returned footprint before invoking `read(from:)`.
    package static func retainedFootprint(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> UInt64 {
        let base = UInt64(MemoryLayout<FieldValue>.stride + 32)
        switch try decoder.readUInt8() {
        case 0:
            return base
        case 1:
            _ = try decoder.readBool(); return base
        case 2:
            _ = try decoder.readInt8(); return base
        case 3:
            _ = try decoder.readInt16(); return base
        case 4:
            _ = try decoder.readInt32(); return base
        case 5:
            _ = try decoder.readInt64(); return base
        case 6:
            _ = try decoder.readUInt8(); return base
        case 7:
            _ = try decoder.readUInt16(); return base
        case 8:
            _ = try decoder.readUInt32(); return base
        case 9:
            _ = try decoder.readUInt64(); return base
        case 10:
            _ = try decoder.readFloat(); return base
        case 11:
            _ = try decoder.readDouble(); return base
        case 12:
            _ = try decoder.readInt128()
            _ = try decoder.readInt32()
            return base
        case 13:
            return try adding(
                base,
                UInt64(decoder.readValidatedStringBytes().count)
            )
        case 14:
            return try adding(base, UInt64(decoder.readBytes().count))
        case 15:
            _ = try decoder.readInt32()
            _ = try decoder.readUInt8()
            _ = try decoder.readUInt8()
            return base
        case 16:
            try skipCivilTime(from: &decoder)
            return base
        case 17:
            _ = try decoder.readInt32()
            _ = try decoder.readUInt8()
            _ = try decoder.readUInt8()
            try skipCivilTime(from: &decoder)
            return base
        case 18, 19:
            _ = try decoder.readInt64()
            _ = try decoder.readUInt32()
            return base
        case 20:
            _ = try decoder.readInt64()
            _ = try decoder.readInt64()
            return base
        case 21:
            _ = try decoder.readDouble()
            _ = try decoder.readDouble()
            return base
        case 22:
            _ = try decoder.readDouble()
            _ = try decoder.readDouble()
            _ = try decoder.readDouble()
            return base
        case 23:
            let elementTag = try decoder.readUInt8()
            let count = try decoder.readCount()
            let stride: UInt64
            switch elementTag {
            case 0, 4: stride = 1
            case 1, 5: stride = 2
            case 2, 6, 8: stride = 4
            case 3, 7, 9: stride = 8
            default: throw .invalidValueTag(elementTag)
            }
            let byteCount = try multiplied(stride, by: UInt64(count))
            _ = try decoder.readRawBytes(count: Int(byteCount))
            return try adding(base + 32, byteCount)
        case 24:
            _ = try decoder.readUInt64()
            _ = try decoder.readUInt64()
            return base
        case 25:
            let count = try decoder.readCount()
            var total = try adding(
                base + UInt64(MemoryLayout<[FieldValue]>.stride),
                try multiplied(
                    UInt64(MemoryLayout<FieldValue>.stride + 16),
                    by: UInt64(count)
                )
            )
            for _ in 0..<count {
                total = try adding(
                    total,
                    decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(
                            StorageFrameError
                        ) -> UInt64 in
                        try retainedFootprint(from: &child)
                    }
                )
            }
            return total
        case 26:
            return try adding(base, retainedObjectFootprint(from: &decoder))
        case 27:
            let entityByteCount = try decoder.readValidatedStringBytes().count
            var total = try adding(base + 64, UInt64(entityByteCount))
            total = try adding(
                total,
                decoder.readLengthPrefixed {
                    (child: inout StorageFrameDecoder) throws(
                        StorageFrameError
                    ) -> UInt64 in
                    try retainedReferenceIdentifierFootprint(from: &child)
                }
            )
            return try adding(
                total,
                retainedObjectFootprint(from: &decoder)
            )
        case 28:
            let bytes = try decoder.readBytes()
            return try adding(
                base + 128,
                try multiplied(UInt64(bytes.count), by: 4)
            )
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    package static func read(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> FieldValue {
        switch try decoder.readUInt8() {
        case 0:
            return .null
        case 1:
            return .bool(try decoder.readBool())
        case 2:
            return .int8(try decoder.readInt8())
        case 3:
            return .int16(try decoder.readInt16())
        case 4:
            return .int32(try decoder.readInt32())
        case 5:
            return .int64(try decoder.readInt64())
        case 6:
            return .uint8(try decoder.readUInt8())
        case 7:
            return .uint16(try decoder.readUInt16())
        case 8:
            return .uint32(try decoder.readUInt32())
        case 9:
            return .uint64(try decoder.readUInt64())
        case 10:
            return .float32(try decoder.readFloat())
        case 11:
            return .float64(try decoder.readDouble())
        case 12:
            return .decimal(
                ExactDecimal(
                    coefficient: try decoder.readInt128(),
                    scale: try decoder.readInt32()
                )
            )
        case 13:
            return .string(try decoder.readString())
        case 14:
            return .bytes(try decoder.readBytes())
        case 15:
            return .date(
                try valid {
                    try CivilDate(
                        year: decoder.readInt32(),
                        month: decoder.readUInt8(),
                        day: decoder.readUInt8()
                    )
                }
            )
        case 16:
            return .time(try readCivilTime(from: &decoder))
        case 17:
            let date = try valid {
                try CivilDate(
                    year: decoder.readInt32(),
                    month: decoder.readUInt8(),
                    day: decoder.readUInt8()
                )
            }
            return .dateTime(
                CivilDateTime(
                    date: date,
                    time: try readCivilTime(from: &decoder)
                )
            )
        case 18:
            return .timestamp(
                try valid {
                    try Timestamp(
                        secondsSinceUnixEpoch: decoder.readInt64(),
                        nanoseconds: decoder.readUInt32()
                    )
                }
            )
        case 19:
            return .timeSpan(
                try valid {
                    try TimeSpan(
                        seconds: decoder.readInt64(),
                        nanoseconds: decoder.readUInt32()
                    )
                }
            )
        case 20:
            return .calendarPeriod(
                CalendarPeriod(
                    months: try decoder.readInt64(),
                    days: try decoder.readInt64()
                )
            )
        case 21:
            return .geographicPoint(
                try valid {
                    try GeographicPoint(
                        latitude: decoder.readDouble(),
                        longitude: decoder.readDouble()
                    )
                }
            )
        case 22:
            return .geographicPosition(
                try valid {
                    try GeographicPosition(
                        latitude: decoder.readDouble(),
                        longitude: decoder.readDouble(),
                        ellipsoidalHeightInMeters: decoder.readDouble()
                    )
                }
            )
        case 23:
            return .vector(try readVector(from: &decoder))
        case 24:
            return .uuid(
                DatabaseTypes.UUID(
                    high: try decoder.readUInt64(),
                    low: try decoder.readUInt64()
                )
            )
        case 25:
            let count = try decoder.readCount()
            var values: [FieldValue] = []
            values.reserveCapacity(count)
            for _ in 0..<count {
                values.append(
                    try decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                        try read(from: &child)
                    }
                )
            }
            return .array(values)
        case 26:
            return .object(try readFieldObject(from: &decoder))
        case 27:
            let entity = try decoder.readString()
            let identifier = try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readReferenceIdentifier(from: &child)
            }
            let partitions = try readFieldObject(from: &decoder)
            return .reference(
                try valid {
                    try EntityReference(
                        entity: entity,
                        id: identifier,
                        partitions: partitions
                    )
                }
            )
        case 28:
            let bytes = try decoder.readBytes()
            let limits: RDFTermStorageLimits
            do {
                limits = try RDFTermStorageLimits(
                    maximumBytes: decoder.limits.maximumByteStringBytes,
                    maximumDepth: decoder.limits.maximumNestingDepth,
                    maximumObjectCount: decoder.limits.maximumCollectionCount
                )
            } catch {
                throw .invalidRDFTermLimits(error)
            }
            do {
                return .rdfTerm(try RDFTermStorageFormat.decode(bytes, limits: limits))
            } catch {
                throw .invalidRDFTerm(error)
            }
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    private static func readCivilTime(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> CivilTime {
        try valid {
            try CivilTime(
                hour: decoder.readUInt8(),
                minute: decoder.readUInt8(),
                second: decoder.readUInt8(),
                nanoseconds: decoder.readUInt32()
            )
        }
    }

    private static func skipCivilTime(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        _ = try decoder.readUInt8()
        _ = try decoder.readUInt8()
        _ = try decoder.readUInt8()
        _ = try decoder.readUInt32()
    }

    private static func retainedObjectFootprint(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> UInt64 {
        let count = try decoder.readCount()
        var total = try adding(
            UInt64(MemoryLayout<FieldObject>.stride + 64),
            try multiplied(64, by: UInt64(count))
        )
        for _ in 0..<count {
            let keyByteCount = try decoder.readValidatedStringBytes().count
            total = try adding(total, UInt64(keyByteCount))
            total = try adding(
                total,
                decoder.readLengthPrefixed {
                    (child: inout StorageFrameDecoder) throws(
                        StorageFrameError
                    ) -> UInt64 in
                    try retainedFootprint(from: &child)
                }
            )
        }
        return total
    }

    private static func retainedFootprint(
        of object: FieldObject
    ) throws -> UInt64 {
        var total = try DatabaseIntermediateFootprint(
            bytes: UInt64(MemoryLayout<FieldObject>.stride + 64)
        ).adding(
            try DatabaseIntermediateFootprint(bytes: 64).multiplied(
                by: UInt64(object.count)
            )
        )
        for field in object.fields {
            total = try total.adding(
                DatabaseIntermediateFootprint(
                    bytes: UInt64(field.key.utf8.count)
                )
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: try retainedFootprint(of: field.value)
                )
            )
        }
        return total.bytes
    }

    private static func retainedFootprint(
        of identifier: ReferenceIdentifier
    ) throws -> UInt64 {
        let base: UInt64 = 64
        switch identifier {
        case .bool, .int8, .int16, .int32, .int64,
             .uint8, .uint16, .uint32, .uint64, .uuid:
            return base
        case .string(let string):
            return try addingFootprints(base, UInt64(string.utf8.count))
        case .bytes(let bytes):
            return try addingFootprints(base, UInt64(bytes.count))
        case .composite(let components):
            var total = try DatabaseIntermediateFootprint(
                bytes: base
                    + UInt64(MemoryLayout<[ReferenceIdentifier]>.stride)
            ).adding(
                try DatabaseIntermediateFootprint(bytes: 64).multiplied(
                    by: UInt64(components.count)
                )
            )
            for component in components {
                total = try total.adding(
                    DatabaseIntermediateFootprint(
                        bytes: try retainedFootprint(of: component)
                    )
                )
            }
            return total.bytes
        }
    }

    private static func addingFootprints(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws -> UInt64 {
        try DatabaseIntermediateFootprint(bytes: lhs).adding(
            DatabaseIntermediateFootprint(bytes: rhs)
        ).bytes
    }

    private static func retainedReferenceIdentifierFootprint(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> UInt64 {
        let base: UInt64 = 64
        switch try decoder.readUInt8() {
        case 0:
            _ = try decoder.readBool(); return base
        case 1:
            _ = try decoder.readInt8(); return base
        case 2:
            _ = try decoder.readInt16(); return base
        case 3:
            _ = try decoder.readInt32(); return base
        case 4:
            _ = try decoder.readInt64(); return base
        case 5:
            _ = try decoder.readUInt8(); return base
        case 6:
            _ = try decoder.readUInt16(); return base
        case 7:
            _ = try decoder.readUInt32(); return base
        case 8:
            _ = try decoder.readUInt64(); return base
        case 9:
            return try adding(
                base,
                UInt64(decoder.readValidatedStringBytes().count)
            )
        case 10:
            return try adding(base, UInt64(decoder.readBytes().count))
        case 11:
            _ = try decoder.readUInt64()
            _ = try decoder.readUInt64()
            return base
        case 12:
            let count = try decoder.readCount()
            guard count > 0 else { throw .invalidValue }
            var total = try adding(
                base + UInt64(MemoryLayout<[ReferenceIdentifier]>.stride),
                try multiplied(64, by: UInt64(count))
            )
            for _ in 0..<count {
                total = try adding(
                    total,
                    decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(
                            StorageFrameError
                        ) -> UInt64 in
                        try retainedReferenceIdentifierFootprint(from: &child)
                    }
                )
            }
            return total
        case let tag:
            throw .invalidReferenceIdentifierTag(tag)
        }
    }

    private static func adding(
        _ lhs: UInt64,
        _ rhs: UInt64
    ) throws(StorageFrameError) -> UInt64 {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else { throw .byteCountOverflow }
        return result
    }

    private static func multiplied(
        _ value: UInt64,
        by multiplier: UInt64
    ) throws(StorageFrameError) -> UInt64 {
        let (result, overflow) = value.multipliedReportingOverflow(
            by: multiplier
        )
        guard !overflow else { throw .byteCountOverflow }
        return result
    }

    private static func readFieldObject(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> FieldObject {
        let count = try decoder.readCount()
        var fields: [(key: String, value: FieldValue)] = []
        fields.reserveCapacity(count)
        for _ in 0..<count {
            let key = try decoder.readString()
            let value = try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try read(from: &child)
            }
            fields.append((key: key, value: value))
        }
        return try valid {
            try FieldObject(fields)
        }
    }

    private static func readReferenceIdentifier(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> ReferenceIdentifier {
        switch try decoder.readUInt8() {
        case 0:
            return .bool(try decoder.readBool())
        case 1:
            return .int8(try decoder.readInt8())
        case 2:
            return .int16(try decoder.readInt16())
        case 3:
            return .int32(try decoder.readInt32())
        case 4:
            return .int64(try decoder.readInt64())
        case 5:
            return .uint8(try decoder.readUInt8())
        case 6:
            return .uint16(try decoder.readUInt16())
        case 7:
            return .uint32(try decoder.readUInt32())
        case 8:
            return .uint64(try decoder.readUInt64())
        case 9:
            return .string(try decoder.readString())
        case 10:
            return .bytes(try decoder.readBytes())
        case 11:
            return .uuid(
                DatabaseTypes.UUID(
                    high: try decoder.readUInt64(),
                    low: try decoder.readUInt64()
                )
            )
        case 12:
            let count = try decoder.readCount()
            var components: [ReferenceIdentifier] = []
            components.reserveCapacity(count)
            for _ in 0..<count {
                components.append(
                    try decoder.readLengthPrefixed {
                        (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                        try readReferenceIdentifier(from: &child)
                    }
                )
            }
            guard !components.isEmpty else {
                throw .invalidValue
            }
            return .composite(components)
        case let tag:
            throw .invalidReferenceIdentifierTag(tag)
        }
    }

    private static func readVector(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Vector {
        let tag = try decoder.readUInt8()
        let count = try decoder.readCount()
        switch tag {
        case 0:
            var values: [Int8] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt8()) }
            return Vector(int8: values)
        case 1:
            var values: [Int16] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt16()) }
            return Vector(int16: values)
        case 2:
            var values: [Int32] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt32()) }
            return Vector(int32: values)
        case 3:
            var values: [Int64] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readInt64()) }
            return Vector(int64: values)
        case 4:
            var values: [UInt8] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt8()) }
            return Vector(uint8: values)
        case 5:
            var values: [UInt16] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt16()) }
            return Vector(uint16: values)
        case 6:
            var values: [UInt32] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt32()) }
            return Vector(uint32: values)
        case 7:
            var values: [UInt64] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readUInt64()) }
            return Vector(uint64: values)
        case 8:
            var values: [Float] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readFloat()) }
            return try valid { try Vector(float32: values) }
        case 9:
            var values: [Double] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readDouble()) }
            return try valid { try Vector(float64: values) }
        default:
            throw .invalidValueTag(tag)
        }
    }

    private static func valid<Value>(
        _ body: () throws -> Value
    ) throws(StorageFrameError) -> Value {
        do {
            return try body()
        } catch {
            throw .invalidValue
        }
    }
}
