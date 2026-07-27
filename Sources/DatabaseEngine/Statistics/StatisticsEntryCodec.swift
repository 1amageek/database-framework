import DatabaseKit
import DatabaseTypes
import DatabaseTypesFoundation
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit

public enum StatisticsEntryCodec {
    private enum EntryKind: UInt8 {
        case table = 1
        case field = 2
        case index = 3
        case vector = 4
        case fullText = 5
        case spatial = 6
    }

    private static let magic: UInt32 = 0x5441_5453
    private static let version: UInt8 = 1

    public static func encode(_ value: TableStatisticsData) throws -> ByteString {
        try encode(kind: .table) { writer in
            writer.writeInt64(value.rowCount)
            writer.writeInt64(try int64(value.avgRowSize))
            writer.writeInt64(try int64(value.sampleSize))
            writer.writeDouble(value.sampleRate)
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeTable(_ bytes: ByteString) throws -> TableStatisticsData {
        var reader = try reader(for: bytes, expected: .table)
        let value = TableStatisticsData(
            rowCount: try reader.readInt64(),
            avgRowSize: try int(try reader.readInt64()),
            sampleSize: try int(try reader.readInt64()),
            sampleRate: try reader.readDouble(),
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    public static func encode(_ value: FieldStatisticsData) throws -> ByteString {
        try encode(kind: .field) { writer in
            try writer.writeString(value.fieldName)
            writer.writeInt64(value.distinctCount)
            writer.writeInt64(value.nullCount)
            writer.writeInt64(value.totalCount)
            try writeOptionalFieldValue(value.minValue, into: &writer)
            try writeOptionalFieldValue(value.maxValue, into: &writer)
            try writeOptionalMCV(value.mcv, into: &writer)
            try writeOptionalHistogram(value.histogram, into: &writer)
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeField(_ bytes: ByteString) throws -> FieldStatisticsData {
        var reader = try reader(for: bytes, expected: .field)
        let value = FieldStatisticsData(
            fieldName: try reader.readString(),
            distinctCount: try reader.readInt64(),
            nullCount: try reader.readInt64(),
            totalCount: try reader.readInt64(),
            minValue: try readOptionalFieldValue(from: &reader),
            maxValue: try readOptionalFieldValue(from: &reader),
            mcv: try readOptionalMCV(from: &reader),
            histogram: try readOptionalHistogram(from: &reader),
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    public static func encode(_ value: IndexStatisticsData) throws -> ByteString {
        try encode(kind: .index) { writer in
            try writer.writeString(value.indexName)
            writer.writeInt64(value.entryCount)
            writer.writeInt64(value.distinctKeyCount)
            writer.writeDouble(value.avgEntriesPerKey)
            writer.writeBool(value.sizeBytes != nil)
            if let sizeBytes = value.sizeBytes {
                writer.writeInt64(sizeBytes)
            }
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeIndex(_ bytes: ByteString) throws -> IndexStatisticsData {
        var reader = try reader(for: bytes, expected: .index)
        let indexName = try reader.readString()
        let entryCount = try reader.readInt64()
        let distinctKeyCount = try reader.readInt64()
        let avgEntriesPerKey = try reader.readDouble()
        let sizeBytes = try reader.readBool() ? try reader.readInt64() : nil
        let value = IndexStatisticsData(
            indexName: indexName,
            entryCount: entryCount,
            distinctKeyCount: distinctKeyCount,
            avgEntriesPerKey: avgEntriesPerKey,
            sizeBytes: sizeBytes,
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    public static func encode(_ value: VectorStatisticsData) throws -> ByteString {
        try encode(kind: .vector) { writer in
            try writer.writeString(value.indexName)
            writer.writeInt64(value.vectorCount)
            writer.writeInt64(try int64(value.dimensions))
            writer.writeDouble(value.avgL2Norm)
            writer.writeDouble(value.stdDevL2Norm)
            writer.writeBool(value.normBuckets != nil)
            if let buckets = value.normBuckets {
                try writer.writeCount(buckets.count)
                for bucket in buckets {
                    writer.writeDouble(bucket.minNorm)
                    writer.writeDouble(bucket.maxNorm)
                    writer.writeInt64(bucket.count)
                }
            }
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeVector(_ bytes: ByteString) throws -> VectorStatisticsData {
        var reader = try reader(for: bytes, expected: .vector)
        let indexName = try reader.readString()
        let vectorCount = try reader.readInt64()
        let dimensions = try int(try reader.readInt64())
        let avgL2Norm = try reader.readDouble()
        let stdDevL2Norm = try reader.readDouble()
        let normBuckets: [VectorStatisticsData.NormBucketData]?
        if try reader.readBool() {
            let count = try reader.readCount()
            var buckets: [VectorStatisticsData.NormBucketData] = []
            buckets.reserveCapacity(count)
            for _ in 0..<count {
                buckets.append(
                    VectorStatisticsData.NormBucketData(
                        minNorm: try reader.readDouble(),
                        maxNorm: try reader.readDouble(),
                        count: try reader.readInt64()
                    )
                )
            }
            normBuckets = buckets
        } else {
            normBuckets = nil
        }
        let value = VectorStatisticsData(
            indexName: indexName,
            vectorCount: vectorCount,
            dimensions: dimensions,
            avgL2Norm: avgL2Norm,
            stdDevL2Norm: stdDevL2Norm,
            normBuckets: normBuckets,
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    public static func encode(_ value: FullTextStatisticsData) throws -> ByteString {
        try encode(kind: .fullText) { writer in
            try writer.writeString(value.indexName)
            writer.writeInt64(value.totalDocs)
            writer.writeDouble(value.avgDocLength)
            writer.writeInt64(value.uniqueTerms)
            writer.writeBool(value.topTerms != nil)
            if let terms = value.topTerms {
                try writer.writeCount(terms.count)
                for term in terms {
                    try writer.writeString(term.term)
                    writer.writeInt64(term.docFreq)
                }
            }
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeFullText(_ bytes: ByteString) throws -> FullTextStatisticsData {
        var reader = try reader(for: bytes, expected: .fullText)
        let indexName = try reader.readString()
        let totalDocs = try reader.readInt64()
        let avgDocLength = try reader.readDouble()
        let uniqueTerms = try reader.readInt64()
        let topTerms: [FullTextStatisticsData.TermFrequency]?
        if try reader.readBool() {
            let count = try reader.readCount()
            var terms: [FullTextStatisticsData.TermFrequency] = []
            terms.reserveCapacity(count)
            for _ in 0..<count {
                terms.append(
                    FullTextStatisticsData.TermFrequency(
                        term: try reader.readString(),
                        docFreq: try reader.readInt64()
                    )
                )
            }
            topTerms = terms
        } else {
            topTerms = nil
        }
        let value = FullTextStatisticsData(
            indexName: indexName,
            totalDocs: totalDocs,
            avgDocLength: avgDocLength,
            uniqueTerms: uniqueTerms,
            topTerms: topTerms,
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    public static func encode(_ value: SpatialStatisticsData) throws -> ByteString {
        try encode(kind: .spatial) { writer in
            try writer.writeString(value.indexName)
            writer.writeInt64(value.entryCount)
            writer.writeBool(value.boundingBox != nil)
            if let box = value.boundingBox {
                writer.writeDouble(box.minLat)
                writer.writeDouble(box.minLon)
                writer.writeDouble(box.maxLat)
                writer.writeDouble(box.maxLon)
            }
            writer.writeInt64(value.cellCount)
            writer.writeDouble(value.avgCellDensity)
            writer.writeBool(value.hotCells != nil)
            if let cells = value.hotCells {
                try writer.writeCount(cells.count)
                for cell in cells {
                    writer.writeUInt64(cell)
                }
            }
            try writeTimestamp(value.timestamp, into: &writer)
        }
    }

    public static func decodeSpatial(_ bytes: ByteString) throws -> SpatialStatisticsData {
        var reader = try reader(for: bytes, expected: .spatial)
        let indexName = try reader.readString()
        let entryCount = try reader.readInt64()
        let boundingBox: SpatialStatisticsData.BoundingBox?
        if try reader.readBool() {
            boundingBox = SpatialStatisticsData.BoundingBox(
                minLat: try reader.readDouble(),
                minLon: try reader.readDouble(),
                maxLat: try reader.readDouble(),
                maxLon: try reader.readDouble()
            )
        } else {
            boundingBox = nil
        }
        let cellCount = try reader.readInt64()
        let avgCellDensity = try reader.readDouble()
        let hotCells: [UInt64]?
        if try reader.readBool() {
            let count = try reader.readCount()
            var cells: [UInt64] = []
            cells.reserveCapacity(count)
            for _ in 0..<count {
                cells.append(try reader.readUInt64())
            }
            hotCells = cells
        } else {
            hotCells = nil
        }
        let value = SpatialStatisticsData(
            indexName: indexName,
            entryCount: entryCount,
            boundingBox: boundingBox,
            cellCount: cellCount,
            avgCellDensity: avgCellDensity,
            hotCells: hotCells,
            timestamp: try date(from: &reader)
        )
        try reader.ensureFullyRead()
        return value
    }

    private static func encode(
        kind: EntryKind,
        _ body: (inout StorageFrameEncoder) throws -> Void
    ) throws -> ByteString {
        let encoded = try StorageFrameEncoder.encodeReportingFailure {
            writer in
            writer.writeUInt32(magic)
            writer.writeUInt8(version)
            writer.writeUInt8(kind.rawValue)
            try body(&writer)
        }
        return encoded
    }

    private static func reader(
        for bytes: ByteString,
        expected kind: EntryKind
    ) throws -> StorageFrameDecoder {
        var reader = try StorageFrameDecoder(bytes)
        guard try reader.readUInt32() == magic else {
            throw StatisticsStorageError.invalidMagic
        }
        let decodedVersion = try reader.readUInt8()
        guard decodedVersion == version else {
            throw StatisticsStorageError.unsupportedVersion(decodedVersion)
        }
        let decodedKind = try reader.readUInt8()
        guard decodedKind == kind.rawValue else {
            throw StatisticsStorageError.unexpectedEntryKind(
                expected: kind.rawValue,
                actual: decodedKind
            )
        }
        return reader
    }

    private static func writeOptionalFieldValue(
        _ value: FieldValue?,
        into writer: inout StorageFrameEncoder
    ) throws {
        writer.writeBool(value != nil)
        if let value {
            try StorageValueEncoder.write(value, into: &writer)
        }
    }

    private static func readOptionalFieldValue(
        from reader: inout StorageFrameDecoder
    ) throws -> FieldValue? {
        guard try reader.readBool() else {
            return nil
        }
        return try StorageValueDecoder.read(from: &reader)
    }

    private static func writeOptionalMCV(
        _ value: MostCommonValues?,
        into writer: inout StorageFrameEncoder
    ) throws {
        writer.writeBool(value != nil)
        guard let value else {
            return
        }
        try writer.writeCount(value.entries.count)
        for entry in value.entries {
            try StorageValueEncoder.write(entry.value, into: &writer)
            writer.writeDouble(entry.frequency)
            writer.writeInt64(entry.count)
        }
        try writeTimestamp(value.timestamp, into: &writer)
    }

    private static func readOptionalMCV(
        from reader: inout StorageFrameDecoder
    ) throws -> MostCommonValues? {
        guard try reader.readBool() else {
            return nil
        }
        let count = try reader.readCount()
        var entries: [MostCommonValues.Entry] = []
        entries.reserveCapacity(count)
        for _ in 0..<count {
            let value = try StorageValueDecoder.read(from: &reader)
            entries.append(
                MostCommonValues.Entry(
                    value: value,
                    frequency: try reader.readDouble(),
                    count: try reader.readInt64()
                )
            )
        }
        return MostCommonValues(
            entries: entries,
            timestamp: try date(from: &reader)
        )
    }

    private static func writeOptionalHistogram(
        _ value: Histogram?,
        into writer: inout StorageFrameEncoder
    ) throws {
        writer.writeBool(value != nil)
        guard let value else {
            return
        }
        try writer.writeCount(value.buckets.count)
        for bucket in value.buckets {
            try StorageValueEncoder.write(
                bucket.lowerBound,
                into: &writer
            )
            try StorageValueEncoder.write(
                bucket.upperBound,
                into: &writer
            )
            writer.writeInt64(bucket.count)
            writer.writeInt64(bucket.distinctCount)
        }
        writer.writeInt64(value.totalCount)
        writer.writeInt64(value.nullCount)
        writer.writeInt64(value.distinctCount)
        try writeTimestamp(value.timestamp, into: &writer)
    }

    private static func readOptionalHistogram(
        from reader: inout StorageFrameDecoder
    ) throws -> Histogram? {
        guard try reader.readBool() else {
            return nil
        }
        let count = try reader.readCount()
        var buckets: [Histogram.Bucket] = []
        buckets.reserveCapacity(count)
        for _ in 0..<count {
            let lowerBound = try StorageValueDecoder.read(from: &reader)
            let upperBound = try StorageValueDecoder.read(from: &reader)
            buckets.append(
                Histogram.Bucket(
                    lowerBound: lowerBound,
                    upperBound: upperBound,
                    count: try reader.readInt64(),
                    distinctCount: try reader.readInt64()
                )
            )
        }
        return Histogram(
            buckets: buckets,
            totalCount: try reader.readInt64(),
            nullCount: try reader.readInt64(),
            distinctCount: try reader.readInt64(),
            timestamp: try date(from: &reader)
        )
    }

    private static func writeTimestamp(
        _ date: Date,
        into writer: inout StorageFrameEncoder
    ) throws {
        let timestamp: Timestamp
        do {
            timestamp = try Timestamp(date)
        } catch {
            throw StatisticsStorageError.invalidTimestamp
        }
        writer.writeInt64(timestamp.secondsSinceUnixEpoch)
        writer.writeUInt32(timestamp.nanoseconds)
    }

    private static func date(
        from reader: inout StorageFrameDecoder
    ) throws -> Date {
        let seconds = try reader.readInt64()
        let nanoseconds = try reader.readUInt32()
        let timestamp: Timestamp
        do {
            timestamp = try Timestamp(
                secondsSinceUnixEpoch: seconds,
                nanoseconds: nanoseconds
            )
        } catch {
            throw StatisticsStorageError.invalidTimestamp
        }
        return Date(timestamp)
    }

    private static func int64(_ value: Int) throws -> Int64 {
        guard let converted = Int64(exactly: value) else {
            throw StatisticsStorageError.integerOutOfRange
        }
        return converted
    }

    private static func int(_ value: Int64) throws -> Int {
        guard let converted = Int(exactly: value) else {
            throw StatisticsStorageError.integerOutOfRange
        }
        return converted
    }
}
