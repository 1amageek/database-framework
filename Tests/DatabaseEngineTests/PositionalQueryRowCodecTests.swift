#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import Testing
@testable import DatabaseEngine

@Persistable
private struct PositionalRichModel {
    var id: String
    var numeric: QueryNumericValue
    var json: QueryJSONValue
    var jsonb: QueryJSONBValue
    var interval: QueryIntervalValue
    var array: QueryArray<Int32>
}

@Persistable
private struct PositionalPrimitiveModel {
    var id: String
    var count: Int32
}

@Suite("Positional public query result and declared model projection", .timeLimit(.minutes(1)))
struct PositionalQueryRowCodecTests {
    private var limits: QueryValueLimits {
        QueryValueLimits(maximumRetainedBytes: 16_384, maximumTotalNodes: 1_024,
                         maximumNestingDepth: 32, maximumCollectionElements: 256)
    }

    @Test("S01-C01 duplicate labels preserve ordered cells and reject dictionary projection")
    func duplicateLabels() throws {
        let columns = try [
            QueryColumn(number: 1, name: "same", type: .sql(.integer), isNullable: false),
            QueryColumn(number: 2, name: "same", type: .sql(.integer), isNullable: false)
        ]
        let row = try ExecutionQueryRow(values: [.primitive(.int32(1)), .primitive(.int32(2))], columns: columns)
        let response = try QueryResponse(columns: columns, rows: [row])
        #expect(response.rows[0].values == [.primitive(.int32(1)), .primitive(.int32(2))])
        #expect(throws: QueryRowCodecError.duplicateColumn("same")) {
            try row.namedValues(columns: columns)
        }
        #expect(throws: QueryRowCodecError.duplicateColumn("same")) {
            try row.primitiveFields(columns: columns)
        }
        #expect(throws: QueryRowCodecError.duplicateColumn("same")) {
            try QueryRowCodec.decode(row, columns: columns, as: PositionalPrimitiveModel.self)
        }
    }

    @Test("S01-C02 empty output retains explicitly declared rich and nullable columns")
    func emptyResultSchema() throws {
        let columns = try [
            QueryColumn(number: 1, name: "", type: .sql(.json), isNullable: true),
            QueryColumn(number: 2, name: "numeric", type: .sql(.decimal(precision: nil, scale: nil)), isNullable: false)
        ]
        let result = try QueryResponse(columns: columns)
        #expect(result.rows.isEmpty)
        #expect(result.columns == columns)
    }

    @Test("Public construction rejects nonconsecutive columns, wrong arity, wrong type and required null")
    func invalidPublicConstruction() throws {
        let column = try QueryColumn(number: 1, name: "count", type: .sql(.integer), isNullable: false)
        #expect(throws: QueryRowCodecError.arityMismatch(expected: 1, actual: 0)) {
            try ExecutionQueryRow(values: [], columns: [column])
        }
        #expect(throws: QueryRowCodecError.incompatibleValue(column: 1)) {
            try ExecutionQueryRow(values: [.primitive(.string("wrong"))], columns: [column])
        }
        #expect(throws: QueryRowCodecError.nullInRequiredColumn(column: 1)) {
            try ExecutionQueryRow(values: [.primitive(.null)], columns: [column])
        }
        let invalid = try QueryColumn(number: 2, name: "count", type: .sql(.integer), isNullable: false)
        #expect(throws: QueryValueError.invalidColumnNumber(2)) {
            try QueryResponse(columns: [invalid])
        }
        let other = try QueryColumn(number: 1, name: "count", type: .sql(.text), isNullable: false)
        let row = try ExecutionQueryRow(values: [.primitive(.string("text"))], columns: [other])
        #expect(throws: QueryRowCodecError.incompatibleValue(column: 1)) {
            try QueryResponse(columns: [column], rows: [row])
        }
    }

    @Test("S01-C04–C08 rich model projection preserves metadata through explicit adapters")
    func richModelRoundTrip() throws {
        let model = try richModel()
        let columns = try columns(for: PositionalRichModel.fieldSchemas)
        let row = try QueryRowCodec.encode(model, columns: columns)
        let decoded = try QueryRowCodec.decode(row, columns: columns, as: PositionalRichModel.self)
        #expect(decoded.id == model.id)
        #expect(decoded.numeric == model.numeric)
        #expect(decoded.numeric.displayScale == 6)
        #expect(decoded.json.bytes == model.json.bytes)
        #expect(decoded.jsonb == model.jsonb)
        #expect(decoded.interval == model.interval)
        #expect(decoded.array.arrayValue == model.array.arrayValue)
        #expect(decoded.array.arrayValue.dimensions[0].lowerBound == -2)
        let entity = try Schema.Entity(from: PositionalRichModel.self)
        let persisted = try QueryRowCodec.persistedModel(from: row, columns: columns, entity: entity)
        let reencoded = try QueryRowCodec.encode(persisted, entity: entity, columns: columns)
        #expect(reencoded.values == row.values)
        #expect(reencoded.version == row.version)
    }

    @Test("Rich cells cannot silently become primitive dictionary fields")
    func primitiveProjectionRejectsRich() throws {
        let columns = try columns(for: PositionalRichModel.fieldSchemas)
        let row = try QueryRowCodec.encode(richModel(), columns: columns)
        #expect(throws: QueryRowCodecError.self) { try row.primitiveFields(columns: columns) }
        #expect(try row.namedValues(columns: columns)["numeric"] == .numeric(richModel().numeric))
    }

    @Test("Primitive SQL columns reconstruct declared model storage without numeric coercion")
    func primitiveModelRoundTrip() throws {
        let columns = try [
            QueryColumn(number: 1, name: "count", type: .sql(.integer), isNullable: false),
            QueryColumn(number: 2, name: "id", type: .sql(.text), isNullable: false)
        ]
        let row = try ExecutionQueryRow(values: [.primitive(.int32(4)), .primitive(.string("p"))], columns: columns)
        let decoded = try QueryRowCodec.decode(row, columns: columns, as: PositionalPrimitiveModel.self)
        #expect(decoded.id == "p")
        #expect(decoded.count == 4)
        #expect(try row.primitiveFields(columns: columns) == ["count": .int32(4), "id": .string("p")])
    }

    @Test("Model projection rejects missing required fields, unknown names and incompatible descriptors")
    func invalidModelProjection() throws {
        let idColumns = try [QueryColumn(number: 1, name: "id", type: .sql(.text), isNullable: false)]
        let idRow = try ExecutionQueryRow(values: [.primitive(.string("p"))], columns: idColumns)
        #expect(throws: QueryRowCodecError.missingField(type: PositionalPrimitiveModel.persistableType, field: "count")) {
            try QueryRowCodec.decode(idRow, columns: idColumns, as: PositionalPrimitiveModel.self)
        }
        let unknown = try [QueryColumn(number: 1, name: "unknown", type: .sql(.text), isNullable: false)]
        #expect(throws: QueryRowCodecError.unknownField(type: PositionalPrimitiveModel.persistableType, field: "unknown")) {
            try QueryRowCodec.decode(idRow, columns: unknown, as: PositionalPrimitiveModel.self)
        }
        let wrong = try [
            QueryColumn(number: 1, name: "id", type: .sql(.text), isNullable: false),
            QueryColumn(number: 2, name: "count", type: .sql(.bigint), isNullable: false)
        ]
        let wrongRow = try ExecutionQueryRow(values: [.primitive(.string("p")), .primitive(.int64(4))], columns: wrong)
        #expect(throws: QueryRowCodecError.incompatibleField(type: PositionalPrimitiveModel.persistableType, field: "count")) {
            try QueryRowCodec.decode(wrongRow, columns: wrong, as: PositionalPrimitiveModel.self)
        }
    }

    @Test("S01-C09 primitive envelope lookalikes remain primitive and declared malformed envelopes fail")
    func noEnvelopeInference() throws {
        let object = try FieldObject([("kind", FieldValue.string("json")), ("bytes", .string("invalid"))])
        let primitiveEntity = try Schema.Entity(name: "Plain", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "payload", fieldNumber: 2, type: .object)
        ])
        let richEntity = try Schema.Entity(name: "Plain", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "payload", fieldNumber: 2, type: .object, queryType: .sql(.json))
        ])
        let persisted = try PersistedModel(entity: "Plain", fields: [
            PersistableField(number: 1, name: "id", value: .string("p")),
            PersistableField(number: 2, name: "payload", value: .object(object))
        ])
        let primitiveColumns = try columns(for: primitiveEntity.fields)
        let row = try QueryRowCodec.encode(persisted, entity: primitiveEntity, columns: primitiveColumns)
        #expect(row.values[1] == .primitive(.object(object)))
        #expect(throws: QueryValueError.self) {
            try QueryRowCodec.encode(persisted, entity: richEntity, columns: columns(for: richEntity.fields))
        }
        #expect(throws: QueryRowCodecError.incompatibleField(type: "Plain", field: "payload")) {
            try QueryRowCodec.persistedModel(from: row, columns: primitiveColumns, entity: richEntity)
        }
    }

    @Test("Positional source version hashing preserves canonical field-map bytes")
    func versionHashMatchesPrimitiveFields() throws {
        let fields = try [
            PersistableField(number: 2, name: "z", value: .int32(7)),
            PersistableField(number: 1, name: "a", value: .string("value"))
        ]
        let map: [String: FieldValue] = ["a": .string("value"), "z": .int32(7)]
        #expect(try PersistableVersionTokenCodec.token(for: fields) == PersistableVersionTokenCodec.token(for: map))
        #expect(try PersistableVersionTokenCodec.digest(fields: fields) == PersistableVersionTokenCodec.digest(fields: map))
        let duplicate = try PersistableField(number: 3, name: "a", value: .string("other"))
        #expect(throws: PersistableVersionTokenCodecError.self) {
            try PersistableVersionTokenCodec.token(for: fields + [duplicate])
        }
    }

    @Test("Positional fingerprint preserves duplicate ordinals and rich display metadata")
    func positionalFingerprintPreservesRichMetadata() throws {
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        let columns = try [
            QueryColumn(number: 1, name: "same", type: .sql(.decimal(precision: nil, scale: nil)), isNullable: false),
            QueryColumn(number: 2, name: "same", type: .sql(.decimal(precision: nil, scale: nil)), isNullable: false)
        ]
        let first = QueryValue.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 2))
        let second = QueryValue.numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 6))
        let row = try ExecutionQueryRow(values: [first, second], columns: columns)
        let reordered = try ExecutionQueryRow(values: [second, first], columns: columns)
        let fingerprint = try CanonicalRowFingerprint.compute(row, columns: columns, workMeter: meter)
        #expect(try fingerprint != CanonicalRowFingerprint.compute(reordered, columns: columns, workMeter: meter))
        let renamed = try [
            QueryColumn(number: 1, name: "different", type: columns[0].type, isNullable: false), columns[1]
        ]
        #expect(try fingerprint != CanonicalRowFingerprint.compute(row, columns: renamed, workMeter: meter))
        #expect(try fingerprint == CanonicalRowFingerprint.compute(row, columns: columns, workMeter: meter))

        // Physical fingerprints preserve carrier identity independently of SQL equality.
        let zero = try QueryTimeValue(microsecondsSinceMidnight: 0)
        let one = try QueryTimeValue(microsecondsSinceMidnight: 1)
        let endpoint = try QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000)
        let temporal: [(DataType, [QueryValue])] = [
            (.time(withTimeZone: false), [.primitive(.time(try CivilTime(hour: 0, minute: 0, second: 0))), .time(zero), .time(one), .time(endpoint)]),
            (.time(withTimeZone: true), [.timeWithTimeZone(.init(time: zero, utcOffsetSeconds: .min)),
                .timeWithTimeZone(.init(time: zero, utcOffsetSeconds: .max)), .timeWithTimeZone(.init(time: one, utcOffsetSeconds: .min))]),
            (.date, [.dateInfinity(.negative), .dateInfinity(.positive)]),
            (.timestamp(withTimeZone: false), [.timestampInfinity(.negative, withTimeZone: false), .timestampInfinity(.positive, withTimeZone: false)]),
            (.timestamp(withTimeZone: true), [.timestampInfinity(.negative, withTimeZone: true), .timestampInfinity(.positive, withTimeZone: true)]),
            (.interval(), [.intervalInfinity(.negative), .intervalInfinity(.positive)]),
        ]
        func digest(_ value: QueryValue, _ type: DataType) throws -> ByteString {
            let column = try QueryColumn(number: 1, name: "temporal", type: .sql(type), isNullable: true)
            return try CanonicalRowFingerprint.compute(ExecutionQueryRow(values: [value], columns: [column]), columns: [column], workMeter: meter)
        }
        for (type, values) in temporal {
            var scalars: Set<ByteString> = [], arrays: Set<ByteString> = []
            for value in values {
                let scalar = try digest(value, type)
                #expect(try scalar == digest(value, type))
                scalars.insert(scalar)
                let array = try QueryArrayValue(elementType: type, dimensions: [QueryArrayDimension(lowerBound: -1, length: 2)],
                    elements: [value, .primitive(.null)], limits: QueryValueLimits(maximumRetainedBytes: 4096,
                        maximumTotalNodes: 16, maximumNestingDepth: 8, maximumCollectionElements: 8))
                arrays.insert(try digest(.array(array), .array(type)))
            }
            #expect(scalars.count == values.count)
            #expect(arrays.count == values.count)
        }
        var descriptorFingerprints: Set<ByteString> = []
        let types = [DataType.interval()] + SQLIntervalFields.allCases.map { .interval(fields: $0) }
            + [.interval(precision: 0), .interval(precision: 6), .interval(fields: .dayToSecond, precision: 0), .interval(fields: .dayToSecond, precision: 6)]
        for type in types {
            descriptorFingerprints.insert(try digest(.primitive(.null), type))
            let empty = try QueryArrayValue(elementType: type, dimensions: [], elements: [],
                limits: QueryValueLimits(maximumRetainedBytes: 4096, maximumTotalNodes: 16,
                    maximumNestingDepth: 8, maximumCollectionElements: 8))
            descriptorFingerprints.insert(try digest(.array(empty), .array(type)))
        }
        #expect(descriptorFingerprints.count == types.count * 2)
    }

    @Test("Named RDF binding fingerprint retains its previous canonical bytes")
    func namedBindingFingerprintGolden() throws {
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        let fingerprint = try CanonicalRowFingerprint.compute(fields: ["x": .int32(7)], workMeter: meter)
        #expect(DatabaseTextFormatting.lowercaseHex(fingerprint) == "ff182749ff784ac839f7308a7a9073e0219f7edb2d145106cea1fa45cedd2a0a")
    }

    @Test("Composite fingerprints retain presence, field order and descriptor identity")
    func compositeFingerprintIdentity() throws {
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        func descriptor(_ name: String, nullable: Bool = true) throws -> QueryCompositeType {
            try QueryCompositeType(columns: [
                QueryColumn(number: 1, name: name, type: .sql(.integer), isNullable: nullable),
                QueryColumn(number: 2, name: name, type: .sql(.integer), isNullable: true),
            ], limits: limits)
        }
        let type = try descriptor("same")
        func digest(_ value: QueryValue, type: QueryCompositeType) throws -> ByteString {
            let column = try QueryColumn(number: 1, name: "row", type: .sql(.composite(type)), isNullable: true)
            return try CanonicalRowFingerprint.compute(ExecutionQueryRow(values: [value], columns: [column]),
                columns: [column], workMeter: meter)
        }
        let present = QueryValue.composite(try QueryCompositeValue(type: type,
            fields: [.primitive(.null), .primitive(.null)], limits: limits))
        #expect(try digest(present, type: type) != digest(.primitive(.null), type: type))
        let first = QueryValue.composite(try QueryCompositeValue(type: type,
            fields: [.primitive(.int32(1)), .primitive(.int32(2))], limits: limits))
        let reversed = QueryValue.composite(try QueryCompositeValue(type: type,
            fields: [.primitive(.int32(2)), .primitive(.int32(1))], limits: limits))
        #expect(try digest(first, type: type) != digest(reversed, type: type))
        let renamed = try descriptor("other")
        let required = try descriptor("same", nullable: false)
        #expect(try digest(.primitive(.null), type: type) != digest(.primitive(.null), type: renamed))
        #expect(try digest(.primitive(.null), type: type) != digest(.primitive(.null), type: required))
        #expect(try digest(first, type: type) == digest(first, type: type))
    }

    @Test("Opaque query payload leases retain claims until their last owner ends")
    func opaquePayloadLeaseLastOwner() throws {
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        var lease: DatabaseQueryPayloadLease?
        do {
            let reservation = try meter.reserveIntermediate(bytes: 128, at: .projection)
            lease = DatabaseQueryPayloadLease(retaining: reservation)
        }
        #expect(meter.retainedIntermediateBytes == 128)
        var retainedCopy = lease
        lease = nil
        #expect(try #require(retainedCopy).workMeter === meter)
        #expect(meter.retainedIntermediateBytes == 128)
        retainedCopy = nil
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Arbitrary index row slices reserve known backing capacity and origin leases retain that claim")
    func indexRowSliceCapacityAndOrigin() throws {
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        let owner = ByteString([UInt8](repeating: 7, count: 1_024))
        let slice = owner[0..<1]
        let capacity = try #require(slice.retainedByteCount)
        #expect(capacity >= 1_024)
        var result: IndexReadResult? = try IndexReadResult.build(
            workMeter: meter, ordering: .unordered, expectedCount: 1
        ) { rows in
            try rows.append(IndexReadRow(fields: ["payload": .bytes(slice)]))
        }
        let admittedBytes = meter.retainedIntermediateBytes
        #expect(admittedBytes >= UInt64(capacity))
        var origin = try #require(result).originPayloadLeases.first
        result = nil
        #expect(try #require(origin).workMeter === meter)
        #expect(meter.retainedIntermediateBytes == admittedBytes)
        origin = nil
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Deep JSONB fingerprint traversal remains bounded and preserves canonical leaf identity")
    func deepJSONBFingerprint() throws {
        let depth = 4_096
        let limits = QueryValueLimits(maximumRetainedBytes: 1_048_576, maximumTotalNodes: 16_384,
            maximumNestingDepth: UInt64(depth + 1), maximumCollectionElements: 16_384)
        func value(leaf: String) throws -> QueryJSONBValue {
            let text = String(repeating: "[", count: depth) + leaf + String(repeating: "]", count: depth)
            return try QueryJSONBValue(json: QueryJSONValue(bytes: ByteString(utf8: text), limits: limits), limits: limits)
        }
        let columns = try [QueryColumn(number: 1, name: "deep", type: .sql(.jsonb), isNullable: false)]
        let first = try ExecutionQueryRow(values: [.jsonb(value(leaf: "1"))], columns: columns)
        let second = try ExecutionQueryRow(values: [.jsonb(value(leaf: "2"))], columns: columns)
        let meter = DatabaseWorkMeter(unbounded: ExecutionBudget(timeoutMilliseconds: UInt32.max))
        let fingerprint = try CanonicalRowFingerprint.compute(first, columns: columns, workMeter: meter)
        #expect(try fingerprint == CanonicalRowFingerprint.compute(first, columns: columns, workMeter: meter))
        #expect(try fingerprint != CanonicalRowFingerprint.compute(second, columns: columns, workMeter: meter))
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func columns(for fields: [FieldSchema]) throws -> [QueryColumn] {
        try fields.enumerated().map { index, field in
            try QueryColumn(number: UInt32(index + 1), name: field.name,
                            type: field.queryType ?? .primitive(field.type, isArray: field.isArray),
                            isNullable: field.isOptional)
        }
    }

    private func richModel() throws -> PositionalRichModel {
        let json = try QueryJSONValue(bytes: ByteString(utf8: " {\"b\":1.00, \"a\":null, \"b\":2} "), limits: limits)
        return try PositionalRichModel(
            id: "rich",
            numeric: QueryNumericValue(value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 6),
            json: json,
            jsonb: QueryJSONBValue(json: json, limits: limits),
            interval: QueryIntervalValue(period: CalendarPeriod(months: -2, days: 3),
                                         time: TimeSpan(seconds: -1, nanoseconds: 500_000_000)),
            array: QueryArray(arrayValue: QueryArrayValue(
                elementType: .integer, dimensions: [QueryArrayDimension(lowerBound: -2, length: 2)],
                elements: [.primitive(.int32(1)), .primitive(.int32(2))], limits: limits
            ), limits: limits)
        )
    }
}
#endif
