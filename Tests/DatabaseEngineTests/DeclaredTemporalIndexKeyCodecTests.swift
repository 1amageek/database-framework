import DatabaseKit
import DatabaseTypes
import ScalarIndex
import StorageKit
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Declared temporal Directory and index adaptation")
struct DeclaredTemporalIndexKeyCodecTests {
    private func payload(_ value: QueryValue, type: QueryColumnType) throws -> FieldValue {
        try QueryFieldStorageAdapter.encode(value, type: type,
            limits: QueryFieldStorageAdapter.inferredLimits(for: value, type: type))
    }

    private func families() throws -> [(FieldSchema, QueryValue)] {
        let date = try CivilDate(year: 2024, month: 1, day: 2)
        let time = try CivilTime(hour: 3, minute: 4, second: 5)
        return [
            (FieldSchema(name: "value", fieldNumber: 2, type: .date, queryType: .sql(.date)), .primitive(.date(date))),
            (FieldSchema(name: "value", fieldNumber: 2, type: .dateTime, queryType: .sql(.timestamp(withTimeZone: false))),
             .primitive(.dateTime(CivilDateTime(date: date, time: time)))),
            (FieldSchema(name: "value", fieldNumber: 2, type: .timestamp, queryType: .sql(.timestamp(withTimeZone: true))),
             .primitive(.timestamp(Timestamp(secondsSinceUnixEpoch: 0))))
        ]
    }

    private func infinity(_ sign: QueryTemporalInfinity, schema: FieldSchema) -> QueryValue {
        if schema.type == .date { return .dateInfinity(sign) }
        return .timestampInfinity(sign, withTimeZone: schema.type == .timestamp)
    }

    @Test("Finite keys keep their bytes and both infinities bracket the finite domain")
    func finiteKeysAndInfinityOrder() throws {
        for (schema, finite) in try families() {
            let type = try #require(schema.queryType)
            let raw = try payload(finite, type: type)
            let middle = try DeclaredTemporalIndexKeyCodec.encode(raw, schema: schema)
            #expect(Tuple([middle]).pack() == Tuple([try raw.toTupleElement()]).pack())
            let lower = try payload(infinity(.negative, schema: schema), type: type)
            let upper = try payload(infinity(.positive, schema: schema), type: type)
            let lowKey = try DeclaredTemporalIndexKeyCodec.encode(lower, schema: schema)
            let highKey = try DeclaredTemporalIndexKeyCodec.encode(upper, schema: schema)
            #expect(Tuple([lowKey]).pack().lexicographicallyPrecedes(Tuple([middle]).pack()))
            #expect(Tuple([middle]).pack().lexicographicallyPrecedes(Tuple([highKey]).pack()))
            #expect(try DeclaredTemporalIndexKeyCodec.decode(lowKey, schema: schema) == lower)
            #expect(try DeclaredTemporalIndexKeyCodec.decode(highKey, schema: schema) == upper)
            #expect(try DeclaredTemporalIndexKeyCodec.decode(middle, schema: schema) == raw)
        }
    }

    @Test("TIME24 has fixed key bytes while the primitive decoder remains strict")
    func timeEndpointAndCarrierEquality() throws {
        let schema = FieldSchema(name: "value", fieldNumber: 2, type: .time, queryType: .sql(.time(withTimeZone: false)))
        let type = try #require(schema.queryType)
        let midnight = FieldValue.time(try CivilTime(hour: 0, minute: 0, second: 0))
        let sqlMidnight = try payload(.time(QueryTimeValue(microsecondsSinceMidnight: 0)), type: type)
        #expect(Tuple([try DeclaredTemporalIndexKeyCodec.encode(midnight, schema: schema)]).pack()
            == Tuple([try DeclaredTemporalIndexKeyCodec.encode(sqlMidnight, schema: schema)]).pack())
        let endpoint = try payload(.time(QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000)), type: type)
        let key = try DeclaredTemporalIndexKeyCodec.encode(endpoint, schema: schema)
        let expected = ByteString([0x01, 0x44, 0x56, 0x42, 0x01, 0x31, 0x03, 0x0A,
            0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x00])
        #expect(Tuple([key]).pack() == expected)
        #expect(try DeclaredTemporalIndexKeyCodec.decode(key, schema: schema) == endpoint)
        #expect(throws: FieldValueTupleCodecError.self) { try FieldValue(tupleElement: key) }
        let last = FieldValue.time(try CivilTime(hour: 23, minute: 59, second: 59, nanoseconds: 999_999_000))
        #expect(Tuple([try last.toTupleElement()]).pack().lexicographicallyPrecedes(expected))
    }

    @Test("Sentinel source values reject under SQL metadata and plain objects stay primitive")
    func descriptorAuthorityAndNoncollision() throws {
        let dateSchema = FieldSchema(name: "value", fieldNumber: 2, type: .date, queryType: .sql(.date))
        for year in [Int32.min, Int32.max] {
            let raw = FieldValue.date(try CivilDate(year: year, month: 1, day: 1))
            #expect(throws: FieldValueTupleCodecError.self) {
                try DeclaredTemporalIndexKeyCodec.encode(raw, schema: dateSchema)
            }
        }
        let object = try payload(.dateInfinity(.positive), type: .sql(.date))
        let primitive = FieldSchema(name: "value", fieldNumber: 2, type: .object)
        let element = try DeclaredTemporalIndexKeyCodec.encode(object, schema: primitive)
        #expect(try DeclaredTemporalIndexKeyCodec.decode(element, schema: primitive) == object)
        let wrong = FieldSchema(name: "value", fieldNumber: 2, type: .dateTime, queryType: .sql(.timestamp(withTimeZone: false)))
        #expect(throws: FieldValueTupleCodecError.self) { try DeclaredTemporalIndexKeyCodec.encode(object, schema: wrong) }
    }

    @Test("Directory extensions preserve physical identity and canonical spellings")
    func directoryTemporalGoldenVectors() throws {
        for (schema, finite) in try families() {
            let type = try #require(schema.queryType)
            let raw = try payload(finite, type: type)
            #expect(try DirectoryComponentCodec.encode(raw, schema: schema) == DirectoryComponentCodec.encode(raw))
            let tag = schema.type == .date ? "qdi" : (schema.type == .dateTime ? "qdti" : "qtsi")
            for (sign, token) in [(QueryTemporalInfinity.negative, "n"), (.positive, "p")] {
                let value = try payload(infinity(sign, schema: schema), type: type)
                let expected = "\(tag)-\(token)"
                #expect(try DirectoryComponentCodec.encode(value, schema: schema) == expected)
                #expect(try DirectoryComponentCodec.decode(expected, schema: schema) == value)
                #expect(DirectoryComponentCodec.isCanonicalImage(expected))
                #expect(throws: DirectoryComponentCodecError.self) { try DirectoryComponentCodec.encode(value) }
            }
        }
        let schema = FieldSchema(name: "value", fieldNumber: 2, type: .time, queryType: .sql(.time(withTimeZone: false)))
        for micros: Int64 in [0, 1, 86_400_000_000] {
            let raw = try payload(.time(QueryTimeValue(microsecondsSinceMidnight: micros)), type: .sql(.time(withTimeZone: false)))
            #expect(try DirectoryComponentCodec.encode(raw, schema: schema) == "qtime-\(micros)")
            #expect(try DirectoryComponentCodec.decode("qtime-\(micros)", schema: schema) == raw)
        }
        let primitive = FieldValue.time(try CivilTime(hour: 0, minute: 0, second: 0))
        #expect(try DirectoryComponentCodec.encode(primitive, schema: schema) == "t-0.0.0.0")
        for invalid in ["qtime-00", "qtime-86400000001", "qdi-x"] {
            #expect(!DirectoryComponentCodec.isCanonicalImage(invalid))
        }
        #expect(throws: DirectoryComponentCodecError.self) {
            try DirectoryComponentCodec.decode("qtime-0", schema: FieldSchema(name: "value", fieldNumber: 2, type: .time))
        }
    }

    @Test("Durable source scopes retain exact dynamic descriptors without a target schema")
    func sourceScopeRoundTrip() throws {
        let entity = try Schema.Entity(name: "Removed", identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "day", fieldNumber: 2, type: .date, queryType: .sql(.date))
        ], directoryComponents: [.staticPath("removed"), .dynamicField(fieldName: "day")], directoryLayer: .partition)
        let scope = DatabaseIndexStorageScope.entity(entity)
        let bytes = try DatabaseIndexStorageScopeCodec.encode(scope)
        let restored = try DatabaseIndexStorageScopeCodec.decode(bytes)
        #expect(restored == scope)
        #expect(restored.stableOrderingKey == scope.stableOrderingKey)
        let value = try payload(.dateInfinity(.negative), type: .sql(.date))
        #expect(try restored.accepts(partitions: FieldObject([("day", value)])))
        #expect(throws: StorageFrameError.self) {
            try DatabaseIndexStorageScopeCodec.decode(bytes[..<(bytes.endIndex - 1)])
        }
        #expect(throws: DatabaseIndexStorageScopeError.self) {
            try DatabaseIndexStorageScopeCodec.encode(.entity(name: "Removed",
                directoryComponents: entity.directoryComponents))
        }
        func intervalEntity(fields: SQLIntervalFields?, precision: Int?) throws -> Schema.Entity {
            try Schema.Entity(name: "IntervalMetadata", identifierType: .string, fields: [
                FieldSchema(name: "id", fieldNumber: 1, type: .string),
                FieldSchema(name: "period", fieldNumber: 2, type: .object,
                    queryType: .sql(.interval(fields: fields, precision: precision)))
            ])
        }
        for fields in [SQLIntervalFields?](arrayLiteral: nil) + SQLIntervalFields.allCases.map({ Optional.some($0) }) {
            let precisions: [Int?]
            switch fields {
            case nil, .second?, .dayToSecond?, .hourToSecond?, .minuteToSecond?:
                precisions = [nil, 0, 6]
            default:
                precisions = [nil]
                for precision in [0, 6] {
                    #expect(throws: SchemaEntityError.self) {
                        try intervalEntity(fields: fields, precision: precision)
                    }
                }
            }
            for precision in precisions {
                let entity = try intervalEntity(fields: fields, precision: precision)
                #expect(try SchemaEntityEntryCodec.decode(SchemaEntityEntryCodec.encode(entity)) == entity)
            }
        }
    }

    @Test("Real ordered maintenance, uniqueness, covering and build never expose sentinels")
    func orderedMaintainerLifecycle() async throws {
        let engine = InMemoryEngine()
        defer { engine.requestShutdown(); await engine.waitUntilShutdown() }
        var scenarios = try families().map { schema, finite in
            (schema, [infinity(.negative, schema: schema), finite, infinity(.positive, schema: schema)])
        }
        let timeSchema = FieldSchema(name: "value", fieldNumber: 2, type: .time, queryType: .sql(.time(withTimeZone: false)))
        scenarios.append((timeSchema, [
            .primitive(.time(try CivilTime(hour: 0, minute: 0, second: 0))),
            .time(try QueryTimeValue(microsecondsSinceMidnight: 3_600_000_000)),
            .time(try QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000))
        ]))
        for (schema, queryValues) in scenarios {
            let type = try #require(schema.queryType)
            let descriptor = try IndexDescriptor(entityName: "Temporal", declaration: .ordered(name: "by_value",
                keys: [.ascending(FieldIdentity(name: "value", number: 2))], unique: true),
                fieldSchemas: [FieldSchema(name: "id", fieldNumber: 1, type: .string), schema])
            let index = ResolvedIndex(descriptor: descriptor, rootExpression: FieldKeyExpression(fieldName: "value"))
            let subspace = Subspace(prefix: Tuple("temporal", schema.type.rawValue).pack())
            let maintainer = ScalarIndexMaintainer<PersistedModel>(index: index, subspace: subspace,
                idExpression: FieldKeyExpression(fieldName: "id"))
            let values = try queryValues.map { try payload($0, type: type) }
            let models = try values.enumerated().map { pair in
                try model(id: String(pair.offset), value: pair.element)
            }
            try await engine.withTransaction { transaction in
                for item in models { try await maintainer.updateIndex(oldItem: nil, newItem: item, transaction: transaction) }
                let range = subspace.range()
                let entries = try await transaction.collectRange(begin: range.begin, end: range.end, snapshot: false)
                #expect(entries.count == 3)
                for (position, entry) in entries.enumerated() {
                    let decoded = try ScalarIndexPhysicalEntryDecoder().decode(key: entry.0, in: subspace, index: index)
                    let expectedKeyValue: FieldValue
                    if schema.type == .time, position == 1 {
                        expectedKeyValue = .time(try CivilTime(hour: 1, minute: 0, second: 0))
                    } else { expectedKeyValue = values[position] }
                    #expect(decoded.indexedValues == [expectedKeyValue])
                    #expect(try CoveringValueBuilder.decode(entry.1, includedFieldNames: ["value"])["value"] == values[position])
                }
                let duplicateValue = schema.type == .time
                    ? try payload(.time(QueryTimeValue(microsecondsSinceMidnight: 0)), type: type) : values[0]
                let duplicate = try model(id: "duplicate", value: duplicateValue)
                let conflicts = try await maintainer.uniquenessConflicts(for: duplicate, id: Tuple("duplicate"), transaction: transaction)
                #expect(conflicts.count == 1)
                #expect(conflicts[0].conflictingValues == [values[0]])
                #expect(conflicts[0].existingPrimaryKey == Tuple("0"))
            }
            await #expect(throws: FieldValueTupleCodecError.self) {
                try await engine.withTransaction { transaction in
                    try await maintainer.updateIndex(oldItem: models[0], newItem: model(id: "0", value: .string("invalid")), transaction: transaction)
                }
            }
            try await engine.withTransaction { transaction in
                let range = subspace.range()
                #expect(try await transaction.collectRange(begin: range.begin, end: range.end, snapshot: false).count == 3)
                for item in models { try await maintainer.updateIndex(oldItem: item, newItem: nil, transaction: transaction) }
                #expect(try await transaction.collectRange(begin: range.begin, end: range.end, snapshot: false).isEmpty)
                for (position, item) in models.enumerated() {
                    try await maintainer.scanItem(item, id: Tuple(String(position)), transaction: transaction)
                }
                #expect(try await transaction.collectRange(begin: range.begin, end: range.end, snapshot: false).count == 3)
            }
        }
    }

    private func model(id: String, value: FieldValue) throws -> PersistedModel {
        try PersistedModel(entity: "Temporal", fields: [
            PersistableField(number: 1, name: "id", value: .string(id)),
            PersistableField(number: 2, name: "value", value: value)
        ])
    }
}
