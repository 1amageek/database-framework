import DatabaseKit
import DatabaseTypes
import StorageKit

/// Projects keys under compiled metadata; synthetic endpoints never become data.
package enum DeclaredTemporalIndexKeyCodec {
    package static func encode(
        _ payload: FieldValue, schema: FieldSchema
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        guard let type = temporalType(schema) else { return try payload.toTupleElement() }
        let value = try decoded(payload, schema: schema, type: type)
        switch value {
        case .primitive(let primitive): return try primitive.toTupleElement()
        case .time(let time):
            let micros = time.microsecondsSinceMidnight
            if micros == 86_400_000_000 { return FieldValueTupleCodec.sqlTimeEndpointKey() }
            do {
                let civil = try CivilTime(hour: UInt8(micros / 3_600_000_000),
                    minute: UInt8((micros / 60_000_000) % 60), second: UInt8((micros / 1_000_000) % 60),
                    nanoseconds: UInt32((micros % 1_000_000) * 1_000))
                return try FieldValue.time(civil).toTupleElement()
            } catch let error as FieldValueTupleCodecError { throw error }
            catch { throw .invalidDeclaredQueryValue(.incompatibleType) }
        case .dateInfinity(let sign):
            return try sentinel(for: type, sign: sign).toTupleElement()
        case .timestampInfinity(let sign, _):
            return try sentinel(for: type, sign: sign).toTupleElement()
        default: throw .invalidDeclaredQueryValue(.incompatibleType)
        }
    }

    package static func decode(
        _ element: any TupleElement, schema: FieldSchema
    ) throws(FieldValueTupleCodecError) -> FieldValue {
        guard let type = temporalType(schema) else { return try FieldValue(tupleElement: element) }
        if case .sql(.time(false, _)) = type, FieldValueTupleCodec.isSQLTimeEndpointKey(element) {
            do { return try stored(.time(QueryTimeValue(microsecondsSinceMidnight: 86_400_000_000)), type: type) }
            catch let error as FieldValueTupleCodecError { throw error }
            catch { throw .invalidDeclaredQueryValue(.incompatibleType) }
        }
        let primitive = try FieldValue(tupleElement: element)
        if case .sql(.date) = type {
            for sign in [QueryTemporalInfinity.negative, .positive] {
                if primitive == (try sentinel(for: type, sign: sign)) { return try stored(.dateInfinity(sign), type: type) }
            }
        } else if case .sql(.timestamp(let withTimeZone, _)) = type {
            for sign in [QueryTemporalInfinity.negative, .positive] {
                if primitive == (try sentinel(for: type, sign: sign)) {
                    return try stored(.timestampInfinity(sign, withTimeZone: withTimeZone), type: type)
                }
            }
        }
        _ = try decoded(primitive, schema: schema, type: type)
        return primitive
    }

    package static func requiresAdaptation(_ schema: FieldSchema) -> Bool {
        temporalType(schema) != nil
    }

    private static func temporalType(_ schema: FieldSchema) -> QueryColumnType? {
        guard let type = schema.queryType else { return nil }
        switch type {
        case .sql(.date), .sql(.time(false, _)), .sql(.timestamp): return type
        default: return nil
        }
    }

    private static func decoded(
        _ payload: FieldValue, schema: FieldSchema, type: QueryColumnType
    ) throws(FieldValueTupleCodecError) -> QueryValue {
        do throws(QueryValueError) {
            try QueryFieldStorageAdapter.validateDescriptor(type, fieldType: schema.type, isArray: schema.isArray)
            return try QueryFieldStorageAdapter.decode(payload, type: type,
                limits: QueryFieldStorageAdapter.inferredLimits(for: payload, type: type))
        } catch { throw .invalidDeclaredQueryValue(error) }
    }

    private static func stored(_ value: QueryValue, type: QueryColumnType) throws(FieldValueTupleCodecError) -> FieldValue {
        do throws(QueryValueError) {
            return try QueryFieldStorageAdapter.encode(value, type: type,
                limits: QueryFieldStorageAdapter.inferredLimits(for: value, type: type))
        } catch { throw .invalidDeclaredQueryValue(error) }
    }

    private static func sentinel(
        for type: QueryColumnType, sign: QueryTemporalInfinity
    ) throws(FieldValueTupleCodecError) -> FieldValue {
        let positive = sign == .positive
        do {
            switch type {
            case .sql(.date):
                return .date(try CivilDate(year: positive ? Int32.max : Int32.min, month: positive ? 12 : 1, day: positive ? 31 : 1))
            case .sql(.timestamp(false, _)):
                return .dateTime(CivilDateTime(
                    date: try CivilDate(year: positive ? Int32.max : Int32.min, month: positive ? 12 : 1, day: positive ? 31 : 1),
                    time: try CivilTime(hour: positive ? 23 : 0, minute: positive ? 59 : 0, second: positive ? 59 : 0,
                        nanoseconds: positive ? 999_999_999 : 0)))
            case .sql(.timestamp(true, _)):
                return .timestamp(Timestamp(secondsSinceUnixEpoch: positive ? Int64.max : Int64.min))
            default: throw FieldValueTupleCodecError.invalidDeclaredQueryValue(.incompatibleType)
            }
        } catch let error as FieldValueTupleCodecError { throw error }
        catch { throw .invalidDeclaredQueryValue(.incompatibleType) }
    }
}
