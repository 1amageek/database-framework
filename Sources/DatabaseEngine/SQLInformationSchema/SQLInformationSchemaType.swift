import DatabaseKit

/// SQL-standard type attributes derived from admitted effective column types.
struct SQLInformationSchemaType {
    let name: String
    var precision: Int? = nil
    var radix: Int? = nil
    var scale: Int? = nil
    var length: Int? = nil
    var datetimePrecision: Int? = nil
    var userType: String? = nil
    var userSchema: String? = nil

    init(_ type: QueryColumnType) {
        switch type {
        case .sql(let type): self = Self(type)
        case .primitive(let type, let array):
            if array { self.init(name: "ARRAY"); return }
            switch type {
            case .bool: self = Self(.boolean)
            case .int16: self = Self(.smallint)
            case .int32: self = Self(.integer)
            case .int64: self = Self(.bigint)
            case .float32: self = Self(.real)
            case .float64: self = Self(.doublePrecision)
            case .decimal: self = Self(.decimal(precision: nil, scale: nil))
            case .string: self = Self(.text)
            case .bytes: self = Self(.blob)
            case .uuid: self = Self(.uuid)
            case .date: self = Self(.date)
            case .time: self = Self(.time(withTimeZone: false))
            case .timestamp: self = Self(.timestamp(withTimeZone: true))
            case .dateTime: self = Self(.timestamp(withTimeZone: false))
            case .timeSpan, .calendarPeriod: self = Self(.interval())
            default: self.init(name: "USER-DEFINED", userType: type.rawValue)
            }
        }
    }

    private init(name: String, precision: Int? = nil, radix: Int? = nil, scale: Int? = nil,
        length: Int? = nil, datetimePrecision: Int? = nil, userType: String? = nil, userSchema: String? = nil) {
        self.name = name; self.precision = precision; self.radix = radix; self.scale = scale
        self.length = length; self.datetimePrecision = datetimePrecision
        self.userType = userType; self.userSchema = userSchema
    }

    init(_ type: DataType) {
        switch type {
        case .boolean: self.init(name: "boolean")
        case .smallint: self.init(name: "smallint", precision: 16, radix: 2, scale: 0)
        case .integer: self.init(name: "integer", precision: 32, radix: 2, scale: 0)
        case .bigint: self.init(name: "bigint", precision: 64, radix: 2, scale: 0)
        case .real: self.init(name: "real", precision: 24, radix: 2)
        case .doublePrecision: self.init(name: "double precision", precision: 53, radix: 2)
        case .decimal(let precision, let scale): self.init(name: "numeric", precision: precision, radix: 10, scale: scale)
        case .char(let length): self.init(name: "character", length: length)
        case .varchar(let length): self.init(name: "character varying", length: length)
        case .text: self.init(name: "text")
        case .date: self.init(name: "date", datetimePrecision: 0)
        case .time(let zone, let precision): self.init(name: zone ? "time with time zone" : "time without time zone", datetimePrecision: precision ?? 6)
        case .timestamp(let zone, let precision): self.init(name: zone ? "timestamp with time zone" : "timestamp without time zone", datetimePrecision: precision ?? 6)
        case .interval(_, let precision): self.init(name: "interval", datetimePrecision: precision ?? 6)
        case .binary, .varbinary, .blob: self.init(name: "bytea")
        case .json: self.init(name: "json")
        case .jsonb: self.init(name: "jsonb")
        case .uuid: self.init(name: "uuid")
        case .array: self.init(name: "ARRAY")
        case .custom(let name, let schema): self.init(name: "USER-DEFINED", userType: name, userSchema: schema)
        case .composite: self.init(name: "USER-DEFINED")
        }
    }
}
