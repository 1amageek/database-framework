import FoundationDB

/// Protocol for expressions that extract key values from records
///
/// KeyExpressions are used to define primary keys and index keys.
/// They use the Visitor pattern to extract values from records through RecordAccess.
public protocol KeyExpression: Sendable {
    /// Number of columns this expression produces
    var columnCount: Int { get }
}

// MARK: - Field Key Expression

/// Expression that extracts a single field from a record
public struct FieldKeyExpression: KeyExpression {
    public let fieldName: String

    public init(fieldName: String) {
        self.fieldName = fieldName
    }

    public var columnCount: Int { 1 }
}

// MARK: - Concatenate Key Expression

/// Expression that combines multiple expressions into a single key
public struct ConcatenateKeyExpression: KeyExpression {
    public let children: [KeyExpression]

    public init(children: [KeyExpression]) {
        self.children = children
    }

    public var columnCount: Int {
        return children.reduce(0) { $0 + $1.columnCount }
    }
}

// MARK: - Literal Key Expression

/// Expression that always returns a literal value
public struct LiteralKeyExpression<T: TupleElement>: KeyExpression {
    public let value: T

    public init(value: T) {
        self.value = value
    }

    public var columnCount: Int { 1 }
}

// MARK: - Empty Key Expression

/// Expression that returns an empty key
public struct EmptyKeyExpression: KeyExpression {
    public init() {}

    public var columnCount: Int { 0 }
}

// MARK: - Nest Expression

/// Expression that evaluates a child expression on a nested field
public struct NestExpression: KeyExpression {
    public let parentField: String
    public let child: KeyExpression

    public init(parentField: String, child: KeyExpression) {
        self.parentField = parentField
        self.child = child
    }

    public var columnCount: Int {
        return child.columnCount
    }
}

// MARK: - Range Key Expression

/// Expression that extracts a boundary from a Range-type field
public struct RangeKeyExpression: KeyExpression {
    public let fieldName: String
    public let component: RangeComponent

    public init(fieldName: String, component: RangeComponent) {
        self.fieldName = fieldName
        self.component = component
    }

    public var columnCount: Int { 1 }
}

// MARK: - Range Component

/// Component of a Range to extract (lowerBound or upperBound)
public enum RangeComponent: String, Sendable, Codable {
    case lowerBound
    case upperBound
}

// MARK: - KeyExpression Factory

extension KeyExpression where Self == FieldKeyExpression {
    /// Create a KeyExpression from a dot-notation string
    ///
    /// Handles both simple fields and nested paths:
    /// - "email" → FieldKeyExpression(fieldName: "email")
    /// - "address.city" → NestExpression(parentField: "address", child: FieldKeyExpression(fieldName: "city"))
    /// - "user.address.city" → NestExpression(parentField: "user", child: NestExpression(...))
    ///
    /// - Parameter dotNotation: Field path with dot notation (e.g., "address.city")
    /// - Returns: A KeyExpression representing the field path
    public static func from(dotNotation: String) -> KeyExpression {
        return KeyExpressionFactory.from(dotNotation: dotNotation)
    }
}

/// Factory for creating KeyExpressions from various inputs
public enum KeyExpressionFactory {
    /// Create a KeyExpression from a dot-notation string
    ///
    /// **Examples**:
    /// - "email" → FieldKeyExpression(fieldName: "email")
    /// - "address.city" → NestExpression(parentField: "address", child: FieldKeyExpression(fieldName: "city"))
    /// - "user.address.city" → NestExpression(parentField: "user", child: NestExpression(parentField: "address", child: FieldKeyExpression(fieldName: "city")))
    ///
    /// - Parameter dotNotation: Field path with dot notation (e.g., "address.city")
    /// - Returns: A KeyExpression representing the field path
    public static func from(dotNotation: String) -> KeyExpression {
        let components = dotNotation.split(separator: ".").map(String.init)
        return from(components: components)
    }

    /// Create a KeyExpression from an array of path components
    ///
    /// - Parameter components: Array of field names (e.g., ["address", "city"])
    /// - Returns: A KeyExpression representing the field path
    public static func from(components: [String]) -> KeyExpression {
        guard !components.isEmpty else {
            return EmptyKeyExpression()
        }

        if components.count == 1 {
            return FieldKeyExpression(fieldName: components[0])
        }

        // Build nested expression from right to left
        // ["user", "address", "city"] → Nest("user", Nest("address", Field("city")))
        var expression: KeyExpression = FieldKeyExpression(fieldName: components.last!)
        for i in stride(from: components.count - 2, through: 0, by: -1) {
            expression = NestExpression(parentField: components[i], child: expression)
        }
        return expression
    }

    /// Create a KeyExpression from an array of dot-notation keyPaths
    ///
    /// When multiple keyPaths are provided, they are concatenated.
    ///
    /// - Parameter keyPaths: Array of dot-notation strings (e.g., ["category", "price"])
    /// - Returns: A KeyExpression representing all fields
    public static func from(keyPaths: [String]) -> KeyExpression {
        guard !keyPaths.isEmpty else {
            return EmptyKeyExpression()
        }

        if keyPaths.count == 1 {
            return from(dotNotation: keyPaths[0])
        }

        // Multiple keyPaths: create ConcatenateKeyExpression
        let expressions = keyPaths.map { from(dotNotation: $0) }
        return ConcatenateKeyExpression(children: expressions)
    }
}
