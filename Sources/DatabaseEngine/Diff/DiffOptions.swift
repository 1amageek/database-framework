import DatabaseTypes

/// Options that control model-diff execution.
public struct DiffOptions: Sendable {
    /// Whether arrays are compared element by element.
    public var detailedArrayDiff: Bool

    /// Largest array expanded into element-level changes.
    ///
    /// A non-positive value disables element-level expansion.
    public var maximumDetailedArrayCount: Int

    /// Field paths excluded from comparison.
    public var excludedFields: Set<String>

    /// Whether unchanged fields are included in the result.
    public var includesUnchangedFields: Bool

    /// Field-specific equality predicates.
    public var comparators: [
        String: @Sendable (FieldValue, FieldValue) -> Bool
    ]

    public init(
        detailedArrayDiff: Bool = false,
        maximumDetailedArrayCount: Int = 1_000,
        excludedFields: Set<String> = [],
        includesUnchangedFields: Bool = false,
        comparators: [
            String: @Sendable (FieldValue, FieldValue) -> Bool
        ] = [:]
    ) {
        self.detailedArrayDiff = detailedArrayDiff
        self.maximumDetailedArrayCount = maximumDetailedArrayCount
        self.excludedFields = excludedFields
        self.includesUnchangedFields = includesUnchangedFields
        self.comparators = comparators
    }

    public static let `default` = Self()

    public static let audit = Self(includesUnchangedFields: true)

    public static let debug = Self(
        detailedArrayDiff: true,
        includesUnchangedFields: true
    )

    public func enablingDetailedArrayDiff(
        maximumCount: Int? = nil
    ) -> Self {
        var options = self
        options.detailedArrayDiff = true
        if let maximumCount {
            options.maximumDetailedArrayCount = maximumCount
        }
        return options
    }

    public func excluding(_ fields: String...) -> Self {
        var options = self
        options.excludedFields.formUnion(fields)
        return options
    }

    public func includingUnchangedFields(_ include: Bool = true) -> Self {
        var options = self
        options.includesUnchangedFields = include
        return options
    }

    public func comparing(
        field: String,
        using comparator: @escaping @Sendable (FieldValue, FieldValue) -> Bool
    ) -> Self {
        var options = self
        options.comparators[field] = comparator
        return options
    }
}

extension DiffOptions: CustomStringConvertible {
    public var description: String {
        var components: [String] = []

        if detailedArrayDiff {
            components.append(
                "detailedArrayDiff(maximumCount: \(maximumDetailedArrayCount))"
            )
        }
        if !excludedFields.isEmpty {
            components.append(
                "excludedFields: [\(excludedFields.sorted().joined(separator: ", "))]"
            )
        }
        if includesUnchangedFields {
            components.append("includesUnchangedFields")
        }
        if !comparators.isEmpty {
            components.append(
                "comparators: [\(comparators.keys.sorted().joined(separator: ", "))]"
            )
        }

        return components.isEmpty
            ? "DiffOptions.default"
            : "DiffOptions(\(components.joined(separator: ", ")))"
    }
}
