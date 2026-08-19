import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

struct AggregationContributionFields {
    let grouping: [any TupleElement]
    let value: any TupleElement
}

/// Extracts canonical aggregation fields without applying sparse-index rules to
/// grouping keys. Null grouping values are encoded as canonical `FieldValue`
/// null elements and therefore form one real group. Only a null aggregate value
/// suppresses a SUM/AVG/MIN/MAX/DISTINCT/PERCENTILE contribution.
enum AggregationFieldExtractor {
    static func grouping<Item: PersistedEntityValue, FieldNames: Collection>(
        from item: Item,
        fieldNames: FieldNames,
        indexName: String
    ) throws -> [any TupleElement] where FieldNames.Element == String {
        var grouping: [any TupleElement] = []
        grouping.reserveCapacity(fieldNames.count)
        for fieldName in fieldNames {
            grouping.append(
                try fieldElement(
                    from: item,
                    fieldName: fieldName,
                    nullBehavior: .encode,
                    indexName: indexName
                )
            )
        }
        return grouping
    }

    /// Extracts the value before evaluating grouping fields. This ordering is
    /// required for sparse aggregates: an item whose aggregate value is null
    /// contributes nothing, even when a grouping path is also null.
    static func contribution<Item: PersistedEntityValue>(
        from item: Item,
        index: ResolvedIndex
    ) throws -> AggregationContributionFields? {
        let fieldNames = index.fieldNames
        guard !fieldNames.isEmpty,
              fieldNames.count == index.rootExpression.columnCount,
              let valueFieldName = fieldNames.last else {
            throw AggregationIndexError.invalidStructure(
                "Aggregation index '\(index.name)' has inconsistent field metadata"
            )
        }

        let value: any TupleElement
        do {
            value = try fieldElement(
                from: item,
                fieldName: valueFieldName,
                nullBehavior: .exclude,
                indexName: index.name
            )
        } catch AggregationNullValue.excluded {
            return nil
        }

        let grouping = try grouping(
            from: item,
            fieldNames: fieldNames.dropLast(),
            indexName: index.name
        )
        return AggregationContributionFields(
            grouping: grouping,
            value: value
        )
    }

    private enum NullBehavior {
        case encode
        case exclude
    }

    private enum AggregationNullValue: Error {
        case excluded
    }

    private static func fieldElement<Item: PersistedEntityValue>(
        from item: Item,
        fieldName: String,
        nullBehavior: NullBehavior,
        indexName: String
    ) throws -> any TupleElement {
        let value = try canonicalValue(from: item, at: fieldName)
        if value.isNull {
            switch nullBehavior {
            case .encode:
                return try FieldValue.null.toTupleElement()
            case .exclude:
                throw AggregationNullValue.excluded
            }
        }
        let elements = try DataAccess.extractField(
            from: item,
            keyPath: fieldName
        )

        guard elements.count == 1, let element = elements.first else {
            throw AggregationIndexError.invalidStructure(
                "Aggregation index '\(indexName)' field '\(fieldName)' must produce exactly one value"
            )
        }
        if let canonical = element as? CanonicalFieldValueTupleElement,
           canonical.prepared.value.isNull {
            switch nullBehavior {
            case .encode:
                return element
            case .exclude:
                throw AggregationNullValue.excluded
            }
        }
        return element
    }

    private static func canonicalValue<Item: PersistedEntityValue>(
        from item: Item,
        at fieldPath: String
    ) throws -> FieldValue {
        let components = fieldPath.split(
            separator: ".",
            omittingEmptySubsequences: false
        )
        guard let first = components.first,
              !first.isEmpty,
              var value = try item.persistedValue(
                forFieldNamed: String(first)
              ) else {
            throw DataAccessError.fieldNotFound(
                itemType: item.persistedEntityName,
                keyPath: fieldPath
            )
        }
        for component in components.dropFirst() {
            guard !component.isEmpty,
                  case .object(let object) = value,
                  let nested = object[String(component)] else {
                throw DataAccessError.fieldNotFound(
                    itemType: item.persistedEntityName,
                    keyPath: fieldPath
                )
            }
            value = nested
        }
        return value
    }
}
