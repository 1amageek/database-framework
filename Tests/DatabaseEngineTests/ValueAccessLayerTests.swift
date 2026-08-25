#if !os(WASI)
import Foundation
import Testing
import TestHeartbeat
import DatabaseTypes
@testable import DatabaseKit
@testable import DatabaseEngine

private typealias QueryPredicate = DatabaseEngine.Predicate

@Persistable
private struct ValueAccessEntity {
    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int64 = 0
    var score: Double = 0
    var isActive: Bool = true
    var tag: String? = nil
    var embedding: [Float] = []
}

@Suite("Generated persisted field access", .heartbeat)
struct PersistedFieldAccessTests {
    @Test("Generated field identities select exact canonical values")
    func generatedFieldsSelectValues() throws {
        let entity = ValueAccessEntity(
            name: "Alice",
            age: 30,
            score: 95.5,
            isActive: false,
            tag: "vip",
            embedding: [1, 0.5, 0]
        )

        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.name.identity
            ) == .string("Alice")
        )
        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.age.identity
            ) == .int64(30)
        )
        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.score.identity
            ) == .float64(95.5)
        )
        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.isActive.identity
            ) == .bool(false)
        )
        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.tag.identity
            ) == .string("vip")
        )
        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.embedding.identity
            ) == .array([.float32(1), .float32(0.5), .float32(0)])
        )
    }

    @Test("Optional nil is represented explicitly as null")
    func nilOptionalIsNull() throws {
        let entity = ValueAccessEntity(tag: nil)

        #expect(
            try entity.persistedFieldValue(
                for: ValueAccessEntity.fields.tag.identity
            ) == .null
        )
    }

    @Test("Unknown field identity is not treated as a value")
    func unknownFieldReturnsNil() throws {
        let entity = ValueAccessEntity()
        let unknown = FieldIdentity(name: "missing", number: 999)

        #expect(try entity.persistedFieldValue(for: unknown) == nil)
    }

    @Test("Query rows round-trip canonical vector storage")
    func queryRowRoundTripsVector() throws {
        let entity = ValueAccessEntity(
            id: "vector-1",
            embedding: [1, 0.5, 0]
        )

        let row = try QueryRowCodec.encode(entity)
        let decoded = try QueryRowCodec.decode(
            row,
            as: ValueAccessEntity.self
        )

        #expect(
            row.fields["embedding"]
                == .array([.float32(1), .float32(0.5), .float32(0)])
        )
        #expect(decoded.id == entity.id)
        #expect(decoded.embedding == entity.embedding)
    }
}

@Suite("Compiled field predicate evaluation", .heartbeat)
struct CompiledFieldPredicateTests {
    private let alice = ValueAccessEntity(
        name: "Alice",
        age: 30,
        score: 95.5,
        isActive: true,
        tag: "vip"
    )
    private let bob = ValueAccessEntity(
        name: "Bob",
        age: 25,
        score: 80,
        isActive: false,
        tag: nil
    )

    @Test("Typed equality and ordering predicates use generated fields")
    func typedEqualityAndOrdering() throws {
        let equal: QueryPredicate<ValueAccessEntity> =
            ValueAccessEntity.fields.age == 30
        let lessThan: QueryPredicate<ValueAccessEntity> =
            ValueAccessEntity.fields.age < 28
        let greaterThan: QueryPredicate<ValueAccessEntity> =
            ValueAccessEntity.fields.score > 90

        guard case .comparison(let equalComparison) = equal,
              case .comparison(let lessThanComparison) = lessThan,
              case .comparison(let greaterThanComparison) = greaterThan
        else {
            Issue.record("Expected compiled field comparisons")
            return
        }

        #expect(try equalComparison.evaluate(on: alice))
        #expect(try !equalComparison.evaluate(on: bob))
        #expect(try !lessThanComparison.evaluate(on: alice))
        #expect(try lessThanComparison.evaluate(on: bob))
        #expect(try greaterThanComparison.evaluate(on: alice))
        #expect(try !greaterThanComparison.evaluate(on: bob))
    }

    @Test("String predicates preserve typed field identity")
    func stringPredicates() throws {
        let contains = QueryPredicate<ValueAccessEntity>.contains(
            "lic",
            in: ValueAccessEntity.fields.name
        )
        let prefix = QueryPredicate<ValueAccessEntity>.hasPrefix(
            "Bo",
            in: ValueAccessEntity.fields.name
        )
        let suffix = QueryPredicate<ValueAccessEntity>.hasSuffix(
            "ce",
            in: ValueAccessEntity.fields.name
        )

        guard case .comparison(let containsComparison) = contains,
              case .comparison(let prefixComparison) = prefix,
              case .comparison(let suffixComparison) = suffix
        else {
            Issue.record("Expected compiled string comparisons")
            return
        }

        #expect(try containsComparison.evaluate(on: alice))
        #expect(try !containsComparison.evaluate(on: bob))
        #expect(try !prefixComparison.evaluate(on: alice))
        #expect(try prefixComparison.evaluate(on: bob))
        #expect(try suffixComparison.evaluate(on: alice))
    }

    @Test("Collection and optional predicates use canonical values")
    func collectionAndOptionalPredicates() throws {
        let matches = QueryPredicate<ValueAccessEntity>.matchesAny(
            of: [25, 35, 45],
            at: ValueAccessEntity.fields.age
        )
        let isNil: QueryPredicate<ValueAccessEntity> =
            ValueAccessEntity.fields.tag == Optional<String>.self
        let isNotNil: QueryPredicate<ValueAccessEntity> =
            ValueAccessEntity.fields.tag != Optional<String>.self

        guard case .comparison(let matchesComparison) = matches,
              case .comparison(let nilComparison) = isNil,
              case .comparison(let nonNilComparison) = isNotNil
        else {
            Issue.record("Expected compiled collection and nil comparisons")
            return
        }

        #expect(try !matchesComparison.evaluate(on: alice))
        #expect(try matchesComparison.evaluate(on: bob))
        #expect(try !nilComparison.evaluate(on: alice))
        #expect(try nilComparison.evaluate(on: bob))
        #expect(try nonNilComparison.evaluate(on: alice))
        #expect(try !nonNilComparison.evaluate(on: bob))
    }

    @Test("Missing generated field identity throws")
    func missingFieldThrows() {
        let missing = Field<ValueAccessEntity, Int64>(
            identity: FieldIdentity(name: "missing", number: 999),
            type: .int64
        )
        let comparison = FieldComparison(
            field: missing,
            op: .equal,
            value: Int64(1)
        )

        do {
            _ = try comparison.evaluate(on: alice)
            Issue.record("Expected missing-field failure")
        } catch let error {
            #expect(
                error == .missingField(
                    entity: ValueAccessEntity.persistableType,
                    field: missing.identity
                )
            )
        }
    }
}

@Suite("Compiled field ordering", .heartbeat)
struct CompiledFieldOrderingTests {
    private let alice = ValueAccessEntity(
        name: "Alice",
        age: 30,
        score: 95.5
    )
    private let bob = ValueAccessEntity(
        name: "Bob",
        age: 25,
        score: 80
    )
    private let charlie = ValueAccessEntity(
        name: "Charlie",
        age: 30,
        score: 95.5
    )

    @Test("Ascending and descending directions invert comparison")
    func directionsInvertComparison() throws {
        let ascending = SortDescriptor(
            field: ValueAccessEntity.fields.age,
            order: .ascending
        )
        let descending = SortDescriptor(
            field: ValueAccessEntity.fields.age,
            order: .descending
        )

        #expect(
            try ascending.orderedComparison(bob, alice) == .lessThan
        )
        #expect(
            try ascending.orderedComparison(alice, bob) == .greaterThan
        )
        #expect(
            try descending.orderedComparison(bob, alice) == .greaterThan
        )
        #expect(
            try descending.orderedComparison(alice, bob) == .lessThan
        )
    }

    @Test("Equal values remain equal")
    func equalValuesRemainEqual() throws {
        let age = SortDescriptor(
            field: ValueAccessEntity.fields.age,
            order: .ascending
        )
        let score = SortDescriptor(
            field: ValueAccessEntity.fields.score,
            order: .descending
        )

        #expect(try age.orderedComparison(alice, charlie) == .equal)
        #expect(try score.orderedComparison(alice, charlie) == .equal)
    }

    @Test("Multiple descriptors provide deterministic lexicographic order")
    func multipleDescriptorsOrderDeterministically() throws {
        let descriptors = [
            SortDescriptor(
                field: ValueAccessEntity.fields.age,
                order: .ascending
            ),
            SortDescriptor(
                field: ValueAccessEntity.fields.name,
                order: .ascending
            ),
        ]
        let values = try [alice, bob, charlie].sorted { lhs, rhs in
            for descriptor in descriptors {
                let comparison = try descriptor.orderedComparison(
                    lhs,
                    rhs
                )
                if comparison == .lessThan {
                    return true
                }
                if comparison == .greaterThan {
                    return false
                }
            }
            return false
        }

        #expect(values.map { $0.name } == ["Bob", "Alice", "Charlie"])
    }
}
#endif
