# RelationshipIndex

`RelationshipIndex` owns relationship execution for persisted models. The
relationship declarations and `DatabaseReference` value live in
`database-kit`; loading, join execution, reverse lookup, and delete-rule
enforcement live in this target.

```text
database-kit/Relationship
    declaration + reference value
              |
              v
database-framework/RelationshipIndex
    validation + loading + join + delete rules
```

## Relationship declarations

An application model declares typed references. The model stores only the
reference; it does not store an eagerly loaded entity.

```swift
@Persistable
struct Customer {
    var name: String
}

@Persistable
struct Order {
    var total: Double

    @Relationship
    var customer: DatabaseReference<Customer>?
}
```

The schema macro emits the `RelationshipDescriptor` consumed by the runtime.
The runtime validates that the declared owner, related type, cardinality, and
delete rule match before executing relationship behavior.

## Loading a relation

Use `related` when only the related entity is needed:

```swift
let customer = try await context.related(order, \.customer)
```

Use `joining` when the owner and its related entities must be read at the same
transaction version:

```swift
let snapshots = try await context
    .fetch(Order.self)
    .joining(\.customer)
    .execute()

for snapshot in snapshots {
    let customer = try snapshot.ref(\.customer)
    print(snapshot.total, customer?.name ?? "unassigned")
}
```

A single owner can be loaded by its complete typed reference:

```swift
let snapshot = try await context.get(
    orderReference,
    joining: \.customer
)
```

## RelationshipSnapshot

`RelationshipSnapshot<Model>` is an execution result owned by
`RelationshipIndex`. It contains:

- the persisted model in `item`;
- only the relationships explicitly requested by the operation;
- relationships read at the same transaction version as the model.

Dynamic member lookup forwards model properties:

```swift
let total = snapshot.total
let sameTotal = snapshot.item.total
```

To-one and to-many accessors are typed:

```swift
let customer = try snapshot.ref(\.customer)
let orders = try snapshot.refs(\.orders)
```

The access contract distinguishes all relevant states:

| State | Result |
|---|---|
| relationship was not joined | `RelationshipSnapshotError.relationNotLoaded` |
| optional to-one was joined and has no reference | `nil` |
| to-many was joined and is empty | `[]` |
| stored cardinality does not match the key path | typed cardinality error |
| loaded entity type does not match the key path | typed related-type error |

The loaded relationship map is not public. Callers cannot inject untyped
values or make a snapshot claim a relationship was loaded when it was not.

To-many results keep the exact `[Related]` value in type-erased storage.
Reading it back uses the same copy-on-write array allocation; it does not
materialize an intermediate entity array.

## Join execution

```text
base query
    |
    v
collect ordered typed identities
    |
    v
deduplicate identities
    |
    v
load each identity in the active transaction
    |
    v
assemble typed loaded relationships
    |
    v
[RelationshipSnapshot<Model>]
```

The complete operation executes in one transaction. Missing or invalid
relationship descriptors, malformed stored references, and loaded type
mismatches are failures; they are not converted to empty results.

## Delete rules

Relationship mutation and delete handling use the compiled relationship
catalog. Supported declarations are:

- `cascade`
- `nullify`
- `deny`
- `noAction`

Rule application, relationship index updates, and the owner mutation execute
inside the same transaction. A failed rule or index update fails the mutation
rather than leaving a partially updated relationship state.

## Ownership boundary

This target does not own:

- `DatabaseReference`, `RelationshipDescriptor`, or declaration macros;
- query language models or wire operations;
- storage transaction primitives;
- application schemas.

Those remain in `database-kit`, `storage-kit`, or the application that owns
the schema.
