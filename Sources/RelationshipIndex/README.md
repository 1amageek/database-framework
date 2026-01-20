# RelationshipIndex

SwiftData-like relationship management with FK indexes, eager loading, and delete rule enforcement.

## Overview

RelationshipIndex provides type-safe relationship management between Persistable types using the `@Relationship` macro. It enables efficient cross-type queries, eager relationship loading via `joining()`, and automatic delete rule enforcement following SwiftData patterns.

**Features**:
- **@Relationship Macro**: Declarative relationship definition with delete rules
- **Automatic FK Indexing**: ScalarIndex generated for foreign key fields
- **Eager Loading**: Load related items with `joining()` in single query
- **Delete Rules**: cascade, nullify, deny, noAction enforcement
- **Snapshot API**: Type-safe access to loaded relationships

**Storage Layout**:
```
// FK Index (generated ScalarIndex)
Key: [indexSubspace]["{Type}_{relationship}"][fkValue]/[primaryKey]
Value: '' (empty)

// Reverse Index for Delete Rule Enforcement
Key: [indexSubspace]["{Type}_{relationship}"][relatedId]/[ownerId]
Value: '' (empty)

Example (Order.customerID -> Customer):
  [I]/RTestOrder_customer/["C001"]/["O001"] = ''
  [I]/RTestOrder_customer/["C001"]/["O002"] = ''

Example (Customer.orderIDs -> Order, To-Many):
  [I]/RTestCustomer_orders/["O001"]/["C001"] = ''
  [I]/RTestCustomer_orders/["O002"]/["C001"] = ''
```

**Relationship Model**:
```
┌─────────────────────────────────────────────────────────────────┐
│                    Relationship Types                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  To-One (Single FK):                                             │
│    @Relationship(Customer.self)                                  │
│    var customerID: String?       // Optional FK                  │
│                                                                  │
│    @Relationship(Customer.self)                                  │
│    var customerID: String        // Required FK                  │
│                                                                  │
│  To-Many (Array FK):                                             │
│    @Relationship(Order.self)                                     │
│    var orderIDs: [String] = []   // Array of FKs                │
│                                                                  │
│  Naming Convention:                                              │
│    customerID  → "customer" relationship                         │
│    orderIDs    → "orders" relationship                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Use Cases

### 1. Basic To-One Relationship

**Scenario**: Orders reference a single Customer.

```swift
@Persistable
struct Customer {
    var id: String = ULID().ulidString
    var name: String = ""
    var tier: String = "standard"
}

@Persistable
struct Order {
    var id: String = ULID().ulidString
    var total: Double = 0
    var status: String = "pending"

    // To-one relationship to Customer
    @Relationship(Customer.self)
    var customerID: String? = nil
}

// Create order with relationship
var order = Order(total: 99.99)
order.customerID = customer.id
context.insert(order)
try await context.save()

// Load related customer
let loadedOrder = try await context.model(for: orderId, as: Order.self)!
let customer = try await context.related(loadedOrder, \.customerID, as: Customer.self)
print(customer?.name)  // "Alice"
```

**Performance**: O(1) - Direct ID lookup for related item.

### 2. To-Many Relationship

**Scenario**: Customer has many Orders.

```swift
@Persistable
struct Customer {
    var id: String = ULID().ulidString
    var name: String = ""

    // To-many relationship to Order
    @Relationship(Order.self)
    var orderIDs: [String] = []
}

// Create customer with multiple orders
var customer = Customer(name: "Alice")
customer.orderIDs = [order1.id, order2.id, order3.id]
context.insert(customer)
try await context.save()

// Load related orders
let loadedCustomer = try await context.model(for: customerId, as: Customer.self)!
let orders = try await context.related(loadedCustomer, \.orderIDs, as: Order.self)
for order in orders {
    print("Order: \(order.total)")
}
```

**Performance**: O(k) where k = number of related items.

### 3. Eager Loading with joining()

**Scenario**: Load orders with customers in a single query.

```swift
// Fetch orders with customer data pre-loaded
let snapshots = try await context.fetch(Order.self)
    .where(\.status == "pending")
    .joining(\.customerID, as: Customer.self)
    .execute()

for snapshot in snapshots {
    // Access order properties directly
    print("Order: \(snapshot.total)")

    // Access related customer via ref()
    let customer = snapshot.ref(Customer.self, \.customerID)
    print("Customer: \(customer?.name ?? "N/A")")
}
```

**Batch Loading**: FK values are collected and related items are batch-loaded to minimize round trips.

### 4. To-Many Eager Loading

**Scenario**: Load customers with their orders in a single query.

```swift
// Fetch customers with orders pre-loaded
let snapshots = try await context.fetch(Customer.self)
    .joining(\.orderIDs, as: Order.self)
    .execute()

for snapshot in snapshots {
    print("Customer: \(snapshot.name)")

    // Access related orders via refs()
    let orders = snapshot.refs(Order.self, \.orderIDs)
    let total = orders.reduce(0) { $0 + $1.total }
    print("Total orders value: \(total)")
}
```

### 5. Get with Single Item Joining

**Scenario**: Fetch single item with relationship loaded.

```swift
// Get order with customer joined
let snapshot = try await context.get(
    Order.self,
    id: orderId,
    joining: \.customerID,
    as: Customer.self
)

if let order = snapshot {
    print("Order total: \(order.total)")
    let customer = order.ref(Customer.self, \.customerID)
    print("Customer: \(customer?.name ?? "N/A")")
}

// Get customer with orders joined
let customerSnapshot = try await context.get(
    Customer.self,
    id: customerId,
    joining: \.orderIDs,
    as: Order.self
)

if let customer = customerSnapshot {
    let orders = customer.refs(Order.self, \.orderIDs)
    print("Customer has \(orders.count) orders")
}
```

### 6. Delete Rule Enforcement

**Scenario**: Enforce referential integrity on delete.

```swift
@Persistable
struct Customer {
    var name: String = ""

    @Relationship(Order.self, deleteRule: .cascade)
    var orderIDs: [String] = []
}

@Persistable
struct Order {
    var total: Double = 0

    @Relationship(Customer.self, deleteRule: .deny)
    var customerID: String? = nil
}

// Cascade: Deleting customer also deletes all their orders
try await context.deleteEnforcingRelationshipRules(customer)
// Customer and all related orders are deleted

// Deny: Cannot delete customer if orders reference them
// Throws RelationshipError.deleteRuleDenied if orders exist

// Nullify: Set FK to nil on referencing items
@Relationship(Customer.self, deleteRule: .nullify)
var customerID: String? = nil
// Deleting customer sets order.customerID = nil

// NoAction: Do nothing (may leave orphan references)
@Relationship(Customer.self, deleteRule: .noAction)
var customerID: String? = nil
```

## Design Patterns

### Delete Rules

```
┌─────────────────────────────────────────────────────────────────┐
│                      Delete Rules                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  .cascade                                                        │
│    When: Deleting Customer                                       │
│    Effect: All Orders referencing Customer are also deleted     │
│    Use: Parent-child relationships (Order → LineItems)          │
│    ⚠️ Be careful with bidirectional cascade                     │
│                                                                  │
│  .deny                                                           │
│    When: Deleting Customer                                       │
│    Effect: Throws error if any Orders reference Customer        │
│    Use: Strong referential integrity (Department → Employees)   │
│                                                                  │
│  .nullify (default)                                              │
│    When: Deleting Customer                                       │
│    Effect: Set order.customerID = nil for all Orders            │
│    Use: Optional relationships (Order → Customer)               │
│                                                                  │
│  .noAction                                                       │
│    When: Deleting Customer                                       │
│    Effect: Do nothing, may leave orphan FKs                     │
│    Use: When cleanup is handled elsewhere                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Snapshot API

```
┌─────────────────────────────────────────────────────────────────┐
│                      Snapshot<T> API                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Properties:                                                     │
│    .item         → Access underlying T item                     │
│    .relations    → Dictionary of loaded relationships           │
│                                                                  │
│  Dynamic Member:                                                 │
│    snapshot.propertyName  → Same as snapshot.item.propertyName  │
│                                                                  │
│  To-One Access:                                                  │
│    snapshot.ref(Type.self, \.fkField) -> Type?                  │
│                                                                  │
│  To-Many Access:                                                 │
│    snapshot.refs(Type.self, \.fkArrayField) -> [Type]           │
│                                                                  │
│  Mutation (returns new Snapshot):                                │
│    snapshot.with(\.fkField, loadedAs: item)                     │
│    snapshot.with(\.fkArrayField, loadedAs: items)               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Index Generation

The `@Relationship` macro generates:

1. **ScalarIndex** on the FK field for efficient reverse lookups
2. **RelationshipDescriptor** for delete rule enforcement

```
┌─────────────────────────────────────────────────────────────────┐
│                  Macro-Generated Artifacts                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  @Persistable                                                    │
│  struct Order {                                                  │
│      @Relationship(Customer.self, deleteRule: .nullify)         │
│      var customerID: String? = nil                              │
│  }                                                               │
│                                                                  │
│  Generates:                                                      │
│                                                                  │
│  1. ScalarIndex named "Order_customer"                          │
│     Key: [I]/Order_customer/[customerID]/[orderId]              │
│                                                                  │
│  2. RelationshipDescriptor:                                      │
│     - name: "Order_customer"                                     │
│     - propertyName: "customerID"                                │
│     - relatedTypeName: "Customer"                               │
│     - deleteRule: .nullify                                      │
│     - isToMany: false                                           │
│     - relationshipPropertyName: "customer"                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Query Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                 Eager Loading Flow                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  context.fetch(Order.self)                                      │
│      .joining(\.customerID, as: Customer.self)                  │
│      .execute()                                                  │
│          │                                                       │
│          ▼                                                       │
│  1. Execute base query → [Order]                                │
│          │                                                       │
│          ▼                                                       │
│  2. Collect FK values → Set<CustomerID>                         │
│          │                                                       │
│          ▼                                                       │
│  3. Batch load customers → [CustomerID: Customer]               │
│          │                                                       │
│          ▼                                                       │
│  4. Build Snapshots with relations                              │
│          │                                                       │
│          ▼                                                       │
│  5. Return [Snapshot<Order>]                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Orphan FK Handling

Relationship indexes allow FKs to reference non-existent items:

```swift
// FK pointing to non-existent customer (allowed)
var order = Order(total: 99.99)
order.customerID = "nonexistent-customer"
context.insert(order)
try await context.save()  // Success

// related() returns nil for non-existent FK target
let customer = try await context.related(order, \.customerID, as: Customer.self)
// customer == nil

// Delete of referenced item leaves orphan FK
context.delete(customer)
try await context.save()
// order.customerID still contains the deleted customer ID
// related() now returns nil
```

## Error Handling

RelationshipIndex throws specific errors for invalid configurations or data:

```swift
public enum RelationshipIndexError: Error {
    /// Configuration not found for the index
    case configurationNotFound(indexName: String, modelType: String)

    /// FK field type is invalid (must be String or [String])
    case invalidForeignKeyType(fieldName: String, expectedType: String, actualType: String)

    /// Related field value is nil - cannot create index entry
    case relatedFieldIsNil(fieldName: String, relatedType: String)

    /// Field value cannot be converted to TupleElement
    case fieldNotConvertibleToTupleElement(fieldName: String, relatedType: String, actualType: String)

    /// Transaction is required for computing index keys
    case transactionRequired(indexName: String)
}
```

### Error Scenarios

| Error | Cause | Solution |
|-------|-------|----------|
| `invalidForeignKeyType` | FK field is not `String` (To-One) or `[String]` (To-Many) | Ensure FK field type matches relationship type |
| `relatedFieldIsNil` | Related item exists but indexed field is nil | Ensure indexed fields are non-nil or use Optional |
| `fieldNotConvertibleToTupleElement` | Field type cannot be used as index key | Use primitive types (String, Int, Double, etc.) |
| `transactionRequired` | Internal: non-transaction computeIndexKeys called | Use transaction-aware version (internal only) |

### Design Philosophy

- **Fail Fast**: Invalid configurations throw errors at index maintenance time
- **No Silent Failures**: Type mismatches and nil values cause explicit errors
- **Orphan FKs Allowed**: FK pointing to non-existent item is valid (returns nil on load)

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| @Relationship macro | ✅ Complete | To-One and To-Many support |
| ScalarIndex generation | ✅ Complete | Automatic FK indexing |
| RelationshipDescriptor | ✅ Complete | Delete rule metadata |
| related() API | ✅ Complete | To-One and To-Many loading |
| joining() API | ✅ Complete | Eager loading in queries |
| Snapshot<T> wrapper | ✅ Complete | ref() and refs() access |
| get() with joining | ✅ Complete | Single item with relation |
| Delete rule: cascade | ✅ Complete | Recursive deletion |
| Delete rule: nullify | ✅ Complete | FK set to nil |
| Delete rule: deny | ✅ Complete | Error if references exist |
| Delete rule: noAction | ✅ Complete | No enforcement |
| Inverse relationships | ❌ Not implemented | Manual bidirectional setup |
| Automatic FK sync | ❌ Not implemented | Manual consistency |
| Cascade cycle detection | ❌ Not implemented | User responsibility |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| related() To-One | O(1) | Direct ID lookup |
| related() To-Many | O(k) | k = number of related items |
| joining() batch load | O(n + m) | n items, m unique FKs |
| Delete with cascade | O(c) | c = cascaded items |
| Delete with nullify | O(n) | n = referencing items |
| Delete with deny | O(1) | Single range check |
| Index update | O(1) | FK change |
| To-Many index update | O(k) | k = array size |

### Storage Overhead

| Component | Storage |
|-----------|---------|
| FK Index entry | ~20-40 bytes per FK |
| To-Many index entries | ~20-40 bytes × array size |
| RelationshipDescriptor | Metadata only (no FDB storage) |

### FDB Considerations

- **Transaction Size**: Large cascading deletes may approach 10MB limit
- **Key Size**: FK values must fit within FDB's 10KB key limit
- **Batch Loading**: FK values are deduplicated before batch loading
- **Orphan FKs**: Allowed by default (no referential integrity enforcement on insert)

## Benchmark Results

Run with: `swift test --filter RelationshipIndexPerformanceTests`

### Insert Performance

| Records | Relationship Type | Insert Time | Throughput |
|---------|-------------------|-------------|------------|
| 100 | To-One | ~30ms | ~3,300/s |
| 1,000 | To-One | ~300ms | ~3,300/s |
| 100 | To-Many (5 items) | ~50ms | ~2,000/s |

### Query Performance

| Items | Operation | Latency (p50) |
|-------|-----------|---------------|
| 1,000 | related() To-One | ~1ms |
| 1,000 | related() To-Many (5) | ~3ms |
| 100 | fetch().joining() | ~10ms |
| 1,000 | fetch().joining() | ~50ms |

### Delete Rule Performance

| Items | Delete Rule | Referencing Items | Latency (p50) |
|-------|-------------|-------------------|---------------|
| 1 | cascade | 10 | ~20ms |
| 1 | cascade | 100 | ~100ms |
| 1 | nullify | 10 | ~15ms |
| 1 | deny (blocked) | 10 | ~5ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [SwiftData @Relationship](https://developer.apple.com/documentation/swiftdata/relationship) - Apple documentation
- [SwiftData DeleteRule](https://developer.apple.com/documentation/swiftdata/schema/relationship/deleterule) - Delete rule patterns
- [FDB Record Layer Relationships](https://github.com/FoundationDB/fdb-record-layer) - Reference implementation
