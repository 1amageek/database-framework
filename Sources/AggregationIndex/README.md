# AggregationIndex

`AggregationIndex` executes aggregate and update-count declarations. It owns
incremental maintenance and canonical aggregate reads; `DatabaseKit` owns the
function and field contract.

```swift
#Index(.aggregate(
    name: "sales_sum_by_region",
    function: .sum,
    groupBy: [.ascending(\Sale.region)],
    value: \Sale.amount
))

#Index(.updateCount(
    name: "profile_update_count",
    field: \UserProfile.id
))
```

Supported aggregate functions are count, sum, minimum, maximum, average,
non-null count, approximate distinct, and percentile. Numeric bounds and field
compatibility are validated when the schema is constructed. Providers
pattern-match the full definition and never infer a function from an index
name.
