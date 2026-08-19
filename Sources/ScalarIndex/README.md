# ScalarIndex

`ScalarIndex` executes `IndexType.ordered` declarations. It owns ordered key
maintenance, equality/range reads, covering values, and uniqueness enforcement.
The logical declaration remains in `DatabaseKit`.

```swift
#Index(.ordered(
    name: "users_by_email",
    keys: [.ascending(\User.email)],
    includedFields: [\User.displayName],
    unique: true
))
```

Compound order is the order of `keys`. Alternate orders are separate ordered
declarations; there is no Permuted index family.

The provider accepts only `.ordered` definitions and receives a fingerprinted
physical subspace from `DatabaseEngine`. Reads require the exact generation to
be `readable`.
