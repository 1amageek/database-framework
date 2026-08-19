# BitmapIndex

`BitmapIndex` executes `IndexType.bitmap` declarations for equality-comparable
fields.

```swift
#Index(.bitmap(
    name: "users_by_status",
    field: \User.status
))
```

The module owns bitmap entry maintenance and set operations. Declaration,
field validation, lifecycle, and physical-generation selection remain owned by
`DatabaseKit` and `DatabaseEngine` respectively.
