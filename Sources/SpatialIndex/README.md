# SpatialIndex

`SpatialIndex` executes geographic lookup declarations.

```swift
#Index(.spatial(
    name: "stores_by_location",
    location: \Store.location,
    encoding: .s2,
    level: 15
))
```

The schema accepts canonical geographic point or position fields and validates
the encoding level. This module owns encoding, maintenance, and spatial reads;
it does not own schema metadata or storage-engine lifecycle.
