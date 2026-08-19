# VersionIndex

`VersionIndex` executes history declarations.

```swift
#Index(.history(
    name: "documents_history",
    version: \Document.version,
    retention: .keepLast(20)
))
```

Retention is part of the logical definition and therefore part of the physical
generation fingerprint. The module owns history entry maintenance and reads;
lifecycle transitions remain in `DatabaseEngine`.
