# FullTextIndex

`FullTextIndex` executes typed text declarations in full-text or autocomplete
mode.

```swift
#Index(.text(
    name: "documents_text",
    fields: [\Document.title, \Document.body],
    mode: .fullText(
        tokenizer: .simple,
        storePositions: true,
        ngramSize: 3,
        minimumTermLength: 2
    )
))

#Index(.text(
    name: "documents_autocomplete",
    fields: [\Document.title],
    mode: .autocomplete(
        minimumPrefixLength: 2,
        maximumPrefixLength: 12
    )
))
```

The definition fixes observable text semantics. Runtime configuration may
supply execution resources but cannot change the declared mode or fields.
