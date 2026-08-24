import DatabaseTypes

/// Self-contained bytes whose complete feature-visible owner is charged to
/// the request meter for exactly as long as the value remains retained.
package struct FusionIndexReadValue: Sendable {
    package let bytes: ByteString

    init(bytes: ByteString) {
        self.bytes = bytes
    }
}

/// One self-contained physical row admitted to the feature boundary.
package struct FusionIndexReadRow: Sendable {
    package let key: ByteString
    package let value: ByteString

    init(
        key: ByteString,
        value: ByteString
    ) {
        self.key = key
        self.value = value
    }
}
