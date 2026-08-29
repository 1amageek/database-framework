/// Why a value has no canonical Directory component, or why a component is not
/// the canonical image of the value it describes.
package enum DirectoryComponentCodecError: Error, Equatable, Sendable {
    /// The value's kind has no canonical Directory component. SPEC 10.1 admits
    /// only a required scalar field as a dynamic `#Directory` component, so the
    /// payload names the rejected `FieldValue` case.
    case unsupportedFieldKind(String)

    /// The component violates the grammar of SPEC 12.2.
    case malformedComponent(Malformation)

    /// The tag names no admitted field kind.
    case unknownTag(String)

    /// The component parses but is not the canonical image of the value it
    /// describes. `canonical` is the component that value does encode to, so a
    /// caller can report both forms without re-running the converter.
    case nonCanonicalComponent(canonical: String)

    /// The grammar rule a malformed component violates.
    package enum Malformation: Equatable, Sendable {
        /// No `-` separates the tag from the body.
        case missingTagSeparator

        /// The component begins with the tag separator.
        case emptyTag

        /// A `%` is not followed by two uppercase hexadecimal digits.
        case invalidEscape

        /// A raw byte is neither unreserved nor the escape introducer.
        case invalidCharacter

        /// The body carries a token count the tag does not define.
        case tokenCount(expected: Int, actual: Int)

        /// A decimal token is empty or carries a byte that is not a digit.
        case invalidNumber

        /// A decimal token does not fit the integer width of its position.
        case numberOutOfRange

        /// A hexadecimal token has the wrong length or a digit that is not an
        /// uppercase hexadecimal digit.
        case invalidHexadecimal

        /// An unescaped `string` token is not valid UTF-8.
        case invalidUTF8

        /// The tokens parse but the value type rejects the combination.
        case invalidValue
    }
}
