internal enum DatabaseBase64Error: Error, Sendable, Equatable {
    case invalidLength
    case invalidCharacter
    case invalidPadding
    case decodedValueTooLarge
}
