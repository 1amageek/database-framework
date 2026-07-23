/// Resource limits applied when QueryIR is compiled into SPARQL expression
/// algebra. These limits also cover expressions constructed directly in Swift,
/// after the binary decoder boundary.
public struct SPARQLExpressionCompilationLimits: Sendable, Equatable {
    public let maximumDepth: Int
    public let maximumNodes: Int
    public let maximumFunctionArguments: Int
    public let maximumCollectionElements: Int
    public let maximumStringUTF8Count: Int

    public init(
        maximumDepth: Int = 128,
        maximumNodes: Int = 4_096,
        maximumFunctionArguments: Int = 1_024,
        maximumCollectionElements: Int = 4_096,
        maximumStringUTF8Count: Int = 1_048_576
    ) {
        self.maximumDepth = maximumDepth
        self.maximumNodes = maximumNodes
        self.maximumFunctionArguments = maximumFunctionArguments
        self.maximumCollectionElements = maximumCollectionElements
        self.maximumStringUTF8Count = maximumStringUTF8Count
    }

    public static let `default` = Self()
}
