enum SPARQLBindingFootprintError: Error, Sendable, Equatable {
    case unknownByteStringRetainedSize
    case unknownVectorRetainedSize
}
