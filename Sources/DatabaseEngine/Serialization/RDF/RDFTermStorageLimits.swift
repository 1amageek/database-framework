package struct RDFTermStorageLimits: Sendable, Equatable {
    package let maximumBytes: Int
    package let maximumDepth: Int
    package let maximumObjectCount: Int

    package init(
        maximumBytes: Int = 65_536,
        maximumDepth: Int = 32,
        maximumObjectCount: Int = 65_536
    ) throws(RDFTermStorageLimitsError) {
        guard maximumBytes >= 0 else {
            throw .negativeMaximumBytes(maximumBytes)
        }
        guard maximumDepth >= 0 else {
            throw .negativeMaximumDepth(maximumDepth)
        }
        guard maximumObjectCount > 0 else {
            throw .nonPositiveMaximumObjectCount(maximumObjectCount)
        }
        self.maximumBytes = maximumBytes
        self.maximumDepth = maximumDepth
        self.maximumObjectCount = maximumObjectCount
    }

    init(
        validatedMaximumBytes maximumBytes: Int,
        maximumDepth: Int,
        maximumObjectCount: Int
    ) {
        self.maximumBytes = maximumBytes
        self.maximumDepth = maximumDepth
        self.maximumObjectCount = maximumObjectCount
    }

    package static let `default` = Self(
        validatedMaximumBytes: 65_536,
        maximumDepth: 32,
        maximumObjectCount: 65_536
    )
}
