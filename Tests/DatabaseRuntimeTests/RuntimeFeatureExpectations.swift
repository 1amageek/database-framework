enum RuntimeFeatureExpectations {
    #if ScalarIndexes
    static let scalarIndexes = true
    #else
    static let scalarIndexes = false
    #endif

    #if VectorIndexes
    static let vectorIndexes = true
    #else
    static let vectorIndexes = false
    #endif

    #if FullTextIndexes
    static let fullTextIndexes = true
    #else
    static let fullTextIndexes = false
    #endif

    #if SpatialIndexes
    static let spatialIndexes = true
    #else
    static let spatialIndexes = false
    #endif

    #if RankIndexes
    static let rankIndexes = true
    #else
    static let rankIndexes = false
    #endif

    #if BitmapIndexes
    static let bitmapIndexes = true
    #else
    static let bitmapIndexes = false
    #endif

    #if VersionIndexes
    static let versionIndexes = true
    #else
    static let versionIndexes = false
    #endif

    #if GraphIndexes
    static let graphIndexes = true
    #else
    static let graphIndexes = false
    #endif

    #if AggregationIndexes
    static let aggregationIndexes = true
    #else
    static let aggregationIndexes = false
    #endif

    #if LeaderboardIndexes
    static let leaderboardIndexes = true
    #else
    static let leaderboardIndexes = false
    #endif

    #if Relationships
    static let relationships = true
    #else
    static let relationships = false
    #endif
}
