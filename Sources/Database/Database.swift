// Database is the stable umbrella import for database operations.
//
// The consuming package selects backend and index implementations through
// SwiftPM package traits. Only selected implementations enter this target's
// dependency graph and re-exported API.

// database-kit (client-safe model definitions)
@_exported import DatabaseKit

// storage-kit (storage engine abstraction)
@_exported import StorageKit
#if FOUNDATION_DB
@_exported import FDBStorage
#endif
#if SQLITE
@_exported import SQLiteStorage
#endif
#if POSTGRESQL
@_exported import PostgreSQLStorage
#endif

// database-framework (execution layer)
@_exported import DatabaseEngine
@_exported import DatabaseRuntime
#if DATABASE_SCALAR_INDEXES
@_exported import ScalarIndex
#endif
#if DATABASE_VECTOR_INDEXES
@_exported import VectorIndex
#endif
#if DATABASE_FULL_TEXT_INDEXES
@_exported import FullTextIndex
#endif
#if DATABASE_SPATIAL_INDEXES
@_exported import SpatialIndex
#endif
#if DATABASE_RANK_INDEXES
@_exported import RankIndex
#endif
#if DATABASE_PERMUTED_INDEXES
@_exported import PermutedIndex
#endif
#if DATABASE_GRAPH_INDEXES
@_exported import GraphIndex
@_exported import OntologyIndex
#endif
#if DATABASE_AGGREGATION_INDEXES
@_exported import AggregationIndex
#endif
#if DATABASE_VERSION_INDEXES
@_exported import VersionIndex
#endif
#if DATABASE_BITMAP_INDEXES
@_exported import BitmapIndex
#endif
#if DATABASE_LEADERBOARD_INDEXES
@_exported import LeaderboardIndex
#endif
#if DATABASE_RELATIONSHIPS
@_exported import RelationshipIndex
#endif
@_exported import QueryAST
