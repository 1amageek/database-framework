// FDBite - All-in-one import for on-device database operations
//
// Counterpart to Database module. Uses SQLite instead of FoundationDB.
// Works on iOS and macOS.

// database-kit (shared model definitions)
@_exported import Core
@_exported import Graph
@_exported import Relationship

// database-framework (storage-agnostic execution layer)
@_exported import DatabaseEngine
@_exported import ScalarIndex
@_exported import VectorIndex
@_exported import FullTextIndex
@_exported import SpatialIndex
@_exported import RankIndex
@_exported import PermutedIndex
@_exported import GraphIndex
@_exported import AggregationIndex
@_exported import VersionIndex
@_exported import BitmapIndex
@_exported import LeaderboardIndex
@_exported import RelationshipIndex
@_exported import OntologyIndex
@_exported import QueryIR
@_exported import QueryAST

// SQLite storage backend
@_exported import SQLiteStorage
