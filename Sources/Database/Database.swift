// Database.swift
// Database - All-in-one import for database operations
//
// This module re-exports all Database modules for convenience.
// Storage backend is selected via SPM traits:
//   - FoundationDB (default): distributed database
//   - SQLite: on-device / embedded
//   - PostgreSQL: server-side RDBMS
//
// For better build performance, consider importing individual modules instead.

// database-kit (client-safe model definitions)
@_exported import Core
@_exported import Graph
@_exported import Relationship

// storage-kit (storage engine abstraction)
@_exported import StorageKit
#if FOUNDATION_DB
@_exported import FDBStorage
#endif
#if SQLITE
@_exported import SQLiteStorage
#endif

// database-framework (execution layer)
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
