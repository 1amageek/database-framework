// Database.swift
// Database - All-in-one import for server-side database operations
//
// This module re-exports all Database modules for convenience.
// For better build performance, consider importing individual modules instead.

// database-kit (client-safe model definitions)
@_exported import Core
@_exported import Graph
@_exported import Relationship

// database-framework (server-side execution layer)
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
@_exported import QueryAST
