// Database.swift
// Database - All-in-one import for server-side database operations
//
// This module re-exports all Database modules for convenience.
// For better build performance, consider importing individual modules instead.

@_exported import DatabaseEngine
@_exported import ScalarIndex
@_exported import VectorIndex
@_exported import FullTextIndex
@_exported import SpatialIndex
@_exported import RankIndex
@_exported import PermutedIndex
@_exported import GraphIndex
@_exported import TripleIndex
@_exported import AggregationIndex
@_exported import VersionIndex
