// LimitReason.swift
// GraphIndex - Re-exports LimitReason from DatabaseEngine for backwards compatibility
//
// LimitReason has been moved to DatabaseEngine for sharing across index modules.

import DatabaseEngine

// Re-export LimitReason from DatabaseEngine
// This maintains backwards compatibility for existing GraphIndex users
public typealias LimitReason = DatabaseEngine.LimitReason
