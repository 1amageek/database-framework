// FieldValue+TupleElement.swift
// Extension to bridge Core.FieldValue with FoundationDB TupleElement

import Foundation
import FoundationDB
import Core

// MARK: - TupleElement Conversion

extension FieldValue {
    /// Sentinel marker for null values
    ///
    /// Uses a two-byte sequence that's unlikely to appear in normal data:
    /// - 0xFF: High byte (invalid UTF-8 start)
    /// - 0x00: Null terminator
    ///
    /// This distinguishes null from empty Data (which maps to empty [UInt8])
    private static let nullSentinel: [UInt8] = [0xFF, 0x00]

    /// Convert to TupleElement for FDB operations
    ///
    /// This is the single point of conversion from FieldValue to TupleElement,
    /// eliminating duplicate conversion logic throughout the codebase.
    public func toTupleElement() -> any TupleElement {
        switch self {
        case .null:
            // Use sentinel to distinguish from empty Data
            return Self.nullSentinel
        case .bool(let v):
            return v
        case .int64(let v):
            return v
        case .double(let v):
            return v
        case .string(let v):
            return v
        case .data(let v):
            // Empty data maps to empty [UInt8], distinct from null sentinel
            return [UInt8](v)
        case .array(let values):
            // Convert array to Tuple for nested structures
            return Tuple(values.map { $0.toTupleElement() })
        }
    }

    /// Convert array of FieldValue to array of TupleElement
    public static func toTupleElements(_ values: [FieldValue]) -> [any TupleElement] {
        values.map { $0.toTupleElement() }
    }

    /// Create FieldValue from TupleElement
    ///
    /// This enables reading index values back from FoundationDB
    public init?(tupleElement: any TupleElement) {
        switch tupleElement {
        case let v as Bool:
            self = .bool(v)
        case let v as Int64:
            self = .int64(v)
        case let v as Int:
            self = .int64(Int64(v))
        case let v as Double:
            self = .double(v)
        case let v as Float:
            self = .double(Double(v))
        case let v as String:
            self = .string(v)
        case let v as UUID:
            self = .string(v.uuidString)
        case let v as Date:
            self = .double(v.timeIntervalSince1970)
        case let v as [UInt8]:
            // Check for null sentinel
            if v == Self.nullSentinel {
                self = .null
            } else {
                // All other byte arrays are Data (including empty)
                self = .data(Data(v))
            }
        case let v as Tuple:
            var elements: [FieldValue] = []
            for i in 0..<v.count {
                if let element = v[i], let fv = FieldValue(tupleElement: element) {
                    elements.append(fv)
                } else {
                    return nil
                }
            }
            self = .array(elements)
        default:
            return nil
        }
    }
}
