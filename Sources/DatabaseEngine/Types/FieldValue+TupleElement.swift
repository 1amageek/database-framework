// FieldValue+TupleElement.swift
// Conversion between Core.FieldValue and FoundationDB TupleElement

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Core

// MARK: - TupleElement Conversion

extension FieldValue {
    /// Convert to TupleElement for FDB operations
    ///
    /// This is the single point of conversion from FieldValue to TupleElement,
    /// eliminating duplicate conversion logic throughout the codebase.
    public func toTupleElement(
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try FieldValueTupleCodec.tupleElement(for: self, limits: limits)
    }

    /// Convert array of FieldValue to array of TupleElement
    public static func toTupleElements(
        _ values: [FieldValue],
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> [any TupleElement] {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(values.count)
        for value in values {
            elements.append(try value.toTupleElement(limits: limits))
        }
        return elements
    }

    /// Create FieldValue from TupleElement
    ///
    /// This enables reading index values back from FoundationDB
    public init(
        tupleElement: any TupleElement,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) {
        switch tupleElement {
        case let value as Float:
            self = .double(Double(value))
        case let value as UUID:
            self = .string(value.uuidString)
        default:
            self = try FieldValueTupleCodec.decode(tupleElement, limits: limits)
        }
    }
}
