#if canImport(Darwin)
import Darwin
#elseif canImport(Musl)
import Musl
#elseif canImport(Glibc)
import Glibc
#elseif canImport(WASILibc)
import WASILibc
#else
#error("DatabaseMath requires a supported C math runtime")
#endif

/// Platform-neutral access to the C math operations used by database planning.
public enum DatabaseMath {
    @inlinable
    public static func naturalLogarithm(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.log(value)
#elseif canImport(Musl)
        Musl.log(value)
#elseif canImport(Glibc)
        Glibc.log(value)
#else
        WASILibc.log(value)
#endif
    }

    @inlinable
    public static func binaryLogarithm(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.log2(value)
#elseif canImport(Musl)
        Musl.log2(value)
#elseif canImport(Glibc)
        Glibc.log2(value)
#else
        WASILibc.log2(value)
#endif
    }

    @inlinable
    public static func logarithmOfOnePlus(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.log1p(value)
#elseif canImport(Musl)
        Musl.log1p(value)
#elseif canImport(Glibc)
        Glibc.log1p(value)
#else
        WASILibc.log1p(value)
#endif
    }

    @inlinable
    public static func exponential(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.exp(value)
#elseif canImport(Musl)
        Musl.exp(value)
#elseif canImport(Glibc)
        Glibc.exp(value)
#else
        WASILibc.exp(value)
#endif
    }

    @inlinable
    public static func power(_ base: Double, _ exponent: Double) -> Double {
#if canImport(Darwin)
        Darwin.pow(base, exponent)
#elseif canImport(Musl)
        Musl.pow(base, exponent)
#elseif canImport(Glibc)
        Glibc.pow(base, exponent)
#else
        WASILibc.pow(base, exponent)
#endif
    }

    @inlinable
    public static func sine(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.sin(value)
#elseif canImport(Musl)
        Musl.sin(value)
#elseif canImport(Glibc)
        Glibc.sin(value)
#else
        WASILibc.sin(value)
#endif
    }

    @inlinable
    public static func cosine(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.cos(value)
#elseif canImport(Musl)
        Musl.cos(value)
#elseif canImport(Glibc)
        Glibc.cos(value)
#else
        WASILibc.cos(value)
#endif
    }

    @inlinable
    public static func arcSine(_ value: Double) -> Double {
#if canImport(Darwin)
        Darwin.asin(value)
#elseif canImport(Musl)
        Musl.asin(value)
#elseif canImport(Glibc)
        Glibc.asin(value)
#else
        WASILibc.asin(value)
#endif
    }

    @inlinable
    public static func arcTangent(y: Double, x: Double) -> Double {
#if canImport(Darwin)
        Darwin.atan2(y, x)
#elseif canImport(Musl)
        Musl.atan2(y, x)
#elseif canImport(Glibc)
        Glibc.atan2(y, x)
#else
        WASILibc.atan2(y, x)
#endif
    }

    @inlinable
    public static func squareRoot(_ value: Double) -> Double {
        value.squareRoot()
    }

    @inlinable
    public static func squareRoot(_ value: Float) -> Float {
        value.squareRoot()
    }

    @inlinable
    public static func ceiling(_ value: Double) -> Double {
        value.rounded(.up)
    }

    @inlinable
    public static func floor(_ value: Double) -> Double {
        value.rounded(.down)
    }

    @inlinable
    public static func rounded(_ value: Double) -> Double {
        value.rounded(.toNearestOrAwayFromZero)
    }
}
