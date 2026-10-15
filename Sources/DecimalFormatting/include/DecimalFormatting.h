#ifndef DECIMAL_FORMATTING_H
#define DECIMAL_FORMATTING_H

#include <stddef.h>
#include <stdint.h>

// Payload bound: sign + 15 digits + decimal point + e + exponent sign +
// three exponent digits. Capacity includes one terminating NUL.
enum { DECIMAL_FORMATTING_CAPACITY = 23 };

enum {
    DECIMAL_FORMATTING_SUCCESS = 0,
    DECIMAL_FORMATTING_INVALID_ARGUMENT = 1,
    DECIMAL_FORMATTING_NONFINITE = 2,
    DECIMAL_FORMATTING_INSUFFICIENT_CAPACITY = 3,
    DECIMAL_FORMATTING_LOCALE_UNAVAILABLE = 4,
    DECIMAL_FORMATTING_FAILED = 5,
    DECIMAL_FORMATTING_LOCALE_RESTORATION_FAILED = 6
};

// The caller owns output[0..<capacity]. The call retains no pointer. On success,
// output[0..<*payload_count] is initialized ASCII and output[*payload_count] is NUL.
int32_t decimal_format_finite(double value, int32_t significant_digits,
                              uint8_t *output, size_t capacity, size_t *payload_count);

#endif
