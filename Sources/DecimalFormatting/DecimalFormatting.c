#define _GNU_SOURCE 1
#include "DecimalFormatting.h"
#include <errno.h>
#include <locale.h>
#include <math.h>
#include <stdio.h>
#if defined(__APPLE__)
#include <xlocale.h>
#endif

int32_t decimal_format_finite(double value, int32_t significant_digits,
                              uint8_t *output, size_t capacity, size_t *payload_count) {
    if (payload_count == NULL) return DECIMAL_FORMATTING_INVALID_ARGUMENT;
    *payload_count = 0;
    if (output == NULL || capacity == 0 ||
        (significant_digits != 6 && significant_digits != 15)) {
        return DECIMAL_FORMATTING_INVALID_ARGUMENT;
    }
    if (!isfinite(value)) return DECIMAL_FORMATTING_NONFINITE;
    const int saved_errno = errno;
    locale_t locale = newlocale(LC_NUMERIC_MASK, "C", (locale_t)0);
    if (locale == (locale_t)0) return DECIMAL_FORMATTING_LOCALE_UNAVAILABLE;
    int count;
#if defined(__APPLE__)
    count = snprintf_l((char *)output, capacity, locale, "%.*g", (int)significant_digits, value);
#else
    locale_t previous = uselocale(locale);
    if (previous == (locale_t)0) {
        freelocale(locale);
        return DECIMAL_FORMATTING_LOCALE_UNAVAILABLE;
    }
    count = snprintf((char *)output, capacity, "%.*g", (int)significant_digits, value);
    // previous is the valid handle returned by uselocale, including LC_GLOBAL_LOCALE.
    // POSIX guarantees its restoration; never free an active locale if libc violates it.
    if (uselocale(previous) == (locale_t)0) return DECIMAL_FORMATTING_LOCALE_RESTORATION_FAILED;
#endif
    freelocale(locale);
    errno = saved_errno;
    if (count < 0) return DECIMAL_FORMATTING_FAILED;
    if ((size_t)count >= capacity) return DECIMAL_FORMATTING_INSUFFICIENT_CAPACITY;
    *payload_count = (size_t)count;
    return DECIMAL_FORMATTING_SUCCESS;
}
