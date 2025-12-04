// SnowballStemmer.swift
// FullTextIndex - Porter2 English stemmer
//
// Reference: Snowball Porter2 stemmer
// https://snowballstem.org/algorithms/english/stemmer.html

import Foundation

// MARK: - SnowballStemmer

/// Porter2 English stemmer
///
/// Implements the Snowball Porter2 stemming algorithm for reducing
/// English words to their root form.
///
/// **Examples**:
/// - "running" → "run"
/// - "connection" → "connect"
/// - "agreed" → "agree"
///
/// **Reference**: https://snowballstem.org/algorithms/english/stemmer.html
public struct SnowballStemmer: TokenFilter, Sendable {
    public static var identifier: String { "snowball_en" }

    public init() {}

    // MARK: - TokenFilter

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        tokens.map { token in
            AnalyzedToken(
                text: stem(token.text),
                position: token.position,
                startOffset: token.startOffset,
                endOffset: token.endOffset,
                type: token.type
            )
        }
    }

    // MARK: - Stemming

    /// Stem a single word
    ///
    /// - Parameter word: The word to stem
    /// - Returns: The stemmed form
    public func stem(_ word: String) -> String {
        var w = word.lowercased()

        // Words must be at least 3 characters to stem
        guard w.count > 2 else { return w }

        // Handle special cases
        if let special = specialCases[w] {
            return special
        }

        // Remove initial apostrophe
        if w.hasPrefix("'") {
            w = String(w.dropFirst())
        }

        // Set up Y handling (treat Y as consonant at word start or after vowel)
        w = handleY(w)

        // Find R1 and R2 regions
        let r1Start = findR1(w)
        let r2Start = findR2(w, r1Start: r1Start)

        // Step 0: Remove 's, 's, '
        w = step0(w)

        // Step 1a: Handle plurals and -ed/-ing
        w = step1a(w)

        // Step 1b: Handle -ed/-ing with vowel
        w = step1b(w, r1Start: r1Start)

        // Step 1c: Replace Y with i
        w = step1c(w)

        // Step 2: Map suffixes to shorter ones
        w = step2(w, r1Start: r1Start)

        // Step 3: More suffix mapping
        w = step3(w, r1Start: r1Start, r2Start: r2Start)

        // Step 4: Remove suffixes in R2
        w = step4(w, r2Start: r2Start)

        // Step 5: Remove final e/l
        w = step5(w, r1Start: r1Start, r2Start: r2Start)

        // Restore Y to y
        w = w.replacingOccurrences(of: "Y", with: "y")

        return w
    }

    // MARK: - Helper Methods

    private let vowels = Set<Character>("aeiouy")
    private let doubles = Set(["bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"])
    private let liEnding = Set<Character>("cdeghkmnrt")

    private func isVowel(_ c: Character) -> Bool {
        vowels.contains(c)
    }

    private func isConsonant(_ c: Character) -> Bool {
        !vowels.contains(c)
    }

    /// Replace Y with uppercase Y where it should be treated as consonant
    private func handleY(_ word: String) -> String {
        var chars = Array(word)
        if !chars.isEmpty && chars[0] == "y" {
            chars[0] = "Y"
        }
        for i in 1..<chars.count {
            if chars[i] == "y" && isVowel(chars[i-1]) {
                chars[i] = "Y"
            }
        }
        return String(chars)
    }

    /// Find the start of R1 (region after first non-vowel after vowel)
    private func findR1(_ word: String) -> Int {
        let chars = Array(word)

        // Special cases for R1
        if word.hasPrefix("gener") || word.hasPrefix("arsen") {
            return 5
        }
        if word.hasPrefix("commun") {
            return 6
        }

        var foundVowel = false
        for i in 0..<chars.count {
            if isVowel(chars[i]) {
                foundVowel = true
            } else if foundVowel {
                return i + 1
            }
        }
        return chars.count
    }

    /// Find the start of R2 (R1 region applied again within R1)
    private func findR2(_ word: String, r1Start: Int) -> Int {
        let chars = Array(word)
        guard r1Start < chars.count else { return chars.count }

        var foundVowel = false
        for i in r1Start..<chars.count {
            if isVowel(chars[i]) {
                foundVowel = true
            } else if foundVowel {
                return i + 1
            }
        }
        return chars.count
    }

    /// Check if word ends with a short syllable
    private func endsWithShortSyllable(_ word: String) -> Bool {
        let chars = Array(word)
        let n = chars.count

        if n >= 2 && isConsonant(chars[n-1]) && isVowel(chars[n-2]) {
            if n == 2 || (n >= 3 && isConsonant(chars[n-3]) && chars[n-1] != "w" && chars[n-1] != "x" && chars[n-1] != "Y") {
                return true
            }
        }
        return false
    }

    /// Check if word is short (R1 is null and ends with short syllable)
    private func isShortWord(_ word: String, r1Start: Int) -> Bool {
        return r1Start >= word.count && endsWithShortSyllable(word)
    }

    // MARK: - Stemming Steps

    private func step0(_ word: String) -> String {
        if word.hasSuffix("'s'") {
            return String(word.dropLast(3))
        }
        if word.hasSuffix("'s") {
            return String(word.dropLast(2))
        }
        if word.hasSuffix("'") {
            return String(word.dropLast(1))
        }
        return word
    }

    private func step1a(_ word: String) -> String {
        if word.hasSuffix("sses") {
            return String(word.dropLast(2))
        }
        if word.hasSuffix("ied") || word.hasSuffix("ies") {
            return word.count > 4 ? String(word.dropLast(2)) : String(word.dropLast(1))
        }
        if word.hasSuffix("us") || word.hasSuffix("ss") {
            return word
        }
        if word.hasSuffix("s") {
            let stem = String(word.dropLast())
            // Check if preceding part contains a vowel
            let beforeS = stem.dropLast()  // Everything before the 's'
            for c in beforeS {
                if isVowel(c) {
                    return stem
                }
            }
        }
        return word
    }

    private func step1b(_ word: String, r1Start: Int) -> String {
        if word.hasSuffix("eedly") {
            if word.count - 5 >= r1Start {
                return String(word.dropLast(3))  // -eedly → -ee
            }
            return word
        }
        if word.hasSuffix("eed") {
            if word.count - 3 >= r1Start {
                return String(word.dropLast(1))  // -eed → -ee
            }
            return word
        }

        var w = word
        var modified = false

        for suffix in ["ingly", "edly", "ing", "ed"] {
            if w.hasSuffix(suffix) {
                let stem = String(w.dropLast(suffix.count))
                // Check if stem contains vowel
                var hasVowel = false
                for c in stem {
                    if isVowel(c) {
                        hasVowel = true
                        break
                    }
                }
                if hasVowel {
                    w = stem
                    modified = true
                    break
                }
            }
        }

        if modified {
            if w.hasSuffix("at") || w.hasSuffix("bl") || w.hasSuffix("iz") {
                return w + "e"
            }
            let lastTwo = String(w.suffix(2))
            if doubles.contains(lastTwo) {
                return String(w.dropLast())
            }
            if isShortWord(w, r1Start: r1Start) {
                return w + "e"
            }
        }

        return w
    }

    private func step1c(_ word: String) -> String {
        if word.count > 2 {
            let chars = Array(word)
            if (chars.last == "y" || chars.last == "Y") && isConsonant(chars[chars.count - 2]) {
                var newWord = String(word.dropLast())
                newWord.append("i")
                return newWord
            }
        }
        return word
    }

    private let step2Suffixes: [(suffix: String, replacement: String)] = [
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("abli", "able"),
        ("entli", "ent"),
        ("izer", "ize"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("aliti", "al"),
        ("alli", "al"),
        ("fulness", "ful"),
        ("ousli", "ous"),
        ("ousness", "ous"),
        ("iveness", "ive"),
        ("iviti", "ive"),
        ("biliti", "ble"),
        ("bli", "ble"),
        ("fulli", "ful"),
        ("lessli", "less"),
        ("ogi", "og"),  // only if preceded by l
        ("li", "")      // only if preceded by valid li-ending
    ]

    private func step2(_ word: String, r1Start: Int) -> String {
        for (suffix, replacement) in step2Suffixes {
            if word.hasSuffix(suffix) && word.count - suffix.count >= r1Start {
                let stem = String(word.dropLast(suffix.count))

                // Special case for "ogi" - must be preceded by 'l'
                if suffix == "ogi" && !stem.hasSuffix("l") {
                    continue
                }

                // Special case for "li" - must be preceded by valid li-ending
                if suffix == "li" {
                    if let lastChar = stem.last, liEnding.contains(lastChar) {
                        return stem
                    }
                    continue
                }

                return stem + replacement
            }
        }
        return word
    }

    private let step3Suffixes: [(suffix: String, replacement: String, r2Only: Bool)] = [
        ("ational", "ate", false),
        ("tional", "tion", false),
        ("alize", "al", false),
        ("icate", "ic", false),
        ("iciti", "ic", false),
        ("ical", "ic", false),
        ("ful", "", false),
        ("ness", "", false),
        ("ative", "", true)  // Only delete if in R2
    ]

    private func step3(_ word: String, r1Start: Int, r2Start: Int) -> String {
        for (suffix, replacement, r2Only) in step3Suffixes {
            if word.hasSuffix(suffix) {
                let checkRegion = r2Only ? r2Start : r1Start
                if word.count - suffix.count >= checkRegion {
                    return String(word.dropLast(suffix.count)) + replacement
                }
            }
        }
        return word
    }

    private let step4Suffixes = [
        "ement", "ment", "ence", "ance", "able", "ible",
        "ant", "ent", "ism", "ate", "iti", "ous", "ive", "ize",
        "ion", "al", "er", "ic"
    ]

    private func step4(_ word: String, r2Start: Int) -> String {
        for suffix in step4Suffixes {
            if word.hasSuffix(suffix) && word.count - suffix.count >= r2Start {
                // Special case for "ion" - must be preceded by s or t
                if suffix == "ion" {
                    let stem = String(word.dropLast(3))
                    if stem.hasSuffix("s") || stem.hasSuffix("t") {
                        return stem
                    }
                    continue
                }
                return String(word.dropLast(suffix.count))
            }
        }
        return word
    }

    private func step5(_ word: String, r1Start: Int, r2Start: Int) -> String {
        var w = word

        // Remove final 'e' if in R2, or in R1 and not preceded by short syllable
        if w.hasSuffix("e") {
            if w.count - 1 >= r2Start {
                w = String(w.dropLast())
            } else if w.count - 1 >= r1Start {
                let stem = String(w.dropLast())
                if !endsWithShortSyllable(stem) {
                    w = stem
                }
            }
        }

        // Remove final 'll' if in R2
        if w.hasSuffix("ll") && w.count - 1 >= r2Start {
            w = String(w.dropLast())
        }

        return w
    }

    // MARK: - Special Cases

    private let specialCases: [String: String] = [
        "skis": "ski",
        "skies": "sky",
        "dying": "die",
        "lying": "lie",
        "tying": "tie",
        "idly": "idl",
        "gently": "gentl",
        "ugly": "ugli",
        "early": "earli",
        "only": "onli",
        "singly": "singl",
        "sky": "sky",
        "news": "news",
        "howe": "howe",
        "atlas": "atlas",
        "cosmos": "cosmos",
        "bias": "bias",
        "andes": "andes"
    ]
}

// MARK: - Porter Stemmer (Original)

/// Original Porter stemmer (for compatibility)
///
/// The Porter2 (Snowball) stemmer above is preferred.
/// This is the original 1980 algorithm.
///
/// **Reference**: Porter, M.F. "An algorithm for suffix stripping"
/// Program 14(3): 130-137, 1980.
public struct PorterStemmer: TokenFilter, Sendable {
    public static var identifier: String { "porter" }

    private let snowball = SnowballStemmer()

    public init() {}

    public func filter(_ tokens: [AnalyzedToken]) -> [AnalyzedToken] {
        // Use Snowball implementation - it's Porter2 which is an improvement
        snowball.filter(tokens)
    }

    public func stem(_ word: String) -> String {
        snowball.stem(word)
    }
}
