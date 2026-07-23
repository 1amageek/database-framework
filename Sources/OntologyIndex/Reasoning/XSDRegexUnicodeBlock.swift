/// Unicode blocks prescribed by XML Schema 1.1 regular expressions.
///
/// The table is the Unicode 5.1 block database referenced by XSD 1.1. Block
/// names use the XSD spelling: spaces are removed while hyphens are retained.
package struct XSDRegexUnicodeBlock: Sendable {
    private struct ScalarRange: Sendable {
        let lowerBound: UInt32
        let upperBound: UInt32

        func contains(_ value: UInt32) -> Bool {
            value >= lowerBound && value <= upperBound
        }
    }

    private let primary: ScalarRange
    private let secondary: ScalarRange?
    private let tertiary: ScalarRange?

    private init(
        _ lowerBound: UInt32,
        _ upperBound: UInt32,
        secondary: (UInt32, UInt32)? = nil,
        tertiary: (UInt32, UInt32)? = nil
    ) {
        primary = ScalarRange(lowerBound: lowerBound, upperBound: upperBound)
        self.secondary = secondary.map {
            ScalarRange(lowerBound: $0.0, upperBound: $0.1)
        }
        self.tertiary = tertiary.map {
            ScalarRange(lowerBound: $0.0, upperBound: $0.1)
        }
    }

    package init?(name: Substring) {
        switch name {
        case "BasicLatin": self.init(0x0000, 0x007F)
        case "Latin-1Supplement": self.init(0x0080, 0x00FF)
        case "LatinExtended-A": self.init(0x0100, 0x017F)
        case "LatinExtended-B": self.init(0x0180, 0x024F)
        case "IPAExtensions": self.init(0x0250, 0x02AF)
        case "SpacingModifierLetters": self.init(0x02B0, 0x02FF)
        case "CombiningDiacriticalMarks": self.init(0x0300, 0x036F)
        case "GreekandCoptic": self.init(0x0370, 0x03FF)
        case "Cyrillic": self.init(0x0400, 0x04FF)
        case "CyrillicSupplement": self.init(0x0500, 0x052F)
        case "Armenian": self.init(0x0530, 0x058F)
        case "Hebrew": self.init(0x0590, 0x05FF)
        case "Arabic": self.init(0x0600, 0x06FF)
        case "Syriac": self.init(0x0700, 0x074F)
        case "ArabicSupplement": self.init(0x0750, 0x077F)
        case "Thaana": self.init(0x0780, 0x07BF)
        case "NKo": self.init(0x07C0, 0x07FF)
        case "Devanagari": self.init(0x0900, 0x097F)
        case "Bengali": self.init(0x0980, 0x09FF)
        case "Gurmukhi": self.init(0x0A00, 0x0A7F)
        case "Gujarati": self.init(0x0A80, 0x0AFF)
        case "Oriya": self.init(0x0B00, 0x0B7F)
        case "Tamil": self.init(0x0B80, 0x0BFF)
        case "Telugu": self.init(0x0C00, 0x0C7F)
        case "Kannada": self.init(0x0C80, 0x0CFF)
        case "Malayalam": self.init(0x0D00, 0x0D7F)
        case "Sinhala": self.init(0x0D80, 0x0DFF)
        case "Thai": self.init(0x0E00, 0x0E7F)
        case "Lao": self.init(0x0E80, 0x0EFF)
        case "Tibetan": self.init(0x0F00, 0x0FFF)
        case "Myanmar": self.init(0x1000, 0x109F)
        case "Georgian": self.init(0x10A0, 0x10FF)
        case "HangulJamo": self.init(0x1100, 0x11FF)
        case "Ethiopic": self.init(0x1200, 0x137F)
        case "EthiopicSupplement": self.init(0x1380, 0x139F)
        case "Cherokee": self.init(0x13A0, 0x13FF)
        case "UnifiedCanadianAboriginalSyllabics": self.init(0x1400, 0x167F)
        case "Ogham": self.init(0x1680, 0x169F)
        case "Runic": self.init(0x16A0, 0x16FF)
        case "Tagalog": self.init(0x1700, 0x171F)
        case "Hanunoo": self.init(0x1720, 0x173F)
        case "Buhid": self.init(0x1740, 0x175F)
        case "Tagbanwa": self.init(0x1760, 0x177F)
        case "Khmer": self.init(0x1780, 0x17FF)
        case "Mongolian": self.init(0x1800, 0x18AF)
        case "Limbu": self.init(0x1900, 0x194F)
        case "TaiLe": self.init(0x1950, 0x197F)
        case "NewTaiLue": self.init(0x1980, 0x19DF)
        case "KhmerSymbols": self.init(0x19E0, 0x19FF)
        case "Buginese": self.init(0x1A00, 0x1A1F)
        case "Balinese": self.init(0x1B00, 0x1B7F)
        case "Sundanese": self.init(0x1B80, 0x1BBF)
        case "Lepcha": self.init(0x1C00, 0x1C4F)
        case "OlChiki": self.init(0x1C50, 0x1C7F)
        case "PhoneticExtensions": self.init(0x1D00, 0x1D7F)
        case "PhoneticExtensionsSupplement": self.init(0x1D80, 0x1DBF)
        case "CombiningDiacriticalMarksSupplement": self.init(0x1DC0, 0x1DFF)
        case "LatinExtendedAdditional": self.init(0x1E00, 0x1EFF)
        case "GreekExtended": self.init(0x1F00, 0x1FFF)
        case "GeneralPunctuation": self.init(0x2000, 0x206F)
        case "SuperscriptsandSubscripts": self.init(0x2070, 0x209F)
        case "CurrencySymbols": self.init(0x20A0, 0x20CF)
        case "CombiningDiacriticalMarksforSymbols": self.init(0x20D0, 0x20FF)
        case "LetterlikeSymbols": self.init(0x2100, 0x214F)
        case "NumberForms": self.init(0x2150, 0x218F)
        case "Arrows": self.init(0x2190, 0x21FF)
        case "MathematicalOperators": self.init(0x2200, 0x22FF)
        case "MiscellaneousTechnical": self.init(0x2300, 0x23FF)
        case "ControlPictures": self.init(0x2400, 0x243F)
        case "OpticalCharacterRecognition": self.init(0x2440, 0x245F)
        case "EnclosedAlphanumerics": self.init(0x2460, 0x24FF)
        case "BoxDrawing": self.init(0x2500, 0x257F)
        case "BlockElements": self.init(0x2580, 0x259F)
        case "GeometricShapes": self.init(0x25A0, 0x25FF)
        case "MiscellaneousSymbols": self.init(0x2600, 0x26FF)
        case "Dingbats": self.init(0x2700, 0x27BF)
        case "MiscellaneousMathematicalSymbols-A": self.init(0x27C0, 0x27EF)
        case "SupplementalArrows-A": self.init(0x27F0, 0x27FF)
        case "BraillePatterns": self.init(0x2800, 0x28FF)
        case "SupplementalArrows-B": self.init(0x2900, 0x297F)
        case "MiscellaneousMathematicalSymbols-B": self.init(0x2980, 0x29FF)
        case "SupplementalMathematicalOperators": self.init(0x2A00, 0x2AFF)
        case "MiscellaneousSymbolsandArrows": self.init(0x2B00, 0x2BFF)
        case "Glagolitic": self.init(0x2C00, 0x2C5F)
        case "LatinExtended-C": self.init(0x2C60, 0x2C7F)
        case "Coptic": self.init(0x2C80, 0x2CFF)
        case "GeorgianSupplement": self.init(0x2D00, 0x2D2F)
        case "Tifinagh": self.init(0x2D30, 0x2D7F)
        case "EthiopicExtended": self.init(0x2D80, 0x2DDF)
        case "CyrillicExtended-A": self.init(0x2DE0, 0x2DFF)
        case "SupplementalPunctuation": self.init(0x2E00, 0x2E7F)
        case "CJKRadicalsSupplement": self.init(0x2E80, 0x2EFF)
        case "KangxiRadicals": self.init(0x2F00, 0x2FDF)
        case "IdeographicDescriptionCharacters": self.init(0x2FF0, 0x2FFF)
        case "CJKSymbolsandPunctuation": self.init(0x3000, 0x303F)
        case "Hiragana": self.init(0x3040, 0x309F)
        case "Katakana": self.init(0x30A0, 0x30FF)
        case "Bopomofo": self.init(0x3100, 0x312F)
        case "HangulCompatibilityJamo": self.init(0x3130, 0x318F)
        case "Kanbun": self.init(0x3190, 0x319F)
        case "BopomofoExtended": self.init(0x31A0, 0x31BF)
        case "CJKStrokes": self.init(0x31C0, 0x31EF)
        case "KatakanaPhoneticExtensions": self.init(0x31F0, 0x31FF)
        case "EnclosedCJKLettersandMonths": self.init(0x3200, 0x32FF)
        case "CJKCompatibility": self.init(0x3300, 0x33FF)
        case "CJKUnifiedIdeographsExtensionA": self.init(0x3400, 0x4DBF)
        case "YijingHexagramSymbols": self.init(0x4DC0, 0x4DFF)
        case "CJKUnifiedIdeographs": self.init(0x4E00, 0x9FFF)
        case "YiSyllables": self.init(0xA000, 0xA48F)
        case "YiRadicals": self.init(0xA490, 0xA4CF)
        case "Vai": self.init(0xA500, 0xA63F)
        case "CyrillicExtended-B": self.init(0xA640, 0xA69F)
        case "ModifierToneLetters": self.init(0xA700, 0xA71F)
        case "LatinExtended-D": self.init(0xA720, 0xA7FF)
        case "SylotiNagri": self.init(0xA800, 0xA82F)
        case "Phags-pa": self.init(0xA840, 0xA87F)
        case "Saurashtra": self.init(0xA880, 0xA8DF)
        case "KayahLi": self.init(0xA900, 0xA92F)
        case "Rejang": self.init(0xA930, 0xA95F)
        case "Cham": self.init(0xAA00, 0xAA5F)
        case "HangulSyllables": self.init(0xAC00, 0xD7AF)
        case "HighSurrogates": self.init(0xD800, 0xDB7F)
        case "HighPrivateUseSurrogates": self.init(0xDB80, 0xDBFF)
        case "LowSurrogates": self.init(0xDC00, 0xDFFF)
        case "PrivateUseArea": self.init(0xE000, 0xF8FF)
        case "CJKCompatibilityIdeographs": self.init(0xF900, 0xFAFF)
        case "AlphabeticPresentationForms": self.init(0xFB00, 0xFB4F)
        case "ArabicPresentationForms-A": self.init(0xFB50, 0xFDFF)
        case "VariationSelectors": self.init(0xFE00, 0xFE0F)
        case "VerticalForms": self.init(0xFE10, 0xFE1F)
        case "CombiningHalfMarks": self.init(0xFE20, 0xFE2F)
        case "CJKCompatibilityForms": self.init(0xFE30, 0xFE4F)
        case "SmallFormVariants": self.init(0xFE50, 0xFE6F)
        case "ArabicPresentationForms-B": self.init(0xFE70, 0xFEFF)
        case "HalfwidthandFullwidthForms": self.init(0xFF00, 0xFFEF)
        case "Specials": self.init(0xFFF0, 0xFFFF)
        case "LinearBSyllabary": self.init(0x10000, 0x1007F)
        case "LinearBIdeograms": self.init(0x10080, 0x100FF)
        case "AegeanNumbers": self.init(0x10100, 0x1013F)
        case "AncientGreekNumbers": self.init(0x10140, 0x1018F)
        case "AncientSymbols": self.init(0x10190, 0x101CF)
        case "PhaistosDisc": self.init(0x101D0, 0x101FF)
        case "Lycian": self.init(0x10280, 0x1029F)
        case "Carian": self.init(0x102A0, 0x102DF)
        case "OldItalic": self.init(0x10300, 0x1032F)
        case "Gothic": self.init(0x10330, 0x1034F)
        case "Ugaritic": self.init(0x10380, 0x1039F)
        case "OldPersian": self.init(0x103A0, 0x103DF)
        case "Deseret": self.init(0x10400, 0x1044F)
        case "Shavian": self.init(0x10450, 0x1047F)
        case "Osmanya": self.init(0x10480, 0x104AF)
        case "CypriotSyllabary": self.init(0x10800, 0x1083F)
        case "Phoenician": self.init(0x10900, 0x1091F)
        case "Lydian": self.init(0x10920, 0x1093F)
        case "Kharoshthi": self.init(0x10A00, 0x10A5F)
        case "Cuneiform": self.init(0x12000, 0x123FF)
        case "CuneiformNumbersandPunctuation": self.init(0x12400, 0x1247F)
        case "ByzantineMusicalSymbols": self.init(0x1D000, 0x1D0FF)
        case "MusicalSymbols": self.init(0x1D100, 0x1D1FF)
        case "AncientGreekMusicalNotation": self.init(0x1D200, 0x1D24F)
        case "TaiXuanJingSymbols": self.init(0x1D300, 0x1D35F)
        case "CountingRodNumerals": self.init(0x1D360, 0x1D37F)
        case "MathematicalAlphanumericSymbols": self.init(0x1D400, 0x1D7FF)
        case "MahjongTiles": self.init(0x1F000, 0x1F02F)
        case "DominoTiles": self.init(0x1F030, 0x1F09F)
        case "CJKUnifiedIdeographsExtensionB": self.init(0x20000, 0x2A6DF)
        case "CJKCompatibilityIdeographsSupplement": self.init(0x2F800, 0x2FA1F)
        case "Tags": self.init(0xE0000, 0xE007F)
        case "VariationSelectorsSupplement": self.init(0xE0100, 0xE01EF)
        case "SupplementaryPrivateUseArea-A": self.init(0xF0000, 0xFFFFF)
        case "SupplementaryPrivateUseArea-B": self.init(0x100000, 0x10FFFF)

        // XSD retains the Unicode 3.1 names for superseded block names.
        case "Greek": self.init(0x0370, 0x03FF)
        case "CombiningMarksforSymbols": self.init(0x20D0, 0x20FF)
        case "PrivateUse":
            self.init(
                0xE000,
                0xF8FF,
                secondary: (0xF0000, 0xFFFFD),
                tertiary: (0x100000, 0x10FFFD)
            )
        default:
            return nil
        }
    }

    package func contains(_ scalar: Unicode.Scalar) -> Bool {
        let value = scalar.value
        return primary.contains(value)
            || secondary?.contains(value) == true
            || tertiary?.contains(value) == true
    }
}
