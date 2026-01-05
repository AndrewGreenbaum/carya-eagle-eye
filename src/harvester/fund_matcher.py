"""
Centralized Fund Name Matching.

Used by both extractor.py and storage.py to consistently match investor names
to tracked fund slugs. Prevents deal mislinks caused by order-dependent substring matching.

FIX: Added word boundary matching to prevent false positives like "menlo business" → "menlo".
FIX: Partner names now require investment context to avoid false positives.
FIX: Negative keywords now use word boundaries instead of substring matching.
"""

import re
from typing import Optional, Tuple


# Fund name variants for matching (FUND NAMES ONLY - no partner names)
# Order matters within each fund's list - more specific variants first
FUND_NAME_VARIANTS: dict[str, list[str]] = {
    "a16z": [
        "andreessen horowitz",
        # NOTE: "a16z crypto" removed - it's a SEPARATE fund with different thesis
        # Crypto deals should NOT count toward main a16z Enterprise AI portfolio
        "a16z bio",
        "a16z games",
        "a16z",
    ],
    "sequoia": [
        "sequoia capital",
        "sequoia",
    ],
    "benchmark": [
        "benchmark capital",
        "benchmark",
    ],
    "founders_fund": [
        "founders fund",
    ],
    "greylock": [
        "greylock partners",
        "greylock",
    ],
    "accel": [
        "accel partners",
        "accel",
    ],
    "index": [
        "index ventures",
    ],
    "insight": [
        "insight partners",
    ],
    "felicis": [
        "felicis ventures",
        "felicis",
    ],
    "redpoint": [
        "redpoint ventures",
        "redpoint",
    ],
    "khosla": [
        "khosla ventures",
        "khosla",
    ],
    "menlo": [
        "menlo ventures",
    ],
    "bessemer": [
        "bessemer venture partners",
        "bessemer",
        "bvp",
    ],
    "gv": [
        "google ventures",
        "gv",  # Word boundary regex handles disambiguation (no trailing space needed)
    ],
    "usv": [
        "union square ventures",
        "usv",
    ],
    "thrive": [
        "thrive capital",
        "thrive",  # Standalone - relies on negative keywords to filter "Thrive Global", "Thrive IT", etc.
    ],
    "general_catalyst": [
        "general catalyst",
    ],
    "first_round": [
        "first round capital",
        "first round",
    ],
}

# Partner names mapped to their funds
# FIX: These require investment context to avoid false positives like
# "Peter Thiel spoke at conference" matching as Founders Fund deal
# UPDATED 2026-01: Synced with funds.py partner_names for complete coverage
PARTNER_NAMES: dict[str, list[str]] = {
    "benchmark": [
        # Current GPs
        "chetan puttagunta",
        "peter fenton",
        "eric vishria",
        # Former (still newsworthy)
        "bill gurley",
        "sarah tavel",
        "miles grimshaw",
        "victor lazarte",
    ],
    "founders_fund": [
        # Current
        "peter thiel",
        "napoleon ta",
        "trae stephens",
        "lauren gross",
        "scott nolan",
        "john luttig",
        "delian asparouhov",
        "joey krug",
        "matias van thienen",
        "amin mirzadegan",
        "sean liu",
        # Former (still newsworthy)
        "keith rabois",
        "brian singerman",
    ],
    "khosla": [
        # Managing Directors
        "vinod khosla",
        "samir kaul",
        "sven strohband",
        "david weiden",
        "keith rabois",
        # Key Investors
        "ethan choi",
        "kanu gulati",
        "alex morgan",
        "jai sajnani",
    ],
    "greylock": [
        "reid hoffman",
        "asheem chandna",
        "jerry chen",
        "saam motamedi",
        "seth rosenberg",
        "mike duboe",
        "david sze",
        "josh elman",
        "corinne riley",
        "christine kim",
        "mor chen",
        "shreya shekhar",
    ],
    "redpoint": [
        "tomasz tunguz",
        "logan bartlett",
        "annie kadavy",
        "scott raney",
        "jason warner",
    ],
    "gv": [
        # Current GPs
        "m.g. siegler",
        "mg siegler",
        "crystal huang",
        "terri burns",
        "krishna yeshwant",
        "frédérique dame",
        "frederique dame",
        "david krane",
        "tyson clark",
        "brendan bulik-sullivan",
        "sangeen zeb",
        "issi rozen",
    ],
    "thrive": [
        # Current team
        "josh kushner",
        "kareem zaki",
        "nabil mallick",
        "belen mella",
        "gaurav ahuja",
        "joe kahn",
        "kyle yuan",
    ],
    "first_round": [
        # Current partners
        "josh kopelman",
        "todd jackson",
        "brett berson",
        "bill trenchard",
        "meka asonye",
        "liz wessel",
        "hayley barna",
    ],
}

# Investment context keywords - required when matching by partner name
# FIX: Partner names only match when investment context is present
INVESTMENT_CONTEXT_KEYWORDS = [
    "led", "leads", "leading", "co-led",
    "invested", "invests", "investing", "investment",
    "backed", "backs", "backing",
    "funding", "funded", "round",
    "series a", "series b", "series c", "seed",
    "raised", "raises", "raising",
    "participated", "participating", "participation",
    "joined", "joining",
]


# Negative keywords - if these appear, DON'T match to that fund
# NOTE: Be careful not to be too aggressive - these should only exclude
# clear false positives, not valid mentions that happen to contain similar words
NEGATIVE_KEYWORDS: dict[str, list[str]] = {
    "a16z": [
        "a16z crypto",           # Separate fund - different thesis (crypto-focused)
        "crypto fund",           # Crypto context
        "a16z cryptocurrency",   # Crypto variant
    ],
    "gv": [
        "nyse:gv",
        "nyse: gv",
        "gv stock",
        "visionary holdings",
        "global ventures",  # Different fund
    ],
    "thrive": [
        "thrive global",
        "thrive it",
        "thrive market",
        "thrive services",
        "thrive pet",  # Thrive Pet Healthcare - veterinary chain
        "thrive wellness",
        "thrive causemetics",  # Beauty brand
        "thrive fitness",
    ],
    "sequoia": [
        "hongshan",
        "peak xv",
        "sequoia india",
        "sequoia china",
        "sequoia heritage",  # Different entity
    ],
    "benchmark": [
        "benchmark international",
        "benchmark electronics",
        "benchmark capital management",  # Different fund
        "benchmark mineral",
        "benchmark litigation",
    ],
    "insight": [
        "insight venture management",  # Subsidiary - actually still valid
        # Removed: "market insight", "business insight", "consumer insight"
        # These were too aggressive and could appear in valid Insight Partners articles
    ],
    "index": [
        "index fund",
        "stock index",
        "s&p index",
        "market index",
        # These are specific enough to not exclude valid Index Ventures mentions
    ],
    "accel": [
        # Removed: "accelerate", "acceleration" - too aggressive
        # These words commonly appear in valid startup funding news
        "accelerator program",  # Keep - refers to accelerators, not Accel
        "accel entertainment",  # Different company (gaming)
    ],
    "menlo": [
        # Removed: "menlo park" - Menlo Ventures IS based in Menlo Park
        # and many articles mention both legitimately
        "menlo college",
        "menlo school",
    ],
    "redpoint": [
        "redpoint bio",
        "redpoint positioning",
        "redpoint global",  # Different fund
    ],
    "general_catalyst": [
        # Removed: "general catalyst partners" - this is just their full legal name
        # and shouldn't be excluded
    ],
}


# SEC legal entity patterns → fund slugs
# SEC Form D filings use legal entity names, not public fund names
SEC_ENTITY_PATTERNS: dict[str, list[str]] = {
    "benchmark": [
        "benchmark capital management",
        "benchmark capital partners",
    ],
    "thrive": [
        "thrive partners",
        "tc group",  # Thrive Capital's legal name
    ],
    "greylock": [
        "greylock partners",
        "greylock xv",
        "greylock xiv",
        "greylock xiii",
    ],
    "redpoint": [
        "redpoint omega",
        "redpoint ventures",
    ],
    "first_round": [
        "first round capital",
        "first round lp",
    ],
    "a16z": [
        "andreessen horowitz",
        "ah capital",
    ],
    "sequoia": [
        "sequoia capital",
        "sc us",  # Sequoia Capital US
    ],
    "founders_fund": [
        "founders fund",
        "ff angel",
    ],
    "index": [
        "index ventures",
    ],
    "insight": [
        "insight partners",
        "insight venture partners",
    ],
    "bessemer": [
        "bessemer venture partners",
        "bvp",
    ],
    "gv": [
        "google ventures",
        "gv",  # Word boundary used in matcher
    ],
    "felicis": [
        "felicis ventures",
    ],
    "general_catalyst": [
        "general catalyst",
    ],
    "khosla": [
        "khosla ventures",
    ],
    "menlo": [
        "menlo ventures",
    ],
    "usv": [
        "union square ventures",
    ],
    "accel": [
        "accel partners",
    ],
}


def _has_investment_context(text: str) -> bool:
    """Check if text contains investment-related context words."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in INVESTMENT_CONTEXT_KEYWORDS)


def _check_negative_keywords(name_lower: str, slug: str) -> bool:
    """
    Check if name matches any negative keywords for a fund.

    FIX: Uses word boundaries instead of substring matching to avoid
    false positives like "global ventures" matching "gv" negative "global ventures".
    """
    negative_list = NEGATIVE_KEYWORDS.get(slug, [])
    for neg in negative_list:
        # Use word boundaries for negative keywords
        pattern = rf'\b{re.escape(neg)}\b'
        if re.search(pattern, name_lower):
            return True
    return False


def match_fund_name(investor_name: str, context_text: str = "") -> Optional[str]:
    """
    Match an investor name to a tracked fund slug.

    Uses a multi-pass approach:
    1. Check negative keywords first to exclude false positives (using word boundaries)
    2. Check fund name variants for direct matches
    3. Check partner names ONLY if investment context is present

    Args:
        investor_name: The investor name to match (e.g., "Felicis Ventures")
        context_text: Optional surrounding text for partner name context validation

    Returns:
        Fund slug if matched (e.g., "felicis"), None if no match

    Examples:
        >>> match_fund_name("Felicis Ventures")
        'felicis'
        >>> match_fund_name("Andreessen Horowitz")
        'a16z'
        >>> match_fund_name("Benchmark International")  # Excluded by negative keyword
        None
        >>> match_fund_name("Bill Gurley")  # No investment context
        None
        >>> match_fund_name("Bill Gurley", "Bill Gurley led the Series A")
        'benchmark'
    """
    if not investor_name:
        return None

    name_lower = investor_name.lower().strip()
    context_lower = context_text.lower() if context_text else ""

    # Build a map of all matches found
    matches: list[tuple[str, int]] = []  # (slug, match_position)

    # Pass 1: Check fund name variants (always valid)
    for slug, variants in FUND_NAME_VARIANTS.items():
        # Check negative keywords first (using word boundaries)
        if _check_negative_keywords(name_lower, slug):
            continue  # Skip this fund entirely

        # Check for positive matches with word boundary
        for variant in variants:
            pattern = rf'\b{re.escape(variant)}\b'
            match = re.search(pattern, name_lower)
            if match:
                pos = match.start()
                matches.append((slug, pos))
                break  # Only count first matching variant per fund

    # Pass 2: Check partner names (only with investment context)
    # FIX: Partner names like "Bill Gurley" require investment context
    # to avoid matching "Bill Gurley spoke at conference"
    has_context = _has_investment_context(name_lower) or _has_investment_context(context_lower)

    if has_context:
        for slug, partners in PARTNER_NAMES.items():
            # Skip if negative keywords apply
            if _check_negative_keywords(name_lower, slug):
                continue

            # Already matched this slug via fund name
            if any(s == slug for s, _ in matches):
                continue

            for partner in partners:
                pattern = rf'\b{re.escape(partner)}\b'
                match = re.search(pattern, name_lower)
                if match:
                    pos = match.start()
                    matches.append((slug, pos))
                    break

    if not matches:
        return None

    # Return the match that appears earliest in the string
    matches.sort(key=lambda x: x[1])
    return matches[0][0]
