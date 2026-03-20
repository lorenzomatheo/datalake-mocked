import re
from dataclasses import dataclass
from typing import List

from rapidfuzz import fuzz, process

from maggulake.utils.strings import remove_accents


def tokenize(input_str: str) -> set:
    """
    Tokenizes the input string into a set of words, removing accents and converting to lowercase.
    """
    # Remove accents and convert to lowercase
    input_str = remove_accents(input_str.lower())
    # Extract words using regex
    return set(re.findall(r'\w+', input_str))


@dataclass
class SuccessfulResult:
    matched_choice: str
    score: float
    token_overlap: int
    scorer_name: str


def fuzzy_matching(string: str, enum_options: List[str]) -> str | None:
    """
    Perform fuzzy matching to find the best match for the input string from a
    list of enum options using multiple scoring methods and considering token overlap.
    """

    if not string:
        return None

    scorers = [
        fuzz.ratio,
        fuzz.partial_ratio,
        fuzz.token_sort_ratio,
        fuzz.token_set_ratio,
        fuzz.WRatio,
        fuzz.QRatio,
    ]

    # Clear and tokenize the input string
    clean_string = remove_accents(string)
    string_tokens = tokenize(clean_string)

    results: list[SuccessfulResult] = []

    for scorer in scorers:
        # Use process.extract to get all matches, not just the best one
        all_results = process.extract(
            query=clean_string, choices=enum_options, scorer=scorer, limit=None
        )

        if not all_results:  # Se nao houver resultado, early continue
            continue

        for matched_choice, score, _ in all_results:
            match_tokens = tokenize(matched_choice)

            token_overlap = len(string_tokens & match_tokens)

            if token_overlap > 0:
                results.append(
                    SuccessfulResult(
                        matched_choice=matched_choice,
                        score=score,
                        token_overlap=token_overlap,
                        scorer_name=scorer.__name__,
                    )
                )

    if not results:
        return None

    # Ordena por token_overlap primeiro e depois por score
    best_match = max(results, key=lambda x: (x.token_overlap, x.score))

    # Retorna o match de melhor correspondência
    return best_match.matched_choice
