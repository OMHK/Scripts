import pandas as pd
import re
from rapidfuzz import fuzz
import phonetics  # pip install phonetics

# -------------------------
# Config
# -------------------------
STOPWORDS = {"of", "the", "and", "al", "el"}
LOW_CONFIDENCE_THRESHOLD = 0.75  # below this, flag for review

# -------------------------
# Text Cleaning
# -------------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.sub(r"[^\w\s]", "", text)   # remove punctuation
    tokens = [t for t in text.split() if t not in STOPWORDS]
    return " ".join(tokens)

# -------------------------
# Phonetic Helper
# -------------------------
def phonetic_code(word):
    try:
        return phonetics.soundex(word) or word
    except:
        return word

# -------------------------
# Ensemble Scoring
# -------------------------
def ensemble_score(a, b):
    a = clean_text(a)
    b = clean_text(b)
    if not a or not b:
        return 0

    # Base fuzzy metrics
    ratio = fuzz.ratio(a, b) / 100
    partial = fuzz.partial_ratio(a, b) / 100
    token_sort = fuzz.token_sort_ratio(a, b) / 100
    token_set = fuzz.token_set_ratio(a, b) / 100

    # Phonetic similarity (first token)
    a_first = phonetic_code(a.split()[0]) if a.split() else ""
    b_first = phonetic_code(b.split()[0]) if b.split() else ""
    phonetic_match = 1.0 if a_first == b_first and a_first else 0.0

    # Weighted score (you can tune these weights)
    score = (
        0.25 * ratio +
        0.25 * partial +
        0.25 * token_sort +
        0.20 * token_set +
        0.05 * phonetic_match
    )
    return min(score, 1.0)

# -------------------------
# Best Match Finder
# -------------------------
def best_match(user_name, choices):
    scores = [(choice, ensemble_score(user_name, choice)) for choice in choices if pd.notna(choice)]
    if not scores:
        return None, 0
    return max(scores, key=lambda x: x[1])  # (best_name, best_score)

# -------------------------
# Main Script
# -------------------------
def main():
    # Load Excel
    df = pd.read_excel("input.xlsx")

    # Create lookup (Official Name -> ID)
    ref_dict = dict(zip(df["ALPHA"], df["ID"]))

    # Run fuzzy matching
    matches = df["BETA"].apply(lambda x: best_match(x, df["ALPHA"]))

    # Extract results
    df["MatchedOfficial"] = matches.apply(lambda x: x[0])
    df["SimilarityScore"] = matches.apply(lambda x: round(x[1], 3))
    df["MatchedID"] = df["MatchedOfficial"].map(ref_dict)

    # Flag low-confidence matches
    df["NeedsReview"] = df["SimilarityScore"].apply(lambda s: s < LOW_CONFIDENCE_THRESHOLD)

    # Save to Excel
    df.to_excel("output.xlsx", index=False)

    print("✅ Matching complete. Results saved to output.xlsx")

# -------------------------
if __name__ == "__main__":
    main()
