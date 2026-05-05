import re
import pandas as pd
import unicodedata

def clean_text_values(
    values,
    *,
    normalize="NFKC",
    fix_common_punctuation=True,
    collapse_whitespace=True,
    strip=True,
    remove_control_chars=True,
    remove_invisible_chars=True,
    preserve_index=True
):
    """
    Clean a pandas Series or list-like of text values.

    Parameters
    ----------
    values : pandas.Series | list | tuple | numpy.ndarray
        Input text values.
    normalize : str | None
        Unicode normalization form. Default "NFKC".
    fix_common_punctuation : bool
        Replace common curly quotes, dashes and ellipses with standard characters.
    collapse_whitespace : bool
        Replace any sequence of whitespace with a single space.
    strip : bool
        Remove leading and trailing whitespace.
    remove_control_chars : bool
        Remove non-printable control characters.
    remove_invisible_chars : bool
        Remove invisible unicode formatting characters.
    preserve_index : bool
        Preserve pandas Series index if input is a Series.

    Returns
    -------
    pandas.Series
        Cleaned text values.
    """
    if isinstance(values, pd.Series):
        index = values.index if preserve_index else None
        is_category = values.dtype.name == 'category'
    else:
        index = None
        is_category = False

    series = pd.Series(values, index=index, dtype="object")

    control_pattern = re.compile(r"[\x00-\x1f\x7f-\x9f]")
    invisible_pattern = re.compile(
        r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]"
    )
    whitespace_pattern = re.compile(r"\s+")
    punctuation_map = str.maketrans({
    "‘": "'",
    "’": "'",
    "‚": ",",
    "‛": "'",
    "“": '"',
    "”": '"',
    "„": '"',
    "‟": '"',
    "–": "-",
    "—": "-",
    "−": "-",
    "-": "-",
    "…": "...",
    "·": ".",
    "•": "-",
    "´": "'",
    "˚": "°",
    "ª": "a",
    "º": "o",
})

    def decode_bytes(value):
        for encoding in ("utf-8", "cp1252", "latin1"):
            try:
                return value.decode(encoding)
            except Exception:
                continue
        return value.decode("utf-8", errors="replace")

    def clean_scalar(value):
        if pd.isna(value):
            return value

        if isinstance(value, (bytes, bytearray)):
            text = decode_bytes(value)
        elif not isinstance(value, str):
            text = str(value)
        else:
            text = value

        if normalize:
            text = unicodedata.normalize(normalize, text)

        if fix_common_punctuation:
            text = text.translate(punctuation_map)

        if remove_invisible_chars:
            text = invisible_pattern.sub("", text)

        if remove_control_chars:
            text = control_pattern.sub("", text)

        if collapse_whitespace:
            text = whitespace_pattern.sub(" ", text)

        if strip:
            text = text.strip()

        return text

    if is_category:
        categories = values.cat.categories

        cleaned_categories = pd.Series(categories).map(clean_scalar)

        # Handle possible duplicates after cleaning
        cleaned = values.cat.rename_categories(cleaned_categories)

        # If duplicates occurred, re-factorize
        if cleaned_categories.duplicated().any():
            cleaned = cleaned.astype("category")

        return cleaned

    cleaned = series.map(clean_scalar)
    
    return cleaned