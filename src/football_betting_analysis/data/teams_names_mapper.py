import re
from rapidfuzz import process, fuzz

def build_team_mapping(normalize, source_names, target_names, threshold=88, aliases = None):
    """
    Maps each source_name to exact target_name.

    Parameters:
        normalize: The normalize function, which normalizes the names of the teams before the matching is performed
        source_names: The invalid names, that will be renamed
        target_names: The valid names, that will be used to rename the source names
        threshold: The tolerance level, deciding the cutoff point between a "match" and a "non-match" when comparing the source_names and the target_names
        aliases: An aliases dict, which if passed to the function, is with highest priority and checks for more specific cases with words completely different one from another

    Returns:
        mapping: dict {source_name: target_name}
        unresolved: list of names not confidently matched
    """

    # Normalize target names but KEEP original
    normalized_targets = {
        normalize(name): name for name in target_names
    }

    target_keys = list(normalized_targets.keys())

    mapping = {}
    unresolved = []

    for source in source_names:
        norm_source = normalize(source)

        # matching the ones which are with completely different names (highest priority)
        if aliases is not None:
            if norm_source in aliases:
                mapped = aliases[norm_source]

                if mapped in target_names:
                    mapping[source] = mapped
                else:
                    mapping[source] = None
                    unresolved.append(source)

                continue
        
        # Skipping obvious "B teams"
        if re.search(r"\bB\b", source):
            continue
        
        # exact normalized match
        if norm_source in normalized_targets:
            mapping[source] = normalized_targets[norm_source]
            continue

        # token-based strict match (subset logic)
        source_tokens = set(norm_source.split())

        best_match = None
        best_score = 0

        for tgt_norm in target_keys:
            tgt_tokens = set(tgt_norm.split())

            # Token overlap score
            overlap = len(source_tokens & tgt_tokens) / len(tgt_tokens)

            if overlap > best_score:
                best_score = overlap
                best_match = tgt_norm

        if best_score == 1.0:
            mapping[source] = normalized_targets[best_match]
            continue

        # fuzzy match (LAST fallback)
        match, score, _ = process.extractOne(
            norm_source,
            target_keys,
            scorer=fuzz.token_set_ratio
        )

        if score >= threshold:
            mapping[source] = normalized_targets[match]
        else:
            mapping[source] = None
            unresolved.append(source)

    return mapping, unresolved