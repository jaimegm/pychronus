def keyify(original_key):
    replacements = [
        ("ß", "ss"),
        ("ä", "ae"),
        ("ö", "oe"),
        ("ü", "ue"),
        ("-", "_"),
        ("–", "_"),
        (" ", "_"),
        (":", ""),
        ("(", ""),
        (")", ""),
        ("#", "count"),
        ("/", ""),
        (".", ""),
        (",", ""),
        ("?", ""),
        ("+", ""),
        ("___", "_"),
        ("__", "_"),
    ]
    replaced = original_key.lower()
    for umlaut, replacement in replacements:
        replaced = replaced.replace(umlaut, replacement)
    return replaced.lower()
