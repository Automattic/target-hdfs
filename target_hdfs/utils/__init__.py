import re


def bytes_to_mb(x):
    return x / (1024 * 1024)


def convert_size_to_bytes(size_str):
    """Convert a size string to bytes"""
    match = re.match(r"(\d+)([KkMmGg]?)", size_str)
    if not match:
        raise ValueError(f"Invalid size string: {size_str}")

    value, unit = match.groups()
    value = int(value)

    if unit.lower() == "k":
        value *= 1024
    elif unit.lower() == "m":
        value *= 1024 * 1024
    elif unit.lower() == "g":
        value *= 1024 * 1024 * 1024
    else:
        raise ValueError(f"Invalid unit: {unit}")

    return value
