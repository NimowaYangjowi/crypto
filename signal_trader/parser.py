"""Signal parser — regex/template-based signal parsing for trading signals."""

import re

# ── Default Signal Pattern ─────────────────────────────────

SIGNAL_PATTERN = re.compile(
    r"#(\w+)\s*[–—-]\s*(LONG|SHORT)\s*"
    r".*?진입\s*포인트[:\s]*([\d.]+)\s*"
    r".*?목표\s*수익[:\s]*([\d.,\s]+)\s*"
    r".*?손절가[:\s]*([\d.]+)",
    re.DOTALL | re.IGNORECASE,
)


def parse_signal(text):
    """Parse a trading signal using the default pattern. Returns dict or None."""
    match = SIGNAL_PATTERN.search(text)
    if not match:
        return None
    ticker = match.group(1).upper()
    side = match.group(2).upper()
    entry = float(match.group(3))
    targets = [float(t.strip()) for t in match.group(4).split(",") if t.strip()]
    sl = float(match.group(5))
    if len(targets) < 3:
        return None
    return {
        "ticker": ticker,
        "side": side,
        "entry": entry,
        "tp1": targets[0],
        "tp2": targets[1],
        "tp3": targets[2],
        "tp4": targets[3] if len(targets) > 3 else targets[2],
        "sl": sl,
    }


# ── Template System ──────────────────────────────────────

PLACEHOLDER_RE = re.compile(r'\{(ticker|side|entry|tp1|tp2|tp3|sl|leverage|_\?|_)\}')

CAPTURE_MAP = {
    'ticker': r'(\w+)',
    'side': r'(LONG|SHORT|long|short)',
    'entry': r'([\d,.]+)',
    'tp1': r'([\d,.]+)',
    'tp2': r'([\d,.]+)',
    'tp3': r'([\d,.]+)',
    'sl': r'([\d,.]+)',
    'leverage': r'(\d+)',
    '_': r'(?:\S+)',   # non-capturing skip (required)
    '_?': r'(?:.*?)',  # non-capturing skip (optional, lazy)
}

WS_MARKER = '\x00WS\x00'


def compile_template(template: str):
    """Convert a template with {placeholders} to (compiled_regex, field_list)."""
    parts = PLACEHOLDER_RE.split(template)
    fields = []
    regex_str = ''

    for i, part in enumerate(parts):
        if i % 2 == 1:  # field name
            if part in ('_', '_?'):
                regex_str += CAPTURE_MAP[part]  # non-capturing skip
            else:
                fields.append(part)
                regex_str += CAPTURE_MAP[part]
        else:  # literal text
            cleaned = re.sub(r'\s+', WS_MARKER, part)
            escaped = re.escape(cleaned)
            escaped = escaped.replace(re.escape(WS_MARKER), r'\s+')
            regex_str += escaped

    compiled = re.compile(regex_str, re.DOTALL | re.IGNORECASE)
    return compiled, fields


def parse_with_template(text, compiled_regex, fields, default_side='LONG'):
    """Parse text using a compiled template regex."""
    match = compiled_regex.search(text)
    if not match:
        return None

    result = {}
    for i, field in enumerate(fields):
        value = match.group(i + 1).strip()
        if field == 'ticker':
            result['ticker'] = value.upper()
        elif field == 'side':
            result['side'] = value.upper()
        elif field == 'leverage':
            try:
                result['leverage'] = int(value)
            except ValueError:
                result['leverage'] = 1
        else:
            try:
                result[field] = float(value.replace(',', ''))
            except ValueError:
                pass

    if 'ticker' not in result:
        return None
    if 'side' not in result:
        result['side'] = default_side

    return result


def fill_signal_defaults(signal):
    """Fill missing TP/SL with defaults based on entry price and side."""
    entry = signal.get('entry')
    if entry is None:
        return signal

    side = signal.get('side', 'LONG')
    if side == 'LONG':
        signal.setdefault('sl', round(entry * 0.95, 8))
        signal.setdefault('tp1', round(entry * 1.015, 8))
        signal.setdefault('tp2', round(entry * 1.035, 8))
        signal.setdefault('tp3', round(entry * 1.10, 8))
    else:
        signal.setdefault('sl', round(entry * 1.05, 8))
        signal.setdefault('tp1', round(entry * 0.985, 8))
        signal.setdefault('tp2', round(entry * 0.965, 8))
        signal.setdefault('tp3', round(entry * 0.90, 8))

    signal.setdefault('tp4', signal['tp3'])
    return signal


def test_template(template, sample, default_side='LONG'):
    """Test a template against sample text. Returns parsed result or error."""
    try:
        compiled, fields = compile_template(template)
    except Exception as e:
        return {"error": f"Template compile error: {e}"}

    signal = parse_with_template(sample, compiled, fields, default_side)
    if not signal:
        return {"match": False, "pattern": compiled.pattern}

    # Simulate defaults
    if "entry" not in signal:
        signal["market_order"] = True
        signal["entry"] = "(market price)"
    else:
        fill_signal_defaults(signal)

    return {"match": True, "signal": signal, "fields_found": fields, "pattern": compiled.pattern}
