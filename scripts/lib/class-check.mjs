// Pure helpers for checking Tailwind classes against a generated host stylesheet.

/** Escape a class name the way Tailwind escapes it in selectors (md:grid-cols-3 -> md\:grid-cols-3). */
export function escapeClass(cls) {
  return cls.replace(/[^a-zA-Z0-9_-]/g, (ch) => `\\${ch}`);
}

// Returns the index just past the closing quote/backtick of a literal starting at `i`,
// and the static text of the literal (template interpolations removed).
function readLiteral(src, i) {
  const quote = src[i];
  let j = i + 1;
  let text = '';
  while (j < src.length && src[j] !== quote) {
    if (src[j] === '\\') {
      text += ' ';
      j += 2;
      continue;
    }
    if (quote === '`' && src[j] === '$' && src[j + 1] === '{') {
      let depth = 1;
      j += 2;
      while (j < src.length && depth > 0) {
        if (src[j] === '"' || src[j] === "'" || src[j] === '`') {
          const [end, inner] = readLiteral(src, j);
          text += ' ' + inner + ' ';
          j = end;
          continue;
        }
        if (src[j] === '{') depth++;
        else if (src[j] === '}') depth--;
        j++;
      }
      text += ' ';
      continue;
    }
    text += src[j];
    j++;
  }
  return [j + 1, text];
}

// A literal compared with ===, !==, ==, != or used as a `case` label is data, not a class list.
function isComparisonOperand(src, start, end) {
  const before = src.slice(Math.max(0, start - 12), start);
  const after = src.slice(end, end + 6);
  return /(?:[=!]==?|\bcase)\s*$/.test(before) || /^\s*[=!]==?/.test(after);
}

/** Collect every static string literal inside a `{...}` expression starting at `start` (the `{`). */
function readExpression(src, start) {
  const strings = [];
  let depth = 0;
  let i = start;
  while (i < src.length) {
    const c = src[i];
    if (c === '"' || c === "'" || c === '`') {
      const [end, text] = readLiteral(src, i);
      if (!isComparisonOperand(src, i, end)) strings.push(text);
      i = end;
      continue;
    }
    if (c === '{') depth++;
    else if (c === '}') {
      depth--;
      if (depth === 0) return [i + 1, strings];
    }
    i++;
  }
  return [i, strings];
}

const TOKEN_RE = /^[[!\w-][\w:/[\].%#,()!&>*=~+-]*$/;

/** Like extractClasses, but also returns tokens that failed the token filter (never dropped silently). */
export function extractClassesDetailed(source) {
  const found = new Set();
  const rejected = new Set();
  const re = /className\s*=\s*/g;
  while (re.exec(source) !== null) {
    const i = re.lastIndex;
    const c = source[i];
    let chunks = [];
    if (c === '"' || c === "'") {
      chunks = [readLiteral(source, i)[1]];
    } else if (c === '{') {
      chunks = readExpression(source, i)[1];
    }
    for (const chunk of chunks) {
      for (const tok of chunk.split(/\s+/)) {
        if (!tok) continue;
        (TOKEN_RE.test(tok) ? found : rejected).add(tok);
      }
    }
  }
  return { classes: [...found], rejected: [...rejected] };
}

/** Class tokens from className="...", className={...} (every non-comparison string literal inside). */
export function extractClasses(source) {
  return extractClassesDetailed(source).classes;
}

// Marker classes with no CSS rule of their own.
const MARKER_RE = /^(?:peer|group)(?:\/[\w-]+)?$/;

/** Classes absent from cssText. `nkz-` token classes, peer/group markers and `allow` entries are not checked. */
export function missingClasses(classes, cssText, allow = []) {
  const missing = [];
  for (const cls of classes) {
    if (cls.includes('nkz-') || MARKER_RE.test(cls) || allow.includes(cls)) continue;
    const sel = `.${escapeClass(cls)}`;
    let idx = cssText.indexOf(sel);
    let ok = false;
    while (idx !== -1) {
      const next = cssText[idx + sel.length];
      if (next !== undefined && '{: ,>.'.includes(next)) {
        ok = true;
        break;
      }
      idx = cssText.indexOf(sel, idx + 1);
    }
    if (!ok) missing.push(cls);
  }
  return missing;
}
