#!/usr/bin/env node
// Known blind spots (not checked): class strings held in variables or maps, `*ClassName` props, cva().
// Usage: node scripts/check-host-classes.mjs --css <path-or-url> [files...] [--allow a,b] [--allow-empty]
// Defaults to changed src/**/*.tsx vs origin/main. HOST_CSS_URL env is used when --css is absent.
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { extractClassesDetailed, missingClasses } from './lib/class-check.mjs';

function parseArgs(argv) {
  const out = { css: process.env.HOST_CSS_URL || '', files: [], allow: [], allowEmpty: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--css') out.css = argv[++i] || '';
    else if (argv[i] === '--allow') out.allow.push(...(argv[++i] || '').split(',').filter(Boolean));
    else if (argv[i] === '--allow-empty') out.allowEmpty = true;
    else out.files.push(argv[i]);
  }
  return out;
}

async function loadCss(src) {
  if (/^https?:\/\//i.test(src)) {
    const res = await fetch(src);
    if (!res.ok) throw new Error(`fetching host css failed: HTTP ${res.status}`);
    return res.text();
  }
  return readFileSync(src, 'utf8');
}

function changedFiles() {
  const out = execFileSync('git', ['diff', '--name-only', 'origin/main', '--', 'src'], {
    encoding: 'utf8',
  });
  const untracked = execFileSync(
    'git',
    ['ls-files', '--others', '--exclude-standard', '--', 'src'],
    { encoding: 'utf8' },
  );
  return [...new Set([...out.split('\n'), ...untracked.split('\n')].filter((f) => /^src\/.*\.tsx$/.test(f)))];
}

async function main() {
  const { css, files: given, allow, allowEmpty } = parseArgs(process.argv.slice(2));
  if (!css) {
    console.error('missing --css <path-or-url> (or HOST_CSS_URL)');
    return 2;
  }
  const cssText = await loadCss(css);
  const files = given.length ? given : changedFiles();
  if (!files.length && !allowEmpty) {
    console.error('WARNING: 0 files to check (no changed/untracked src/**/*.tsx). Pass files or --allow-empty.');
    return 2;
  }
  let bad = 0;
  for (const file of files) {
    const { classes, rejected } = extractClassesDetailed(readFileSync(file, 'utf8'));
    for (const tok of rejected) console.warn(`WARNING ${file}: token not checked (failed filter): ${tok}`);
    const missing = missingClasses(classes, cssText, allow);
    if (missing.length) {
      bad += missing.length;
      console.log(`${file}: ${missing.length} missing\n  ${missing.join('\n  ')}`);
    }
  }
  console.log(bad ? `FAIL: ${bad} missing class(es)` : `OK: ${files.length} file(s) checked, none missing`);
  return bad ? 1 : 0;
}

main().then(
  (code) => process.exit(code),
  (err) => {
    console.error(err.message);
    process.exit(2);
  },
);
