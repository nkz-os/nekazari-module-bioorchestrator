import { describe, expect, it } from 'vitest';
import { escapeClass, extractClasses, extractClassesDetailed, missingClasses } from './class-check.mjs';

describe('extractClasses', () => {
  it('reads plain className strings', () => {
    expect(extractClasses('<div className="flex gap-3 p-2" />').sort()).toEqual(['flex', 'gap-3', 'p-2']);
  });
  it('reads braces and static template literals', () => {
    const src = "<a className={'mt-1 mb-2'} /><b className={`px-2 py-1`} />";
    expect(extractClasses(src).sort()).toEqual(['mb-2', 'mt-1', 'px-2', 'py-1']);
  });
  it('reads ternary and cn() literals, ignoring interpolations', () => {
    const src = 'className={cn("a-1", on ? "b-2 md:c-3" : \'d-4\', `e-5 ${x}`)}';
    expect(extractClasses(src).sort()).toEqual(['a-1', 'b-2', 'd-4', 'e-5', 'md:c-3']);
  });
  it('reads multi-line values', () => {
    const src = '<div\n  className="\n    flex\n    w-1/2\n    h-[2px]\n  "\n/>';
    expect(extractClasses(src).sort()).toEqual(['flex', 'h-[2px]', 'w-1/2']);
  });
  it('ignores non-className strings', () => {
    expect(extractClasses('const x = "not-a-class"; <p title="t-1" />')).toEqual([]);
  });
});

describe('extractClasses fix round 1', () => {
  it('pins modifier and sign forms', () => {
    const src = '<i className="!mt-1 -mt-1 bg-black/50 dark:bg-x hover:underline" />';
    expect(extractClasses(src).sort()).toEqual(['!mt-1', '-mt-1', 'bg-black/50', 'dark:bg-x', 'hover:underline']);
  });
  it('keeps arbitrary-variant tokens with a leading bracket', () => {
    expect(extractClasses('<i className="[&>*]:p-1" />')).toEqual(['[&>*]:p-1']);
  });
  it('skips comparison operands and case labels', () => {
    const src = `<i className={cn(kind === 'high' ? 'p-1' : 'p-2', 'low' !== kind && 'p-3', x == "mid" ? 'p-4' : 'p-5')} />
      <b className={(() => { switch (k) { case 'warning': return 'p-6'; } })()} />`;
    expect(extractClasses(src).sort()).toEqual(['p-1', 'p-2', 'p-3', 'p-4', 'p-5', 'p-6']);
  });
  it('reports rejected tokens instead of dropping them', () => {
    expect(extractClassesDetailed('<i className="ok {weird}" />')).toEqual({ classes: ['ok'], rejected: ['{weird}'] });
  });
});

describe('missingClasses', () => {
  const css = '.gap-3{gap:.75rem}@media (min-width:768px){.md\\:grid-cols-3{grid:x}}.hover\\:bg-nkz-surface:hover{a:b}.w-1\\/2{width:50%}.h-\\[2px\\]{height:2px}.gap-30x{a:b}';
  it('finds present classes', () => {
    expect(missingClasses(['gap-3', 'md:grid-cols-3', 'w-1/2', 'h-[2px]'], css)).toEqual([]);
  });
  it('reports absent classes and does not prefix-match', () => {
    expect(missingClasses(['gap-30', 'gap-4', 'bg-fuchsia-950/13'], css)).toEqual(['gap-30', 'gap-4', 'bg-fuchsia-950/13']);
  });
  it('skips peer/group markers and allowed classes only', () => {
    expect(missingClasses(['peer', 'group', 'group/row', 'peer/x', 'custom', 'other'], '', ['custom'])).toEqual(['other']);
  });
  it('skips nkz- token classes', () => {
    expect(missingClasses(['text-nkz-foo', 'hover:bg-nkz-surface'], '')).toEqual([]);
  });
});

describe('escapeClass', () => {
  it('escapes tailwind special characters', () => {
    expect(escapeClass('md:grid-cols-3')).toBe('md\\:grid-cols-3');
    expect(escapeClass('w-1/2')).toBe('w-1\\/2');
    expect(escapeClass('h-[2px]')).toBe('h-\\[2px\\]');
    expect(escapeClass('w-0.5')).toBe('w-0\\.5');
    expect(escapeClass('top-[10%]')).toBe('top-\\[10\\%\\]');
  });
});
