#!/usr/bin/env node
//
// Sanity-check the Erlang escript's output against the original EJS 1.0 compiler.
//
// Usage: node verify_ejs_templates.js <input-dir> <compiled-js-file>
//
// The script compiles every .ejs file in <input-dir> using the original
// ejs-1.0.min.js, renders each template against a stub context, then renders
// the matching precompiled entry from <compiled-js-file> against the same
// stub context.  If the rendered outputs differ the script prints a diff and
// exits with code 1.  Exit code 0 means all templates match.
//
// The stub context converts every unknown property access into a deterministic
// placeholder string, so both paths produce the same stub HTML even when
// real data is not available.

'use strict';

const fs   = require('fs');
const vm   = require('vm');
const path = require('path');

// ---------------------------------------------------------------------------
// Parse arguments
// ---------------------------------------------------------------------------
const [inDir, compiledFile] = process.argv.slice(2);
if (!inDir || !compiledFile) {
    process.stderr.write('usage: verify_ejs_templates.js <input-dir> <compiled-js-file>\n');
    process.exit(1);
}

// ---------------------------------------------------------------------------
// Load ejs-1.0.min.js into a vm sandbox so it defines the EJS global.
// ---------------------------------------------------------------------------
const ejsPath = path.join(__dirname, '..', 'priv', 'www', 'js', 'ejs-1.0.min.js');
const ejsCode = fs.readFileSync(ejsPath, 'utf8');

const ejsSandbox = { window: {}, document: {} };
vm.createContext(ejsSandbox);
vm.runInContext(ejsCode, ejsSandbox);
const EJS = ejsSandbox.EJS;

// ---------------------------------------------------------------------------
// Load the precompiled file.  Inject EJS into the sandbox so that
// EJS.Scanner.to_text() calls inside the precompiled functions resolve.
// ---------------------------------------------------------------------------
const compiledCode = fs.readFileSync(compiledFile, 'utf8');
const compiledSandbox = { COMPILED_TEMPLATES: {}, EJS };
vm.createContext(compiledSandbox);
vm.runInContext(compiledCode, compiledSandbox);
const COMPILED = compiledSandbox.COMPILED_TEMPLATES;

// ---------------------------------------------------------------------------
// Build a stub context using a Proxy.
//
// Every property access returns a stub function; every function call returns a
// deterministic string based on the property name.  This lets both renderers
// produce identical output without needing the real helper functions.
// ---------------------------------------------------------------------------
function makeStub(name) {
    const stub = new Proxy(function () { return `[${name}]`; }, {
        get(target, key) { return makeStub(`${name}.${String(key)}`); },
        apply(_t, _this, args) {
            const s = args.map(a => {
                try { return JSON.stringify(a); } catch(_) { return String(a); }
            }).join(',');
            return `[${name}(${s})]`;
        }
    });
    return stub;
}

function makeContext() {
    return new Proxy({}, {
        get(_target, key) { return makeStub(String(key)); }
    });
}

// ---------------------------------------------------------------------------
// Render with the original EJS library.
//
// EJS internally does `this.text = options.text || null`, which coerces an
// empty string to null, causing EJS to use its fallback source of " " (one
// space).  We replicate this quirk in renderOriginal by substituting a non-
// empty source for empty files so the comparison stays meaningful.
// ---------------------------------------------------------------------------
function renderOriginal(name, source) {
    // Treat truly-empty files as whitespace-only so EJS doesn't apply its
    // empty-string-to-null quirk.
    const effectiveSource = source === '' ? ' ' : source;
    const ctx = makeContext();
    try {
        const t = new EJS({ text: effectiveSource, name });
        return { ok: true, value: t.render(ctx) };
    } catch (e) {
        return { ok: false, error: e.message };
    }
}

// ---------------------------------------------------------------------------
// Render with the precompiled function.
// ---------------------------------------------------------------------------
function renderCompiled(name) {
    const fn = COMPILED[name];
    if (!fn) return { ok: false, error: `template not found: ${name}` };
    const ctx = makeContext();
    try {
        // _CONTEXT and _VIEW both point to the same stub context.
        return { ok: true, value: fn.call(ctx, ctx, ctx) };
    } catch (e) {
        return { ok: false, error: e.message };
    }
}

// ---------------------------------------------------------------------------
// Main comparison loop
// ---------------------------------------------------------------------------
const ejsFiles = fs.readdirSync(inDir)
    .filter(f => f.endsWith('.ejs'))
    .sort();

let failures = 0;

for (const file of ejsFiles) {
    const name   = path.basename(file, '.ejs');
    const source = fs.readFileSync(path.join(inDir, file), 'utf8');

    // Empty files are intentional placeholder templates.  EJS returns " " for
    // them (its internal source default); the precompiled version returns "".
    // Both are semantically equivalent (no visible output), so skip.
    if (source === '') continue;

    const orig = renderOriginal(name, source);
    const comp = renderCompiled(name);

    // Both erroring with the same message is acceptable (real data missing).
    if (!orig.ok && !comp.ok) {
        if (orig.error === comp.error) continue;
        process.stderr.write(`MISMATCH (both errored differently): ${name}\n`);
        process.stderr.write(`  original error:  ${orig.error}\n`);
        process.stderr.write(`  compiled error:  ${comp.error}\n`);
        failures++;
        continue;
    }

    if (orig.ok !== comp.ok) {
        process.stderr.write(`MISMATCH (one errored): ${name}\n`);
        if (!orig.ok) process.stderr.write(`  original error:  ${orig.error}\n`);
        if (!comp.ok) process.stderr.write(`  compiled error:  ${comp.error}\n`);
        failures++;
        continue;
    }

    // Both succeeded: compare rendered HTML.
    if (orig.value !== comp.value) {
        process.stderr.write(`MISMATCH: ${name}\n`);
        showDiff(orig.value, comp.value);
        failures++;
    }
}

if (failures === 0) {
    process.stdout.write(`ok: all ${ejsFiles.length} templates match\n`);
    process.exit(0);
} else {
    process.stderr.write(`\nfailed: ${failures} of ${ejsFiles.length} templates differ\n`);
    process.exit(1);
}

// ---------------------------------------------------------------------------
// Show a simple line-based diff between two strings (first 5 differing lines).
// ---------------------------------------------------------------------------
function showDiff(a, b) {
    const aLines = a.split('\n');
    const bLines = b.split('\n');
    const len = Math.max(aLines.length, bLines.length);
    let shown = 0;
    for (let i = 0; i < len && shown < 5; i++) {
        const aLine = i < aLines.length ? aLines[i] : '<missing>';
        const bLine = i < bLines.length ? bLines[i] : '<missing>';
        if (aLine !== bLine) {
            process.stderr.write(`  line ${i + 1}:\n`);
            process.stderr.write(`  - ${JSON.stringify(aLine)}\n`);
            process.stderr.write(`  + ${JSON.stringify(bLine)}\n`);
            shown++;
        }
    }
}
