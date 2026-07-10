// Minimal EJS runtime shim.
//
// The full ejs-1.0.min.js library is no longer loaded at runtime because all
// templates are precompiled at build time (see scripts/precompile_ejs_templates
// and the compile-ejs-templates Makefile target).
//
// The only EJS symbol that precompiled templates still reference is
// EJS.Scanner.to_text, which is called for every <%= expression %>.
// Everything else (EJS.Compiler, EJS.Buffer, EJS.Scanner regex machinery,
// EJS.Helpers, etc.) is no longer needed and has been removed.

var EJS = EJS || {};
EJS.Scanner = EJS.Scanner || {};

EJS.Scanner.to_text = function(input) {
    if (input == null || input === undefined) { return ''; }
    if (input instanceof Date) { return input.toDateString(); }
    if (input.toString) { return input.toString(); }
    return '';
};
