import { readFile } from 'node:fs/promises';
let [_node, _script, s, p, r] = process.argv;
let source = await readFile(s, 'utf-8');
let pattern = await readFile(p, 'utf-8');
let replacement = await readFile(r, 'utf-8');
console.log(source.replace(pattern, replacement));
