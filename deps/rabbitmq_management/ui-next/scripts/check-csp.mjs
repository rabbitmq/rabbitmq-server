// Fails when the built HTML would violate the broker's default Content Security Policy,
// `script-src 'self'; object-src 'self'`: no inline scripts and no inline import maps.
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'

const dir = process.argv[2]
if (!dir) {
  console.error('usage: check-csp.mjs <build dir>')
  process.exit(2)
}

const problems = []
for (const file of readdirSync(dir).filter((f) => f.endsWith('.html'))) {
  const html = readFileSync(join(dir, file), 'utf8')
  for (const match of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
    const [, attrs, body] = match
    if (/type\s*=\s*["']?importmap/i.test(attrs)) problems.push(`${file}: inline import map`)
    else if (!/\bsrc\s*=/.test(attrs) || body.trim() !== '') problems.push(`${file}: inline script`)
  }
  if (/\son[a-z]+\s*=/i.test(html)) problems.push(`${file}: inline event handler attribute`)
}

if (problems.length > 0) {
  console.error(`CSP check failed:\n  ${problems.join('\n  ')}`)
  process.exit(1)
}
console.log('CSP check passed')
