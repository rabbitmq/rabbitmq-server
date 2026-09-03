import { register } from 'node:module';
import { pathToFileURL } from 'node:url';
import path from 'node:path';

const mgmtJsDir = pathToFileURL(path.resolve(import.meta.dirname, '../priv/www/js')).href + '/';

export async function resolve(specifier, context, nextResolve) {
  try {
    return await nextResolve(specifier, context);
  } catch (err) {
    if (err.code === 'ERR_MODULE_NOT_FOUND' && context.parentURL && (specifier.startsWith('./') || specifier.startsWith('../'))) {
      const fallback = new URL(specifier.replace(/^\.\//, ''), mgmtJsDir).href;
      return await nextResolve(fallback, context);
    }
    throw err;
  }
}

register(import.meta.url);
