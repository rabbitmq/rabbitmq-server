// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WWW = path.join(__dirname, '../priv/www');

// `bootstrap.js` is served by `rabbit_mgmt_oauth_bootstrap`, and
// `management-ejs.js` is compiled from the templates at build time.
const GENERATED = ['js/oidc-oauth/bootstrap.js', 'js/management-ejs.js'];

function localReferences(html) {
    return [...html.matchAll(/<(?:script|link)\b[^>]*?\b(?:src|href)="([^"]+)"/g)]
        .map(match => match[1])
        .filter(ref => !/^([a-z]+:)?\/\//i.test(ref));
}

describe('HTML pages', () => {
    const pages = fs.readdirSync(WWW, { recursive: true })
        .filter(file => file.endsWith('.html'));

    for (const page of pages) {
        it(`${page} only loads files that exist`, () => {
            const html = fs.readFileSync(path.join(WWW, page), 'utf8');
            const missing = localReferences(html)
                .map(ref => path.relative(WWW, path.join(WWW, path.dirname(page), ref)))
                .filter(file => !GENERATED.includes(file) &&
                        !fs.existsSync(path.join(WWW, file)));
            assert.deepEqual(missing, []);
        });
    }
});
