// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import test from 'node:test';
import assert from 'node:assert/strict';
import { createRun, press, release, advance, pause, resume, exit, contact, alphaMask,
    GROUND, SPEED, MIN_GAP, JUMP_SPEED, GRAVITY } from '../priv/www/js/bunny-hop/model.js';

function emptyRun() {
    const run = createRun();
    run.nextSpawn = Infinity;
    return run;
}
function tick(run, seconds) {
    for (let i = 0; i < seconds * 120; i++) advance(run, 1000 / 120);
}
function obstacle(x, y = GROUND - 35) {
    return { id: 0, x, y, width: 36, height: 35, counted: false };
}

test('jump requires a fresh grounded press and lands exactly', () => {
    const r = emptyRun();
    assert.ok(press(r, 'Space'));
    assert.equal(press(r, 'Space'), false);
    release(r, 'Space');
    assert.equal(press(r, 'Space'), false);
    tick(r, 1);
    assert.equal(r.character.y, GROUND - r.character.height);
    assert.equal(r.character.vy, 0);
    assert.equal(press(r, 'Space'), false);
    release(r, 'Space');
    assert.ok(press(r, 'Space'));
    assert.equal(r.hops, 0);
});

test('twenty cleared obstacles count once each, empty jumps count zero', () => {
    const r = emptyRun();
    press(r, 'Space');
    tick(r, 1);
    assert.equal(r.hops, 0);
    for (let i = 0; i < 20; i++) {
        r.obstacles.push({ ...obstacle(60, 0), id: i });
        tick(r, 0.1);
        assert.equal(r.hops, i + 1);
    }
    tick(r, 1);
    assert.equal(r.hops, 20);
});

test('one jump may clear multiple obstacles', () => {
    const r = emptyRun();
    r.obstacles = [obstacle(190), { ...obstacle(230), id: 1 }];
    press(r, 'Space');
    tick(r, 0.65);
    assert.equal(r.state, 'active');
    assert.equal(r.hops, 2);
});

test('distance is cumulative travel independent of frame schedule', () => {
    for (const fps of [30, 60, 120]) {
        const r = emptyRun();
        for (let i = 0; i < fps * 2; i++) advance(r, 1000 / fps);
        assert.equal(r.distance, SPEED * 2);
    }
});

test('spawn spacing permits a complete jump and reaction time', () => {
    assert.ok(MIN_GAP >= SPEED * (2 * JUMP_SPEED / GRAVITY + 0.5) + 100);
    const r = createRun();
    r.character.mask = [];
    const arrivals = new Map();
    for (let i = 0; i < 6000; i++) {
        advance(r, 1000 / 120);
        for (const o of r.obstacles) if (!arrivals.has(o.id)) arrivals.set(o.id, r.distance);
    }
    const distances = [...arrivals.values()];
    assert.ok(distances.length > 20);
    for (let i = 1; i < distances.length; i++) assert.ok(distances[i] - distances[i - 1] >= MIN_GAP - 2.5);
});

test('swept contact catches an obstacle crossed between frames', () => {
    const c = { x: 10, y: 10, mask: [{ x: 0, y: 0, width: 2, height: 2 }] };
    assert.equal(contact(c, { x: 20, y: 10, width: 2, height: 2 }, 30, 0), 8 / 30);
    assert.equal(contact(c, { x: 12, y: 12, width: 2, height: 2 }, 0, 0), 0);
    assert.equal(contact(c, { x: 13, y: 13, width: 2, height: 2 }, 0, 0), Infinity);
});

test('alpha mask trims padding and preserves holes and ears', () => {
    const data = new Uint8ClampedArray(5 * 5 * 4);
    for (const [x, y] of [[1, 1], [3, 1], [1, 2], [3, 2], [1, 3], [2, 3], [3, 3]]) {
        data[(y * 5 + x) * 4 + 3] = 255;
    }
    const mask = alphaMask(data, 5, 5);
    assert.deepEqual(mask.bounds, { x: 1, y: 1, width: 3, height: 3 });
    const c = { x: 0, y: 0, mask: mask.rectangles };
    assert.equal(contact(c, { x: 30, y: 2, width: 2, height: 2 }, 0, 0), Infinity);
    assert.equal(contact(c, { x: 2, y: 2, width: 2, height: 2 }, 0, 0), 0);
    assert.throws(() => alphaMask(new Uint8Array(4), 1, 1));
});

test('twenty contacts end runs before scoring and freeze immutable results', () => {
    for (let i = 0; i < 20; i++) {
        const r = emptyRun();
        r.obstacles = [obstacle(145 + i)];
        advance(r, 10000);
        assert.equal(r.state, 'finished');
        assert.equal(r.hops, 0);
        assert.equal(r.result.distance, Math.floor(r.distance));
        assert.ok(Object.isFrozen(r.result));
        const snapshot = JSON.stringify(r);
        tick(r, 5);
        assert.equal(press(r, 'Space'), false);
        assert.equal(JSON.stringify(r), snapshot);
    }
});

test('long frames are bounded and cannot skip a nearby obstacle', () => {
    const r = emptyRun();
    advance(r, 30000);
    assert.ok(r.distance <= 30);
    r.obstacles = [obstacle(150)];
    advance(r, 30000);
    assert.equal(r.state, 'finished');
});

test('replay starts clean ten times', () => {
    for (let i = 0; i < 10; i++) {
        const r = createRun();
        assert.equal(r.distance, 0);
        assert.equal(r.hops, 0);
        assert.equal(r.character.grounded, true);
        assert.equal(r.obstacles.length, 0);
        assert.equal(r.held.size, 0);
        assert.equal(r.result, null);
        assert.equal(r.remainder, 0);
    }
});

test('pause preserves the run for thirty seconds and requires resume', () => {
    const r = emptyRun();
    press(r, 'Space');
    tick(r, 0.1);
    pause(r);
    assert.equal(r.held.size, 0);
    const snapshot = JSON.stringify(r);
    tick(r, 30);
    assert.equal(press(r, 'Space'), false);
    assert.equal(JSON.stringify(r), snapshot);
    resume(r);
    advance(r, 1000 / 60);
    assert.ok(r.distance > JSON.parse(snapshot).distance);
});

test('exit is idempotent and reopening starts fresh', () => {
    const r = emptyRun();
    press(r, 'Space');
    exit(r);
    exit(r);
    assert.equal(r.state, 'exited');
    assert.equal(r.held.size, 0);
    advance(r, 1000);
    assert.equal(r.distance, 0);
    assert.equal(createRun().distance, 0);
});

test('generated courses remain jumpable across forty successive obstacles', () => {
    const run = createRun();
    for (let i = 0; i < 24000 && run.hops < 40; i++) {
        const next = run.obstacles.find(o => !o.counted && o.x + o.width >= run.character.x);
        if (next && next.x <= 210 && run.character.grounded) {
            press(run, 'Space');
            release(run, 'Space');
        }
        advance(run, 1000 / 120);
        assert.equal(run.state, 'active');
    }
    assert.equal(run.hops, 40);
});
