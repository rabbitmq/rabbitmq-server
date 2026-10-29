// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

export const WIDTH = 960;
export const HEIGHT = 400;
export const GROUND = 310;
export const SPEED = 300;
export const GRAVITY = 1600;
export const JUMP_SPEED = 570;
export const STEP = 1 / 120;
export const MIN_GAP = SPEED * (2 * JUMP_SPEED / GRAVITY + 0.5) + 104;

export function createSession() {
    return { state: 'loading', run: null, result: null, disposed: false };
}

export function createRun(mask = [{ x: 0, y: 0, width: 64, height: 75 }], height = 75) {
    return {
        state: 'active', distance: 0, hops: 0, time: 0, remainder: 0,
        character: { x: 80, y: GROUND - height, width: 64, height, vy: 0, grounded: true, mask },
        obstacles: [], nextSpawn: 180, nextId: 0, seed: 17, held: new Set(), result: null
    };
}

export function press(run, key) {
    if (run.state !== 'active' || run.held.has(key)) return false;
    run.held.add(key);
    if (!run.character.grounded) return false;
    run.character.vy = -JUMP_SPEED;
    run.character.grounded = false;
    return true;
}

export function release(run, key) {
    run.held.delete(key);
}

export function pause(run) {
    if (run.state !== 'active') return;
    run.state = 'paused';
    run.held.clear();
    run.remainder = 0;
}

export function resume(run) {
    if (run.state !== 'paused') return;
    run.remainder = 0;
    run.state = 'active';
}

export function exit(run) {
    run.state = 'exited';
    run.held.clear();
    run.remainder = 0;
}

export function alphaMask(pixels, width, height) {
    let left = width, top = height, right = 0, bottom = 0;
    const rows = [];
    for (let y = 0; y < height; y++) {
        const spans = [];
        for (let x = 0; x < width;) {
            if (pixels[(y * width + x) * 4 + 3] === 0) { x++; continue; }
            const start = x;
            while (x < width && pixels[(y * width + x) * 4 + 3] !== 0) x++;
            spans.push({ x: start, y, width: x - start, height: 1 });
            left = Math.min(left, start);
            right = Math.max(right, x);
            top = Math.min(top, y);
            bottom = y + 1;
        }
        rows.push(spans);
    }
    if (right <= left) throw new Error('Empty character image');
    const rectangles = [];
    let previous = new Map();
    for (const row of rows) {
        const current = new Map();
        for (const span of row) {
            const key = `${span.x}:${span.width}`;
            const prior = previous.get(key);
            if (prior) prior.height++;
            else rectangles.push(span);
            current.set(key, prior || span);
        }
        previous = current;
    }
    const scale = 64 / (right - left);
    return {
        bounds: { x: left, y: top, width: right - left, height: bottom - top },
        height: (bottom - top) * scale,
        rectangles: rectangles.map(r => ({ x: (r.x - left) * scale, y: (r.y - top) * scale,
            width: r.width * scale, height: r.height * scale }))
    };
}

function interval(position, movement, min, max) {
    if (movement === 0) return position >= min && position <= max ? [-Infinity, Infinity] : null;
    const a = (min - position) / movement;
    const b = (max - position) / movement;
    return [Math.min(a, b), Math.max(a, b)];
}

export function contact(character, obstacle, travel, dy) {
    let first = Infinity;
    for (const r of character.mask) {
        const x = interval(character.x + r.x - obstacle.x, travel, -r.width, obstacle.width);
        const y = interval(character.y + r.y - obstacle.y, dy, -r.height, obstacle.height);
        if (!x || !y) continue;
        const enter = Math.max(0, x[0], y[0]);
        if (enter <= Math.min(1, x[1], y[1])) first = Math.min(first, enter);
    }
    return first;
}

function step(run) {
    const c = run.character;
    if (run.distance >= run.nextSpawn) {
        run.seed = (Math.imul(run.seed, 1664525) + 1013904223) >>> 0;
        const height = 30 + run.seed % 15;
        run.obstacles.push({ id: run.nextId++, x: WIDTH, previousX: WIDTH, y: GROUND - height,
            width: 36, height, motif: run.seed % 2 ? 'message' : 'queue', counted: false });
        run.nextSpawn += MIN_GAP + run.seed % 100;
    }
    const travel = SPEED * STEP;
    const nextY = Math.min(GROUND - c.height, c.y + c.vy * STEP + GRAVITY * STEP * STEP / 2);
    const dy = nextY - c.y;
    let hit = Infinity;
    for (const obstacle of run.obstacles) hit = Math.min(hit, contact(c, obstacle, travel, dy));
    const fraction = Math.min(1, hit);
    run.distance += travel * fraction;
    run.time += STEP * fraction;
    c.y += dy * fraction;
    c.vy += GRAVITY * STEP * fraction;
    if (c.y >= GROUND - c.height) {
        c.y = GROUND - c.height;
        c.vy = 0;
        c.grounded = true;
    }
    for (const obstacle of run.obstacles) {
        obstacle.previousX = obstacle.x;
        obstacle.x -= travel * fraction;
    }
    if (hit <= 1) {
        run.state = 'finished';
        run.result = Object.freeze({ distance: Math.floor(run.distance), hops: run.hops });
        run.held.clear();
        return;
    }
    for (const obstacle of run.obstacles) {
        if (!obstacle.counted && obstacle.x + obstacle.width < c.x) {
            obstacle.counted = true;
            run.hops++;
        }
    }
    run.obstacles = run.obstacles.filter(o => o.x + o.width >= 0);
}

export function advance(run, milliseconds) {
    if (run.state !== 'active' || !Number.isFinite(milliseconds) || milliseconds <= 0) return;
    run.remainder += Math.min(milliseconds, 100) / 1000;
    while (run.remainder + 1e-10 >= STEP && run.state === 'active') {
        run.remainder -= STEP;
        step(run);
    }
}
