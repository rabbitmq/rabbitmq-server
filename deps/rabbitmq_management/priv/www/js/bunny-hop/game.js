// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

import { createSession, createRun, advance, press, release, pause, resume, exit,
    alphaMask, WIDTH, HEIGHT, GROUND } from './model.js';

export function mount({ isDisposed }) {
    const session = createSession();
    const course = document.getElementById('course');
    const panel = document.getElementById('panel');
    const status = document.getElementById('status');
    const message = document.getElementById('message');
    const action = document.getElementById('action');
    const distance = document.getElementById('distance');
    const hops = document.getElementById('hops');
    const events = new AbortController();
    let app, pixi, texture, logo, world, track, mask, observer, compatibilityScript;
    let initialized = false;
    let assetLoaded = false;
    let firstFrame = true;
    const obstacles = new Map();
    const logoUrl = new URL('../../img/bunny-hop/rabbitmq-logo.png', import.meta.url).href;

    function disposed() { return session.disposed || isDisposed(); }

    function destroyRenderer() {
        if (initialized) {
            app.destroy({ removeView: true, releaseGlobalResources: true }, { children: true });
            initialized = false;
        }
        if (texture) { texture.destroy(); texture = null; }
        if (assetLoaded) {
            assetLoaded = false;
            pixi.Assets.unload(logoUrl).catch(() => {});
        }
    }

    function dispose() {
        if (session.disposed) return;
        session.disposed = true;
        session.state = 'exited';
        if (session.run) exit(session.run);
        events.abort();
        observer?.disconnect();
        compatibilityScript?.remove();
        destroyRenderer();
    }

    function setState(state) {
        session.state = state;
        document.body.dataset.state = state;
        panel.hidden = state === 'active';
        action.hidden = !['ready', 'paused', 'finished'].includes(state);
        const labels = {
            ready: ['Ready to hop?', 'Jump over messages and queues. Every obstacle cleared is one hop.', 'Start running'],
            paused: ['Taking a breather', 'Your run is paused. Resume when you’re ready.', 'Resume'],
            finished: ['That’s a wrap', `You ran ${session.run?.result?.distance ?? 0} distance units and cleared ${session.run?.result?.hops ?? 0} obstacles.`, 'Play again'],
            error: ['The course could not load', 'Please reload to try again, or press q to return to management.', '']
        };
        if (labels[state]) {
            [status.textContent, message.textContent, action.textContent] = labels[state];
        }
        if (state !== 'active' && initialized) app.stop();
        if (['paused', 'finished'].includes(state)) action.focus({ preventScroll: true });
    }

    function fail() {
        if (disposed()) return;
        if (session.run) pause(session.run);
        observer?.disconnect();
        compatibilityScript?.remove();
        destroyRenderer();
        setState('error');
    }

    function obstacleGraphic(o) {
        const g = new pixi.Graphics().rect(0, 0, o.width, o.height).fill(0x364b44);
        if (o.motif === 'message') {
            g.moveTo(4, 5).lineTo(o.width / 2, o.height / 2).lineTo(o.width - 4, 5)
                .stroke({ color: 0xe9eddf, width: 2 });
        } else {
            for (let y = 8; y < o.height; y += 9) {
                g.moveTo(7, y).lineTo(o.width - 7, y).stroke({ color: 0xe9eddf, width: 2 });
            }
        }
        world.addChild(g);
        return g;
    }

    function draw() {
        if (!initialized || disposed()) return;
        const run = session.run;
        logo.position.set(run.character.x, run.character.y);
        for (const [id, g] of obstacles) {
            if (!run.obstacles.some(o => o.id === id)) {
                world.removeChild(g);
                g.destroy();
                obstacles.delete(id);
            }
        }
        for (const o of run.obstacles) {
            if (!obstacles.has(o.id)) obstacles.set(o.id, obstacleGraphic(o));
            obstacles.get(o.id).position.set(o.x, o.y);
        }
        track.x = -(run.distance % 40);
        const nextDistance = String(Math.floor(run.distance));
        const nextHops = String(run.hops);
        if (distance.textContent !== nextDistance) distance.textContent = nextDistance;
        if (hops.textContent !== nextHops) hops.textContent = nextHops;
    }

    function resize() {
        if (!initialized || disposed()) return;
        const width = course.clientWidth;
        const height = course.clientHeight;
        app.renderer.resize(width, height);
        const scale = Math.min(width / WIDTH, height / HEIGHT);
        world.scale.set(scale);
        world.position.set((width - WIDTH * scale) / 2, (height - HEIGHT * scale) / 2);
        app.render();
    }

    function start() {
        if (disposed() || !initialized) return;
        if (session.state === 'paused') {
            if (document.hidden || !document.hasFocus()) return;
            resume(session.run);
        } else if (session.state === 'ready' || session.state === 'finished') {
            for (const g of obstacles.values()) { world.removeChild(g); g.destroy(); }
            obstacles.clear();
            session.run = createRun(mask.rectangles, mask.height);
        } else return;
        firstFrame = true;
        setState('active');
        course.focus({ preventScroll: true });
        draw();
        app.start();
    }

    function stop() {
        if (session.state !== 'active') return;
        pause(session.run);
        setState('paused');
        draw();
        app.render();
    }

    action.addEventListener('click', start, { signal: events.signal });
    document.addEventListener('keydown', event => {
        if (!['Space', 'ArrowUp'].includes(event.code) || event.ctrlKey || event.metaKey || event.altKey) return;
        if (event.target.closest('button, a, input, select, textarea')) return;
        if (session.state !== 'active') return;
        event.preventDefault();
        if (!event.repeat) press(session.run, event.code);
    }, { signal: events.signal });
    document.addEventListener('keyup', event => {
        if (session.run) release(session.run, event.code);
    }, { signal: events.signal });
    course.addEventListener('pointerdown', event => {
        if (session.state !== 'active' || !event.isPrimary || event.button !== 0) return;
        event.preventDefault();
        press(session.run, 'pointer');
    }, { signal: events.signal });
    for (const type of ['pointerup', 'pointercancel']) {
        window.addEventListener(type, () => {
            if (session.run) release(session.run, 'pointer');
        }, { signal: events.signal });
    }
    document.addEventListener('visibilitychange', () => { if (document.hidden) stop(); }, { signal: events.signal });
    window.addEventListener('blur', stop, { signal: events.signal });
    window.addEventListener('pagehide', dispose, { signal: events.signal });

    async function initialize() {
        pixi = await import('./vendor/pixi-8.22.0.min.mjs');
        if (disposed()) return;
        window.PIXI = { ...pixi };
        await new Promise((resolve, reject) => {
            compatibilityScript = document.createElement('script');
            compatibilityScript.src = new URL('./vendor/unsafe-eval-8.22.0.min.js', import.meta.url).href;
            compatibilityScript.onload = resolve;
            compatibilityScript.onerror = reject;
            document.head.appendChild(compatibilityScript);
        });
        if (disposed()) return;
        app = new pixi.Application();
        await app.init({ width: WIDTH, height: HEIGHT, background: 0xf4f1e8,
            preference: 'webgl', autoStart: false, sharedTicker: false,
            autoDensity: true, resolution: Math.min(window.devicePixelRatio || 1, 2), antialias: true });
        initialized = true;
        if (disposed()) { destroyRenderer(); return; }
        pixi.loadTextures.config.preferWorkers = false;
        const loaded = await pixi.Assets.load(logoUrl);
        assetLoaded = true;
        if (disposed()) { destroyRenderer(); return; }
        const image = loaded.source.resource;
        const buffer = document.createElement('canvas');
        buffer.width = image.width;
        buffer.height = image.height;
        const context = buffer.getContext('2d', { willReadFrequently: true });
        context.drawImage(image, 0, 0);
        mask = alphaMask(context.getImageData(0, 0, buffer.width, buffer.height).data, buffer.width, buffer.height);
        texture = new pixi.Texture({ source: loaded.source, frame: new pixi.Rectangle(
            mask.bounds.x, mask.bounds.y, mask.bounds.width, mask.bounds.height) });
        session.run = createRun(mask.rectangles, mask.height);
        world = new pixi.Container();
        world.eventMode = 'none';
        app.stage.addChild(world);
        const background = new pixi.Graphics();
        background.rect(0, 0, WIDTH, HEIGHT).fill(0xf4f1e8);
        background.circle(785, 95, 40).fill(0xe8e3d6);
        background.moveTo(0, GROUND).lineTo(WIDTH, GROUND).stroke({ color: 0xb4bdac, width: 2 });
        world.addChild(background);
        track = new pixi.Graphics();
        for (let x = 0; x < WIDTH + 40; x += 40) track.rect(x, GROUND + 14, 12, 2).fill(0xc5cbbd);
        world.addChild(track);
        logo = new pixi.Sprite(texture);
        logo.width = session.run.character.width;
        logo.height = mask.height;
        world.addChild(logo);
        app.canvas.setAttribute('aria-hidden', 'true');
        course.tabIndex = -1;
        course.appendChild(app.canvas);
        app.ticker.maxFPS = 60;
        app.ticker.add(ticker => {
            try {
                if (session.state !== 'active') return;
                if (firstFrame) firstFrame = false;
                else advance(session.run, ticker.deltaMS);
                draw();
                if (session.run.state === 'finished') {
                    session.result = session.run.result;
                    setState('finished');
                }
            } catch { fail(); }
        });
        app.canvas.addEventListener('webglcontextlost', fail, { signal: events.signal });
        observer = new ResizeObserver(resize);
        observer.observe(course);
        setState('ready');
        draw();
        resize();
    }

    initialize().catch(error => {
        console.error('Bunny Hop failed to initialize', error);
        if (disposed()) destroyRenderer();
        else fail();
    });
    return dispose;
}
