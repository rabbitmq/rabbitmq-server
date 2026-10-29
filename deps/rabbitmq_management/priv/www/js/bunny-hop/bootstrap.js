// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

(function () {
    const exitLink = document.getElementById('exit');
    let disposed = false;
    let disposeGame = null;
    const events = new AbortController();

    function dispose() {
        if (disposed) return;
        disposed = true;
        events.abort();
        if (disposeGame) disposeGame();
    }

    function exit(event) {
        event.preventDefault();
        dispose();
        location.assign(exitLink.href);
    }

    exitLink.addEventListener('click', exit, { signal: events.signal });
    document.addEventListener('keydown', function (event) {
        if (event.key.toLowerCase() === 'q' && !event.repeat &&
            !event.ctrlKey && !event.metaKey && !event.altKey) exit(event);
    }, { signal: events.signal });
    window.addEventListener('pagehide', dispose, { signal: events.signal });

    import('./game.js').then(async function (game) {
        if (disposed) return;
        disposeGame = await game.mount({ isDisposed: () => disposed });
        if (disposed) disposeGame();
    }).catch(function () {
        if (disposed) return;
        if (disposeGame) disposeGame();
        document.body.dataset.state = 'error';
        document.getElementById('status').textContent = 'The course could not load';
        document.getElementById('message').textContent = 'Please reload to try again, or press q to return to management.';
        document.getElementById('panel').hidden = false;
        document.getElementById('action').hidden = true;
    });
}());
