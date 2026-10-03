// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

const SESSION_ID = 'session_id';

function get_session_id()     { return get_local_pref(SESSION_ID); }
function store_session_id(id) { store_local_pref(SESSION_ID, id); }

var _session_heartbeat_timer = null;

// Reports the HTTP status of failed requests, which `sync_req` and `with_req` do
// not, and never shows a popup or starts a logout. Synchronous unless `on_done` is given.
function session_req(method, session_id, on_done) {
    var path = 'api/session' + (session_id ? '/' + encodeURIComponent(session_id) : '');
    var req = xmlHttpRequest();
    var result = function() {
        return {http_status: req.status, responseText: req.responseText};
    };
    req.open(method, path, on_done !== undefined);
    req.setRequestHeader('content-type', 'application/json');
    var authorization = authorization_header();
    if (authorization) {
        req.setRequestHeader('authorization', authorization);
    }
    if (on_done !== undefined) {
        req.onreadystatechange = function() {
            if (req.readyState === 4) {
                on_done(result());
            }
        };
    }
    try {
        req.send('{}');
    } catch (e) {
        var failed = {http_status: 0, responseText: ''};
        if (on_done === undefined) {
            return failed;
        }
        on_done(failed);
        return undefined;
    }
    return on_done === undefined ? result() : undefined;
}

function is_success(res) {
    return res.http_status >= 200 && res.http_status < 300;
}

function is_session_rejected(res) {
    return res.http_status === 401 || res.http_status === 403 || res.http_status === 404;
}

function stop_session_heartbeat() {
    if (_session_heartbeat_timer) {
        clearInterval(_session_heartbeat_timer);
        _session_heartbeat_timer = null;
    }
}

function clear_session() {
    stop_session_heartbeat();
    var id = get_session_id();
    clear_local_pref(SESSION_ID);
    if (id) {
        session_req('DELETE', id);
    }
}

function check_session() {
    var existing_id = get_session_id();
    if (existing_id) {
        var res = session_req('PUT', existing_id);
        if (is_success(res)) {
            return 'ok';
        } else if (is_session_rejected(res)) {
            clear_local_pref(SESSION_ID);
        } else {
            return 'error';
        }
    }

    var created = session_req('POST');
    if (created.http_status === 403) {
        return 'limit_reached';
    }
    if (created.http_status === 201) {
        var data = JSON.parse(created.responseText);
        if (data.session_id) {
            store_session_id(data.session_id);
            return 'ok';
        }
    }
    return 'error';
}

function end_rejected_session() {
    stop_session_heartbeat();
    clear_auth();
    location.reload();
}

function _send_heartbeat(session_id) {
    session_req('PUT', session_id, function(res) {
        if (is_session_rejected(res)) {
            end_rejected_session();
        }
    });
}

function start_session_heartbeat(session_id, interval_sec) {
    stop_session_heartbeat();

    var interval_ms = (typeof interval_sec === 'number' && interval_sec > 0) ? interval_sec * 1000 : 30000;
    _session_heartbeat_timer = setInterval(function() { _send_heartbeat(session_id); }, interval_ms);
}

window.get_session_id = get_session_id;
window.check_session = check_session;
window.clear_session = clear_session;
window.stop_session_heartbeat = stop_session_heartbeat;
window.start_session_heartbeat = start_session_heartbeat;

if (typeof registerLoginProcessor === 'function') {
    registerLoginProcessor('session', function(context) {
        var sessions = context.settings && context.settings.sessions;
        if (!sessions || sessions.enabled !== true) {
            return { ok: true };
        }
        var status = check_session();
        if (status === 'ok') {
            start_session_heartbeat(get_session_id(), sessions.heartbeat_interval);
            return { ok: true };
        } else if (status === 'limit_reached') {
            return { ok: false, error: 'Concurrent session limit reached' };
        } else {
            return { ok: false, error: 'Failed to establish session with server' };
        }
    });
}

if (typeof registerInitFailedProcessor === 'function') {
    registerInitFailedProcessor('session', function(error) {
        stop_session_heartbeat();
    });
}

if (typeof registerLogoutProcessor === 'function') {
    registerLogoutProcessor('session', function(context) {
        if (context && context.involuntary) {
            stop_session_heartbeat();
        } else {
            clear_session();
        }
    });
}

