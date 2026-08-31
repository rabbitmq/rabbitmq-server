const SESSION_ID = 'session_id';

function get_session_id()     { return get_local_pref(SESSION_ID); }
function store_session_id(id) { store_local_pref(SESSION_ID, id); }

var _session_heartbeat_timer = null;

function clear_session() {
    if (_session_heartbeat_timer) {
        clearInterval(_session_heartbeat_timer);
        _session_heartbeat_timer = null;
    }
    var id = get_session_id();
    if (id) {
        sync_req('DELETE', {}, '/session/' + encodeURIComponent(id));
    }
    clear_local_pref(SESSION_ID);
}

function check_session() {
    var existing_id = get_session_id();
    if (existing_id) {
        var res = sync_req('PUT', {}, '/session/' + encodeURIComponent(existing_id));
        if (res && (res.http_status === 200 || res.http_status === 204)) {
            return 'ok';
        } else {
            clear_local_pref(SESSION_ID);
        }
    }

    var resPost = sync_req('POST', {}, '/session');
    if (resPost) {
        if (resPost.http_status === 201) {
            var data = JSON.parse(resPost.responseText);
            if (data.session_id) {
                store_session_id(data.session_id);
            }
            return 'ok';
        } else if (resPost.http_status === 404) {
            // Feature disabled
            return 'ok';
        } else if (resPost.http_status === 403) {
            // Limit reached
            return 'limit_reached';
        }
    }
    return 'error';
}

function _send_heartbeat(session_id, is_initial) {
    if (is_initial) {
        var res = sync_req('PUT', {}, '/session/' + encodeURIComponent(session_id));
        if (res && (res.http_status === 200 || res.http_status === 204)) {
            return true;
        } else {
            clear_session();
            clear_auth();
            location.reload();
            return false;
        }
    } else {
        with_req('PUT', '/session/' + encodeURIComponent(session_id), "{}", function(req) {
            // success, do nothing
        }, function(req) {
            // custom on404fun (also handles 401/403 since we pass it to with_req)
            if (req.status === 401 || req.status === 403 || req.status === 404) {
                clear_session();
                clear_auth();
                location.reload();
            }
        });
    }
}

function start_session_heartbeat(session_id, interval_sec) {
    if (_session_heartbeat_timer) clearInterval(_session_heartbeat_timer);
    
    var interval_ms = (typeof interval_sec === 'number' && interval_sec > 0) ? interval_sec * 1000 : 30000;
    
    if (_send_heartbeat(session_id, true)) {
        _session_heartbeat_timer = setInterval(function() { _send_heartbeat(session_id, false); }, interval_ms);
        return true;
    }
    return false;
}

window.get_session_id = get_session_id;
window.check_session = check_session;
window.clear_session = clear_session;
window.start_session_heartbeat = start_session_heartbeat;

if (typeof registerLoginProcessor === 'function') {
    registerLoginProcessor('session', function(context) {
        if (typeof sessions_enabled === 'function' && !sessions_enabled()) {
            return { ok: true };
        }
        var status = check_session();
        if (status === 'ok' || status === true) {
            var heartbeat_interval = (context.settings && context.settings.sessions)
                ? context.settings.sessions.heartbeat_interval
                : 30;
            start_session_heartbeat(get_session_id(), heartbeat_interval);
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
        clear_session();
    });
}

if (typeof registerLogoutProcessor === 'function') {
    registerLogoutProcessor('session', function() {
        clear_session();
    });
}

