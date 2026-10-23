// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

const TERMINOLOGY_PREF = 'terminology';
const TERMINOLOGY_MODERN = 'modern';
const TERMINOLOGY_LEGACY = 'legacy';

// Maps a legacy label to its modern one.
const MODERN_TERMS = {
    'Ready':                    'Available',
    'Unacked':                  'Acquired',
    'Unacknowledged':           'Acquired',
    'ack':                      'accept',
    'deliver / get':            'deliver',
    'Consumer ack':             'Consumer accept',
    'Publisher confirm':        'Accepted by queue',
    'confirm':                  'accepted by queue',
    'Unconfirmed':              'Unsettled',
    'Deliver (manual ack)':     'Deliver (unsettled)',
    'Deliver (auto ack)':       'Deliver (pre-settled)',
    'Messages unacknowledged':  'Messages acquired',
    'Messages unconfirmed':     'Messages unsettled',
    'Prefetch count':           'Link credit',
    'Ack required':             'Settlement required'
};

// Labels of `basic.get` rates, which have no AMQP 1.0 equivalent.
const MODERN_HIDDEN_TERMS = [
    'Get (manual ack)',
    'Get (auto ack)',
    'Get (empty)'
];

const MODERN_HELP_EDITS = {
    'queued-messages': [
        ['<dt>Ready</dt>', '<dt>Available</dt>'],
        ['<dt>Unacknowledged</dt>', '<dt>Acquired</dt>'],
        ['for which the server is waiting for acknowledgement',
         'that have been delivered and are waiting to be settled']
    ],
    'message-rates': [
        [/<dt>Get \([^<]*<\/dt>\s*<dd>[^<]*<\/dd>\s*/g, ''],
        ['<dt>Publisher confirm</dt>', '<dt>Accepted by queue</dt>'],
        ['Rate at which the server is confirming publishes.',
         'Rate at which publishes are accepted by queues.'],
        ['<dt>Deliver (manual ack)</dt>', '<dt>Deliver (unsettled)</dt>'],
        ['that use manual acknowledgements', 'that settle deliveries themselves'],
        ['<dt>Deliver (auto ack)</dt>', '<dt>Deliver (pre-settled)</dt>'],
        ['that use automatic acknowledgements', 'that receive deliveries pre-settled'],
        ['<dt>Consumer ack</dt>', '<dt>Consumer accept</dt>'],
        ['are being acknowledged by consumers', 'are being accepted by consumers']
    ],
    'queue-max-length': [['(ready)', '(available)']],
    'queue-max-length-bytes': [['for ready messages', 'for available messages']],
    'amqp-unconfirmed-messages': [['have not been confirmed by all queues',
                                   'have not been accepted by all queues']]
};

function terminology_mode() {
    var pref = get_pref(TERMINOLOGY_PREF);
    if (pref === TERMINOLOGY_MODERN || pref === TERMINOLOGY_LEGACY) {
        return pref;
    }
    var configured = window.app_settings && window.app_settings.ui_terminology;
    return configured === TERMINOLOGY_LEGACY ? TERMINOLOGY_LEGACY : TERMINOLOGY_MODERN;
}

function is_modern_terminology() {
    return terminology_mode() === TERMINOLOGY_MODERN;
}

function t(label) {
    if (is_modern_terminology() && Object.hasOwn(MODERN_TERMS, label)) {
        return MODERN_TERMS[label];
    }
    return label;
}

// Translates the labels of `[label, key]` items and drops the hidden ones.
function t_items(items) {
    if (!is_modern_terminology()) {
        return items;
    }
    var res = [];
    for (var i = 0; i < items.length; i++) {
        if (MODERN_HIDDEN_TERMS.indexOf(items[i][0]) === -1) {
            res.push([t(items[i][0])].concat(items[i].slice(1)));
        }
    }
    return res;
}

function help_text(id) {
    var text = HELP[id];
    if (!is_modern_terminology() || typeof text !== 'string'
        || !Object.hasOwn(MODERN_HELP_EDITS, id)) {
        return text;
    }
    var edits = MODERN_HELP_EDITS[id];
    for (var i = 0; i < edits.length; i++) {
        text = text.replace(edits[i][0], edits[i][1]);
    }
    return text;
}

// Channels are an AMQP 0-9-1 concept. In modern mode, channel views
// are shown once a channel has been seen.
var channels_seen = false;
var channels_check_in_flight = false;

function channels_visible() {
    return !is_modern_terminology() || channels_seen;
}

function check_channels_present() {
    if (channels_visible() || channels_check_in_flight || disable_stats) {
        return;
    }
    channels_check_in_flight = true;
    var req = xmlHttpRequest();
    req.open('GET', 'api/channels?page=1&page_size=1&disable_stats=true');
    req.setRequestHeader('authorization', authorization_header());
    req.onreadystatechange = function() {
        if (req.readyState !== XMLHttpRequest.DONE) {
            return;
        }
        channels_check_in_flight = false;
        if (req.status === 200 && JSON.parse(req.responseText).total_count > 0) {
            channels_seen = true;
            update_navigation();
        }
    };
    req.send(null);
}

function toggle_terminology() {
    store_pref(TERMINOLOGY_PREF,
               is_modern_terminology() ? TERMINOLOGY_LEGACY : TERMINOLOGY_MODERN);
    full_refresh();
}
