%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue_common).

-export([notify_sent/2, notify_sent_queue_down/1, delete_exclusive/2]).

-define(MORE_CONSUMER_CREDIT_AFTER, 50).

-spec notify_sent(pid(), pid()) -> 'ok'.

notify_sent(QPid, ChPid) ->
    Key = {consumer_credit_to, QPid},
    put(Key, case get(Key) of
                 1         -> gen_server2:cast(
                                QPid, {notify_sent, ChPid,
                                       ?MORE_CONSUMER_CREDIT_AFTER}),
                              ?MORE_CONSUMER_CREDIT_AFTER;
                 undefined -> erlang:monitor(process, QPid),
                              ?MORE_CONSUMER_CREDIT_AFTER - 1;
                 C         -> C - 1
             end),
    ok.

-spec notify_sent_queue_down(pid()) -> 'ok'.

notify_sent_queue_down(QPid) ->
    erase({consumer_credit_to, QPid}),
    ok.

-spec delete_exclusive([pid()], pid()) -> 'ok'.

delete_exclusive(QPids, ConnId) ->
    [gen_server2:cast(QPid, {delete_exclusive, ConnId}) || QPid <- QPids],
    ok.
