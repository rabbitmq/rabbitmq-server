%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(dummy_queue_decorator).

-behaviour(rabbit_queue_decorator).

-export([set_target/2, clear_target/0]).
-export([startup/1, shutdown/1, policy_changed/2, active_for/1,
         consumer_state_changed/3]).

set_target(QName, Pid) ->
    persistent_term:put(?MODULE, {QName, Pid}).

clear_target() ->
    _ = persistent_term:erase(?MODULE),
    ok.

startup(_Q) ->
    ok.

shutdown(_Q) ->
    ok.

policy_changed(_Q1, _Q2) ->
    ok.

active_for(Q) ->
    case persistent_term:get(?MODULE, undefined) of
        {QName, _Pid} ->
            amqqueue:get_name(Q) =:= QName;
        undefined ->
            false
    end.

consumer_state_changed(Q, MaxActivePriority, IsEmpty) ->
    case persistent_term:get(?MODULE, undefined) of
        {_QName, Pid} ->
            Pid ! {?MODULE, amqqueue:get_name(Q), MaxActivePriority, IsEmpty},
            ok;
        undefined ->
            ok
    end.
