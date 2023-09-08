% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue_control).

-export([await_new_pid/3, await_state/3, await_state/4]).

-define(DEFAULT_AWAIT_STATE_TIMEOUT,  30000).
-define(AWAIT_NEW_PID_DELAY_INTERVAL, 10).
-define(AWAIT_STATE_DELAY_INTERVAL,   100).
-define(AWAIT_STATE_DELAY_TIME_DELTA, 100).

-include_lib("rabbit_common/include/resource.hrl").

-spec await_new_pid(node(), rabbit_amqqueue:name(), pid()) -> pid().
await_new_pid(Node, QRes = #resource{kind = queue}, OldPid) ->
    case rabbit_amqqueue:pid_or_crashed(Node, QRes) of
        OldPid -> timer:sleep(?AWAIT_NEW_PID_DELAY_INTERVAL),
                  await_new_pid(Node, QRes, OldPid);
        New    -> New
    end.

-spec await_state(node(), rabbit_amqqueue:name() | binary(), atom()) -> 'ok'.
await_state(Node, QName, State) when is_binary(QName) ->
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    await_state(Node, QRes, State);
await_state(Node, QRes = #resource{kind = queue}, State) ->
    await_state(Node, QRes, State, ?DEFAULT_AWAIT_STATE_TIMEOUT).

-spec await_state(node(), rabbit_amqqueue:name() | binary(), atom(), integer()) -> 'ok'.
await_state(Node, QName, State, Time) when is_binary(QName) ->
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    await_state(Node, QRes, State, Time);
await_state(Node, QRes = #resource{kind = queue}, State, Time) ->
    case state(Node, QRes) of
        State ->
            ok;
        Other ->
            case Time of
                0 -> exit({timeout_awaiting_state, State, Other});
                _ -> timer:sleep(?AWAIT_STATE_DELAY_INTERVAL),
                     await_state(Node, QRes, State, Time - ?AWAIT_STATE_DELAY_TIME_DELTA)
            end
    end.

state(Node, QRes = #resource{virtual_host = VHost, kind = queue}) ->
    Infos = rpc:call(Node, rabbit_amqqueue, info_all, [VHost, [name, state]]),
    fetch_state(QRes, Infos).

fetch_state(_QRes, [])                                   -> undefined;
fetch_state(QRes,  [[{name, QRes}, {state, State}] | _]) -> State;
fetch_state(QRes,  [[{name, _}, {state, _State}] | Rem]) ->
    fetch_state(QRes, Rem).
