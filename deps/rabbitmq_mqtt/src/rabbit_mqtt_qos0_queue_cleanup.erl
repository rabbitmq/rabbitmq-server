%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_qos0_queue_cleanup).

-behaviour(gen_server).

-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("kernel/include/logger.hrl").

-export([start_link/0, retry_delete/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pending = [] :: [{key(), {amqqueue:amqqueue(), rabbit_types:username()}}],
                fence = undefined :: undefined | pid()}).

-type key() :: {rabbit_amqqueue:name(), pid() | none}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec retry_delete(amqqueue:amqqueue(), rabbit_types:username()) -> ok.
retry_delete(Q, Username) ->
    gen_server:cast(?MODULE, {retry_delete, Q, Username}).

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(Request, From, State) ->
    {stop, {unexpected_call, Request, From}, State}.

handle_cast({retry_delete, Q, Username},
            State0 = #state{pending = Pending0}) ->
    Key = {amqqueue:get_name(Q), amqqueue:get_exclusive_owner(Q)},
    Pending = [{Key, {Q, Username}} | Pending0],
    {noreply, maybe_wait_for_khepri(State0#state{pending = Pending})};
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(retry_delete, State0 = #state{pending = Pending0}) ->
    case Pending0 of
        [] ->
            {noreply, State0};
        [{Key, {Q, Username}} | Rest] ->
            case rabbit_queue_type:delete(Q, false, false, Username) of
                {error, timeout} ->
                    {noreply, maybe_wait_for_khepri(State0)};
                _ ->
                    ?LOG_INFO("Deleting stale MQTT QoS0 queue metadata: ~0p", [Key]),
                    self() ! retry_delete,
                    {noreply, State0#state{pending = Rest}}
            end
    end;
handle_info({'EXIT', Pid, Reason}, State0 = #state{fence = Pid}) ->
    State1 = State0#state{fence = undefined},
    case Reason of
        normal ->
            self() ! retry_delete,
            {noreply, State1};
        _ ->
            {noreply, maybe_wait_for_khepri(State1)}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_wait_for_khepri(State = #state{pending = [_ | _], fence = undefined}) ->
    Pid = spawn_link(fun wait_for_khepri/0),
    State#state{fence = Pid};
maybe_wait_for_khepri(State) ->
    State.

%% rabbit_khepri:fence/1 can return immediately with an error (e.g. `noproc`)
%% instead of blocking when the local Khepri store isn't up yet, so retry
%% with a fixed delay instead of busy-looping.
wait_for_khepri() ->
    case rabbit_khepri:fence(infinity) of
        ok ->
            ok;
        {error, _} ->
            timer:sleep(1000),
            wait_for_khepri()
    end.
