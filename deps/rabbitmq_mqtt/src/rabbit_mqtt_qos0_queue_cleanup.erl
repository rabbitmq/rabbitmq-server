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

-record(state, {pending = #{} :: #{key() => {amqqueue:amqqueue(), rabbit_types:username()}},
                fence = undefined :: undefined | {pid(), reference()}}).

-type key() :: {rabbit_amqqueue:name(), pid() | none}.

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec retry_delete(amqqueue:amqqueue(), rabbit_types:username()) -> ok.
retry_delete(Q, Username) ->
    gen_server:cast(?MODULE, {retry_delete, Q, Username}).

init([]) ->
    {ok, #state{}}.

handle_call(Request, From, State) ->
    {stop, {unexpected_call, Request, From}, State}.

handle_cast({retry_delete, Q, Username},
            State0 = #state{pending = Pending0}) ->
    Key = {amqqueue:get_name(Q), amqqueue:get_exclusive_owner(Q)},
    Pending = Pending0#{Key => {Q, Username}},
    {noreply, maybe_wait_for_khepri(State0#state{pending = Pending})};
handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(retry_delete, State0 = #state{pending = Pending0}) ->
    case maps:to_list(Pending0) of
        [] ->
            {noreply, State0};
        [{Key, {Q, Username}} | _] ->
            case rabbit_queue_type:delete(Q, false, false, Username) of
                {error, timeout} ->
                    {noreply, maybe_wait_for_khepri(State0)};
                _ ->
                    ?LOG_INFO("Deleting stale MQTT QoS0 queue metadata: ~0p", [Key]),
                    Pending = maps:remove(Key, Pending0),
                    self() ! retry_delete,
                    {noreply, State0#state{pending = Pending}}
            end
    end;
handle_info({khepri_available, Pid},
            State0 = #state{fence = {Pid, _Ref}}) ->
    self() ! retry_delete,
    {noreply, State0#state{fence = undefined}};
handle_info({'DOWN', Ref, process, Pid, _Reason},
            State0 = #state{fence = {Pid, Ref}}) ->
    {noreply, maybe_wait_for_khepri(State0#state{fence = undefined})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fence = {Pid, _Ref}}) ->
    exit(Pid, shutdown),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_wait_for_khepri(State = #state{pending = Pending, fence = undefined})
  when map_size(Pending) > 0 ->
    Parent = self(),
    {Pid, Ref} = spawn_monitor(fun() -> wait_for_khepri(Parent) end),
    State#state{fence = {Pid, Ref}};
maybe_wait_for_khepri(State) ->
    State.

%% rabbit_khepri:fence/1 can return immediately with an error (e.g. `noproc`)
%% instead of blocking when the local Khepri store isn't up yet, so retry
%% with a fixed delay instead of busy-looping.
wait_for_khepri(Parent) ->
    case rabbit_khepri:fence(infinity) of
        ok ->
            Parent ! {khepri_available, self()};
        {error, _} ->
            timer:sleep(1000),
            wait_for_khepri(Parent)
    end.
