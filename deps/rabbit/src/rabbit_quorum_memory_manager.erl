%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_quorum_memory_manager).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).
-export([register/0, unregister/0]).

-record(state, {last_roll_over,
                interval}).

-rabbit_boot_step({rabbit_quorum_memory_manager,
                   [{description, "quorum memory manager"},
                    {mfa,         {?MODULE, register, []}},
                    {cleanup,     {?MODULE, unregister, []}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

register() ->
    gen_event:add_handler(rabbit_alarm, ?MODULE, []).

unregister() ->
    gen_event:delete_handler(rabbit_alarm, ?MODULE, []).

init([]) ->
    {ok, #state{interval = interval()}}.

handle_call( _, State) ->
    {ok, ok, State}.

handle_event({set_alarm, {{resource_limit, memory, Node}, []}},
             #state{last_roll_over = undefined} = State) when Node == node() ->
    {ok, force_roll_over(State)};
handle_event({set_alarm, {{resource_limit, memory, Node}, []}},
             #state{last_roll_over = Last, interval = Interval } = State)
  when Node == node() ->
    Now = erlang:system_time(millisecond),
    case Now > (Last + Interval) of
        true ->
            {ok, force_roll_over(State)};
        false ->
            {ok, State}
    end;
handle_event(_, State) ->
    {ok, State}.

handle_info(_, State) ->
    {ok, State}.

terminate(_, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

force_roll_over(State) ->
    rabbit_quorum_queue:wal_force_roll_over(node()),
    State#state{last_roll_over = erlang:system_time(millisecond)}.

interval() ->
    application:get_env(rabbit, min_wal_roll_over_interval, 20000).
