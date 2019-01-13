%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
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
    ra_log_wal:force_roll_over(ra_log_wal),
    State#state{last_roll_over = erlang:system_time(millisecond)}.

interval() ->
    application:get_env(rabbit, min_wal_roll_over_interval, 20000).
