%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_db_handler).

-behaviour(gen_event).

-export([add_handler/0, gc/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

add_handler() ->
    ensure_statistics_enabled(),
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

gc() ->
    erlang:garbage_collect(whereis(rabbit_event)).

%%----------------------------------------------------------------------------

ensure_statistics_enabled() ->
    {ok, ForceStats} = application:get_env(rabbitmq_management_agent,
                                           force_fine_statistics),
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    case {ForceStats, StatsLevel} of
        {true,  fine} ->
            ok;
        {true,  _} ->
            application:set_env(rabbit, collect_statistics, fine);
        {false, none} ->
            application:set_env(rabbit, collect_statistics, coarse);
        {_, _} ->
            ok
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event, State) ->
    gen_server:cast({global, rabbit_mgmt_db}, {event, Event}),
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
