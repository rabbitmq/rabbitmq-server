%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_db_handler).

-include_lib("rabbit_common/include/rabbit.hrl").

%% Make sure our database is hooked in *before* listening on the network or
%% recovering queues (i.e. so there can't be any events fired before it starts).
-rabbit_boot_step({rabbit_mgmt_db_handler,
                   [{description, "management agent"},
                    {mfa,         {?MODULE, add_handler, []}},
                    {cleanup,     {gen_event, delete_handler,
                                   [rabbit_event, ?MODULE, []]}},
                    {requires,    rabbit_event},
                    {enables,     recovery}]}).

-behaviour(gen_event).

-export([add_handler/0, gc/0, rates_mode/0]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

add_handler() ->
    ok = ensure_statistics_enabled(),
    gen_event:add_handler(rabbit_event, ?MODULE, []).

gc() ->
    erlang:garbage_collect(whereis(rabbit_event)).

rates_mode() ->
    case rabbit_mgmt_agent_config:get_env(rates_mode) of
        undefined -> basic;
        Mode      -> Mode
    end.

handle_force_fine_statistics() ->
    case rabbit_mgmt_agent_config:get_env(force_fine_statistics) of
        undefined ->
            ok;
        X ->
            _ = rabbit_log:warning(
              "force_fine_statistics set to ~p; ignored.~n"
              "Replaced by {rates_mode, none} in the rabbitmq_management "
              "application.~n", [X])
    end.

%%----------------------------------------------------------------------------

ensure_statistics_enabled() ->
    ForceStats = rates_mode() =/= none,
    handle_force_fine_statistics(),
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    _ = rabbit_log:info("Management plugin: using rates mode '~p'~n", [rates_mode()]),
    case {ForceStats, StatsLevel} of
        {true,  fine} ->
            ok;
        {true,  _} ->
            application:set_env(rabbit, collect_statistics, fine);
        {false, none} ->
            application:set_env(rabbit, collect_statistics, coarse);
        {_, _} ->
            ok
    end,
    ok = rabbit:force_event_refresh(erlang:make_ref()).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, []}.

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type = Type} = Event, State)
  when Type == connection_closed; Type == channel_closed; Type == queue_deleted;
       Type == exchange_deleted; Type == vhost_deleted;
       Type == consumer_deleted; Type == node_node_deleted;
       Type == channel_consumer_deleted ->
    gen_server:cast(rabbit_mgmt_metrics_gc:name(Type), {event, Event}),
    {ok, State};
handle_event(_, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
