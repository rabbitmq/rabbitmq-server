%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This gen_server starts a periodic timer on behalf of
%% a short lived process that kicks off peer discovery.
%% This is so that the timer is not automatically canceled
%% and cleaned up by the timer server when the short lived
%% process terminates.

-module(rabbitmq_peer_discovery_consul_health_check_helper).

-behaviour(gen_server).

-include_lib("rabbitmq_peer_discovery_common/include/rabbit_peer_discovery.hrl").
-include("rabbit_peer_discovery_consul.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {timer_ref}).


%%
%% API
%%

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    case rabbit_peer_discovery:backend() of
        rabbit_peer_discovery_consul ->
            set_up_periodic_health_check();
        rabbitmq_peer_discovery_consul ->
            set_up_periodic_health_check();
        _ ->
            {ok, #state{}}
    end.

handle_call(_Msg, _From, State) ->
    {reply, not_understood, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_MSg, State) ->
    {noreply, State}.

terminate(_Arg, #state{timer_ref = undefined}) ->
    ok;

terminate(_Arg, #state{timer_ref = TRef}) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%
%% Implementation
%%

set_up_periodic_health_check() ->
    M = rabbit_peer_discovery_config:config_map(peer_discover_consul),
    case rabbit_peer_discovery_config:get(consul_svc_ttl, ?CONFIG_MAPPING, M) of
        undefined ->
            {ok, #state{}};
        %% in seconds
        Interval  ->
            %% We cannot use timer:apply_interval/4 in rabbit_peer_discovery_consul
            %% because this function is executed in a short live process and when it
            %% exits, the timer module will automatically cancel the
            %% timer.
            %%
            %% Instead we delegate to a locally registered gen_server,
            %% `rabbitmq_peer_discovery_consul_health_check_helper`.
            %%
            %% The register step cannot call this gen_server either because when mnesia is
            %% started the plugins are not yet loaded.
            %%
            %% The value is 1/2 of what's configured to avoid a race
            %% condition between check TTL expiration and in flight
            %% notifications

            IntervalInMs = Interval * 500, % note this is 1/2
            _ = rabbit_log:info("Starting Consul health check notifier (effective interval: ~p milliseconds)", [IntervalInMs]),
            {ok, TRef} = timer:apply_interval(IntervalInMs, rabbit_peer_discovery_consul,
                                              send_health_check_pass, []),
            {ok, #state{timer_ref = TRef}}
    end.
