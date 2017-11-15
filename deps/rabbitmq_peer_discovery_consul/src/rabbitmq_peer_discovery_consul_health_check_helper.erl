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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
                    rabbit_log:info("Starting Consul health check notifier (effective interval: ~p milliseconds)", [IntervalInMs]),
                    {ok, TRef} = timer:apply_interval(IntervalInMs, rabbit_peer_discovery_consul,
                                                      send_health_check_pass, []),
                    {ok, #state{timer_ref = TRef}}
            end;
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
