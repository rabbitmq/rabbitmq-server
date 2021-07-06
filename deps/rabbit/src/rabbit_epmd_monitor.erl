%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_epmd_monitor).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {timer, mod, me, host, port}).

-define(SERVER, ?MODULE).
-define(CHECK_FREQUENCY, 60000).

%%----------------------------------------------------------------------------
%% It's possible for epmd to be killed out from underneath us. If that
%% happens, then obviously clustering and rabbitmqctl stop
%% working. This process checks up on epmd and restarts it /
%% re-registers us with it if it has gone away.
%%
%% How could epmd be killed?
%%
%% 1) The most popular way for this to happen is when running as a
%%    Windows service. The user starts rabbitmqctl first, and this starts
%%    epmd under the user's account. When they log out epmd is killed.
%%
%% 2) Some packagings of (non-RabbitMQ?) Erlang apps might do "killall
%%    epmd" as a shutdown or uninstall step.
%% ----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {Me, Host} = rabbit_nodes:parts(node()),
    Mod = net_kernel:epmd_module(),
    {ok, Port} = handle_port_please(init, Mod:port_please(Me, Host), Me, undefined),
    State = #state{mod = Mod, me = Me, host = Host, port = Port},
    {ok, ensure_timer(State)}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(check, State0) ->
    {ok, State1} = check_epmd(State0),
    {noreply, ensure_timer(State1#state{timer = undefined})};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State0) ->
    {ok, State1} = check_epmd(State0),
    {noreply, ensure_timer(State1#state{timer = undefined})};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

ensure_timer(State) ->
    rabbit_misc:ensure_timer(State, #state.timer, ?CHECK_FREQUENCY, check).

check_epmd(State = #state{mod  = Mod,
                          me   = Me,
                          host = Host,
                          port = Port0}) ->
    {ok, Port1} = handle_port_please(check, Mod:port_please(Me, Host), Me, Port0),
    rabbit_nodes:ensure_epmd(),
    Mod:register_node(Me, Port1),
    {ok, State#state{port = Port1}}.

handle_port_please(init, noport, Me, Port) ->
    rabbit_log:info("epmd does not know us, re-registering as ~s", [Me]),
    {ok, Port};
handle_port_please(check, noport, Me, Port) ->
    rabbit_log:warning("epmd does not know us, re-registering ~s at port ~b", [Me, Port]),
    {ok, Port};
handle_port_please(_, closed, _Me, Port) ->
    rabbit_log:error("epmd monitor failed to retrieve our port from epmd: closed"),
    {ok, Port};
handle_port_please(init, {port, NewPort, _Version}, _Me, _Port) ->
    rabbit_log:info("epmd monitor knows us, inter-node communication (distribution) port: ~p", [NewPort]),
    {ok, NewPort};
handle_port_please(check, {port, NewPort, _Version}, _Me, _Port) ->
    {ok, NewPort};
handle_port_please(_, {error, Error}, _Me, Port) ->
    rabbit_log:error("epmd monitor failed to retrieve our port from epmd: ~p", [Error]),
    {ok, Port}.
