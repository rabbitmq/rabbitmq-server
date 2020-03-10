%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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
    init_handle_port_please(Mod:port_please(Me, Host), Mod, Me, Host).

init_handle_port_please(noport, Mod, Me, Host) ->
    State = #state{mod = Mod,
                   me = Me,
                   host = Host,
                   port = undefined},
    rabbit_log:info("epmd does not know us, re-registering as ~s~n", [Me]),
    {ok, ensure_timer(State)};
init_handle_port_please({port, Port, _Version}, Mod, Me, Host) ->
    rabbit_log:info("epmd monitor knows us, inter-node communication (distribution) port: ~p", [Port]),
    State = #state{mod = Mod,
                   me = Me,
                   host = Host,
                   port = Port},
    {ok, ensure_timer(State)};
init_handle_port_please({error, Error}, Mod, Me, Host) ->
    rabbit_log:error("epmd monitor failed to retrieve our port from epmd: ~p", [Error]),
    State = #state{mod = Mod,
                   me = Me,
                   host = Host,
                   port = undefined},
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
                          port = Port}) ->
    rabbit_log:debug("Asked to [re-]register this node (~s@~s) with epmd...", [Me, Host]),
    Port1 = case Mod:port_please(Me, Host) of
                noport ->
                    rabbit_log:warning("epmd does not know us, re-registering ~s at port ~b~n",
                                       [Me, Port]),
                    Port;
                {port, NewPort, _Version} ->
                    NewPort;
                {error, Error} ->
                    rabbit_log:error("epmd monitor failed to retrieve our port from epmd: ~p", [Error]),
                    Port
            end,
    rabbit_nodes:ensure_epmd(),
    Mod:register_node(Me, Port1),
    rabbit_log:debug("[Re-]registered this node (~s@~s) with epmd at port ~p", [Me, Host, Port1]),
    {ok, State#state{port = Port1}}.
