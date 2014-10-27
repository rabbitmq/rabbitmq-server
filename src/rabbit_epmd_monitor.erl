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
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
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

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).

-endif.

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

start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    {Me, Host} = rabbit_nodes:parts(node()),
    Mod = net_kernel:epmd_module(),
    {port, Port, _Version} = Mod:port_please(Me, Host),
    {ok, ensure_timer(#state{mod  = Mod,
                             me   = Me,
                             host = Host,
                             port = Port})}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check, State) ->
    check_epmd(State),
    {noreply, ensure_timer(State#state{timer = undefined})};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

ensure_timer(State) ->
    rabbit_misc:ensure_timer(State, #state.timer, ?CHECK_FREQUENCY, check).

check_epmd(#state{mod  = Mod,
                  me   = Me,
                  host = Host,
                  port = Port}) ->
    case Mod:port_please(Me, Host) of
        noport -> rabbit_log:warning(
                    "epmd does not know us, re-registering ~s at port ~b~n",
                    [Me, Port]),
                  rabbit_nodes:ensure_epmd(),
                  erl_epmd:register_node(Me, Port);
        _      -> ok
    end.
