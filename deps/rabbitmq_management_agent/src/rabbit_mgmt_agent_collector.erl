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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_mgmt_agent_collector).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {server, table, tref, interval}).

%%----------------------------------------------------------------------------
%% Specs
%%----------------------------------------------------------------------------

-spec start_link(pid(), atom(), integer()) -> rabbit_types:ok_pid_or_error().

%%----------------------------------------------------------------------------
%% The agent collector pushes the raw metrics to the management plugin, and
%% it is started on demand by the later. The interval is controlled by the
%% management plugin, which usually corresponds to the smaller sample retention
%% policy for such metric.
%%----------------------------------------------------------------------------

start_link(Server, Table, Interval) ->
    gen_server:start_link(?MODULE, [Server, Table, Interval], []).

%% ---------------------------------------------------------------------------
%% gen_server
%% ---------------------------------------------------------------------------

init([Server, Table, Interval]) ->
    TRef = erlang:send_after(Interval, self(), push_metrics),
    {ok, #state{server = Server, table = Table, tref = TRef,
		interval = Interval}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(push_metrics, #state{interval = Interval} = State) ->
    Metrics = ets:tab2list(State#state.table),
    case Metrics of
	[] ->
	    ok;
	_ ->
	    gen_server:cast(State#state.server,
			    {metrics, exometer_slide:timestamp(), Metrics})
    end,
    TRef = erlang:send_after(Interval, self(), push_metrics),
    {noreply, State#state{tref = TRef}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
