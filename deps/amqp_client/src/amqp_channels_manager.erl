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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_channels_manager).

-include("amqp_client.hrl").

-behaviour(gen_server).

-export([start_link/2, open_channel/3, set_channel_max/2, is_empty/1,
         num_channels/1, get_pid_and_framing/2, signal_connection_closing/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-record(state, {connection,
                channel_sup_sup,
                map_num_pf      = gb_trees:empty(), %% Number -> {Pid, Framing}
                map_pid_num     = dict:new(),       %% Pid -> Number
                channel_max     = ?MAX_CHANNEL_NUMBER,
                closing         = false}).

%%---------------------------------------------------------------------------
%% Interface
%%---------------------------------------------------------------------------

start_link(Connection, ChSupSup) ->
    gen_server:start_link(?MODULE, [Connection, ChSupSup], []).

open_channel(ChMgr, ProposedNumber, InfraArgs) ->
    gen_server:call(ChMgr, {open_channel, ProposedNumber, InfraArgs}, infinity).

set_channel_max(ChMgr, ChannelMax) ->
    gen_server:cast(ChMgr, {set_channel_max, ChannelMax}).

is_empty(ChMgr) ->
    gen_server:call(ChMgr, is_empty, infinity).

num_channels(ChMgr) ->
    gen_server:call(ChMgr, num_channels, infinity).

get_pid_and_framing(ChMgr, ChannelNumber) ->
    gen_server:call(ChMgr, {get_pid_and_framing, ChannelNumber}, infinity).

signal_connection_closing(ChMgr, ChannelCloseType, Reason) ->
    gen_server:cast(ChMgr, {connection_closing, ChannelCloseType, Reason}).

%%---------------------------------------------------------------------------
%% gen_server callbacks
%%---------------------------------------------------------------------------

init([Connection, ChSupSup]) ->
    {ok, #state{connection = Connection, channel_sup_sup = ChSupSup}}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

handle_call({open_channel, ProposedNumber, InfraArgs}, _,
            State = #state{closing = false}) ->
    handle_open_channel(ProposedNumber, InfraArgs, State);
handle_call(is_empty, _, State) ->
    {reply, internal_is_empty(State), State};
handle_call(num_channels, _, State) ->
    {reply, internal_num_channels(State), State};
handle_call({get_pid_and_framing, Number}, _, State) ->
    {reply, internal_lookup_npf(Number, State), State}.

handle_cast({set_channel_max, ChannelMax}, State) ->
    {noreply, State#state{channel_max = ChannelMax}};
handle_cast({connection_closing, ChannelCloseType, Reason}, State) ->
    handle_connection_closing(ChannelCloseType, Reason, State).

handle_info({'DOWN', _, process, Pid, Reason}, State) ->
    handle_down(Pid, Reason, State).

%%---------------------------------------------------------------------------
%% Internal plumbing
%%---------------------------------------------------------------------------

handle_open_channel(ProposedNumber, InfraArgs,
                    State = #state{channel_sup_sup = ChSupSup}) ->
    case new_number(ProposedNumber, State) of
        {ok, Number} ->
            {ok, _ChSup, {Ch, Framing}} =
                amqp_channel_sup_sup:start_channel_sup(ChSupSup, InfraArgs,
                                                       Number),
            NewState = internal_register(Number, Ch, Framing, State),
            erlang:monitor(process, Ch),
            {reply, {ok, Ch}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end.

new_number(none, #state{channel_max = ChannelMax, map_num_pf = MapNPF}) ->
    case gb_trees:is_empty(MapNPF) of
        true  -> {ok, 1};
        false -> {Smallest, _} = gb_trees:smallest(MapNPF),
                 if Smallest > 1 ->
                        {ok, Smallest - 1};
                    true ->
                        {Largest, _} = gb_trees:largest(MapNPF),
                        if Largest < ChannelMax -> {ok, Largest + 1};
                           true                 -> find_free(MapNPF)
                        end
                 end
    end;
new_number(Proposed, State = #state{channel_max = ChannelMax,
                                    map_num_pf  = MapNPF}) ->
    IsValid = Proposed > 0 andalso Proposed =< ChannelMax andalso
        not gb_trees:is_defined(Proposed, MapNPF),
    if IsValid -> {ok, Proposed};
       true    -> new_number(none, State)
    end.

find_free(MapNPF) ->
    find_free(gb_trees:iterator(MapNPF), 1).

find_free(It, Candidate) ->
    case gb_trees:next(It) of
        {Number, _, It1} -> if Number > Candidate   -> {ok, Number - 1};
                               Number =:= Candidate -> find_free(It1,
                                                                 Candidate + 1);
                               true                 -> exit(unexpected)
                            end;
        none             -> {error, out_of_channel_numbers}
    end.

handle_down(Pid, Reason, State) ->
    case internal_lookup_pn(Pid, State) of
        undefined -> {stop, {error, unexpected_down}, State};
        Number    -> handle_channel_down(Pid, Number, Reason, State)
    end.

handle_channel_down(Pid, Number, Reason, State) ->
    down_side_effect(Pid, Reason, State),
    NewState = internal_unregister(Number, Pid, State),
    check_all_channels_terminated(NewState),
    {noreply, NewState}.

down_side_effect(_Pid, normal, _State) ->
    ok;
down_side_effect(Pid, {server_initiated_close, Code, _Text} = Reason, State) ->
    {IsHardError, _, _} = ?PROTOCOL:lookup_amqp_exception(
                            ?PROTOCOL:amqp_exception(Code)),
    case IsHardError of
        true  -> signal_connection({hard_error_in_channel, Pid, Reason}, State);
        false -> ok
    end;
down_side_effect(_Pid, {app_initiated_close, _, _}, _State) ->
    ok;
down_side_effect(_Pid, {connection_closing, _}, _State) ->
    ok;
down_side_effect(Pid, Other, State) ->
    signal_connection({channel_internal_error, Pid, Other}, State).

check_all_channels_terminated(#state{closing = false}) ->
    ok;
check_all_channels_terminated(State = #state{closing = true}) ->
    case internal_is_empty(State) of
        true  -> signal_connection(all_channels_terminated, State);
        false -> ok
    end.

handle_connection_closing(ChannelCloseType, Reason, State) ->
    case internal_is_empty(State) of
        true  -> signal_connection(all_channels_terminated, State);
        false -> signal_channels({connection_closing, ChannelCloseType, Reason},
                                 State)
    end,
    {noreply, State#state{closing = true}}.

%%---------------------------------------------------------------------------

internal_register(Number, Pid, Framing,
                  State = #state{map_num_pf = MapNPF, map_pid_num = MapPN}) ->
    MapNPF1 = gb_trees:enter(Number, {Pid, Framing}, MapNPF),
    MapPN1 = dict:store(Pid, Number, MapPN),
    State#state{map_num_pf  = MapNPF1,
                map_pid_num = MapPN1}.

internal_unregister(Number, Pid,
                    State = #state{map_num_pf = MapNPF, map_pid_num = MapPN}) ->
    MapNPF1 = gb_trees:delete(Number, MapNPF),
    MapPN1 = dict:erase(Pid, MapPN),
    State#state{map_num_pf  = MapNPF1,
                map_pid_num = MapPN1}.

internal_is_empty(#state{map_num_pf = MapNPF}) ->
    gb_trees:is_empty(MapNPF).

internal_num_channels(#state{map_num_pf = MapNPF}) ->
    gb_trees:size(MapNPF).

internal_lookup_npf(Number, #state{map_num_pf = MapNPF}) ->
    case gb_trees:lookup(Number, MapNPF) of {value, PF} -> PF;
                                            none        -> undefined
    end.

internal_lookup_pn(Pid, #state{map_pid_num = MapPN}) ->
    case dict:find(Pid, MapPN) of {ok, Number} -> Number;
                                  error        -> undefined
    end.

signal_channels(Msg, #state{map_pid_num = MapPN}) ->
    dict:map(fun(Pid, _) -> Pid ! Msg, ok end, MapPN),
    ok.

signal_connection(_, #state{connection = undefined}) ->
    ?LOG_WARN("No connection registered in channels manager (~p).~n", [self()]);
signal_connection(Msg, #state{connection = Connection}) ->
    Connection ! Msg,
    ok.
