%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% for testing purposes - TODO: remove
-export([get_connection_name/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-record(state, {inbound_conn, inbound_ch, outbound_conn, outbound_ch,
                name, type, config, inbound_uri, outbound_uri, unacked,
                remaining, %% [1]
                remaining_unacked}). %% [2]

%% [1] Counts down until we shut down in all modes
%% [2] Counts down until we stop publishing in on-confirm mode

start_link(Type, Name, Config) ->
    ok = rabbit_shovel_status:report(Name, Type, starting),
    gen_server2:start_link(?MODULE, [Type, Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Type, Name, Config0]) ->
    gen_server2:cast(self(), init),
    Config = case Type of
                static -> Config0;
                dynamic ->
                    {ok, Conf} = rabbit_shovel_parameters:parse(Name, Config0),
                    Conf
            end,

    % error_logger:info_msg("Config ~p Name ~p Type ~p~n", [Config, Name, Type]),

    {ok, #state{name = Name, type = Type, config = Config}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config =
                                 #{source := #{module := SrcMod},
                                   dest := #{module := DstMod}} = Config}) ->
    Config1 = SrcMod:connect_source(Config),
    Config2 = DstMod:connect_dest(Config1),
    %% Don't trap exits until we have established connections so that
    %% if we try to shut down while waiting for a connection to be
    %% established then we don't block
    process_flag(trap_exit, true),
    Config3 = SrcMod:init_source(Config2),
    Config4 = DstMod:init_dest(Config3),
    State1 = State#state{config = Config4},
    ok = report_running(State1),
    {noreply, State1}.


handle_info(Msg, State = #state{config =
                                #{source := #{module := SrcMod},
                                  dest := #{module := DstMod}} = Config}) ->
    error_logger:info_msg("handle_info ~p~n", [Msg]),
    case SrcMod:handle_source(Msg, Config) of
        not_handled ->
            case DstMod:handle_dest(Msg, Config) of
                not_handled ->
                    error_logger:info_msg("message not handled! ~p ~p~n",
                                          [Msg, Config]),
                    {noreply, State};
                {stop, Reason} ->
                    {stop, Reason, State};
                Config1 ->
                    {noreply, State#state{config = Config1}}
            end;
        {stop, Reason} ->
            {stop, Reason, State};
        Config1 ->
            {noreply, State#state{config = Config1}}
    end.

terminate({shutdown, autodelete}, State = #state{name = {VHost, Name},
                                                 type = dynamic}) ->
    error_logger:info_msg("terminating dynamic worker ~p~n", [{VHost, Name}]),
    close_connections(State),
    %% See rabbit_shovel_dyn_worker_sup_sup:stop_child/1
    put(shovel_worker_autodelete, true),
    rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name, ?SHOVEL_USER),
    rabbit_shovel_status:remove({VHost, Name}),
    ok;
terminate(Reason, State) ->
    error_logger:info_msg("terminating static worker ~p ~p~n", [State, Reason]),
    close_connections(State),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------

report_running(#state{config = Config = #{source := #{module := Src},
                                          dest := #{module := Dst}}} = State) ->
    InUri = Src:source_uri(Config),
    OutUri = Dst:dest_uri(Config),
    rabbit_shovel_status:report(
      State#state.name, State#state.type,
      {running, [{src_uri,  InUri},
                 {dest_uri, OutUri}]}).


%% for static shovels, name is an atom from the configuration file
get_connection_name(ShovelName) when is_atom(ShovelName) ->
    Prefix = <<"Shovel ">>,
    ShovelNameAsBinary = atom_to_binary(ShovelName, utf8),
    <<Prefix/binary, ShovelNameAsBinary/binary>>;

%% for dynamic shovels, name is a tuple with a binary
get_connection_name({_, Name}) when is_binary(Name) ->
    Prefix = <<"Shovel ">>,
    <<Prefix/binary, Name/binary>>;

%% fallback
get_connection_name(_) ->
    <<"Shovel">>.

close_connections(#state{config = #{dest := #{module := DstMod},
                                    source := #{module := SrcMod}} = Conf}) ->
    ok = SrcMod:close_source(Conf),
    ok = DstMod:close_dest(Conf).
