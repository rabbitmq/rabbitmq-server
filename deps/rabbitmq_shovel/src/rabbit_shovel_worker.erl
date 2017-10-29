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
%%  Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% for testing purposes
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
                static ->
                     Config0;
                dynamic ->
                    ClusterName = rabbit_nodes:cluster_name(),
                    {ok, Conf} = rabbit_shovel_parameters:parse(Name,
                                                                ClusterName,
                                                                Config0),
                    Conf
            end,
    {ok, #state{name = Name, type = Type, config = Config}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config = Config}) ->
    Config1 = rabbit_shovel_behaviour:connect_source(Config),
    Config2 =  rabbit_shovel_behaviour:connect_dest(Config1),
    %% Don't trap exits until we have established connections so that
    %% if we try to shut down while waiting for a connection to be
    %% established then we don't block
    process_flag(trap_exit, true),
    Config3 = rabbit_shovel_behaviour:init_dest(Config2),
    Config4 = rabbit_shovel_behaviour:init_source(Config3),
    State1 = State#state{config = Config4},
    ok = report_running(State1),
    {noreply, State1}.


handle_info(Msg, State = #state{config = Config, name = Name}) ->
    case rabbit_shovel_behaviour:handle_source(Msg, Config) of
        not_handled ->
            case rabbit_shovel_behaviour:handle_dest(Msg, Config) of
                not_handled ->
                    error_logger:info_msg("Shovel ~p message not handled! ~p~n",
                                          [Name, Msg]),
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
    _ = rabbit_runtime_parameters:clear(VHost, <<"shovel">>, Name, ?SHOVEL_USER),
    rabbit_shovel_status:remove({VHost, Name}),
    ok;
terminate(Reason, State) ->
    error_logger:info_msg("terminating static worker with ~p ~n", [Reason]),
    close_connections(State),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, Reason}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------

report_running(#state{config = Config} = State) ->
    InUri = rabbit_shovel_behaviour:source_uri(Config),
    OutUri = rabbit_shovel_behaviour:dest_uri(Config),
    InProto = rabbit_shovel_behaviour:source_protocol(Config),
    OutProto = rabbit_shovel_behaviour:dest_protocol(Config),
    InEndpoint = rabbit_shovel_behaviour:source_endpoint(Config),
    OutEndpoint = rabbit_shovel_behaviour:dest_endpoint(Config),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {running, [{src_uri,  rabbit_data_coercion:to_binary(InUri)},
                                           {src_protocol, rabbit_data_coercion:to_binary(InProto)},
                                           {dest_protocol, rabbit_data_coercion:to_binary(OutProto)},
                                           {dest_uri, rabbit_data_coercion:to_binary(OutUri)}]
                                 ++ props_to_binary(InEndpoint) ++ props_to_binary(OutEndpoint)
                                }).

props_to_binary(Props) ->
    [{K, rabbit_data_coercion:to_binary(V)} || {K, V} <- Props].

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

close_connections(#state{config = Conf}) ->
    ok = rabbit_shovel_behaviour:close_source(Conf),
    ok = rabbit_shovel_behaviour:close_dest(Conf).
