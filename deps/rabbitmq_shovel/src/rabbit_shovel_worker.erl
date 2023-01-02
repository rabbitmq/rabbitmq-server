%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_shovel_worker).
-behaviour(gen_server2).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% for testing purposes
-export([get_connection_name/1,
         get_internal_config/1]).

-include("rabbit_shovel.hrl").

-record(state, {name :: binary() | {rabbit_types:vhost(), binary()},
                type :: static | dynamic,
                config :: rabbit_shovel_behaviour:state(),
                last_reported_status = running :: rabbit_shovel_status:blocked_status()}).

start_link(Type, Name, Config) ->
    ShovelParameter = rabbit_shovel_util:get_shovel_parameter(Name),
    maybe_start_link(ShovelParameter, Type, Name, Config).

maybe_start_link(not_found, dynamic, _Name, _Config) ->
    %% See rabbitmq/rabbitmq-server#2655.
    %% All dynamic shovels require that their associated parameter is present.
    %% If not, this shovel has been deleted and stale child spec information
    %% may still reside in the supervisor.
    %%
    %% We return 'ignore' to ensure that the child is not [re-]added in such case.
    ignore;
maybe_start_link(_, Type, Name, Config) ->
    ok = rabbit_shovel_status:report(Name, Type, starting),
    gen_server2:start_link(?MODULE, [Type, Name, Config], []).

%%---------------------------
%% Gen Server Implementation
%%---------------------------

init([Type, Name, Config0]) ->
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
    rabbit_log_shovel:debug("Initialising a Shovel ~ts of type '~ts'", [human_readable_name(Name), Type]),
    gen_server2:cast(self(), init),
    {ok, #state{name = Name, type = Type, config = Config}}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(init, State = #state{config = Config0}) ->
    try rabbit_shovel_behaviour:connect_source(Config0) of
      Config ->
        rabbit_log_shovel:debug("Shovel ~ts connected to source", [human_readable_name(maps:get(name, Config))]),
        %% this makes sure that connection pid is updated in case
        %% any of the subsequent connection/init steps fail. See
        %% rabbitmq/rabbitmq-shovel#54 for context.
        gen_server2:cast(self(), connect_dest),
        {noreply, State#state{config = Config}}
    catch _:_ ->
      rabbit_log_shovel:error("Shovel ~ts could not connect to source", [human_readable_name(maps:get(name, Config0))]),
      {stop, shutdown, State}
    end;
handle_cast(connect_dest, State = #state{config = Config0}) ->
    try rabbit_shovel_behaviour:connect_dest(Config0) of
      Config ->
        rabbit_log_shovel:debug("Shovel ~ts connected to destination", [human_readable_name(maps:get(name, Config))]),
        gen_server2:cast(self(), init_shovel),
        {noreply, State#state{config = Config}}
    catch _:_ ->
      rabbit_log_shovel:error("Shovel ~ts could not connect to destination", [human_readable_name(maps:get(name, Config0))]),
      {stop, shutdown, State}
    end;
handle_cast(init_shovel, State = #state{config = Config}) ->
    %% Don't trap exits until we have established connections so that
    %% if we try to shut down while waiting for a connection to be
    %% established then we don't block
    process_flag(trap_exit, true),
    Config1 = rabbit_shovel_behaviour:init_dest(Config),
    Config2 = rabbit_shovel_behaviour:init_source(Config1),
    rabbit_log_shovel:debug("Shovel ~ts has finished setting up its topology", [human_readable_name(maps:get(name, Config2))]),
    State1 = State#state{config = Config2},
    ok = report_running(State1),
    {noreply, State1}.


handle_info(Msg, State = #state{config = Config, name = Name}) ->
    case rabbit_shovel_behaviour:handle_source(Msg, Config) of
        not_handled ->
            case rabbit_shovel_behaviour:handle_dest(Msg, Config) of
                not_handled ->
                    rabbit_log_shovel:warning("Shovel ~ts could not handle a destination message ~tp", [human_readable_name(Name), Msg]),
                    {noreply, State};
                {stop, {outbound_conn_died, heartbeat_timeout}} ->
                    rabbit_log_shovel:error("Shovel ~ts detected missed heartbeats on destination connection", [human_readable_name(Name)]),
                    {stop, {shutdown, heartbeat_timeout}, State};
                {stop, {outbound_conn_died, Reason}} ->
                    rabbit_log_shovel:error("Shovel ~ts detected destination connection failure: ~tp", [human_readable_name(Name), Reason]),
                    {stop, Reason, State};
	            {stop, {outbound_link_or_channel_closure, Reason}} ->
    		        rabbit_log_shovel:error("Shovel ~ts detected destination shovel failure: ~tp", [human_readable_name(Name), Reason]),
    		        {stop, Reason, State};
                {stop, Reason} ->
                    rabbit_log_shovel:debug("Shovel ~ts decided to stop due a message from destination: ~tp", [human_readable_name(Name), Reason]),
                    {stop, Reason, State};
                Config1 ->
                    State1 = State#state{config = Config1},
                    State2 = maybe_report_blocked_status(State1),
                    {noreply, State2}
            end;
        {stop, {inbound_conn_died, heartbeat_timeout}} ->
            rabbit_log_shovel:error("Shovel ~ts detected missed heartbeats on source connection", [human_readable_name(Name)]),
            {stop, {shutdown, heartbeat_timeout}, State};
        {stop, {inbound_conn_died, Reason}} ->
            rabbit_log_shovel:error("Shovel ~ts detected source connection failure: ~tp", [human_readable_name(Name), Reason]),
            {stop, Reason, State};
        {stop, {inbound_link_or_channel_closure, Reason}} ->
	        rabbit_log_shovel:error("Shovel ~ts detected source Shovel (or link,  or channel) failure: ~tp", [human_readable_name(Name), Reason]),
	        {stop, Reason, State};
        {stop, Reason} ->
            rabbit_log_shovel:error("Shovel ~ts decided to stop due a message from source: ~tp", [human_readable_name(Name), Reason]),
            {stop, Reason, State};
        Config1 ->
            State1 = State#state{config = Config1},
            State2 = maybe_report_blocked_status(State1),
            {noreply, State2}
    end.

terminate({shutdown, autodelete}, State = #state{name = Name,
                                                 type = dynamic}) ->
    {VHost, ShovelName} = Name,
    rabbit_log_shovel:info("Shovel '~ts' is stopping (it was configured to autodelete and transfer is completed)",
                           [human_readable_name(Name)]),
    close_connections(State),
    %% See rabbit_shovel_dyn_worker_sup_sup:stop_child/1
    put({shovel_worker_autodelete, Name}, true),
    _ = rabbit_runtime_parameters:clear(VHost, <<"shovel">>, ShovelName, ?SHOVEL_USER),
    rabbit_shovel_status:remove(Name),
    ok;
terminate(shutdown, State) ->
    close_connections(State),
    ok;
terminate(socket_closed_unexpectedly, State) ->
    close_connections(State),
    ok;
terminate({'EXIT', heartbeat_timeout}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping because of a heartbeat timeout", [human_readable_name(Name)]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "heartbeat timeout"}),
    close_connections(State),
    ok;
terminate({'EXIT', outbound_conn_died}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping because destination connection failed", [human_readable_name(Name)]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "destination connection failed"}),
    close_connections(State),
    ok;
terminate({'EXIT', inbound_conn_died}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping because destination connection failed", [human_readable_name(Name)]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "source connection failed"}),
    close_connections(State),
    ok;
terminate({shutdown, heartbeat_timeout}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping because of a heartbeat timeout", [human_readable_name(Name)]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "heartbeat timeout"}),
    close_connections(State),
    ok;
terminate({shutdown, restart}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping to restart", [human_readable_name(Name)]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "needed a restart"}),
    close_connections(State),
    ok;
terminate({{shutdown, {server_initiated_close, Code, Reason}}, _}, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping: one of its connections closed "
                            "with code ~b, reason: ~ts",
                            [human_readable_name(Name), Code, Reason]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, "needed a restart"}),
    close_connections(State),
    ok;
terminate(Reason, State = #state{name = Name}) ->
    rabbit_log_shovel:error("Shovel ~ts is stopping, reason: ~tp", [human_readable_name(Name), Reason]),
    rabbit_shovel_status:report(State#state.name, State#state.type,
                                {terminated, Reason}),
    close_connections(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%---------------------------
%% Helpers
%%---------------------------

human_readable_name(Name) ->
  case Name of
    {VHost, ShovelName} -> rabbit_misc:format("'~ts' in virtual host '~ts'", [ShovelName, VHost]);
    ShovelName          -> rabbit_misc:format("'~ts'", [ShovelName])
  end.

maybe_report_blocked_status(#state{config = Config,
                                   last_reported_status = LastStatus} = State) ->
    case rabbit_shovel_behaviour:status(Config) of
        ignore ->
            State;
        LastStatus ->
            State;
        NewStatus ->
            rabbit_shovel_status:report_blocked_status(State#state.name, NewStatus),
            State#state{last_reported_status = NewStatus}
    end.

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

get_internal_config(#state{config = Conf}) ->
    Conf.
