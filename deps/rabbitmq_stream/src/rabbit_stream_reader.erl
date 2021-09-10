%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_reader).

-behaviour(gen_statem).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-include("rabbit_stream_metrics.hrl").

-type stream() :: binary().
-type publisher_id() :: byte().
-type publisher_reference() :: binary().
-type subscription_id() :: byte().

-record(publisher,
        {publisher_id :: publisher_id(),
         stream :: stream(),
         reference :: undefined | publisher_reference(),
         leader :: pid(),
         message_counters :: atomics:atomics_ref()}).
-record(consumer,
        {socket :: rabbit_net:socket(), %% ranch_transport:socket(),
         member_pid :: pid(),
         offset :: osiris:offset(),
         subscription_id :: subscription_id(),
         segment :: osiris_log:state(),
         credit :: integer(),
         stream :: stream(),
         counters :: atomics:atomics_ref(),
         properties :: map()}).
-record(stream_connection_state,
        {data :: rabbit_stream_core:state(), blocked :: boolean(),
         consumers :: #{subscription_id() => #consumer{}}}).
-record(stream_connection,
        {name :: binary(),
         %% server host
         host,
         %% client host
         peer_host,
         %% server port
         port,
         %% client port
         peer_port,
         auth_mechanism,
         connected_at :: integer(),
         helper_sup :: pid(),
         socket :: rabbit_net:socket(),
         publishers ::
             #{publisher_id() =>
                   #publisher{}}, %% FIXME replace with a list (0-255 lookup faster?)
         publisher_to_ids ::
             #{{stream(), publisher_reference()} => publisher_id()},
         stream_leaders :: #{stream() => pid()},
         stream_subscriptions :: #{stream() => [subscription_id()]},
         credits :: atomics:atomics_ref(),
         authentication_state :: atom(),
         user :: undefined | #user{},
         virtual_host :: undefined | binary(),
         connection_step ::
             atom(), % tcp_connected, peer_properties_exchanged, authenticating, authenticated, tuning, tuned, opened, failure, closing, closing_done
         frame_max :: integer(),
         heartbeat :: undefined | integer(),
         heartbeater :: any(),
         client_properties = #{} :: #{binary() => binary()},
         monitors = #{} :: #{reference() => stream()},
         stats_timer :: undefined | reference(),
         resource_alarm :: boolean(),
         send_file_oct ::
             atomics:atomics_ref(), % number of bytes sent with send_file (for metrics)
         transport :: tcp | ssl,
         proxy_socket :: undefined | ranch_proxy:proxy_socket()}).
-record(configuration,
        {initial_credits :: integer(),
         credits_required_for_unblocking :: integer(),
         frame_max :: integer(),
         heartbeat :: integer(),
         connection_negotiation_step_timeout :: integer()}).
-record(statem_data,
        {transport :: module(),
         connection :: #stream_connection{},
         connection_state :: #stream_connection_state{},
         config :: #configuration{}}).

-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         port,
         peer_port,
         host,
         peer_host,
         ssl,
         peer_cert_subject,
         peer_cert_issuer,
         peer_cert_validity,
         auth_mechanism,
         ssl_protocol,
         ssl_key_exchange,
         ssl_cipher,
         ssl_hash,
         protocol,
         user,
         vhost,
         protocol,
         timeout,
         frame_max,
         channel_max,
         client_properties,
         connected_at,
         node,
         user_who_performed_action]).
-define(SIMPLE_METRICS, [pid, recv_oct, send_oct, reductions]).
-define(OTHER_METRICS,
        [recv_cnt,
         send_cnt,
         send_pend,
         state,
         channels,
         garbage_collection,
         timeout]).
-define(AUTH_NOTIFICATION_INFO_KEYS,
        [host,
         name,
         peer_host,
         peer_port,
         protocol,
         auth_mechanism,
         ssl,
         ssl_protocol,
         ssl_cipher,
         peer_cert_issuer,
         peer_cert_subject,
         peer_cert_validity]).

%% client API
-export([start_link/4,
         info/2,
         consumers_info/2,
         publishers_info/2,
         in_vhost/2]).
-export([resource_alarm/3]).
%% gen_statem callbacks
-export([callback_mode/0,
         terminate/3,
         init/1,
         tcp_connected/3,
         peer_properties_exchanged/3,
         authenticating/3,
         tuning/3,
         tuned/3,
         open/3,
         close_sent/3]).

         %% not called by gen_statem since gen_statem:enter_loop/4 is used

         %% states

callback_mode() ->
    [state_functions, state_enter].

terminate(Reason, State, StatemData) ->
    rabbit_networking:unregister_non_amqp_connection(self()),
    notify_connection_closed(StatemData),
    rabbit_log:debug("~p terminating in state '~s' with reason '~p'",
                     [?MODULE, State, Reason]).

start_link(KeepaliveSup, Transport, Ref, Opts) ->
    {ok,
     proc_lib:spawn_link(?MODULE, init,
                         [[KeepaliveSup, Transport, Ref, Opts]])}.

init([KeepaliveSup,
      Transport,
      Ref,
      #{initial_credits := InitialCredits,
        credits_required_for_unblocking := CreditsRequiredBeforeUnblocking,
        frame_max := FrameMax,
        heartbeat := Heartbeat,
        transport := ConnTransport}]) ->
    process_flag(trap_exit, true),
    {ok, Sock} =
        rabbit_networking:handshake(Ref,
                                    application:get_env(rabbitmq_stream,
                                                        proxy_protocol, false)),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            Credits = atomics:new(1, [{signed, true}]),
            SendFileOct = atomics:new(1, [{signed, false}]),
            atomics:put(SendFileOct, 1, 0),
            init_credit(Credits, InitialCredits),
            {PeerHost, PeerPort, Host, Port} =
                socket_op(Sock,
                          fun(S) -> rabbit_net:socket_ends(S, inbound) end),
            Connection =
                #stream_connection{name =
                                       rabbit_data_coercion:to_binary(ConnStr),
                                   host = Host,
                                   peer_host = PeerHost,
                                   port = Port,
                                   peer_port = PeerPort,
                                   connected_at = os:system_time(milli_seconds),
                                   auth_mechanism = none,
                                   helper_sup = KeepaliveSup,
                                   socket = RealSocket,
                                   publishers = #{},
                                   publisher_to_ids = #{},
                                   stream_leaders = #{},
                                   stream_subscriptions = #{},
                                   credits = Credits,
                                   authentication_state = none,
                                   connection_step = tcp_connected,
                                   frame_max = FrameMax,
                                   resource_alarm = false,
                                   send_file_oct = SendFileOct,
                                   transport = ConnTransport,
                                   proxy_socket =
                                       rabbit_net:maybe_get_proxy_socket(Sock)},
            State =
                #stream_connection_state{consumers = #{},
                                         blocked = false,
                                         data =
                                             rabbit_stream_core:init(undefined)},
            Transport:setopts(RealSocket, [{active, once}]),
            rabbit_alarm:register(self(), {?MODULE, resource_alarm, []}),
            ConnectionNegotiationStepTimeout =
                application:get_env(rabbitmq_stream,
                                    connection_negotiation_step_timeout,
                                    10_000),
            % gen_statem process has its start_link call not return until the init function returns.
            % This is problematic, because we won't be able to call ranch:handshake/2
            % from the init callback as this would cause a deadlock to happen.
            % Therefore, we use the gen_statem:enter_loop/4 function.
            % See https://ninenines.eu/docs/en/ranch/2.0/guide/protocols/
            gen_statem:enter_loop(?MODULE,
                                  [],
                                  tcp_connected,
                                  #statem_data{transport = Transport,
                                               connection = Connection,
                                               connection_state = State,
                                               config =
                                                   #configuration{initial_credits
                                                                      =
                                                                      InitialCredits,
                                                                  credits_required_for_unblocking
                                                                      =
                                                                      CreditsRequiredBeforeUnblocking,
                                                                  frame_max =
                                                                      FrameMax,
                                                                  heartbeat =
                                                                      Heartbeat,
                                                                  connection_negotiation_step_timeout
                                                                      =
                                                                      ConnectionNegotiationStepTimeout}});
        {Error, Reason} ->
            rabbit_net:fast_close(RealSocket),
            rabbit_log_connection:warning("Closing connection because of ~p ~p",
                                          [Error, Reason])
    end.

tcp_connected(enter, _OldState,
              #statem_data{config =
                               #configuration{connection_negotiation_step_timeout
                                                  = StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
tcp_connected(state_timeout, close,
              #statem_data{transport = Transport,
                           connection = #stream_connection{socket = Socket}}) ->
    state_timeout(?FUNCTION_NAME, Transport, Socket);
tcp_connected(info, Msg, StateData) ->
    handle_info(Msg, StateData,
                fun(NextConnectionStep,
                    #statem_data{transport = Transport,
                                 connection = #stream_connection{socket = S}} =
                        StatemData,
                    NewConnection,
                    NewConnectionState) ->
                   if NextConnectionStep =:= peer_properties_exchanged ->
                          {next_state, peer_properties_exchanged,
                           StatemData#statem_data{connection = NewConnection,
                                                  connection_state =
                                                      NewConnectionState}};
                      true ->
                          invalid_transition(Transport,
                                             S,
                                             ?FUNCTION_NAME,
                                             NextConnectionStep)
                   end
                end).

peer_properties_exchanged(enter, _OldState,
                          #statem_data{config =
                                           #configuration{connection_negotiation_step_timeout
                                                              =
                                                              StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
peer_properties_exchanged(state_timeout, close,
                          #statem_data{transport = Transport,
                                       connection =
                                           #stream_connection{socket =
                                                                  Socket}}) ->
    state_timeout(?FUNCTION_NAME, Transport, Socket);
peer_properties_exchanged(info, Msg, StateData) ->
    handle_info(Msg, StateData,
                fun(NextConnectionStep,
                    #statem_data{transport = Transport,
                                 connection = #stream_connection{socket = S}} =
                        StatemData,
                    NewConnection,
                    NewConnectionState) ->
                   if NextConnectionStep =:= authenticating ->
                          {next_state, authenticating,
                           StatemData#statem_data{connection = NewConnection,
                                                  connection_state =
                                                      NewConnectionState}};
                      true ->
                          invalid_transition(Transport,
                                             S,
                                             ?FUNCTION_NAME,
                                             NextConnectionStep)
                   end
                end).

authenticating(enter, _OldState,
               #statem_data{config =
                                #configuration{connection_negotiation_step_timeout
                                                   = StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
authenticating(state_timeout, close,
               #statem_data{transport = Transport,
                            connection =
                                #stream_connection{socket = Socket}}) ->
    state_timeout(?FUNCTION_NAME, Transport, Socket);
authenticating(info, Msg, StateData) ->
    handle_info(Msg, StateData,
                fun(NextConnectionStep,
                    #statem_data{transport = Transport,
                                 connection = #stream_connection{socket = S},
                                 config =
                                     #configuration{frame_max = FrameMax,
                                                    heartbeat = Heartbeat}} =
                        StatemData,
                    NewConnection,
                    NewConnectionState) ->
                   if NextConnectionStep =:= authenticated ->
                          Frame =
                              rabbit_stream_core:frame({tune, FrameMax,
                                                        Heartbeat}),
                          send(Transport, S, Frame),
                          {next_state, tuning,
                           StatemData#statem_data{connection =
                                                      NewConnection#stream_connection{connection_step
                                                                                          =
                                                                                          tuning},
                                                  connection_state =
                                                      NewConnectionState}};
                      true ->
                          invalid_transition(Transport,
                                             S,
                                             ?FUNCTION_NAME,
                                             NextConnectionStep)
                   end
                end).

tuning(enter, _OldState,
       #statem_data{config =
                        #configuration{connection_negotiation_step_timeout =
                                           StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
tuning(state_timeout, close,
       #statem_data{transport = Transport,
                    connection = #stream_connection{socket = Socket}}) ->
    state_timeout(?FUNCTION_NAME, Transport, Socket);
tuning(info, Msg, StateData) ->
    handle_info(Msg, StateData,
                fun(NextConnectionStep,
                    #statem_data{transport = Transport,
                                 connection = #stream_connection{socket = S},
                                 config = Configuration} =
                        StatemData,
                    NewConnection,
                    NewConnectionState) ->
                   case NextConnectionStep of
                       tuned ->
                           {next_state, tuned,
                            StatemData#statem_data{connection = NewConnection,
                                                   connection_state =
                                                       NewConnectionState}};
                       opened ->
                           transition_to_opened(Transport,
                                                Configuration,
                                                NewConnection,
                                                NewConnectionState);
                       _ ->
                           invalid_transition(Transport,
                                              S,
                                              ?FUNCTION_NAME,
                                              NextConnectionStep)
                   end
                end).

tuned(enter, _OldState,
      #statem_data{config =
                       #configuration{connection_negotiation_step_timeout =
                                          StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
tuned(state_timeout, close,
      #statem_data{transport = Transport,
                   connection = #stream_connection{socket = Socket}}) ->
    state_timeout(?FUNCTION_NAME, Transport, Socket);
tuned(info, Msg, StateData) ->
    handle_info(Msg, StateData,
                fun(NextConnectionStep,
                    #statem_data{transport = Transport,
                                 connection = #stream_connection{socket = S},
                                 config = Configuration},
                    NewConnection,
                    NewConnectionState) ->
                   if NextConnectionStep =:= opened ->
                          transition_to_opened(Transport,
                                               Configuration,
                                               NewConnection,
                                               NewConnectionState);
                      true ->
                          invalid_transition(Transport,
                                             S,
                                             ?FUNCTION_NAME,
                                             NextConnectionStep)
                   end
                end).

state_timeout(State, Transport, Socket) ->
    rabbit_log_connection:warning("Closing connection because of timeout in state '~s' likely due to lack of client action.",
                                  [State]),
    close_immediately(Transport, Socket),
    stop.

handle_info(Msg,
            #statem_data{transport = Transport,
                         connection =
                             #stream_connection{socket = S,
                                                connection_step =
                                                    PreviousConnectionStep} =
                                 Connection,
                         connection_state = State} =
                StatemData,
            Transition) ->
    {OK, Closed, Error, _Passive} = Transport:messages(),
    case Msg of
        {OK, S, Data} ->
            {Connection1, State1} =
                handle_inbound_data_pre_auth(Transport,
                                             Connection,
                                             State,
                                             Data),
            Transport:setopts(S, [{active, once}]),
            #stream_connection{connection_step = NewConnectionStep} =
                Connection1,
            rabbit_log_connection:debug("Transitioned from ~s to ~s",
                                        [PreviousConnectionStep,
                                         NewConnectionStep]),
            Transition(NewConnectionStep, StatemData, Connection1, State1);
        {Closed, S} ->
            rabbit_log_connection:warning("Stream protocol connection socket ~w closed",
                                          [S]),
            stop;
        {Error, S, Reason} ->
            rabbit_log_connection:warning("Socket error ~p [~w]", [Reason, S]),
            stop;
        {resource_alarm, IsThereAlarm} ->
            {keep_state,
             StatemData#statem_data{connection =
                                        Connection#stream_connection{resource_alarm
                                                                         =
                                                                         IsThereAlarm},
                                    connection_state =
                                        State#stream_connection_state{blocked =
                                                                          true}}};
        Unknown ->
            rabbit_log:warning("Received unknown message ~p", [Unknown]),
            close_immediately(Transport, S),
            stop
    end.

transition_to_opened(Transport,
                     Configuration,
                     NewConnection,
                     NewConnectionState) ->
    % TODO remove registration to rabbit_stream_connections
    % just meant to be able to close the connection remotely
    % should be possible once the connections are available in ctl list_connections
    pg_local:join(rabbit_stream_connections, self()),
    Connection1 =
        rabbit_event:init_stats_timer(NewConnection,
                                      #stream_connection.stats_timer),
    Connection2 = ensure_stats_timer(Connection1),
    Infos =
        augment_infos_with_user_provided_connection_name(infos(?CREATION_EVENT_KEYS,
                                                               Connection2,
                                                               NewConnectionState),
                                                         Connection2),
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    rabbit_networking:register_non_amqp_connection(self()),
    {next_state, open,
     #statem_data{transport = Transport,
                  connection = Connection2,
                  connection_state = NewConnectionState,
                  config = Configuration}}.

invalid_transition(Transport, Socket, From, To) ->
    rabbit_log_connection:warning("Closing socket ~w. Invalid transition from ~s to ~s.",
                                  [Socket, From, To]),
    close_immediately(Transport, Socket),
    stop.

resource_alarm(ConnectionPid, disk,
               {_WasAlarmSetForNode,
                IsThereAnyAlarmsForSameResourceInTheCluster, _Node}) ->
    ConnectionPid
    ! {resource_alarm, IsThereAnyAlarmsForSameResourceInTheCluster},
    ok;
resource_alarm(_ConnectionPid, _Resource, _Alert) ->
    ok.

socket_op(Sock, Fun) ->
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case Fun(Sock) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            rabbit_log_connection:warning("Error during socket operation ~p",
                                          [Reason]),
            rabbit_net:fast_close(RealSocket),
            exit(normal)
    end.

should_unblock(#stream_connection{publishers = Publishers}, _)
    when map_size(Publishers) == 0 ->
    %% always unblock a connection without publishers
    true;
should_unblock(#stream_connection{credits = Credits,
                                  resource_alarm = ResourceAlarm},
               #configuration{credits_required_for_unblocking =
                                  CreditsRequiredForUnblocking}) ->
    case {ResourceAlarm,
          has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking)}
    of
        {true, _} ->
            false;
        {false, EnoughCreditsToUnblock} ->
            EnoughCreditsToUnblock
    end.

init_credit(CreditReference, Credits) ->
    atomics:put(CreditReference, 1, Credits).

sub_credits(CreditReference, Credits) ->
    atomics:sub(CreditReference, 1, Credits).

add_credits(CreditReference, Credits) ->
    atomics:add(CreditReference, 1, Credits).

has_credits(CreditReference) ->
    atomics:get(CreditReference, 1) > 0.

has_enough_credits_to_unblock(CreditReference,
                              CreditsRequiredForUnblocking) ->
    atomics:get(CreditReference, 1) > CreditsRequiredForUnblocking.

increase_messages_consumed(Counters, Count) ->
    rabbit_global_counters:messages_delivered(stream, ?STREAM_QUEUE_TYPE,
                                              Count),
    atomics:add(Counters, 1, Count).

set_consumer_offset(Counters, Offset) ->
    atomics:put(Counters, 2, Offset).

increase_messages_received(Counters, Count) ->
    rabbit_global_counters:messages_received(stream, Count),
    rabbit_global_counters:messages_received_confirm(stream, Count),
    atomics:add(Counters, 1, Count).

increase_messages_confirmed(Counters, Count) ->
    rabbit_global_counters:messages_confirmed(stream, Count),
    atomics:add(Counters, 2, Count).

increase_messages_errored(Counters, Count) ->
    atomics:add(Counters, 3, Count).

messages_consumed(Counters) ->
    atomics:get(Counters, 1).

consumer_offset(Counters) ->
    atomics:get(Counters, 2).

messages_published(Counters) ->
    atomics:get(Counters, 1).

messages_confirmed(Counters) ->
    atomics:get(Counters, 2).

messages_errored(Counters) ->
    atomics:get(Counters, 3).

stream_stored_offset(Log) ->
    osiris_log:committed_offset(Log).

augment_infos_with_user_provided_connection_name(Infos,
                                                 #stream_connection{client_properties
                                                                        =
                                                                        ClientProperties}) ->
    case ClientProperties of
        #{<<"connection_name">> := UserProvidedConnectionName} ->
            [{user_provided_name, UserProvidedConnectionName} | Infos];
        _ ->
            Infos
    end.

close(Transport, S,
      #stream_connection_state{consumers = Consumers}) ->
    [osiris_log:close(Log)
     || #consumer{segment = Log} <- maps:values(Consumers)],
    Transport:shutdown(S, write),
    Transport:close(S).

% Do not read or write any further data from / to Socket.
% Useful to close sockets for unauthenticated clients.
close_immediately(Transport, S) ->
    Transport:shutdown(S, read),
    Transport:close(S).

open(enter, _OldState, _StateData) ->
    keep_state_and_data;
open(info, {resource_alarm, IsThereAlarm},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{socket = S,
                                         name = ConnectionName,
                                         credits = Credits,
                                         heartbeater = Heartbeater} =
                          Connection,
                  connection_state =
                      #stream_connection_state{blocked = Blocked} = State,
                  config =
                      #configuration{credits_required_for_unblocking =
                                         CreditsRequiredForUnblocking}} =
         StatemData) ->
    rabbit_log_connection:debug("Connection ~p received resource alarm. Alarm "
                                "on? ~p",
                                [ConnectionName, IsThereAlarm]),
    EnoughCreditsToUnblock =
        has_enough_credits_to_unblock(Credits, CreditsRequiredForUnblocking),
    NewBlockedState =
        case {IsThereAlarm, EnoughCreditsToUnblock} of
            {true, _} ->
                true;
            {false, EnoughCredits} ->
                not EnoughCredits
        end,
    rabbit_log_connection:debug("Connection ~p had blocked status set to ~p, new "
                                "blocked status is now ~p",
                                [ConnectionName, Blocked, NewBlockedState]),
    case {Blocked, NewBlockedState} of
        {true, false} ->
            Transport:setopts(S, [{active, once}]),
            ok = rabbit_heartbeat:resume_monitor(Heartbeater),
            rabbit_log_connection:debug("Unblocking connection ~p",
                                        [ConnectionName]);
        {false, true} ->
            ok = rabbit_heartbeat:pause_monitor(Heartbeater),
            rabbit_log_connection:debug("Blocking connection ~p after resource alarm",
                                        [ConnectionName]);
        _ ->
            ok
    end,
    {keep_state,
     StatemData#statem_data{connection =
                                Connection#stream_connection{resource_alarm =
                                                                 IsThereAlarm},
                            connection_state =
                                State#stream_connection_state{blocked =
                                                                  NewBlockedState}}};
open(info, {OK, S, Data},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{socket = S,
                                         credits = Credits,
                                         heartbeater = Heartbeater} =
                          Connection,
                  connection_state =
                      #stream_connection_state{blocked = Blocked} = State,
                  config = Configuration} =
         StatemData)
    when OK =:= tcp; OK =:= ssl ->
    {Connection1, State1} =
        handle_inbound_data_post_auth(Transport, Connection, State, Data),
    #stream_connection{connection_step = Step} = Connection1,
    case Step of
        closing ->
            close(Transport, S, State),
            stop;
        close_sent ->
            rabbit_log_connection:debug("Transitioned to close_sent"),
            Transport:setopts(S, [{active, once}]),
            {next_state, close_sent,
             StatemData#statem_data{connection = Connection1,
                                    connection_state = State1}};
        _ ->
            State2 =
                case Blocked of
                    true ->
                        case should_unblock(Connection, Configuration) of
                            true ->
                                Transport:setopts(S, [{active, once}]),
                                ok =
                                    rabbit_heartbeat:resume_monitor(Heartbeater),
                                State1#stream_connection_state{blocked = false};
                            false ->
                                State1
                        end;
                    false ->
                        case has_credits(Credits) of
                            true ->
                                Transport:setopts(S, [{active, once}]),
                                State1;
                            false ->
                                ok =
                                    rabbit_heartbeat:pause_monitor(Heartbeater),
                                State1#stream_connection_state{blocked = true}
                        end
                end,
            {keep_state,
             StatemData#statem_data{connection = Connection1,
                                    connection_state = State2}}
    end;
open(info, {Closed, Socket}, #statem_data{connection = Connection})
    when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    demonitor_all_streams(Connection),
    rabbit_log_connection:warning("Socket ~w closed [~w]",
                                  [Socket, self()]),
    stop;
open(info, {Error, Socket, Reason},
     #statem_data{connection = Connection})
    when Error =:= tcp_error; Error =:= ssl_error ->
    demonitor_all_streams(Connection),
    rabbit_log_connection:error("Socket error ~p [~w] [~w]",
                                [Reason, Socket, self()]),
    stop;
open(info, {'DOWN', MonitorRef, process, _OsirisPid, _Reason},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{socket = S, monitors = Monitors} =
                          Connection,
                  connection_state = State} =
         StatemData) ->
    {Connection1, State1} =
        case Monitors of
            #{MonitorRef := Stream} ->
                Monitors1 = maps:remove(MonitorRef, Monitors),
                C = Connection#stream_connection{monitors = Monitors1},
                case clean_state_after_stream_deletion_or_failure(Stream, C,
                                                                  State)
                of
                    {cleaned, NewConnection, NewState} ->
                        Command =
                            {metadata_update, Stream,
                             ?RESPONSE_CODE_STREAM_NOT_AVAILABLE},
                        Frame = rabbit_stream_core:frame(Command),
                        send(Transport, S, Frame),
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_NOT_AVAILABLE,
                                                                         1),
                        {NewConnection, NewState};
                    {not_cleaned, SameConnection, SameState} ->
                        {SameConnection, SameState}
                end;
            _ ->
                {Connection, State}
        end,
    {keep_state,
     StatemData#statem_data{connection = Connection1,
                            connection_state = State1}};
open(info, heartbeat_send,
     #statem_data{transport = Transport,
                  connection = #stream_connection{socket = S} = Connection,
                  connection_state = State}) ->
    Frame = rabbit_stream_core:frame(heartbeat),
    case catch send(Transport, S, Frame) of
        ok ->
            keep_state_and_data;
        Unexpected ->
            rabbit_log_connection:info("Heartbeat send error ~p, closing connection",
                                       [Unexpected]),
            _C1 = demonitor_all_streams(Connection),
            close(Transport, S, State),
            stop
    end;
open(info, heartbeat_timeout,
     #statem_data{transport = Transport,
                  connection = #stream_connection{socket = S} = Connection,
                  connection_state = State}) ->
    rabbit_log_connection:debug("Heartbeat timeout, closing connection"),
    _C1 = demonitor_all_streams(Connection),
    close(Transport, S, State),
    stop;
open(info, {infos, From},
     #statem_data{connection =
                      #stream_connection{client_properties =
                                             ClientProperties}}) ->
    From ! {self(), ClientProperties},
    keep_state_and_data;
open(info, emit_stats,
     #statem_data{connection = Connection, connection_state = State} =
         StatemData) ->
    Connection1 = emit_stats(Connection, State),
    {keep_state, StatemData#statem_data{connection = Connection1}};
open(info, Unknown, _StatemData) ->
    rabbit_log_connection:warning("Received unknown message ~p in state ~s",
                                  [Unknown, ?FUNCTION_NAME]),
    %% FIXME send close
    keep_state_and_data;
open({call, From}, info,
     #statem_data{connection = Connection, connection_state = State}) ->
    {keep_state_and_data,
     {reply, From, infos(?INFO_ITEMS, Connection, State)}};
open({call, From}, {info, Items},
     #statem_data{connection = Connection, connection_state = State}) ->
    {keep_state_and_data, {reply, From, infos(Items, Connection, State)}};
open({call, From}, {consumers_info, Items},
     #statem_data{connection_state = State}) ->
    {keep_state_and_data, {reply, From, consumers_infos(Items, State)}};
open({call, From}, {publishers_info, Items},
     #statem_data{connection = Connection}) ->
    {keep_state_and_data,
     {reply, From, publishers_infos(Items, Connection)}};
open({call, From}, {shutdown, Explanation},
     #statem_data{transport = Transport,
                  connection = #stream_connection{socket = S} = Connection,
                  connection_state = State}) ->
    % likely closing call from the management plugin
    rabbit_log_connection:info("Forcing stream connection ~p closing: ~p",
                               [self(), Explanation]),
    demonitor_all_streams(Connection),
    close(Transport, S, State),
    {stop_and_reply, normal, {reply, From, ok}};
open(cast,
     {queue_event, _, {osiris_written, _, undefined, CorrelationList}},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{socket = S,
                                         credits = Credits,
                                         heartbeater = Heartbeater,
                                         publishers = Publishers} =
                          Connection,
                  connection_state =
                      #stream_connection_state{blocked = Blocked} = State,
                  config = Configuration} =
         StatemData) ->
    ByPublisher =
        lists:foldr(fun({PublisherId, PublishingId}, Acc) ->
                       case maps:is_key(PublisherId, Publishers) of
                           true ->
                               case maps:get(PublisherId, Acc, undefined) of
                                   undefined ->
                                       Acc#{PublisherId => [PublishingId]};
                                   Ids ->
                                       Acc#{PublisherId => [PublishingId | Ids]}
                               end;
                           false ->
                               Acc
                       end
                    end,
                    #{}, CorrelationList),
    _ = maps:map(fun(PublisherId, PublishingIds) ->
                    Command = {publish_confirm, PublisherId, PublishingIds},
                    send(Transport, S, rabbit_stream_core:frame(Command)),
                    #{PublisherId := #publisher{message_counters = Cnt}} =
                        Publishers,
                    increase_messages_confirmed(Cnt, length(PublishingIds))
                 end,
                 ByPublisher),
    CorrelationIdCount = length(CorrelationList),
    add_credits(Credits, CorrelationIdCount),
    State1 =
        case Blocked of
            true ->
                case should_unblock(Connection, Configuration) of
                    true ->
                        Transport:setopts(S, [{active, once}]),
                        ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                        State#stream_connection_state{blocked = false};
                    false ->
                        State
                end;
            false ->
                State
        end,
    {keep_state, StatemData#statem_data{connection_state = State1}};
open(cast,
     {queue_event, _QueueResource,
      {osiris_written,
       #resource{name = Stream},
       PublisherReference,
       CorrelationList}},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{socket = S,
                                         credits = Credits,
                                         heartbeater = Heartbeater,
                                         publishers = Publishers,
                                         publisher_to_ids = PublisherRefToIds} =
                          Connection,
                  connection_state =
                      #stream_connection_state{blocked = Blocked} = State,
                  config = Configuration} =
         StatemData) ->
    PublishingIdCount = length(CorrelationList),
    case maps:get({Stream, PublisherReference}, PublisherRefToIds,
                  undefined)
    of
        undefined ->
            ok;
        PublisherId ->
            Command = {publish_confirm, PublisherId, CorrelationList},
            send(Transport, S, rabbit_stream_core:frame(Command)),
            #{PublisherId := #publisher{message_counters = Counters}} =
                Publishers,
            increase_messages_confirmed(Counters, PublishingIdCount)
    end,
    add_credits(Credits, PublishingIdCount),
    State1 =
        case Blocked of
            true ->
                case should_unblock(Connection, Configuration) of
                    true ->
                        Transport:setopts(S, [{active, once}]),
                        ok = rabbit_heartbeat:resume_monitor(Heartbeater),
                        State#stream_connection_state{blocked = false};
                    false ->
                        State
                end;
            false ->
                State
        end,
    {keep_state, StatemData#statem_data{connection_state = State1}};
open(cast,
     {queue_event, #resource{name = StreamName},
      {osiris_offset, _QueueResource, -1}},
     _StatemData) ->
    rabbit_log:debug("Stream protocol connection received osiris offset event for ~p with offset ~p",
                     [StreamName, -1]),
    keep_state_and_data;
open(cast,
     {queue_event, #resource{name = StreamName},
      {osiris_offset, _QueueResource, Offset}},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{stream_subscriptions =
                                             StreamSubscriptions,
                                         send_file_oct = SendFileOct} =
                          Connection,
                  connection_state =
                      #stream_connection_state{consumers = Consumers} = State} =
         StatemData)
    when Offset > -1 ->
    {Connection1, State1} =
        case maps:get(StreamName, StreamSubscriptions, undefined) of
            undefined ->
                rabbit_log:debug("Stream protocol connection: osiris offset event for ~p, but no subscription (leftover messages after unsubscribe?)",
                                 [StreamName]),
                {Connection, State};
            [] ->
                rabbit_log:debug("Stream protocol connection: osiris offset event for ~p, but no registered consumers!",
                                 [StreamName]),
                {Connection#stream_connection{stream_subscriptions =
                                                  maps:remove(StreamName,
                                                              StreamSubscriptions)},
                 State};
            CorrelationIds when is_list(CorrelationIds) ->
                Consumers1 =
                    lists:foldl(fun(CorrelationId, ConsumersAcc) ->
                                   #{CorrelationId := Consumer} = ConsumersAcc,
                                   #consumer{credit = Credit} = Consumer,
                                   Consumer1 =
                                       case Credit of
                                           0 ->
                                               Consumer;
                                           _ ->
                                               case send_chunks(Transport,
                                                                Consumer,
                                                                SendFileOct)
                                               of
                                                   {error, closed} ->
                                                       rabbit_log_connection:info("Stream protocol connection has been closed by peer", []),
                                                       throw({stop, normal});
                                                   {error, Reason} ->
                                                       rabbit_log_connection:info("Error while sending chunks: ~p",
                                                                                  [Reason]),
                                                       %% likely a connection problem
                                                       Consumer;
                                                   {{segment, Segment1},
                                                    {credit, Credit1}} ->
                                                       Consumer#consumer{segment
                                                                             =
                                                                             Segment1,
                                                                         credit
                                                                             =
                                                                             Credit1}
                                               end
                                       end,
                                   ConsumersAcc#{CorrelationId => Consumer1}
                                end,
                                Consumers, CorrelationIds),
                {Connection,
                 State#stream_connection_state{consumers = Consumers1}}
        end,
    {keep_state,
     StatemData#statem_data{connection = Connection1,
                            connection_state = State1}};
open(cast, {force_event_refresh, Ref},
     #statem_data{connection = Connection, connection_state = State} =
         StatemData) ->
    Infos =
        augment_infos_with_user_provided_connection_name(infos(?CREATION_EVENT_KEYS,
                                                               Connection,
                                                               State),
                                                         Connection),
    rabbit_event:notify(connection_created, Infos, Ref),
    Connection1 =
        rabbit_event:init_stats_timer(Connection,
                                      #stream_connection.stats_timer),
    Connection2 = ensure_stats_timer(Connection1),
    {keep_state, StatemData#statem_data{connection = Connection2}}.

close_sent(enter, _OldState,
           #statem_data{config =
                            #configuration{connection_negotiation_step_timeout =
                                               StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
close_sent(state_timeout, close,
           #statem_data{transport = Transport,
                        connection = #stream_connection{socket = Socket},
                        connection_state = State}) ->
    rabbit_log_connection:warning("Closing connection because of timeout in state '~s' likely due to lack of client action.",
                                  [?FUNCTION_NAME]),
    close(Transport, Socket, State),
    stop;
close_sent(info, {tcp, S, Data},
           #statem_data{transport = Transport,
                        connection = Connection,
                        connection_state = State} =
               StatemData)
    when byte_size(Data) > 1 ->
    {Connection1, State1} =
        handle_inbound_data_post_close(Transport, Connection, State, Data),
    #stream_connection{connection_step = Step} = Connection1,
    rabbit_log_connection:debug("Stream reader has transitioned from ~s to ~s",
                                [?FUNCTION_NAME, Step]),
    case Step of
        closing_done ->
            close(Transport, S, State1),
            stop;
        _ ->
            Transport:setopts(S, [{active, once}]),
            {keep_state,
             StatemData#statem_data{connection = Connection1,
                                    connection_state = State1}}
    end;
close_sent(info, {tcp_closed, S}, _StatemData) ->
    rabbit_log_connection:debug("Stream protocol connection socket ~w closed [~w]",
                                [S, self()]),
    stop;
close_sent(info, {tcp_error, S, Reason},
           #statem_data{transport = Transport, connection_state = State}) ->
    rabbit_log_connection:error("Stream protocol connection socket error: ~p [~w] [~w]",
                                [Reason, S, self()]),
    close(Transport, S, State),
    stop;
close_sent(info, {resource_alarm, IsThereAlarm},
           StatemData = #statem_data{connection = Connection}) ->
    rabbit_log:warning("Stream protocol connection ignored a resource alarm ~p in state ~s",
                       [IsThereAlarm, ?FUNCTION_NAME]),
    {keep_state,
     StatemData#statem_data{connection =
                                Connection#stream_connection{resource_alarm =
                                                                 IsThereAlarm}}};
close_sent(info, Msg, _StatemData) ->
    rabbit_log_connection:warning("Ignored unknown message ~p in state ~s",
                                  [Msg, ?FUNCTION_NAME]),
    keep_state_and_data.

handle_inbound_data_pre_auth(Transport, Connection, State, Data) ->
    handle_inbound_data(Transport,
                        Connection,
                        State,
                        Data,
                        fun handle_frame_pre_auth/4).

handle_inbound_data_post_auth(Transport, Connection, State, Data) ->
    handle_inbound_data(Transport,
                        Connection,
                        State,
                        Data,
                        fun handle_frame_post_auth/4).

handle_inbound_data_post_close(Transport, Connection, State, Data) ->
    handle_inbound_data(Transport,
                        Connection,
                        State,
                        Data,
                        fun handle_frame_post_close/4).

handle_inbound_data(Transport,
                    Connection,
                    #stream_connection_state{data = CoreState0} = State,
                    Data,
                    HandleFrameFun) ->
    CoreState1 = rabbit_stream_core:incoming_data(Data, CoreState0),
    {Commands, CoreState} = rabbit_stream_core:all_commands(CoreState1),
    lists:foldl(fun(Command, {C, S}) ->
                   HandleFrameFun(Transport, C, S, Command)
                end,
                {Connection, State#stream_connection_state{data = CoreState}},
                Commands).

publishing_ids_from_messages(<<>>) ->
    [];
publishing_ids_from_messages(<<PublishingId:64,
                               0:1,
                               MessageSize:31,
                               _Message:MessageSize/binary,
                               Rest/binary>>) ->
    [PublishingId | publishing_ids_from_messages(Rest)];
publishing_ids_from_messages(<<PublishingId:64,
                               1:1,
                               _CompressionType:3,
                               _Unused:4,
                               _MessageCount:16,
                               _UncompressedSize:32,
                               BatchSize:32,
                               _Batch:BatchSize/binary,
                               Rest/binary>>) ->
    [PublishingId | publishing_ids_from_messages(Rest)].

handle_frame_pre_auth(Transport,
                      #stream_connection{socket = S} = Connection,
                      State,
                      {request, CorrelationId,
                       {peer_properties, ClientProperties}}) ->
    {ok, Product} = application:get_key(rabbit, description),
    {ok, Version} = application:get_key(rabbit, vsn),

    %% Get any configuration-specified server properties
    RawConfigServerProps =
        application:get_env(rabbit, server_properties, []),

    ConfigServerProperties =
        lists:foldl(fun({K, V}, Acc) ->
                       maps:put(
                           rabbit_data_coercion:to_binary(K),
                           rabbit_data_coercion:to_binary(V), Acc)
                    end,
                    #{}, RawConfigServerProps),

    ServerProperties0 =
        maps:merge(ConfigServerProperties,
                   #{<<"product">> => Product,
                     <<"version">> => Version,
                     <<"cluster_name">> => rabbit_nodes:cluster_name(),
                     <<"platform">> => rabbit_misc:platform_and_version(),
                     <<"copyright">> => ?COPYRIGHT_MESSAGE,
                     <<"information">> => ?INFORMATION_MESSAGE}),
    ServerProperties =
        maps:map(fun(_, V) -> rabbit_data_coercion:to_binary(V) end,
                 ServerProperties0),
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {peer_properties, ?RESPONSE_CODE_OK,
                                   ServerProperties}}),
    send(Transport, S, Frame),
    {Connection#stream_connection{client_properties = ClientProperties,
                                  authentication_state =
                                      peer_properties_exchanged,
                                  connection_step = peer_properties_exchanged},
     State};
handle_frame_pre_auth(Transport,
                      #stream_connection{socket = S} = Connection,
                      State,
                      {request, CorrelationId, sasl_handshake}) ->
    Mechanisms = rabbit_stream_utils:auth_mechanisms(S),
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {sasl_handshake, ?RESPONSE_CODE_OK,
                                   Mechanisms}}),
    send(Transport, S, Frame),
    {Connection#stream_connection{connection_step = authenticating},
     State};
handle_frame_pre_auth(Transport,
                      #stream_connection{socket = S,
                                         authentication_state = AuthState0,
                                         host = Host} =
                          Connection0,
                      State,
                      {request, CorrelationId,
                       {sasl_authenticate, Mechanism, SaslBin}}) ->
    Connection1 =
        case rabbit_stream_utils:auth_mechanism_to_module(Mechanism, S) of
            {ok, AuthMechanism} ->
                AuthState =
                    case AuthState0 of
                        none ->
                            AuthMechanism:init(S);
                        AS ->
                            AS
                    end,
                RemoteAddress = list_to_binary(inet:ntoa(Host)),
                C1 = Connection0#stream_connection{auth_mechanism =
                                                       {Mechanism,
                                                        AuthMechanism}},
                {C2, CmdBody} =
                    case AuthMechanism:handle_response(SaslBin, AuthState) of
                        {refused, Username, Msg, Args} ->
                            rabbit_core_metrics:auth_attempt_failed(RemoteAddress,
                                                                    Username,
                                                                    stream),
                            auth_fail(Username, Msg, Args, C1, State),
                            rabbit_log_connection:warning(Msg, Args),
                            {C1#stream_connection{connection_step = failure},
                             {sasl_authenticate,
                              ?RESPONSE_AUTHENTICATION_FAILURE, <<>>}};
                        {protocol_error, Msg, Args} ->
                            rabbit_core_metrics:auth_attempt_failed(RemoteAddress,
                                                                    <<>>,
                                                                    stream),
                            notify_auth_result(none,
                                               user_authentication_failure,
                                               [{error,
                                                 rabbit_misc:format(Msg,
                                                                    Args)}],
                                               C1,
                                               State),
                            rabbit_log_connection:warning(Msg, Args),
                            {C1#stream_connection{connection_step = failure},
                             {sasl_authenticate, ?RESPONSE_SASL_ERROR, <<>>}};
                        {challenge, Challenge, AuthState1} ->
                            rabbit_core_metrics:auth_attempt_succeeded(RemoteAddress,
                                                                       <<>>,
                                                                       stream),
                            {C1#stream_connection{authentication_state =
                                                      AuthState1,
                                                  connection_step =
                                                      authenticating},
                             {sasl_authenticate, ?RESPONSE_SASL_CHALLENGE,
                              Challenge}};
                        {ok, User = #user{username = Username}} ->
                            case
                                rabbit_access_control:check_user_loopback(Username,
                                                                          S)
                            of
                                ok ->
                                    rabbit_core_metrics:auth_attempt_succeeded(RemoteAddress,
                                                                               Username,
                                                                               stream),
                                    notify_auth_result(Username,
                                                       user_authentication_success,
                                                       [],
                                                       C1,
                                                       State),
                                    {C1#stream_connection{authentication_state =
                                                              done,
                                                          user = User,
                                                          connection_step =
                                                              authenticated},
                                     {sasl_authenticate, ?RESPONSE_CODE_OK,
                                      <<>>}};
                                not_allowed ->
                                    rabbit_core_metrics:auth_attempt_failed(RemoteAddress,
                                                                            Username,
                                                                            stream),
                                    rabbit_log_connection:warning("User '~s' can only connect via localhost",
                                                                  [Username]),
                                    {C1#stream_connection{connection_step =
                                                              failure},
                                     {sasl_authenticate,
                                      ?RESPONSE_SASL_AUTHENTICATION_FAILURE_LOOPBACK,
                                      <<>>}}
                            end
                    end,
                Frame =
                    rabbit_stream_core:frame({response, CorrelationId,
                                              CmdBody}),
                send(Transport, S, Frame),
                C2;
            {error, _} ->
                CmdBody =
                    {sasl_authenticate, ?RESPONSE_SASL_MECHANISM_NOT_SUPPORTED,
                     <<>>},
                Frame =
                    rabbit_stream_core:frame({response, CorrelationId,
                                              CmdBody}),
                send(Transport, S, Frame),
                Connection0#stream_connection{connection_step = failure}
        end,

    {Connection1, State};
handle_frame_pre_auth(Transport,
                      Connection,
                      State,
                      {response, _, {tune, _, _} = Tune}) ->
    ?FUNCTION_NAME(Transport, Connection, State, Tune);
handle_frame_pre_auth(_Transport,
                      #stream_connection{helper_sup = SupPid,
                                         socket = Sock,
                                         name = ConnectionName} =
                          Connection,
                      #stream_connection_state{blocked = Blocked} = State,
                      {tune, FrameMax, Heartbeat}) ->
    rabbit_log_connection:debug("Tuning response ~p ~p ",
                                [FrameMax, Heartbeat]),
    Parent = self(),
    %% sending a message to the main process so the heartbeat frame is sent from this main process
    %% otherwise heartbeat frames can interleave with chunk delivery
    %% (chunk delivery is made of 2 calls on the socket, one for the header and one send_file for the chunk,
    %% we don't want a heartbeat frame to sneak in in-between)
    SendFun =
        fun() ->
           Parent ! heartbeat_send,
           ok
        end,
    ReceiveFun = fun() -> Parent ! heartbeat_timeout end,
    Heartbeater =
        rabbit_heartbeat:start(SupPid,
                               Sock,
                               ConnectionName,
                               Heartbeat,
                               SendFun,
                               Heartbeat,
                               ReceiveFun),
    case Blocked of
        true ->
            ok = rabbit_heartbeat:pause_monitor(Heartbeater);
        _ ->
            ok
    end,
    {Connection#stream_connection{connection_step = tuned,
                                  frame_max = FrameMax,
                                  heartbeat = Heartbeat,
                                  heartbeater = Heartbeater},
     State};
handle_frame_pre_auth(Transport,
                      #stream_connection{user = User,
                                         socket = S,
                                         transport = TransportLayer} =
                          Connection,
                      State,
                      {request, CorrelationId, {open, VirtualHost}}) ->
    %% FIXME enforce connection limit (see rabbit_reader:is_over_connection_limit/2)
    rabbit_log:debug("Open frame received for ~s", [VirtualHost]),
    Connection1 =
        try
            rabbit_access_control:check_vhost_access(User,
                                                     VirtualHost,
                                                     {socket, S},
                                                     #{}),
            AdvertisedHost = rabbit_stream:host(),
            AdvertisedPort =
                case TransportLayer of
                    tcp ->
                        rabbit_data_coercion:to_binary(
                            rabbit_stream:port());
                    ssl ->
                        rabbit_data_coercion:to_binary(
                            rabbit_stream:tls_port())
                end,

            ConnectionProperties =
                #{<<"advertised_host">> => AdvertisedHost,
                  <<"advertised_port">> => AdvertisedPort},

            rabbit_log:debug("sending open response ok ~s", [VirtualHost]),
            Frame =
                rabbit_stream_core:frame({response, CorrelationId,
                                          {open, ?RESPONSE_CODE_OK,
                                           ConnectionProperties}}),

            send(Transport, S, Frame),
            %% FIXME check if vhost is alive (see rabbit_reader:is_vhost_alive/2)
            Connection#stream_connection{connection_step = opened,
                                         virtual_host = VirtualHost}
        catch
            exit:_ ->
                F = rabbit_stream_core:frame({response, CorrelationId,
                                              {open,
                                               ?RESPONSE_VHOST_ACCESS_FAILURE,
                                               #{}}}),
                send(Transport, S, F),
                Connection#stream_connection{connection_step = failure}
        end,

    {Connection1, State};
handle_frame_pre_auth(_Transport, Connection, State, heartbeat) ->
    rabbit_log:debug("Received heartbeat frame pre auth"),
    {Connection, State};
handle_frame_pre_auth(_Transport, Connection, State, Command) ->
    rabbit_log_connection:warning("unknown command ~w, closing connection.",
                                  [Command]),
    {Connection#stream_connection{connection_step = failure}, State}.

auth_fail(Username, Msg, Args, Connection, ConnectionState) ->
    notify_auth_result(Username,
                       user_authentication_failure,
                       [{error, rabbit_misc:format(Msg, Args)}],
                       Connection,
                       ConnectionState).

notify_auth_result(Username,
                   AuthResult,
                   ExtraProps,
                   Connection,
                   ConnectionState) ->
    EventProps =
        [{connection_type, network},
         {name,
          case Username of
              none ->
                  '';
              _ ->
                  Username
          end}]
        ++ [case Item of
                name ->
                    {connection_name, i(name, Connection, ConnectionState)};
                _ ->
                    {Item, i(Item, Connection, ConnectionState)}
            end
            || Item <- ?AUTH_NOTIFICATION_INFO_KEYS]
        ++ ExtraProps,
    rabbit_event:notify(AuthResult,
                        [P || {_, V} = P <- EventProps, V =/= '']).

handle_frame_post_auth(Transport,
                       #stream_connection{resource_alarm = true} = Connection0,
                       State,
                       {request, CorrelationId,
                        {declare_publisher,
                         PublisherId,
                         _WriterRef,
                         Stream}}) ->
    rabbit_log_connection:info("Cannot create publisher ~p on stream ~p, connection "
                               "is blocked because of resource alarm",
                               [PublisherId, Stream]),
    response(Transport,
             Connection0,
             declare_publisher,
             CorrelationId,
             ?RESPONSE_CODE_PRECONDITION_FAILED),
    rabbit_global_counters:increase_protocol_counter(stream,
                                                     ?PRECONDITION_FAILED, 1),
    {Connection0, State};
handle_frame_post_auth(Transport,
                       #stream_connection{user = User,
                                          publishers = Publishers0,
                                          publisher_to_ids = RefIds0,
                                          resource_alarm = false} =
                           Connection0,
                       State,
                       {request, CorrelationId,
                        {declare_publisher, PublisherId, WriterRef, Stream}}) ->
    case rabbit_stream_utils:check_write_permitted(stream_r(Stream,
                                                            Connection0),
                                                   User, #{})
    of
        ok ->
            case {maps:is_key(PublisherId, Publishers0),
                  maps:is_key({Stream, WriterRef}, RefIds0)}
            of
                {false, false} ->
                    case lookup_leader(Stream, Connection0) of
                        cluster_not_found ->
                            response(Transport,
                                     Connection0,
                                     declare_publisher,
                                     CorrelationId,
                                     ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?STREAM_DOES_NOT_EXIST,
                                                                             1),
                            {Connection0, State};
                        {ClusterLeader,
                         #stream_connection{publishers = Publishers0,
                                            publisher_to_ids = RefIds0} =
                             Connection1} ->
                            {PublisherReference, RefIds1} =
                                case WriterRef of
                                    <<"">> ->
                                        {undefined, RefIds0};
                                    _ ->
                                        {WriterRef,
                                         RefIds0#{{Stream, WriterRef} =>
                                                      PublisherId}}
                                end,
                            Publisher =
                                #publisher{publisher_id = PublisherId,
                                           stream = Stream,
                                           reference = PublisherReference,
                                           leader = ClusterLeader,
                                           message_counters =
                                               atomics:new(3,
                                                           [{signed, false}])},
                            response(Transport,
                                     Connection0,
                                     declare_publisher,
                                     CorrelationId,
                                     ?RESPONSE_CODE_OK),
                            rabbit_stream_metrics:publisher_created(self(),
                                                                    stream_r(Stream,
                                                                             Connection1),
                                                                    PublisherId,
                                                                    PublisherReference),
                            {Connection1#stream_connection{publishers =
                                                               Publishers0#{PublisherId
                                                                                =>
                                                                                Publisher},
                                                           publisher_to_ids =
                                                               RefIds1},
                             State}
                    end;
                {_, _} ->
                    response(Transport,
                             Connection0,
                             declare_publisher,
                             CorrelationId,
                             ?RESPONSE_CODE_PRECONDITION_FAILED),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?PRECONDITION_FAILED,
                                                                     1),
                    {Connection0, State}
            end;
        error ->
            response(Transport,
                     Connection0,
                     declare_publisher,
                     CorrelationId,
                     ?RESPONSE_CODE_ACCESS_REFUSED),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?ACCESS_REFUSED,
                                                             1),
            {Connection0, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          credits = Credits,
                                          virtual_host = VirtualHost,
                                          user = User,
                                          publishers = Publishers} =
                           Connection,
                       State,
                       {publish, PublisherId, MessageCount, Messages}) ->
    case Publishers of
        #{PublisherId := Publisher} ->
            #publisher{stream = Stream,
                       reference = Reference,
                       leader = Leader,
                       message_counters = Counters} =
                Publisher,
            increase_messages_received(Counters, MessageCount),
            case rabbit_stream_utils:check_write_permitted(#resource{name =
                                                                         Stream,
                                                                     kind =
                                                                         queue,
                                                                     virtual_host
                                                                         =
                                                                         VirtualHost},
                                                           User, #{})
            of
                ok ->
                    rabbit_stream_utils:write_messages(Leader,
                                                       Reference,
                                                       PublisherId,
                                                       Messages),
                    sub_credits(Credits, MessageCount),
                    {Connection, State};
                error ->
                    PublishingIds = publishing_ids_from_messages(Messages),
                    Command =
                        {publish_error,
                         PublisherId,
                         ?RESPONSE_CODE_ACCESS_REFUSED,
                         PublishingIds},
                    Frame = rabbit_stream_core:frame(Command),
                    send(Transport, S, Frame),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?ACCESS_REFUSED,
                                                                     1),
                    increase_messages_errored(Counters, MessageCount),
                    {Connection, State}
            end;
        _ ->
            PublishingIds = publishing_ids_from_messages(Messages),
            Command =
                {publish_error,
                 PublisherId,
                 ?RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST,
                 PublishingIds},
            Frame = rabbit_stream_core:frame(Command),
            send(Transport, S, Frame),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?PUBLISHER_DOES_NOT_EXIST,
                                                             1),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost,
                                          user = User} =
                           Connection,
                       State,
                       {request, CorrelationId,
                        {query_publisher_sequence, Reference, Stream}}) ->
    % FrameSize = ?RESPONSE_FRAME_SIZE + 8,
    {ResponseCode, Sequence} =
        case rabbit_stream_utils:check_read_permitted(#resource{name = Stream,
                                                                kind = queue,
                                                                virtual_host =
                                                                    VirtualHost},
                                                      User, #{})
        of
            ok ->
                case rabbit_stream_manager:lookup_local_member(VirtualHost,
                                                               Stream)
                of
                    {error, not_found} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_DOES_NOT_EXIST,
                                                                         1),
                        {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 0};
                    {ok, LocalMemberPid} ->
                        {?RESPONSE_CODE_OK,
                         case osiris:fetch_writer_seq(LocalMemberPid, Reference)
                         of
                             undefined ->
                                 0;
                             Offt ->
                                 Offt
                         end}
                end;
            error ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?ACCESS_REFUSED,
                                                                 1),
                {?RESPONSE_CODE_ACCESS_REFUSED, 0}
        end,
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {query_publisher_sequence, ResponseCode,
                                   Sequence}}),
    send(Transport, S, Frame),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{publishers = Publishers,
                                          publisher_to_ids = PubToIds} =
                           Connection0,
                       State,
                       {request, CorrelationId,
                        {delete_publisher, PublisherId}}) ->
    case Publishers of
        #{PublisherId := #publisher{stream = Stream, reference = Ref}} ->
            Connection1 =
                Connection0#stream_connection{publishers =
                                                  maps:remove(PublisherId,
                                                              Publishers),
                                              publisher_to_ids =
                                                  maps:remove({Stream, Ref},
                                                              PubToIds)},
            Connection2 =
                maybe_clean_connection_from_stream(Stream, Connection1),
            response(Transport,
                     Connection1,
                     delete_publisher,
                     CorrelationId,
                     ?RESPONSE_CODE_OK),
            rabbit_stream_metrics:publisher_deleted(self(),
                                                    stream_r(Stream,
                                                             Connection2),
                                                    PublisherId),
            {Connection2, State};
        _ ->
            response(Transport,
                     Connection0,
                     delete_publisher,
                     CorrelationId,
                     ?RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?PUBLISHER_DOES_NOT_EXIST,
                                                             1),
            {Connection0, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = Socket,
                                          stream_subscriptions =
                                              StreamSubscriptions,
                                          virtual_host = VirtualHost,
                                          user = User,
                                          send_file_oct = SendFileOct,
                                          transport = ConnTransport} =
                           Connection,
                       #stream_connection_state{consumers = Consumers} = State,
                       {request, CorrelationId,
                        {subscribe,
                         SubscriptionId,
                         Stream,
                         OffsetSpec,
                         Credit,
                         Properties}}) ->
    QueueResource =
        #resource{name = Stream,
                  kind = queue,
                  virtual_host = VirtualHost},
    %% FIXME check the max number of subs is not reached already
    case rabbit_stream_utils:check_read_permitted(QueueResource, User,
                                                  #{})
    of
        ok ->
            case rabbit_stream_manager:lookup_local_member(VirtualHost, Stream)
            of
                {error, not_available} ->
                    response(Transport,
                             Connection,
                             subscribe,
                             CorrelationId,
                             ?RESPONSE_CODE_STREAM_NOT_AVAILABLE),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?STREAM_NOT_AVAILABLE,
                                                                     1),
                    {Connection, State};
                {error, not_found} ->
                    response(Transport,
                             Connection,
                             subscribe,
                             CorrelationId,
                             ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?STREAM_DOES_NOT_EXIST,
                                                                     1),
                    {Connection, State};
                {ok, LocalMemberPid} ->
                    case subscription_exists(StreamSubscriptions,
                                             SubscriptionId)
                    of
                        true ->
                            response(Transport,
                                     Connection,
                                     subscribe,
                                     CorrelationId,
                                     ?RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?SUBSCRIPTION_ID_ALREADY_EXISTS,
                                                                             1),
                            {Connection, State};
                        false ->
                            rabbit_log:debug("Creating subscription ~p to ~p, with offset specificat"
                                             "ion ~p, properties ~0p",
                                             [SubscriptionId,
                                              Stream,
                                              OffsetSpec,
                                              Properties]),
                            CounterSpec =
                                {{?MODULE,
                                  QueueResource,
                                  SubscriptionId,
                                  self()},
                                 []},
                            Options =
                                #{transport => ConnTransport,
                                  chunk_selector =>
                                      get_chunk_selector(Properties)},
                            {ok, Segment} =
                                osiris:init_reader(LocalMemberPid,
                                                   OffsetSpec,
                                                   CounterSpec,
                                                   Options),
                            rabbit_log:debug("Next offset for subscription ~p is ~p",
                                             [SubscriptionId,
                                              osiris_log:next_offset(Segment)]),
                            ConsumerCounters =
                                atomics:new(2, [{signed, false}]),
                            ConsumerState =
                                #consumer{member_pid = LocalMemberPid,
                                          offset = OffsetSpec,
                                          subscription_id = SubscriptionId,
                                          socket = Socket,
                                          segment = Segment,
                                          credit = Credit,
                                          stream = Stream,
                                          counters = ConsumerCounters,
                                          properties = Properties},

                            Connection1 =
                                maybe_monitor_stream(LocalMemberPid, Stream,
                                                     Connection),

                            response_ok(Transport,
                                        Connection,
                                        subscribe,
                                        CorrelationId),

                            rabbit_log:debug("Distributing existing messages to subscription ~p",
                                             [SubscriptionId]),

                            case send_chunks(Transport, ConsumerState, SendFileOct) of
                                {error, closed} ->
                                    rabbit_log_connection:info("Stream protocol connection has been closed by peer", []),
                                    throw({stop, normal});
                                {{segment, Segment1}, {credit, Credit1}} ->
                                    ConsumerState1 =
                                    ConsumerState#consumer{segment = Segment1,
                                                           credit = Credit1},
                                    Consumers1 =
                                    Consumers#{SubscriptionId => ConsumerState1},

                                    StreamSubscriptions1 =
                                    case StreamSubscriptions of
                                        #{Stream := SubscriptionIds} ->
                                            StreamSubscriptions#{Stream =>
                                                                 [SubscriptionId]
                                                                 ++ SubscriptionIds};
                                        _ ->
                                            StreamSubscriptions#{Stream =>
                                                                 [SubscriptionId]}
                                    end,

                                    #consumer{counters = ConsumerCounters1} =
                                    ConsumerState1,

                                    ConsumerOffset = osiris_log:next_offset(Segment1),
                                    ConsumerOffsetLag =
                                    consumer_i(offset_lag, ConsumerState1),

                                    rabbit_log:debug("Subscription ~p is now at offset ~p with ~p message(s) "
                                                     "distributed after subscription",
                                                     [SubscriptionId, ConsumerOffset,
                                                      messages_consumed(ConsumerCounters1)]),

                                    rabbit_stream_metrics:consumer_created(self(),
                                                                           stream_r(Stream,
                                                                                    Connection1),
                                                                           SubscriptionId,
                                                                           Credit1,
                                                                           messages_consumed(ConsumerCounters1),
                                                                           ConsumerOffset,
                                                                           ConsumerOffsetLag,
                                                                           Properties),
                                    {Connection1#stream_connection{stream_subscriptions
                                                                   =
                                                                   StreamSubscriptions1},
                                     State#stream_connection_state{consumers =
                                                                   Consumers1}}
                            end
                    end
            end;
        error ->
            response(Transport,
                     Connection,
                     subscribe,
                     CorrelationId,
                     ?RESPONSE_CODE_ACCESS_REFUSED),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?ACCESS_REFUSED,
                                                             1),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          send_file_oct = SendFileOct} =
                           Connection,
                       #stream_connection_state{consumers = Consumers} = State,
                       {credit, SubscriptionId, Credit}) ->
    case Consumers of
        #{SubscriptionId := Consumer} ->
            #consumer{credit = AvailableCredit} = Consumer,
            case send_chunks(Transport,
                             Consumer,
                             AvailableCredit + Credit,
                             SendFileOct)
            of
                {error, closed} ->
                    rabbit_log_connection:info("Stream protocol connection has been closed by peer", []),
                    throw({stop, normal});
                {{segment, Segment1}, {credit, Credit1}} ->
                    Consumer1 =
                        Consumer#consumer{segment = Segment1, credit = Credit1},
                    {Connection,
                     State#stream_connection_state{consumers =
                                                       Consumers#{SubscriptionId
                                                                      =>
                                                                      Consumer1}}}
            end;
        _ ->
            rabbit_log:warning("Giving credit to unknown subscription: ~p",
                               [SubscriptionId]),

            Code = ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST,
            Frame =
                rabbit_stream_core:frame({response, 1,
                                          {credit, Code, SubscriptionId}}),
            send(Transport, S, Frame),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?SUBSCRIPTION_ID_DOES_NOT_EXIST,
                                                             1),
            {Connection, State}
    end;
handle_frame_post_auth(_Transport,
                       #stream_connection{virtual_host = VirtualHost,
                                          user = User} =
                           Connection,
                       State,
                       {store_offset, Reference, Stream, Offset}) ->
    case rabbit_stream_utils:check_write_permitted(#resource{name =
                                                                 Stream,
                                                             kind = queue,
                                                             virtual_host =
                                                                 VirtualHost},
                                                   User, #{})
    of
        ok ->
            case lookup_leader(Stream, Connection) of
                cluster_not_found ->
                    rabbit_log:warning("Could not find leader to store offset on ~p",
                                       [Stream]),
                    %% FIXME store offset is fire-and-forget, so no response even if error, change this?
                    {Connection, State};
                {ClusterLeader, Connection1} ->
                    osiris:write_tracking(ClusterLeader, Reference, Offset),
                    {Connection1, State}
            end;
        error ->
            %% FIXME store offset is fire-and-forget, so no response even if error, change this?
            rabbit_log:warning("Not authorized to store offset on stream ~p",
                               [Stream]),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost,
                                          user = User} =
                           Connection0,
                       State,
                       {request, CorrelationId,
                        {query_offset, Reference, Stream}}) ->
    {ResponseCode, Offset, Connection1} =
        case rabbit_stream_utils:check_read_permitted(#resource{name = Stream,
                                                                kind = queue,
                                                                virtual_host =
                                                                    VirtualHost},
                                                      User, #{})
        of
            ok ->
                case lookup_leader(Stream, Connection0) of
                    cluster_not_found ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_DOES_NOT_EXIST,
                                                                         1),
                        {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 0, Connection0};
                    {LeaderPid, C} ->
                        {?RESPONSE_CODE_OK,
                         case osiris:read_tracking(LeaderPid, Reference) of
                             undefined ->
                                 0;
                             {offset, Offt} ->
                                 Offt
                         end,
                         C}
                end;
            error ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?ACCESS_REFUSED,
                                                                 1),
                {?RESPONSE_CODE_ACCESS_REFUSED, 0, Connection0}
        end,
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {query_offset, ResponseCode, Offset}}),
    send(Transport, S, Frame),
    {Connection1, State};
handle_frame_post_auth(Transport,
                       #stream_connection{stream_subscriptions =
                                              StreamSubscriptions} =
                           Connection,
                       #stream_connection_state{} = State,
                       {request, CorrelationId,
                        {unsubscribe, SubscriptionId}}) ->
    case subscription_exists(StreamSubscriptions, SubscriptionId) of
        false ->
            response(Transport,
                     Connection,
                     unsubscribe,
                     CorrelationId,
                     ?RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?SUBSCRIPTION_ID_DOES_NOT_EXIST,
                                                             1),
            {Connection, State};
        true ->
            {Connection1, State1} =
                remove_subscription(SubscriptionId, Connection, State),
            response_ok(Transport, Connection, unsubscribe, CorrelationId),
            {Connection1, State1}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{virtual_host = VirtualHost,
                                          user =
                                              #user{username = Username} =
                                                  User} =
                           Connection,
                       State,
                       {request, CorrelationId,
                        {create_stream, Stream, Arguments}}) ->
    case rabbit_stream_utils:enforce_correct_stream_name(Stream) of
        {ok, StreamName} ->
            case rabbit_stream_utils:check_configure_permitted(#resource{name =
                                                                             StreamName,
                                                                         kind =
                                                                             queue,
                                                                         virtual_host
                                                                             =
                                                                             VirtualHost},
                                                               User, #{})
            of
                ok ->
                    case rabbit_stream_manager:create(VirtualHost,
                                                      StreamName,
                                                      Arguments,
                                                      Username)
                    of
                        {ok,
                         #{leader_node := LeaderPid,
                           replica_nodes := ReturnedReplicas}} ->
                            rabbit_log:debug("Created stream cluster with leader on ~p and replicas on ~p",
                                             [LeaderPid, ReturnedReplicas]),
                            response_ok(Transport,
                                        Connection,
                                        create_stream,
                                        CorrelationId),
                            {Connection, State};
                        {error, validation_failed} ->
                            response(Transport,
                                     Connection,
                                     create_stream,
                                     CorrelationId,
                                     ?RESPONSE_CODE_PRECONDITION_FAILED),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?PRECONDITION_FAILED,
                                                                             1),
                            {Connection, State};
                        {error, reference_already_exists} ->
                            response(Transport,
                                     Connection,
                                     create_stream,
                                     CorrelationId,
                                     ?RESPONSE_CODE_STREAM_ALREADY_EXISTS),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?STREAM_ALREADY_EXISTS,
                                                                             1),
                            {Connection, State};
                        {error, _} ->
                            response(Transport,
                                     Connection,
                                     create_stream,
                                     CorrelationId,
                                     ?RESPONSE_CODE_INTERNAL_ERROR),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?INTERNAL_ERROR,
                                                                             1),
                            {Connection, State}
                    end;
                error ->
                    response(Transport,
                             Connection,
                             create_stream,
                             CorrelationId,
                             ?RESPONSE_CODE_ACCESS_REFUSED),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?ACCESS_REFUSED,
                                                                     1),
                    {Connection, State}
            end;
        _ ->
            response(Transport,
                     Connection,
                     create_stream,
                     CorrelationId,
                     ?RESPONSE_CODE_PRECONDITION_FAILED),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?PRECONDITION_FAILED,
                                                             1),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost,
                                          user =
                                              #user{username = Username} =
                                                  User} =
                           Connection,
                       State,
                       {request, CorrelationId, {delete_stream, Stream}}) ->
    case rabbit_stream_utils:check_configure_permitted(#resource{name =
                                                                     Stream,
                                                                 kind = queue,
                                                                 virtual_host =
                                                                     VirtualHost},
                                                       User, #{})
    of
        ok ->
            case rabbit_stream_manager:delete(VirtualHost, Stream, Username) of
                {ok, deleted} ->
                    response_ok(Transport,
                                Connection,
                                delete_stream,
                                CorrelationId),
                    {Connection1, State1} =
                        case
                            clean_state_after_stream_deletion_or_failure(Stream,
                                                                         Connection,
                                                                         State)
                        of
                            {cleaned, NewConnection, NewState} ->
                                Command =
                                    {metadata_update, Stream,
                                     ?RESPONSE_CODE_STREAM_NOT_AVAILABLE},
                                Frame = rabbit_stream_core:frame(Command),
                                send(Transport, S, Frame),
                                rabbit_global_counters:increase_protocol_counter(stream,
                                                                                 ?STREAM_NOT_AVAILABLE,
                                                                                 1),
                                {NewConnection, NewState};
                            {not_cleaned, SameConnection, SameState} ->
                                {SameConnection, SameState}
                        end,
                    {Connection1, State1};
                {error, reference_not_found} ->
                    response(Transport,
                             Connection,
                             delete_stream,
                             CorrelationId,
                             ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                     ?STREAM_DOES_NOT_EXIST,
                                                                     1),
                    {Connection, State}
            end;
        error ->
            response(Transport,
                     Connection,
                     delete_stream,
                     CorrelationId,
                     ?RESPONSE_CODE_ACCESS_REFUSED),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?ACCESS_REFUSED,
                                                             1),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost,
                                          transport = TransportLayer} =
                           Connection,
                       State,
                       {request, CorrelationId, {metadata, Streams}}) ->
    Topology =
        lists:foldl(fun(Stream, Acc) ->
                       Acc#{Stream =>
                                rabbit_stream_manager:topology(VirtualHost,
                                                               Stream)}
                    end,
                    #{}, Streams),

    %% get the nodes involved in the streams
    NodesMap =
        lists:foldl(fun(Stream, Acc) ->
                       case maps:get(Stream, Topology) of
                           {ok,
                            #{leader_node := undefined,
                              replica_nodes := ReplicaNodes}} ->
                               lists:foldl(fun(ReplicaNode, NodesAcc) ->
                                              maps:put(ReplicaNode, ok,
                                                       NodesAcc)
                                           end,
                                           Acc, ReplicaNodes);
                           {ok,
                            #{leader_node := LeaderNode,
                              replica_nodes := ReplicaNodes}} ->
                               Acc1 = maps:put(LeaderNode, ok, Acc),
                               lists:foldl(fun(ReplicaNode, NodesAcc) ->
                                              maps:put(ReplicaNode, ok,
                                                       NodesAcc)
                                           end,
                                           Acc1, ReplicaNodes);
                           {error, _} ->
                               Acc
                       end
                    end,
                    #{}, Streams),

    Nodes =
        lists:sort(
            maps:keys(NodesMap)),
    NodeEndpoints =
        lists:foldr(fun(Node, Acc) ->
                       PortFunction =
                           case TransportLayer of
                               tcp ->
                                   port;
                               ssl ->
                                   tls_port
                           end,
                       Host = rpc:call(Node, rabbit_stream, host, []),
                       Port = rpc:call(Node, rabbit_stream, PortFunction, []),
                       case {is_binary(Host), is_integer(Port)} of
                           {true, true} ->
                               Acc#{Node => {Host, Port}};
                           _ ->
                               rabbit_log:warning("Error when retrieving broker metadata: ~p ~p",
                                                  [Host, Port]),
                               Acc
                       end
                    end,
                    #{}, Nodes),

    Metadata =
        lists:foldl(fun(Stream, Acc) ->
                       case maps:get(Stream, Topology) of
                           {error, Err} ->
                               Acc#{Stream => Err};
                           {ok,
                            #{leader_node := LeaderNode,
                              replica_nodes := Replicas}} ->
                               LeaderInfo =
                                   case NodeEndpoints of
                                       #{LeaderNode := Info} ->
                                           Info;
                                       _ ->
                                           undefined
                                   end,
                               ReplicaInfos =
                                   lists:foldr(fun(Replica, A) ->
                                                  case NodeEndpoints of
                                                      #{Replica := I} ->
                                                          [I | A];
                                                      _ ->
                                                          A
                                                  end
                                               end,
                                               [], Replicas),
                               Acc#{Stream => {LeaderInfo, ReplicaInfos}}
                       end
                    end,
                    #{}, Streams),
    Endpoints =
        lists:usort(
            maps:values(NodeEndpoints)),
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {metadata, Endpoints, Metadata}}),
    send(Transport, S, Frame),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost} =
                           Connection,
                       State,
                       {request, CorrelationId,
                        {route, RoutingKey, SuperStream}}) ->
    {ResponseCode, StreamBin} =
        case rabbit_stream_manager:route(RoutingKey, VirtualHost, SuperStream)
        of
            {ok, no_route} ->
                {?RESPONSE_CODE_OK, <<(-1):16>>};
            {ok, Stream} ->
                StreamSize = byte_size(Stream),
                {?RESPONSE_CODE_OK,
                 <<StreamSize:16, Stream:StreamSize/binary>>};
            {error, _} ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?STREAM_DOES_NOT_EXIST,
                                                                 1),
                {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, <<(-1):16>>}
        end,

    Frame =
        <<?COMMAND_ROUTE:16,
          ?VERSION_1:16,
          CorrelationId:32,
          ResponseCode:16,
          StreamBin/binary>>,
    FrameSize = byte_size(Frame),
    Transport:send(S, <<FrameSize:32, Frame/binary>>),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost} =
                           Connection,
                       State,
                       {request, CorrelationId, {partitions, SuperStream}}) ->
    {ResponseCode, PartitionsBin} =
        case rabbit_stream_manager:partitions(VirtualHost, SuperStream) of
            {ok, []} ->
                {?RESPONSE_CODE_OK, <<0:32>>};
            {ok, Streams} ->
                StreamCount = length(Streams),
                Bin = lists:foldl(fun(Stream, Acc) ->
                                     StreamSize = byte_size(Stream),
                                     <<Acc/binary, StreamSize:16,
                                       Stream:StreamSize/binary>>
                                  end,
                                  <<StreamCount:32>>, Streams),
                {?RESPONSE_CODE_OK, Bin};
            {error, _} ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?STREAM_DOES_NOT_EXIST,
                                                                 1),
                {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, <<0:32>>}
        end,

    Frame =
        <<?COMMAND_PARTITIONS:16,
          ?VERSION_1:16,
          CorrelationId:32,
          ResponseCode:16,
          PartitionsBin/binary>>,
    FrameSize = byte_size(Frame),
    Transport:send(S, <<FrameSize:32, Frame/binary>>),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S} = Connection,
                       State,
                       {request, CorrelationId,
                        {close, ClosingCode, ClosingReason}}) ->
    rabbit_log:debug("Stream protocol reader received close command ~p ~p",
                     [ClosingCode, ClosingReason]),
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {close, ?RESPONSE_CODE_OK}}),
    Transport:send(S, Frame),
    {Connection#stream_connection{connection_step = closing},
     State}; %% we ignore any subsequent frames
handle_frame_post_auth(_Transport, Connection, State, heartbeat) ->
    rabbit_log:debug("Received heartbeat frame post auth"),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S} = Connection,
                       State,
                       Command) ->
    rabbit_log:warning("unknown command ~p, sending close command.",
                       [Command]),
    CloseReason = <<"unknown frame">>,
    Frame =
        rabbit_stream_core:frame({request, 1,
                                  {close, ?RESPONSE_CODE_UNKNOWN_FRAME,
                                   CloseReason}}),
    send(Transport, S, Frame),
    rabbit_global_counters:increase_protocol_counter(stream,
                                                     ?UNKNOWN_FRAME, 1),
    {Connection#stream_connection{connection_step = close_sent}, State}.

notify_connection_closed(#statem_data{connection =
                                          #stream_connection{name = Name,
                                                             publishers =
                                                                 Publishers} =
                                              Connection,
                                      connection_state =
                                          #stream_connection_state{consumers =
                                                                       Consumers} =
                                              ConnectionState}) ->
    rabbit_core_metrics:connection_closed(self()),
    [rabbit_stream_metrics:consumer_cancelled(self(),
                                              stream_r(S, Connection), SubId)
     || #consumer{stream = S, subscription_id = SubId}
            <- maps:values(Consumers)],
    [rabbit_stream_metrics:publisher_deleted(self(),
                                             stream_r(S, Connection), PubId)
     || #publisher{stream = S, publisher_id = PubId}
            <- maps:values(Publishers)],
    ClientProperties = i(client_properties, Connection, ConnectionState),
    EventProperties =
        [{name, Name},
         {pid, self()},
         {node, node()},
         {client_properties, ClientProperties}],
    rabbit_event:notify(connection_closed,
                        augment_infos_with_user_provided_connection_name(EventProperties,
                                                                         Connection)).

handle_frame_post_close(_Transport,
                        Connection,
                        State,
                        {response, _CorrelationId, {close, _Code}}) ->
    rabbit_log_connection:info("Received close confirmation from client"),
    {Connection#stream_connection{connection_step = closing_done}, State};
handle_frame_post_close(_Transport, Connection, State, heartbeat) ->
    rabbit_log_connection:debug("Received heartbeat command post close"),
    {Connection, State};
handle_frame_post_close(_Transport, Connection, State, Command) ->
    rabbit_log_connection:warning("ignored command on close ~p .",
                                  [Command]),
    {Connection, State}.

stream_r(Stream, #stream_connection{virtual_host = VHost}) ->
    #resource{name = Stream,
              kind = queue,
              virtual_host = VHost}.

clean_state_after_stream_deletion_or_failure(Stream,
                                             #stream_connection{stream_subscriptions
                                                                    =
                                                                    StreamSubscriptions,
                                                                publishers =
                                                                    Publishers,
                                                                publisher_to_ids
                                                                    =
                                                                    PublisherToIds,
                                                                stream_leaders =
                                                                    Leaders} =
                                                 C0,
                                             #stream_connection_state{consumers
                                                                          =
                                                                          Consumers} =
                                                 S0) ->
    {SubscriptionsCleaned, C1, S1} =
        case stream_has_subscriptions(Stream, C0) of
            true ->
                #{Stream := SubscriptionIds} = StreamSubscriptions,
                [rabbit_stream_metrics:consumer_cancelled(self(),
                                                          stream_r(Stream, C0),
                                                          SubId)
                 || SubId <- SubscriptionIds],
                {true,
                 C0#stream_connection{stream_subscriptions =
                                          maps:remove(Stream,
                                                      StreamSubscriptions)},
                 S0#stream_connection_state{consumers =
                                                maps:without(SubscriptionIds,
                                                             Consumers)}};
            false ->
                {false, C0, S0}
        end,
    {PublishersCleaned, C2, S2} =
        case stream_has_publishers(Stream, C1) of
            true ->
                {PurgedPubs, PurgedPubToIds} =
                    maps:fold(fun(PubId,
                                  #publisher{stream = S, reference = Ref},
                                  {Pubs, PubToIds}) ->
                                 case S of
                                     Stream ->
                                         rabbit_stream_metrics:publisher_deleted(self(),
                                                                                 stream_r(S,
                                                                                          C1),
                                                                                 PubId),
                                         {maps:remove(PubId, Pubs),
                                          maps:remove({Stream, Ref}, PubToIds)};
                                     _ ->
                                         {Pubs, PubToIds}
                                 end
                              end,
                              {Publishers, PublisherToIds}, Publishers),
                {true,
                 C1#stream_connection{publishers = PurgedPubs,
                                      publisher_to_ids = PurgedPubToIds},
                 S1};
            false ->
                {false, C1, S1}
        end,
    {LeadersCleaned, Leaders1} =
        case Leaders of
            #{Stream := _} ->
                {true, maps:remove(Stream, Leaders)};
            _ ->
                {false, Leaders}
        end,
    case SubscriptionsCleaned
         orelse PublishersCleaned
         orelse LeadersCleaned
    of
        true ->
            C3 = demonitor_stream(Stream, C2),
            {cleaned, C3#stream_connection{stream_leaders = Leaders1}, S2};
        false ->
            {not_cleaned, C2#stream_connection{stream_leaders = Leaders1}, S2}
    end.

lookup_leader(Stream,
              #stream_connection{stream_leaders = StreamLeaders,
                                 virtual_host = VirtualHost} =
                  Connection) ->
    case maps:get(Stream, StreamLeaders, undefined) of
        undefined ->
            case lookup_leader_from_manager(VirtualHost, Stream) of
                cluster_not_found ->
                    cluster_not_found;
                LeaderPid ->
                    Connection1 =
                        maybe_monitor_stream(LeaderPid, Stream, Connection),
                    {LeaderPid,
                     Connection1#stream_connection{stream_leaders =
                                                       StreamLeaders#{Stream =>
                                                                          LeaderPid}}}
            end;
        LeaderPid ->
            {LeaderPid, Connection}
    end.

lookup_leader_from_manager(VirtualHost, Stream) ->
    rabbit_stream_manager:lookup_leader(VirtualHost, Stream).

remove_subscription(SubscriptionId,
                    #stream_connection{stream_subscriptions =
                                           StreamSubscriptions} =
                        Connection,
                    #stream_connection_state{consumers = Consumers} = State) ->
    #{SubscriptionId := Consumer} = Consumers,
    Stream = Consumer#consumer.stream,
    #{Stream := SubscriptionsForThisStream} = StreamSubscriptions,
    SubscriptionsForThisStream1 =
        lists:delete(SubscriptionId, SubscriptionsForThisStream),
    StreamSubscriptions1 =
        case length(SubscriptionsForThisStream1) of
            0 ->
                % no more subscription for this stream
                maps:remove(Stream, StreamSubscriptions);
            _ ->
                StreamSubscriptions#{Stream => SubscriptionsForThisStream1}
        end,
    Connection1 =
        Connection#stream_connection{stream_subscriptions =
                                         StreamSubscriptions1},
    Consumers1 = maps:remove(SubscriptionId, Consumers),
    Connection2 = maybe_clean_connection_from_stream(Stream, Connection1),
    rabbit_stream_metrics:consumer_cancelled(self(),
                                             stream_r(Stream, Connection2),
                                             SubscriptionId),
    {Connection2, State#stream_connection_state{consumers = Consumers1}}.

maybe_clean_connection_from_stream(Stream,
                                   #stream_connection{stream_leaders =
                                                          Leaders} =
                                       Connection0) ->
    Connection1 =
        case {stream_has_publishers(Stream, Connection0),
              stream_has_subscriptions(Stream, Connection0)}
        of
            {false, false} ->
                demonitor_stream(Stream, Connection0);
            _ ->
                Connection0
        end,
    Connection1#stream_connection{stream_leaders =
                                      maps:remove(Stream, Leaders)}.

maybe_monitor_stream(Pid, Stream,
                     #stream_connection{monitors = Monitors} = Connection) ->
    case lists:member(Stream, maps:values(Monitors)) of
        true ->
            Connection;
        false ->
            MonitorRef = monitor(process, Pid),
            Connection#stream_connection{monitors =
                                             maps:put(MonitorRef, Stream,
                                                      Monitors)}
    end.

demonitor_stream(Stream,
                 #stream_connection{monitors = Monitors0} = Connection) ->
    Monitors =
        maps:fold(fun(MonitorRef, Strm, Acc) ->
                     case Strm of
                         Stream ->
                             demonitor(MonitorRef, [flush]),
                             Acc;
                         _ ->
                             maps:put(MonitorRef, Strm, Acc)
                     end
                  end,
                  #{}, Monitors0),
    Connection#stream_connection{monitors = Monitors}.

stream_has_subscriptions(Stream,
                         #stream_connection{stream_subscriptions =
                                                Subscriptions}) ->
    case Subscriptions of
        #{Stream := StreamSubscriptions}
            when length(StreamSubscriptions) > 0 ->
            true;
        _ ->
            false
    end.

stream_has_publishers(Stream,
                      #stream_connection{publishers = Publishers}) ->
    lists:any(fun(#publisher{stream = S}) ->
                 case S of
                     Stream ->
                         true;
                     _ ->
                         false
                 end
              end,
              maps:values(Publishers)).

demonitor_all_streams(#stream_connection{monitors = Monitors} =
                          Connection) ->
    lists:foreach(fun(MonitorRef) -> demonitor(MonitorRef, [flush]) end,
                  maps:keys(Monitors)),
    Connection#stream_connection{monitors = #{}}.

response_ok(Transport, State, Command, CorrelationId) ->
    response(Transport, State, Command, CorrelationId, ?RESPONSE_CODE_OK).

response(Transport,
         #stream_connection{socket = S},
         Command,
         CorrelationId,
         ResponseCode)
    when is_atom(Command) ->
    send(Transport, S,
         rabbit_stream_core:frame({response, CorrelationId,
                                   {Command, ResponseCode}})).

subscription_exists(StreamSubscriptions, SubscriptionId) ->
    SubscriptionIds =
        lists:flatten(
            maps:values(StreamSubscriptions)),
    lists:any(fun(Id) -> Id =:= SubscriptionId end, SubscriptionIds).

send_file_callback(Transport,
                   #consumer{socket = S,
                             subscription_id = SubscriptionId,
                             counters = Counters},
                   Counter) ->
    fun(#{chunk_id := FirstOffsetInChunk, num_entries := NumEntries},
        Size) ->
       FrameSize = 2 + 2 + 1 + Size,
       FrameBeginning =
           <<FrameSize:32,
             ?REQUEST:1,
             ?COMMAND_DELIVER:15,
             ?VERSION_1:16,
             SubscriptionId:8/unsigned>>,
       Transport:send(S, FrameBeginning),
       atomics:add(Counter, 1, Size),
       increase_messages_consumed(Counters, NumEntries),
       set_consumer_offset(Counters, FirstOffsetInChunk)
    end.

send_chunks(Transport, #consumer{credit = Credit} = State, Counter) ->
    send_chunks(Transport, State, Credit, Counter).

send_chunks(_Transport, #consumer{segment = Segment}, 0, _Counter) ->
    {{segment, Segment}, {credit, 0}};
send_chunks(Transport,
            #consumer{segment = Segment} = State,
            Credit,
            Counter) ->
    send_chunks(Transport, State, Segment, Credit, true, Counter).

send_chunks(_Transport,
            _State,
            Segment,
            0 = _Credit,
            _Retry,
            _Counter) ->
    {{segment, Segment}, {credit, 0}};
send_chunks(Transport,
            #consumer{socket = S} = State,
            Segment,
            Credit,
            Retry,
            Counter) ->
    case osiris_log:send_file(S, Segment,
                              send_file_callback(Transport, State, Counter))
    of
        {ok, Segment1} ->
            send_chunks(Transport, State, Segment1, Credit - 1, true, Counter);
        {error, closed} ->
            {error, closed};
        {error, enotconn} ->
            {error, closed};
        {error, Reason} ->
            {error, Reason};
        {end_of_stream, Segment1} ->
            case Retry of
                true ->
                    timer:sleep(1),
                    send_chunks(Transport,
                                State,
                                Segment1,
                                Credit,
                                false,
                                Counter);
                false ->
                    #consumer{member_pid = LocalMember} = State,
                    osiris:register_offset_listener(LocalMember,
                                                    osiris_log:next_offset(Segment1)),
                    {{segment, Segment1}, {credit, Credit}}
            end
    end.

emit_stats(#stream_connection{publishers = Publishers} = Connection,
           #stream_connection_state{consumers = Consumers} = ConnectionState) ->
    [{_, Pid}, {_, Recv_oct}, {_, Send_oct}, {_, Reductions}] =
        I = infos(?SIMPLE_METRICS, Connection, ConnectionState),
    Infos = infos(?OTHER_METRICS, Connection, ConnectionState),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid,
                                         Recv_oct,
                                         Send_oct,
                                         Reductions),
    rabbit_event:notify(connection_stats, Infos ++ I),
    [rabbit_stream_metrics:consumer_updated(self(),
                                            stream_r(S, Connection),
                                            Id,
                                            Credit,
                                            messages_consumed(Counters),
                                            consumer_offset(Counters),
                                            consumer_i(offset_lag, Consumer),
                                            Properties)
     || #consumer{stream = S,
                  subscription_id = Id,
                  credit = Credit,
                  counters = Counters,
                  properties = Properties} =
            Consumer
            <- maps:values(Consumers)],
    [rabbit_stream_metrics:publisher_updated(self(),
                                             stream_r(S, Connection),
                                             Id,
                                             PubReference,
                                             messages_published(Counters),
                                             messages_confirmed(Counters),
                                             messages_errored(Counters))
     || #publisher{stream = S,
                   publisher_id = Id,
                   reference = PubReference,
                   message_counters = Counters}
            <- maps:values(Publishers)],
    Connection1 =
        rabbit_event:reset_stats_timer(Connection,
                                       #stream_connection.stats_timer),
    ensure_stats_timer(Connection1).

ensure_stats_timer(Connection = #stream_connection{}) ->
    rabbit_event:ensure_stats_timer(Connection,
                                    #stream_connection.stats_timer, emit_stats).

in_vhost(_Pid, undefined) ->
    true;
in_vhost(Pid, VHost) ->
    case info(Pid, [vhost]) of
        [{vhost, VHost}] ->
            true;
        _ ->
            false
    end.

consumers_info(Pid, InfoItems) ->
    case InfoItems -- ?CONSUMER_INFO_ITEMS of
        [] ->
            gen_server2:call(Pid, {consumers_info, InfoItems});
        UnknownItems ->
            throw({bad_argument, UnknownItems})
    end.

consumers_infos(Items,
                #stream_connection_state{consumers = Consumers}) ->
    [[{Item, consumer_i(Item, Consumer)} || Item <- Items]
     || Consumer <- maps:values(Consumers)].

consumer_i(subscription_id, #consumer{subscription_id = SubId}) ->
    SubId;
consumer_i(credits, #consumer{credit = Credits}) ->
    Credits;
consumer_i(messages_consumed, #consumer{counters = Counters}) ->
    messages_consumed(Counters);
consumer_i(offset, #consumer{counters = Counters}) ->
    consumer_offset(Counters);
consumer_i(offset_lag,
           #consumer{counters = Counters, segment = Log}) ->
    stream_stored_offset(Log) - consumer_offset(Counters);
consumer_i(connection_pid, _) ->
    self();
consumer_i(properties, #consumer{properties = Properties}) ->
    Properties;
consumer_i(stream, #consumer{stream = Stream}) ->
    Stream.

publishers_info(Pid, InfoItems) ->
    case InfoItems -- ?PUBLISHER_INFO_ITEMS of
        [] ->
            gen_server2:call(Pid, {publishers_info, InfoItems});
        UnknownItems ->
            throw({bad_argument, UnknownItems})
    end.

publishers_infos(Items,
                 #stream_connection{publishers = Publishers}) ->
    [[{Item, publisher_i(Item, Publisher)} || Item <- Items]
     || Publisher <- maps:values(Publishers)].

publisher_i(stream, #publisher{stream = S}) ->
    S;
publisher_i(connection_pid, _) ->
    self();
publisher_i(publisher_id, #publisher{publisher_id = Id}) ->
    Id;
publisher_i(reference, #publisher{reference = undefined}) ->
    <<"">>;
publisher_i(reference, #publisher{reference = Ref}) ->
    Ref;
publisher_i(messages_published,
            #publisher{message_counters = Counters}) ->
    messages_published(Counters);
publisher_i(messages_confirmed,
            #publisher{message_counters = Counters}) ->
    messages_confirmed(Counters);
publisher_i(messages_errored,
            #publisher{message_counters = Counters}) ->
    messages_errored(Counters).

info(Pid, InfoItems) ->
    case InfoItems -- ?INFO_ITEMS of
        [] ->
            gen_server2:call(Pid, {info, InfoItems}, infinity);
        UnknownItems ->
            throw({bad_argument, UnknownItems})
    end.

infos(Items, Connection, State) ->
    [{Item, i(Item, Connection, State)} || Item <- Items].

i(pid, _, _) ->
    self();
i(node, _, _) ->
    node();
i(SockStat,
  #stream_connection{socket = Sock, send_file_oct = Counter}, _)
    when SockStat =:= send_oct -> % Number of bytes sent from the socket.
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{_, N}]} when is_number(N) ->
            N + atomics:get(Counter, 1);
        _ ->
            0 + atomics:get(Counter, 1)
    end;
i(SockStat, #stream_connection{socket = Sock}, _)
    when SockStat =:= recv_oct; % Number of bytes received by the socket.
         SockStat =:= recv_cnt; % Number of packets received by the socket.
         SockStat =:= send_cnt; % Number of packets sent from the socket.
         SockStat
         =:= send_pend -> % Number of bytes waiting to be sent by the socket.
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{_, N}]} when is_number(N) ->
            N;
        _ ->
            0
    end;
i(reductions, _, _) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(garbage_collection, _, _) ->
    rabbit_misc:get_gc_info(self());
i(state, Connection, ConnectionState) ->
    i(connection_state, Connection, ConnectionState);
i(timeout, Connection, ConnectionState) ->
    i(heartbeat, Connection, ConnectionState);
i(name, Connection, ConnectionState) ->
    i(conn_name, Connection, ConnectionState);
i(conn_name, #stream_connection{name = Name}, _) ->
    Name;
i(port, #stream_connection{port = Port}, _) ->
    Port;
i(peer_port, #stream_connection{peer_port = PeerPort}, _) ->
    PeerPort;
i(host, #stream_connection{host = Host}, _) ->
    Host;
i(peer_host, #stream_connection{peer_host = PeerHost}, _) ->
    PeerHost;
i(ssl, #stream_connection{socket = Socket, proxy_socket = ProxySock},
  _) ->
    rabbit_net:proxy_ssl_info(Socket, ProxySock) /= nossl;
i(peer_cert_subject, S, _) ->
    cert_info(fun rabbit_ssl:peer_cert_subject/1, S);
i(peer_cert_issuer, S, _) ->
    cert_info(fun rabbit_ssl:peer_cert_issuer/1, S);
i(peer_cert_validity, S, _) ->
    cert_info(fun rabbit_ssl:peer_cert_validity/1, S);
i(ssl_protocol, S, _) ->
    ssl_info(fun({P, _}) -> P end, S);
i(ssl_key_exchange, S, _) ->
    ssl_info(fun({_, {K, _, _}}) -> K end, S);
i(ssl_cipher, S, _) ->
    ssl_info(fun({_, {_, C, _}}) -> C end, S);
i(ssl_hash, S, _) ->
    ssl_info(fun({_, {_, _, H}}) -> H end, S);
i(channels, _, _) ->
    0;
i(protocol, _, _) ->
    <<"stream">>;
i(user_who_performed_action, Connection, ConnectionState) ->
    i(user, Connection, ConnectionState);
i(user, #stream_connection{user = U}, _) ->
    U#user.username;
i(vhost, #stream_connection{virtual_host = VirtualHost}, _) ->
    VirtualHost;
i(subscriptions, _,
  #stream_connection_state{consumers = Consumers}) ->
    maps:size(Consumers);
i(connection_state, _Connection,
  #stream_connection_state{blocked = true}) ->
    blocked;
i(connection_state, _Connection,
  #stream_connection_state{blocked = false}) ->
    running;
i(auth_mechanism, #stream_connection{auth_mechanism = none}, _) ->
    none;
i(auth_mechanism, #stream_connection{auth_mechanism = {Name, _Mod}},
  _) ->
    Name;
i(heartbeat, #stream_connection{heartbeat = Heartbeat}, _) ->
    Heartbeat;
i(frame_max, #stream_connection{frame_max = FrameMax}, _) ->
    FrameMax;
i(channel_max, _, _) ->
    0;
i(client_properties, #stream_connection{client_properties = CP}, _) ->
    rabbit_misc:to_amqp_table(CP);
i(connected_at, #stream_connection{connected_at = T}, _) ->
    T;
i(Item, #stream_connection{}, _) ->
    throw({bad_argument, Item}).

-spec send(module(), rabbit_net:socket(), iodata()) -> ok.
send(Transport, Socket, Data) when is_atom(Transport) ->
    Transport:send(Socket, Data).

cert_info(F, #stream_connection{socket = Sock}) ->
    case rabbit_net:peercert(Sock) of
        nossl ->
            '';
        {error, _} ->
            '';
        {ok, Cert} ->
            list_to_binary(F(Cert))
    end.

ssl_info(F,
         #stream_connection{socket = Sock, proxy_socket = ProxySock}) ->
    case rabbit_net:proxy_ssl_info(Sock, ProxySock) of
        nossl ->
            '';
        {error, _} ->
            '';
        {ok, Items} ->
            P = proplists:get_value(protocol, Items),
            #{cipher := C,
              key_exchange := K,
              mac := H} =
                proplists:get_value(selected_cipher_suite, Items),
            F({P, {K, C, H}})
    end.

get_chunk_selector(Properties) ->
    binary_to_atom(maps:get(<<"chunk_selector">>, Properties,
                            <<"user_data">>)).
