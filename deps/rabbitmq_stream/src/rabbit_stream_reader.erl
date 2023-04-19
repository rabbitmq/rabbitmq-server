%% The contents of this file are subject to the Mozilla Public License
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
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
-record(consumer_configuration,
        {socket :: rabbit_net:socket(), %% ranch_transport:socket(),
         member_pid :: pid(),
         subscription_id :: subscription_id(),
         stream :: stream(),
         offset :: osiris:offset(),
         counters :: atomics:atomics_ref(),
         properties :: map(),
         active :: boolean()}).
-record(consumer,
        {configuration :: #consumer_configuration{},
         credit :: non_neg_integer(),
         send_limit :: non_neg_integer(),
         log :: undefined | osiris_log:state(),
         last_listener_offset = undefined :: undefined | osiris:offset()}).
-record(request,
        {start :: integer(),
         content :: term()}).
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
         stats_timer :: undefined | rabbit_event:state(),
         resource_alarm :: boolean(),
         send_file_oct ::
             atomics:atomics_ref(), % number of bytes sent with send_file (for metrics)
         transport :: tcp | ssl,
         proxy_socket :: undefined | ranch_transport:socket(),
         correlation_id_sequence :: integer(),
         outstanding_requests :: #{integer() => #request{}},
         deliver_version :: rabbit_stream_core:command_version(),
         request_timeout :: pos_integer(),
         outstanding_requests_timer :: undefined | erlang:reference()}).
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
-define(UNKNOWN_FIELD, unknown_field).

%% client API
-export([start_link/4,
         info/2,
         consumers_info/2,
         publishers_info/2,
         in_vhost/2]).
-export([resource_alarm/3,
         single_active_consumer/1]).
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

terminate(Reason, State,
          #statem_data{transport = Transport,
                       connection = Connection,
                       connection_state = ConnectionState} =
              StatemData) ->
    close(Transport, Connection, ConnectionState),
    rabbit_networking:unregister_non_amqp_connection(self()),
    notify_connection_closed(StatemData),
    rabbit_log:debug("~ts terminating in state '~ts' with reason '~W'",
                     [?MODULE, State, Reason, 10]).

start_link(KeepaliveSup, Transport, Ref, Opts) ->
    {ok,
     proc_lib:spawn_link(?MODULE, init,
                         [[KeepaliveSup, Transport, Ref, Opts]])}.

%% Because of gen_statem:enter_loop/4 usage inside init/1
-dialyzer({no_behaviours, init/1}).

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
            DeliverVersion = ?VERSION_1,
            RequestTimeout = application:get_env(rabbitmq_stream,
                                                 request_timeout, 60_000),
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
                                       rabbit_net:maybe_get_proxy_socket(Sock),
                                   correlation_id_sequence = 0,
                                   outstanding_requests = #{},
                                   request_timeout = RequestTimeout,
                                   deliver_version = DeliverVersion},
            State =
                #stream_connection_state{consumers = #{},
                                         blocked = false,
                                         data =
                                             rabbit_stream_core:init(undefined)},
            Transport:setopts(RealSocket, [{active, once}]),
            _ = rabbit_alarm:register(self(), {?MODULE, resource_alarm, []}),
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
            rabbit_log_connection:warning("Closing connection because of ~tp ~tp",
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
    rabbit_log_connection:warning("Closing connection because of timeout in state "
                                  "'~ts' likely due to lack of client action.",
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
            setopts(Transport, S, [{active, once}]),
            #stream_connection{connection_step = NewConnectionStep} =
                Connection1,
            rabbit_log_connection:debug("Transitioned from ~ts to ~ts",
                                        [PreviousConnectionStep,
                                         NewConnectionStep]),
            Transition(NewConnectionStep, StatemData, Connection1, State1);
        {Closed, S} ->
            rabbit_log_connection:debug("Stream protocol connection socket ~w closed",
                                        [S]),
            stop;
        {Error, S, Reason} ->
            rabbit_log_connection:warning("Socket error ~tp [~w]", [Reason, S]),
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
            rabbit_log:warning("Received unknown message ~tp", [Unknown]),
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
    rabbit_log_connection:warning("Closing socket ~w. Invalid transition from ~ts "
                                  "to ~ts.",
                                  [Socket, From, To]),
    close_immediately(Transport, Socket),
    stop.

-spec resource_alarm(pid(),
                     rabbit_alarm:resource_alarm_source(),
                     rabbit_alarm:resource_alert()) -> ok.
resource_alarm(ConnectionPid, disk, {_, Conserve, _}) ->
    ConnectionPid ! {resource_alarm, Conserve},
    ok;
resource_alarm(_ConnectionPid, _Resource, _Alert) ->
    ok.

socket_op(Sock, Fun) ->
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case Fun(Sock) of
        {ok, Res} ->
            Res;
        {error, Reason} ->
            rabbit_log_connection:warning("Error during socket operation ~tp",
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

close(Transport,
      #stream_connection{socket = S, virtual_host = VirtualHost,
                         outstanding_requests = Requests},
      #stream_connection_state{consumers = Consumers}) ->
    [begin
         %% we discard the result (updated requests) because they are no longer used
         _ = maybe_unregister_consumer(VirtualHost, Consumer,
                                       single_active_consumer(Properties),
                                       Requests),
         case Log of
             undefined ->
                 ok; %% segment may not be defined on subscription (single active consumer)
             L ->
                 osiris_log:close(L)
         end
     end
     || #consumer{log = Log,
                  configuration =
                      #consumer_configuration{properties = Properties}} =
            Consumer
            <- maps:values(Consumers)],
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
    rabbit_log_connection:debug("Connection ~tp received resource alarm. Alarm "
                                "on? ~tp",
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
    rabbit_log_connection:debug("Connection ~tp had blocked status set to ~tp, "
                                "new blocked status is now ~tp",
                                [ConnectionName, Blocked, NewBlockedState]),
    case {Blocked, NewBlockedState} of
        {true, false} ->
            setopts(Transport, S, [{active, once}]),
            ok = rabbit_heartbeat:resume_monitor(Heartbeater),
            rabbit_log_connection:debug("Unblocking connection ~tp",
                                        [ConnectionName]);
        {false, true} ->
            ok = rabbit_heartbeat:pause_monitor(Heartbeater),
            rabbit_log_connection:debug("Blocking connection ~tp after resource alarm",
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
            stop;
        close_sent ->
            rabbit_log_connection:debug("Transitioned to close_sent"),
            setopts(Transport, S, [{active, once}]),
            {next_state, close_sent,
             StatemData#statem_data{connection = Connection1,
                                    connection_state = State1}};
        _ ->
            State2 =
                case Blocked of
                    true ->
                        case should_unblock(Connection, Configuration) of
                            true ->
                                setopts(Transport, S, [{active, once}]),
                                ok =
                                    rabbit_heartbeat:resume_monitor(Heartbeater),
                                State1#stream_connection_state{blocked = false};
                            false ->
                                State1
                        end;
                    false ->
                        case has_credits(Credits) of
                            true ->
                                setopts(Transport, S, [{active, once}]),
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
open(info,
     {sac, {{subscription_id, SubId},
            {active, Active}, {extra, Extra}}},
     State) ->
    Msg0 = #{subscription_id => SubId,
             active => Active},
    Msg1 = case Extra of
               [{stepping_down, true}] ->
                   Msg0#{stepping_down => true};
               _ ->
                   Msg0
           end,
    open(info, {sac, Msg1}, State);
open(info,
     {sac, #{subscription_id := SubId,
             active := Active} = Msg},
     #statem_data{transport = Transport,
                  connection = #stream_connection{virtual_host = VirtualHost} = Connection0,
                  connection_state = ConnState0} =
         State) ->
    #stream_connection_state{consumers = Consumers0} = ConnState0,
    Stream = case Msg of
                 #{stream := S} ->
                     S;
                 _ ->
                     stream_from_consumers(SubId, Consumers0)
             end,

    rabbit_log:debug("Subscription ~tp on ~tp instructed to become active: "
                     "~tp",
                     [SubId, Stream, Active]),
    {Connection1, ConnState1} =
        case Consumers0 of
            #{SubId :=
                  #consumer{configuration =
                                #consumer_configuration{properties =
                                                            Properties} =
                                    Conf0,
                            log = Log0} =
                      Consumer0} ->
                case single_active_consumer(Properties) of
                    true ->
                        Log1 =
                            case {Active, Log0} of
                                {false, undefined} ->
                                    undefined;
                                {false, L} ->
                                    rabbit_log:debug("Closing Osiris segment of subscription ~tp for "
                                                     "now",
                                                     [SubId]),
                                    osiris_log:close(L),
                                    undefined;
                                _ ->
                                    Log0
                            end,
                        Consumer1 =
                            Consumer0#consumer{configuration =
                                                   Conf0#consumer_configuration{active
                                                                                    =
                                                                                    Active},
                                               log = Log1},

                        Conn1 =
                            maybe_send_consumer_update(Transport,
                                                       Connection0,
                                                       Consumer1,
                                                       Active,
                                                       Msg),
                        {Conn1,
                         ConnState0#stream_connection_state{consumers =
                                                                Consumers0#{SubId
                                                                                =>
                                                                                Consumer1}}};
                    false ->
                        rabbit_log:warning("Received SAC event for subscription ~tp, which "
                                           "is not a SAC. Not doing anything.",
                                           [SubId]),
                        {Connection0, ConnState0}
                end;
            _ ->
                rabbit_log:debug("Subscription ~tp on ~tp has been deleted.",
                                 [SubId, Stream]),
                rabbit_log:debug("Active ~tp, message ~tp", [Active, Msg]),
                case {Active, Msg} of
                    {false, #{stepping_down := true,
                              stream := St,
                              consumer_name := ConsumerName}} ->
                        rabbit_log:debug("Former active consumer gone, activating consumer " ++
                                         "on stream ~tp, group ~tp", [St, ConsumerName]),
                        _ = rabbit_stream_sac_coordinator:activate_consumer(VirtualHost,
                                                                            St,
                                                                            ConsumerName);
                    _ ->
                        ok
                end,
                {Connection0, ConnState0}
        end,
    {keep_state,
     State#statem_data{connection = Connection1,
                       connection_state = ConnState1}};
open(info, {Closed, Socket}, #statem_data{connection = Connection})
    when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    _ = demonitor_all_streams(Connection),
    rabbit_log_connection:warning("Socket ~w closed [~w]",
                                  [Socket, self()]),
    stop;
open(info, {Error, Socket, Reason},
     #statem_data{connection = Connection})
    when Error =:= tcp_error; Error =:= ssl_error ->
    _ = demonitor_all_streams(Connection),
    rabbit_log_connection:error("Socket error ~tp [~w] [~w]",
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
                  connection = #stream_connection{socket = S} = Connection}) ->
    Frame = rabbit_stream_core:frame(heartbeat),
    case catch send(Transport, S, Frame) of
        ok ->
            keep_state_and_data;
        Unexpected ->
            rabbit_log_connection:info("Heartbeat send error ~tp, closing connection",
                                       [Unexpected]),
            _C1 = demonitor_all_streams(Connection),
            stop
    end;
open(info, heartbeat_timeout,
     #statem_data{connection = #stream_connection{} = Connection}) ->
    rabbit_log_connection:debug("Heartbeat timeout, closing connection"),
    _C1 = demonitor_all_streams(Connection),
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
open(info, check_outstanding_requests,
     #statem_data{connection = #stream_connection{outstanding_requests = Requests,
                                                  request_timeout = Timeout} = Connection0} =
         StatemData) ->
    Time = erlang:monotonic_time(millisecond),
    rabbit_log:debug("Checking outstanding requests at ~tp: ~tp", [Time, Requests]),
    HasTimedOut = maps:fold(fun(_, #request{}, true) ->
                                    true;
                               (K, #request{content = Ctnt, start = Start}, false) ->
                                    case (Time - Start) > Timeout of
                                        true ->
                                            rabbit_log:debug("Request ~tp with content ~tp has timed out",
                                                             [K, Ctnt]),

                                            true;
                                        false ->
                                            false
                                    end
                            end, false, Requests),
    case HasTimedOut of
        true ->
            rabbit_log_connection:info("Forcing stream connection ~tp closing: request to client timed out",
                                       [self()]),
            _ = demonitor_all_streams(Connection0),
            {stop, {request_timeout, <<"Request timeout">>}};
        false ->
            Connection1 = ensure_outstanding_requests_timer(
                            Connection0#stream_connection{outstanding_requests_timer = undefined}
                           ),
            {keep_state, StatemData#statem_data{connection = Connection1}}
    end;
open(info, {shutdown, Explanation} = Reason,
     #statem_data{connection = Connection}) ->
    %% rabbitmq_management or rabbitmq_stream_management plugin
    %% requests to close connection.
    rabbit_log_connection:info("Forcing stream connection ~tp closing: ~tp",
                               [self(), Explanation]),
    _ = demonitor_all_streams(Connection),
    {stop, Reason};
open(info, Unknown, _StatemData) ->
    rabbit_log_connection:warning("Received unknown message ~tp in state ~ts",
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
                           false -> Acc
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
                        setopts(Transport, S, [{active, once}]),
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
                        setopts(Transport, S, [{active, once}]),
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
    rabbit_log:debug("Stream protocol connection received osiris offset "
                     "event for ~tp with offset ~tp",
                     [StreamName, -1]),
    keep_state_and_data;
open(cast,
     {queue_event, #resource{name = StreamName},
      {osiris_offset, _QueueResource, Offset}},
     #statem_data{transport = Transport,
                  connection =
                      #stream_connection{stream_subscriptions =
                                             StreamSubscriptions,
                                         send_file_oct = SendFileOct,
                                         deliver_version = DeliverVersion} =
                          Connection,
                  connection_state =
                      #stream_connection_state{consumers = Consumers} = State} =
         StatemData)
    when Offset > -1 ->
    {Connection1, State1} =
        case maps:get(StreamName, StreamSubscriptions, undefined) of
            undefined ->
                rabbit_log:debug("Stream protocol connection: osiris offset event "
                                 "for ~tp, but no subscription (leftover messages "
                                 "after unsubscribe?)",
                                 [StreamName]),
                {Connection, State};
            [] ->
                rabbit_log:debug("Stream protocol connection: osiris offset event "
                                 "for ~tp, but no registered consumers!",
                                 [StreamName]),
                {Connection#stream_connection{stream_subscriptions =
                                                  maps:remove(StreamName,
                                                              StreamSubscriptions)},
                 State};
            SubscriptionIds when is_list(SubscriptionIds) ->
                Consumers1 =
                    lists:foldl(fun(SubscriptionId, ConsumersAcc) ->
                                   #{SubscriptionId := Consumer} = ConsumersAcc,
                                   #consumer{credit = Credit, log = Log} =
                                       Consumer,
                                   Consumer1 =
                                       case {Credit, Log} of
                                           {_, undefined} ->
                                               Consumer; %% SAC not active
                                           {0, _} -> Consumer;
                                           {_, _} ->
                                               case send_chunks(DeliverVersion,
                                                                Transport,
                                                                Consumer,
                                                                SendFileOct)
                                               of
                                                   {error, closed} ->
                                                       rabbit_log_connection:info("Stream protocol connection has been closed by "
                                                                                  "peer",
                                                                                  []),
                                                       throw({stop, normal});
                                                   {error, Reason} ->
                                                       rabbit_log_connection:info("Error while sending chunks: ~tp",
                                                                                  [Reason]),
                                                       %% likely a connection problem
                                                       Consumer;
                                                   {ok, Csmr} -> Csmr
                                               end
                                       end,
                                   ConsumersAcc#{SubscriptionId => Consumer1}
                                end,
                                Consumers, SubscriptionIds),
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
    {keep_state, StatemData#statem_data{connection = Connection2}};
open(cast, refresh_config, _StatemData) ->
    %% tracing not supported
    keep_state_and_data.

close_sent(enter, _OldState,
           #statem_data{config =
                            #configuration{connection_negotiation_step_timeout =
                                               StateTimeout}}) ->
    {keep_state_and_data, {state_timeout, StateTimeout, close}};
close_sent(state_timeout, close, #statem_data{}) ->
    rabbit_log_connection:warning("Closing connection because of timeout in state "
                                  "'~ts' likely due to lack of client action.",
                                  [?FUNCTION_NAME]),
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
    rabbit_log_connection:debug("Stream reader has transitioned from ~ts to ~ts",
                                [?FUNCTION_NAME, Step]),
    case Step of
        closing_done ->
            stop;
        _ ->
            setopts(Transport, S, [{active, once}]),
            {keep_state,
             StatemData#statem_data{connection = Connection1,
                                    connection_state = State1}}
    end;
close_sent(info, {tcp_closed, S}, _StatemData) ->
    rabbit_log_connection:debug("Stream protocol connection socket ~w closed [~w]",
                                [S, self()]),
    stop;
close_sent(info, {tcp_error, S, Reason}, #statem_data{}) ->
    rabbit_log_connection:error("Stream protocol connection socket error: ~tp "
                                "[~w] [~w]",
                                [Reason, S, self()]),
    stop;
close_sent(info, {resource_alarm, IsThereAlarm},
           StatemData = #statem_data{connection = Connection}) ->
    rabbit_log:warning("Stream protocol connection ignored a resource "
                       "alarm ~tp in state ~ts",
                       [IsThereAlarm, ?FUNCTION_NAME]),
    {keep_state,
     StatemData#statem_data{connection =
                                Connection#stream_connection{resource_alarm =
                                                                 IsThereAlarm}}};
close_sent(info, Msg, _StatemData) ->
    rabbit_log_connection:warning("Ignored unknown message ~tp in state ~ts",
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
                C1 = Connection0#stream_connection{auth_mechanism =
                                                       {Mechanism,
                                                        AuthMechanism}},
                {C2, CmdBody} =
                    case AuthMechanism:handle_response(SaslBin, AuthState) of
                        {refused, Username, Msg, Args} ->
                            rabbit_core_metrics:auth_attempt_failed(Host,
                                                                    Username,
                                                                    stream),
                            auth_fail(Username, Msg, Args, C1, State),
                            rabbit_log_connection:warning(Msg, Args),
                            {C1#stream_connection{connection_step = failure},
                             {sasl_authenticate,
                              ?RESPONSE_AUTHENTICATION_FAILURE, <<>>}};
                        {protocol_error, Msg, Args} ->
                            rabbit_core_metrics:auth_attempt_failed(Host,
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
                            rabbit_core_metrics:auth_attempt_succeeded(Host,
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
                                    rabbit_core_metrics:auth_attempt_succeeded(Host,
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
                                    rabbit_core_metrics:auth_attempt_failed(Host,
                                                                            Username,
                                                                            stream),
                                    rabbit_log_connection:warning("User '~ts' can only connect via localhost",
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
    rabbit_log_connection:debug("Tuning response ~tp ~tp ",
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
    rabbit_log:debug("Open frame received for ~ts", [VirtualHost]),
    Connection1 =
        try
            rabbit_access_control:check_vhost_access(User,
                                                     VirtualHost,
                                                     {socket, S},
                                                     #{}),
            AdvertisedHost =
                case TransportLayer of
                    tcp ->
                        rabbit_stream:host();
                    ssl ->
                        rabbit_stream:tls_host()
                end,
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

            rabbit_log:debug("sending open response ok ~ts", [VirtualHost]),
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
    rabbit_log_connection:info("Cannot create publisher ~tp on stream ~tp, connection "
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
                        {error, not_found} ->
                            response(Transport,
                                     Connection0,
                                     declare_publisher,
                                     CorrelationId,
                                     ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?STREAM_DOES_NOT_EXIST,
                                                                             1),
                            {Connection0, State};
                        {error, not_available} ->
                            response(Transport,
                                     Connection0,
                                     declare_publisher,
                                     CorrelationId,
                                     ?RESPONSE_CODE_STREAM_NOT_AVAILABLE),
                            rabbit_global_counters:increase_protocol_counter(stream,
                                                                             ?STREAM_NOT_AVAILABLE,
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
    {ResponseCode, Sequence} =
        case rabbit_stream_utils:check_read_permitted(#resource{name = Stream,
                                                                kind = queue,
                                                                virtual_host =
                                                                    VirtualHost},
                                                      User, #{})
        of
            ok ->
                case rabbit_stream_manager:lookup_leader(VirtualHost, Stream) of
                    {error, not_found} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_DOES_NOT_EXIST,
                                                                         1),
                        {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 0};
                    {ok, LeaderPid} ->
                        {?RESPONSE_CODE_OK,
                         case osiris:fetch_writer_seq(LeaderPid, Reference) of
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
                       #stream_connection{name = ConnName,
                                          socket = Socket,
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
                            rabbit_log:debug("Creating subscription ~tp to ~tp, with offset "
                                             "specification ~tp, properties ~0p",
                                             [SubscriptionId,
                                              Stream,
                                              OffsetSpec,
                                              Properties]),
                            Sac = single_active_consumer(Properties),
                            ConsumerName = consumer_name(Properties),
                            case {Sac, ConsumerName}
                            of
                                {true, undefined} ->
                                    rabbit_log:warning("Cannot create subcription ~tp, a single active "
                                                       "consumer must have a name",
                                                       [SubscriptionId]),
                                    response(Transport,
                                             Connection,
                                             subscribe,
                                             CorrelationId,
                                             ?RESPONSE_CODE_PRECONDITION_FAILED),
                                    rabbit_global_counters:increase_protocol_counter(stream,
                                                                                     ?PRECONDITION_FAILED,
                                                                                     1),
                                    {Connection, State};
                                _ ->
                                    Log = case Sac of
                                              true ->
                                                  undefined;
                                              false ->
                                                  init_reader(ConnTransport,
                                                              LocalMemberPid,
                                                              QueueResource,
                                                              SubscriptionId,
                                                              Properties,
                                                              OffsetSpec)
                                          end,

                                    ConsumerCounters =
                                        atomics:new(2, [{signed, false}]),

                                    response_ok(Transport,
                                                Connection,
                                                subscribe,
                                                CorrelationId),

                                    Active =
                                        maybe_register_consumer(VirtualHost,
                                                                Stream,
                                                                ConsumerName,
                                                                ConnName,
                                                                SubscriptionId,
                                                                Properties,
                                                                Sac),

                                    ConsumerConfiguration =
                                        #consumer_configuration{member_pid =
                                                                    LocalMemberPid,
                                                                subscription_id
                                                                    =
                                                                    SubscriptionId,
                                                                socket = Socket,
                                                                stream = Stream,
                                                                offset =
                                                                    OffsetSpec,
                                                                counters =
                                                                    ConsumerCounters,
                                                                properties =
                                                                    Properties,
                                                                active =
                                                                    Active},
                                    SendLimit = Credit div 2,
                                    ConsumerState =
                                        #consumer{configuration =
                                                      ConsumerConfiguration,
                                                  log = Log,
                                                  send_limit = SendLimit,
                                                  credit = Credit},

                                    Connection1 =
                                        maybe_monitor_stream(LocalMemberPid,
                                                             Stream,
                                                             Connection),

                                    State1 =
                                        maybe_dispatch_on_subscription(Transport,
                                                                       State,
                                                                       ConsumerState,
                                                                       Connection1,
                                                                       Consumers,
                                                                       Stream,
                                                                       SubscriptionId,
                                                                       Properties,
                                                                       SendFileOct,
                                                                       Sac),
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
                                    {Connection1#stream_connection{stream_subscriptions
                                                                       =
                                                                       StreamSubscriptions1},
                                     State1}
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
                                          send_file_oct = SendFileOct,
                                          deliver_version = DeliverVersion} =
                           Connection,
                       #stream_connection_state{consumers = Consumers} = State,
                       {credit, SubscriptionId, Credit}) ->
    case Consumers of
        #{SubscriptionId := #consumer{log = undefined} = Consumer} ->
            %% the consumer is not active, it's likely to be credit leftovers
            %% from a formerly active consumer. Taking the credits,
            %% logging and sending an error
            rabbit_log:debug("Giving credit to an inactive consumer: ~tp",
                             [SubscriptionId]),
            #consumer{credit = AvailableCredit} = Consumer,
            Consumer1 = Consumer#consumer{credit = AvailableCredit + Credit},

            Code = ?RESPONSE_CODE_PRECONDITION_FAILED,
            Frame =
                rabbit_stream_core:frame({response, 1,
                                          {credit, Code, SubscriptionId}}),
            send(Transport, S, Frame),
            rabbit_global_counters:increase_protocol_counter(stream,
                                                             ?PRECONDITION_FAILED,
                                                             1),
            {Connection,
             State#stream_connection_state{consumers =
                                           Consumers#{SubscriptionId => Consumer1}}};
        #{SubscriptionId := Consumer} ->
            #consumer{credit = AvailableCredit, last_listener_offset = LLO} =
                Consumer,
            case send_chunks(DeliverVersion,
                             Transport,
                             Consumer,
                             AvailableCredit + Credit,
                             LLO,
                             SendFileOct)
            of
                {error, closed} ->
                    rabbit_log_connection:info("Stream protocol connection has been closed by "
                                               "peer",
                                               []),
                    throw({stop, normal});
                {ok, Consumer1} ->
                    {Connection,
                     State#stream_connection_state{consumers =
                                                       Consumers#{SubscriptionId
                                                                      =>
                                                                      Consumer1}}}
            end;
        _ ->
            rabbit_log:warning("Giving credit to unknown subscription: ~tp",
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
                {error, Error} ->
                    rabbit_log:warning("Could not find leader to store offset on ~tp: "
                                       "~tp",
                                       [Stream, Error]),
                    %% FIXME store offset is fire-and-forget, so no response even if error, change this?
                    {Connection, State};
                {ClusterLeader, Connection1} ->
                    osiris:write_tracking(ClusterLeader, Reference, Offset),
                    {Connection1, State}
            end;
        error ->
            %% FIXME store offset is fire-and-forget, so no response even if error, change this?
            rabbit_log:warning("Not authorized to store offset on stream ~tp",
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
                    {error, not_found} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_DOES_NOT_EXIST,
                                                                         1),
                        {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, 0, Connection0};
                    {error, not_available} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_NOT_AVAILABLE,
                                                                         1),
                        {?RESPONSE_CODE_STREAM_NOT_AVAILABLE, 0, Connection0};
                    {LeaderPid, C} ->
                        {RC, O} =
                            case osiris:read_tracking(LeaderPid, Reference) of
                                undefined ->
                                    {?RESPONSE_CODE_NO_OFFSET, 0};
                                {offset, Offt} ->
                                    {?RESPONSE_CODE_OK, Offt}
                            end,
                        {RC, O, C}
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
    case rabbit_stream_utils:enforce_correct_name(Stream) of
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
                            rabbit_log:debug("Created stream cluster with leader on ~tp and "
                                             "replicas on ~tp",
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
                           {error, _} -> Acc
                       end
                    end,
                    #{}, Streams),

    Nodes0 =
        lists:sort(
            maps:keys(NodesMap)),
    %% filter out nodes in maintenance
    Nodes =
        lists:filter(fun(N) ->
                        rabbit_maintenance:is_being_drained_consistent_read(N)
                        =:= false
                     end,
                     Nodes0),
    NodeEndpoints =
        lists:foldr(fun(Node, Acc) ->
                       PortFunction =
                           case TransportLayer of
                               tcp -> port;
                               ssl -> tls_port
                           end,
                       Host = rpc:call(Node, rabbit_stream, host, []),
                       Port = rpc:call(Node, rabbit_stream, PortFunction, []),
                       case {is_binary(Host), is_integer(Port)} of
                           {true, true} -> Acc#{Node => {Host, Port}};
                           _ ->
                               rabbit_log:warning("Error when retrieving broker metadata: ~tp ~tp",
                                                  [Host, Port]),
                               Acc
                       end
                    end,
                    #{}, Nodes),

    Metadata =
        lists:foldl(fun(Stream, Acc) ->
                       case maps:get(Stream, Topology) of
                           {error, Err} -> Acc#{Stream => Err};
                           {ok,
                            #{leader_node := LeaderNode,
                              replica_nodes := Replicas}} ->
                               LeaderInfo =
                                   case NodeEndpoints of
                                       #{LeaderNode := Info} -> Info;
                                       _ -> undefined
                                   end,
                               ReplicaInfos =
                                   lists:foldr(fun(Replica, A) ->
                                                  case NodeEndpoints of
                                                      #{Replica := I} ->
                                                          [I | A];
                                                      _ -> A
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
    {ResponseCode, Streams} =
        case rabbit_stream_manager:route(RoutingKey, VirtualHost, SuperStream)
        of
            {ok, no_route} ->
                {?RESPONSE_CODE_OK, []};
            {ok, Strs} ->
                {?RESPONSE_CODE_OK, Strs};
            {error, _} ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?STREAM_DOES_NOT_EXIST,
                                                                 1),
                {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, []}
        end,

    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {route, ResponseCode, Streams}}),

    Transport:send(S, Frame),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost} =
                           Connection,
                       State,
                       {request, CorrelationId, {partitions, SuperStream}}) ->
    {ResponseCode, Partitions} =
        case rabbit_stream_manager:partitions(VirtualHost, SuperStream) of
            {ok, Streams} ->
                {?RESPONSE_CODE_OK, Streams};
            {error, _} ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?STREAM_DOES_NOT_EXIST,
                                                                 1),
                {?RESPONSE_CODE_STREAM_DOES_NOT_EXIST, []}
        end,

    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {partitions, ResponseCode, Partitions}}),

    Transport:send(S, Frame),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{transport = ConnTransport,
                                          outstanding_requests = Requests0,
                                          send_file_oct = SendFileOct,
                                          virtual_host = VirtualHost,
                                          deliver_version = DeliverVersion} =
                           Connection,
                       #stream_connection_state{consumers = Consumers} = State,
                       {response, CorrelationId,
                        {consumer_update, ResponseCode, ResponseOffsetSpec}}) ->
    case ResponseCode of
        ?RESPONSE_CODE_OK ->
            ok;
        RC ->
            rabbit_log:info("Unexpected consumer update response code: ~tp",
                            [RC])
    end,
    case maps:take(CorrelationId, Requests0) of
        {#request{content = #{subscription_id := SubscriptionId} = Msg}, Rs} ->
            Stream = stream_from_consumers(SubscriptionId, Consumers),
            rabbit_log:debug("Received consumer update response for subscription "
                             "~tp on stream ~tp, correlation ID ~tp",
                             [SubscriptionId, Stream, CorrelationId]),
            Consumers1 =
                case Consumers of
                    #{SubscriptionId :=
                          #consumer{configuration =
                                        #consumer_configuration{active =
                                                                    true}} =
                              Consumer} ->
                        %% active, dispatch messages
                        #consumer{configuration =
                                      #consumer_configuration{properties =
                                                                  Properties,
                                                              member_pid =
                                                                  LocalMemberPid,
                                                              offset =
                                                                  SubscriptionOffsetSpec}} =
                            Consumer,

                        OffsetSpec =
                            case ResponseOffsetSpec of
                                none ->
                                    SubscriptionOffsetSpec;
                                ROS ->
                                    ROS
                            end,

                        rabbit_log:debug("Initializing reader for active consumer "
                                         "(subscription ~tp, stream ~tp), offset "
                                         "spec is ~tp",
                                         [SubscriptionId, Stream, OffsetSpec]),
                        QueueResource =
                            #resource{name = Stream,
                                      kind = queue,
                                      virtual_host = VirtualHost},

                        Segment =
                            init_reader(ConnTransport,
                                        LocalMemberPid,
                                        QueueResource,
                                        SubscriptionId,
                                        Properties,
                                        OffsetSpec),
                        Consumer1 = Consumer#consumer{log = Segment},
                        #consumer{credit = Crdt,
                                  send_limit = SndLmt,
                                  configuration = #consumer_configuration{counters = ConsumerCounters}} = Consumer1,

                        rabbit_log:debug("Dispatching to subscription ~tp (stream ~tp), "
                                         "credit(s) ~tp, send limit ~tp",
                                         [SubscriptionId,
                                          Stream,
                                          Crdt,
                                          SndLmt]),

                        ConsumedMessagesBefore = messages_consumed(ConsumerCounters),

                        Consumer2 =
                            case send_chunks(DeliverVersion,
                                             Transport,
                                             Consumer1,
                                             SendFileOct)
                            of
                                {error, closed} ->
                                    rabbit_log_connection:info("Stream protocol connection has been closed by "
                                                               "peer",
                                                               []),
                                    throw({stop, normal});
                                {error, Reason} ->
                                    rabbit_log_connection:info("Error while sending chunks: ~tp",
                                                               [Reason]),
                                    %% likely a connection problem
                                    Consumer;
                                {ok, Csmr} ->
                                    Csmr
                            end,
                        #consumer{log = Log2} = Consumer2,
                        ConsumerOffset = osiris_log:next_offset(Log2),

                        ConsumedMessagesAfter = messages_consumed(ConsumerCounters),
                        rabbit_log:debug("Subscription ~tp (stream ~tp) is now at offset ~tp with ~tp "
                                         "message(s) distributed after subscription",
                                         [SubscriptionId,
                                          Stream,
                                          ConsumerOffset,
                                          ConsumedMessagesAfter - ConsumedMessagesBefore]),

                        Consumers#{SubscriptionId => Consumer2};
                    #{SubscriptionId :=
                          #consumer{configuration =
                                        #consumer_configuration{active = false,
                                                                stream = Stream,
                                                                properties =
                                                                    Properties}}} ->
                        rabbit_log:debug("Not an active consumer"),

                        case Msg of
                            #{stepping_down := true} ->
                                ConsumerName = consumer_name(Properties),
                                rabbit_log:debug("Subscription ~tp on stream ~tp, group ~tp " ++
                                                 "has stepped down, activating consumer",
                                                 [SubscriptionId, Stream, ConsumerName]),
                                _ = rabbit_stream_sac_coordinator:activate_consumer(VirtualHost,
                                                                                    Stream,
                                                                                    ConsumerName),
                                ok;
                            _ ->
                                ok
                        end,

                        Consumers;
                    _ ->
                        rabbit_log:debug("No consumer found for subscription ~tp",
                                         [SubscriptionId]),
                        Consumers
                end,

            {Connection#stream_connection{outstanding_requests = Rs},
             State#stream_connection_state{consumers = Consumers1}};
        {V, _Rs} ->
            rabbit_log:warning("Unexpected outstanding requests for correlation "
                               "ID ~tp: ~tp",
                               [CorrelationId, V]),
            {Connection, State};
        error ->
            rabbit_log:warning("Could not find outstanding consumer update request "
                               "with correlation ID ~tp. No actions taken for "
                               "the subscription.",
                               [CorrelationId]),
            {Connection, State}
    end;
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S} = Connection0,
                       State,
                       {request, CorrelationId,
                        {exchange_command_versions, CommandVersions}}) ->
    Frame =
        rabbit_stream_core:frame({response, CorrelationId,
                                  {exchange_command_versions, ?RESPONSE_CODE_OK,
                                   rabbit_stream_utils:command_versions()}}),
    send(Transport, S, Frame),

    %% adapt connection handlers to client capabilities
    Connection1 =
        process_client_command_versions(Connection0, CommandVersions),
    {Connection1, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S,
                                          virtual_host = VirtualHost,
                                          user = User} =
                           Connection,
                       State,
                       {request, CorrelationId, {stream_stats, Stream}}) ->
    QueueResource =
        #resource{name = Stream,
                  kind = queue,
                  virtual_host = VirtualHost},
    Response =
        case rabbit_stream_utils:check_read_permitted(QueueResource, User,
                                                      #{})
        of
            ok ->
                case rabbit_stream_manager:lookup_member(VirtualHost, Stream) of
                    {error, not_available} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_NOT_AVAILABLE,
                                                                         1),
                        {stream_stats, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE,
                         #{}};
                    {error, not_found} ->
                        rabbit_global_counters:increase_protocol_counter(stream,
                                                                         ?STREAM_DOES_NOT_EXIST,
                                                                         1),
                        {stream_stats, ?RESPONSE_CODE_STREAM_DOES_NOT_EXIST,
                         #{}};
                    {ok, MemberPid} ->
                        StreamStats =
                            maps:fold(fun(K, V, Acc) ->
                                         Acc#{atom_to_binary(K) => V}
                                      end,
                                      #{}, osiris:get_stats(MemberPid)),
                        {stream_stats, ?RESPONSE_CODE_OK, StreamStats}
                end;
            error ->
                rabbit_global_counters:increase_protocol_counter(stream,
                                                                 ?ACCESS_REFUSED,
                                                                 1),
                {stream_stats, ?RESPONSE_CODE_ACCESS_REFUSED, #{}}
        end,
    Frame = rabbit_stream_core:frame({response, CorrelationId, Response}),
    send(Transport, S, Frame),
    {Connection, State};
handle_frame_post_auth(Transport,
                       #stream_connection{socket = S} = Connection,
                       State,
                       {request, CorrelationId,
                        {close, ClosingCode, ClosingReason}}) ->
    rabbit_log:debug("Stream protocol reader received close command "
                     "~tp ~tp",
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
    rabbit_log:warning("unknown command ~tp, sending close command.",
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

process_client_command_versions(C, []) ->
    C;
process_client_command_versions(C, [H | T]) ->
    process_client_command_versions(process_client_command_api(C, H), T).

process_client_command_api(C, {deliver, _, ?VERSION_2}) ->
    C#stream_connection{deliver_version = ?VERSION_2};
process_client_command_api(C, _) ->
    C.

init_reader(ConnectionTransport,
            LocalMemberPid,
            QueueResource,
            SubscriptionId,
            Properties,
            OffsetSpec) ->
    CounterSpec = {{?MODULE, QueueResource, SubscriptionId, self()}, []},
    Options =
        #{transport => ConnectionTransport,
          chunk_selector => get_chunk_selector(Properties)},
    {ok, Segment} =
        osiris:init_reader(LocalMemberPid, OffsetSpec, CounterSpec, Options),
    rabbit_log:debug("Next offset for subscription ~tp is ~tp",
                     [SubscriptionId, osiris_log:next_offset(Segment)]),
    Segment.


single_active_consumer(#consumer{configuration =
                                 #consumer_configuration{properties = Properties}}) ->
    single_active_consumer(Properties);
single_active_consumer(#{<<"single-active-consumer">> :=
                             <<"true">>}) ->
    true;
single_active_consumer(_Properties) ->
    false.

consumer_name(#{<<"name">> := Name}) ->
    Name;
consumer_name(_Properties) ->
    undefined.

maybe_dispatch_on_subscription(Transport,
                               State,
                               ConsumerState,
                               #stream_connection{deliver_version =
                                                      DeliverVersion} =
                                   Connection,
                               Consumers,
                               Stream,
                               SubscriptionId,
                               SubscriptionProperties,
                               SendFileOct,
                               false = _Sac) ->
    rabbit_log:debug("Distributing existing messages to subscription "
                     "~tp on ~tp",
                     [SubscriptionId, Stream]),
    case send_chunks(DeliverVersion,
                     Transport,
                     ConsumerState,
                     SendFileOct)
    of
        {error, closed} ->
            rabbit_log_connection:info("Stream protocol connection has been closed by "
                                       "peer",
                                       []),
            throw({stop, normal});
        {ok, #consumer{log = Log1, credit = Credit1} = ConsumerState1} ->
            Consumers1 = Consumers#{SubscriptionId => ConsumerState1},

            #consumer{configuration =
                          #consumer_configuration{counters =
                                                      ConsumerCounters1}} =
                ConsumerState1,

            ConsumerOffset = osiris_log:next_offset(Log1),
            ConsumerOffsetLag = consumer_i(offset_lag, ConsumerState1),

            rabbit_log:debug("Subscription ~tp on ~tp is now at offset ~tp with ~tp "
                             "message(s) distributed after subscription",
                             [SubscriptionId, Stream, ConsumerOffset,
                              messages_consumed(ConsumerCounters1)]),

            rabbit_stream_metrics:consumer_created(self(),
                                                   stream_r(Stream, Connection),
                                                   SubscriptionId,
                                                   Credit1,
                                                   messages_consumed(ConsumerCounters1),
                                                   ConsumerOffset,
                                                   ConsumerOffsetLag,
                                                   true,
                                                   SubscriptionProperties),
            State#stream_connection_state{consumers = Consumers1}
    end;
maybe_dispatch_on_subscription(_Transport,
                               State,
                               ConsumerState,
                               Connection,
                               Consumers,
                               Stream,
                               SubscriptionId,
                               SubscriptionProperties,
                               _SendFileOct,
                               true = _Sac) ->
    rabbit_log:debug("No initial dispatch for subscription ~tp for "
                     "now, waiting for consumer update response from "
                     "client (single active consumer)",
                     [SubscriptionId]),
    #consumer{credit = Credit,
              configuration =
                  #consumer_configuration{offset = Offset, active = Active}} =
        ConsumerState,

    rabbit_stream_metrics:consumer_created(self(),
                                           stream_r(Stream, Connection),
                                           SubscriptionId,
                                           Credit,
                                           0, %% messages consumed
                                           Offset,
                                           0, %% offset lag
                                           Active,
                                           SubscriptionProperties),
    Consumers1 = Consumers#{SubscriptionId => ConsumerState},
    State#stream_connection_state{consumers = Consumers1}.

maybe_register_consumer(_, _, _, _, _, _, false = _Sac) ->
    true;
maybe_register_consumer(VirtualHost,
                        Stream,
                        ConsumerName,
                        ConnectionName,
                        SubscriptionId,
                        Properties,
                        true) ->
    PartitionIndex = partition_index(VirtualHost, Stream, Properties),
    {ok, Active} =
        rabbit_stream_sac_coordinator:register_consumer(VirtualHost,
                                                        Stream,
                                                        PartitionIndex,
                                                        ConsumerName,
                                                        self(),
                                                        ConnectionName,
                                                        SubscriptionId),
    Active.

maybe_send_consumer_update(Transport,
                           Connection = #stream_connection{
                                           socket = S,
                                           correlation_id_sequence = CorrIdSeq},
                           Consumer,
                           Active,
                           Msg) ->
    #consumer{configuration =
              #consumer_configuration{subscription_id = SubscriptionId}} = Consumer,
    Frame = rabbit_stream_core:frame({request, CorrIdSeq,
                                      {consumer_update, SubscriptionId, Active}}),

    Connection1 = register_request(Connection, Msg),

    send(Transport, S, Frame),
    Connection1.

register_request(#stream_connection{outstanding_requests = Requests0,
                                    correlation_id_sequence = CorrIdSeq} = C,
                 RequestContent) ->
    rabbit_log:debug("Registering RPC request ~tp with correlation ID ~tp",
                     [RequestContent, CorrIdSeq]),

    Requests1 = maps:put(CorrIdSeq, request(RequestContent), Requests0),

    ensure_outstanding_requests_timer(
      C#stream_connection{correlation_id_sequence = CorrIdSeq + 1,
                          outstanding_requests = Requests1}).

request(Content) ->
    #request{start = erlang:monotonic_time(millisecond),
             content = Content}.

ensure_outstanding_requests_timer(#stream_connection{
                                     outstanding_requests = Requests,
                                     outstanding_requests_timer = undefined
                                    } = C) when map_size(Requests) =:= 0 ->
    C;
ensure_outstanding_requests_timer(#stream_connection{
                                     outstanding_requests = Requests,
                                     outstanding_requests_timer = TRef
                                    } = C) when map_size(Requests) =:= 0 ->
    _ = erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
    C#stream_connection{outstanding_requests_timer = undefined};
ensure_outstanding_requests_timer(#stream_connection{
                                     outstanding_requests = Requests,
                                     outstanding_requests_timer = undefined,
                                     request_timeout = Timeout
                                    } = C) when map_size(Requests) > 0 ->
    TRef = erlang:send_after(Timeout, self(), check_outstanding_requests),
    C#stream_connection{outstanding_requests_timer = TRef};
ensure_outstanding_requests_timer(C) ->
    C.

maybe_unregister_consumer(_, _, false = _Sac, Requests) ->
    Requests;
maybe_unregister_consumer(VirtualHost,
                          #consumer{configuration =
                                        #consumer_configuration{stream = Stream,
                                                                properties =
                                                                    Properties,
                                                                subscription_id
                                                                    =
                                                                    SubscriptionId}},
                          true = _Sac,
                          Requests) ->
    ConsumerName = consumer_name(Properties),

    Requests1 = maps:fold(
                  fun(_, #request{content =
                                  #{active := false,
                                    subscription_id := SubId,
                                    stepping_down := true}}, Acc) when SubId =:= SubscriptionId ->
                          _ = rabbit_stream_sac_coordinator:activate_consumer(VirtualHost,
                                                                              Stream,
                                                                              ConsumerName),
                          rabbit_log:debug("Outstanding SAC activation request for stream '~tp', " ++
                                           "group '~tp', sending activation.",
                                           [Stream, ConsumerName]),
                          Acc;
                     (K, V, Acc) ->
                          Acc#{K => V}
                  end, maps:new(), Requests),

    _ = rabbit_stream_sac_coordinator:unregister_consumer(VirtualHost,
                                                          Stream,
                                                          ConsumerName,
                                                          self(),
                                                          SubscriptionId),
    Requests1.

partition_index(VirtualHost, Stream, Properties) ->
    case Properties of
        #{<<"super-stream">> := SuperStream} ->
            case rabbit_stream_manager:partition_index(VirtualHost, SuperStream,
                                                       Stream)
            of
                {ok, Index} ->
                    Index;
                _ ->
                    -1
            end;
        _ ->
            -1
    end.

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
     || #consumer{configuration =
                      #consumer_configuration{stream = S,
                                              subscription_id = SubId}}
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
    rabbit_log_connection:warning("ignored command on close ~tp .",
                                  [Command]),
    {Connection, State}.

stream_r(Stream, #stream_connection{virtual_host = VHost}) ->
    #resource{name = Stream,
              kind = queue,
              virtual_host = VHost}.

clean_state_after_stream_deletion_or_failure(Stream,
                                             #stream_connection{virtual_host =
                                                                    VirtualHost,
                                                                stream_subscriptions
                                                                    =
                                                                    StreamSubscriptions,
                                                                publishers =
                                                                    Publishers,
                                                                publisher_to_ids
                                                                    =
                                                                    PublisherToIds,
                                                                stream_leaders =
                                                                    Leaders,
                                                                outstanding_requests = Requests0} =
                                                 C0,
                                             #stream_connection_state{consumers
                                                                          =
                                                                          Consumers} =
                                                 S0) ->
    {SubscriptionsCleaned, C1, S1} =
        case stream_has_subscriptions(Stream, C0) of
            true ->
                #{Stream := SubscriptionIds} = StreamSubscriptions,
                Requests1 = lists:foldl(
                              fun(SubId, Rqsts0) ->
                                      rabbit_stream_metrics:consumer_cancelled(self(),
                                                                               stream_r(Stream,
                                                                                        C0),
                                                                               SubId),
                                      #{SubId := Consumer} = Consumers,
                                      Rqsts1 = maybe_unregister_consumer(
                                                 VirtualHost, Consumer,
                                                 single_active_consumer(Consumer),
                                                 Rqsts0),
                                      Rqsts1
                              end, Requests0, SubscriptionIds),
                {true,
                 C0#stream_connection{stream_subscriptions =
                                          maps:remove(Stream,
                                                      StreamSubscriptions),
                                      outstanding_requests = Requests1},
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
                                     _ -> {Pubs, PubToIds}
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
                {error, Error} ->
                    {error, Error};
                {ok, LeaderPid} ->
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
                    #stream_connection{virtual_host = VirtualHost,
                                       outstanding_requests = Requests0,
                                       stream_subscriptions =
                                           StreamSubscriptions} =
                        Connection,
                    #stream_connection_state{consumers = Consumers} = State) ->
    #{SubscriptionId := Consumer} = Consumers,
    #consumer{log = Log,
              configuration = #consumer_configuration{stream = Stream}} =
        Consumer,
    rabbit_log:debug("Deleting subscription ~tp (stream ~tp)",
                     [SubscriptionId, Stream]),
    close_log(Log),
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

    Requests1 = maybe_unregister_consumer(
                  VirtualHost, Consumer,
                  single_active_consumer(
                    Consumer#consumer.configuration#consumer_configuration.properties),
                  Requests0),
    {Connection2#stream_connection{outstanding_requests = Requests1},
     State#stream_connection_state{consumers = Consumers1}}.

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
                         _ -> maps:put(MonitorRef, Strm, Acc)
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
                     Stream -> true;
                     _ -> false
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

send_file_callback(?VERSION_1,
                   Transport,
                   _Log,
                   #consumer{configuration =
                                 #consumer_configuration{socket = S,
                                                         subscription_id =
                                                             SubscriptionId,
                                                         counters = Counters}},
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
    end;
send_file_callback(?VERSION_2,
                   Transport,
                   Log,
                   #consumer{configuration =
                                 #consumer_configuration{socket = S,
                                                         subscription_id =
                                                             SubscriptionId,
                                                         counters = Counters}},
                   Counter) ->
    fun(#{chunk_id := FirstOffsetInChunk, num_entries := NumEntries},
        Size) ->
       FrameSize = 2 + 2 + 1 + 8 + Size,
       CommittedChunkId = osiris_log:committed_offset(Log),
       FrameBeginning =
           <<FrameSize:32,
             ?REQUEST:1,
             ?COMMAND_DELIVER:15,
             ?VERSION_2:16,
             SubscriptionId:8/unsigned,
             CommittedChunkId:64>>,
       Transport:send(S, FrameBeginning),
       atomics:add(Counter, 1, Size),
       increase_messages_consumed(Counters, NumEntries),
       set_consumer_offset(Counters, FirstOffsetInChunk)
    end.

send_chunks(DeliverVersion,
            Transport,
            #consumer{credit = Credit, last_listener_offset = LastLstOffset} =
                Consumer,
            Counter) ->
    send_chunks(DeliverVersion,
                Transport,
                Consumer,
                Credit,
                LastLstOffset,
                Counter).

send_chunks(_DeliverVersion,
            _Transport,
            #consumer{send_limit = SendLimit} = Consumer,
            Credit,
            LastLstOffset,
            _Counter) when Credit =< SendLimit ->
    %% there are fewer credits than the credit limit so we won't enter
    %% the send_chunks loop until we have more than the limit available.
    %% Once we have that we are able to consume all credits all the way down
    %% to zero
    {ok,
     Consumer#consumer{credit = Credit, last_listener_offset = LastLstOffset}};
send_chunks(DeliverVersion,
            Transport,
            #consumer{configuration = #consumer_configuration{socket = Socket},
                      log = Log} = Consumer,
            Credit,
            LastLstOffset,
            Counter) ->
    setopts(Transport, Socket, [{nopush, true}]),
    send_chunks(DeliverVersion,
                Transport,
                Consumer,
                Log,
                Credit,
                LastLstOffset,
                true,
                Counter).

send_chunks(_DeliverVersion,
            Transport,
            #consumer{
                      configuration = #consumer_configuration{socket = Socket}} =
                Consumer,
            Log,
            0,
            LastLstOffset,
            _Retry,
            _Counter) ->
    %% we have finished sending so need to uncork
    setopts(Transport, Socket, [{nopush, false}]),
    {ok,
     Consumer#consumer{log = Log,
                       credit = 0,
                       last_listener_offset = LastLstOffset}};
send_chunks(DeliverVersion,
            Transport,
            #consumer{configuration = #consumer_configuration{socket = Socket}} =
                Consumer,
            Log,
            Credit,
            LastLstOffset,
            Retry,
            Counter) ->
    case osiris_log:send_file(Socket, Log,
                              send_file_callback(DeliverVersion,
                                                 Transport,
                                                 Log,
                                                 Consumer,
                                                 Counter))
    of
        {ok, Log1} ->
            send_chunks(DeliverVersion,
                        Transport,
                        Consumer,
                        Log1,
                        Credit - 1,
                        LastLstOffset,
                        true,
                        Counter);
        {error, closed} ->
            {error, closed};
        {error, enotconn} ->
            {error, closed};
        {error, Reason} ->
            {error, Reason};
        {end_of_stream, Log1} ->
            setopts(Transport, Socket, [{nopush, false}]),
            case Retry of
                true ->
                    timer:sleep(1),
                    send_chunks(DeliverVersion,
                                Transport,
                                Consumer,
                                Log1,
                                Credit,
                                LastLstOffset,
                                false,
                                Counter);
                false ->
                    #consumer{configuration =
                                  #consumer_configuration{member_pid =
                                                              LocalMember}} =
                        Consumer,
                    NextOffset = osiris_log:next_offset(Log1),
                    LLO = case {LastLstOffset, NextOffset > LastLstOffset} of
                              {undefined, _} ->
                                  osiris:register_offset_listener(LocalMember,
                                                                  NextOffset),
                                  NextOffset;
                              {_, true} ->
                                  osiris:register_offset_listener(LocalMember,
                                                                  NextOffset),
                                  NextOffset;
                              _ ->
                                  LastLstOffset
                          end,
                    {ok,
                     Consumer#consumer{log = Log1,
                                       credit = Credit,
                                       last_listener_offset = LLO}}
            end
    end.

emit_stats(#stream_connection{publishers = Publishers} = Connection,
           #stream_connection_state{consumers = Consumers} = ConnectionState) ->
    [{_, Pid}, {_, Recv_oct}, {_, Send_oct}, {_, Reductions}] =
        infos(?SIMPLE_METRICS, Connection, ConnectionState),
    Infos = infos(?OTHER_METRICS, Connection, ConnectionState),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid,
                                         Recv_oct,
                                         Send_oct,
                                         Reductions),
    [rabbit_stream_metrics:consumer_updated(self(),
                                            stream_r(S, Connection),
                                            Id,
                                            Credit,
                                            messages_consumed(Counters),
                                            consumer_offset(Counters),
                                            consumer_i(offset_lag, Consumer),
                                            Active,
                                            Properties)
     || #consumer{configuration =
                      #consumer_configuration{stream = S,
                                              subscription_id = Id,
                                              counters = Counters,
                                              active = Active,
                                              properties = Properties},
                  credit = Credit} =
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
    gen_server2:call(Pid, {consumers_info, InfoItems}).

consumers_infos(Items,
                #stream_connection_state{consumers = Consumers}) ->
    [[{Item, consumer_i(Item, Consumer)} || Item <- Items]
     || Consumer <- maps:values(Consumers)].

consumer_i(subscription_id,
           #consumer{configuration =
                         #consumer_configuration{subscription_id = SubId}}) ->
    SubId;
consumer_i(credits, #consumer{credit = Credits}) ->
    Credits;
consumer_i(messages_consumed,
           #consumer{configuration =
                         #consumer_configuration{counters = Counters}}) ->
    messages_consumed(Counters);
consumer_i(offset,
           #consumer{configuration =
                         #consumer_configuration{counters = Counters}}) ->
    consumer_offset(Counters);
consumer_i(offset_lag, #consumer{log = undefined}) ->
    0;
consumer_i(offset_lag,
           #consumer{configuration =
                         #consumer_configuration{counters = Counters},
                     log = Log}) ->
    stream_stored_offset(Log) - consumer_offset(Counters);
consumer_i(connection_pid, _) ->
    self();
consumer_i(properties,
           #consumer{configuration =
                         #consumer_configuration{properties = Properties}}) ->
    Properties;
consumer_i(stream,
           #consumer{configuration =
                         #consumer_configuration{stream = Stream}}) ->
    Stream;
consumer_i(active,
           #consumer{configuration =
                         #consumer_configuration{active = Active}}) ->
    Active;
consumer_i(activity_status,
           #consumer{configuration =
                         #consumer_configuration{active = Active,
                                                 properties = Properties}}) ->
    rabbit_stream_utils:consumer_activity_status(Active, Properties);
consumer_i(_Unknown, _) ->
    ?UNKNOWN_FIELD.

publishers_info(Pid, InfoItems) ->
    gen_server2:call(Pid, {publishers_info, InfoItems}).

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
    messages_errored(Counters);
publisher_i(_Unknow, _) ->
    ?UNKNOWN_FIELD.

info(Pid, InfoItems) ->
    gen_server2:call(Pid, {info, InfoItems}, infinity).

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
i(SSL, #stream_connection{socket = Sock, proxy_socket = ProxySock}, _)
  when SSL =:= ssl;
       SSL =:= ssl_protocol;
       SSL =:= ssl_key_exchange;
       SSL =:= ssl_cipher;
       SSL =:= ssl_hash ->
    rabbit_ssl:info(SSL, {Sock, ProxySock});
i(Cert, #stream_connection{socket = Sock},_)
  when Cert =:= peer_cert_issuer;
       Cert =:= peer_cert_subject;
       Cert =:= peer_cert_validity ->
    rabbit_ssl:cert_info(Cert, Sock);
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
i(_Unknown, _, _) ->
    ?UNKNOWN_FIELD.

-spec send(module(), rabbit_net:socket(), iodata()) -> ok.
send(Transport, Socket, Data) when is_atom(Transport) ->
    Transport:send(Socket, Data).

get_chunk_selector(Properties) ->
    binary_to_atom(maps:get(<<"chunk_selector">>, Properties,
                            <<"user_data">>)).

close_log(undefined) ->
    ok;
close_log(Log) ->
    osiris_log:close(Log).

setopts(Transport, Sock, Opts) ->
    ok = Transport:setopts(Sock, Opts).

stream_from_consumers(SubId, Consumers) ->
    case Consumers of
        #{SubId := #consumer{configuration = #consumer_configuration{stream = S}}} ->
            S;
        _ ->
            undefined
    end.

