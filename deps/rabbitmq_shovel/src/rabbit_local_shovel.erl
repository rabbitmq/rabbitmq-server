%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_local_shovel).

-behaviour(rabbit_shovel_behaviour).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_shovel.hrl").

-rabbit_boot_step({rabbit_global_local_shovel_counters,
                   [{description, "global local shovel counters"},
                    {mfa,         {?MODULE, boot_step,
                                   []}},
                    {requires,    rabbit_global_counters},
                    {enables,     external_infrastructure}]}).

-rabbit_boot_step(
   {rabbit_local_shovel_protocol,
    [{description, "Local shovel protocol"},
     {mfa,      {rabbit_registry, register,
                 [shovel_protocol, <<"local">>, ?MODULE]}},
     {cleanup,  {rabbit_registry, unregister,
                 [shovel_protocol, <<"local">>]}},
     {requires, rabbit_registry}]}).

-export([
         boot_step/0,
         conserve_resources/3,
         parse/2,
         parse_source/1,
         parse_dest/4,
         validate_src/1,
         validate_dest/1,
         validate_src_funs/2,
         validate_dest_funs/2,
         connect_source/1,
         connect_dest/1,
         init_source/1,
         init_dest/1,
         source_uri/1,
         dest_uri/1,
         source_protocol/1,
         dest_protocol/1,
         source_endpoint/1,
         dest_endpoint/1,
         close_dest/1,
         close_source/1,
         handle_source/2,
         handle_dest/2,
         ack/3,
         nack/3,
         forward/3,
         status/1,
         pending_count/1
        ]).

-export([
         src_decl_exchange/4,
         decl_queue/4,
         dest_decl_queue/4,
         check_queue/4,
         dest_check_queue/4,
         decl_fun/3,
         check_fun/3
        ]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_shovel_util, [pget2count/3,
                             deobfuscated_uris/2,
                             validate_uri_fun/1]).

-define(APP, rabbitmq_shovel).
-define(QUEUE, lqueue).
%% "Note that, despite its name, the delivery-count is not a count but a
%% sequence number initialized at an arbitrary point by the sender."
%% See rabbit_amqp_session.erl
-define(INITIAL_DELIVERY_COUNT, 16#ff_ff_ff_ff - 4).
-define(DEFAULT_MAX_LINK_CREDIT, 1000).
-define(PROTOCOL, 'local-shovel').

-record(pending_ack, {
                      delivery_tag,
                      msg_id
                     }).

boot_step() ->
    Labels = #{protocol => ?PROTOCOL},
    rabbit_global_counters:init(Labels),
    rabbit_global_counters:init(Labels#{queue_type => rabbit_classic_queue}),
    rabbit_global_counters:init(Labels#{queue_type => rabbit_quorum_queue}),
    rabbit_global_counters:init(Labels#{queue_type => rabbit_stream_queue}).

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, Source, {_, Conserve, _}) ->
    gen_server:cast(Pid, {conserve_resources, Source, Conserve}).

parse(_Name, {source, Source}) ->
    Queue = parse_parameter(queue, fun parse_binary/1,
                            proplists:get_value(queue, Source)),
    CArgs = proplists:get_value(consumer_args, Source, []),
    CTag = proplists:get_value(consumer_name, Source, <<>>),
    #{module => ?MODULE,
      uris => proplists:get_value(uris, Source),
      resource_decl => rabbit_shovel_util:decl_fun(?MODULE, {source, Source}),
      queue => Queue,
      delete_after => proplists:get_value(delete_after, Source, never),
      consumer_args => CArgs,
      consumer_name => CTag};
parse(_Name, {destination, Dest}) ->
    Exchange = parse_parameter(dest_exchange, fun parse_binary/1,
                               proplists:get_value(dest_exchange, Dest, none)),
    RK = parse_parameter(dest_exchange_key, fun parse_binary/1,
                         proplists:get_value(dest_routing_key, Dest, none)),
    #{module => ?MODULE,
      uris => proplists:get_value(uris, Dest),
      resource_decl  => rabbit_shovel_util:decl_fun(?MODULE, {destination, Dest}),
      exchange => Exchange,
      routing_key => RK,
      add_forward_headers => proplists:get_value(add_forward_headers, Dest, false),
      add_timestamp_header => proplists:get_value(add_timestamp_header, Dest, false)}.

parse_source(Def) ->
    %% TODO add exchange source back
    Mod      = rabbit_local_shovel,
    SrcURIs  = deobfuscated_uris(<<"src-uri">>, Def),
    SrcX     = pget(<<"src-exchange">>,Def, none),
    SrcXKey  = pget(<<"src-exchange-key">>, Def, <<>>),
    SrcQ     = pget(<<"src-queue">>, Def, none),
    SrcQArgs = pget(<<"src-queue-args">>,   Def, #{}),
    SrcCArgs = rabbit_misc:to_amqp_table(pget(<<"src-consumer-args">>, Def, [])),
    SrcCTag = pget(<<"src-consumer-name">>, Def, <<>>),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"src-predeclared">>, Def, GlobalPredeclared),
    {SrcDeclFun, Queue, DestHeaders} =
    case SrcQ of
        none -> {{Mod, src_decl_exchange, [SrcX, SrcXKey]}, <<>>,
                 [{<<"src-exchange">>,     SrcX},
                  {<<"src-exchange-key">>, SrcXKey}]};
        _ -> case Predeclared of
                false ->
                    {{Mod, decl_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]};
                true ->
                    {{Mod, check_queue, [SrcQ, SrcQArgs]},
                        SrcQ, [{<<"src-queue">>, SrcQ}]}
            end
    end,
    DeleteAfter = pget(<<"src-delete-after">>, Def,
                       pget(<<"delete-after">>, Def, <<"never">>)),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{exchange, SrcX},
                                                   {routing_key, SrcXKey}],
                                        V =/= none]),
    {maps:merge(#{module => Mod,
                  uris => SrcURIs,
                  resource_decl => SrcDeclFun,
                  queue => Queue,
                  delete_after => opt_b2a(DeleteAfter),
                  consumer_args => SrcCArgs,
                  consumer_name => SrcCTag
                 }, Details), DestHeaders}.

parse_dest({_VHost, _Name}, _ClusterName, Def, _SourceHeaders) ->
    Mod       = rabbit_local_shovel,
    DestURIs  = deobfuscated_uris(<<"dest-uri">>,      Def),
    DestX     = pget(<<"dest-exchange">>,     Def, none),
    DestXKey  = pget(<<"dest-exchange-key">>, Def, none),
    DestQ     = pget(<<"dest-queue">>,        Def, none),
    DestQArgs = pget(<<"dest-queue-args">>,   Def, #{}),
    GlobalPredeclared = proplists:get_value(predeclared, application:get_env(?APP, topology, []), false),
    Predeclared = pget(<<"dest-predeclared">>, Def, GlobalPredeclared),
    DestDeclFun = case Predeclared of
        true -> {Mod, dest_check_queue, [DestQ, DestQArgs]};
        false -> {Mod, dest_decl_queue, [DestQ, DestQArgs]}
    end,

    AddHeaders = pget(<<"dest-add-forward-headers">>, Def, false),
    AddTimestampHeader = pget(<<"dest-add-timestamp-header">>, Def, false),
    %% Details are only used for status report in rabbitmqctl, as vhost is not
    %% available to query the runtime parameters.
    Details = maps:from_list([{K, V} || {K, V} <- [{exchange, DestX},
                                                   {routing_key, DestXKey},
                                                   {queue, DestQ}],
                                        V =/= none]),
    maps:merge(#{module => rabbit_local_shovel,
                 uris => DestURIs,
                 resource_decl => DestDeclFun,
                 add_forward_headers => AddHeaders,
                 add_timestamp_header => AddTimestampHeader
                }, Details).

validate_src(Def) ->
    [case pget2count(<<"src-exchange">>, <<"src-queue">>, Def) of
         zero -> {error, "Must specify 'src-exchange' or 'src-queue'", []};
         one  -> ok;
         both -> {error, "Cannot specify 'src-exchange' and 'src-queue'", []}
     end,
     case {pget(<<"src-delete-after">>, Def, pget(<<"delete-after">>, Def)), pget(<<"ack-mode">>, Def)} of
         {N, <<"no-ack">>} when is_integer(N) ->
             {error, "Cannot specify 'no-ack' and numerical 'delete-after'", []};
         _ ->
             ok
     end].

validate_dest(Def) ->
    [case pget2count(<<"dest-exchange">>, <<"dest-queue">>, Def) of
         zero -> ok;
         one  -> ok;
         both -> {error, "Cannot specify 'dest-exchange' and 'dest-queue'", []}
     end].

validate_src_funs(_Def, User) ->
    [
     {<<"src-uri">>, validate_uri_fun(User), mandatory},
     {<<"src-exchange">>,     fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-exchange-key">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-queue-args">>,   fun rabbit_shovel_util:validate_queue_args/2, optional},
     {<<"src-consumer-args">>, fun rabbit_shovel_util:validate_consumer_args/2, optional},
     {<<"src-consumer-name">>, fun rabbit_parameter_validation:binary/2, optional},
     {<<"src-delete-after">>, fun rabbit_shovel_util:validate_delete_after/2, optional},
     {<<"src-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

validate_dest_funs(_Def, User) ->
    [{<<"dest-uri">>,        validate_uri_fun(User), mandatory},
     {<<"dest-exchange">>,   fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-exchange-key">>,fun rabbit_parameter_validation:binary/2,optional},
     {<<"dest-queue">>,      fun rabbit_parameter_validation:amqp091_queue_name/2,optional},
     {<<"dest-queue-args">>, fun rabbit_shovel_util:validate_queue_args/2, optional},
     {<<"dest-add-forward-headers">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-add-timestamp-header">>, fun rabbit_parameter_validation:boolean/2,optional},
     {<<"dest-predeclared">>,  fun rabbit_parameter_validation:boolean/2, optional}
    ].

connect_source(State = #{source := Src = #{resource_decl := {M, F, MFArgs},
                                           queue := QName0,
                                           uris := [Uri | _]}}) ->
    QState = rabbit_queue_type:init(),
    {User, VHost} = get_user_vhost_from_amqp_param(Uri),
    %% We handle the most recently declared queue to use anonymous functions
    %% It's usually the channel that does it
    MRDQ = apply(M, F, MFArgs ++ [VHost, User]),
    QName = case QName0 of
                <<>> -> MRDQ;
                _ -> QName0
            end,
    Queue = rabbit_misc:r(VHost, queue, QName),
    State#{source => Src#{current => #{queue_states => QState,
                                       next_tag => 1,
                                       user => User,
                                       vhost => VHost,
                                       unacked_message_q => ?QUEUE:new()},
                          queue => QName,
                          queue_r => Queue}}.

connect_dest(State = #{dest := Dest = #{resource_decl := {M, F, MFArgs},
                                        uris := [Uri | _]},
                       ack_mode := AckMode}) ->
    {User, VHost} = get_user_vhost_from_amqp_param(Uri),
    apply(M, F, MFArgs ++ [VHost, User]),

    QState = rabbit_queue_type:init(),
    maybe_add_dest_queue(
      case AckMode of
          on_confirm ->
              State#{dest => Dest#{current => #{queue_states => QState,
                                                delivery_id => 1,
                                                vhost => VHost},
                                   unconfirmed => rabbit_shovel_confirms:init(),
                                   rejected => [],
                                   rejected_count => 0,
                                   confirmed => [],
                                   confirmed_count => 0}};
          _ ->
              State#{dest => Dest#{current => #{queue_states => QState,
                                                vhost => VHost},
                                   unconfirmed => rabbit_shovel_confirms:init(),
                                   confirmed => [],
                                   confirmed_count => 0,
                                   rejected => [],
                                   rejected_count => 0}}
      end).

maybe_add_dest_queue(State = #{dest := Dest = #{queue := QName,
                                                current := #{vhost := VHost}}}) ->
    Queue = rabbit_misc:r(VHost, queue, QName),
    State#{dest => Dest#{queue_r => Queue}};
maybe_add_dest_queue(State) ->
    State.

init_source(State = #{source := #{queue_r := QName,
                                  consumer_args := Args,
                                  consumer_name := CTag0,
                                  current := #{queue_states := QState0,
                                               vhost := VHost} = Current} = Src,
                      name := Name,
                      ack_mode := AckMode}) ->
    Mode = {credited, ?INITIAL_DELIVERY_COUNT},
    MaxLinkCredit = max_link_credit(),
    CTag = case CTag0 of
               <<>> -> consumer_tag(Name);
               _    -> CTag0
           end,
    case rabbit_amqqueue:with(
           QName,
           fun(Q) ->
                   SndSettled = case AckMode of
                                    no_ack -> true;
                                    on_publish -> false;
                                    on_confirm -> false
                               end,
                   Spec = #{no_ack => SndSettled,
                            channel_pid => self(),
                            limiter_pid => none,
                            limiter_active => false,
                            mode => Mode,
                            consumer_tag => CTag,
                            exclusive_consume => false,
                            args => Args,
                            ok_msg => undefined,
                            acting_user => ?SHOVEL_USER},
                   case remaining(Q, State) of
                       0 ->
                          {0, {error, autodelete}};
                       Remaining ->
                           {Remaining, rabbit_queue_type:consume(Q, Spec, QState0)}
                   end
           end) of
        {Remaining, {ok, QState1}} ->
            rabbit_global_counters:consumer_created(?PROTOCOL),
            {ok, QState, Actions} = rabbit_queue_type:credit(QName, CTag, ?INITIAL_DELIVERY_COUNT, MaxLinkCredit, false, QState1),
            State2 = State#{source => Src#{current => Current#{queue_states => QState,
                                                               consumer_tag => CTag},
                                           remaining => Remaining,
                                           remaining_unacked => Remaining,
                                           delivery_count => ?INITIAL_DELIVERY_COUNT,
                                           max_link_credit => MaxLinkCredit,
                                           credit => MaxLinkCredit,
                                           at_least_one_credit_req_in_flight => true}},
            handle_queue_actions(Actions, State2);
        {0, {error, autodelete}} ->
            exit({shutdown, autodelete});
        {_Remaining, {error, Reason}} ->
            ?LOG_ERROR(
               "Shovel '~ts' in vhost '~ts' failed to consume: ~ts",
               [Name, VHost, Reason]),
            exit({shutdown, failed_to_consume_from_source});
        {unlimited, {error, not_implemented, Reason, ReasonArgs}} ->
            ?LOG_ERROR(
              "Shovel '~ts' in vhost '~ts' failed to consume: ~ts",
              [Name, VHost, io_lib:format(Reason, ReasonArgs)]),
            exit({shutdown, failed_to_consume_from_source});
        {error, not_found} ->
            exit({shutdown, missing_source_queue})
    end.

init_dest(#{name := Name,
            shovel_type := Type,
            dest := #{add_forward_headers := AFH} = Dst} = State) ->
    rabbit_global_counters:publisher_created(?PROTOCOL),
    _TRef = erlang:send_after(1000, self(), send_confirms_and_nacks),
    Alarms0 = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    Alarms = sets:from_list(Alarms0),
    case AFH of
        true ->
            Props = #{<<"x-opt-shovelled-by">> => rabbit_nodes:cluster_name(),
                      <<"x-opt-shovel-type">> => rabbit_data_coercion:to_binary(Type),
                      <<"x-opt-shovel-name">> => rabbit_data_coercion:to_binary(Name)},
            State#{dest => Dst#{cached_forward_headers => Props,
                                alarms => Alarms}};
        false ->
            State#{dest => Dst#{alarms => Alarms}}
    end.

source_uri(_State) ->
    "".

dest_uri(_State) ->
    "".

source_protocol(_State) ->
    local.

dest_protocol(_State) ->
    local.

source_endpoint(#{source := #{exchange := SrcX,
                              routing_key := SrcXKey}}) ->
    [{src_exchange, SrcX},
     {src_exchange_key, SrcXKey}];
source_endpoint(#{source := #{queue := Queue}}) ->
    [{src_queue, Queue}];
source_endpoint(_Config) ->
    [].

dest_endpoint(#{dest := #{exchange := SrcX,
                          routing_key := SrcXKey}}) ->
    [{dest_exchange, SrcX},
     {dest_exchange_key, SrcXKey}];
dest_endpoint(#{dest := #{queue := Queue}}) ->
    [{dest_queue, Queue}];
dest_endpoint(_Config) ->
    [].
      
close_dest(_State) ->
    rabbit_global_counters:publisher_deleted(?PROTOCOL),
    ok.

close_source(#{source := #{current := #{queue_states := QStates0,
                                        consumer_tag := CTag,
                                        user := User},
                           queue_r := QName}}) ->
    rabbit_global_counters:consumer_deleted(?PROTOCOL),
    case rabbit_amqqueue:with(
           QName,
           fun(Q) ->
                   rabbit_queue_type:cancel(Q, #{consumer_tag => CTag,
                                                 reason => remove,
                                                 user => User#user.username}, QStates0)
           end) of
        {ok, _QStates} ->
            ok;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            ?LOG_WARNING("Local shovel failed to remove consumer ~tp: ~tp",
                         [CTag, Reason]),
            ok
    end;
close_source(_) ->
    %% No consumer tag, no consumer to cancel
    ok.

handle_source({queue_event, #resource{name = Queue,
                                      kind = queue,
                                      virtual_host = VHost} = QRef, Evt},
              #{source := Source = #{queue := Queue,
                                     current := Current = #{queue_states := QueueStates0,
                                                            vhost := VHost}}} = State0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QueueStates0) of
        {ok, QState1, Actions} ->
            State = State0#{source => Source#{current => Current#{queue_states => QState1}}},
            handle_queue_actions(Actions, State);
        {eol, Actions} ->
            _ = handle_queue_actions(Actions, State0),
            {stop, {inbound_link_or_channel_closure, queue_deleted}};
        {protocol_error, _Type, Reason, ReasonArgs} ->
            {stop, list_to_binary(io_lib:format(Reason, ReasonArgs))}
    end;
handle_source(send_confirms_and_nacks, State) ->
    _TRef = erlang:send_after(1000, self(), send_confirms_and_nacks),
    send_confirms_and_nacks(State);
handle_source({{'DOWN', #resource{name = Queue,
                                  kind = queue,
                                  virtual_host = VHost}}, _, _, _, _}  ,
              #{source := #{queue := Queue, current := #{vhost := VHost}}}) ->
    {stop, {inbound_link_or_channel_closure, source_queue_down}};
handle_source(_Msg, _State) ->
    not_handled.

handle_dest({queue_event, QRef, Evt},
            #{ack_mode := on_confirm,
              dest := Dest = #{current := Current = #{queue_states := QueueStates0}}} = State0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QueueStates0) of
        {ok, QState1, Actions} ->
            State = State0#{dest => Dest#{current => Current#{queue_states => QState1}}},
            send_confirms_and_nacks(handle_dest_queue_actions(Actions, State));
        {eol, Actions} ->
            _ = send_confirms_and_nacks(handle_dest_queue_actions(Actions, State0)),
            {stop, {outbound_link_or_channel_closure, queue_deleted}};
        {protocol_error, _Type, Reason, ReasonArgs} ->
            {stop, list_to_binary(io_lib:format(Reason, ReasonArgs))}
    end;
handle_dest({{'DOWN', #resource{name = Queue,
                                kind = queue,
                                virtual_host = VHost}}, _, _, _, _}  ,
            #{dest := #{queue := Queue, current := #{vhost := VHost}}}) ->
    {stop, {outbound_link_or_channel_closure, dest_queue_down}};
handle_dest({{'DOWN', #resource{kind = queue,
                                virtual_host = VHost} = QName}, _MRef, process, QPid, Reason},
            #{dest := Dest = #{current := Current = #{vhost := VHost,
                                                      queue_states := QStates0}}} = State0) ->
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QState1, Actions} ->
            State1 = State0#{dest => Dest#{current => Current#{queue_states => QState1}}},
            handle_dest_queue_actions(Actions, State1);
        {eol, QState1, _QRef} ->
            State0#{dest => Dest#{current => Current#{queue_states => QState1}}}
    end;
handle_dest({conserve_resources, Alarm, Conserve}, #{dest := #{alarms := Alarms0} = Dest} = State0) ->
    Alarms = case Conserve of
                 true -> sets:add_element(Alarm, Alarms0);
                 false -> sets:del_element(Alarm, Alarms0)
             end,
    State = State0#{dest => Dest#{alarms => Alarms}},
    case {sets:is_empty(Alarms0), sets:is_empty(Alarms)} of
        {false, true} ->
            %% All alarms cleared
            forward_pending_delivery(State);
        {_, _} ->
            State
    end;
handle_dest(_Msg, State) ->
    State.

ack(DeliveryTag, Multiple, State) ->
    maybe_grant_credit(settle(complete, DeliveryTag, Multiple, State)).

nack(DeliveryTag, Multiple, State) ->
    maybe_grant_credit(settle(requeue, DeliveryTag, Multiple, State)).

forward(_, _, #{source := #{remaining_unacked := 0}} = State) ->
    %% We are in on-confirm mode, and are autodelete. We have
    %% published all the messages we need to; we just wait for acks to
    %% come back. So drop subsequent messages on the floor to be
    %% requeued later
    State;
forward(Tag, Msg, State) ->
    case is_blocked(State) of
        true ->
            PendingEntry = {Tag, Msg},
            add_pending_delivery(PendingEntry, State);
        false ->
            do_forward(Tag, Msg, State)
    end.

do_forward(Tag, Msg0, #{dest := #{current := #{queue_states := QState} = Current} = Dest,
                     ack_mode := AckMode} = State0) ->
    {Options, #{dest := #{current := Current1} = Dest1} = State} =
        case AckMode of
            on_confirm  ->
                DeliveryId = maps:get(delivery_id, Current),
                Opts = #{correlation => DeliveryId},
                {Opts, State0#{dest => Dest#{current => Current#{delivery_id => DeliveryId + 1}}}};
            _ ->
                {#{}, State0}
        end,
    Msg = set_annotations(Msg0, Dest),
    RoutedQNames = route(Msg, Dest),
    Queues = rabbit_db_queue:get_targets(RoutedQNames),
    messages_received(AckMode),
    case rabbit_queue_type:deliver(Queues, Msg, Options, QState) of
        {ok, QState1, Actions} ->
            State1 = State#{dest => Dest1#{current => Current1#{queue_states => QState1}}},
            State2 = rabbit_shovel_behaviour:incr_forwarded(State1),
            State3 = rabbit_shovel_behaviour:decr_remaining_unacked(
                       case AckMode of
                           on_confirm when length(Queues) > 0 ->
                               State2;
                           on_publish ->
                               record_confirms([{Tag, Tag}], State2);
                           _ ->
                               decr_remaining(1, State2)
                       end),
            MsgSeqNo = maps:get(correlation, Options, undefined),
            QNames = lists:map(fun({QName, _}) -> QName;
                                  (QName) -> QName
                               end, RoutedQNames),
            State4 = process_routing_confirm(MsgSeqNo, Tag, QNames, State3),
            send_confirms_and_nacks(handle_dest_queue_actions(Actions, State4));
        {error, Reason} ->
            exit({shutdown, Reason})
    end.

set_annotations(Msg, Dest) ->
    add_routing(add_forward_headers(add_timestamp_header(Msg, Dest), Dest), Dest).

add_timestamp_header(Msg, #{add_timestamp_header := true}) ->
    mc:set_annotation(<<"x-opt-shovelled-timestamp">>, os:system_time(milli_seconds), Msg);
add_timestamp_header(Msg, _) ->
    Msg.

add_forward_headers(Msg, #{cached_forward_headers := Props}) ->
    maps:fold(fun(K, V, Acc) ->
                      mc:set_annotation(K, V, Acc)
              end, Msg, Props);
add_forward_headers(Msg, _D) ->
    Msg.

add_routing(Msg0, Dest) ->
    Msg = case maps:get(exchange, Dest, undefined) of
              undefined -> Msg0;
              Exchange -> mc:set_annotation(?ANN_EXCHANGE, Exchange, Msg0)
          end,
    case maps:get(routing_key, Dest, undefined) of
        undefined -> Msg;
        RK -> mc:set_annotation(?ANN_ROUTING_KEYS, [RK], Msg)
    end.

status(State) ->
    case is_blocked(State) of
        true -> blocked;
        false -> running
    end.

pending_count(#{dest := #{pending_delivery := Pending}}) ->
    lqueue:len(Pending);
pending_count(#{source := #{current := #{unacked_message_q := UAMQ}}}) ->
    ?QUEUE:len(UAMQ);
pending_count(_) ->
    0.

%% Internal

parse_parameter(_, _, none) ->
    none;
parse_parameter(Param, Fun, Value) ->
    try
        Fun(Value)
    catch
        _:{error, Err} ->
            fail({invalid_parameter_value, Param, Err})
    end.

parse_binary(Binary) when is_binary(Binary) ->
    Binary;
parse_binary(NotABinary) ->
    fail({require_binary, NotABinary}).

consumer_tag(Name) ->
    CTag0 = rabbit_shovel_util:gen_unique_name(Name, "receiver"),
    rabbit_data_coercion:to_binary(CTag0).

-spec fail(term()) -> no_return().
fail(Reason) -> throw({error, Reason}).

handle_queue_actions(Actions, State) ->
    lists:foldl(
      fun({deliver, _CTag, AckRequired, Msgs}, S0) ->
              handle_deliver(AckRequired, Msgs, S0);
         ({credit_reply, _, _, _, _, _} = Action, S0) ->
              handle_credit_reply(Action, S0);
         (_Action, S0) ->
              S0
      end, State, Actions).

handle_deliver(AckRequired, Msgs, State) when is_list(Msgs) ->
    NumMsgs = length(Msgs),
    maybe_grant_credit(
      lists:foldl(
        fun({QName, _QPid, MsgId, _Redelivered, Mc}, #{source := #{current := #{queue_states := QStates }}} = S0) ->
                messages_delivered(QName, QStates),
                DeliveryTag = next_tag(S0),
                S = record_pending(AckRequired, DeliveryTag, MsgId, increase_next_tag(S0)),
                rabbit_shovel_behaviour:forward(DeliveryTag, Mc, S)
        end, sent_delivery(State, NumMsgs), Msgs)).

next_tag(#{source := #{current := #{next_tag := DeliveryTag}}}) ->
    DeliveryTag.

increase_next_tag(#{source := Source = #{current := Current = #{next_tag := DeliveryTag}}} = State) ->
    State#{source => Source#{current => Current#{next_tag => DeliveryTag + 1}}}.

handle_dest_queue_actions(Actions, State) ->
    lists:foldl(
      fun({settled, QName, MsgSeqNos}, S0) ->
              confirm(MsgSeqNos, QName, S0);
         ({rejected, _QName, _Reason, MsgSeqNos},
          #{dest := Dst = #{unconfirmed := U0}} = S0) ->
              {U, Rej} =
              lists:foldr(
                fun(SeqNo, {U1, Acc}) ->
                        case rabbit_shovel_confirms:reject(SeqNo, U1) of
                            {ok, MX, U2} ->
                                {U2, [MX | Acc]};
                            {error, not_found} ->
                                {U1, Acc}
                        end
                end, {U0, []}, MsgSeqNos),
              S = S0#{dest => Dst#{unconfirmed => U}},
              record_rejects(Rej, S);
         %% TODO handle {block, QName}
         (_Action, S0) ->
              S0
      end, State, Actions).

record_pending(false, _DeliveryTag, _MsgId, State) ->
    State;
record_pending(true, DeliveryTag, MsgId,
               #{source := Src = #{current := Current = #{unacked_message_q := UAMQ0}}} = State) ->
    UAMQ = ?QUEUE:in(#pending_ack{delivery_tag = DeliveryTag,
                                  msg_id = MsgId}, UAMQ0),
    State#{source => Src#{current => Current#{unacked_message_q => UAMQ}}}.

remaining(_Q, #{source := #{delete_after := never}}) ->
    unlimited;
remaining(Q, #{source := #{delete_after := 'queue-length'}}) ->
    [{messages, Count}] = rabbit_amqqueue:info(Q, [messages]),
    Count;
remaining(_Q, #{source := #{delete_after := Count}}) ->
    Count.

decl_fun(Decl, VHost, User) ->
    lists:foldr(
      fun(Method, MRDQ) -> %% keep track of most recently declared queue
              Reply = rabbit_channel:handle_method(
                        expand_shortcuts(Method, MRDQ),
                        none, #{}, none, VHost, User),
              case {Method, Reply} of
                  {#'queue.declare'{}, {ok, QName, _, _}} ->
                      QName#resource.name;
                  _ ->
                      MRDQ
              end
      end, <<>>, Decl).

expand_shortcuts(#'queue.bind'   {queue = Q, routing_key = K} = M, MRDQ) ->
    M#'queue.bind'   {queue       = expand_queue_name_shortcut(Q, MRDQ),
                      routing_key = expand_routing_key_shortcut(Q, K, MRDQ)};
expand_shortcuts(#'queue.unbind' {queue = Q, routing_key = K} = M, MRDQ) ->
    M#'queue.unbind' {queue       = expand_queue_name_shortcut(Q, MRDQ),
                      routing_key = expand_routing_key_shortcut(Q, K, MRDQ)};
expand_shortcuts(M, _State) ->
    M.

expand_queue_name_shortcut(<<>>, <<>>) ->
    exit({shutdown, {not_found, "no previously declared queue"}});
expand_queue_name_shortcut(<<>>, MRDQ) ->
    MRDQ;
expand_queue_name_shortcut(QueueNameBin, _) ->
    QueueNameBin.

expand_routing_key_shortcut(<<>>, <<>>, <<>>) ->
    exit({shutdown, {not_found, "no previously declared queue"}});
expand_routing_key_shortcut(<<>>, <<>>, MRDQ) ->
    MRDQ;
expand_routing_key_shortcut(_QueueNameBin, RoutingKey, _) ->
    RoutingKey.

check_fun(QName, VHost, User) ->
    Method = #'queue.declare'{queue = QName,
                              passive = true},
    decl_fun([Method], VHost, User).

src_decl_exchange(SrcX, SrcXKey, VHost, User) ->
    Methods = [#'queue.bind'{routing_key = SrcXKey,
                             exchange    = SrcX},
               #'queue.declare'{exclusive = true}],
    decl_fun(Methods, VHost, User).

dest_decl_queue(none, _, _, _) ->
    ok;
dest_decl_queue(QName, QArgs, VHost, User) ->
    decl_queue(QName, QArgs, VHost, User).

decl_queue(QName, QArgs, VHost, User) ->
    Args = rabbit_misc:to_amqp_table(QArgs),
    Method = #'queue.declare'{queue = QName,
                              durable = true,
                              arguments = Args},
    try
        decl_fun([#'queue.declare'{queue = QName,
                                   passive = true}], VHost, User)
    catch exit:{amqp_error, not_found, _, _} ->
            decl_fun([Method], VHost, User)
    end.

dest_check_queue(none, _, _, _) ->
    ok;
dest_check_queue(QName, QArgs, VHost, User) ->
    check_queue(QName, QArgs, VHost, User).

check_queue(QName, _QArgs, VHost, User) ->
    Method = #'queue.declare'{queue = QName,
                              passive = true},
    decl_fun([Method], VHost, User).

get_user_vhost_from_amqp_param(Uri) ->
    {ok, AmqpParam} = amqp_uri:parse(Uri),
    {Username, Password, VHost} =
        case AmqpParam of
            #amqp_params_direct{username = U,
                                password = P,
                                virtual_host = V} ->
                case {U, P} of
                    {none, none} ->
                        %% Default user for uris like amqp://
                        {<<"guest">>, <<"guest">>, V};
                    _ ->
                        {U, P, V}
                end;
            #amqp_params_network{username = U,
                                 password = P,
                                 virtual_host = V} ->
                {U, P, V}
        end,
    case rabbit_access_control:check_user_login(Username, [{password, Password}]) of
        {ok, User} ->
            try
                rabbit_access_control:check_vhost_access(User, VHost, undefined, #{}) of
                ok ->
                    {User, VHost}
            catch
                exit:#amqp_error{name = not_allowed} ->
                    exit({shutdown, {access_refused, Username}})
            end;
        {refused, Username, _Msg, _Module} ->
            ?LOG_ERROR("Local shovel user ~ts was refused access", [Username]),
            exit({shutdown, {access_refused, Username}})
    end.

settle(Op, DeliveryTag, Multiple,
       #{source := #{queue_r := QRef,
                     current := Current = #{consumer_tag := CTag,
                                            unacked_message_q := UAMQ0,
                                            queue_states := QState0}
                    } = Src} = State0) ->
    {MsgIds, UAMQ} = collect_acks(UAMQ0, DeliveryTag, Multiple),
    case rabbit_queue_type:settle(QRef, Op, CTag, lists:reverse(MsgIds), QState0) of
        {ok, QState1, Actions} ->
            messages_acknowledged(Op, QRef, QState1, MsgIds),
            State = State0#{source => Src#{current => Current#{queue_states => QState1,
                                                               unacked_message_q => UAMQ}}},
            handle_queue_actions(Actions, State);
        {'protocol_error', Type, Reason, Args} ->
            ?LOG_ERROR("Shovel failed to settle ~p acknowledgments with ~tp: ~tp",
                       [Op, Type, io_lib:format(Reason, Args)]),
            exit({shutdown, {ack_failed, Reason}})
    end.

%% From rabbit_channel
%% Records a client-sent acknowledgement. Handles both single delivery acks
%% and multi-acks.
%%
%% Returns a tuple of acknowledged pending acks and remaining pending acks.
%% Sorts each group in the youngest-first order (descending by delivery tag).
collect_acks(UAMQ, DeliveryTag, Multiple) ->
    collect_acks([], [], UAMQ, DeliveryTag, Multiple).

collect_acks(AcknowledgedAcc, RemainingAcc, UAMQ, DeliveryTag, Multiple) ->
    case ?QUEUE:out(UAMQ) of
        {{value, UnackedMsg = #pending_ack{delivery_tag = CurrentDT,
                                           msg_id = Id}},
         UAMQTail} ->
            if CurrentDT == DeliveryTag ->
                   {[Id | AcknowledgedAcc],
                    case RemainingAcc of
                        [] -> UAMQTail;
                        _  -> ?QUEUE:join(
                                 ?QUEUE:from_list(lists:reverse(RemainingAcc)),
                                 UAMQTail)
                    end};
               Multiple ->
                    collect_acks([Id | AcknowledgedAcc], RemainingAcc,
                                 UAMQTail, DeliveryTag, Multiple);
               true ->
                    collect_acks(AcknowledgedAcc, [UnackedMsg | RemainingAcc],
                                 UAMQTail, DeliveryTag, Multiple)
            end;
        {empty, UAMQTail} ->
           {AcknowledgedAcc, UAMQTail}
    end.

route(_Msg, #{queue_r := QueueR,
              queue := Queue}) when Queue =/= none ->
    [QueueR];
route(Msg, #{current := #{vhost := VHost}}) ->
    ExchangeName = rabbit_misc:r(VHost, exchange, mc:exchange(Msg)),
    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    rabbit_exchange:route(Exchange, Msg, #{return_binding_keys => true}).

sent_delivery(#{source := #{delivery_count := DeliveryCount0,
                            credit := Credit0} = Src
               } = State0, NumMsgs) ->
    DeliveryCount = serial_number:add(DeliveryCount0, NumMsgs),
    Credit = max(0, Credit0 - NumMsgs),
    State0#{source => Src#{credit => Credit,
                           delivery_count => DeliveryCount
                          }}.

maybe_grant_credit(#{source := #{queue_r := QName,
                                 credit := Credit,
                                 max_link_credit := MaxLinkCredit,
                                 delivery_count := DeliveryCount,
                                 at_least_one_credit_req_in_flight := HaveCreditReqInFlight,
                                 current := #{consumer_tag := CTag,
                                              queue_states := QState0,
                                              unacked_message_q := Q} = Current
                                } = Src} = State0) ->
    GrantLinkCredit = grant_link_credit(Credit, MaxLinkCredit, ?QUEUE:len(Q)),
    {ok, QState, Actions} = case (GrantLinkCredit and not HaveCreditReqInFlight) of
                                true ->
                                    rabbit_queue_type:credit(
                                      QName, CTag, DeliveryCount, MaxLinkCredit,
                                      false, QState0);
                                _ ->
                                    {ok, QState0, []}
                            end,
    CreditReqInFlight = case GrantLinkCredit of
                            true -> true;
                            false -> HaveCreditReqInFlight
                        end,
    State = State0#{source => Src#{current => Current#{queue_states => QState},
                                   at_least_one_credit_req_in_flight => CreditReqInFlight
                                  }},
    handle_queue_actions(Actions, State).

max_link_credit() ->
    application:get_env(rabbitmq_shovel, max_local_shovel_credit, ?DEFAULT_MAX_LINK_CREDIT).

grant_link_credit(Credit, MaxLinkCredit, NumUnconfirmed) ->
    Credit =< MaxLinkCredit div 2 andalso
    NumUnconfirmed < MaxLinkCredit.

%% Drain is ignored because local shovels do not use it.
handle_credit_reply({credit_reply, CTag, DeliveryCount, Credit, _Available, _Drain},
                    #{source := #{credit := CCredit,
                                  max_link_credit := MaxLinkCredit,
                                  delivery_count := QDeliveryCount,
                                  queue_r := QName,
                                  current := Current = #{queue_states := QState0,
                                                         unacked_message_q := Q}
                                 } = Src} = State0) ->
    %% Assertion: Our (receiver) delivery-count should be always
    %% in sync with the delivery-count of the sending queue.
    QDeliveryCount = DeliveryCount,
    case grant_link_credit(CCredit, MaxLinkCredit, ?QUEUE:len(Q)) of
        true ->
            {ok, QState, Actions} = rabbit_queue_type:credit(QName, CTag, QDeliveryCount,
                                                             MaxLinkCredit, false, QState0),
            State = State0#{source => Src#{credit => MaxLinkCredit,
                                           at_least_one_credit_req_in_flight => true,
                                           current => Current#{queue_states => QState}}},
            handle_queue_actions(Actions, State);
        false ->
            %% Although we (the receiver) usually determine link credit, we set here
            %% our link credit to what the queue says our link credit is (which is safer
            %% in case credit requests got applied out of order in quorum queues).
            %% This should be fine given that we asserted earlier that our delivery-count is
            %% in sync with the delivery-count of the sending queue.
            State0#{source => Src#{credit => Credit,
                                   at_least_one_credit_req_in_flight => false}}
    end.

process_routing_confirm(undefined, _, [], State) ->
    rabbit_global_counters:messages_unroutable_returned(?PROTOCOL, 1),
    State;
process_routing_confirm(undefined, _, QRefs, State) ->
    rabbit_global_counters:messages_routed(?PROTOCOL, length(QRefs)),
    State;
process_routing_confirm(MsgSeqNo, Tag, [], State)
  when is_integer(MsgSeqNo) ->
    rabbit_global_counters:messages_unroutable_dropped(?PROTOCOL, 1),
    record_confirms([{MsgSeqNo, Tag}], State);
process_routing_confirm(MsgSeqNo, Tag, QRefs, #{dest := Dst = #{unconfirmed := Unconfirmed}} = State) when is_integer(MsgSeqNo) ->
    rabbit_global_counters:messages_routed(?PROTOCOL, length(QRefs)),
    State#{dest => Dst#{unconfirmed =>
                            rabbit_shovel_confirms:insert(MsgSeqNo, QRefs, Tag, Unconfirmed)}}.

record_confirms([], State) ->
    State;
record_confirms(MXs, State = #{dest := Dst = #{confirmed := C,
                                              confirmed_count := CC}}) ->
    Num = length(MXs),
    decr_remaining(
      Num, State#{dest => Dst#{confirmed => [MXs | C],
                               confirmed_count => CC + Num}}).

record_rejects([], State) ->
    State;
record_rejects(MXs, State = #{dest := Dst = #{rejected := R,
                                              rejected_count := RC}}) ->
    Num = length(MXs),
    decr_remaining(
      Num, State#{dest => Dst#{rejected => [MXs | R],
                               rejected_count => RC + Num}}).

confirm(MsgSeqNos, QRef, State = #{dest := Dst = #{unconfirmed := UC}}) ->
    {ConfirmMXs, UC1} = rabbit_shovel_confirms:confirm(MsgSeqNos, QRef, UC),
    record_confirms(ConfirmMXs, State#{dest => Dst#{unconfirmed => UC1}}).

send_nacks([], _, State) ->
    State;
send_nacks(Rs, Cs, State) ->
    coalesce_and_send(Rs, Cs,
                      fun(MsgSeqNo, Multiple, StateX) ->
                              rabbit_shovel_behaviour:nack(MsgSeqNo, Multiple, StateX)
                      end, State).

send_confirms([], _, State) ->
    State;
send_confirms([MsgSeqNo], _, State) ->
    rabbit_global_counters:messages_confirmed(?PROTOCOL, 1),
    rabbit_shovel_behaviour:ack(MsgSeqNo, false, State);
send_confirms(Cs, Rs, State) ->
    rabbit_global_counters:messages_confirmed(?PROTOCOL, length(Cs)),
    coalesce_and_send(Cs, Rs,
                      fun(MsgSeqNo, Multiple, StateX) ->
                              rabbit_shovel_behaviour:ack(MsgSeqNo, Multiple, StateX)
                      end, State).

coalesce_and_send(MsgSeqNos, NegativeMsgSeqNos, MkMsgFun,
                  State = #{dest := #{unconfirmed := UC}}) ->
    SMsgSeqNos = lists:usort(MsgSeqNos),
    UnconfirmedCutoff = case rabbit_shovel_confirms:is_empty(UC) of
                            true  -> lists:last(SMsgSeqNos) + 1;
                            false -> rabbit_shovel_confirms:smallest(UC)
                        end,
    Cutoff = lists:min([UnconfirmedCutoff | NegativeMsgSeqNos]),
    {Ms, Ss} = lists:splitwith(fun(X) -> X < Cutoff end, SMsgSeqNos),
    State1 = case Ms of
                 [] -> State;
                 _  -> MkMsgFun(lists:last(Ms), true, State)
             end,
    lists:foldl(fun(SeqNo, S) ->
                        MkMsgFun(SeqNo, false, S)
                end, State1, Ss).

%% Todo remove XName from confirm/unconfirm as we don't need it for local
send_confirms_and_nacks(State = #{dest := #{confirmed := [],
                                            rejected := []}}) ->
    State;
send_confirms_and_nacks(State = #{dest := Dst = #{confirmed := C,
                                                  rejected := R}}) ->
    Confirms = lists:append(C),
    ConfirmTags = [Tag || {_, Tag} <- Confirms],
    Rejects = lists:append(R),
    RejectTags = [Tag || {_, Tag} <- Rejects],
    State1 = #{dest := Dst2}
        = send_confirms(ConfirmTags,
                        RejectTags,
                        State#{dest => Dst#{confirmed => [],
                                            confirmed_count => 0}}),
    send_nacks(RejectTags,
               ConfirmTags,
               State1#{dest => Dst2#{rejected => [],
                                     rejected_count => 0}}).

decr_remaining(Num, State) ->
    try
        rabbit_shovel_behaviour:decr_remaining(Num, State)
    catch
        exit:{shutdown, autodelete} = R ->
            _ = send_confirms_and_nacks(State),
            exit(R)
    end.

messages_acknowledged(complete, QName, QS, MsgIds) ->
    case rabbit_queue_type:module(QName, QS) of
        {ok, QType} ->
            rabbit_global_counters:messages_acknowledged(?PROTOCOL, QType, length(MsgIds));
        _ ->
            ok
    end;
messages_acknowledged(_, _, _, _) ->
    ok.

messages_received(AckMode) ->
    rabbit_global_counters:messages_received(?PROTOCOL, 1),
    case AckMode of
        on_confirm ->
            rabbit_global_counters:messages_received_confirm(?PROTOCOL, 1);
        _ ->
            ok
    end.

messages_delivered(QName, S0) ->
    case rabbit_queue_type:module(QName, S0) of
        {ok, QType} ->
            rabbit_global_counters:messages_delivered(?PROTOCOL, QType, 1);
        _ ->
            ok
    end.

is_blocked(#{dest := #{alarms := Alarms}}) ->
    not sets:is_empty(Alarms);
is_blocked(_) ->
    false.

add_pending_delivery(Elem, State = #{dest := Dest}) ->
    Pending = maps:get(pending_delivery, Dest, lqueue:new()),
    State#{dest => Dest#{pending_delivery => lqueue:in(Elem, Pending)}}.

pop_pending_delivery(State = #{dest := Dest}) ->
    Pending = maps:get(pending_delivery, Dest, lqueue:new()),
    case lqueue:out(Pending) of
        {empty, _} ->
            empty;
        {{value, Elem}, Pending2} ->
            {Elem, State#{dest => Dest#{pending_delivery => Pending2}}}
    end.

forward_pending_delivery(State) ->
    case pop_pending_delivery(State) of
        empty ->
            State;
        {{Tag, Mc}, S} ->
            S2 = do_forward(Tag, Mc, S),
            case is_blocked(S2) of
                true ->
                    S2;
                false ->
                    forward_pending_delivery(S2)
            end
    end.

opt_b2a(B) when is_binary(B) -> list_to_atom(binary_to_list(B));
opt_b2a(N)                   -> N.
