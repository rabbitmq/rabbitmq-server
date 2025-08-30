%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_local_shovel).

-behaviour(rabbit_shovel_behaviour).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_shovel.hrl").

-export([
         parse/2,
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
         status/1
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

-define(QUEUE, lqueue).
%% "Note that, despite its name, the delivery-count is not a count but a
%% sequence number initialized at an arbitrary point by the sender."
%% See rabbit_amqp_session.erl
-define(INITIAL_DELIVERY_COUNT, 16#ff_ff_ff_ff - 4).
-define(DEFAULT_MAX_LINK_CREDIT, 1000).

-record(pending_ack, {
                      delivery_tag,
                      msg_id
                     }).

parse(_Name, {source, Source}) ->
    Queue = parse_parameter(queue, fun parse_binary/1,
                            proplists:get_value(queue, Source)),
    CArgs = proplists:get_value(consumer_args, Source, []),
    #{module => ?MODULE,
      uris => proplists:get_value(uris, Source),
      resource_decl => rabbit_shovel_util:decl_fun(?MODULE, {source, Source}),
      queue => Queue,
      delete_after => proplists:get_value(delete_after, Source, never),
      consumer_args => CArgs};
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

connect_source(State = #{source := Src = #{resource_decl := {M, F, MFArgs},
                                           queue := QName0,
                                           uris := [Uri | _]}}) ->
    case rabbit_feature_flags:is_enabled('rabbitmq_4.0.0') of
        true ->
            ok;
        false ->
            exit({shutdown, feature_flag_rabbitmq_4_0_0_is_disabled})
    end,
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
    case rabbit_feature_flags:is_enabled('rabbitmq_4.0.0') of
        true ->
            ok;
        false ->
            exit({shutdown, feature_flag_rabbitmq_4_0_0_is_disabled})
    end,
    {User, VHost} = get_user_vhost_from_amqp_param(Uri),
    apply(M, F, MFArgs ++ [VHost, User]),

    QState = rabbit_queue_type:init(),
    maybe_add_dest_queue(
      case AckMode of
          on_confirm ->
              State#{dest => Dest#{current => #{queue_states => QState,
                                                delivery_id => 1,
                                                vhost => VHost},
                                   unacked => #{}}};
          _ ->
              State#{dest => Dest#{current => #{queue_states => QState,
                                                vhost => VHost},
                                   unacked => #{}}}
      end).

maybe_add_dest_queue(State = #{dest := Dest = #{queue := QName,
                                                current := #{vhost := VHost}}}) ->
    Queue = rabbit_misc:r(VHost, queue, QName),
    State#{dest => Dest#{queue_r => Queue}};
maybe_add_dest_queue(State) ->
    State.

init_source(State = #{source := #{queue_r := QName,
                                  consumer_args := Args,
                                  current := #{queue_states := QState0,
                                               vhost := VHost} = Current} = Src,
                      name := Name,
                      ack_mode := AckMode}) ->
    Mode = {credited, ?INITIAL_DELIVERY_COUNT},
    MaxLinkCredit = max_link_credit(),
    CTag = consumer_tag(Name),
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
    case AFH of
        true ->
            Props = #{<<"x-opt-shovelled-by">> => rabbit_nodes:cluster_name(),
                      <<"x-opt-shovel-type">> => rabbit_data_coercion:to_binary(Type),
                      <<"x-opt-shovel-name">> => rabbit_data_coercion:to_binary(Name)},
            State#{dest => Dst#{cached_forward_headers => Props}};
        false ->
            State
    end.

source_uri(_State) ->
    "".

dest_uri(_State) ->
    "".

source_protocol(_State) ->
    local.

dest_protocol(_State) ->
    local.

source_endpoint(#{source := #{queue := Queue,
                              exchange := SrcX,
                              routing_key := SrcXKey}}) ->
    [{src_exchange, SrcX},
     {src_exchange_key, SrcXKey},
     {src_queue, Queue}];
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
    ok.

close_source(#{source := #{current := #{queue_states := QStates0,
                                        consumer_tag := CTag,
                                        user := User},
                           queue_r := QName}}) ->
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
            handle_dest_queue_actions(Actions, State);
        {eol, Actions} ->
            _ = handle_dest_queue_actions(Actions, State0),
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
handle_dest(_Msg, State) ->
    State.

ack(DeliveryTag, Multiple, State) ->
    maybe_grant_credit(settle(complete, DeliveryTag, Multiple, State)).

nack(DeliveryTag, Multiple, State) ->
    maybe_grant_credit(settle(requeue, DeliveryTag, Multiple, State)).

forward(Tag, Msg0, #{dest := #{current := #{queue_states := QState} = Current,
                               unacked := Unacked} = Dest,
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
    QNames = route(Msg, Dest),
    Queues = rabbit_amqqueue:lookup_many(QNames),
    case rabbit_queue_type:deliver(Queues, Msg, Options, QState) of
        {ok, QState1, Actions} ->
            State1 = State#{dest => Dest1#{current => Current1#{queue_states => QState1}}},
            #{dest := Dst1} = State2 = rabbit_shovel_behaviour:incr_forwarded(State1),
            State4 = rabbit_shovel_behaviour:decr_remaining_unacked(
                       case AckMode of
                           no_ack ->
                               rabbit_shovel_behaviour:decr_remaining(1, State2);
                           on_confirm when length(Queues) > 0 ->
                               Correlation = maps:get(correlation, Options),
                               State2#{dest => Dst1#{unacked => Unacked#{Correlation => Tag}}};
                           on_confirm ->
                               %% Drop the messages as 0.9.1, no destination available
                               State3 = rabbit_shovel_behaviour:ack(Tag, false, State2),
                               rabbit_shovel_behaviour:decr_remaining(1, State3);
                           on_publish ->
                               State3 = rabbit_shovel_behaviour:ack(Tag, false, State2),
                               rabbit_shovel_behaviour:decr_remaining(1, State3)
                       end),
            handle_dest_queue_actions(Actions, State4);
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

status(_) ->
    running.

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
    maybe_grant_credit(
      lists:foldl(
        fun({_QName, _QPid, MsgId, _Redelivered, Mc}, S0) ->
                DeliveryTag = next_tag(S0),
                S = record_pending(AckRequired, DeliveryTag, MsgId, increase_next_tag(S0)),
                rabbit_shovel_behaviour:forward(DeliveryTag, Mc, S)
        end, sent_delivery(State, length(Msgs)), Msgs)).

next_tag(#{source := #{current := #{next_tag := DeliveryTag}}}) ->
    DeliveryTag.

increase_next_tag(#{source := Source = #{current := Current = #{next_tag := DeliveryTag}}} = State) ->
    State#{source => Source#{current => Current#{next_tag => DeliveryTag + 1}}}.

handle_dest_queue_actions(Actions, State) ->
    lists:foldl(
      fun({settled, _QName, MsgSeqNos}, S0) ->
              confirm_to_inbound(fun(Tag, StateX) ->
                                         rabbit_shovel_behaviour:ack(Tag, false, StateX)
                                 end, MsgSeqNos, S0);
         ({rejected, _QName, MsgSeqNos}, S0) ->
              confirm_to_inbound(fun(Tag, StateX) ->
                                         rabbit_shovel_behaviour:nack(Tag, false, StateX)
                                 end, MsgSeqNos, S0);
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
    decl_fun([Method], VHost, User).

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

confirm_to_inbound(ConfirmFun, SeqNos, State)
  when is_list(SeqNos) ->
    lists:foldl(fun(Seq, State0) ->
                        confirm_to_inbound(ConfirmFun, Seq, State0)
                end, State, SeqNos);
confirm_to_inbound(ConfirmFun, Seq,
                   State0 = #{dest := #{unacked := Unacked} = Dst}) ->
    case Unacked of
        #{Seq := InTag} ->
            Unacked1 = maps:remove(Seq, Unacked),
            State = rabbit_shovel_behaviour:decr_remaining(
                      1, State0#{dest => Dst#{unacked => Unacked1}}),
            ConfirmFun(InTag, State);
        _ ->
            State0
    end.

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
