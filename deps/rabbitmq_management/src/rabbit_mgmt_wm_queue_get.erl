%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_queue_get).

-export([init/2, resource_exists/2, is_authorized/2, allow_missing_post/2,
  allowed_methods/2, accept_content/2, content_types_provided/2,
  content_types_accepted/2]).
-export([variances/2]).
-export([parse_ackmode/1]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-define(DEFAULT_MAX_MESSAGES, 1000).
-define(DEFAULT_MAX_PAYLOAD_BYTES, 67108864).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"POST">>, <<"OPTIONS">>], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData0, Context) ->
    case rabbit_mgmt_wm_queue:queue(ReqData0) of
        not_found ->
            ReqData1 = rabbit_mgmt_util:set_resp_not_found(<<"queue_not_found">>, ReqData0),
            {false, ReqData1, Context};
        _ ->
            {true, ReqData0, Context}
    end.

allow_missing_post(ReqData, Context) ->
    {false, ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

accept_content(ReqData, Context) ->
    rabbit_mgmt_util:post_respond(do_it(ReqData, Context)).

do_it(ReqData0, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData0),
    Q = rabbit_mgmt_util:id(queue, ReqData0),
    rabbit_mgmt_util:with_decode(
      [ackmode, count, encoding], ReqData0, Context,
      fun([AckModeBin, CountBin, EncBin], Body, ReqData) ->
              case maps:get(<<"protocol">>, Body, <<"amqp091">>) of
                  <<"amqp091">> ->
                      get_amqp091(VHost, Q, AckModeBin, CountBin, EncBin, Body,
                                  ReqData, Context);
                  <<"amqp">> ->
                      get_amqp(VHost, Q, AckModeBin, CountBin, EncBin, Body,
                               ReqData, Context);
                  Protocol ->
                      throw({error, {unsupported_protocol, Protocol}})
              end
      end).

get_amqp091(VHost, Q, AckModeBin, CountBin, EncBin, Body, ReqData, Context) ->
              rabbit_mgmt_util:with_channel(
                VHost, ReqData, Context,
                fun (Ch) ->
                        AckMode = case parse_ackmode(AckModeBin) of
                                      modify ->
                                          throw({error, <<"The modify ackmode requires protocol amqp.">>});
                                      AckMode0 ->
                                          AckMode0
                                  end,
                        MaxMessages = application:get_env(
                                        rabbitmq_management, basic_get_max_messages,
                                        ?DEFAULT_MAX_MESSAGES),
                        MaxPayloadBytes = application:get_env(
                                            rabbitmq_management, basic_get_max_payload_bytes,
                                            ?DEFAULT_MAX_PAYLOAD_BYTES),
                        Count = parse_count(CountBin, MaxMessages),
                        Enc = case EncBin of
                                  <<"auto">>   -> auto;
                                  <<"base64">> -> base64;
                                  _            -> throw({error, <<"Unsupported encoding. Please use auto or base64.">>})
                              end,
                        Trunc = case maps:get(truncate, Body, undefined) of
                                    undefined -> none;
                                    TruncBin  -> parse_non_negative_integer(TruncBin)
                                end,

                        Reply = basic_gets(Count, MaxPayloadBytes, Ch, Q, AckMode, Enc, Trunc),
                        maybe_return(Reply, Ch, AckMode),
                        rabbit_mgmt_util:reply(remove_delivery_tag(Reply),
					       ReqData, Context)
                end).

get_amqp(VHost, QNameBin, AckModeBin, CountBin, EncBin, Body, ReqData,
         Context = #context{user = User}) ->
    AckMode = case parse_ackmode(AckModeBin) of
                  modify -> {modify, modify_op(Body)};
                  AckMode0 -> AckMode0
              end,
    MaxMessages = application:get_env(rabbitmq_management, basic_get_max_messages,
                                      ?DEFAULT_MAX_MESSAGES),
    MaxPayloadBytes = application:get_env(rabbitmq_management, basic_get_max_payload_bytes,
                                          ?DEFAULT_MAX_PAYLOAD_BYTES),
    Count = parse_count(CountBin, MaxMessages),
    Enc = parse_encoding(EncBin),
    Trunc = parse_truncate(Body),
    QName = rabbit_misc:r(VHost, queue, QNameBin),
    try rabbit_access_control:check_resource_access(User, QName, read, #{}) of
        ok ->
            case rabbit_amqqueue:lookup(QName) of
                {ok, Q} ->
                    IcptCtx = #{protocol => amqp10,
                                vhost => VHost,
                                username => User#user.username,
                                connection_name => <<"HTTP API">>},
                    Msgs = amqp_gets(Count, MaxPayloadBytes, Q, AckMode, Enc, Trunc,
                                     IcptCtx, rabbit_queue_type:init(), 1, 0, []),
                    rabbit_mgmt_util:reply(Msgs, ReqData, Context);
                {error, not_found} ->
                    rabbit_mgmt_util:not_found(queue_not_found, ReqData, Context)
            end
    catch
        exit:#amqp_error{explanation = Expl} ->
            rabbit_mgmt_util:not_authorised(Expl, ReqData, Context)
    end.

amqp_gets(Count, MaxPayloadBytes, Q, AckMode, Enc, Trunc, IcptCtx, QStates0, N,
          Total, Acc)
  when N =< Count, Total < MaxPayloadBytes ->
    CTag = <<"management-get-", (integer_to_binary(N))/binary>>,
    case rabbit_queue_type:dequeue(Q, amqp_no_ack(AckMode), undefined,
                                   CTag, QStates0) of
        {ok, MsgCount, {QName, _, MsgId, Redelivered, Mc}, QStates1} ->
            Msg = amqp_msg(Mc, MsgCount, Redelivered, Enc, Trunc, IcptCtx),
            Size = proplists:get_value(payload_bytes, Msg),
            Acc1 = [{Msg, QName, CTag, MsgId} | Acc],
            amqp_gets(Count, MaxPayloadBytes, Q, AckMode, Enc, Trunc, IcptCtx,
                      QStates1, N + 1, Total + Size, Acc1);
        {empty, QStates1} ->
            amqp_settle(Acc, AckMode, QStates1);
        {error, Reason} ->
            amqp_settle(Acc, AckMode, QStates0),
            throw({error, iolist_to_binary(io_lib:format("~tp", [Reason]))});
        {protocol_error, _Type, Reason, Args} ->
            amqp_settle(Acc, AckMode, QStates0),
            throw({error, iolist_to_binary(io_lib:format(Reason, Args))})
    end;
amqp_gets(_Count, _MaxPayloadBytes, _Q, AckMode, _Enc, _Trunc, _IcptCtx, QStates,
          _N, _Total, Acc) ->
    amqp_settle(Acc, AckMode, QStates).

amqp_settle(Acc, AckMode, QStates) ->
    Op = case AckMode of
             ack_requeue_true -> requeue;
             reject_requeue_true -> requeue;
             reject_requeue_false -> discard;
             {modify, ModifyOp} -> ModifyOp;
             ack_requeue_false -> none
         end,
    case Op of
        none ->
            ok;
        _ ->
            lists:foldl(
              fun({_Msg, QName, CTag, MsgId}, QStates0) ->
                      {ok, QStates1, _} = rabbit_queue_type:settle(
                                            QName, Op, CTag, [MsgId], QStates0),
                      QStates1
              end, QStates, Acc)
    end,
    [Msg || {Msg, _, _, _} <- lists:reverse(Acc)].

amqp_no_ack({modify, _}) -> false;
amqp_no_ack(AckMode) -> ackmode_no_ack(AckMode).

modify_op(Body) ->
    DeliveryFailed = maps:get(<<"delivery_failed">>, Body, false),
    UndeliverableHere = maps:get(<<"undeliverable_here">>, Body, false),
    Anns = maps:get(<<"annotations">>, Body, #{}),
    is_boolean(DeliveryFailed) andalso is_boolean(UndeliverableHere) andalso
        is_map(Anns) orelse
        throw({error, <<"Invalid modify settlement fields.">>}),
    maps:foreach(
      fun(<<"x-", _/binary>>, V)
            when is_binary(V); is_float(V); is_boolean(V);
                 is_integer(V), V >= -9223372036854775808, V =< 9223372036854775807 ->
              ok;
         (K, _) ->
              throw({error, iolist_to_binary(
                              [<<"Invalid message annotation: ">>, K,
                               <<". Names must start with x- and values must be "
                                 "strings, booleans or numbers within the signed 64-bit range.">>])})
      end, Anns),
    {modify, DeliveryFailed, UndeliverableHere, Anns}.

amqp_msg(Mc0, MsgCount, Redelivered, Enc, Trunc, IcptCtx) ->
    Mc1 = rabbit_msg_interceptor:intercept_outgoing(Mc0, IcptCtx),
    Mc = mc:convert(mc_amqp, Mc1),
    Sections = amqp10_framing:decode_bin(
                 iolist_to_binary(mc:protocol_state(Mc))),
    Payload = iolist_to_binary(
                [body_bytes(S) || S <- Sections, is_body_section(S)]),
    RoutingKey = case mc:routing_keys(Mc) of
                     [RK | _] -> RK;
                     [] -> <<>>
                 end,
    [{payload_bytes, size(Payload)},
     {redelivered, Redelivered},
     {exchange, mc:exchange(Mc)},
     {routing_key, RoutingKey},
     {message_count, MsgCount},
     {properties, section_to_json(lists:keyfind('v1_0.properties', 1, Sections))}] ++
        optional_section(header, 'v1_0.header', Sections) ++
        optional_section(application_properties, 'v1_0.application_properties', Sections) ++
        optional_section(message_annotations, 'v1_0.message_annotations', Sections) ++
        payload_part(maybe_truncate(Payload, Trunc), Enc).

optional_section(Key, Tag, Sections) ->
    case section_to_json(lists:keyfind(Tag, 1, Sections)) of
        Json when map_size(Json) =:= 0 -> [];
        Json -> [{Key, Json}]
    end.

is_body_section(#'v1_0.data'{}) -> true;
is_body_section(#'v1_0.amqp_value'{}) -> true;
is_body_section(#'v1_0.amqp_sequence'{}) -> true;
is_body_section(_) -> false.

body_bytes(#'v1_0.data'{content = Bin}) ->
    Bin;
body_bytes(#'v1_0.amqp_value'{content = {utf8, Bin}}) ->
    Bin;
body_bytes(Section) ->
    amqp10_framing:encode_bin(Section).

section_to_json(false) ->
    #{};
section_to_json(#'v1_0.header'{durable = D, priority = P, ttl = T,
                               delivery_count = C}) ->
    fields_to_json([{durable, D}, {priority, P}, {ttl, T}, {delivery_count, C}]);
section_to_json(#'v1_0.properties'{} = Props) ->
    [_ | Fields] = tuple_to_list(Props),
    Names = [message_id, user_id, to, subject, reply_to, correlation_id,
             content_type, content_encoding, absolute_expiry_time,
             creation_time, group_id, group_sequence, reply_to_group_id],
    fields_to_json(lists:zip(Names, Fields));
section_to_json(#'v1_0.application_properties'{content = Content}) ->
    amqp_to_json({map, Content});
section_to_json(#'v1_0.message_annotations'{content = Content}) ->
    amqp_to_json({map, Content}).

fields_to_json(Fields) ->
    maps:from_list([{K, amqp_to_json(V)} || {K, V} <- Fields, V =/= undefined]).

amqp_to_json({map, KVs}) ->
    maps:from_list([{amqp_key(K), amqp_to_json(V)} || {K, V} <- KVs]);
amqp_to_json({list, L}) ->
    [amqp_to_json(V) || V <- L];
amqp_to_json({array, _Type, L}) ->
    [amqp_to_json(V) || V <- L];
amqp_to_json({binary, Bin}) ->
    case is_utf8(Bin) of
        true -> Bin;
        false -> base64:encode(Bin)
    end;
amqp_to_json({uuid, Bin}) ->
    base64:encode(Bin);
amqp_to_json({T, V}) when T =:= utf8; T =:= symbol; T =:= char ->
    V;
amqp_to_json({T, V}) when is_atom(T), (is_number(V) orelse is_boolean(V)) ->
    V;
amqp_to_json(V) when is_number(V); is_boolean(V); V =:= null ->
    V;
amqp_to_json(V) ->
    iolist_to_binary(io_lib:format("~tp", [V])).

amqp_key({T, K}) when T =:= utf8; T =:= symbol ->
    K;
amqp_key(K) ->
    amqp_to_json(K).

parse_encoding(<<"auto">>)   -> auto;
parse_encoding(<<"base64">>) -> base64;
parse_encoding(_) ->
    throw({error, <<"Unsupported encoding. Please use auto or base64.">>}).

parse_truncate(Body) ->
    case maps:get(truncate, Body, undefined) of
        undefined -> none;
        TruncBin  -> parse_non_negative_integer(TruncBin)
    end.




parse_non_negative_integer(TruncBin) ->
    case rabbit_mgmt_util:parse_int(TruncBin) of
        Len when Len >= 0 ->
            Len;
        _ ->
            throw({error, <<"truncate must be a non-negative integer">>})
    end.

parse_count(CountBin, MaxMessages) ->
    case rabbit_mgmt_util:parse_int(CountBin) of
        Count when is_integer(Count), Count >= 1, Count =< MaxMessages ->
            Count;
        _ ->
            throw({error, iolist_to_binary(
                            [<<"Invalid count value: ">>, to_bin(CountBin),
                             <<". It must be an integer between 1 and ">>,
                             integer_to_binary(MaxMessages)])})
    end.

to_bin(V) when is_binary(V)  -> V;
to_bin(V) when is_integer(V) -> integer_to_binary(V);
to_bin(V)                    -> iolist_to_binary(io_lib:format("~p", [V])).

basic_gets(Count, MaxPayloadBytes, Ch, Q, AckMode, Enc, Trunc) ->
    basic_gets(Count, MaxPayloadBytes, 0, Ch, Q, AckMode, Enc, Trunc, []).

basic_gets(0, _, _, _, _, _, _, _, Acc) ->
    lists:reverse(Acc);
basic_gets(_, MaxPayloadBytes, Total, _, _, _, _, _, Acc)
  when Total >= MaxPayloadBytes ->
    lists:reverse(Acc);
basic_gets(Count, MaxPayloadBytes, Total, Ch, Q, AckMode, Enc, Trunc, Acc) ->
    case basic_get(Ch, Q, AckMode, Enc, Trunc) of
        none ->
            lists:reverse(Acc);
        M ->
            Size = proplists:get_value(payload_bytes, M),
            basic_gets(Count - 1, MaxPayloadBytes, Total + Size,
                       Ch, Q, AckMode, Enc, Trunc, [M | Acc])
    end.



ackmode_to_requeue(reject_requeue_false) -> false;
ackmode_to_requeue(reject_requeue_true) -> true.

-type ackmode() :: ack_requeue_true | ack_requeue_false |
                   reject_requeue_true | reject_requeue_false | modify.

-spec parse_ackmode(binary()) -> ackmode().
parse_ackmode(<<"ack_requeue_true">>)     -> ack_requeue_true;
parse_ackmode(<<"ack_requeue_false">>)    -> ack_requeue_false;
parse_ackmode(<<"reject_requeue_true">>)  -> reject_requeue_true;
parse_ackmode(<<"reject_requeue_false">>) -> reject_requeue_false;
parse_ackmode(<<"modify">>)               -> modify;
parse_ackmode(Other) ->
    throw({error, iolist_to_binary(
                    [<<"Invalid ackmode value: ">>, Other,
                     <<". Valid values are: ack_requeue_true, "
                       "ack_requeue_false, reject_requeue_true, "
                       "reject_requeue_false, modify">>])}).

-spec ackmode_no_ack(ackmode()) -> boolean().
ackmode_no_ack(ack_requeue_false)    -> true;
ackmode_no_ack(ack_requeue_true)     -> false;
ackmode_no_ack(reject_requeue_false) -> false;
ackmode_no_ack(reject_requeue_true)  -> false;
ackmode_no_ack(modify)               -> false.


% the messages must rejects later,
% because we get always the same message if the
% messages are requeued inside basic_get/5
maybe_return(R, Ch, AckMode) ->
    lists:foreach(fun(X) ->
                          maybe_reject_or_nack(Ch, AckMode,
                                               proplists:get_value(delivery_tag, X))
                  end, R).

% removes the delivery_tag from the reply.
% it is not necessary
remove_delivery_tag([])    -> [];
remove_delivery_tag([H|T]) ->
    [proplists:delete(delivery_tag, H) | [X || X <- remove_delivery_tag(T)]].


maybe_reject_or_nack(Ch, AckMode, DeliveryTag)
  when AckMode == reject_requeue_true;
       AckMode == reject_requeue_false ->
    amqp_channel:call(Ch,
                      #'basic.reject'{delivery_tag = DeliveryTag,
                                      requeue = ackmode_to_requeue(AckMode)});
maybe_reject_or_nack(Ch, ack_requeue_true, DeliveryTag) ->
    amqp_channel:call(Ch,
                      #'basic.nack'{delivery_tag = DeliveryTag,
                                    multiple = false,
                                    requeue = true});
maybe_reject_or_nack(_Ch, _AckMode, _DeliveryTag) -> ok.


basic_get(Ch, Q, AckMode, Enc, Trunc) ->
    case amqp_channel:call(Ch,
			   #'basic.get'{queue = Q,
					no_ack = ackmode_no_ack(AckMode)}) of
        {#'basic.get_ok'{redelivered   = Redelivered,
                         exchange      = Exchange,
                         routing_key   = RoutingKey,
                         message_count = MessageCount,
                         delivery_tag  = DeliveryTag},
         #amqp_msg{props = Props, payload = Payload}} ->
            [{payload_bytes, size(Payload)},
             {redelivered,   Redelivered},
             {exchange,      Exchange},
             {routing_key,   RoutingKey},
             {message_count, MessageCount},
             {delivery_tag,  DeliveryTag},
             {properties,    rabbit_mgmt_format:basic_properties(Props)}] ++
                payload_part(maybe_truncate(Payload, Trunc), Enc);
        #'basic.get_empty'{} ->
            none
    end.


is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_vhost(ReqData, Context).

%%--------------------------------------------------------------------

maybe_truncate(Payload, none)                         -> Payload;
maybe_truncate(Payload, Len) when size(Payload) < Len -> Payload;
maybe_truncate(Payload, Len) ->
    <<Start:Len/binary, _Rest/binary>> = Payload,
    binary:copy(Start).

payload_part(Payload, Enc) ->
    {PL, E} = case Enc of
                  auto -> case is_utf8(Payload) of
                              true -> {Payload, string};
                              false -> {base64:encode(Payload), base64}
                          end;
                  _    -> {base64:encode(Payload), base64}
              end,
    [{payload, PL}, {payload_encoding, E}].

is_utf8(<<>>) -> true;
is_utf8(<<_/utf8, Rest/bits>>) -> is_utf8(Rest);
is_utf8(_) -> false.
