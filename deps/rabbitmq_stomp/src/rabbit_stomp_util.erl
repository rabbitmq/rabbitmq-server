%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stomp_util).

-export([parse_message_id/1, subscription_queue_name/3]).
-export([longstr_field/2]).
-export([ack_mode/1, consumer_tag_reply_to/1, consumer_tag/1, message_headers/1,
         headers_post_process/1, headers/9, message_properties/1, tag_to_id/1,
         msg_header_name/1, ack_header_name/1, build_arguments/1, build_params/2,
         has_durable_header/1]).
-export([negotiate_version/2]).
-export([trim_headers/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(INTERNAL_TAG_PREFIX, "T_").
-define(QUEUE_TAG_PREFIX, "Q_").

%%--------------------------------------------------------------------
%% Frame and Header Parsing
%%--------------------------------------------------------------------

consumer_tag_reply_to(QueueId) ->
    internal_tag(<<?TEMP_QUEUE_ID_PREFIX, QueueId/binary>>).

consumer_tag(Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_ID) of
        {ok, Id} ->
            case Id of
                <<"/temp-queue/", _/binary>> ->
                    {error, invalid_prefix};
                _ ->
                    {ok, internal_tag(Id),
                     <<"id='", Id/binary, "'">>}
            end;
        not_found ->
            case rabbit_stomp_frame:header(Frame, ?HEADER_DESTINATION) of
                {ok, DestHdr} ->
                    {ok, queue_tag(DestHdr),
                     <<"destination='", DestHdr/binary, "'">>};
                not_found ->
                    {error, missing_destination_header}
            end
    end.

ack_mode(Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_ACK, <<"auto">>) of
        <<"auto">>              -> {auto, false};
        <<"client">>            -> {client, true};
        <<"client-individual">> -> {client, false}
    end.

message_properties(Frame = #stomp_frame{headers = Headers}) ->
    BinH = fun(K) -> rabbit_stomp_frame:binary_header(Frame, K, undefined) end,
    IntH = fun(K) -> rabbit_stomp_frame:integer_header(Frame, K, undefined) end,

    DeliveryMode = case rabbit_stomp_frame:boolean_header(
                          Frame, ?HEADER_PERSISTENT, false) of
                       true  -> 2;
                       false -> undefined
                   end,

    #'P_basic'{ content_type     = BinH(?HEADER_CONTENT_TYPE),
                content_encoding = BinH(?HEADER_CONTENT_ENCODING),
                headers          = user_headers(Headers),
                delivery_mode    = DeliveryMode,
                priority         = IntH(?HEADER_PRIORITY),
                correlation_id   = BinH(?HEADER_CORRELATION_ID),
                reply_to         = BinH(?HEADER_REPLY_TO),
                expiration       = BinH(?HEADER_EXPIRATION),
                message_id       = BinH(?HEADER_AMQP_MESSAGE_ID),
                timestamp        = IntH(?HEADER_TIMESTAMP),
                type             = BinH(?HEADER_TYPE),
                user_id          = BinH(?HEADER_USER_ID),
                app_id           = BinH(?HEADER_APP_ID) }.

message_headers(Props = #'P_basic'{headers = Headers}) ->
    adhoc_convert_headers(
      Headers,
      lists:foldl(fun({Header, Index}, Acc) ->
                          maybe_header(Header, element(Index, Props), Acc)
                  end, [],
                  [{?HEADER_CONTENT_TYPE,     #'P_basic'.content_type},
                   {?HEADER_CONTENT_ENCODING, #'P_basic'.content_encoding},
                   {?HEADER_PERSISTENT,       #'P_basic'.delivery_mode},
                   {?HEADER_PRIORITY,         #'P_basic'.priority},
                   {?HEADER_CORRELATION_ID,   #'P_basic'.correlation_id},
                   {?HEADER_REPLY_TO,         #'P_basic'.reply_to},
                   {?HEADER_EXPIRATION,       #'P_basic'.expiration},
                   {?HEADER_AMQP_MESSAGE_ID,  #'P_basic'.message_id},
                   {?HEADER_TIMESTAMP,        #'P_basic'.timestamp},
                   {?HEADER_TYPE,             #'P_basic'.type},
                   {?HEADER_USER_ID,          #'P_basic'.user_id},
                   {?HEADER_APP_ID,           #'P_basic'.app_id}])).

adhoc_convert_headers(undefined, Existing) ->
    Existing;
adhoc_convert_headers(Headers, Existing) ->
    lists:foldr(fun ({K, longstr, V}, Acc) ->
                        [{K, V} | Acc];
                    ({K, signedint, V}, Acc) ->
                        [{K, integer_to_binary(V)} | Acc];
                    ({K, long, V}, Acc) ->
                        [{K, integer_to_binary(V)} | Acc];
                    (_, Acc) ->
                        Acc
                end, Existing, Headers).

headers_extra(SessionId, ConsumerTag, DeliveryTag,
              ExchangeBin, RoutingKeyBin, Redelivered,
              AckMode, Version) ->
    case tag_to_id(ConsumerTag) of
        {ok, {internal, Id}} -> [{?HEADER_SUBSCRIPTION, Id}];
        _                    -> []
    end ++
    [{?HEADER_DESTINATION,
      format_destination(ExchangeBin, RoutingKeyBin)},
     {?HEADER_MESSAGE_ID,
      create_message_id(ConsumerTag, SessionId, DeliveryTag)},
     {?HEADER_REDELIVERED, Redelivered}] ++
    case AckMode == client andalso Version == "1.2" of
        true  -> [{?HEADER_ACK,
                   create_message_id(ConsumerTag, SessionId, DeliveryTag)}];
        false -> []
    end.

headers_post_process(Headers) ->
    [case Header of
         {?HEADER_REPLY_TO, V} ->
             case lists:any(fun(P) ->
                                S = byte_size(P),
                                case V of
                                    <<P:S/binary, _/binary>> -> true;
                                    _ -> false
                                end
                            end, ?DEST_PREFIXES) of
                 true  -> {?HEADER_REPLY_TO, V};
                 false -> {?HEADER_REPLY_TO, <<(?REPLY_QUEUE_PREFIX)/binary, V/binary>>}
             end;
         {_, _} ->
             Header
     end || Header <- Headers].

headers(SessionId, ConsumerTag, DeliveryTag,
        ExchangeBin, RoutingKey, Redelivered,
        Properties, AckMode, Version) ->
    maps:merge(
      maps:from_list(
        headers_extra(SessionId, ConsumerTag, DeliveryTag,
                      ExchangeBin, RoutingKey, Redelivered,
                      AckMode, Version)),
      maps:from_list(
        headers_post_process(message_headers(Properties)))).

tag_to_id(<<?INTERNAL_TAG_PREFIX, Id/binary>>) ->
    {ok, {internal, Id}};
tag_to_id(<<?QUEUE_TAG_PREFIX, Id/binary>>) ->
    {ok, {queue, Id}};
tag_to_id(Other) when is_binary(Other) ->
    {error, {unknown, Other}}.

user_headers(Headers) ->
    maps:fold(fun(K, V, Acc) ->
                  case user_header(K) of
                      true  -> [longstr_field(K, V) | Acc];
                      false -> Acc
                  end
              end, [], Headers).

user_header(Hdr)
  when Hdr =:= ?HEADER_CONTENT_TYPE orelse
       Hdr =:= ?HEADER_CONTENT_ENCODING orelse
       Hdr =:= ?HEADER_PERSISTENT orelse
       Hdr =:= ?HEADER_PRIORITY orelse
       Hdr =:= ?HEADER_CORRELATION_ID orelse
       Hdr =:= ?HEADER_REPLY_TO orelse
       Hdr =:= ?HEADER_EXPIRATION orelse
       Hdr =:= ?HEADER_AMQP_MESSAGE_ID orelse
       Hdr =:= ?HEADER_TIMESTAMP orelse
       Hdr =:= ?HEADER_TYPE orelse
       Hdr =:= ?HEADER_USER_ID orelse
       Hdr =:= ?HEADER_APP_ID orelse
       Hdr =:= ?HEADER_DESTINATION ->
    false;
user_header(_) ->
    true.

parse_message_id(MessageId) ->
    case binary:split(MessageId, ?MESSAGE_ID_SEPARATOR, [global]) of
        [ConsumerTag, SessionId, DeliveryTag] ->
            {ok, {ConsumerTag,
                  binary_to_list(SessionId),
                  binary_to_integer(DeliveryTag)}};
        _ ->
            {error, invalid_message_id}
    end.

negotiate_version(ClientVers, ServerVers) ->
    Common = lists:filter(fun(Ver) ->
                                  lists:member(Ver, ServerVers)
                          end, ClientVers),
    case Common of
        [] ->
            {error, no_common_version};
        [H|T] ->
            {ok, lists:foldl(fun(Ver, AccN) ->
                                max_version(Ver, AccN)
                        end, H, T)}
    end.

max_version(V, V) ->
    V;
max_version(V1, V2) ->
    Split = fun(X) -> re:split(X, "\\.", [{return, list}]) end,
    find_max_version({V1, Split(V1)}, {V2, Split(V2)}).

find_max_version({V1, [X|T1]}, {V2, [X|T2]}) ->
    find_max_version({V1, T1}, {V2, T2});
find_max_version({V1, [X]}, {V2, [Y]}) ->
    case list_to_integer(X) >= list_to_integer(Y) of
        true  -> V1;
        false -> V2
    end;
find_max_version({_V1, []}, {V2, Y}) when length(Y) > 0 ->
    V2;
find_max_version({V1, X}, {_V2, []}) when length(X) > 0 ->
    V1.

%% ---- Header processing helpers ----

longstr_field(K, V) ->
    {K, longstr, V}.

maybe_header(_Key, undefined, Acc) ->
    Acc;
maybe_header(?HEADER_PERSISTENT, 2, Acc) ->
    [{?HEADER_PERSISTENT, <<"true">>} | Acc];
maybe_header(Key, Value, Acc) when is_binary(Value) ->
    [{Key, Value} | Acc];
maybe_header(Key, Value, Acc) when is_integer(Value) ->
    [{Key, integer_to_binary(Value)} | Acc];
maybe_header(_Key, _Value, Acc) ->
    Acc.

create_message_id(ConsumerTag, SessionId, DeliveryTag) ->
    iolist_to_binary([ConsumerTag, ?MESSAGE_ID_SEPARATOR,
                      SessionId, ?MESSAGE_ID_SEPARATOR,
                      integer_to_binary(DeliveryTag)]).

trim_headers(Frame = #stomp_frame{headers = Hdrs}) ->
    Frame#stomp_frame{headers = maps:map(fun(_K, V) -> string:trim(V, leading) end, Hdrs)}.

internal_tag(Base) ->
    <<?INTERNAL_TAG_PREFIX, Base/binary>>.

queue_tag(Base) ->
    <<?QUEUE_TAG_PREFIX, Base/binary>>.

ack_header_name("1.2") -> ?HEADER_ID;
ack_header_name("1.1") -> ?HEADER_MESSAGE_ID;
ack_header_name("1.0") -> ?HEADER_MESSAGE_ID.

msg_header_name("1.2") -> ?HEADER_ACK;
msg_header_name("1.1") -> ?HEADER_MESSAGE_ID;
msg_header_name("1.0") -> ?HEADER_MESSAGE_ID.

build_arguments(Headers) ->
    Arguments =
        fold_headers(fun(K, V, Acc) ->
                         case lists:member(K, ?HEADER_ARGUMENTS) of
                             true  -> [build_argument(K, V) | Acc];
                             false -> Acc
                         end
                     end, [], Headers),
    {arguments, Arguments}.

fold_headers(Fun, Acc, Headers) ->
    maps:fold(Fun, Acc, Headers).

build_argument(?HEADER_X_DEAD_LETTER_EXCHANGE, Val) ->
    {?HEADER_X_DEAD_LETTER_EXCHANGE, longstr, string:trim(Val)};
build_argument(?HEADER_X_DEAD_LETTER_ROUTING_KEY, Val) ->
    {?HEADER_X_DEAD_LETTER_ROUTING_KEY, longstr, string:trim(Val)};
build_argument(?HEADER_X_EXPIRES, Val) ->
    {?HEADER_X_EXPIRES, long, binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_MAX_LENGTH, Val) ->
    {?HEADER_X_MAX_LENGTH, long, binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_MAX_LENGTH_BYTES, Val) ->
    {?HEADER_X_MAX_LENGTH_BYTES, long, binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_MAX_PRIORITY, Val) ->
    {?HEADER_X_MAX_PRIORITY, long, binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_MESSAGE_TTL, Val) ->
    {?HEADER_X_MESSAGE_TTL, long, binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_MAX_AGE, Val) ->
    {?HEADER_X_MAX_AGE, longstr, string:trim(Val)};
build_argument(?HEADER_X_STREAM_MAX_SEGMENT_SIZE_BYTES, Val) ->
    {?HEADER_X_STREAM_MAX_SEGMENT_SIZE_BYTES, long,
     binary_to_integer(string:trim(Val))};
build_argument(?HEADER_X_QUEUE_TYPE, Val) ->
    {?HEADER_X_QUEUE_TYPE, longstr, string:trim(Val)};
build_argument(?HEADER_X_STREAM_FILTER_SIZE_BYTES, Val) ->
    {?HEADER_X_STREAM_FILTER_SIZE_BYTES, long,
     binary_to_integer(string:trim(Val))}.


build_params(EndPoint, Headers) ->
    Params = fold_headers(fun(K, V, Acc) ->
                              case lists:member(K, ?HEADER_PARAMS) of
                                  true  -> [build_param(K, V) | Acc];
                                  false -> Acc
                              end
                          end, [], Headers),
    rabbit_misc:plmerge(default_params(EndPoint), Params).

build_param(?HEADER_PERSISTENT, Val) ->
    {durable, string_to_boolean(Val)};

build_param(?HEADER_DURABLE, Val) ->
    {durable, string_to_boolean(Val)};

build_param(?HEADER_AUTO_DELETE, Val) ->
    {auto_delete, string_to_boolean(Val)};

build_param(?HEADER_EXCLUSIVE, Val) ->
    {exclusive, string_to_boolean(Val)}.

default_params({queue, _}) ->
    [{durable, true}];

default_params({exchange, _}) ->
    [{exclusive, true}, {auto_delete, true}];

default_params({topic, _}) ->
    [{exclusive, false}, {auto_delete, true}];

default_params(_) ->
    [{durable, false}].

string_to_boolean(<<"True">>) ->
    true;
string_to_boolean(<<"true">>) ->
    true;
string_to_boolean(<<"False">>) ->
    false;
string_to_boolean(<<"false">>) ->
    false;
string_to_boolean(_) ->
    undefined.

has_durable_header(Frame) ->
    rabbit_stomp_frame:boolean_header(
      Frame, ?HEADER_DURABLE, false) or
    rabbit_stomp_frame:boolean_header(
      Frame, ?HEADER_PERSISTENT, false).

%%--------------------------------------------------------------------
%% Destination Formatting
%%--------------------------------------------------------------------

format_destination(<<>>, RoutingKey) ->
    iolist_to_binary([?QUEUE_PREFIX, $/, escape_dest(RoutingKey)]);
format_destination(<<"amq.topic">>, RoutingKey) ->
    iolist_to_binary([?TOPIC_PREFIX, $/, escape_dest(RoutingKey)]);
format_destination(Exchange, <<>>) ->
    iolist_to_binary([?EXCHANGE_PREFIX, $/, escape_dest(Exchange)]);
format_destination(Exchange, RoutingKey) ->
    iolist_to_binary([?EXCHANGE_PREFIX, $/, escape_dest(Exchange), $/, escape_dest(RoutingKey)]).

%%--------------------------------------------------------------------
%% Destination Parsing
%%--------------------------------------------------------------------

subscription_queue_name(Destination, SubscriptionId, Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_X_QUEUE_NAME, undefined) of
        undefined ->
            rabbit_guid:binary(
              erlang:md5(
                term_to_binary_compat:term_to_binary_1(
                  {Destination, SubscriptionId})),
              "stomp-subscription");
        Name ->
            Name
    end.

%% ---- Helpers ----

escape_dest(Bin) -> escape_dest(Bin, []).

escape_dest(<<>>, Acc) -> iolist_to_binary(lists:reverse(Acc));
escape_dest(<<$/, Rest/binary>>, Acc) -> escape_dest(Rest, [<<"%2F">> | Acc]);
escape_dest(<<$%, Rest/binary>>, Acc) -> escape_dest(Rest, [<<"%25">> | Acc]);
escape_dest(<<X, Rest/binary>>, Acc) when X < 32; X > 127 ->
    escape_dest(Rest, [revhex_bin(X) | Acc]);
escape_dest(<<C, Rest/binary>>, Acc) -> escape_dest(Rest, [C | Acc]).

revhex_bin(I) ->
    iolist_to_binary([$%, hexdig(I bsr 4), hexdig(I)]).

hexdig(I) -> erlang:integer_to_list(I band 15, 16).
