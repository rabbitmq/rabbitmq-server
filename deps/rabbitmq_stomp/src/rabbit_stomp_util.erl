%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stomp_util).

-export([parse_message_id/1, subscription_queue_name/3]).
-export([longstr_field/2]).
-export([ack_mode/1, consumer_tag_reply_to/1, consumer_tag/1, message_headers/1,
         headers_post_process/1, headers/5, message_properties/1, tag_to_id/1,
         msg_header_name/1, ack_header_name/1, build_arguments/1, build_params/2,
         has_durable_header/1]).
-export([negotiate_version/2]).
-export([trim_headers/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("amqp_client/include/rabbit_routing_prefixes.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(INTERNAL_TAG_PREFIX, "T_").
-define(QUEUE_TAG_PREFIX, "Q_").

%%--------------------------------------------------------------------
%% Frame and Header Parsing
%%--------------------------------------------------------------------

consumer_tag_reply_to(QueueId) ->
    internal_tag(?TEMP_QUEUE_ID_PREFIX ++ QueueId).

consumer_tag(Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_ID) of
        {ok, Id} ->
            case lists:prefix(?TEMP_QUEUE_ID_PREFIX, Id) of
                false -> {ok, internal_tag(Id), "id='" ++ Id ++ "'"};
                true  -> {error, invalid_prefix}
            end;
        not_found ->
            case rabbit_stomp_frame:header(Frame, ?HEADER_DESTINATION) of
                {ok, DestHdr} ->
                    {ok, queue_tag(DestHdr),
                     "destination='" ++ DestHdr ++ "'"};
                not_found ->
                    {error, missing_destination_header}
            end
    end.

ack_mode(Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_ACK, "auto") of
        "auto"              -> {auto, false};
        "client"            -> {client, true};
        "client-individual" -> {client, false}
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
                headers          = [longstr_field(K, V) ||
                                       {K, V} <- Headers, user_header(K)],
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
                        [{binary_to_list(K), binary_to_list(V)} | Acc];
                    ({K, signedint, V}, Acc) ->
                        [{binary_to_list(K), integer_to_list(V)} | Acc];
                    ({K, long, V}, Acc) ->
                        [{binary_to_list(K), integer_to_list(V)} | Acc];
                    (_, Acc) ->
                        Acc
                end, Existing, Headers).

headers_extra(SessionId, AckMode, Version,
              #'basic.deliver'{consumer_tag = ConsumerTag,
                               delivery_tag = DeliveryTag,
                               exchange     = ExchangeBin,
                               routing_key  = RoutingKeyBin,
                               redelivered  = Redelivered}) ->
    case tag_to_id(ConsumerTag) of
        {ok, {internal, Id}} -> [{?HEADER_SUBSCRIPTION, Id}];
        _                    -> []
    end ++
    [{?HEADER_DESTINATION,
      format_destination(binary_to_list(ExchangeBin),
                         binary_to_list(RoutingKeyBin))},
     {?HEADER_MESSAGE_ID,
      create_message_id(ConsumerTag, SessionId, DeliveryTag)},
     {?HEADER_REDELIVERED, Redelivered}] ++
    case AckMode == client andalso Version == "1.2" of
        true  -> [{?HEADER_ACK,
                   create_message_id(ConsumerTag, SessionId, DeliveryTag)}];
        false -> []
    end.

headers_post_process(Headers) ->
    Prefixes = rabbit_routing_util:dest_prefixes(),
    [case Header of
         {?HEADER_REPLY_TO, V} ->
             case lists:any(fun (P) -> lists:prefix(P, V) end, Prefixes) of
                 true  -> {?HEADER_REPLY_TO, V};
                 false -> {?HEADER_REPLY_TO, ?REPLY_QUEUE_PREFIX ++ V}
             end;
         {_, _} ->
             Header
     end || Header <- Headers].

headers(SessionId, Delivery, Properties, AckMode, Version) ->
    headers_extra(SessionId, AckMode, Version, Delivery) ++
    headers_post_process(message_headers(Properties)).

tag_to_id(<<?INTERNAL_TAG_PREFIX, Id/binary>>) ->
    {ok, {internal, binary_to_list(Id)}};
tag_to_id(<<?QUEUE_TAG_PREFIX,    Id/binary>>) ->
    {ok, {queue, binary_to_list(Id)}};
tag_to_id(Other) when is_binary(Other) ->
    {error, {unknown, binary_to_list(Other)}}.

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
    case split(MessageId, ?MESSAGE_ID_SEPARATOR) of
        [ConsumerTag, SessionId, DeliveryTag] ->
            {ok, {list_to_binary(ConsumerTag),
                  SessionId,
                  list_to_integer(DeliveryTag)}};
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
    {list_to_binary(K), longstr, list_to_binary(V)}.

maybe_header(_Key, undefined, Acc) ->
    Acc;
maybe_header(?HEADER_PERSISTENT, 2, Acc) ->
    [{?HEADER_PERSISTENT, "true"} | Acc];
maybe_header(Key, Value, Acc) when is_binary(Value) ->
    [{Key, binary_to_list(Value)} | Acc];
maybe_header(Key, Value, Acc) when is_integer(Value) ->
    [{Key, integer_to_list(Value)}| Acc];
maybe_header(_Key, _Value, Acc) ->
    Acc.

create_message_id(ConsumerTag, SessionId, DeliveryTag) ->
    [ConsumerTag,
     ?MESSAGE_ID_SEPARATOR,
     SessionId,
     ?MESSAGE_ID_SEPARATOR,
     integer_to_list(DeliveryTag)].

trim_headers(Frame = #stomp_frame{headers = Hdrs}) ->
    Frame#stomp_frame{headers = [{K, string:strip(V, left)} || {K, V} <- Hdrs]}.

internal_tag(Base) ->
    list_to_binary(?INTERNAL_TAG_PREFIX ++ Base).

queue_tag(Base) ->
    list_to_binary(?QUEUE_TAG_PREFIX ++ Base).

ack_header_name("1.2") -> ?HEADER_ID;
ack_header_name("1.1") -> ?HEADER_MESSAGE_ID;
ack_header_name("1.0") -> ?HEADER_MESSAGE_ID.

msg_header_name("1.2") -> ?HEADER_ACK;
msg_header_name("1.1") -> ?HEADER_MESSAGE_ID;
msg_header_name("1.0") -> ?HEADER_MESSAGE_ID.

build_arguments(Headers) ->
    Arguments =
        lists:foldl(fun({K, V}, Acc) ->
                            case lists:member(K, ?HEADER_ARGUMENTS) of
                                true  -> [build_argument(K, V) | Acc];
                                false -> Acc
                            end
                    end,
                    [],
                    Headers),
    {arguments, Arguments}.

build_argument(?HEADER_X_DEAD_LETTER_EXCHANGE, Val) ->
    {list_to_binary(?HEADER_X_DEAD_LETTER_EXCHANGE), longstr,
     list_to_binary(string:strip(Val))};
build_argument(?HEADER_X_DEAD_LETTER_ROUTING_KEY, Val) ->
    {list_to_binary(?HEADER_X_DEAD_LETTER_ROUTING_KEY), longstr,
     list_to_binary(string:strip(Val))};
build_argument(?HEADER_X_EXPIRES, Val) ->
    {list_to_binary(?HEADER_X_EXPIRES), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_MAX_LENGTH, Val) ->
    {list_to_binary(?HEADER_X_MAX_LENGTH), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_MAX_LENGTH_BYTES, Val) ->
    {list_to_binary(?HEADER_X_MAX_LENGTH_BYTES), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_MAX_PRIORITY, Val) ->
    {list_to_binary(?HEADER_X_MAX_PRIORITY), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_MESSAGE_TTL, Val) ->
    {list_to_binary(?HEADER_X_MESSAGE_TTL), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_MAX_AGE, Val) ->
    {list_to_binary(?HEADER_X_MAX_AGE), longstr,
     list_to_binary(string:strip(Val))};
build_argument(?HEADER_X_STREAM_MAX_SEGMENT_SIZE_BYTES, Val) ->
    {list_to_binary(?HEADER_X_STREAM_MAX_SEGMENT_SIZE_BYTES), long,
     list_to_integer(string:strip(Val))};
build_argument(?HEADER_X_QUEUE_TYPE, Val) ->
  {list_to_binary(?HEADER_X_QUEUE_TYPE), longstr,
    list_to_binary(string:strip(Val))}.

build_params(EndPoint, Headers) ->
    Params = lists:foldl(fun({K, V}, Acc) ->
                             case lists:member(K, ?HEADER_PARAMS) of
                               true  -> [build_param(K, V) | Acc];
                               false -> Acc
                             end
                         end,
                         [],
                         Headers),
    rabbit_misc:plmerge(Params, default_params(EndPoint)).

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

string_to_boolean("True") ->
    true;
string_to_boolean("true") ->
    true;
string_to_boolean("False") ->
    false;
string_to_boolean("false") ->
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

format_destination("", RoutingKey) ->
    ?QUEUE_PREFIX ++ "/" ++ escape(RoutingKey);
format_destination("amq.topic", RoutingKey) ->
    ?TOPIC_PREFIX ++ "/" ++ escape(RoutingKey);
format_destination(Exchange, "") ->
    ?EXCHANGE_PREFIX ++ "/" ++ escape(Exchange);
format_destination(Exchange, RoutingKey) ->
    ?EXCHANGE_PREFIX ++ "/" ++ escape(Exchange) ++ "/" ++ escape(RoutingKey).

%%--------------------------------------------------------------------
%% Destination Parsing
%%--------------------------------------------------------------------

subscription_queue_name(Destination, SubscriptionId, Frame) ->
    case rabbit_stomp_frame:header(Frame, ?HEADER_X_QUEUE_NAME, undefined) of
        undefined ->
            %% We need a queue name that a) can be derived from the
            %% Destination and SubscriptionId, and b) meets the constraints on
            %% AMQP queue names. It doesn't need to be secure; we use md5 here
            %% simply as a convenient means to bound the length.
            rabbit_guid:string(
                erlang:md5(
                    term_to_binary_compat:term_to_binary_1(
                        {Destination, SubscriptionId})),
              "stomp-subscription");
        Name ->
            Name
    end.

%% ---- Helpers ----

split([],      _Splitter) -> [];
split(Content, Splitter)  -> split(Content, [], [], Splitter).

split([], RPart, RParts, _Splitter) ->
    lists:reverse([lists:reverse(RPart) | RParts]);
split(Content = [Elem | Rest1], RPart, RParts, Splitter) ->
    case take_prefix(Splitter, Content) of
        {ok, Rest2} ->
            split(Rest2, [], [lists:reverse(RPart) | RParts], Splitter);
        not_found ->
            split(Rest1, [Elem | RPart], RParts, Splitter)
    end.

take_prefix([Char | Prefix], [Char | List]) -> take_prefix(Prefix, List);
take_prefix([],              List)          -> {ok, List};
take_prefix(_Prefix,         _List)         -> not_found.

escape(Str) -> escape(Str, []).

escape([$/ | Str], Acc) -> escape(Str, "F2%" ++ Acc);  %% $/ == '2F'x
escape([$% | Str], Acc) -> escape(Str, "52%" ++ Acc);  %% $% == '25'x
escape([X | Str],  Acc) when X < 32 orelse X > 127 ->
                           escape(Str, revhex(X) ++ "%" ++ Acc);
escape([C | Str],  Acc) -> escape(Str, [C | Acc]);
escape([],         Acc) -> lists:reverse(Acc).

revhex(I) -> hexdig(I) ++ hexdig(I bsr 4).

hexdig(I) -> erlang:integer_to_list(I band 15, 16).
