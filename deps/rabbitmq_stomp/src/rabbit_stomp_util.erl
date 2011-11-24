%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_stomp_util).

-export([parse_destination/1, parse_routing_information/1,
         parse_message_id/1, durable_subscription_queue/2]).
-export([longstr_field/2]).
-export([ack_mode/1, consumer_tag/1, message_headers/4, message_properties/1]).
-export([negotiate_version/2]).
-export([trim_headers/1]).
-export([valid_dest_prefixes/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_prefixes.hrl").

-define(MESSAGE_ID_SEPARATOR, "@@").
-define(HEADER_CONTENT_TYPE, "content-type").
-define(HEADER_CONTENT_ENCODING, "content-encoding").
-define(HEADER_PERSISTENT, "persistent").
-define(HEADER_PRIORITY, "priority").
-define(HEADER_CORRELATION_ID, "correlation-id").
-define(HEADER_REPLY_TO, "reply-to").
-define(HEADER_AMQP_MESSAGE_ID, "amqp-message-id").

%%--------------------------------------------------------------------
%% Frame and Header Parsing
%%--------------------------------------------------------------------

consumer_tag(Frame) ->
    case rabbit_stomp_frame:header(Frame, "id") of
        {ok, Str} ->
            {ok, list_to_binary("T_" ++ Str), "id='" ++ Str ++ "'"};
        not_found ->
            case rabbit_stomp_frame:header(Frame, "destination") of
                {ok, DestHdr} ->
                    {ok, list_to_binary("Q_" ++ DestHdr), "destination='" ++ DestHdr ++ "'"};
                not_found ->
                    {error, missing_destination_header}
            end
    end.

ack_mode(Frame) ->
    case rabbit_stomp_frame:header(Frame, "ack", "auto") of
        "auto"              -> {auto, false};
        "client"            -> {client, true};
        "client-individual" -> {client, false}
    end.

message_properties(Frame = #stomp_frame{headers = Headers}) ->
    BinH = fun(K, V) -> rabbit_stomp_frame:binary_header(Frame, K, V) end,
    IntH = fun(K, V) -> rabbit_stomp_frame:integer_header(Frame, K, V) end,

    DeliveryMode =
        case rabbit_stomp_frame:boolean_header(Frame, "persistent", false) of
            true  -> 2;
            false -> undefined
        end,

    #'P_basic'{
      content_type     = BinH(?HEADER_CONTENT_TYPE,     undefined),
      content_encoding = BinH(?HEADER_CONTENT_ENCODING, undefined),
      delivery_mode    = DeliveryMode,
      priority         = IntH(?HEADER_PRIORITY,         undefined),
      correlation_id   = BinH(?HEADER_CORRELATION_ID,   undefined),
      reply_to         = BinH(?HEADER_REPLY_TO,         undefined),
      message_id       = BinH(?HEADER_AMQP_MESSAGE_ID,  undefined),
      headers          = [longstr_field(K, V) ||
                             {K, V} <- Headers, user_header(K)]}.

message_headers(Destination, SessionId,
                #'basic.deliver'{consumer_tag = ConsumerTag,
                                 delivery_tag = DeliveryTag},
                Props = #'P_basic'{headers       = Headers}) ->
    Basic = [{"destination", Destination},
             {"message-id",
              create_message_id(ConsumerTag, SessionId, DeliveryTag)}],

    Standard =
        lists:foldl(
          fun({Header, Index}, Acc) ->
                  maybe_header(Header, element(Index, Props), Acc)
          end,
          case ConsumerTag of
              <<"T_", Id/binary>> ->
                  [{"subscription", binary_to_list(Id)} | Basic];
              _ ->
                  Basic
          end,
          [{?HEADER_CONTENT_TYPE,     #'P_basic'.content_type},
           {?HEADER_CONTENT_ENCODING, #'P_basic'.content_encoding},
           {?HEADER_PERSISTENT,       #'P_basic'.delivery_mode},
           {?HEADER_PRIORITY,         #'P_basic'.priority},
           {?HEADER_CORRELATION_ID,   #'P_basic'.correlation_id},
           {?HEADER_REPLY_TO,         #'P_basic'.reply_to},
           {?HEADER_AMQP_MESSAGE_ID,  #'P_basic'.message_id}]),

    adhoc_convert_headers(Headers, Standard).

user_header(Hdr)
  when Hdr =:= ?HEADER_CONTENT_TYPE orelse
       Hdr =:= ?HEADER_CONTENT_ENCODING orelse
       Hdr =:= ?HEADER_PERSISTENT orelse
       Hdr =:= ?HEADER_PRIORITY orelse
       Hdr =:= ?HEADER_CORRELATION_ID orelse
       Hdr =:= ?HEADER_REPLY_TO orelse
       Hdr =:= ?HEADER_AMQP_MESSAGE_ID orelse
       Hdr =:= "destination" ->
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

adhoc_convert_headers(undefined, Existing) ->
    Existing;
adhoc_convert_headers(Headers, Existing) ->
    lists:foldr(fun ({K, longstr, V}, Acc) ->
                        [{binary_to_list(K), binary_to_list(V)} | Acc];
                    ({K, signedint, V}, Acc) ->
                        [{binary_to_list(K), integer_to_list(V)} | Acc];
                    (_, Acc) ->
                        Acc
                end, Existing, Headers).

create_message_id(ConsumerTag, SessionId, DeliveryTag) ->
    [ConsumerTag,
     ?MESSAGE_ID_SEPARATOR,
     SessionId,
     ?MESSAGE_ID_SEPARATOR,
     integer_to_list(DeliveryTag)].

trim_headers(Frame = #stomp_frame{headers = Hdrs}) ->
    Frame#stomp_frame{headers = [{K, string:strip(V, left)} || {K, V} <- Hdrs]}.

%%--------------------------------------------------------------------
%% Destination Parsing
%%--------------------------------------------------------------------

parse_destination(?QUEUE_PREFIX ++ Rest) ->
    parse_simple_destination(queue, Rest);
parse_destination(?TOPIC_PREFIX ++ Rest) ->
    parse_simple_destination(topic, Rest);
parse_destination(?AMQQUEUE_PREFIX ++ Rest) ->
    parse_simple_destination(amqqueue, Rest);
parse_destination(?TEMP_QUEUE_PREFIX ++ Rest) ->
    parse_simple_destination(temp_queue, Rest);
parse_destination(?REPLY_QUEUE_PREFIX ++ Rest) ->
    %% reply queue names might have slashes
    {ok, {reply_queue, Rest}};
parse_destination(?EXCHANGE_PREFIX ++ Rest) ->
    case parse_content(Rest) of
        %% One cannot refer to the default exchange this way; it has
        %% different semantics for subscribe and send
        ["" | _]        -> {error, {invalid_destination, exchange, Rest}};
        [Name]          -> {ok, {exchange, {Name, undefined}}};
        [Name, Pattern] -> {ok, {exchange, {Name, Pattern}}};
        _               -> {error, {invalid_destination, exchange, Rest}}
    end;
parse_destination(Destination) ->
    {error, {unknown_destination, Destination}}.

parse_routing_information({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing_information({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing_information({topic, Name}) ->
    {"amq.topic", Name};
parse_routing_information({Type, Name})
  when Type =:= queue orelse Type =:= reply_queue orelse Type =:= amqqueue ->
    {"", Name}.

valid_dest_prefixes() -> ?VALID_DEST_PREFIXES.

durable_subscription_queue(Destination, SubscriptionId) ->
    <<(list_to_binary("stomp.dsub." ++ Destination ++ "."))/binary,
      (erlang:md5(SubscriptionId))/binary>>.

%% ---- Destination parsing helpers ----

parse_simple_destination(Type, Content) ->
    case parse_content(Content) of
        [Name = [_|_]] -> {ok, {Type, Name}};
        _              -> {error, {invalid_destination, Type, Content}}
    end.

parse_content(Content)->
    {_, Content2} = take_prefix("/", Content),
    [unescape(X) || X <- split(Content2, "/")].

split([], _Splitter) ->
    [];
split(Content, []) ->
    Content;
split(Content, Splitter) ->
    split(Content, [], [], Splitter).

split([], RPart, RParts, _Splitter) ->
    lists:reverse([lists:reverse(RPart) | RParts]);
split(Content = [Elem | Rest1], RPart, RParts, Splitter) ->
    case take_prefix(Splitter, Content) of
        {true, Rest2} ->
            split(Rest2, [], [lists:reverse(RPart) | RParts], Splitter);
        {false, _} ->
            split(Rest1, [Elem | RPart], RParts, Splitter)
    end.

take_prefix(Prefix, List) ->
    take_prefix(Prefix, List, true).

take_prefix([Char | Prefix], [Char | List], AllMatched) ->
    take_prefix(Prefix, List, AllMatched);
take_prefix([], List, AllMatched) ->
    {AllMatched, List};
take_prefix(_Prefix, List, _AllMatched) ->
    {false, List}.

unescape(Str) ->
    unescape(Str, []).

unescape("%2F" ++ Str, Acc) ->
    unescape(Str, [$/ | Acc]);
unescape([C | Str], Acc) ->
    unescape(Str, [C | Acc]);
unescape([], Acc) ->
    lists:reverse(Acc).
