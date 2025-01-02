%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_basic).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-export([message/3,
         message_no_id/3,
         message/4,
         properties/1,
         prepend_table_header/3,
         extract_headers/1,
         extract_timestamp/1,
         map_headers/2,
         delivery/4,
         delivery/5,
         header_routes/1, parse_expiration/1, header/2, header/3]).
-export([build_content/2, from_content/1, msg_size/1,
         maybe_gc_large_msg/1, maybe_gc_large_msg/2]).
-export([strip_bcc_header/1,
         add_header/4,
         peek_fmt_message/3]).

%%----------------------------------------------------------------------------

-type properties_input() ::
        rabbit_framing:amqp_property_record() | [{atom(), any()}].
-type header() :: any().
-type headers() :: rabbit_framing:amqp_table() | 'undefined'.

%%----------------------------------------------------------------------------

-spec delivery
        (boolean(), boolean(), mc:state(), undefined | integer()) ->
            rabbit_types:delivery().
delivery(Mandatory, Confirm, Message, MsgSeqNo) ->
    delivery(Mandatory, Confirm, Message, MsgSeqNo, noflow).

delivery(Mandatory, Confirm, Message, MsgSeqNo, Flow) ->
    #delivery{mandatory = Mandatory, confirm = Confirm, sender = self(),
              message = Message, msg_seq_no = MsgSeqNo, flow = Flow}.

-spec build_content
        (rabbit_framing:amqp_property_record(), binary() | [binary()]) ->
            rabbit_types:content().

build_content(Properties, BodyBin) when is_binary(BodyBin) ->
    build_content(Properties, [BodyBin]);

build_content(Properties, PFR) ->
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = none,
             protocol = none,
             payload_fragments_rev = PFR}.

-spec from_content
        (rabbit_types:content()) ->
            {rabbit_framing:amqp_property_record(), binary()}.

from_content(Content) ->
    #content{class_id = ClassId,
             properties = Props,
             payload_fragments_rev = FragmentsRev} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% basic.publish hasn't changed so we can just hard-code amqp_0_9_1
    {ClassId, _MethodId} =
        rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    {Props, list_to_binary(lists:reverse(FragmentsRev))}.

strip_bcc_header(#content{} = Content) ->
    strip_header(Content, ?DELETED_HEADER).

%% This breaks the spec rule forbidding message modification
strip_header(#content{properties = #'P_basic'{headers = undefined}}
             = DecodedContent, _Key) ->
    DecodedContent;
strip_header(#content{properties = Props = #'P_basic'{headers = Headers}}
             = DecodedContent, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        false          -> DecodedContent;
        {value, Found} -> Headers0 = lists:delete(Found, Headers),
                          rabbit_binary_generator:clear_encoded_content(
                            DecodedContent#content{
                              properties = Props#'P_basic'{
                                             headers = Headers0}})
    end.

-spec message
        (rabbit_exchange:name(), rabbit_router:routing_key(),
         rabbit_types:decoded_content()) ->
            rabbit_types:ok_or_error2(rabbit_types:message(), any()).
message(XName, RoutingKey, Content) ->
    make_message(XName, RoutingKey, Content, rabbit_guid:gen()).

%% only used by channel to avoid unnecessarily generating a guid when
%% queue types do not need them
-spec message_no_id
        (rabbit_exchange:name(), rabbit_router:routing_key(),
         rabbit_types:decoded_content()) ->
            rabbit_types:ok_or_error2(rabbit_types:message(), any()).
 message_no_id(XName, RoutingKey, Content) ->
    make_message(XName, RoutingKey, Content, <<>>).

-spec message
        (rabbit_exchange:name(), rabbit_router:routing_key(), properties_input(),
         binary()) ->
            rabbit_types:message().
message(XName, RoutingKey, RawProperties, Body) ->
    Properties = properties(RawProperties),
    Content = build_content(Properties, Body),
    {ok, Msg} = message(XName, RoutingKey, Content),
    Msg.

-spec properties
        (properties_input()) -> rabbit_framing:amqp_property_record().

properties(P = #'P_basic'{}) ->
    P;
properties(P) when is_list(P) ->
    %% Yes, this is O(length(P) * record_info(size, 'P_basic') / 2),
    %% i.e. slow. Use the definition of 'P_basic' directly if
    %% possible!
    lists:foldl(fun ({Key, Value}, Acc) ->
                        case indexof(record_info(fields, 'P_basic'), Key) of
                            0 -> throw({unknown_basic_property, Key});
                            N -> setelement(N + 1, Acc, Value)
                        end
                end, #'P_basic'{}, P).

-spec prepend_table_header
        (binary(), rabbit_framing:amqp_table(), headers()) -> headers().

prepend_table_header(Name, Info, undefined) ->
    prepend_table_header(Name, Info, []);
prepend_table_header(Name, Info, Headers) ->
    case rabbit_misc:table_lookup(Headers, Name) of
        {array, Existing} ->
            prepend_table(Name, Info, Existing, Headers);
        undefined ->
            prepend_table(Name, Info, [], Headers);
        Other ->
            Headers2 = prepend_table(Name, Info, [], Headers),
            set_invalid_header(Name, Other, Headers2)
    end.

prepend_table(Name, Info, Prior, Headers) ->
    rabbit_misc:set_table_value(Headers, Name, array, [{table, Info} | Prior]).

set_invalid_header(Name, {_, _}=Value, Headers) when is_list(Headers) ->
    case rabbit_misc:table_lookup(Headers, ?INVALID_HEADERS_KEY) of
        undefined ->
            set_invalid([{Name, array, [Value]}], Headers);
        {table, ExistingHdr} ->
            update_invalid(Name, Value, ExistingHdr, Headers);
        Other ->
            %% somehow the x-invalid-headers header is corrupt
            Invalid = [{?INVALID_HEADERS_KEY, array, [Other]}],
            set_invalid_header(Name, Value, set_invalid(Invalid, Headers))
    end.

set_invalid(NewHdr, Headers) ->
    rabbit_misc:set_table_value(Headers, ?INVALID_HEADERS_KEY, table, NewHdr).

update_invalid(Name, Value, ExistingHdr, Header) ->
    Values = case rabbit_misc:table_lookup(ExistingHdr, Name) of
                 undefined      -> [Value];
                 {array, Prior} -> [Value | Prior]
             end,
    NewHdr = rabbit_misc:set_table_value(ExistingHdr, Name, array, Values),
    set_invalid(NewHdr, Header).

-spec header(header(), headers()) -> 'undefined' | any().

header(_Header, undefined) ->
    undefined;
header(_Header, []) ->
    undefined;
header(Header, Headers) ->
    header(Header, Headers, undefined).

-spec header(header(), headers(), any()) -> 'undefined' | any().

header(Header, Headers, Default) ->
    case lists:keysearch(Header, 1, Headers) of
        false        -> Default;
        {value, Val} -> Val
    end.

-spec extract_headers(rabbit_types:content()) -> headers().

extract_headers(Content) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    Headers.

extract_timestamp(Content) ->
    #content{properties = #'P_basic'{timestamp = Timestamp}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    Timestamp.

-spec map_headers
        (fun((headers()) -> headers()), rabbit_types:content()) ->
            rabbit_types:content().

map_headers(F, Content) ->
    Content1 = rabbit_binary_parser:ensure_content_decoded(Content),
    #content{properties = #'P_basic'{headers = Headers} = Props} = Content1,
    Headers1 = F(Headers),
    rabbit_binary_generator:clear_encoded_content(
      Content1#content{properties = Props#'P_basic'{headers = Headers1}}).

indexof(L, Element) -> indexof(L, Element, 1).

indexof([],               _Element, _N) -> 0;
indexof([Element | _Rest], Element,  N) -> N;
indexof([_ | Rest],        Element,  N) -> indexof(Rest, Element, N + 1).

is_message_persistent(#content{properties = #'P_basic'{
                                 delivery_mode = Mode}}) ->
    case Mode of
        1         -> false;
        2         -> true;
        undefined -> false;
        Other     -> throw({error, {delivery_mode_unknown, Other}})
    end.

%% Extract CC routes from headers

-spec header_routes(undefined | rabbit_framing:amqp_table()) -> [string()] | {error, Reason :: any()}.

header_routes(undefined) ->
    [];
header_routes(HeadersTable) ->
    try
        lists:append(
          [case rabbit_misc:table_lookup(HeadersTable, HeaderKey) of
               {array, Routes} -> [Route || {longstr, Route} <- Routes];
               undefined       -> [];
               {Type, _Val}    -> throw({error, {unacceptable_type_in_header,
                                                 binary_to_list(HeaderKey), Type}})
           end || HeaderKey <- ?ROUTING_HEADERS])
    catch
        {error, _Reason} = Error ->
            Error
    end.

-spec parse_expiration
        (rabbit_framing:amqp_property_record()) ->
            rabbit_types:ok_or_error2('undefined' | non_neg_integer(), any()).

parse_expiration(#'P_basic'{expiration = undefined}) ->
    {ok, undefined};
parse_expiration(#'P_basic'{expiration = Expiration}) ->
    parse_expiration(Expiration);
parse_expiration(Expiration) when is_binary(Expiration) ->
    case string:to_integer(binary_to_list(Expiration)) of
        {error, no_integer} = E ->
            E;
        {N, ""} ->
            case rabbit_misc:check_expiry(N) of
                ok             -> {ok, N};
                E = {error, _} -> E
            end;
        {_, S} ->
            {error, {leftover_string, S}}
    end.

maybe_gc_large_msg(Content) ->
    rabbit_writer:maybe_gc_large_msg(Content).

maybe_gc_large_msg(Content, undefined) ->
    rabbit_writer:msg_size(Content);
maybe_gc_large_msg(Content, GCThreshold) ->
    rabbit_writer:maybe_gc_large_msg(Content, GCThreshold).

msg_size(Content) ->
    rabbit_writer:msg_size(Content).

add_header(Name, Type, Value, #basic_message{content = Content0} = Msg) ->
    Content = map_headers(
                fun(undefined) ->
                        rabbit_misc:set_table_value([], Name, Type, Value);
                   (Headers) ->
                        rabbit_misc:set_table_value(Headers, Name, Type, Value)
                end, Content0),
    Msg#basic_message{content = Content}.

peek_fmt_message(Ex, RKeys, #content{payload_fragments_rev = Payl0,
                                     properties = Props}) ->
    Fields = [atom_to_binary(F, utf8) || F <- record_info(fields, 'P_basic')],
    T = lists:zip(Fields, tl(tuple_to_list(Props))),
    lists:foldl(
      fun ({<<"headers">>, Hdrs}, Acc) ->
              case Hdrs of
                  [] ->
                      Acc;
                  _ ->
                      Acc ++ [{header_key(H), V} || {H, _T, V} <- Hdrs]
              end;
          ({_, undefined}, Acc) ->
              Acc;
          (KV, Acc) ->
              [KV | Acc]
      end, [], [{<<"payload (max 64 bytes)">>,
                 %% restric payload to 64 bytes
                 binary_prefix_64(iolist_to_binary(lists:reverse(Payl0)), 64)},
                {<<"exchange">>, Ex},
                {<<"routing_keys">>, RKeys} | T]).

header_key(A) ->
    <<"header.", A/binary>>.

binary_prefix_64(Bin, Len) ->
    binary:part(Bin, 0, min(byte_size(Bin), Len)).

make_message(XName, RoutingKey, #content{properties = Props} = DecodedContent, Guid) ->
    case header_routes(Props#'P_basic'.headers) of
        {error, _} = Error ->
            Error;
        Routes ->
            try
                {ok, #basic_message{
                        exchange_name = XName,
                        content       = strip_header(DecodedContent, ?DELETED_HEADER),
                        id            = Guid,
                        is_persistent = is_message_persistent(DecodedContent),
                        routing_keys  = [RoutingKey | Routes]}}
            catch
                {error, _Reason} = Error -> Error
            end
    end.

