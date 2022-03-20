%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(amqp10_msg).

-export([from_amqp_records/1,
         to_amqp_records/1,
         % "read" api
         delivery_id/1,
         delivery_tag/1,
         handle/1,
         settled/1,
         message_format/1,
         headers/1,
         header/2,
         delivery_annotations/1,
         message_annotations/1,
         properties/1,
         application_properties/1,
         body/1,
         body_bin/1,
         footer/1,
         % "write" api
         new/2,
         new/3,
         set_handle/2,
         set_settled/2,
         set_message_format/2,
         set_headers/2,
         set_properties/2,
         set_application_properties/2,
         set_delivery_annotations/2,
         set_message_annotations/2
        ]).

-include_lib("amqp10_common/include/amqp10_framing.hrl").

-type maybe(T) :: T | undefined.

-type delivery_tag() :: binary().
-type content_type() :: term(). % TODO: refine
-type content_encoding() :: term(). % TODO: refine

% annotations keys are restricted to be of type symbol or of type ulong
-type annotations_key() :: binary() | non_neg_integer().

-type header_key() :: durable | priority | ttl | first_acquirer |
                      delivery_count.

-type amqp10_header() :: #{durable => boolean(), % false
                           priority => byte(), % 4
                           ttl => maybe(non_neg_integer()),
                           first_acquirer => boolean(), % false
                           delivery_count => non_neg_integer()}. % 0

-type amqp10_properties() :: #{message_id => maybe(any()),
                               user_id => maybe(binary()),
                               to => maybe(any()),
                               subject => maybe(binary()),
                               reply_to => maybe(any()),
                               correlation_id => maybe(any()),
                               content_type => maybe(content_type()),
                               content_encoding => maybe(content_encoding()),
                               absolute_expiry_time => maybe(non_neg_integer()),
                               creation_time => maybe(non_neg_integer()),
                               group_id => maybe(binary()),
                               group_sequence => maybe(non_neg_integer()),
                               reply_to_group_id => maybe(binary())}.

-type amqp10_body() :: [#'v1_0.data'{}] |
                       [#'v1_0.amqp_sequence'{}] |
                       #'v1_0.amqp_value'{}.



-record(amqp10_msg,
        {transfer :: #'v1_0.transfer'{},
         header :: maybe(#'v1_0.header'{}),
         delivery_annotations :: maybe(#'v1_0.delivery_annotations'{}),
         message_annotations :: maybe(#'v1_0.message_annotations'{}),
         properties :: maybe(#'v1_0.properties'{}),
         application_properties :: maybe(#'v1_0.application_properties'{}),
         body :: amqp10_body() | unset,
         footer :: maybe(#'v1_0.footer'{})
         }).

-opaque amqp10_msg() :: #amqp10_msg{}.

-export_type([amqp10_msg/0,
              amqp10_header/0,
              amqp10_properties/0,
              amqp10_body/0,
              delivery_tag/0
             ]).

-define(record_to_tuplelist(Rec, Ref),
        lists:zip(record_info(fields, Rec), tl(tuple_to_list(Ref)))).


%% API functions

-spec from_amqp_records([amqp10_client_types:amqp10_msg_record()]) ->
    amqp10_msg().
from_amqp_records([#'v1_0.transfer'{} = Transfer | Records]) ->
    lists:foldl(fun parse_from_amqp/2, #amqp10_msg{transfer = Transfer,
                                                   body = unset}, Records).

-spec to_amqp_records(amqp10_msg()) -> [amqp10_client_types:amqp10_msg_record()].
to_amqp_records(#amqp10_msg{transfer = T,
                            header = H,
                            delivery_annotations = DAs,
                            message_annotations = MAs,
                            properties = Ps,
                            application_properties = APs,
                            body = B,
                            footer = F
                            }) ->
    L = lists:flatten([T, H, DAs, MAs, Ps, APs, B, F]),
    lists:filter(fun has_value/1, L).

-spec delivery_tag(amqp10_msg()) -> delivery_tag().
delivery_tag(#amqp10_msg{transfer = #'v1_0.transfer'{delivery_tag = Tag}}) ->
    unpack(Tag).

-spec delivery_id(amqp10_msg()) -> non_neg_integer().
delivery_id(#amqp10_msg{transfer = #'v1_0.transfer'{delivery_id = Id}}) ->
    unpack(Id).

-spec handle(amqp10_msg()) -> non_neg_integer().
handle(#amqp10_msg{transfer = #'v1_0.transfer'{handle = Handle}}) ->
    unpack(Handle).

-spec settled(amqp10_msg()) -> boolean().
settled(#amqp10_msg{transfer = #'v1_0.transfer'{settled = Settled}}) ->
    Settled.

% First 3 octets are the format
% the last 1 octet is the version
% See 2.8.11 in the spec
-spec message_format(amqp10_msg()) ->
    maybe({non_neg_integer(), non_neg_integer()}).
message_format(#amqp10_msg{transfer =
                         #'v1_0.transfer'{message_format = undefined}}) ->
    undefined;
message_format(#amqp10_msg{transfer =
                         #'v1_0.transfer'{message_format = {uint, MF}}}) ->
    <<Format:24/unsigned, Version:8/unsigned>> = <<MF:32/unsigned>>,
    {Format, Version}.


-spec headers(amqp10_msg()) -> amqp10_header().
headers(#amqp10_msg{header = undefined}) -> #{};
headers(#amqp10_msg{header = #'v1_0.header'{durable = Durable,
                                            priority = Priority,
                                            ttl = Ttl,
                                            first_acquirer = FA,
                                            delivery_count = DC}}) ->
    Fields = [{durable, header_value(durable, Durable)},
              {priority, header_value(priority, Priority)},
              {ttl, header_value(ttl, Ttl)},
              {first_acquirer, header_value(first_acquirer, FA)},
              {delivery_count, header_value(delivery_count, DC)}],

    lists:foldl(fun ({_Key, undefined}, Acc) -> Acc;
                    ({Key, Value}, Acc) -> Acc#{Key => Value}
                end, #{}, Fields).

-spec header(header_key(), amqp10_msg()) -> term().
header(durable = K, #amqp10_msg{header = #'v1_0.header'{durable = D}}) ->
    header_value(K, D);
header(priority = K,
       #amqp10_msg{header = #'v1_0.header'{priority = D}}) ->
    header_value(K, D);
header(ttl = K, #amqp10_msg{header = #'v1_0.header'{ttl = D}}) ->
    header_value(K, D);
header(first_acquirer = K,
       #amqp10_msg{header = #'v1_0.header'{first_acquirer = D}}) ->
    header_value(K, D);
header(delivery_count = K,
       #amqp10_msg{header = #'v1_0.header'{delivery_count = D}}) ->
    header_value(K, D);
header(K, #amqp10_msg{header = undefined}) -> header_value(K, undefined).

-spec delivery_annotations(amqp10_msg()) -> #{annotations_key() => any()}.
delivery_annotations(#amqp10_msg{delivery_annotations = undefined}) ->
    #{};
delivery_annotations(#amqp10_msg{delivery_annotations =
                               #'v1_0.delivery_annotations'{content = DAs}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end,
                #{}, DAs).

-spec message_annotations(amqp10_msg()) -> #{annotations_key() => any()}.
message_annotations(#amqp10_msg{message_annotations = undefined}) ->
    #{};
message_annotations(#amqp10_msg{message_annotations =
                               #'v1_0.message_annotations'{content = MAs}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end,
                #{}, MAs).

-spec properties(amqp10_msg()) -> amqp10_properties().
properties(#amqp10_msg{properties = undefined}) -> #{};
properties(#amqp10_msg{properties = Props}) ->
    Fields = ?record_to_tuplelist('v1_0.properties', Props),
    lists:foldl(fun ({_Key, undefined}, Acc) -> Acc;
                    ({Key, Value}, Acc) -> Acc#{Key => unpack(Value)}
                end, #{}, Fields).

% application property values can be simple types - no maps or lists
-spec application_properties(amqp10_msg()) ->
    #{binary() => binary() | integer() | string()}.
application_properties(#amqp10_msg{application_properties = undefined}) ->
    #{};
application_properties(
  #amqp10_msg{application_properties =
            #'v1_0.application_properties'{content = MAs}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end,
                #{}, MAs).

-spec footer(amqp10_msg()) -> #{annotations_key() => any()}.
footer(#amqp10_msg{footer = undefined}) -> #{};
footer(#amqp10_msg{footer = #'v1_0.footer'{content = Footer}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end, #{},
                Footer).

-spec body(amqp10_msg()) ->
    [binary()] | [#'v1_0.amqp_sequence'{}] | #'v1_0.amqp_value'{}.
body(#amqp10_msg{body = [#'v1_0.data'{} | _] = Data}) ->
    [Content || #'v1_0.data'{content = Content} <- Data];
body(#amqp10_msg{body = Body}) -> Body.

%% @doc Returns the binary representation
-spec body_bin(amqp10_msg()) -> binary().
body_bin(#amqp10_msg{body = [#'v1_0.data'{content = Bin}]})
  when is_binary(Bin) ->
    Bin;
body_bin(#amqp10_msg{body = Data}) when is_list(Data) ->
    iolist_to_binary([amqp10_framing:encode_bin(D) || D <- Data]);
body_bin(#amqp10_msg{body = #'v1_0.amqp_value'{} = Body}) ->
    %% TODO: to avoid unnecessary decoding and re-encoding we could amend
    %% the parse to provide the body in a lazy fashion, only decoding when
    %% reading. For now we just re-encode it.
    iolist_to_binary(amqp10_framing:encode_bin(Body)).

%% @doc Create a new amqp10 message using the specified delivery tag, body
%% and settlement state. Settled=true means the message is considered settled
%% as soon as sent and no disposition will be issued by the receiver.
%% Settled=false will delay settlement until a disposition has been received.
%% A disposition will be notified to the sender by a message of the
%% following stucture:
%% {amqp10_disposition, {accepted | rejected, DeliveryTag}}
-spec new(delivery_tag(), amqp10_body() | binary(), boolean()) -> amqp10_msg().
new(DeliveryTag, Body, Settled) when is_binary(Body) ->
    #amqp10_msg{transfer = #'v1_0.transfer'{delivery_tag = {binary, DeliveryTag},
                                            settled = Settled,
                                            message_format = {uint, 0}},
                body = [#'v1_0.data'{content = Body}]};
new(DeliveryTag, Body, Settled) -> % TODO: constrain to amqp types
    #amqp10_msg{transfer = #'v1_0.transfer'{delivery_tag = {binary, DeliveryTag},
                                            settled = Settled,
                                            message_format = {uint, 0}},
                body = Body}.

%% @doc Create a new settled amqp10 message using the specified delivery tag
%% and body.
-spec new(delivery_tag(), amqp10_body() | binary()) -> amqp10_msg().
new(DeliveryTag, Body) ->
    new(DeliveryTag, Body, false).


% First 3 octets are the format
% the last 1 octet is the version
% See 2.8.11 in the spec
%% @doc Set the message format.
-spec set_message_format({non_neg_integer(), non_neg_integer()},
                         amqp10_msg()) -> amqp10_msg().
set_message_format({Format, Version}, #amqp10_msg{transfer = T} = Msg) ->
    <<MsgFormat:32/unsigned>> = <<Format:24/unsigned, Version:8/unsigned>>,
    Msg#amqp10_msg{transfer = T#'v1_0.transfer'{message_format =
                                                {uint, MsgFormat}}}.

%% @doc Set the link handle used for the message transfer.
-spec set_handle(non_neg_integer(), amqp10_msg()) -> amqp10_msg().
set_handle(Handle, #amqp10_msg{transfer = T} = Msg) ->
    Msg#amqp10_msg{transfer = T#'v1_0.transfer'{handle = {uint, Handle}}}.

%% @doc Set the settledment mode.
%% Settled=true means the message is considered settled
%% as soon as sent and no disposition will be issued by the receiver.
%% Settled=false will delay settlement until a disposition has been received.
%% A disposition will be notified to the sender by a message of the
%% following stucture:
%% {amqp10_disposition, {accepted | rejected, DeliveryTag}}
-spec set_settled(boolean(), amqp10_msg()) -> amqp10_msg().
set_settled(Settled, #amqp10_msg{transfer = T} = Msg) ->
    Msg#amqp10_msg{transfer = T#'v1_0.transfer'{settled = Settled}}.

%% @doc Set amqp message headers.
-spec set_headers(#{atom() => any()}, amqp10_msg()) -> amqp10_msg().
set_headers(Headers, #amqp10_msg{header = undefined} = Msg) ->
    set_headers(Headers, Msg#amqp10_msg{header = #'v1_0.header'{}});
set_headers(Headers, #amqp10_msg{header = Current} = Msg) ->
    H = maps:fold(fun(durable, V, Acc) ->
                          Acc#'v1_0.header'{durable = V};
                     (priority, V, Acc) ->
                          Acc#'v1_0.header'{priority = {uint, V}};
                     (first_acquirer, V, Acc) ->
                          Acc#'v1_0.header'{first_acquirer = V};
                     (ttl, V, Acc) ->
                          Acc#'v1_0.header'{ttl = {uint, V}};
                     (delivery_count, V, Acc) ->
                          Acc#'v1_0.header'{delivery_count = {uint, V}}
                  end, Current, Headers),
    Msg#amqp10_msg{header = H}.

%% @doc Set amqp message properties.
-spec set_properties(amqp10_properties(), amqp10_msg()) -> amqp10_msg().
set_properties(Props, #amqp10_msg{properties = undefined} = Msg) ->
    set_properties(Props, Msg#amqp10_msg{properties = #'v1_0.properties'{}});
set_properties(Props, #amqp10_msg{properties = Current} = Msg) ->
    % TODO many fields are `any` types and we need to try to type tag them
    P = maps:fold(fun(message_id, V, Acc) when is_binary(V) ->
                          % message_id can be any type but we restrict it here
                          Acc#'v1_0.properties'{message_id = utf8(V)};
                     (user_id, V, Acc) ->
                          Acc#'v1_0.properties'{user_id = utf8(V)};
                     (to, V, Acc) ->
                          Acc#'v1_0.properties'{to = utf8(V)};
                     (subject, V, Acc) ->
                          Acc#'v1_0.properties'{subject = utf8(V)};
                     (reply_to, V, Acc) ->
                          Acc#'v1_0.properties'{reply_to = utf8(V)};
                     (correlation_id, V, Acc) ->
                          Acc#'v1_0.properties'{correlation_id = utf8(V)};
                     (content_type, V, Acc) ->
                          Acc#'v1_0.properties'{content_type = sym(V)};
                     (content_encoding, V, Acc) ->
                          Acc#'v1_0.properties'{content_encoding = sym(V)};
                     (absolute_expiry_time, V, Acc) ->
                          Acc#'v1_0.properties'{absolute_expiry_time = {timestamp, V}};
                     (creation_time, V, Acc) ->
                          Acc#'v1_0.properties'{creation_time = {timestamp, V}};
                     (group_id, V, Acc) ->
                          Acc#'v1_0.properties'{group_id = utf8(V)};
                     (group_sequence, V, Acc) ->
                          Acc#'v1_0.properties'{group_sequence = uint(V)};
                     (reply_to_group_id, V, Acc) ->
                          Acc#'v1_0.properties'{reply_to_group_id = utf8(V)}
                  end, Current, Props),
    Msg#amqp10_msg{properties = P}.

-spec set_application_properties(#{binary() | string() => binary() | integer() | string()},
                                 amqp10_msg()) -> amqp10_msg().
set_application_properties(Props,
                           #amqp10_msg{application_properties = undefined} =
                           Msg) ->
    APs = #'v1_0.application_properties'{content = []},
    set_application_properties(Props,
                               Msg#amqp10_msg{application_properties = APs});
set_application_properties(
  Props0, #amqp10_msg{application_properties =
                      #'v1_0.application_properties'{content = APs0}} = Msg) ->
    Props = maps:fold(fun (K, V, S) ->
                              S#{utf8(K) => wrap_ap_value(V)}
                      end, maps:from_list(APs0), Props0),
    APs = #'v1_0.application_properties'{content = maps:to_list(Props)},
    Msg#amqp10_msg{application_properties = APs}.

-spec set_delivery_annotations(#{binary() => binary() | integer() | string()},
                                 amqp10_msg()) -> amqp10_msg().
set_delivery_annotations(Props,
                         #amqp10_msg{delivery_annotations = undefined} =
                         Msg) ->
    Anns = #'v1_0.delivery_annotations'{content = []},
    set_delivery_annotations(Props,
                             Msg#amqp10_msg{delivery_annotations = Anns});
set_delivery_annotations(
  Props0, #amqp10_msg{delivery_annotations =
                      #'v1_0.delivery_annotations'{content = Anns0}} = Msg) ->
    Anns = maps:fold(fun (K, V, S) ->
                             S#{sym(K) => wrap_ap_value(V)}
                     end, maps:from_list(Anns0), Props0),
    Anns1 = #'v1_0.delivery_annotations'{content = maps:to_list(Anns)},
    Msg#amqp10_msg{delivery_annotations = Anns1}.

-spec set_message_annotations(#{binary() => binary() | integer() | string()},
                                 amqp10_msg()) -> amqp10_msg().
set_message_annotations(Props,
                         #amqp10_msg{message_annotations = undefined} =
                         Msg) ->
    Anns = #'v1_0.message_annotations'{content = []},
    set_message_annotations(Props,
                             Msg#amqp10_msg{message_annotations = Anns});
set_message_annotations(
  Props0, #amqp10_msg{message_annotations =
                      #'v1_0.message_annotations'{content = Anns0}} = Msg) ->
    Anns = maps:fold(fun (K, V, S) ->
                             S#{sym(K) => wrap_ap_value(V)}
                     end, maps:from_list(Anns0), Props0),
    Anns1 = #'v1_0.message_annotations'{content = maps:to_list(Anns)},
    Msg#amqp10_msg{message_annotations = Anns1}.

wrap_ap_value(true) ->
    {boolean, true};
wrap_ap_value(false) ->
    {boolean, false};
wrap_ap_value(V) when is_integer(V) ->
    {uint, V};
wrap_ap_value(V) when is_binary(V) ->
    utf8(V);
wrap_ap_value(V) when is_list(V) ->
    utf8(list_to_binary(V));
wrap_ap_value(V) when is_atom(V) ->
    utf8(atom_to_list(V)).


%% LOCAL
header_value(durable, undefined) -> false;
header_value(priority, undefined) -> 4;
header_value(first_acquirer, undefined) -> false;
header_value(delivery_count, undefined) -> 0;
header_value(Key, {_Type, Value}) -> header_value(Key, Value);
header_value(_Key, Value) -> Value.

parse_from_amqp(#'v1_0.header'{} = Header, AmqpMsg) ->
    AmqpMsg#amqp10_msg{header = Header};
parse_from_amqp(#'v1_0.delivery_annotations'{} = DAS, AmqpMsg) ->
    AmqpMsg#amqp10_msg{delivery_annotations = DAS};
parse_from_amqp(#'v1_0.message_annotations'{} = DAS, AmqpMsg) ->
    AmqpMsg#amqp10_msg{message_annotations = DAS};
parse_from_amqp(#'v1_0.properties'{} = Header, AmqpMsg) ->
    AmqpMsg#amqp10_msg{properties = Header};
parse_from_amqp(#'v1_0.application_properties'{} = APs, AmqpMsg) ->
    AmqpMsg#amqp10_msg{application_properties = APs};
parse_from_amqp(#'v1_0.amqp_value'{} = Value, AmqpMsg) ->
    AmqpMsg#amqp10_msg{body = Value};
parse_from_amqp(#'v1_0.amqp_sequence'{} = Seq, AmqpMsg) ->
    AmqpMsg#amqp10_msg{body = [Seq]};
parse_from_amqp(#'v1_0.data'{} = Data, AmqpMsg) ->
    AmqpMsg#amqp10_msg{body = [Data]};
parse_from_amqp(#'v1_0.footer'{} = Header, AmqpMsg) ->
    AmqpMsg#amqp10_msg{footer = Header}.

unpack(V) -> amqp10_client_types:unpack(V).
utf8(V) -> amqp10_client_types:utf8(V).
sym(B) when is_list(B) -> {symbol, list_to_binary(B)};
sym(B) when is_binary(B) -> {symbol, B}.
uint(B) -> {uint, B}.

has_value(undefined) -> false;
has_value(_) -> true.
