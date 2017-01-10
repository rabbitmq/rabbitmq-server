-module(amqp10_msg).

-export([
         from_amqp_records/1,
         delivery_tag/1,
         message_format/1,
         delivery_annotations/1,
         message_annotations/1,
         properties/1,
         header/1,
         header/2,
         body/1
        ]).

-include("rabbit_amqp1_0_framing.hrl").

-type maybe(T) :: T | undefined.

-type content_type() :: term(). % TODO: refine
-type content_encoding() :: term(). % TODO: refine

-type header_key() :: durable | priority | ttl | first_acquirer |
                      delivery_count.

-type amqp10_header() :: #{durable => boolean(), % false
                           priority => non_neg_integer(), % 4
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


-export_type([
              amqp10_header/0,
              amqp10_properties/0,
              amqp10_body/0
             ]).

% TODO: need to be able to handle multiple transfer frames
-record(amqp_msg,
        {transfer :: #'v1_0.transfer'{}, % the first transfer
         header :: maybe(#'v1_0.header'{}),
         delivery_annotations :: maybe(#'v1_0.delivery_annotations'{}),
         message_annotations :: maybe(#'v1_0.message_annotations'{}),
         properties :: maybe(#'v1_0.properties'{}),
         application_properties :: maybe(#'v1_0.application_properties'{}),
         body :: amqp10_body() | unset,
         footer :: maybe(#'v1_0.footer'{}),
         additional = [] :: [amqp_msg()]}). % additional transfers

-opaque amqp_msg() :: #amqp_msg{}.

-export_type([amqp_msg/0]).

-define(record_to_tuplelist(Rec, Ref),
        lists:zip(record_info(fields, Rec), tl(tuple_to_list(Ref)))).


%% API functions

-spec from_amqp_records([amqp10_client_types:amqp10_msg_record()]) ->
    amqp_msg().
from_amqp_records([#'v1_0.transfer'{} = Transfer | Records]) ->
    lists:foldl(fun parse_from_amqp/2, #amqp_msg{transfer = Transfer,
                                                 body = unset}, Records).

-spec delivery_tag(amqp_msg()) -> binary().
delivery_tag(#amqp_msg{transfer = #'v1_0.transfer'{delivery_tag = Tag}}) ->
    amqp10_client_types:unpack(Tag).

% First 3 octets are the format
% the last 1 octet is the version
% See 2.8.11 in the spec
-spec message_format(amqp_msg()) -> maybe({non_neg_integer(), non_neg_integer()}).
message_format(#amqp_msg{transfer =
                         #'v1_0.transfer'{message_format = undefined}}) ->
    undefined;
message_format(#amqp_msg{transfer =
                         #'v1_0.transfer'{message_format = {uint, MF}}}) ->
    <<Format:24/unsigned, Version:8/unsigned>> = <<MF:32/unsigned>>,
    {Format, Version}.


-spec header(amqp_msg()) -> amqp10_header().
header(#amqp_msg{header = undefined}) -> #{};
header(#amqp_msg{header = #'v1_0.header'{durable = Durable,
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

-spec header(header_key(), amqp_msg()) -> term().
header(durable = K, #amqp_msg{header = #'v1_0.header'{durable = D}}) ->
    header_value(K, D);
header(priority = K,
       #amqp_msg{header = #'v1_0.header'{priority = D}}) ->
    header_value(K, D);
header(ttl = K, #amqp_msg{header = #'v1_0.header'{ttl = D}}) ->
    header_value(K, D);
header(first_acquirer = K,
       #amqp_msg{header = #'v1_0.header'{first_acquirer = D}}) ->
    header_value(K, D);
header(delivery_count = K,
       #amqp_msg{header = #'v1_0.header'{delivery_count = D}}) ->
    header_value(K, D);
header(K, #amqp_msg{header = undefined}) -> header_value(K, undefined).

-spec delivery_annotations(amqp_msg()) -> #{any() => any()}.
delivery_annotations(#amqp_msg{delivery_annotations = undefined}) ->
    #{};
delivery_annotations(#amqp_msg{delivery_annotations =
                               #'v1_0.delivery_annotations'{content = DAs}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end,
                #{}, DAs).

-spec message_annotations(amqp_msg()) -> #{any() => any()}.
message_annotations(#amqp_msg{message_annotations = undefined}) ->
    #{};
message_annotations(#amqp_msg{message_annotations =
                               #'v1_0.message_annotations'{content = MAs}}) ->
    lists:foldl(fun({K, V}, Acc) -> Acc#{unpack(K) => unpack(V)} end,
                #{}, MAs).

-spec properties(amqp_msg()) -> amqp10_properties().
properties(#amqp_msg{properties = undefined}) -> #{};
properties(#amqp_msg{properties = Props}) ->
    Fields = ?record_to_tuplelist('v1_0.properties', Props),

    lists:foldl(fun ({_Key, undefined}, Acc) -> Acc;
                    ({Key, Value}, Acc) -> Acc#{Key => unpack(Value)}
                end, #{}, Fields).

-spec body(amqp_msg()) ->
    [binary()] | [#'v1_0.amqp_sequence'{}] | #'v1_0.amqp_value'{}.
body(#amqp_msg{body = [#'v1_0.data'{} | _] = Data}) ->
    [Content || #'v1_0.data'{content = Content} <- Data].



%% LOCAL

header_value(durable, undefined) -> false;
header_value(priority, undefined) -> 4;
header_value(first_acquirer, undefined) -> false;
header_value(delivery_count, undefined) -> 0;
header_value(Key, {_Type, Value}) -> header_value(Key, Value);
header_value(_Key, Value) -> Value.

parse_from_amqp(#'v1_0.header'{} = Header, AmqpMsg) ->
    AmqpMsg#amqp_msg{header = Header};
parse_from_amqp(#'v1_0.delivery_annotations'{} = DAS, AmqpMsg) ->
    AmqpMsg#amqp_msg{delivery_annotations = DAS};
parse_from_amqp(#'v1_0.message_annotations'{} = DAS, AmqpMsg) ->
    AmqpMsg#amqp_msg{message_annotations = DAS};
parse_from_amqp(#'v1_0.properties'{} = Header, AmqpMsg) ->
    AmqpMsg#amqp_msg{properties = Header};
parse_from_amqp(#'v1_0.data'{} = Data, AmqpMsg) ->
    AmqpMsg#amqp_msg{body = [Data]}.

unpack(V) -> amqp10_client_types:unpack(V).



