-module(mc_amqpl).
-behaviour(mc).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("mc.hrl").

-export([
         init/1,
         init_amqp/1,
         size/1,
         x_header/2,
         routing_headers/2,
         % get_property/2,
         % set_property/3,
         convert/2,
         protocol_state/2,
         message/3,
         message/4,
         message/5,
         from_basic_message/1,
         set_property/3,
         serialize/2
        ]).

-import(rabbit_misc, [maps_put_truthy/3]).

-define(HEADER_GUESS_SIZE, 100). %% see determine_persist_to/2
-define(AMQP10_TYPE, <<"amqp-1.0">>).
-define(AMQP10_PROPERTIES_HEADER, <<"x-amqp-1.0-properties">>).
-define(AMQP10_APP_PROPERTIES_HEADER, <<"x-amqp-1.0-app-properties">>).
-define(AMQP10_MESSAGE_ANNOTATIONS_HEADER, <<"x-amqp-1.0-message-annotations">>).

-opaque state() :: #content{}.

-export_type([
              state/0
             ]).

%% mc implementation
init(#content{} = Content) ->
    %% TODO: header routes
    %% project essential properties into annotations
    Anns = essential_properties(Content),
    {strip_header(Content, ?DELETED_HEADER), Anns}.

init_amqp(Sections) when is_list(Sections) ->
    {H, MAnn, P, AProp, Body} =
        lists:foldl(
          fun
              (#'v1_0.header'{} = S, Acc) ->
                  setelement(1, Acc, S);
              (#'v1_0.message_annotations'{} = S, Acc) ->
                  setelement(2, Acc, S);
              (#'v1_0.properties'{} = S, Acc) ->
                  setelement(3, Acc, S);
              (#'v1_0.application_properties'{} = S, Acc) ->
                  setelement(4, Acc, S);
              (#'v1_0.footer'{}, Acc) ->
                  %% footer not currently used
                  Acc;
              (undefined, Acc) ->
                  Acc;
              (BodySection, Acc) ->
                  Body = element(5, Acc),
                  setelement(5, Acc, [BodySection | Body])
          end, {undefined, undefined, undefined, undefined, []},
          Sections),

    {Payload, Type0} = case Body of
                           [#'v1_0.data'{content = Bin}] when is_binary(Bin) ->
                               {[Bin], undefined};
                           [#'v1_0.data'{content = Bin}] when is_list(Bin) ->
                               {Bin, undefined};
                           _ ->
                               %% anything else needs to be encoded
                               {[amqp10_framing:encode_bin(X) || X <- Body],
                                ?AMQP10_TYPE}
                       end,
    #'v1_0.properties'{message_id = MsgId,
                       user_id = UserId0,
                       reply_to = ReplyTo0,
                       correlation_id = CorrId,
                       content_type = ContentType,
                       content_encoding = ContentEncoding,
                       creation_time = Timestamp,
                       group_id = GroupId} = case P of
                                                 undefined ->
                                                     #'v1_0.properties'{};
                                                 _ ->
                                                     P
                                             end,

    AP = case AProp of
             #'v1_0.application_properties'{content = AC} -> AC;
             _ -> []
         end,
    MA = case MAnn of
             #'v1_0.message_annotations'{content = MC} -> MC;
             _ -> []
         end,

    DelMode = case H of
                  #'v1_0.header'{durable = true} -> 2;
                  #'v1_0.header'{durable = false} -> 1;
                  _ -> amqp10_map_get(symbol(<<"x-basic-delivery-mode">>), MA)
              end,
    %% TODO: check amqp header first for priority, ttl
    Priority = amqp10_map_get(symbol(<<"x-basic-priority">>), MA),
    Expiration = amqp10_map_get(symbol(<<"x-basic-expiration">>), MA),
    Type = case Type0 of
               undefined ->
                   amqp10_map_get(symbol(<<"x-basic-type">>), MA);
               _ ->
                   Type0
           end,

    Headers0 = [to_091(unwrap(K), V) || {K, V} <- AP],
    %% Add remaining message annotations as headers?
    XHeaders = [to_091(K, V) || {{_, K}, V} <- MA,
                                not is_x_basic_header(K)],
    {Headers1, MsgId091} = message_id(MsgId, <<"x-message-id">>, Headers0),
    {Headers, CorrId091} = message_id(CorrId, <<"x-correlation-id">>, Headers1),

    UserId1 = unwrap(UserId0),
    UserId = case mc_util:is_valid_shortstr(UserId1) of
                 true ->
                     UserId1;
                 false ->
                     %% drop it
                     undefined
             end,

    BP = #'P_basic'{message_id =  MsgId091,
                    delivery_mode = DelMode,
                    expiration = Expiration,
                    user_id = UserId,
                    headers = case XHeaders ++ Headers of
                                  [] -> undefined;
                                  AllHeaders -> AllHeaders 
                              end,
                    reply_to = unwrap(ReplyTo0),
                    type = Type,
                    app_id = unwrap(GroupId),
                    priority = Priority,
                    correlation_id = CorrId091,
                    content_type = unwrap(ContentType),
                    content_encoding = unwrap(ContentEncoding),
                    timestamp = unwrap(Timestamp)
                   },

    #content{class_id = 60,
             properties = BP,
             properties_bin = none,
             payload_fragments_rev = Payload}.


size(#content{properties_bin = PropsBin,
              properties = Props,
              payload_fragments_rev = Payload}) ->
    MetaSize = case is_binary(PropsBin) of
                   true ->
                       byte_size(PropsBin);
                   false ->
                       #'P_basic'{headers = Hs} = Props,
                       case Hs of
                           undefined -> 0;
                           _ -> length(Hs)
                       end * ?HEADER_GUESS_SIZE
               end,
    {MetaSize, iolist_size(Payload)}.

x_header(_Key, #content{properties = #'P_basic'{headers = undefined}} = C) ->
    {undefined, C};
x_header(Key, #content{properties = #'P_basic'{headers = Headers}} = C) ->
    case rabbit_misc:table_lookup(Headers, Key) of
        undefined ->
            {undefined, C};
        {_Type, Value} ->
            {Value, C}
    end.

routing_headers(#content{properties = #'P_basic'{headers = undefined}}, _Opts) ->
    #{};
routing_headers(#content{properties = #'P_basic'{headers = Headers}}, Opts) ->
    IncludeX = lists:member(x_headers, Opts),
    %% TODO: filter out complex AMQP legacy values such as array and table?
    lists:foldl(
      fun({<<"x-", _/binary>> = Key, _T, Value}, Acc) ->
              case IncludeX of
                  true ->
                      Acc#{Key => Value};
                  false ->
                      Acc
              end;
         ({Key, _T,  Value}, Acc) ->
              Acc#{Key => Value}
      end, #{}, Headers).



% get_property(durable,
%              #content{properties = #'P_basic'{delivery_mode = Mode}} = C) ->
%     {Mode == 2, C};
% get_property(ttl, #content{properties = Props} = C) ->
%     {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
%     {MsgTTL, C};
% get_property(priority, #content{properties = #'P_basic'{priority = P}} = C) ->
%     {P, C};
% get_property(timestamp, #content{properties = Props} = C) ->
%     #'P_basic'{timestamp = Timestamp} = Props,
%     case Timestamp of
%         undefined ->
%             {undefined, C};
%         _ ->
%             %% timestamp should be in ms
%             {Timestamp * 1000, C}
%     end;
% get_property(_P, C) ->
%     {undefined, C}.

set_property(ttl, undefined, #content{properties = Props} = C) ->
    %% TODO: impl rest, just what is needed for dead lettering for now
    C#content{properties = Props#'P_basic'{expiration = undefined}};
set_property(_P, _V, Msg) ->
    %% TODO: impl at least ttl set (needed for dead lettering)
    Msg.

serialize(_, _) ->
    <<>>.

convert(?MODULE, C) ->
    C;
convert(mc_amqp, #content{properties = Props,
                          payload_fragments_rev = Payload}) ->
    #'P_basic'{message_id = MsgId,
               expiration = Expiration,
               delivery_mode = DelMode,
               headers = Headers0,
               user_id = UserId,
               reply_to = ReplyTo,
               type = Type,
               priority = Priority,
               app_id = AppId,
               correlation_id = CorrId,
               content_type = ContentType,
               content_encoding = ContentEncoding,
               timestamp = Timestamp} = Props,
    ConvertedTs = case Timestamp of
                      undefined ->
                          undefined;
                      _ ->
                          Timestamp * 1000
                  end,

    Headers = case Headers0 of
                  undefined -> [];
                  _ -> Headers0
              end,
    P = case amqp10_section_header(?AMQP10_PROPERTIES_HEADER, Headers) of
            undefined ->
                #'v1_0.properties'{message_id = wrap(utf8, MsgId),
                                   user_id = wrap(binary, UserId),
                                   to = undefined,
                                   % subject = wrap(utf8, RKey),
                                   reply_to = wrap(utf8, ReplyTo),
                                   correlation_id = wrap(utf8, CorrId),
                                   content_type = wrap(symbol, ContentType),
                                   content_encoding = wrap(symbol, ContentEncoding),
                                   creation_time = wrap(timestamp, ConvertedTs),
                                   %% this is semantically not the best idea but you
                                   %% could imagine these having similar behaviour
                                   group_id = wrap(utf8, AppId)
                                  };
            V10Prop ->
                V10Prop
        end,

    AP = case amqp10_section_header(?AMQP10_APP_PROPERTIES_HEADER, Headers) of
             undefined ->
                 %% non x- headers are stored as application properties when the type allows
                 APC = [{wrap(utf8, K), from_091(T, V)}
                        || {K, T, V} <- Headers,
                           supported_header_value_type(T),
                           not is_x_header(K)],
                 #'v1_0.application_properties'{content = APC};
             A ->
                 A
         end,

    %% x- headers are stored as message annotations
    MA = case amqp10_section_header(?AMQP10_MESSAGE_ANNOTATIONS_HEADER, Headers) of
             undefined ->
                 MAC0 = [{{symbol, K}, from_091(T, V)}
                         || {K, T, V} <- Headers,
                            is_x_header(K),
                            %% all message annotation keys need to be either a symbol or ulong
                            %% but 0.9.1 field-table names are always strings
                            is_binary(K)
                        ],

                 %% properties that _are_ potentially used by the broker
                 %% are stored as message annotations
                 %% an alternative woud be to store priority and delivery mode in
                 %% the amqp (1.0) header section using the dura
                 MAC = map_add(symbol, <<"x-basic-type">>, utf8, Type,
                               map_add(symbol, <<"x-basic-priority">>, ubyte, Priority,
                                       map_add(symbol, <<"x-basic-delivery-mode">>, ubyte, DelMode,
                                               map_add(symbol, <<"x-basic-expiration">>, utf8, Expiration,
                                                       MAC0)))),
                 #'v1_0.message_annotations'{content = MAC};
             Section ->
                 Section
         end,

    mc_amqp:init_amqp([P, AP, MA,
                       #'v1_0.data'{content = lists:reverse(Payload)}]);
convert(_, _C) ->
    not_implemented.

protocol_state(#content{properties = #'P_basic'{headers = H00} = B0} = C,
               Anns) ->
    %% Add any x- annotations as headers
    %% TODO: conversion is very primitive for now
    H0 = case H00 of
             undefined -> [];
             _ ->
                 H00
         end,
    Deaths = maps:get(deaths, Anns, undefined),
    Headers0 = deaths_to_headers(Deaths, H0),
    Headers1 = maps:fold(
                 fun (<<"x-", _/binary>> = Key, Val, H) when is_integer(Val) ->
                         [{Key, long, Val} | H];
                     (<<"x-", _/binary>> = Key, Val, H) when is_binary(Val) ->
                         [{Key, longstr, Val} | H];
                     (<<"timestamp_in_ms">> = Key, Val, H) when is_integer(Val) ->
                         [{Key, long, Val} | H];
                     (_, _, Acc) ->
                         Acc
                 end, Headers0, Anns),
    Headers = case Headers1 of
                  [] ->
                      undefined;
                  _ ->
                      %% Dedup
                      lists:usort(fun({Key1, _, _}, {Key2, _, _}) ->
                                          Key1 =< Key2
                                  end, Headers1)
              end,
    Timestamp = case Anns of
                    #{timestamp := Ts} ->
                        Ts div 1000;
                    _ ->
                        undefined
                end,
    Expiration = case Anns of
                     #{ttl := undefined} ->
                         undefined;
                     #{ttl := Ttl} ->
                         %% not sure this will ever happen
                         %% as we only ever unset the expiry
                         integer_to_binary(Ttl);
                     _ ->
                         B0#'P_basic'.expiration
                 end,

    B = B0#'P_basic'{timestamp = Timestamp,
                     expiration = Expiration,
                     headers = Headers},

    C#content{properties = B,
              properties_bin = none}.

-spec message(rabbit_types:exchange_name(), rabbit_types:routing_key(), #content{}) -> mc:state().
message(ExchangeName, RoutingKey, Content) ->
    message(ExchangeName, RoutingKey, Content, #{}).

-spec message(rabbit_types:exchange_name(), rabbit_types:routing_key(), #content{}, map()) ->
    mc:state() | rabbit_types:message().
message(XName, RoutingKey, Content, Anns) ->
    message(XName, RoutingKey, Content, Anns,
            rabbit_feature_flags:is_enabled(message_containers)).

%% helper for creating message container from messages received from
%% AMQP legacy
message(#resource{name = ExchangeNameBin}, RoutingKey,
        #content{properties = Props} = Content, Anns, true)
  when is_binary(RoutingKey) andalso
       is_map(Anns) ->
            HeaderRoutes = rabbit_basic:header_routes(Props#'P_basic'.headers),
            mc:init(?MODULE,
                    rabbit_basic:strip_bcc_header(Content),
                    Anns#{routing_keys => [RoutingKey | HeaderRoutes],
                          exchange => ExchangeNameBin});
message(#resource{} = XName, RoutingKey,
        #content{} = Content, Anns, false) ->
    {ok, Msg} = rabbit_basic:message(XName, RoutingKey, Content),
    case Anns of
        #{id := Id} ->
            Msg#basic_message{id = Id};
        _ ->
            Msg
    end.

from_basic_message(#basic_message{content = Content,
                                  id = Id,
                                  exchange_name = Ex,
                                  routing_keys = [RKey | _]}) ->
    Anns = case Id of
               undefined ->
                   #{};
               _ ->
                   #{id => Id}
           end,
    message(Ex, RKey, Content, Anns, true).

%% Internal

deaths_to_headers(undefined, Headers) ->
    Headers;
deaths_to_headers(#deaths{first = {FirstQueue, FirstReason} = FirstKey,
                          records = Records},
                  Headers0) ->
    #death{exchange = FirstEx} = maps:get(FirstKey, Records),
    Infos = maps:fold(
              fun ({QName, Reason}, #death{timestamp = Ts,
                                           exchange = Ex,
                                           count = Count,
                                           ttl = Ttl,
                                           routing_keys = RoutingKeys},
                   Acc) ->
                      %% The first routing key is the one specified in the
                      %% basic.publish; all others are CC or BCC keys.
                      RKs  = [hd(RoutingKeys) | rabbit_basic:header_routes(Headers0)],
                      RKeys = [{longstr, Key} || Key <- RKs],
                      ReasonBin = atom_to_binary(Reason, utf8),
                      PerMsgTTL = case Ttl of
                                      undefined -> [];
                                      _ when is_integer(Ttl) ->
                                          Expiration = integer_to_binary(Ttl),
                                          [{<<"original-expiration">>, longstr,
                                            Expiration}]
                                  end,
                      [{table, [{<<"count">>, long, Count},
                                {<<"reason">>, longstr, ReasonBin},
                                {<<"queue">>, longstr, QName},
                                {<<"time">>, timestamp, Ts div 1000},
                                {<<"exchange">>, longstr, Ex},
                                {<<"routing-keys">>, array, RKeys}] ++ PerMsgTTL}
                       | Acc]
              end, [], Records),

    Headers = rabbit_misc:set_table_value(
                Headers0, <<"x-death">>, array, Infos),
    % Headers = rabbit_basic:prepend_table_header(
    %             <<"x-death">>, Infos, Headers0),
    [{<<"x-first-death-reason">>, longstr, atom_to_binary(FirstReason, utf8)},
     {<<"x-first-death-queue">>, longstr, FirstQueue},
     {<<"x-first-death-exchange">>, longstr, FirstEx}
     | Headers].



strip_header(#content{properties = #'P_basic'{headers = undefined}}
             = DecodedContent, _Key) ->
    DecodedContent;
strip_header(#content{properties = Props0 = #'P_basic'{headers = Headers}}
             = Content, Key) ->
    case lists:keysearch(Key, 1, Headers) of
        false ->
            Content;
        {value, Found} ->
            Props = Props0#'P_basic'{headers = lists:delete(Found, Headers)},
            rabbit_binary_generator:clear_encoded_content(
              Content#content{properties = Props})
    end.

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

% unwrap(undefined) ->
%     undefined;
% unwrap({_Type, V}) ->
%     V.

%% TODO: add `array` and `table`
from_091(longstr, V) when is_binary(V) ->
    %% this is a gamble, not all longstr are text but many are
    {utf8, V};
from_091(long, V) -> {long, V};
from_091(unsignedbyte, V) -> {ubyte, V};
from_091(short, V) -> {short, V};
from_091(unsignedshort, V) -> {ushort, V};
from_091(unsignedint, V) -> {uint, V};
from_091(signedint, V) -> {int, V};
from_091(double, V) -> {double, V};
from_091(float, V) -> {float, V};
from_091(bool, V) -> {boolean, V};
from_091(binary, V) -> {binary, V};
from_091(timestamp, V) -> {timestamp, V * 1000};
from_091(byte, V) -> {byte, V};
from_091(void, _V) -> null;
from_091(array, L) ->
    {list, [from_091(T, V) || {T, V} <- L]};
from_091(table, L) ->
    {map, [{wrap(symbol, K), from_091(T, V)} || {K, T, V} <- L]}.

map_add(_T, _Key, _Type, undefined, Acc) ->
    Acc;
map_add(KeyType, Key, Type, Value, Acc) ->
    [{wrap(KeyType, Key), wrap(Type, Value)} | Acc].

supported_header_value_type(array) ->
    false;
supported_header_value_type(table) ->
    false;
supported_header_value_type(_) ->
    true.


amqp10_map_get(_K, []) ->
    undefined;
amqp10_map_get(K, Tuples) ->
    case lists:keyfind(K, 1, Tuples) of
        false ->
            undefined;
        {_, V}  ->
            unwrap(V)
    end.

symbol(T) -> {symbol, T}.

unwrap(undefined) ->
    undefined;
unwrap({timestamp, V}) ->
    V div 1000;
unwrap({_Type, V}) ->
    V.

to_091(Key, {utf8, V}) when is_binary(V) -> {Key, longstr, V};
to_091(Key, {long, V}) -> {Key, long, V};
to_091(Key, {byte, V}) -> {Key, byte, V};
to_091(Key, {ubyte, V}) -> {Key, unsignedbyte, V};
to_091(Key, {short, V}) -> {Key, short, V};
to_091(Key, {ushort, V}) -> {Key, unsignedshort, V};
to_091(Key, {uint, V}) -> {Key, unsignedint, V};
to_091(Key, {int, V}) -> {Key, signedint, V};
to_091(Key, {double, V}) -> {Key, double, V};
to_091(Key, {float, V}) -> {Key, float, V};
%% NB: header values can never be shortstr!
to_091(Key, {timestamp, V}) -> {Key, timestamp, V div 1000};
to_091(Key, {binary, V}) -> {Key, binary, V};
to_091(Key, {boolean, V}) -> {Key, bool, V};
to_091(Key, true) -> {Key, bool, true};
to_091(Key, false) -> {Key, bool, false};
%% TODO
to_091(Key, undefined) -> {Key, void, undefined};
to_091(Key, null) -> {Key, void, undefined}.

message_id({uuid, UUID}, _HKey, H0) ->
    {H0, mc_util:uuid_to_string(UUID)};
message_id({ulong, N}, _HKey, H0) ->
    {H0, erlang:integer_to_binary(N)};
message_id({binary, B}, HKey, H0) ->
    {[{HKey, longstr, B} | H0], undefined};
message_id({utf8, S}, HKey, H0) ->
    case byte_size(S) > 255 of
        true ->
            {[{HKey, longstr, S} | H0], undefined};
        false ->
            {H0, S}
    end;
message_id(undefined, _HKey, H) ->
    {H, undefined}.

essential_properties(#content{} = C) ->
    %% TODO: ensure content decoded
    #'P_basic'{delivery_mode = Mode,
               priority = Priority,
               correlation_id = CorrId,
               message_id = MsgId,
               timestamp = TimestampRaw} = Props = C#content.properties,
    {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
    Timestamp = case TimestampRaw of
                    undefined ->
                        undefined;
                    _ ->
                        %% timestamp should be in ms
                        TimestampRaw * 1000
                end,
    Durable = Mode == 2,
    maps_put_truthy(
      priority, Priority,
      maps_put_truthy(
        ttl, MsgTTL,
        maps_put_truthy(
          timestamp, Timestamp,
          maps_put_truthy(
            durable, Durable,
            maps_put_truthy(
              correlation_id, CorrId,
              maps_put_truthy(
                message_id, MsgId,
                #{})))))).

is_x_header(<<"x-", _/binary>>) ->
    true;
is_x_header(_) ->
    false.

%% headers that are added as annotations during conversions
is_x_basic_header(<<"x-basic-", _/binary>>) ->
    true;
is_x_basic_header(<<"x-routing-key">>) ->
    true;
is_x_basic_header(<<"x-exchange">>) ->
    true;
is_x_basic_header(_) ->
    false.

amqp10_section_header(Header, Headers) ->
    case lists:keyfind(Header, 1, Headers) of
        {value, {_, _, Data}} when is_binary(Data) ->
            [Section] = amqp10_framing:decode_bin(Data),
            Section ;
        _ ->
            undefined
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


