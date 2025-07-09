-module(mc_amqpl).
-behaviour(mc).

-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("mc.hrl").

%% mc
-export([
         init/1,
         size/1,
         x_header/2,
         x_headers/1,
         routing_headers/2,
         convert_to/3,
         convert_from/3,
         protocol_state/2,
         property/2,
         set_property/3,
         prepare/2
        ]).

%% utility functions
-export([
         message/3,
         message/4,
         from_basic_message/1,
         to_091/2,
         from_091/2
        ]).

-import(rabbit_misc,
        [maps_put_truthy/3,
         maps_put_falsy/3
        ]).

-define(HEADER_GUESS_SIZE, 100). %% see determine_persist_to/2
-define(AMQP10_TYPE, <<"amqp-1.0">>).
-define(AMQP10_PROPERTIES_HEADER, <<"x-amqp-1.0-properties">>).
-define(AMQP10_APP_PROPERTIES_HEADER, <<"x-amqp-1.0-app-properties">>).
-define(AMQP10_MESSAGE_ANNOTATIONS_HEADER, <<"x-amqp-1.0-message-annotations">>).
-define(AMQP10_FOOTER, <<"x-amqp-1.0-footer">>).
-define(PROTOMOD, rabbit_framing_amqp_0_9_1).
-define(CLASS_ID, 60).

-opaque state() :: #content{}.

-export_type([
              state/0
             ]).

%% mc implementation
init(#content{} = Content0) ->
    Content1 = rabbit_binary_parser:ensure_content_decoded(Content0),
    %% project essential properties into annotations
    Anns = essential_properties(Content1),
    Content = strip_header(Content1, ?DELETED_HEADER),
    {Content, Anns}.

convert_from(mc_amqp, Sections, Env) ->
    {H, MAnn, Prop, AProp, BodyRev, Footer} =
    lists:foldl(
      fun(#'v1_0.header'{} = S, Acc) ->
              setelement(1, Acc, S);
         (_Ignore = #'v1_0.delivery_annotations'{}, Acc) ->
              Acc;
         (#'v1_0.message_annotations'{} = S, Acc) ->
              setelement(2, Acc, S);
         (#'v1_0.properties'{} = S, Acc) ->
              setelement(3, Acc, S);
         (#'v1_0.application_properties'{} = S, Acc) ->
              setelement(4, Acc, S);
         (BodySect, Acc)
           when is_record(BodySect, 'v1_0.data') orelse
                is_record(BodySect, 'v1_0.amqp_sequence') orelse
                is_record(BodySect, 'v1_0.amqp_value') ->
              Body = element(5, Acc),
              setelement(5, Acc, [BodySect | Body]);
         (Body = {amqp_encoded_body_and_footer, _}, Acc) ->
              %% assertions
              [] = element(5, Acc),
              setelement(5, Acc, Body);
         (#'v1_0.footer'{} = S, Acc) ->
              setelement(6, Acc, S)
      end,
      {undefined, undefined, undefined, undefined, [], undefined},
      Sections),

    {PFR, Type0} = case BodyRev of
                       [#'v1_0.data'{} | _] ->
                           %% We assert that the body consists of one or more data sections.
                           %% If there are multiple data sections, we concatenate the binary data.
                           PFR0 = lists:map(
                                    fun(#'v1_0.data'{content = Content}) ->
                                            %% In practice, when converting from mc_amqp
                                            %% to mc_amqpl, Content will be a single binary,
                                            %% in which case iolist_to_binary/1 is cheap.
                                            iolist_to_binary(Content)
                                    end, BodyRev),
                           {PFR0, undefined};
                       {amqp_encoded_body_and_footer, BodyAndFooterBin} ->
                           {[BodyAndFooterBin], ?AMQP10_TYPE};
                       _ ->
                           %% Anything else needs to be AMQP encoded.
                           PFR0 = lists:map(fun amqp_encoded_binary/1, BodyRev),
                           {PFR0, ?AMQP10_TYPE}
                   end,
    #'v1_0.properties'{message_id = MsgId,
                       user_id = UserId0,
                       reply_to = ReplyTo0,
                       correlation_id = CorrId,
                       content_type = ContentType,
                       content_encoding = ContentEncoding,
                       creation_time = Timestamp,
                       group_id = GroupId} = case Prop of
                                                 undefined ->
                                                     #'v1_0.properties'{};
                                                 _ ->
                                                     Prop
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
    Priority = case H of
                   #'v1_0.header'{priority = {_, P}} -> P;
                   _ -> amqp10_map_get(symbol(<<"x-basic-priority">>), MA)
               end,
    %% check amqp header first for priority, ttl
    Expiration = case H of
                     #'v1_0.header'{ttl = {_, T}} ->
                         integer_to_binary(T);
                     _ ->
                         amqp10_map_get(symbol(<<"x-basic-expiration">>), MA)
                 end,
    Type = case Type0 of
               undefined ->
                   amqp10_map_get(symbol(<<"x-basic-type">>), MA);
               _ ->
                   Type0
           end,

    Headers0 = lists:filtermap(fun({_K, {as_is, _, _}}) ->
                                       false;
                                  ({{utf8, K}, V})
                                    when ?IS_SHORTSTR_LEN(K) ->
                                       {true, to_091(K, V)};
                                  (_) ->
                                       false
                               end, AP),
    %% Add remaining x- message annotations as headers
    XHeaders = lists:filtermap(fun({{symbol, <<"x-cc">>}, V}) ->
                                       {true, to_091(<<"CC">>, V)};
                                  ({{symbol, <<"x-opt-rabbitmq-received-time">>}, {timestamp, Ts}}) ->
                                       {true, {<<"timestamp_in_ms">>, long, Ts}};
                                  ({{symbol, <<"x-opt-deaths">>}, V}) ->
                                       convert_from_amqp_deaths(V);
                                  ({_K, {as_is, _, _}}) ->
                                       false;
                                  ({{symbol, <<"x-", _/binary>> = K}, V})
                                    when ?IS_SHORTSTR_LEN(K) ->
                                       case is_internal_header(K) of
                                           false ->
                                               {true, to_091(K, V)};
                                           true ->
                                               false
                                       end;
                                  (_) ->
                                       false
                               end, MA),
    {Headers1, MsgId091} = message_id(MsgId, <<"x-message-id">>, Headers0),
    {Headers2, CorrId091} = message_id(CorrId, <<"x-correlation-id">>, Headers1),

    Headers = case Env of
                  #{'rabbitmq_4.0.0' := false} ->
                      Headers3 = case AProp of
                                     undefined ->
                                         Headers2;
                                     #'v1_0.application_properties'{} ->
                                         APropBin = amqp_encoded_binary(AProp),
                                         [{?AMQP10_APP_PROPERTIES_HEADER, longstr, APropBin} | Headers2]
                                 end,
                      Headers4 = case Prop of
                                     undefined ->
                                         Headers3;
                                     #'v1_0.properties'{} ->
                                         PropBin = amqp_encoded_binary(Prop),
                                         [{?AMQP10_PROPERTIES_HEADER, longstr, PropBin} | Headers3]
                                 end,
                      Headers5 = case MAnn of
                                     undefined ->
                                         Headers4;
                                     #'v1_0.message_annotations'{} ->
                                         MAnnBin = amqp_encoded_binary(MAnn),
                                         [{?AMQP10_MESSAGE_ANNOTATIONS_HEADER, longstr, MAnnBin} | Headers4]
                                 end,
                      Headers6 = case Footer of
                                     undefined ->
                                         Headers5;
                                     #'v1_0.footer'{} ->
                                         FootBin = amqp_encoded_binary(Footer),
                                         [{?AMQP10_FOOTER, longstr, FootBin} | Headers5]
                                 end,
                      Headers6;
                  _ ->
                      Headers2
              end,

    UserId1 = unwrap(UserId0),
    %% user-id is a binary type so we need to validate
    %% if we can use it as is
    UserId = case mc_util:is_valid_shortstr(UserId1) of
                 true ->
                     UserId1;
                 false ->
                     %% drop it, what else can we do?
                     undefined
             end,

    BP = #'P_basic'{message_id = MsgId091,
                    delivery_mode = DelMode,
                    expiration = Expiration,
                    user_id = UserId,
                    headers = case XHeaders ++ Headers of
                                  [] -> undefined;
                                  AllHeaders -> AllHeaders
                              end,
                    reply_to = unwrap_shortstr(ReplyTo0),
                    type = Type,
                    app_id = unwrap_shortstr(GroupId),
                    priority = Priority,
                    correlation_id = CorrId091,
                    content_type = unwrap(ContentType),
                    content_encoding = unwrap(ContentEncoding),
                    timestamp = unwrap(Timestamp)
                   },
    #content{class_id = ?CLASS_ID,
             properties = BP,
             properties_bin = none,
             payload_fragments_rev = PFR};
convert_from(_SourceProto, _, _) ->
    not_implemented.

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

x_header(_Key, #content{properties = #'P_basic'{headers = undefined}}) ->
    undefined;
x_header(Key, #content{properties = #'P_basic'{headers = Headers}}) ->
    case rabbit_misc:table_lookup(Headers, Key) of
        undefined ->
            undefined;
        {Type, Value} ->
            from_091(Type, Value)
    end;
x_header(Key, #content{properties = none} = Content0) ->
    Content = rabbit_binary_parser:ensure_content_decoded(Content0),
    x_header(Key, Content).

x_headers(#content{properties = #'P_basic'{headers = undefined}}) ->
    #{};
x_headers(#content{properties = #'P_basic'{headers = Headers}}) ->
    L = lists:filtermap(
          fun({Name, Type, Val}) ->
                  case mc_util:is_x_header(Name) of
                      true ->
                          {true, {Name, from_091(Type, Val)}};
                      false ->
                          false
                  end
          end, Headers),
    maps:from_list(L);
x_headers(#content{properties = none} = Content0) ->
    Content = rabbit_binary_parser:ensure_content_decoded(Content0),
    x_headers(Content).

property(Prop, Content) ->
    mc_util:infer_type(mc_compat:get_property(Prop, Content)).

routing_headers(#content{properties = #'P_basic'{headers = undefined}}, _Opts) ->
    #{};
routing_headers(#content{properties = #'P_basic'{headers = Headers}}, Opts) ->
    IncludeX = lists:member(x_headers, Opts),
    %% Complex AMQP values such as array and table are hard to match on but
    %% should still be included as routing headers as users may use a `void'
    %% match which would only check for the presence of the key
    lists:foldl(
      fun({<<"x-", _/binary>> = Key, T, Value}, Acc) ->
              case IncludeX of
                  true ->
                      Acc#{Key => routing_value(T, Value)};
                  false ->
                      Acc
              end;
         ({Key, T,  Value}, Acc) ->
              Acc#{Key => routing_value(T, Value)}
      end, #{}, Headers);
routing_headers(#content{properties = none} = Content, Opts) ->
    routing_headers(prepare(read, Content), Opts).

routing_value(timestamp, V) ->
    V * 1000;
routing_value(_, V) ->
    V.

set_property(ttl, undefined, #content{properties = Props} = C) ->
    %% only ttl is ever modified atm and only unset during dead lettering
    C#content{properties = Props#'P_basic'{expiration = undefined},
              properties_bin = none};
set_property(_P, _V, Msg) ->
    Msg.

prepare(read, Content) ->
    rabbit_binary_parser:ensure_content_decoded(Content);
prepare(store, Content) ->
    rabbit_binary_parser:clear_decoded_content(
      rabbit_binary_generator:ensure_content_encoded(Content, ?PROTOMOD)).

convert_to(?MODULE, Content, _Env) ->
    Content;
convert_to(mc_amqp, #content{payload_fragments_rev = PFR} = Content, Env) ->
    #content{properties = Props} = prepare(read, Content),
    #'P_basic'{message_id = MsgId0,
               expiration = Expiration,
               delivery_mode = DelMode,
               headers = Headers0,
               user_id = UserId,
               reply_to = ReplyTo,
               type = Type,
               priority = Priority,
               app_id = AppId,
               correlation_id = CorrId0,
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
    %% TODO: only add header section if at least one of the fields
    %% needs to be set
    Ttl = case Expiration of
              undefined ->
                  undefined;
              _ ->
                  %% Channel already checked for valid integer.
                  binary_to_integer(Expiration)
          end,

    H = #'v1_0.header'{durable = DelMode =:= 2,
                       ttl = wrap(uint, Ttl),
                       %% TODO: check Priority is a ubyte?
                       priority = wrap(ubyte, Priority)},
    CorrId = case mc_util:urn_string_to_uuid(CorrId0) of
                 {ok, CorrUUID} ->
                     {uuid, CorrUUID};
                 _ ->
                     wrap(utf8, CorrId0)
             end,
    MsgId = case mc_util:urn_string_to_uuid(MsgId0) of
                 {ok, MsgUUID} ->
                     {uuid, MsgUUID};
                 _ ->
                     wrap(utf8, MsgId0)
             end,
    P = case amqp10_section_header(?AMQP10_PROPERTIES_HEADER, Headers) of
            undefined ->
                #'v1_0.properties'{message_id = MsgId,
                                   user_id = wrap(binary, UserId),
                                   to = undefined,
                                   % subject = wrap(utf8, RKey),
                                   reply_to = wrap(utf8, ReplyTo),
                                   correlation_id = CorrId,
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
                           not mc_util:is_x_header(K)],
                 #'v1_0.application_properties'{content = APC};
             A ->
                 A
         end,

    %% x- headers are stored as message annotations
    MA = case amqp10_section_header(?AMQP10_MESSAGE_ANNOTATIONS_HEADER, Headers) of
             undefined ->
                 MAC0 = lists:filtermap(
                          fun({<<"x-", _/binary>> = K, T, V}) ->
                                  %% All message annotation keys need to be either a symbol or ulong
                                  %% but 0.9.1 field-table names are always strings.
                                  {true, {{symbol, K}, from_091(T, V)}};
                             ({<<"CC">>, T = array, V}) ->
                                  %% Special case the 0.9.1 CC header into 1.0 message annotations because
                                  %% 1.0 application properties must not contain list or array values.
                                  {true, {{symbol, <<"x-cc">>}, from_091(T, V)}};
                             (_) ->
                                  false
                          end, Headers),
                 %% `type' doesn't have a direct equivalent so adding as
                 %% a message annotation here
                 MAC = map_add(symbol, <<"x-basic-type">>, utf8, Type, MAC0),
                 #'v1_0.message_annotations'{content = MAC};
             Section ->
                 Section
         end,
    BodySections = case Type of
                       ?AMQP10_TYPE ->
                           amqp10_framing:decode_bin(
                             iolist_to_binary(lists:reverse(PFR)));
                       _ ->
                           [#'v1_0.data'{content = lists:reverse(PFR)}]
                   end,
    Tail = case amqp10_section_header(?AMQP10_FOOTER, Headers) of
               undefined ->
                   BodySections;
               #'v1_0.footer'{} = Footer ->
                   BodySections ++ [Footer]
           end,

    Sections = [H, MA, P, AP | Tail],
    mc_amqp:convert_from(mc_amqp, Sections, Env);
convert_to(_TargetProto, _Content, _Env) ->
    not_implemented.

protocol_state(#content{properties = #'P_basic'{headers = H00,
                                                priority = Priority0,
                                                delivery_mode = DeliveryMode0} = B0} = C,
               Anns) ->
    H0 = case H00 of
             undefined -> [];
             _ ->
                 H00
         end,
    Headers0 = case Anns of
                   #{deaths := Deaths} ->
                       deaths_to_headers(Deaths, H0);
                   _ ->
                       H0
               end,
    %% Add any x- annotations as headers
    Headers1 = maps:fold(
                 fun (<<"x-", _/binary>> = Key, Val, H) when is_integer(Val) ->
                         [{Key, long, Val} | H];
                     (<<"x-", _/binary>> = Key, Val, H) when is_binary(Val) ->
                         [{Key, longstr, Val} | H];
                     (<<"x-", _/binary>> = Key, Val, H) when is_boolean(Val) ->
                         [{Key, bool, Val} | H];
                     (<<"timestamp_in_ms">> = Key, Val, H) when is_integer(Val) ->
                         [{Key, long, Val} | H];
                     (_, _, Acc) ->
                         Acc
                 end, Headers0, Anns),
    Headers = case Headers1 of
                  [] ->
                      H00;
                  _ ->
                      %% Dedup
                      lists:usort(fun({Key1, _, _}, {Key2, _, _}) ->
                                          Key1 =< Key2
                                  end, Headers1)
              end,
    Timestamp = case Anns of
                    #{?ANN_TIMESTAMP := Ts} ->
                        Ts div 1000;
                    _ ->
                        undefined
                end,
    Expiration = case Anns of
                     #{ttl := undefined} ->
                         %% this resets the TTL, only done bt dead lettering
                         %% publishes
                         undefined;
                     #{ttl := Ttl} ->
                         integer_to_binary(Ttl);
                     _ ->
                         B0#'P_basic'.expiration
                 end,
    Priority = case Priority0 of
                   undefined ->
                       case Anns of
                           #{?ANN_PRIORITY := P} ->
                               %% This branch is hit when a message with priority was originally
                               %% published with AMQP to a classic or quorum queue because the
                               %% AMQP header isn't stored on disk.
                               P;
                           _ ->
                               undefined
                       end;
                   _ ->
                       Priority0
               end,
    DelMode = case DeliveryMode0 of
                  undefined ->
                      case Anns of
                          #{?ANN_DURABLE := false} ->
                              %% Leave it undefined which is equivalent to 1.
                              undefined;
                          _ ->
                              %% This branch is hit when a durable message was originally published
                              %% with AMQP to a classic or quorum queue because the AMQP header isn't
                              %% stored on disk.
                              2
                      end;
                  _ ->
                      DeliveryMode0
              end,
    B = B0#'P_basic'{headers = Headers,
                     delivery_mode = DelMode,
                     priority = Priority,
                     expiration = Expiration,
                     timestamp = Timestamp},

    C#content{properties = B,
              properties_bin = none};
protocol_state(Content0, Anns) ->
    %% TODO: refactor to detect _if_ the properties even need decoding
    %% It is possible that no additional annotations or properties need to be
    %% changed
    protocol_state(prepare(read, Content0), Anns).

-spec message(rabbit_types:exchange_name(), rabbit_types:routing_key(), #content{}) ->
    {ok, mc:state()} | {error, Reason :: any()}.
message(ExchangeName, RoutingKey, Content) ->
    message(ExchangeName, RoutingKey, Content, #{}).

%% helper for creating message container from messages received from AMQP legacy
-spec message(rabbit_types:exchange_name(), rabbit_types:routing_key(), #content{}, map()) ->
    {ok, mc:state()} | {error, Reason :: any()}.
message(#resource{name = ExchangeNameBin},
        RoutingKey,
        #content{properties = Props} = Content,
        Anns)
  when is_binary(RoutingKey) andalso
       is_map(Anns) ->
    case rabbit_basic:header_routes(Props#'P_basic'.headers) of
        {error, _} = Error ->
            Error;
        HeaderRoutes ->
            {ok, mc:init(?MODULE,
                         Content,
                         Anns#{?ANN_ROUTING_KEYS => [RoutingKey | HeaderRoutes],
                               ?ANN_EXCHANGE => ExchangeNameBin})}
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
    {ok, Msg} = message(Ex, RKey, prepare(read, Content), Anns),
    Msg.

%% Internal

deaths_to_headers(Deaths, Headers0) ->
    Infos = case Deaths of
                #deaths{records = Records} ->
                    %% sort records by the last timestamp
                    List = lists:sort(
                             fun({_, #death{anns = #{last_time := L1}}},
                                 {_, #death{anns = #{last_time := L2}}}) ->
                                     L1 =< L2
                             end, maps:to_list(Records)),
                    lists:foldl(fun(Record, Acc) ->
                                        Table = death_table(Record),
                                        [Table | Acc]
                                end, [], List);
                _ ->
                    lists:map(fun death_table/1, Deaths)
            end,
    rabbit_misc:set_table_value(Headers0, <<"x-death">>, array, Infos).

convert_from_amqp_deaths({array, map, Maps}) ->
    L = lists:map(
          fun({map, KvList}) ->
                  {Ttl, KvList1} = case KvList of
                                       [{{symbol, <<"ttl">>}, {uint, Ttl0}} | Tail] ->
                                           {Ttl0, Tail};
                                       _ ->
                                           {undefined, KvList}
                                   end,
                  [
                   {{symbol, <<"queue">>}, {utf8, Queue}},
                   {{symbol, <<"reason">>}, {symbol, Reason}},
                   {{symbol, <<"count">>}, {ulong, Count}},
                   {{symbol, <<"first-time">>}, {timestamp, FirstTime}},
                   {{symbol, <<"last-time">>}, {timestamp, _LastTime}},
                   {{symbol, <<"exchange">>}, {utf8, Exchange}},
                   {{symbol, <<"routing-keys">>}, {array, utf8, RKeys0}}
                  ] = KvList1,
                  RKeys = [Key || {utf8, Key} <- RKeys0],
                  death_table(Queue, Reason, Exchange, RKeys, Count, FirstTime, Ttl)
          end, Maps),
    {true, {<<"x-death">>, array, L}};
convert_from_amqp_deaths(_IgnoreUnknownValue) ->
    false.

death_table({{QName, Reason},
             #death{exchange = Exchange,
                    routing_keys = RoutingKeys,
                    count = Count,
                    anns = DeathAnns = #{first_time := FirstTime}}}) ->
    death_table(QName, Reason, Exchange, RoutingKeys, Count, FirstTime,
                maps:get(ttl, DeathAnns, undefined)).

death_table(QName, Reason, Exchange, RoutingKeys, Count, FirstTime, Ttl) ->
    L0 = [
          {<<"count">>, long, Count},
          {<<"reason">>, longstr, rabbit_data_coercion:to_binary(Reason)},
          {<<"queue">>, longstr, QName},
          {<<"time">>, timestamp, FirstTime div 1000},
          {<<"exchange">>, longstr, Exchange},
          {<<"routing-keys">>, array, [{longstr, Key} || Key <- RoutingKeys]}
         ],
    L = case Ttl of
            undefined ->
                L0;
            _ ->
                Expiration = integer_to_binary(Ttl),
                [{<<"original-expiration">>, longstr, Expiration} | L0]
        end,
    {table, L}.

strip_header(#content{properties = #'P_basic'{headers = undefined}}
             = DecodedContent, _Key) ->
    DecodedContent;
strip_header(#content{properties = Props0 = #'P_basic'{headers = Headers0}}
             = Content, Key) ->
    case lists:keytake(Key, 1, Headers0) of
        false ->
            Content;
        {value, _Found, Headers} ->
            Props = Props0#'P_basic'{headers = Headers},
            rabbit_binary_generator:clear_encoded_content(
              Content#content{properties = Props})
    end.

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

from_091(longstr, V) ->
    case mc_util:is_utf8_no_null_limited(V) of
        true ->
            {utf8, V};
        false ->
            {binary, V}
    end;
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

unwrap_shortstr({utf8, V})
  when is_binary(V) andalso
       ?IS_SHORTSTR_LEN(V) ->
    V;
unwrap_shortstr(_) ->
    undefined.

to_091(Key, {utf8, V}) -> {Key, longstr, V};
to_091(Key, {symbol, V}) -> {Key, longstr, V};
to_091(Key, {long, V}) -> {Key, long, V};
to_091(Key, {ulong, V}) -> {Key, long, V}; %% TODO: we could try to constrain this
to_091(Key, {byte, V}) -> {Key, byte, V};
to_091(Key, {ubyte, V}) -> {Key, unsignedbyte, V};
to_091(Key, {short, V}) -> {Key, short, V};
to_091(Key, {ushort, V}) -> {Key, unsignedshort, V};
to_091(Key, {uint, V}) -> {Key, unsignedint, V};
to_091(Key, {int, V}) -> {Key, signedint, V};
to_091(Key, {double, V}) -> {Key, double, V};
to_091(Key, {float, V}) -> {Key, float, V};
to_091(Key, {timestamp, V}) -> {Key, timestamp, V div 1000};
to_091(Key, {binary, V}) -> {Key, longstr, V};
to_091(Key, {boolean, V}) -> {Key, bool, V};
to_091(Key, true) -> {Key, bool, true};
to_091(Key, false) -> {Key, bool, false};
to_091(Key, undefined) -> {Key, void, undefined};
to_091(Key, null) -> {Key, void, undefined};
to_091(Key, {list, L}) ->
    to_091_array(Key, L);
to_091(Key, {map, M}) ->
    T = lists:filtermap(fun({K, V}) when element(1, K) =:= as_is orelse
                                         element(1, V) =:= as_is ->
                                false;
                           ({K, V}) ->
                                {true, to_091(unwrap(K), V)}
                        end, M),
    {Key, table, T};
to_091(Key, {array, _T, L}) ->
    to_091_array(Key, L).

to_091_array(Key, L) ->
    A = lists:filtermap(fun({as_is, _, _}) ->
                                false;
                           (V) ->
                                {true, to_091(V)}
                        end, L),
    {Key, array, A}.

to_091({utf8, V}) -> {longstr, V};
to_091({symbol, V}) -> {longstr, V};
to_091({long, V}) -> {long, V};
to_091({byte, V}) -> {byte, V};
to_091({ubyte, V}) -> {unsignedbyte, V};
to_091({short, V}) -> {short, V};
to_091({ushort, V}) -> {unsignedshort, V};
to_091({uint, V}) -> {unsignedint, V};
to_091({int, V}) -> {signedint, V};
to_091({double, V}) -> {double, V};
to_091({float, V}) -> {float, V};
to_091({timestamp, V}) -> {timestamp, V div 1000};
to_091({binary, V}) -> {longstr, V};
to_091({boolean, V}) -> {bool, V};
to_091(true) -> {bool, true};
to_091(false) -> {bool, false};
to_091(undefined) -> {void, undefined};
to_091(null) -> {void, undefined};
to_091({list, L}) ->
    {array, [to_091(V) || V <- L]};
to_091({map, M}) ->
    {table, [to_091(unwrap(K), V) || {K, V} <- M]}.

message_id({uuid, UUID}, _HKey, H0) ->
    {H0, mc_util:uuid_to_urn_string(UUID)};
message_id({ulong, N}, _HKey, H0) ->
    {H0, erlang:integer_to_binary(N)};
message_id({binary, B}, HKey, H0) ->
    {[{HKey, binary, B} | H0], undefined};
message_id({utf8, S}, HKey, H0) ->
    case ?IS_SHORTSTR_LEN(S) of
        true ->
            {H0, S};
        false ->
            {[{HKey, longstr, S} | H0], undefined}
    end;
message_id(undefined, _HKey, H) ->
    {H, undefined}.

essential_properties(#content{} = C) ->
    #'P_basic'{delivery_mode = Mode,
               priority = Priority,
               timestamp = TimestampRaw,
               headers = Headers} = Props = C#content.properties,
    {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
    Timestamp = case TimestampRaw of
                    undefined ->
                        undefined;
                    _ ->
                        %% timestamp should be in ms
                        TimestampRaw * 1000
                end,
    Durable = Mode == 2,
    BccKeys = case rabbit_basic:header(<<"BCC">>, Headers) of
                  {<<"BCC">>, array, Routes} ->
                      [Route || {longstr, Route} <- Routes];
                  _ ->
                      undefined
              end,
    maps_put_truthy(
      ?ANN_PRIORITY, Priority,
      maps_put_truthy(
        ttl, MsgTTL,
        maps_put_truthy(
          ?ANN_TIMESTAMP, Timestamp,
          maps_put_falsy(
            ?ANN_DURABLE, Durable,
            maps_put_truthy(
              bcc, BccKeys,
              #{}))))).

%% headers that are added as annotations during conversions
is_internal_header(<<"x-basic-", _/binary>>) ->
    true;
is_internal_header(<<"x-routing-key">>) ->
    true;
is_internal_header(<<"x-exchange">>) ->
    true;
is_internal_header(<<"x-death">>) ->
    true;
is_internal_header(_) ->
    false.

amqp10_section_header(Header, Headers) ->
    case lists:keyfind(Header, 1, Headers) of
        {_, _, Data} when is_binary(Data) ->
            [Section] = amqp10_framing:decode_bin(Data),
            Section ;
        _ ->
            undefined
    end.

amqp_encoded_binary(Section) ->
    iolist_to_binary(amqp10_framing:encode_bin(Section)).
