-module(mc_amqp).
-behaviour(mc).

-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include("mc.hrl").

-export([
         init/1,
         size/1,
         x_header/2,
         property/2,
         routing_headers/2,
         convert_to/3,
         convert_from/3,
         protocol_state/2,
         serialize/1,
         prepare/2
        ]).

-import(rabbit_misc,
        [maps_put_truthy/3,
         maps_put_falsy/3
        ]).

-type message_section() ::
    #'v1_0.header'{} |
    #'v1_0.delivery_annotations'{} |
    #'v1_0.message_annotations'{} |
    #'v1_0.properties'{} |
    #'v1_0.application_properties'{} |
    #'v1_0.data'{} |
    #'v1_0.amqp_sequence'{} |
    #'v1_0.amqp_value'{} |
    #'v1_0.footer'{}.

-define(SIMPLE_VALUE(V), is_binary(V) orelse
                         is_number(V) orelse
                         is_boolean(V)).

-type opt(T) :: T | undefined.
-type amqp10_data() :: [#'v1_0.amqp_sequence'{} | #'v1_0.data'{}] |
                       #'v1_0.amqp_value'{}.
-record(msg,
        {
         header :: opt(#'v1_0.header'{}),
         delivery_annotations = []:: list(),
         message_annotations = [] :: list(),
         properties :: opt(#'v1_0.properties'{}),
         application_properties = [] :: list(),
         data = [] :: amqp10_data(),
         footer = [] :: list()
        }).

-opaque state() :: #msg{}.

-export_type([
              state/0,
              message_section/0
             ]).

%% mc implementation
init(Sections) when is_list(Sections) ->
    Msg = decode(Sections, #msg{}),
    init(Msg);
init(#msg{} = Msg) ->
    %% TODO: as the essential annotations, durable, priority, ttl and delivery_count
    %% is all we are interested in it isn't necessary to keep hold of the
    %% incoming AMQP header inside the state
    Anns = essential_properties(Msg),
    {Msg, Anns}.

convert_from(?MODULE, Sections, _Env) ->
    element(1, init(Sections));
convert_from(_SourceProto, _, _Env) ->
    not_implemented.

size(#msg{data = Body}) ->
    %% TODO how to estimate anything but data sections?
    BodySize = if is_list(Body) ->
                      lists:foldl(
                        fun(#'v1_0.data'{content = Data}, Acc) ->
                                iolist_size(Data) + Acc;
                           (#'v1_0.amqp_sequence'{content = _}, Acc) ->
                                Acc
                        end, 0, Body);
                  is_record(Body, 'v1_0.amqp_value') ->
                      0
               end,
    {_MetaSize = 0, BodySize}.

x_header(Key, Msg) ->
    message_annotation(Key, Msg, undefined).

property(correlation_id, #msg{properties = #'v1_0.properties'{correlation_id = Corr}}) ->
    Corr;
property(message_id, #msg{properties = #'v1_0.properties'{message_id = MsgId}}) ->
    MsgId;
property(_Prop, #msg{}) ->
    undefined.

routing_headers(Msg, Opts) ->
    IncludeX = lists:member(x_headers, Opts),
    X = case IncludeX of
            true ->
                message_annotations_as_simple_map(Msg);
            false ->
                #{}
        end,
    application_properties_as_simple_map(Msg, X).


get_property(durable, Msg) ->
    case Msg of
        #msg{header = #'v1_0.header'{durable = Durable}}
          when is_boolean(Durable) ->
            Durable;
        #msg{header = #'v1_0.header'{durable = {boolean, Durable}}} ->
            Durable;
        _ ->
            %% fallback in case the source protocol was old AMQP 0.9.1
            case message_annotation(<<"x-basic-delivery-mode">>, Msg, 2) of
                {ubyte, 2} ->
                    true;
                _ ->
                    false
            end
    end;
get_property(timestamp, Msg) ->
    case Msg of
        #msg{properties = #'v1_0.properties'{creation_time = {timestamp, Ts}}} ->
            Ts;
        _ ->
            undefined
    end;
get_property(ttl, Msg) ->
    case Msg of
        #msg{header = #'v1_0.header'{ttl = {_, Ttl}}} ->
            Ttl;
        _ ->
            %% fallback in case the source protocol was AMQP 0.9.1
            case message_annotation(<<"x-basic-expiration">>, Msg, undefined) of
                {utf8, Expiration}  ->
                    {ok, Ttl} = rabbit_basic:parse_expiration(Expiration),
                    Ttl;
                _ ->
                    undefined
            end
    end;
get_property(priority, Msg) ->
    case Msg of
        #msg{header = #'v1_0.header'{priority = {ubyte, Priority}}} ->
            Priority;
        _ ->
            %% fallback in case the source protocol was AMQP 0.9.1
            case message_annotation(<<"x-basic-priority">>, Msg, undefined) of
                {_, Priority}  ->
                    Priority;
                _ ->
                    undefined
            end
    end.

convert_to(?MODULE, Msg, _Env) ->
    Msg;
convert_to(TargetProto, Msg, Env) ->
    TargetProto:convert_from(?MODULE, msg_to_sections(Msg), Env).

serialize(Sections) ->
    encode_bin(Sections).

protocol_state(Msg0 = #msg{message_annotations = MA0}, Anns) ->
    MA = maps:fold(fun(?ANN_EXCHANGE, Exchange, L) ->
                           maps_upsert(<<"x-exchange">>, {utf8, Exchange}, L);
                      (?ANN_ROUTING_KEYS, RKeys, L) ->
                           RKey = hd(RKeys),
                           maps_upsert(<<"x-routing-key">>, {utf8, RKey}, L);
                      (<<"x-", _/binary>> = K, V, L)
                        when V =/= undefined ->
                           %% any x-* annotations get added as message annotations
                           maps_upsert(K, mc_util:infer_type(V), L);
                      (<<"timestamp_in_ms">>, V, L) ->
                           maps_upsert(<<"x-opt-rabbitmq-received-time">>, {timestamp, V}, L);
                      (deaths, Deaths, L)
                        when is_list(Deaths) ->
                           Maps = encode_deaths(Deaths),
                           maps_upsert(<<"x-opt-deaths">>, {array, map, Maps}, L);
                      (_, _, Acc) ->
                           Acc
                   end, MA0, Anns),
    Msg = Msg0#msg{message_annotations = MA},
    msg_to_sections(Msg).

prepare(_For, Msg) ->
    Msg.

%% internal

msg_to_sections(#msg{header = H,
                     delivery_annotations = DAC,
                     message_annotations = MAC,
                     properties = P,
                     application_properties = APC,
                     data = Data,
                     footer = FC}) ->
    Tail = case FC of
               [] ->
                   [];
               _ ->
                   [#'v1_0.footer'{content = FC}]
           end,
    S0 = case Data of
             #'v1_0.amqp_value'{} ->
                 [Data | Tail];
             _ when is_list(Data) ->
                 Data ++ Tail
         end,
    S1 = case APC of
             [] ->
                 S0;
             _ ->
                 [#'v1_0.application_properties'{content = APC} | S0]
         end,
    S2 = case P of
             undefined ->
                 S1;
             _ ->
                 [P | S1]
         end,
    S3 = case MAC of
             [] ->
                 S2;
             _ ->
                 [#'v1_0.message_annotations'{content = MAC} | S2]
         end,
    S4 = case DAC of
             [] ->
                 S3;
             _ ->
                 [#'v1_0.delivery_annotations'{content = DAC} | S3]
         end,
    case H of
        undefined ->
            S4;
        _ ->
            [H | S4]
    end.

maps_upsert(Key, TaggedVal, KVList) ->
    TaggedKey = {symbol, Key},
    Elem = {TaggedKey, TaggedVal},
    lists:keystore(TaggedKey, 1, KVList, Elem).

encode_bin(undefined) ->
    <<>>;
encode_bin(Sections) when is_list(Sections) ->
    [amqp10_framing:encode_bin(Section) || Section <- Sections,
                                           not is_empty(Section)];
encode_bin(Section) ->
    case is_empty(Section) of
        true ->
            <<>>;
        false ->
            amqp10_framing:encode_bin(Section)
    end.

is_empty(undefined) ->
    true;
is_empty(#'v1_0.properties'{message_id = undefined,
                            user_id = undefined,
                            to = undefined,
                            subject = undefined,
                            reply_to = undefined,
                            correlation_id = undefined,
                            content_type = undefined,
                            content_encoding = undefined,
                            absolute_expiry_time = undefined,
                            creation_time = undefined,
                            group_id = undefined,
                            group_sequence = undefined,
                            reply_to_group_id = undefined}) ->
    true;
is_empty(#'v1_0.application_properties'{content = []}) ->
    true;
is_empty(#'v1_0.message_annotations'{content = []}) ->
    true;
is_empty(#'v1_0.delivery_annotations'{content = []}) ->
    true;
is_empty(#'v1_0.footer'{content = []}) ->
    true;
is_empty(#'v1_0.header'{durable = undefined,
                        priority = undefined,
                        ttl = undefined,
                        first_acquirer = undefined,
                        delivery_count = undefined}) ->
    true;
is_empty(_) ->
    false.


message_annotation(_Key, #msg{message_annotations = []},
                   Default) ->
    Default;
message_annotation(Key, #msg{message_annotations = Content},
                   Default)
  when is_binary(Key) ->
    mc_util:amqp_map_get(Key, Content, Default).

message_annotations_as_simple_map(#msg{message_annotations = []}) ->
    #{};
message_annotations_as_simple_map(#msg{message_annotations = Content}) ->
    %% the section record format really is terrible
    lists:foldl(fun ({{symbol, K}, {_T, V}}, Acc)
                      when ?SIMPLE_VALUE(V) ->
                        Acc#{K => V};
                    (_, Acc)->
                        Acc
                end, #{}, Content).

application_properties_as_simple_map(#msg{application_properties = []}, M) ->
    M;
application_properties_as_simple_map(#msg{application_properties = Content},
                                     M) ->
    %% the section record format really is terrible
    lists:foldl(fun
                    ({{utf8, K}, {_T, V}}, Acc)
                      when ?SIMPLE_VALUE(V) ->
                        Acc#{K => V};
                    ({{utf8, K}, V}, Acc)
                      when V =:= undefined orelse is_boolean(V) ->
                        Acc#{K => V};
                    (_, Acc)->
                        Acc
                end, M, Content).

decode([], Acc) ->
    Acc;
decode([#'v1_0.header'{} = H | Rem], Msg) ->
    decode(Rem, Msg#msg{header = H});
decode([#'v1_0.message_annotations'{content = MAC} | Rem], Msg) ->
    decode(Rem, Msg#msg{message_annotations = MAC});
decode([#'v1_0.properties'{} = P | Rem], Msg) ->
    decode(Rem, Msg#msg{properties = P});
decode([#'v1_0.application_properties'{content = APC} | Rem], Msg) ->
    decode(Rem, Msg#msg{application_properties = APC});
decode([#'v1_0.delivery_annotations'{content = DAC} | Rem], Msg) ->
    decode(Rem, Msg#msg{delivery_annotations = DAC});
decode([#'v1_0.data'{} = D | Rem], #msg{data = Body} = Msg)
  when is_list(Body) ->
    decode(Rem, Msg#msg{data = Body ++ [D]});
decode([#'v1_0.amqp_sequence'{} = D | Rem], #msg{data = Body} = Msg)
  when is_list(Body) ->
    decode(Rem, Msg#msg{data = Body ++ [D]});
decode([#'v1_0.footer'{content = FC} | Rem], Msg) ->
    decode(Rem, Msg#msg{footer = FC});
decode([#'v1_0.amqp_value'{} = B | Rem], #msg{} = Msg) ->
    %% an amqp value can only be a singleton
    decode(Rem, Msg#msg{data = B}).


encode_deaths(Deaths) ->
    lists:map(
      fun({{Queue, Reason},
           #death{exchange = Exchange,
                  routing_keys = RoutingKeys,
                  count = Count,
                  anns = Anns = #{first_time := FirstTime,
                                  last_time := LastTime}}}) ->
              RKeys = [{utf8, Rk} || Rk <- RoutingKeys],
              Map0 = [
                      {{symbol, <<"queue">>}, {utf8, Queue}},
                      {{symbol, <<"reason">>}, {symbol, atom_to_binary(Reason)}},
                      {{symbol, <<"count">>}, {ulong, Count}},
                      {{symbol, <<"first-time">>}, {timestamp, FirstTime}},
                      {{symbol, <<"last-time">>}, {timestamp, LastTime}},
                      {{symbol, <<"exchange">>}, {utf8, Exchange}},
                      {{symbol, <<"routing-keys">>}, {array, utf8, RKeys}}
                     ],
              Map = case Anns of
                        #{ttl := Ttl} ->
                            [{{symbol, <<"ttl">>}, {uint, Ttl}} | Map0];
                        _ ->
                            Map0
                    end,
              {map, Map}
      end, Deaths).

essential_properties(#msg{message_annotations = MA} = Msg) ->
    Durable = get_property(durable, Msg),
    Priority = get_property(priority, Msg),
    Timestamp = get_property(timestamp, Msg),
    Ttl = get_property(ttl, Msg),

    Anns = maps_put_falsy(
             ?ANN_DURABLE, Durable,
             maps_put_truthy(
               ?ANN_PRIORITY, Priority,
               maps_put_truthy(
                 ?ANN_TIMESTAMP, Timestamp,
                 maps_put_truthy(
                   ttl, Ttl,
                     #{})))),
    case MA of
        [] ->
            Anns;
        _ ->
            lists:foldl(
              fun ({{symbol, <<"x-routing-key">>},
                    {utf8, Key}}, Acc) ->
                      maps:update_with(?ANN_ROUTING_KEYS,
                                       fun(L) -> [Key | L] end,
                                       [Key],
                                       Acc);
                  ({{symbol, <<"x-cc">>},
                    {list, CCs0}}, Acc) ->
                      CCs = [CC || {_T, CC} <- CCs0],
                      maps:update_with(?ANN_ROUTING_KEYS,
                                       fun(L) -> L ++ CCs end,
                                       CCs,
                                       Acc);
                  ({{symbol, <<"x-exchange">>},
                    {utf8, Exchange}}, Acc) ->
                      Acc#{?ANN_EXCHANGE => Exchange};
                  (_, Acc) ->
                      Acc
              end, Anns, MA)
    end.
