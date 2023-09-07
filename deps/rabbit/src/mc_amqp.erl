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
         get_property/2,
         convert_to/2,
         convert_from/2,
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

convert_from(?MODULE, Sections) ->
    element(1, init(Sections));
convert_from(_SourceProto, _) ->
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
          when is_atom(Durable) ->
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
get_property(correlation_id, Msg) ->
    case Msg of
        #msg{properties = #'v1_0.properties'{correlation_id = {_Type, CorrId}}} ->
            CorrId;
        _ ->
            undefined
    end;
get_property(message_id, Msg) ->
    case Msg of
        #msg{properties = #'v1_0.properties'{message_id = {_Type, CorrId}}} ->
            CorrId;
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
    end;
get_property(_P, _Msg) ->
    undefined.

convert_to(?MODULE, Msg) ->
    Msg;
convert_to(TargetProto, Msg) ->
    TargetProto:convert_from(?MODULE, msg_to_sections(Msg, fun (X) -> X end)).

serialize(Sections) ->
    encode_bin(Sections).

protocol_state(Msg, Anns) ->
    Exchange = maps:get(exchange, Anns),
    [RKey | _] = maps:get(routing_keys, Anns),

    %% any x-* annotations get added as message annotations
    AnnsToAdd = maps:filter(fun (Key, _) -> mc_util:is_x_header(Key) end, Anns),

    MACFun = fun(MAC) ->
                     add_message_annotations(
                       AnnsToAdd#{<<"x-exchange">> => wrap(utf8, Exchange),
                                  <<"x-routing-key">> => wrap(utf8, RKey)}, MAC)
             end,

    msg_to_sections(Msg, MACFun).

prepare(_For, Msg) ->
    Msg.

%% internal

msg_to_sections(#msg{header = H,
                     delivery_annotations = DAC,
                     message_annotations = MAC0,
                     properties = P,
                     application_properties = APC,
                     data = Data,
                     footer = FC}, MacFun) ->
    Tail = case FC of
               [] -> [];
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
             [] -> S0;
             _ ->
                 [#'v1_0.application_properties'{content = APC} | S0]
         end,
    S2 = case P of
             undefined -> S1;
             _ ->
                 [P | S1]
         end,
    S3 = case MacFun(MAC0) of
             [] -> S2;
             MAC ->
                 [#'v1_0.message_annotations'{content = MAC} | S2]
         end,
    S4 = case DAC of
             [] -> S3;
             _ ->
                 [#'v1_0.delivery_annotations'{content = DAC} | S3]
         end,
    case H of
        undefined -> S4;
        _ ->
            [H | S4]
    end.





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
                    ({{utf8, K}, undefined}, Acc) ->
                        Acc#{K => undefined};
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

add_message_annotations(Anns, MA0) ->
    maps:fold(fun (K, V, Acc) ->
                      map_add(symbol, K, mc_util:infer_type(V), Acc)
              end, MA0, Anns).

map_add(_T, _Key, undefined, Acc) ->
    Acc;
map_add(KeyType, Key, TaggedValue, Acc0) ->
    TaggedKey = wrap(KeyType, Key),
    lists_upsert({TaggedKey, TaggedValue}, Acc0).

wrap(_Type, undefined) ->
    undefined;
wrap(Type, Val) ->
    {Type, Val}.

key_find(K, [{{_, K}, {_, V}} | _]) ->
    V;
key_find(K, [_ | Rem]) ->
    key_find(K, Rem);
key_find(_K, []) ->
    undefined.

recover_deaths([], Acc) ->
    Acc;
recover_deaths([{map, Kvs} | Rem], Acc) ->
    Queue = key_find(<<"queue">>, Kvs),
    Reason = binary_to_atom(key_find(<<"reason">>, Kvs)),
    DA0 = case key_find(<<"original-expiration">>, Kvs) of
              undefined ->
                  #{};
              Exp ->
                  #{ttl => binary_to_integer(Exp)}
          end,
    RKeys = [RK || {_, RK} <- key_find(<<"routing-keys">>, Kvs)],
    Ts = key_find(<<"time">>, Kvs),
    DA = DA0#{first_time => Ts,
              last_time => Ts},
    recover_deaths(Rem,
                   Acc#{{Queue, Reason} =>
                        #death{anns = DA,
                               exchange = key_find(<<"exchange">>, Kvs),
                               count = key_find(<<"count">>, Kvs),
                               routing_keys = RKeys}}).

essential_properties(#msg{message_annotations = MA} = Msg) ->
    Durable = get_property(durable, Msg),
    Priority = get_property(priority, Msg),
    Timestamp = get_property(timestamp, Msg),
    Ttl = get_property(ttl, Msg),

    Deaths = case message_annotation(<<"x-death">>, Msg, undefined) of
                 {list, DeathMaps}  ->
                     %% TODO: make more correct?
                     Def = {utf8, <<>>},
                     {utf8, FstQ} = message_annotation(<<"x-first-death-queue">>, Msg, Def),
                     {utf8, FstR} = message_annotation(<<"x-first-death-reason">>, Msg, Def),
                     {utf8, LastQ} = message_annotation(<<"x-last-death-queue">>, Msg, Def),
                     {utf8, LastR} = message_annotation(<<"x-last-death-reason">>, Msg, Def),
                     #deaths{first = {FstQ, binary_to_atom(FstR)},
                             last = {LastQ, binary_to_atom(LastR)},
                             records = recover_deaths(DeathMaps, #{})};
                 _ ->
                     undefined
             end,
    Anns = maps_put_falsy(
             durable, Durable,
             maps_put_truthy(
               priority, Priority,
               maps_put_truthy(
                 timestamp, Timestamp,
                 maps_put_truthy(
                   ttl, Ttl,
                   maps_put_truthy(
                     deaths, Deaths,
                     #{}))))),
    case MA of
        [] ->
            Anns;
        _ ->
            lists:foldl(
              fun ({{symbol, <<"x-routing-key">>},
                    {utf8, Key}}, Acc) ->
                      maps:update_with(routing_keys,
                                       fun(L) -> [Key | L] end,
                                       [Key],
                                       Acc);
                  ({{symbol, <<"x-cc">>},
                    {list, CCs0}}, Acc) ->
                      CCs = [CC || {_T, CC} <- CCs0],
                      maps:update_with(routing_keys,
                                       fun(L) -> L ++ CCs end,
                                       CCs,
                                       Acc);
                  ({{symbol, <<"x-exchange">>},
                    {utf8, Exchange}}, Acc) ->
                      Acc#{exchange => Exchange};
                  (_, Acc) ->
                      Acc
              end, Anns, MA)
    end.

lists_upsert(New, L) ->
    lists_upsert(New, L, [], L).

lists_upsert({Key, _} = New, [{Key, _} | Rem], Pref, _All) ->
    lists:reverse(Pref, [New | Rem]);
lists_upsert(New, [Item | Rem], Pref, All) ->
    lists_upsert(New, Rem, [Item | Pref], All);
lists_upsert(New, [], _Pref, All) ->
    [New | All].
