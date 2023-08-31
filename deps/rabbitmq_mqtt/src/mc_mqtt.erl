-module(mc_mqtt).
-behaviour(mc).

-include("rabbit_mqtt_packet.hrl").
-include("rabbit_mqtt.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/mc.hrl").

-define(CONTENT_TYPE_AMQP, <<"message/vnd.rabbitmq.amqp">>).
-define(DEFAULT_MQTT_EXCHANGE, <<"amq.topic">>).

-export([
         init/1,
         size/1,
         x_header/2,
         property/2,
         routing_headers/2,
         convert_to/2,
         convert_from/2,
         protocol_state/2,
         prepare/2
        ]).

init(Msg = #mqtt_msg{qos = Qos,
                     props = Props})
  when is_integer(Qos) ->
    Anns0 = case Qos > 0 of
                true ->
                    #{durable => true};
                false ->
                    #{}
            end,
    Anns1 = case Props of
                #{'Message-Expiry-Interval' := Seconds} ->
                    Anns0#{ttl => timer:seconds(Seconds),
                           timestamp => os:system_time(millisecond)};
                _ ->
                    Anns0
            end,
    Anns = case Props of
               #{'Correlation-Data' := Corr} ->
                   case mc_util:is_valid_shortstr(Corr) of
                       true ->
                           Anns1#{correlation_id => Corr};
                       false ->
                           Anns1
                   end;
               _ ->
                   Anns1
           end,
    {Msg, Anns}.

convert_from(mc_amqp, Sections) ->
    {Header, MsgAnns, AmqpProps, AppProps, PayloadRev,
     PayloadFormatIndicator, ContentType} =
    lists:foldl(
      fun(#'v1_0.header'{} = S, Acc) ->
              setelement(1, Acc, S);
         (#'v1_0.message_annotations'{content = List}, Acc) ->
              setelement(2, Acc, List);
         (#'v1_0.properties'{} = S, Acc) ->
              setelement(3, Acc, S);
         (#'v1_0.application_properties'{content = List}, Acc) ->
              setelement(4, Acc, List);
         (#'v1_0.footer'{}, Acc) ->
              Acc;
         (#'v1_0.data'{content = C}, Acc) ->
              setelement(5, Acc, [C | element(5, Acc)]);
         (#'v1_0.amqp_value'{content = {binary, Bin}}, Acc) ->
              setelement(5, Acc, [Bin]);
         (#'v1_0.amqp_value'{content = C} = Val, Acc) ->
              case amqp_to_utf8_string(C) of
                  cannot_convert ->
                      amqp_encode(Val, Acc);
                  String ->
                      Acc1 = setelement(5, Acc, [String]),
                      setelement(6, Acc1, true)
              end;
         (#'v1_0.amqp_sequence'{} = Seq, Acc) ->
              amqp_encode(Seq, Acc)
      end, {undefined, [], undefined, [], [], false, undefined}, Sections),
    Qos = case Header of
              #'v1_0.header'{durable = true} ->
                  ?QOS_1;
              _ ->
                  ?QOS_0
          end,
    Props0 = case PayloadFormatIndicator of
                 true -> #{'Payload-Format-Indicator' => 1};
                 false -> #{}
             end,
    Props1 = case AmqpProps of
                 #'v1_0.properties'{reply_to = {utf8, Address}} ->
                     MqttX = persistent_term:get(?PERSISTENT_TERM_EXCHANGE),
                     case Address of
                         <<"/topic/", Topic/binary>>
                           when MqttX =:= ?DEFAULT_MQTT_EXCHANGE ->
                             add_response_topic(Topic, Props0);
                         <<"/exchange/", MqttX:(byte_size(MqttX))/binary, "/", RoutingKey/binary>> ->
                             add_response_topic(RoutingKey, Props0);
                         _ ->
                             Props0
                     end;
                 _ ->
                     Props0
             end,
    Props2 = case AmqpProps of
                 #'v1_0.properties'{correlation_id = {_Type, _Val} = Corr} ->
                     Props1#{'Correlation-Data' => correlation_id(Corr)};
                 _ ->
                     Props1
             end,
    Props3 = case ContentType of
                 undefined ->
                     case AmqpProps of
                         #'v1_0.properties'{content_type = {symbol, ContentType1}} ->
                             Props2#{'Content-Type' => rabbit_data_coercion:to_binary(ContentType1)};
                         _ ->
                             Props2
                     end;
                 _ ->
                     Props2#{'Content-Type' => ContentType}
             end,
    UserProp0 = lists:filtermap(fun({{symbol, <<"x-", _/binary>> = Key}, Val}) ->
                                        filter_map_amqp_to_utf8_string(Key, Val);
                                   (_) ->
                                        false
                                end, MsgAnns),
    %% "The keys of this map are restricted to be of type string" [AMQP 1.0 3.2.5]
    UserProp1 = lists:filtermap(fun({{utf8, Key}, Val}) ->
                                        filter_map_amqp_to_utf8_string(Key, Val)
                                end, AppProps),
    Props = case UserProp0 ++ UserProp1 of
                [] -> Props3;
                UserProp -> Props3#{'User-Property' => UserProp}
            end,
    Payload = lists:flatten(lists:reverse(PayloadRev)),
    #mqtt_msg{retain = false,
              qos = Qos,
              dup = false,
              props = Props,
              payload = Payload};
convert_from(mc_amqpl, #content{properties = PBasic,
                                payload_fragments_rev = Payload}) ->
    #'P_basic'{expiration = Expiration,
               delivery_mode = DelMode,
               headers = H0,
               correlation_id = CorrId,
               content_type = ContentType} = PBasic,
    Qos = case DelMode of
              2 -> ?QOS_1;
              _ -> ?QOS_0
          end,
    P0 = case is_binary(ContentType) of
             true -> #{'Content-Type' => ContentType};
             false -> #{}
         end,
    H1 = case H0 of
             undefined -> [];
             _ -> H0
         end,
    {P1, H3} = case lists:keytake(<<"x-reply-to-topic">>, 1, H1) of
                   {value, {_, longstr, Topic}, H2} ->
                       {P0#{'Response-Topic' => rabbit_mqtt_util:amqp_to_mqtt(Topic)}, H2};
                   false ->
                       {P0, H1}
               end,
    {P2, H} = case is_binary(CorrId) of
                  true ->
                      {P1#{'Correlation-Data' => CorrId}, H3};
                  false ->
                      case lists:keytake(<<"x-correlation-id">>, 1, H3) of
                          {value, {_, longstr, Corr}, H4} ->
                              {P1#{'Correlation-Data' => Corr}, H4};
                          false ->
                              {P1, H3}
                      end
              end,
    P3 = case amqpl_header_to_user_property(H) of
             [] ->
                 P2;
             UserProperty ->
                 P2#{'User-Property' => UserProperty}
         end,
    P = case is_binary(Expiration) of
            true ->
                Millis = binary_to_integer(Expiration),
                P3#{'Message-Expiry-Interval' => Millis div 1000};
            false ->
                P3
        end,
    #mqtt_msg{retain = false,
              qos = Qos,
              dup = false,
              payload = lists:reverse(Payload),
              props = P};
convert_from(_SourceProto, _) ->
    not_implemented.

convert_to(?MODULE, Msg) ->
    Msg;
convert_to(mc_amqp, #mqtt_msg{qos = Qos,
                              props = Props,
                              payload = Payload}) ->
    Body = case Props of
               #{'Payload-Format-Indicator' := 1}
                 when is_binary(Payload) ->
                   #'v1_0.amqp_value'{content = {utf8, Payload}};
               _ ->
                   #'v1_0.data'{content = Payload}
           end,
    S0 = [Body],

    %% x- prefixed MQTT User Properties go into Message Annotations.
    %% All other MQTT User Properties go into Application Properties.
    %% MQTT User Property allows duplicate keys, while AMQP maps don't.
    %% Order is semantically important in both MQTT User Property and AMQP maps.
    %% Therefore, we must dedup the keys and must maintain order.
    {MsgAnns, AppProps} =
    case Props of
        #{'User-Property' := UserProps} ->
            {MsgAnnsRev, AppPropsRev, _} =
            lists:foldl(fun({Name, _}, Acc = {_, _, M})
                              when is_map_key(Name, M) ->
                                Acc;
                           ({<<"x-", _/binary>> = Name, Val}, Acc = {MAnns, AProps, M}) ->
                                case mc_util:utf8_string_is_ascii(Name) of
                                    true ->
                                        {[{{symbol, Name}, {utf8, Val}} | MAnns], AProps, M#{Name => true}};
                                    false ->
                                        Acc
                                end;
                           ({Name, Val}, {MAnns, AProps, M}) ->
                                {MAnns, [{{utf8, Name}, {utf8, Val}} | AProps], M#{Name => true}}
                        end, {[], [], #{}}, UserProps),
            {lists:reverse(MsgAnnsRev), lists:reverse(AppPropsRev)};
        _ ->
            {[], []}
    end,
    S1 = case AppProps of
             [] -> S0;
             _ -> [#'v1_0.application_properties'{content = AppProps} | S0]
         end,

    ContentType = case Props of
                      #{'Content-Type' := ContType} ->
                          case mc_util:utf8_string_is_ascii(ContType) of
                              true ->
                                  {symbol, ContType};
                              false ->
                                  undefined
                          end;
                      _ ->
                          undefined
                  end,
    CorrId = case Props of
                 #{'Correlation-Data' := Corr} ->
                     {binary, Corr};
                 _ ->
                     undefined
             end,
    ReplyTo = case Props of
                  #{'Response-Topic' := MqttTopic} ->
                      Topic = rabbit_mqtt_util:mqtt_to_amqp(MqttTopic),
                      Address = case persistent_term:get(?PERSISTENT_TERM_EXCHANGE) of
                                    ?DEFAULT_MQTT_EXCHANGE ->
                                        <<"/topic/", Topic/binary>>;
                                    Exchange ->
                                        <<"/exchange/", Exchange/binary, "/", Topic/binary>>
                                end,
                      {utf8, Address};
                  _ ->
                      undefined
              end,
    S2 = case {ContentType, CorrId, ReplyTo} of
             {undefined, undefined, undefined} ->
                 S1;
             _ ->
                 [#'v1_0.properties'{content_type = ContentType,
                                     correlation_id = CorrId,
                                     reply_to = ReplyTo} | S1]
         end,

    S3 = case MsgAnns of
             [] -> S2;
             _ -> [#'v1_0.message_annotations'{content = MsgAnns} | S2]
         end,
    S = [#'v1_0.header'{durable = Qos > 0} | S3],
    mc_amqp:convert_from(mc_amqp, S);
convert_to(mc_amqpl, #mqtt_msg{qos = Qos,
                               props = Props,
                               payload = Payload}) ->
    DelMode = case Qos of
                  ?QOS_0 -> 1;
                  ?QOS_1 -> 2
              end,
    ContentType = case Props of
                      #{'Content-Type' := ContType} -> ContType;
                      _ -> undefined
                  end,
    Hs0 = case Props of
              #{'User-Property' := UserProperty} ->
                  lists:filtermap(
                    fun({Name, Value})
                          when byte_size(Name) =< ?AMQP_LEGACY_FIELD_NAME_MAX_LEN ->
                            {true, {Name, longstr, Value}};
                       (_) ->
                            false
                    end, UserProperty);
              _ ->
                  []
          end,
    Hs1 = case Props of
              #{'Response-Topic' := Topic} ->
                  [{<<"x-reply-to-topic">>, longstr, rabbit_mqtt_util:mqtt_to_amqp(Topic)} | Hs0];
              _ ->
                  Hs0
          end,
    {CorrId, Hs2} = case Props of
                        #{'Correlation-Data' := Corr} ->
                            case mc_util:is_valid_shortstr(Corr) of
                                true ->
                                    {Corr, Hs1};
                                false ->
                                    {undefined, [{<<"x-correlation-id">>, longstr, Corr} | Hs1]}
                            end;
                        _ ->
                            {undefined, Hs1}
                    end,
    Expiration = case Props of
                     #{'Message-Expiry-Interval' := Seconds} ->
                         integer_to_binary(timer:seconds(Seconds));
                     _ ->
                         undefined
                 end,
    %% "Duplicate fields are illegal." [4.2.5.5 Field Tables]
    %% RabbitMQ sorts field tables by keys.
    Hs = lists:usort(fun({Key1, _Type1, _Val1},
                         {Key2, _Type2, _Val2}) ->
                             Key1 =< Key2
                     end, Hs2),
    BP = #'P_basic'{content_type = ContentType,
                    headers = if Hs =:= [] -> undefined;
                                 Hs =/= [] -> Hs
                              end,
                    delivery_mode = DelMode,
                    correlation_id = CorrId,
                    expiration = Expiration},
    PFR = case is_binary(Payload) of
              true -> [Payload];
              false -> lists:reverse(Payload)
          end,
    #content{class_id = 60,
             properties = BP,
             properties_bin = none,
             payload_fragments_rev = PFR};
convert_to(_TargetProto, #mqtt_msg{}) ->
    not_implemented.

size(#mqtt_msg{payload = Payload,
               topic = Topic,
               props = Props}) ->
    PropsSize = maps:fold(fun size_prop/3, 0, Props),
    MetadataSize = PropsSize + byte_size(Topic),
    {MetadataSize, iolist_size(Payload)}.

size_prop(K, Val, Sum)
  when K =:= 'Content-Type' orelse
       K =:= 'Response-Topic' orelse
       K =:= 'Correlation-Data' ->
    byte_size(Val) + Sum;
size_prop('User-Property', L, Sum) ->
    lists:foldl(fun({Name, Val}, Acc) ->
                        byte_size(Name) + byte_size(Val) + Acc
                end, Sum, L);
size_prop(_, _, Sum) ->
    Sum.

x_header(Key, #mqtt_msg{props = #{'User-Property' := UserProp}}) ->
    case proplists:get_value(Key, UserProp) of
        undefined -> undefined;
        Val -> {utf8, Val}
    end;
x_header(_Key, #mqtt_msg{}) ->
    undefined.

property(correlation_id, #mqtt_msg{props = #{'Correlation-Data' := Corr}}) ->
    {binary, Corr};
property(_Key, #mqtt_msg{}) ->
    undefined.

routing_headers(#mqtt_msg{props = #{'User-Property' := UserProperty}}, Opts) ->
    IncludeX = lists:member(x_headers, Opts),
    lists:foldl(fun({<<"x-", _/binary>> = K, V}, M) ->
                        case IncludeX of
                            true -> M#{K => V};
                            false -> M
                        end;
                   ({K, V}, M) ->
                        M#{K => V}
                end, #{}, UserProperty);
routing_headers(#mqtt_msg{}, _Opts) ->
    #{}.

protocol_state(Msg = #mqtt_msg{props = Props0,
                               topic = Topic}, Anns) ->
    %% Remove any PUBLISH or Will Properties that are not forwarded unaltered.
    Props1 = maps:remove('Message-Expiry-Interval', Props0),
    {WillDelay, Props2} = case maps:take('Will-Delay-Interval', Props1) of
                              error -> {0, Props1};
                              ValMap -> ValMap
                          end,
    Props = case maps:get(ttl, Anns, undefined) of
                undefined ->
                    Props2;
                Ttl ->
                    case maps:get(timestamp, Anns) of
                        undefined ->
                            Props2;
                        Timestamp ->
                            SourceProtocolIsMqtt = Topic =/= undefined,
                            %% Only if source protocol is MQTT we know that timestamp was set by the server.
                            case SourceProtocolIsMqtt of
                                false ->
                                    Props2;
                                true ->
                                    %% "The PUBLISH packet sent to a Client by the Server MUST contain a
                                    %% Message Expiry Interval set to the received value minus the time that
                                    %% the Application Message has been waiting in the Server" [MQTT-3.3.2-6]
                                    WaitingMillis0 = os:system_time(millisecond) - Timestamp,
                                    %% For a delayed Will Message, the waiting time starts
                                    %% when the Will Message was published.
                                    WaitingMillis = WaitingMillis0 - WillDelay * 1000,
                                    MEIMillis = max(0, Ttl - WaitingMillis),
                                    Props2#{'Message-Expiry-Interval' => MEIMillis div 1000}
                            end
                    end
            end,
    [RoutingKey | _] = maps:get(routing_keys, Anns),
    Msg#mqtt_msg{topic = rabbit_mqtt_util:amqp_to_mqtt(RoutingKey),
                 props = Props}.

prepare(_For, #mqtt_msg{} = Msg) ->
    Msg.

correlation_id({uuid, UUID}) ->
    mc_util:uuid_to_string(UUID);
correlation_id({_T, Corr}) ->
    rabbit_data_coercion:to_binary(Corr).

%% Translates AMQP 0.9.1 headers to MQTT 5.0 User Properties if
%% the value is convertible to a UTF-8 String.
-spec amqpl_header_to_user_property(rabbit_framing:amqp_table()) ->
    user_property().
amqpl_header_to_user_property(Table) ->
    lists:filtermap(fun amqpl_field_to_utf8_string_pair/1, Table).

amqpl_field_to_utf8_string_pair({K, longstr, V}) ->
    case mc_util:is_utf8_no_null(V) of
        true -> {true, {K, V}};
        false -> false
    end;
amqpl_field_to_utf8_string_pair({K, T, V})
  when T =:= byte;
       T =:= unsignedbyte;
       T =:= short;
       T =:= unsignedshort;
       T =:= signedint;
       T =:= unsignedint;
       T =:= long;
       T =:= timestamp ->
    {true, {K, integer_to_binary(V)}};
amqpl_field_to_utf8_string_pair({K, T, V})
  when T =:= float;
       T =:= double ->
    {true, {K, float_to_binary(V)}};
amqpl_field_to_utf8_string_pair({K, void, _V}) ->
    {true, {K, <<>>}};
amqpl_field_to_utf8_string_pair({K, bool, V}) ->
    {true, {K, atom_to_binary(V)}};
amqpl_field_to_utf8_string_pair({_K, T, _V})
  when T =:= array;
       T =:= table;
       %% Raw binary data is not UTF-8 encoded.
       T =:= binary ->
    false.

filter_map_amqp_to_utf8_string(Key, TypeVal) ->
    case amqp_to_utf8_string(TypeVal) of
        cannot_convert ->
            false;
        String ->
            {true, {Key, String}}
    end.

amqp_to_utf8_string({utf8, Val})
  when is_binary(Val) ->
    Val;
amqp_to_utf8_string(Val)
  when Val =:= null;
       Val =:= undefined ->
    <<>>;
amqp_to_utf8_string({T, Val})
  when T =:= byte;
       T =:= ubyte;
       T =:= short;
       T =:= ushort;
       T =:= int;
       T =:= uint;
       T =:= long;
       T =:= ulong ->
    integer_to_binary(Val);
amqp_to_utf8_string({timestamp, Millis}) ->
    %% MQTT 5.0 defines all intervals (e.g. Keep Alive, Message Expiry Interval,
    %% Session Expiry Interval) in seconds. Therefore let's convert to seconds.
    integer_to_binary(Millis div 1000);
amqp_to_utf8_string({T, Val})
  when T =:= double;
       T =:= float ->
    float_to_binary(Val);
amqp_to_utf8_string(Val)
  when Val =:= true;
       Val =:= {boolean, true} ->
    <<"true">>;
amqp_to_utf8_string(Val)
  when Val =:= false;
       Val =:= {boolean, false} ->
    <<"false">>;
amqp_to_utf8_string({T, _Val})
  when T =:= map;
       T =:= list;
       T =:= array;
       %% Raw binary data is not UTF-8 encoded.
       T =:= binary ->
    cannot_convert.

amqp_encode(Data, Acc0) ->
    Bin = amqp10_framing:encode_bin(Data),
    Acc = setelement(5, Acc0, [Bin | element(5, Acc0)]),
    setelement(7, Acc, ?CONTENT_TYPE_AMQP).

add_response_topic(AmqpTopic, PublishProperties) ->
    MqttTopic = rabbit_mqtt_util:amqp_to_mqtt(AmqpTopic),
    PublishProperties#{'Response-Topic' => MqttTopic}.
