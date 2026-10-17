-module(mc_mqtt).
-behaviour(mc).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/mc.hrl").

-define(CONTENT_TYPE_AMQP, <<"message/vnd.rabbitmq.amqp">>).

-export([
         init/1,
         size/1,
         x_header/2,
         x_headers/1,
         property/2,
         routing_headers/2,
         convert_to/3,
         convert_from/3,
         protocol_state/2,
         prepare/2
        ]).

-ifdef(TEST).
-export([amqp_to_utf8_string/1]).
-endif.

init(Msg = #mqtt_msg{qos = Qos,
                     props = Props}) ->
    Anns0 = #{?ANN_DURABLE => durable(Qos)},
    Anns1 = case Props of
                #{'Message-Expiry-Interval' := Seconds} ->
                    Anns0#{ttl => timer:seconds(Seconds),
                           ?ANN_TIMESTAMP => os:system_time(millisecond)};
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

convert_from(mc_amqp, Sections, Env) ->
    {Header, MsgAnns, AmqpProps, AppProps, PayloadRev, ContentType} =
    lists:foldl(
      fun(#'v1_0.header'{} = S, Acc) ->
              setelement(1, Acc, S);
         (_Ignore = #'v1_0.delivery_annotations'{}, Acc) ->
              Acc;
         (#'v1_0.message_annotations'{content = List}, Acc) ->
              setelement(2, Acc, List);
         (#'v1_0.properties'{} = S, Acc) ->
              setelement(3, Acc, S);
         (#'v1_0.application_properties'{content = List}, Acc) ->
              setelement(4, Acc, List);
         ({amqp_encoded_body_and_footer, Body}, Acc0) ->
              %% assertions
              [] = element(5, Acc0),
              undefined = element(6, Acc0),
              Acc = setelement(5, Acc0, [Body]),
              setelement(6, Acc, ?CONTENT_TYPE_AMQP);
         (#'v1_0.data'{content = C}, Acc) ->
              %% assertion
              undefined = element(6, Acc),
              setelement(5, Acc, [C | element(5, Acc)]);
         (Val, Acc0)
           when is_record(Val, 'v1_0.amqp_value') orelse
                is_record(Val, 'v1_0.amqp_sequence') ->
              IoData = amqp10_framing:encode_bin(Val),
              Acc = setelement(5, Acc0, [IoData | element(5, Acc0)]),
              setelement(6, Acc, ?CONTENT_TYPE_AMQP);
         (_Ignore = #'v1_0.footer'{}, Acc) ->
              Acc
      end, {undefined, [], undefined, [], [], undefined}, Sections),
    Qos = case Header of
              #'v1_0.header'{durable = false} ->
                  ?QOS_0;
              _ ->
                  ?QOS_1
          end,
    Props0 = case AmqpProps of
                 #'v1_0.properties'{reply_to = {utf8, Address}} ->
                     case amqp_response_topic(Address, MsgAnns, Env) of
                         error ->
                             #{};
                         AmqpTopic ->
                             MqttTopic = rabbit_mqtt_util:amqp_to_mqtt(AmqpTopic),
                             #{'Response-Topic' => MqttTopic}
                     end;
                 _ ->
                     #{}
             end,
    Props1 = case AmqpProps of
                 #'v1_0.properties'{correlation_id = {_Type, _Val} = Corr} ->
                     case correlation_id(Corr) of
                         undefined -> Props0;
                         CorrData -> Props0#{'Correlation-Data' => CorrData}
                     end;
                 _ ->
                     Props0
             end,
    Props2 = case ContentType of
                 undefined ->
                     case AmqpProps of
                         #'v1_0.properties'{content_type = {symbol, ContentType1}} ->
                             Props1#{'Content-Type' => rabbit_data_coercion:to_binary(ContentType1)};
                         _ ->
                             Props1
                     end;
                 _ ->
                     Props1#{'Content-Type' => ContentType}
             end,
    UserProp0 = lists:filtermap(fun({{symbol, <<"x-", _/binary>> = Key}, Val}) ->
                                        filter_map_amqp_to_utf8_string(Key, Val);
                                   (_) ->
                                        false
                                end, MsgAnns),
    %% "The keys of this map are restricted to be of type string" [AMQP 1.0 3.2.5]
    %% The decoder does not enforce this.
    UserProp1 = lists:filtermap(fun({{utf8, Key}, Val}) ->
                                        filter_map_amqp_to_utf8_string(Key, Val);
                                   (_) ->
                                        false
                                end, AppProps),
    Props = case UserProp0 ++ UserProp1 of
                [] -> Props2;
                UserProp -> Props2#{'User-Property' => UserProp}
            end,
    Payload = lists:reverse(PayloadRev),
    #mqtt_msg{retain = false,
              qos = Qos,
              dup = false,
              props = drop_invalid_properties(Props),
              payload = Payload};
convert_from(mc_amqpl, #content{properties = PBasic,
                                payload_fragments_rev = PFR},
            _Env) ->
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
    %% A header with this name but the wrong type falls through to generic handling below.
    {P1, H3} = case lists:keytake(<<"x-reply-to-topic">>, 1, H1) of
                   {value, {_, longstr, Topic}, H2} ->
                       {P0#{'Response-Topic' => rabbit_mqtt_util:amqp_to_mqtt(Topic)}, H2};
                   _ ->
                       {P0, H1}
               end,
    {P2, H} = case is_binary(CorrId) of
                  true ->
                      {P1#{'Correlation-Data' => CorrId}, H3};
                  false ->
                      case lists:keytake(<<"x-correlation-id">>, 1, H3) of
                          {value, {_, longstr, Corr}, H4} ->
                              {P1#{'Correlation-Data' => Corr}, H4};
                          _ ->
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
              payload = lists:reverse(PFR),
              props = drop_invalid_properties(P)};
convert_from(_SourceProto, _, _) ->
    not_implemented.

%% MQTT limits Binary Data and UTF-8 strings to 65,535 bytes, and UTF-8 strings
%% must be well-formed without U+0000 [v5 1.5.4, 1.5.6]. AMQP enforces neither.
-spec drop_invalid_properties(properties()) -> properties().
drop_invalid_properties(Props) ->
    maps:filtermap(fun drop_invalid_property/2, Props).

drop_invalid_property('Correlation-Data', Val) ->
    byte_size(Val) =< ?TWO_BYTE_INTEGER_MAX;
drop_invalid_property(Key, Val)
  when Key =:= 'Content-Type';
       Key =:= 'Response-Topic' ->
    is_valid_utf8_string(Val);
drop_invalid_property('User-Property', Pairs) ->
    case [Pair || {Name, Val} = Pair <- Pairs,
                  byte_size(Name) =< ?TWO_BYTE_INTEGER_MAX,
                  byte_size(Val) =< ?TWO_BYTE_INTEGER_MAX] of
        [] -> false;
        Kept -> {true, Kept}
    end;
drop_invalid_property(_Key, _Val) ->
    true.

amqp_response_topic(Address, MsgAnns, Env) ->
    case is_jms_reply_to_topic(MsgAnns) of
        true ->
            Address;
        false ->
            MqttX = maps:get(mqtt_x, Env, ?DEFAULT_MQTT_EXCHANGE),
            case Address of
                <<"/exchanges/",
                  MqttX:(byte_size(MqttX))/binary,
                  "/",
                  RoutingKeyQuoted/binary>> ->
                    try cow_uri:urldecode(RoutingKeyQuoted)
                    catch error:_ -> error
                    end;
                _ ->
                    error
            end
    end.

%% A JMS client (e.g. Qpid JMS) declares the JMSReplyTo destination type via the
%% x-opt-jms-reply-to message annotation [amqp-bindmap-jms-v1.0-wd10].
is_jms_reply_to_topic(MsgAnns) ->
    case lists:keyfind({symbol, <<"x-opt-jms-reply-to">>}, 1, MsgAnns) of
        {_, {byte, Val}} when Val =:= 1 orelse %% JMS Topic
                              Val =:= 3 -> %% JMS TemporaryTopic
            true;
        _ ->
            false
    end.

convert_to(?MODULE, Msg, _Env) ->
    Msg;
convert_to(mc_amqp, #mqtt_msg{qos = Qos,
                              props = Props,
                              payload = Payload}, Env) ->
    S0 = [#'v1_0.data'{content = Payload}],
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
                                {MAnns, [{{utf8, Name}, {utf8, Val}} | AProps],
                                 M#{Name => true}}
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
                     case mc_util:urn_string_to_uuid(Corr) of
                         {ok, MsgUUID} ->
                             {uuid, MsgUUID};
                         _ ->
                             {binary, Corr}
                     end;
                 _ ->
                     undefined
             end,
    ReplyTo = case Props of
                  #{'Response-Topic' := MqttTopic} ->
                      Exchange = maps:get(mqtt_x, Env, ?DEFAULT_MQTT_EXCHANGE),
                      Topic = rabbit_mqtt_util:mqtt_to_amqp(MqttTopic),
                      TopicQuoted = uri_string:quote(Topic),
                      %% We assume here that Exchange doesn't contain characters
                      %% that need to be quoted. This is a reasonable assumption
                      %% given that amq.topic is the default MQTT topic exchange.
                      Address = <<"/exchanges/", Exchange/binary, "/", TopicQuoted/binary>>,
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
    S = [#'v1_0.header'{durable = durable(Qos)} | S3],
    mc_amqp:convert_from(mc_amqp, S, Env);
convert_to(mc_amqpl, #mqtt_msg{qos = Qos,
                               props = Props,
                               payload = Payload}, _Env) ->
    DelMode = case Qos of
                  ?QOS_0 -> 1;
                  ?QOS_1 -> 2
              end,
    ContentType = case Props of
                      #{'Content-Type' := ContType}
                        when ?IS_SHORTSTR_LEN(ContType)->
                          case mc_util:utf8_string_is_ascii(ContType) of
                              true ->
                                  ContType;
                              false ->
                                  undefined
                          end;
                      _ ->
                          undefined
                  end,
    Hs0 = case Props of
              #{'User-Property' := UserProperty} ->
                  lists:filtermap(
                    fun({Name, Value})
                          when ?IS_SHORTSTR_LEN(Name) ->
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
    %% In practice, when converting from mc_mqtt to mc_amqpl, Payload will
    %% be a single binary, in which case iolist_to_binary/1 is cheap.
    PFR = [iolist_to_binary(Payload)],
    #content{class_id = 60,
             properties = BP,
             properties_bin = none,
             payload_fragments_rev = PFR};
convert_to(_TargetProto, #mqtt_msg{}, _Env) ->
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

x_headers(#mqtt_msg{props = #{'User-Property' := UserProp}}) ->
    #{Key => {utf8, Val} || {<<"x-", _/binary>> = Key, Val} <- UserProp};
x_headers(#mqtt_msg{}) ->
    #{}.

property(correlation_id, #mqtt_msg{props = #{'Correlation-Data' := Corr}}) ->
    case mc_util:urn_string_to_uuid(Corr) of
        {ok, UUId} ->
            {uuid, UUId};
        _ ->
            {binary, Corr}
    end;
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
                               topic = Topic,
                               qos = Qos0}, Anns) ->
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
                    case maps:get(?ANN_TIMESTAMP, Anns, undefined) of
                        undefined ->
                            Props2;
                        Timestamp ->
                            SourceProtocolIsMqtt = Topic =/= undefined,
                            %% Only if source protocol is MQTT we know that
                            %% timestamp was set by the server.
                            case SourceProtocolIsMqtt of
                                false ->
                                    Props2;
                                true ->
                                    %% "The PUBLISH packet sent to a Client by
                                    %% the Server MUST contain a
                                    %% Message Expiry Interval set to the received
                                    %% value minus the time that
                                    %% the Application Message has been waiting
                                    %% in the Server" [MQTT-3.3.2-6]
                                    WaitingMillis0 = os:system_time(millisecond) - Timestamp,
                                    %% For a delayed Will Message, the waiting
                                    %% time starts when the Will Message was published.
                                    WaitingMillis = WaitingMillis0 - WillDelay * 1000,
                                    MEIMillis = max(0, Ttl - WaitingMillis),
                                    Props2#{'Message-Expiry-Interval' => MEIMillis div 1000}
                            end
                    end
            end,
    [RoutingKey | _] = maps:get(?ANN_ROUTING_KEYS, Anns),
    %% We rely on the mc annotation to tell whether the message is durable because if
    %% the message was originally sent with AMQP, the AMQP header isn't stored on disk.
    Qos = case Anns of
              #{?ANN_DURABLE := false} ->
                  ?QOS_0;
              #{?ANN_DURABLE := true} ->
                  ?QOS_1;
              _ ->
                  %% If the mc durable annotation isn't set, the message might be durable
                  %% or not depending on whether the message was sent before or after
                  %% https://github.com/rabbitmq/rabbitmq-server/pull/11012 (3.13.2)
                  %% Hence, we rely on the QoS from the mqtt_msg.
                  Qos0
          end,
    Msg#mqtt_msg{qos = Qos,
                 topic = rabbit_mqtt_util:amqp_to_mqtt(RoutingKey),
                 props = Props}.

prepare(_For, #mqtt_msg{} = Msg) ->
    Msg.

correlation_id({uuid, UUID}) ->
    mc_util:uuid_to_urn_string(UUID);
correlation_id({ulong, N}) ->
    integer_to_binary(N);
correlation_id({T, Bin})
  when T =:= binary;
       T =:= utf8 ->
    Bin;
%% The AMQP 1.0 decoder does not enforce the `message-id` types.
correlation_id(_) ->
    undefined.

%% Translates AMQP 0.9.1 headers to MQTT 5.0 User Properties if
%% both the name and the value are convertible to a UTF-8 String.
-spec amqpl_header_to_user_property(rabbit_framing:amqp_table()) ->
    user_property().
amqpl_header_to_user_property(Table) ->
    lists:filtermap(fun({K, _T, _V} = Field) ->
                            mc_util:is_utf8_no_null(K) andalso
                            amqpl_field_to_utf8_string_pair(Field)
                    end, Table).

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
%% Catch-all for field types not representable as a UTF-8 string (array, table, binary, decimal, ...).
amqpl_field_to_utf8_string_pair(_) ->
    false.

%% A value over the MQTT limit is dropped anyway; check the cheap bound first.
-spec is_valid_utf8_string(binary()) -> boolean().
is_valid_utf8_string(Bin) ->
    byte_size(Bin) =< ?TWO_BYTE_INTEGER_MAX andalso mc_util:is_utf8_no_null(Bin).

%% A User Property name is itself a UTF-8 Encoded String [MQTT-1.5.4-1/-2].
filter_map_amqp_to_utf8_string(Key, TypeVal) ->
    case is_valid_utf8_string(Key) of
        true ->
            case amqp_to_utf8_string(TypeVal) of
                cannot_convert -> false;
                String -> {true, {Key, String}}
            end;
        false ->
            false
    end.

%% The AMQP 1.0 decoder does not validate `utf8`/`symbol` content.
amqp_to_utf8_string({T, Val})
  when (T =:= utf8 orelse T =:= symbol) andalso is_binary(Val) ->
    case is_valid_utf8_string(Val) of
        true -> Val;
        false -> cannot_convert
    end;
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
amqp_to_utf8_string(true) ->
    <<"true">>;
amqp_to_utf8_string(false) ->
    <<"false">>;
%% Catch-all for primitive types not worth representing as a UTF-8 string (uuid, char, arrays, ...).
amqp_to_utf8_string(_Other) ->
    cannot_convert.

durable(?QOS_0) -> false;
durable(?QOS_1) -> true.
