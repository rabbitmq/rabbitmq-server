-module(mc_mqtt).
-behaviour(mc).

-include("rabbit_mqtt_packet.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

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
    {Header, _MsgAnns, AmqpProps, PayloadRev} =
    lists:foldl(
      fun(#'v1_0.header'{} = S, Acc) ->
              setelement(1, Acc, S);
         (#'v1_0.message_annotations'{} = S, Acc) ->
              setelement(2, Acc, S);
         (#'v1_0.properties'{} = S, Acc) ->
              setelement(3, Acc, S);
         (#'v1_0.application_properties'{}, Acc) ->
              Acc;
         (#'v1_0.footer'{}, Acc) ->
              Acc;
         (#'v1_0.data'{content = C}, Acc) ->
              setelement(4, Acc, [C | element(4, Acc)]);
         (BodySect, Acc)
           when is_record(BodySect, 'v1_0.amqp_sequence') orelse
                is_record(BodySect, 'v1_0.amqp_value') ->
              %% TODO How to indicate to MQTT client that payload is encoded by AMQP type system?
              %% There is no registered message/amqp content type or encoding.
              Bin = amqp10_framing:encode_bin(BodySect),
              setelement(4, Acc, [Bin | element(4, Acc)])
      end, {undefined, undefined, undefined, []}, Sections),
    Qos = case Header of
              #'v1_0.header'{durable = true} ->
                  ?QOS_1;
              _ ->
                  ?QOS_0
          end,
    %% TODO convert #'v1_0.properties'{reply_to} to Response-Topic
    Props0 = case AmqpProps of
                 #'v1_0.properties'{correlation_id = {_Type, _Val} = Corr} ->
                     #{'Correlation-Data' => correlation_id(Corr)};
                 _ ->
                     #{}
             end,
    Props = case AmqpProps of
                #'v1_0.properties'{content_type = {symbol, ContentType}} ->
                    Props0#{'Content-Type' => rabbit_data_coercion:to_binary(ContentType)};
                _ ->
                    Props0
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
                           ({<<"x-", _/binary>> = Name, Val}, {MAnns, AProps, M}) ->
                                {[{{utf8, Name}, {utf8, Val}} | MAnns], AProps, M#{Name => true}};
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
                          %%TODO MQTT Content Type is UTF-8 whereas
                          %% AMQP Content Type is only ASCII
                          {symbol, ContType};
                      _ ->
                          undefined
                  end,
    CorrId = case Props of
                 #{'Correlation-Data' := Corr} ->
                     case mc_util:is_utf8_no_null(Corr) of
                         true ->
                             {utf8, Corr};
                         false ->
                             {binary, Corr}
                     end;
                 _ ->
                     undefined
             end,
    %% TODO Translate MQTT Response-Topic to AMQP topic.
    %% If operator did not mofidy mqtt.exchange, set reply-to address to "/topic/" RK.
    %% If operator modified mqtt.exchange, set reply-to address to "/exchange/" X "/" RK.
    % case Props of
    %     #{'Response-Topic' := Topic} ->
    %         rabbit_mqtt_util:mqtt_to_amqp(Topic)
    S2 = case {ContentType, CorrId} of
             {undefined, undefined} ->
                 S1;
             _ ->
                 [#'v1_0.properties'{content_type = ContentType,
                                     correlation_id = CorrId} | S1]
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
                  lists:map(fun({Name, Value}) ->
                                    {Name, longstr, Value}
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
                    headers = Hs,
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

protocol_state(Msg = #mqtt_msg{}, _Anns) ->
    Msg.

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
    lists:filtermap(fun amqpl_field_to_string_pair/1, Table).

amqpl_field_to_string_pair({K, longstr, V}) ->
    case mc_util:is_utf8_no_null(V) of
        true -> {true, {K, V}};
        false -> false
    end;
amqpl_field_to_string_pair({K, T, V})
  when T =:= byte;
       T =:= unsignedbyte;
       T =:= short;
       T =:= unsignedshort;
       T =:= signedint;
       T =:= unsignedint;
       T =:= long;
       T =:= timestamp ->
    {true, {K, integer_to_binary(V)}};
amqpl_field_to_string_pair({K, T, V})
  when T =:= float;
       T =:= double ->
    {true, {K, float_to_binary(V)}};
amqpl_field_to_string_pair({K, void, _V}) ->
    {true, {K, <<>>}};
amqpl_field_to_string_pair({K, bool, V}) ->
    {true, {K, atom_to_binary(V)}};
amqpl_field_to_string_pair({_K, T, _V})
  when T =:= array;
       T =:= table;
       %% Raw binary data is not UTF-8 encoded.
       T =:= binary ->
    false.
