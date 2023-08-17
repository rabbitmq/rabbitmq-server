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
    {Header, MsgAnns, AmqpProps, PayloadRev} =
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
    Props0 = case MsgAnns of
                 #'v1_0.message_annotations'{
                    content = #{{symbol, <<"x-opt-reply-to-topic">>} := {utf8, Topic}}} ->
                     #{'Response-Topic' => rabbit_mqtt_util:amqp_to_mqtt(Topic)};
                 _ ->
                     #{}
             end,
    Props1 = case AmqpProps of
                 #'v1_0.properties'{correlation_id = {_Type, _Val} = Corr} ->
                     Props0#{'Correlation-Data' => correlation_id(Corr)};
                 _ ->
                     Props0
             end,
    Props = case AmqpProps of
                #'v1_0.properties'{content_type = {symbol, ContentType}} ->
                    Props1#{'Content-Type' => rabbit_data_coercion:to_binary(ContentType)};
                _ ->
                    Props1
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
               headers = Headers0,
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
    Headers = case Headers0 of
                  undefined -> [];
                  _ -> Headers0
              end,
    P1 = case lists:keyfind(<<"x-opt-reply-to-topic">>, 1, Headers) of
             {_, longstr, Topic} ->
                 P0#{'Response-Topic' => rabbit_mqtt_util:amqp_to_mqtt(Topic)};
             _ ->
                 P0
         end,
    P2 = case is_binary(CorrId) of
             true ->
                 P1#{'Correlation-Data' => CorrId};
             false ->
                 case lists:keyfind(<<"x-correlation-id">>, 1, Headers) of
                     {_, longstr, Corr} ->
                         P1#{'Correlation-Data' => Corr};
                     _ ->
                         P1
                 end
         end,
    P = case is_binary(Expiration) of
            true ->
                Millis = binary_to_integer(Expiration),
                P2#{'Message-Expiry-Interval' => Millis div 1000};
            false ->
                P2
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
    Header = #'v1_0.header'{durable = Qos > 0},
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
    AmqpProps = #'v1_0.properties'{content_type = ContentType,
                                   correlation_id = CorrId},
    AppData = #'v1_0.data'{content = Payload},
    Sections = case Props of
                   #{'Response-Topic' := Topic} ->
                       MsgAnns = #'v1_0.message_annotations'{
                                    content = [{{symbol, <<"x-opt-reply-to-topic">>},
                                                {utf8, rabbit_mqtt_util:mqtt_to_amqp(Topic)}}]},
                       [Header, MsgAnns, AmqpProps, AppData];
                   _ ->
                       [Header, AmqpProps, AppData]
               end,
    mc_amqp:convert_from(mc_amqp, Sections);
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
              #{'Response-Topic' := Topic} ->
                  [{<<"x-opt-reply-to-topic">>, longstr, rabbit_mqtt_util:mqtt_to_amqp(Topic)}];
              _ ->
                  []
          end,
    {CorrId, Hs} = case Props of
                       #{'Correlation-Data' := Corr} ->
                           case mc_util:is_valid_shortstr(Corr) of
                               true ->
                                   {Corr, Hs0};
                               false ->
                                   {undefined, [{<<"x-correlation-id">>, longstr, Corr} | Hs0]}
                           end;
                       _ ->
                           {undefined, Hs0}
                   end,
    Expiration = case Props of
                     #{'Message-Expiry-Interval' := Seconds} ->
                         integer_to_binary(timer:seconds(Seconds));
                     _ ->
                         undefined
                 end,
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
