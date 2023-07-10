-module(mc_mqtt).
-behaviour(mc).

-include("rabbit_mqtt_packet.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         init/1,
         init_amqp/1,
         size/1,
         x_header/2,
         routing_headers/2,
         convert/2,
         protocol_state/2,
         serialize/2
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

init_amqp(Sections)
  when is_list(Sections) ->
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
              payload = Payload}.

convert(?MODULE, Msg) ->
    Msg;
convert(mc_amqp, #mqtt_msg{qos = Qos,
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
    mc_amqp:init_amqp(Sections);
convert(mc_amqpl, #mqtt_msg{qos = Qos,
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
convert(_TargetProto, #mqtt_msg{}) ->
    not_implemented.

size(#mqtt_msg{payload = Payload,
               topic = Topic,
               props = Props}) ->
    PropsSize = maps:fold(fun size_prop/3, 0, Props),
    MetadataSize = PropsSize + byte_size(Topic),
    {MetadataSize, byte_size(Payload)}.

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

x_header(_Key, #mqtt_msg{} = Msg) ->
    {undefined, Msg}.

routing_headers(#mqtt_msg{}, _Opts) ->
    #{}.

protocol_state(Msg = #mqtt_msg{}, _Anns) ->
    Msg.

serialize(#mqtt_msg{}, _Anns) ->
    [].

correlation_id({uuid, UUID}) ->
    mc_util:uuid_to_string(UUID);
correlation_id({_T, Corr}) ->
    rabbit_data_coercion:to_binary(Corr).
