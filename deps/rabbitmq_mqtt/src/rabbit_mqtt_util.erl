%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_util).

-include("rabbit_mqtt.hrl").

-export([subcription_queue_name/1,
         mqtt2amqp/1,
         amqp2mqtt/1,
         gen_client_id/0,
         env/1,
         table_lookup/2,
         path_for/2,
         path_for/3,
         vhost_name_to_table_name/1,
         get_topic_translation_funs/0
        ]).

subcription_queue_name(ClientId) ->
    Base = "mqtt-subscription-" ++ ClientId ++ "qos",
    {list_to_binary(Base ++ "0"), list_to_binary(Base ++ "1")}.

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator
mqtt2amqp(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "/", ".", [global]),
                 "[\+]", "*", [global])).

get_topic_translation_funs() ->
    SparkplugB = env(sparkplug_b),
    ToAmqpFun = fun(T0) ->
                    T1 = string:replace(T0, "/", ".", all),
                    T2 = string:replace(T1, "+", "*", all),
                    erlang:iolist_to_binary(T2)
                end,
    Mqtt2AmqpFun = case SparkplugB of
                       true ->
                           fun(T0) ->
                               case {string:prefix(T0, "spAv1.0/"), string:prefix(T0, "spBv1.0/")} of
                                   {nomatch, nomatch} ->
                                       ToAmqpFun(T0);
                                   {_, _} ->
                                       T1 = string:replace(T0, ".", "_", leading),
                                       ToAmqpFun(T1)
                               end
                           end;
                       _ ->
                           fun(T) ->
                               ToAmqpFun(T)
                           end
                   end,
    {ok, {mqtt2amqp_fun, Mqtt2AmqpFun}, {amqp2mqtt_fun, fun amqp2mqtt/1}}.

amqp2mqtt(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "[\*]", "+", [global]),
                 "[\.]", "/", [global])).

gen_client_id() ->
    lists:nthtail(1, rabbit_guid:string(rabbit_guid:gen_secure(), [])).

env(Key) ->
    case application:get_env(rabbitmq_mqtt, Key) of
        {ok, Val} -> coerce_env_value(Key, Val);
        undefined -> undefined
    end.

%% TODO: move to rabbit_common
coerce_env_value(default_pass, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(default_user, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(exchange, Val)     -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(vhost, Val)        -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(_, Val)            -> Val.

table_lookup(undefined, _Key) ->
    undefined;
table_lookup(Table, Key) ->
    rabbit_misc:table_lookup(Table, Key).

vhost_name_to_dir_name(VHost) ->
    vhost_name_to_dir_name(VHost, ".ets").
vhost_name_to_dir_name(VHost, Suffix) ->
    <<Num:128>> = erlang:md5(VHost),
    "mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num]) ++ Suffix.

path_for(Dir, VHost) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost)).

path_for(Dir, VHost, Suffix) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost, Suffix)).


vhost_name_to_table_name(VHost) ->
  <<Num:128>> = erlang:md5(VHost),
  list_to_atom("rabbit_mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num])).
