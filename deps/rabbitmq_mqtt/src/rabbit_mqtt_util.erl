%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_util).

-include("rabbit_mqtt.hrl").

-export([subcription_queue_name/1,
         gen_client_id/0,
         env/1,
         table_lookup/2,
         path_for/2,
         path_for/3,
         vhost_name_to_table_name/1,
         get_topic_translation_funs/0
        ]).

-define(MAX_TOPIC_TRANSLATION_CACHE_SIZE, 12).

subcription_queue_name(ClientId) ->
    Base = "mqtt-subscription-" ++ ClientId ++ "qos",
    {list_to_binary(Base ++ "0"), list_to_binary(Base ++ "1")}.

cached(CacheName, Fun, Arg) ->
    Cache =
        case get(CacheName) of
            undefined ->
                [];
            Other ->
                Other
        end,
    case lists:keyfind(Arg, 1, Cache) of
        {_, V} ->
            V;
        false ->
            V = Fun(Arg),
            CacheTail = lists:sublist(Cache, ?MAX_TOPIC_TRANSLATION_CACHE_SIZE - 1),
            put(CacheName, [{Arg, V} | CacheTail]),
            V
    end.

to_amqp(T0) ->
    T1 = string:replace(T0, "/", ".", all),
    T2 = string:replace(T1, "+", "*", all),
    erlang:iolist_to_binary(T2).

to_mqtt(T0) ->
    T1 = string:replace(T0, "*", "+", all),
    T2 = string:replace(T1, ".", "/", all),
    erlang:iolist_to_binary(T2).

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator
get_topic_translation_funs() ->
    SparkplugB = env(sparkplug),
    ToAmqpFun = fun(Topic) ->
                        cached(mta_cache, fun to_amqp/1, Topic)
                end,
    ToMqttFun = fun(Topic) ->
                        cached(atm_cache, fun to_mqtt/1, Topic)
                end,
    {M2AFun, A2MFun} = case SparkplugB of
        true ->
            {ok, M2A_SpRe} = re:compile("^sp[AB]v\\d+\\.\\d+/"),
            {ok, A2M_SpRe} = re:compile("^sp[AB]v\\d+___\\d+\\."),
            M2A = fun(T0) ->
                case re:run(T0, M2A_SpRe) of
                    nomatch ->
                        ToAmqpFun(T0);
                    {match, _} ->
                        T1 = string:replace(T0, ".", "___", leading),
                        ToAmqpFun(T1)
                end
            end,
            A2M = fun(T0) ->
                case re:run(T0, A2M_SpRe) of
                    nomatch ->
                        ToMqttFun(T0);
                    {match, _} ->
                        T1 = ToMqttFun(T0),
                        T2 = string:replace(T1, "___", ".", leading),
                        erlang:iolist_to_binary(T2)
                end
            end,
            {M2A, A2M};
        _ ->
            M2A = fun(T) ->
                ToAmqpFun(T)
            end,
            A2M = fun(T) ->
                ToMqttFun(T)
            end,
            {M2A, A2M}
    end,
    {ok, {mqtt2amqp_fun, M2AFun}, {amqp2mqtt_fun, A2MFun}}.

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
