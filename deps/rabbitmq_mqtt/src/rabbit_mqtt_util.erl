%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_util).

-include_lib("rabbit_common/include/resource.hrl").
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-export([queue_name_bin/2,
         qos_from_queue_name/2,
         env/1,
         table_lookup/2,
         path_for/2,
         path_for/3,
         vhost_name_to_table_name/1,
         init_sparkplug/0,
         mqtt_to_amqp/1,
         amqp_to_mqtt/1,
         truncate_binary/2,
         ip_address_to_binary/1
        ]).

-define(MAX_TOPIC_TRANSLATION_CACHE_SIZE, 12).
-define(SPARKPLUG_MP_MQTT_TO_AMQP, sparkplug_mp_mqtt_to_amqp).
-define(SPARKPLUG_MP_AMQP_TO_MQTT, sparkplug_mp_amqp_to_mqtt).

-spec queue_name_bin(binary(), qos() | will) ->
    binary().
queue_name_bin(ClientId, will) ->
    <<"mqtt-will-", ClientId/binary>>;
queue_name_bin(ClientId, QoS) ->
    Prefix = queue_name_prefix(ClientId),
    queue_name0(Prefix, QoS).

queue_name0(Prefix, ?QOS_0) ->
    <<Prefix/binary, "0">>;
queue_name0(Prefix, ?QOS_1) ->
    <<Prefix/binary, "1">>.

-spec qos_from_queue_name(rabbit_amqqueue:name(), binary()) ->
    qos() | no_consuming_queue.
qos_from_queue_name(#resource{name = Name}, ClientId) ->
    Prefix = queue_name_prefix(ClientId),
    PrefixSize = erlang:byte_size(Prefix),
    case Name of
        <<Prefix:PrefixSize/binary, "0">> ->
            ?QOS_0;
        <<Prefix:PrefixSize/binary, "1">> ->
            ?QOS_1;
        _ ->
            no_consuming_queue
    end.

queue_name_prefix(ClientId) ->
    <<"mqtt-subscription-", ClientId/binary, "qos">>.

-spec init_sparkplug() -> ok.
init_sparkplug() ->
    case env(sparkplug) of
        true ->
            {ok, M2A_SpRe} = re:compile("^sp[AB]v\\d+\\.\\d+/"),
            {ok, A2M_SpRe} = re:compile("^sp[AB]v\\d+___\\d+\\."),
            ok = persistent_term:put(?SPARKPLUG_MP_MQTT_TO_AMQP, M2A_SpRe),
            ok = persistent_term:put(?SPARKPLUG_MP_AMQP_TO_MQTT, A2M_SpRe);
        _ ->
            ok
    end.

-spec mqtt_to_amqp(topic()) -> topic().
mqtt_to_amqp(Topic) ->
    T = case persistent_term:get(?SPARKPLUG_MP_MQTT_TO_AMQP, no_sparkplug) of
            no_sparkplug ->
                Topic;
            M2A_SpRe ->
                case re:run(Topic, M2A_SpRe) of
                    nomatch ->
                        Topic;
                    {match, _} ->
                        string:replace(Topic, ".", "___", leading)
                end
        end,
    cached(mta_cache, fun to_amqp/1, T).

-spec amqp_to_mqtt(topic()) -> topic().
amqp_to_mqtt(Topic) ->
    T = cached(atm_cache, fun to_mqtt/1, Topic),
    case persistent_term:get(?SPARKPLUG_MP_AMQP_TO_MQTT, no_sparkplug) of
        no_sparkplug ->
            T;
        A2M_SpRe ->
            case re:run(Topic, A2M_SpRe) of
                nomatch ->
                    T;
                {match, _} ->
                    T1 = string:replace(T, "___", ".", leading),
                    erlang:iolist_to_binary(T1)
            end
    end.

cached(CacheName, Fun, Arg) ->
    Cache = case get(CacheName) of
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

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator

to_amqp(T0) ->
    T1 = string:replace(T0, "/", ".", all),
    T2 = string:replace(T1, "+", "*", all),
    erlang:iolist_to_binary(T2).

to_mqtt(T0) ->
    T1 = string:replace(T0, "*", "+", all),
    T2 = string:replace(T1, ".", "/", all),
    erlang:iolist_to_binary(T2).

-spec env(atom()) -> any().
env(Key) ->
    case application:get_env(?APP_NAME, Key) of
        {ok, Val} -> coerce_env_value(Key, Val);
        undefined -> undefined
    end.

coerce_env_value(default_pass, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(default_user, Val) -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(vhost, Val)        -> rabbit_data_coercion:to_binary(Val);
coerce_env_value(_, Val)            -> Val.

-spec table_lookup(rabbit_framing:amqp_table() | undefined,  binary()) ->
    tuple() | undefined.
table_lookup(undefined, _Key) ->
    undefined;
table_lookup(Table, Key) ->
    rabbit_misc:table_lookup(Table, Key).

vhost_name_to_dir_name(VHost) ->
    vhost_name_to_dir_name(VHost, ".ets").
vhost_name_to_dir_name(VHost, Suffix) ->
    <<Num:128>> = erlang:md5(VHost),
    "mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num]) ++ Suffix.

-spec path_for(file:name_all(), rabbit_types:vhost()) -> file:filename_all().
path_for(Dir, VHost) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost)).

-spec path_for(file:name_all(), rabbit_types:vhost(), string()) -> file:filename_all().
path_for(Dir, VHost, Suffix) ->
  filename:join(Dir, vhost_name_to_dir_name(VHost, Suffix)).

-spec vhost_name_to_table_name(rabbit_types:vhost()) ->
    atom().
vhost_name_to_table_name(VHost) ->
    <<Num:128>> = erlang:md5(VHost),
    list_to_atom("rabbit_mqtt_retained_" ++ rabbit_misc:format("~36.16.0b", [Num])).

-spec truncate_binary(binary(), non_neg_integer()) -> binary().
truncate_binary(Bin, Size)
  when is_binary(Bin) andalso byte_size(Bin) =< Size ->
    Bin;
truncate_binary(Bin, Size)
  when is_binary(Bin) ->
    binary:part(Bin, 0, Size).

-spec ip_address_to_binary(inet:ip_address()) -> binary().
ip_address_to_binary(IpAddress) ->
    list_to_binary(inet:ntoa(IpAddress)).
