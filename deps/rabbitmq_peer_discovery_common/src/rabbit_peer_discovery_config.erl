%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2017 Pivotal Software, Inc. All rights reserved.
%%

-module(rabbit_peer_discovery_config).

-include("rabbit_peer_discovery.hrl").

-export([get/3, config_map/1]).

%%
%% API
%%

-spec get(Key :: atom(),
          Mapping :: #{atom() => #peer_discovery_config_entry_meta{}},
          Config  :: #{atom() => peer_discovery_config_value()}) -> peer_discovery_config_value().

get(Key, Mapping, Config) ->
    case maps:is_key(Key, Mapping) of
        false ->
            rabbit_log:error("Key ~s is not found in peer discovery config mapping ~p!", [Key, Mapping]),
            throw({badkey, Key});
        true  ->
            get_with_entry_meta(Key, maps:get(Key, Mapping), Config)
    end.

-spec config_map(atom()) -> #{atom() => peer_discovery_config_value()}.

config_map(BackendConfigKey) ->
    case application:get_env(rabbit, cluster_formation) of
        undefined         -> #{};
        {ok, ClusterFormation} ->
            case proplists:get_value(BackendConfigKey, ClusterFormation) of
                undefined -> #{};
                Proplist  -> maps:from_list(Proplist)
            end
    end.

%%
%% Implementation
%%

-spec get_with_entry_meta(Key       :: atom(),
                          EntryMeta :: #peer_discovery_config_entry_meta{},
                          Map       :: #{atom() => peer_discovery_config_value()}) -> peer_discovery_config_value().

get_with_entry_meta(Key, #peer_discovery_config_entry_meta{env_variable = EV,
                                                           default_value = Default,
                                                           type    = Type}, Map) ->
    normalize(Type, get_from_env_variable_or_map(Map, EV, Key, Default)).


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return the a value from the OS environment value, or the provided
%% map if OS env var is not set, or the
%% default value.
%% @end
%%--------------------------------------------------------------------
-spec get_from_env_variable_or_map(Map :: map(), OSKey :: string(), AppKey :: atom(),
                                   Default :: atom() | integer() | string())
                                  -> atom() | integer() | string().
get_from_env_variable_or_map(Map, OSKey, AppKey, Default) ->
  case rabbit_peer_discovery_util:getenv(OSKey) of
    false -> maps:get(AppKey, Map, Default);
    Value -> Value
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Return the normalized value in as the proper data type
%% @end
%%--------------------------------------------------------------------
-spec normalize(Type  :: atom(),
                Value :: atom() | boolean() | integer() | string() | list()) ->
  atom() | integer() | string().
%% TODO: switch these to use delegate to rabbit_data_coercion:*
normalize(Type, Value) when Type =:= port ->
  rabbit_peer_discovery_util:parse_port(Value);
normalize(Type, Value) when Type =:= atom ->
  rabbit_peer_discovery_util:as_atom(Value);
normalize(Type, Value) when Type =:= list ->
  rabbit_data_coercion:to_list(Value);
normalize(Type, Value) when Type =:= integer ->
  rabbit_peer_discovery_util:as_integer(Value);
normalize(Type, Value) when Type =:= string ->
  rabbit_peer_discovery_util:as_string(Value);
normalize(Type, Value) when Type =:= proplist ->
  rabbit_peer_discovery_util:as_proplist(Value);
normalize(Type, Value) when Type =:= map ->
  rabbit_peer_discovery_util:as_map(Value).
