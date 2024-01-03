%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_oauth2_config).

-include_lib("oauth2_client/include/oauth2_client.hrl").

-define(APP, rabbitmq_auth_backend_oauth2).
-define(DEFAULT_PREFERRED_USERNAME_CLAIMS, [<<"sub">>, <<"client_id">>]).

-define(TOP_RESOURCE_SERVER_ID, application:get_env(?APP, resource_server_id)).
%% scope aliases map "role names" to a set of scopes


-export([
  add_signing_key/2, add_signing_key/3, replace_signing_keys/1, replace_signing_keys/2,
  get_signing_keys/0, get_signing_keys/1, get_signing_key/2,
  get_key_config/0, get_key_config/1, get_jwks_url/1, get_default_resource_server_id/0,
  get_oauth_provider_for_resource_server_id/2,
  get_allowed_resource_server_ids/0, find_audience_in_resource_server_ids/1,
  is_verify_aud/0, is_verify_aud/1,
  get_additional_scopes_key/0, has_additional_scopes_key/1, get_additional_scopes_key/1,
  get_default_preferred_username_claims/0, get_preferred_username_claims/0, get_preferred_username_claims/1,
  get_scope_prefix/0, get_scope_prefix/1,
  get_resource_server_type/0, get_resource_server_type/1,
  has_scope_aliases/1, get_scope_aliases/1
  ]).

-spec get_default_preferred_username_claims() -> list().
get_default_preferred_username_claims() ->
  ?DEFAULT_PREFERRED_USERNAME_CLAIMS.

-spec get_preferred_username_claims() -> list().
get_preferred_username_claims() ->
  case application:get_env(?APP, preferred_username_claims) of
    {ok, Value} ->  append_or_return_default(Value, ?DEFAULT_PREFERRED_USERNAME_CLAIMS);
    _ -> ?DEFAULT_PREFERRED_USERNAME_CLAIMS
  end.
-spec get_preferred_username_claims(binary()) -> list().
get_preferred_username_claims(ResourceServerId) -> get_preferred_username_claims(get_default_resource_server_id(), ResourceServerId).
get_preferred_username_claims(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId -> get_preferred_username_claims();
get_preferred_username_claims(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  case proplists:get_value(preferred_username_claims, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}))) of
    undefined -> get_preferred_username_claims();
    Value -> append_or_return_default(Value, ?DEFAULT_PREFERRED_USERNAME_CLAIMS)
  end.

-type key_type() :: json | pem | map.
-spec add_signing_key(binary(), {key_type(), binary()} ) -> {ok, map()} | {error, term()}.
add_signing_key(KeyId, Key) ->
  LockId = lock(),
  try do_add_signing_key(KeyId, Key) of
    V -> V
  after
    unlock(LockId)
  end.

-spec add_signing_key(binary(), binary(), {key_type(), binary()}) -> {ok, map()} | {error, term()}.
add_signing_key(ResourceServerId, KeyId, Key) ->
  LockId = lock(),
  try do_add_signing_key(ResourceServerId, KeyId, Key) of
    V -> V
  after
    unlock(LockId)
  end.

do_add_signing_key(KeyId, Key) ->
  do_replace_signing_keys(maps:put(KeyId, Key, get_signing_keys())).

do_add_signing_key(ResourceServerId, KeyId, Key) ->
  do_replace_signing_keys(ResourceServerId, maps:put(KeyId, Key, get_signing_keys(ResourceServerId))).

replace_signing_keys(SigningKeys) ->
  LockId = lock(),
  try do_replace_signing_keys(SigningKeys) of
    V -> V
  after
    unlock(LockId)
  end.

replace_signing_keys(ResourceServerId, SigningKeys) ->
  LockId = lock(),
  try do_replace_signing_keys(ResourceServerId, SigningKeys) of
    V -> V
  after
    unlock(LockId)
  end.

do_replace_signing_keys(SigningKeys) ->
  KeyConfig = application:get_env(?APP, key_config, []),
  KeyConfig1 = proplists:delete(signing_keys, KeyConfig),
  KeyConfig2 = [{signing_keys, SigningKeys} | KeyConfig1],
  application:set_env(?APP, key_config, KeyConfig2),
  rabbit_log:debug("Replacing signing keys  ~p", [ KeyConfig2]),
  SigningKeys.

do_replace_signing_keys(ResourceServerId, SigningKeys) ->
  do_replace_signing_keys(get_default_resource_server_id(), ResourceServerId, SigningKeys).
do_replace_signing_keys(TopResourceServerId, ResourceServerId, SigningKeys) when ResourceServerId =:= TopResourceServerId ->
  do_replace_signing_keys(SigningKeys);
do_replace_signing_keys(TopResourceServerId, ResourceServerId, SigningKeys) when ResourceServerId =/= TopResourceServerId ->
  ResourceServers = application:get_env(?APP, resource_servers, #{}),
  ResourceServer = maps:get(ResourceServerId, ResourceServers, []),
  KeyConfig0 = proplists:get_value(key_config, ResourceServer, []),
  KeyConfig1 = proplists:delete(signing_keys, KeyConfig0),
  KeyConfig2 = [{signing_keys, SigningKeys} | KeyConfig1],

  ResourceServer1 = proplists:delete(key_config, ResourceServer),
  ResourceServer2 = [{key_config, KeyConfig2} | ResourceServer1],

  ResourceServers1 = maps:put(ResourceServerId, ResourceServer2, ResourceServers),
  application:set_env(?APP, resource_servers, ResourceServers1),
  rabbit_log:debug("Replacing signing keys for ~p -> ~p", [ResourceServerId, ResourceServers1]),
  SigningKeys.

-spec get_signing_keys() -> map().
get_signing_keys() -> proplists:get_value(signing_keys, get_key_config(), #{}).

-spec get_signing_keys(binary()) -> map().
get_signing_keys(ResourceServerId) -> get_signing_keys(get_default_resource_server_id(), ResourceServerId).

get_signing_keys(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  get_signing_keys();
get_signing_keys(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  proplists:get_value(signing_keys, get_key_config(ResourceServerId), #{}).

-spec get_oauth_provider_for_resource_server_id(binary(), list()) -> {ok, oauth_provider()} | {error, any()}.

get_oauth_provider_for_resource_server_id(ResourceServerId, RequiredAttributeList) ->
  get_oauth_provider_for_resource_server_id(get_default_resource_server_id(), ResourceServerId, RequiredAttributeList).
get_oauth_provider_for_resource_server_id(TopResourceServerId, ResourceServerId, RequiredAttributeList) when ResourceServerId =:= TopResourceServerId ->
  oauth2_client:get_oauth_provider(RequiredAttributeList);
get_oauth_provider_for_resource_server_id(TopResourceServerId, ResourceServerId, RequiredAttributeList) when ResourceServerId =/= TopResourceServerId ->
  case proplists:get_value(oauth_provider_id, get_resource_server_props(ResourceServerId)) of
    undefined -> rabbit_log:error("Missing oauth_provider_id attribute for ResourceServer ~p", [ResourceServerId]);
    OauthProviderId -> oauth2_client:get_oauth_provider(OauthProviderId, RequiredAttributeList)
  end.

-spec get_key_config() -> list().
get_key_config() -> application:get_env(?APP, key_config, []).

-spec get_key_config(binary()) -> list().
get_key_config(ResourceServerId) -> get_key_config(get_default_resource_server_id(), ResourceServerId).
get_key_config(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  get_key_config();
get_key_config(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  proplists:get_value(key_config, get_resource_server_props(ResourceServerId), get_key_config()).

get_resource_server_props(ResourceServerId) ->
  ResourceServers = application:get_env(?APP, resource_servers, #{}),
  maps:get(ResourceServerId, ResourceServers, []).

get_signing_key(KeyId, ResourceServerId) -> get_signing_key(get_default_resource_server_id(), KeyId, ResourceServerId).

get_signing_key(TopResourceServerId, KeyId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  maps:get(KeyId, get_signing_keys(), undefined);
get_signing_key(TopResourceServerId, KeyId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  maps:get(KeyId, get_signing_keys(ResourceServerId), undefined).


get_jwks_url(ResourceServerId) ->
  rabbit_log:debug("get_jwks_url for resource-server_id ~p : ~p", [ResourceServerId,get_key_config(ResourceServerId)]),
  proplists:get_value(jwks_url, get_key_config(ResourceServerId)).

append_or_return_default(ListOrBinary, Default) ->
  case ListOrBinary of
    VarList when is_list(VarList) -> VarList ++ Default;
    VarBinary when is_binary(VarBinary) -> [VarBinary] ++ Default;
    _ -> Default
  end.

-spec get_default_resource_server_id() -> binary() | {error, term()}.
get_default_resource_server_id() ->
  case ?TOP_RESOURCE_SERVER_ID of
    undefined -> {error, missing_token_audience_and_or_config_resource_server_id };
    {ok, ResourceServerId} -> ResourceServerId
  end.

-spec get_allowed_resource_server_ids() -> list().
get_allowed_resource_server_ids() ->
  ResourceServers = application:get_env(?APP, resource_servers, #{}),
  rabbit_log:debug("ResourceServers: ~p", [ResourceServers]),
  ResourceServerIds = maps:fold(fun(K, V, List) -> List ++ [proplists:get_value(id, V, K)] end, [], ResourceServers),
  rabbit_log:debug("ResourceServersIds: ~p", [ResourceServerIds]),
  ResourceServerIds ++ case get_default_resource_server_id() of
       {error, _} -> [];
       ResourceServerId -> [ ResourceServerId ]
  end.

-spec find_audience_in_resource_server_ids(binary() | list()) -> {ok, binary()} | {error, term()}.
find_audience_in_resource_server_ids(Audience) when is_binary(Audience) ->
  find_audience_in_resource_server_ids(binary:split(Audience, <<" ">>, [global, trim_all]));
find_audience_in_resource_server_ids(AudList) when is_list(AudList) ->
  AllowedAudList = get_allowed_resource_server_ids(),
  %rabbit_log:debug("find_audience_in_resource_server_ids AudList:~p, AllowedAudList:~p",[AudList,AllowedAudList]),
  case intersection(AudList, AllowedAudList) of
   [One] -> {ok, One};
   [_One|_Tail] -> {error, only_one_resource_server_as_audience_found_many};
   [] -> {error, no_matching_aud_found}
  end.


-spec is_verify_aud() -> boolean().
is_verify_aud() -> application:get_env(?APP, verify_aud, true).

-spec is_verify_aud(binary()) -> boolean().
is_verify_aud(ResourceServerId) -> is_verify_aud(get_default_resource_server_id(), ResourceServerId).
is_verify_aud(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId -> is_verify_aud();
is_verify_aud(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  proplists:get_value(verify_aud, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}), []), is_verify_aud()).

-spec has_additional_scopes_key(binary()) -> boolean().
has_additional_scopes_key(ResourceServerId) -> has_additional_scopes_key(get_default_resource_server_id(), ResourceServerId).
has_additional_scopes_key(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  case application:get_env(?APP, extra_scopes_source, undefined) of
    undefined -> false;
    <<>> -> false;
    _ -> true
  end;
has_additional_scopes_key(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  case proplists:get_value(extra_scopes_source, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}), [])) of
    undefined -> has_additional_scopes_key(TopResourceServerId);
    <<>> -> has_additional_scopes_key(TopResourceServerId);
    _ -> true
  end.

-spec get_additional_scopes_key() -> binary() | undefined.
get_additional_scopes_key() -> application:get_env(?APP, extra_scopes_source, undefined).

-spec get_additional_scopes_key(binary()) -> binary()  | undefined .
get_additional_scopes_key(ResourceServerId) -> get_additional_scopes_key(get_default_resource_server_id(), ResourceServerId).
get_additional_scopes_key(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId -> get_additional_scopes_key();
get_additional_scopes_key(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  proplists:get_value(extra_scopes_source, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}), []),
    get_additional_scopes_key()).


-spec get_scope_prefix() -> binary().
get_scope_prefix() ->
  DefaultScopePrefix = erlang:iolist_to_binary([get_default_resource_server_id(), <<".">>]),
  application:get_env(?APP, scope_prefix, DefaultScopePrefix).

-spec get_scope_prefix(binary()) -> binary().
get_scope_prefix(ResourceServerId) -> get_scope_prefix(get_default_resource_server_id(), ResourceServerId).
get_scope_prefix(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId -> get_scope_prefix();
get_scope_prefix(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  case proplists:get_value(scope_prefix, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}), [])) of
    undefined -> case application:get_env(?APP, scope_prefix) of
                   undefined ->  <<ResourceServerId/binary, ".">>;
                   {ok, Prefix} -> Prefix
                 end;
    Prefix -> Prefix
  end.

-spec get_resource_server_type() -> binary().
get_resource_server_type() -> application:get_env(?APP, resource_server_type, <<>>).

-spec get_resource_server_type(binary()) -> binary().
get_resource_server_type(ResourceServerId) -> get_resource_server_type(get_default_resource_server_id(), ResourceServerId).
get_resource_server_type(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId -> get_resource_server_type();
get_resource_server_type(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  proplists:get_value(resource_server_type, maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}), []),
    get_resource_server_type()).

-spec has_scope_aliases(binary()) -> boolean().
has_scope_aliases(ResourceServerId) -> has_scope_aliases(get_default_resource_server_id(), ResourceServerId).
has_scope_aliases(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  case application:get_env(?APP, scope_aliases) of
    undefined -> false;
    _ -> true
  end;
has_scope_aliases(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  ResourceServerProps = maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}),[]),
  case proplists:is_defined(scope_aliases, ResourceServerProps) of
    true -> true;
    false ->  has_scope_aliases(TopResourceServerId)
  end.

-spec get_scope_aliases(binary()) -> map().
get_scope_aliases(ResourceServerId) -> get_scope_aliases(get_default_resource_server_id(), ResourceServerId).
get_scope_aliases(TopResourceServerId, ResourceServerId) when ResourceServerId =:= TopResourceServerId ->
  application:get_env(?APP, scope_aliases, #{});
get_scope_aliases(TopResourceServerId, ResourceServerId) when ResourceServerId =/= TopResourceServerId ->
  ResourceServerProps = maps:get(ResourceServerId, application:get_env(?APP, resource_servers, #{}),[]),
  proplists:get_value(scope_aliases, ResourceServerProps, get_scope_aliases(TopResourceServerId)).


intersection(List1, List2) ->
    [I || I <- List1, lists:member(I, List2)].

lock() ->
    Nodes   = rabbit_nodes:list_running(),
    Retries = rabbit_nodes:lock_retries(),
    LockId = case global:set_lock({oauth2_config_lock, rabbitmq_auth_backend_oauth2}, Nodes, Retries) of
        true  -> rabbitmq_auth_backend_oauth2;
        false -> undefined
    end,
    LockId.

unlock(LockId) ->
    Nodes = rabbit_nodes:list_running(),
    case LockId of
        undefined -> ok;
        Value     ->
          global:del_lock({oauth2_config_lock, Value}, Nodes)
    end,
    ok.
