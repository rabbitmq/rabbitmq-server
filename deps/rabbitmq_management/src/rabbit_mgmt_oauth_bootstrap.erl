%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_oauth_bootstrap).

-export([init/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------

init(Req0, State) ->
  bootstrap_oauth(rabbit_mgmt_headers:set_no_cache_headers(
     rabbit_mgmt_headers:set_common_permission_headers(Req0, ?MODULE), ?MODULE), State).

bootstrap_oauth(Req0, State) ->
  JSContent = oauth_initialize_if_required() ++ set_token_auth(Req0),
  {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"text/javascript; charset=utf-8">>}, JSContent, Req0), State}.

authSettings() ->
  EnableUAA = application:get_env(rabbitmq_management, enable_uaa, false),
  EnableOAUTH = application:get_env(rabbitmq_management, oauth_enabled, false),
  Data = case EnableOAUTH of
    true ->
      OAuthInitiatedLogonType = application:get_env(rabbitmq_management, oauth_initiated_logon_type, sp_initiated),
      OAuthProviderUrl = application:get_env(rabbitmq_management, oauth_provider_url, ""),
      case OAuthInitiatedLogonType of
        sp_initiated ->
          OAuthClientId = application:get_env(rabbitmq_management, oauth_client_id, ""),
          OAuthClientSecret = application:get_env(rabbitmq_management, oauth_client_secret, undefined),
          OAuthMetadataUrl = application:get_env(rabbitmq_management, oauth_metadata_url, undefined),
          OAuthScopes = application:get_env(rabbitmq_management, oauth_scopes, undefined),
          OAuthResourceId = application:get_env(rabbitmq_auth_backend_oauth2, resource_server_id, ""),
          case is_invalid([OAuthResourceId]) of
            true ->
               json_field(oauth_enabled, false, true);
            false ->
                case is_invalid([OAuthClientId, OAuthProviderUrl]) of
                  true ->
                    json_field(oauth_enabled, false, true);
                  false ->
                    json_field(oauth_enabled, true) ++
                    json_field(enable_uaa, EnableUAA) ++
                    json_field(oauth_client_id, OAuthClientId) ++
                    json_field(oauth_client_secret, OAuthClientSecret) ++
                    json_field(oauth_provider_url, OAuthProviderUrl) ++
                    json_field(oauth_scopes, OAuthScopes) ++
                    json_field(oauth_metadata_url, OAuthMetadataUrl) ++
                    json_field(oauth_resource_id, OAuthResourceId, true)
                end
          end;
        idp_initiated ->
           [ json_field(oauth_enabled, true) ++
             json_field(oauth_initiated_logon_type, idp_initiated) ++
             json_field(oauth_provider_url, OAuthProviderUrl, true)
            ]
        end;
     false ->
        [ json_field(oauth_enabled, false, true) ]
  end,
  "{" ++ Data ++ "}".

is_invalid(List) ->
    lists:any(fun(V) -> V == "" end, List).

json_field(Field, Value) -> json_field(Field, Value, false).

json_field(_Field, Value, _LastField) when Value == undefined -> [ ];
json_field(Field, Value, LastField) when is_number(Value) ->
  ["\"", atom_to_list(Field), "\": ", Value, append_comma_if(not LastField)];
json_field(Field, Value, LastField) when is_boolean(Value) ->
  ["\"", atom_to_list(Field), "\": ", atom_to_list(Value), append_comma_if(not LastField)];
json_field(Field, Value, LastField) when is_atom(Value) ->
  ["\"", atom_to_list(Field), "\": \"", atom_to_list(Value), "\"", append_comma_if(not LastField)];
json_field(Field, Value, LastField) ->
  ["\"", atom_to_list(Field), "\": \"", Value, "\"", append_comma_if(not LastField)].


append_comma_if(Append) when Append == true -> ",";
append_comma_if(Append) when Append == false -> "".

oauth_initialize_if_required() ->
  "function oauth_initialize_if_required() { return oauth_initialize(" ++ authSettings() ++ ") }".

set_token_auth(Req0) ->
  case application:get_env(rabbitmq_management, oauth_enabled, false) of
    true ->
      case cowboy_req:parse_header(<<"authorization">>, Req0) of
        {bearer, Token} ->  ["set_token_auth('", Token, "');"];
        _ -> []
      end;
    false -> []
  end.
