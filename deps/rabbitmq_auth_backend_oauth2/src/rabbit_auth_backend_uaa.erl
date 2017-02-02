%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ HTTP authentication.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_uaa).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0]).
-export([user_login_authentication/2, user_login_authorization/1,
         check_vhost_access/3, check_resource_access/3,
         check_topic_access/4]).

-ifdef(TEST).
-compile(export_all).
-endif.
%%--------------------------------------------------------------------

description() ->
    [{name, <<"UAA">>},
     {description, <<"UAA token authentication / authorisation">>}].

%%--------------------------------------------------------------------

user_login_authentication(Username, _AuthProps) ->
    case check_token(Username) of
        {error, _} = E  -> E;
        {refused, Err}  -> {refused, "Denied by UAA plugin with error: ~p",
                            [Err]};
        {ok, _UserData} -> {ok, #auth_user{username = Username,
                                           tags = [],
                                           impl = none}}
    end.

user_login_authorization(Username) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

check_vhost_access(#auth_user{username = Username, impl = DecodedToken},
                   VHost, _Sock) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes = get_scopes(DecodedToken),
            rabbit_oauth2_scope:vhost_access(VHost, Scopes)
        end).

check_resource_access(#auth_user{username = Username, impl = DecodedToken},
                      Resource, Permission) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes = get_scopes(DecodedToken),
            rabbit_oauth2_scope:resource_access(Resource, Permission, Scopes)
        end).

check_topic_access(#auth_user{username = Username, impl = DecodedToken},
                   Resource, Permission, Context) ->
    with_decoded_token(DecodedToken,
        fun() ->
            Scopes = get_scopes(DecodedToken),
            rabbit_oauth2_scope:topic_access(Resource, Permission, Context, Scopes)
        end).

%%--------------------------------------------------------------------

with_decoded_token(DecodedToken, Fun) ->
    case token_expired(DecodedToken) of
        false -> Fun();
        true  -> {error_message, "Token expired"}
    end.

token_expired(#{<<"exp">> := Exp}) when is_integer(Exp) ->
    Exp =< os:system_time(seconds);
token_expired(#{}) -> true.

with_token(Token, Fun) ->
    case check_token(Token) of
        {ok, UserData} -> Fun(UserData);
        _              -> false
    end.

-spec check_token(binary()) -> {ok, map()} | {error, term()}.
check_token(Token) ->
    case 'Elixir.UaaJWT':decode_and_verify(Token) of
        {error, Reason} -> {refused, {error, Reason}};
        {true, Payload} -> validate_payload(Payload);
        {false, _}      -> {refused, signature_invalid}
    end.

validate_payload(#{<<"scope">> := Scope, <<"aud">> := Aud} = UserData) ->
    ResIdStr = application:get_env(rabbitmq_auth_backend_uaa,
                                   resource_server_id, <<>>),
    ResId = rabbit_data_coercion:to_binary(ResIdStr),
    case valid_aud(Aud, ResId) of
        true  -> {ok, UserData#{<<"scope">> => filter_scope(Scope, ResId)}};
        false -> {refused, {invalid_aud, UserData, ResId}}
    end.

filter_scope(Scope, <<"">>) -> Scope;
filter_scope(Scope, ResId)  ->
    Pattern = <<ResId/binary, ".">>,
    PatternLength = byte_size(Pattern),
    lists:filtermap(
        fun(ScopeEl) ->
            case binary:match(ScopeEl, Pattern) of
                {0, PatternLength} ->
                    ElLength = byte_size(ScopeEl),
                    {true,
                     binary:part(ScopeEl,
                                 {PatternLength, ElLength - PatternLength})};
                _ -> false
            end
        end,
        Scope).

valid_aud(_, <<>>)    -> true;
valid_aud(Aud, ResId) ->
    case Aud of
        List when is_list(List) -> lists:member(ResId, Aud);
        _                       -> false
    end.

get_scopes(#{<<"scope">> := Scope}) -> Scope.

%%--------------------------------------------------------------------
