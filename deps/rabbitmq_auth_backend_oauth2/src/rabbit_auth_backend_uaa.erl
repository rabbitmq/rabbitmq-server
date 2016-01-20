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
         check_vhost_access/3, check_resource_access/3]).

%% httpc seems to get racy when using HTTP 1.1
-define(HTTPC_OPTS, [{version, "HTTP/1.0"}]).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"HTTP">>},
     {description, <<"HTTP authentication / authorisation">>}].

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

check_vhost_access(#auth_user{username = Username}, VHost, _Sock) ->
    with_token(Username, 
               fun(UserData) ->
                       rabbit_oauth2_scope:vhost_access(VHost, UserData)
               end).

check_resource_access(#auth_user{username = Username}, Resource, Permission) ->
    with_token(Username, 
               fun(UserData) ->
                       rabbit_oauth2_scope:resource_access(Resource, Permission, UserData)
               end).

%%--------------------------------------------------------------------

with_token(Token, Fun) ->
    case check_token(Token) of
        {ok, UserData} -> Fun(UserData);
        _              -> false
    end.

check_token(Token) ->
    {ok, UaaUri} = application:get_env(rabbitmq_auth_backend_uaa, uri),
    Path   = UaaUri ++ "/check_token",
    {ok, AuthUser} = application:get_env(rabbitmq_auth_backend_uaa, username),
    {ok, AuthPass} = application:get_env(rabbitmq_auth_backend_uaa, password),
    Auth = base64:encode_to_string(AuthUser ++ ":" ++ AuthPass),
    URI  = uri_parser:parse(Path, [{port, 80}]),

    {host, Host} = lists:keyfind(host, 1, URI),
    {port, Port} = lists:keyfind(port, 1, URI),
    HostHdr = rabbit_misc:format("~s:~b", [Host, Port]),
    ReqBody = "token=" ++ http_uri:encode(binary_to_list(Token)),
    Resp = httpc:request(post, 
                         {Path, 
                          [{"Host", HostHdr}, 
                           {"Authorization", "Basic " ++ Auth}], 
                          "application/x-www-form-urlencoded", 
                          ReqBody}, 
                         ?HTTPC_OPTS, []),
    rabbit_log:info("Resp ~p", [Resp]),
    case Resp of
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            case Code of
                200 -> parse_resp(Body);
                400 -> parse_err(Body);
                401 -> {error, invalid_resource_authorization};
                _   -> {error, {Code, Body}}
            end;
        {error, _} = E -> E
    end.

parse_resp(Body) -> 
    {struct, Resp}  = mochijson2:decode(Body),
    % Aud   = proplists:get_value(<<"aud">>, Resp, []),
    % {ok, ResId} = application:get_env(rabbitmq_auth_backend_uaa, 
    %                                   resource_server_id),
    % ValidAud = case Aud of
    %     List when is_list(List) -> lists:member(ResId, Aud);
    %     _                       -> false
    % end,
    ValidAud = true,
    case ValidAud of
        true  -> {ok, Resp};
        false -> {refused, {invalid_aud, Resp}}
    end.

parse_err(Body) ->
    {refused, Body}.


%%--------------------------------------------------------------------
