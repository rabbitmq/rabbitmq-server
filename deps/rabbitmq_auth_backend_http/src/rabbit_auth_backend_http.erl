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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_auth_backend_http).

-include_lib("rabbit_common/include/rabbit.hrl").
-behaviour(rabbit_auth_backend).
-include_lib("rabbit_common/include/rabbit_auth_backend_spec.hrl").

-export([description/0, q/2]).
-export([check_user_login/2, check_vhost_access/3, check_resource_access/3]).

%% httpc seems to get racy when using HTTP 1.1
-define(HTTPC_OPTS, [{version, "HTTP/1.0"}]).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"HTTP">>},
     {description, <<"HTTP authentication / authorisation">>}].

%%--------------------------------------------------------------------

check_user_login(Username, AuthProps) ->
    case http_get(q(user_path, [{username, Username}|AuthProps])) of
        true           -> {ok, #user{username     = Username,
                                     is_admin     = true,
                                     auth_backend = ?MODULE,
                                     impl         = none}};
        false          -> {refused, "", []};
        {error, _} = E -> E
    end.

check_vhost_access(#user{username = Username}, VHost, Permission) ->
    http_get(q(vhost_path, [{username,   Username},
                            {vhost,      VHost},
                            {permission, Permission}])).

check_resource_access(#user{username = Username},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission) ->
    http_get(q(resource_path, [{username,   Username},
                               {vhost,      VHost},
                               {resource,   Type},
                               {name,       Name},
                               {permission, Permission}])).

%%--------------------------------------------------------------------

http_get(Path) ->
    case httpc:request(get, {Path, []}, ?HTTPC_OPTS, []) of
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            case Code of
                200 -> case parse_resp(Body) of
                           {error, _} = E -> E;
                           Resp           -> Resp
                       end;
                _   -> {error, {Code, Body}}
            end;
        {error, _} = E ->
            E
    end.

q(PathName, Args) ->
    {ok, Path} = application:get_env(rabbit_auth_backend_http, PathName),
    R = Path ++ "?" ++ string:join([escape(K, V) || {K, V} <- Args], "&"),
    %%io:format("Q: ~p~n", [R]),
    R.

escape(K, V) ->
    atom_to_list(K) ++ "=" ++ escape(V).

escape(Str) when is_binary(Str) ->
    escape(binary_to_list(Str));
escape(Str) when is_list(Str) ->
    edoc_lib:escape_uri(Str).

parse_resp(Resp) ->
    case string:to_lower(string:strip(Resp)) of
        "true"  -> true;
        "false" -> false;
        _       -> {error, {response, Resp}}
    end.

%%--------------------------------------------------------------------
