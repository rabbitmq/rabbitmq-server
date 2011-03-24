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
    case http_get(q(user_path, [{username, Username}|AuthProps]),
                  ["deny", "allow", "admin"]) of
        {error, _} = E -> E;
        deny           -> {refused, "Denied by HTTP plugin", []};
        Resp           -> {ok, #user{username     = Username,
                                     is_admin     = Resp =:= admin,
                                     auth_backend = ?MODULE,
                                     impl         = none}}
    end.

check_vhost_access(#user{username = Username}, VHost, Permission) ->
    bool_req(vhost_path, [{username,   Username},
                          {vhost,      VHost},
                          {permission, Permission}]).

check_resource_access(#user{username = Username},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission) ->
    bool_req(resource_path, [{username,   Username},
                             {vhost,      VHost},
                             {resource,   Type},
                             {name,       Name},
                             {permission, Permission}]).

%%--------------------------------------------------------------------

bool_req(PathName, Props) ->
    case http_get(q(PathName, Props)) of
        deny  -> false;
        allow -> true;
        E     -> E
    end.

http_get(Path) ->
    http_get(Path, ["allow", "deny"]).

http_get(Path, Allowed) ->
    case httpc:request(get, {Path, []}, ?HTTPC_OPTS, []) of
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            case Code of
                200 -> case parse_resp(Body, Allowed) of
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

escape(V) when is_binary(V) ->
    escape(binary_to_list(V));
escape(V) when is_atom(V) ->
    escape(atom_to_list(V));
escape(V) when is_list(V) ->
    edoc_lib:escape_uri(V).

parse_resp(Resp, Allowed) ->
    Resp1 = string:to_lower(string:strip(Resp)),
    case lists:member(Resp1, Allowed) of
        true  -> list_to_atom(Resp1);
        false -> {error, {response, Resp}}
    end.

%%--------------------------------------------------------------------
