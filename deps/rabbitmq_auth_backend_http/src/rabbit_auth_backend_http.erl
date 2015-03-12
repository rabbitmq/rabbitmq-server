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

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0, q/2]).
-export([user_login_authentication/2, user_login_authorization/1,
         check_vhost_access/3, check_resource_access/3]).

%% httpc seems to get racy when using HTTP 1.1
-define(HTTPC_OPTS, [{version, "HTTP/1.0"}]).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"HTTP">>},
     {description, <<"HTTP authentication / authorisation">>}].

%%--------------------------------------------------------------------

user_login_authentication(Username, AuthProps) ->
    case http_get(q(user_path, [{username, Username}|AuthProps])) of
        {error, _} = E  -> E;
        deny            -> {refused, "Denied by HTTP plugin", []};
        "allow" ++ Rest -> Tags = [list_to_atom(T) ||
                                      T <- string:tokens(Rest, " ")],
                           {ok, #auth_user{username = Username,
                                           tags     = Tags,
                                           impl     = none}};
        Other           -> {error, {bad_response, Other}}
    end.

user_login_authorization(Username) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

check_vhost_access(#auth_user{username = Username}, VHost, _Sock) ->
    bool_req(vhost_path, [{username, Username},
                          {vhost,    VHost}]).

check_resource_access(#auth_user{username = Username},
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
        "deny"  -> false;
        "allow" -> true;
        E       -> E
    end.

http_get(Path) ->
    URI = uri_parser:parse(Path, [{port, 80}]),
    {host, Host} = lists:keyfind(host, 1, URI),
    {port, Port} = lists:keyfind(port, 1, URI),
    HostHdr = rabbit_misc:format("~s:~b", [Host, Port]),
    case httpc:request(get, {Path, [{"Host", HostHdr}]}, ?HTTPC_OPTS, []) of
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
    {ok, Path} = application:get_env(rabbitmq_auth_backend_http, PathName),
    R = Path ++ "?" ++ string:join([escape(K, V) || {K, V} <- Args], "&"),
    %%io:format("Q: ~p~n", [R]),
    R.

escape(K, V) ->
    atom_to_list(K) ++ "=" ++ mochiweb_util:quote_plus(V).

parse_resp(Resp) -> string:to_lower(string:strip(Resp)).

%%--------------------------------------------------------------------
