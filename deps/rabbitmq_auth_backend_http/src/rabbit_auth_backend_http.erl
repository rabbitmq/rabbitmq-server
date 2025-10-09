%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_http).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([description/0, p/1, q/1, join_tags/1]).
-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         expiry_timestamp/1]).

%% If keepalive connection is closed, retry N times before failing.
-define(RETRY_ON_KEEPALIVE_CLOSED, 3).

-define(RESOURCE_REQUEST_PARAMETERS, [username, vhost, resource, name, permission]).

-define(SUCCESSFUL_RESPONSE_CODES, [200, 201]).

%%--------------------------------------------------------------------

description() ->
    [{name, <<"HTTP">>},
     {description, <<"HTTP authentication / authorisation">>}].

%%--------------------------------------------------------------------

user_login_authentication(Username, AuthProps) ->
    Path = p(user_path),
    Query = q([{username, Username}] ++ extract_other_credentials(AuthProps)),
    case http_req(Path, Query) of
        {error, _} = Err  ->
            Err;
        "deny " ++ Reason ->
            ?LOG_INFO("HTTP authentication denied for user '~ts': ~ts",
                      [Username, Reason]),
            {refused, "Denied by the backing HTTP service", []};
        Body ->
            case string:lowercase(Body) of
                "deny" ->
                    {refused, "Denied by the backing HTTP service", []};
                "allow" ++ Rest ->
                    Tags = [rabbit_data_coercion:to_atom(T)
                            || T <- string:tokens(Rest, " ")],
                    {ok, #auth_user{
                            username = Username,
                            tags = Tags,
                            impl = fun() -> proplists:delete(username, AuthProps) end}}
            end
    end.

%% When a protocol plugin uses an internal AMQP 0-9-1 client to interact with RabbitMQ core,
%% what happens that the plugin authenticates the entire authentication context (e.g. all of: password, client_id, vhost, etc)
%% and the internal AMQP 0-9-1 client also performs further authentication.
%%
%% In the latter case, the complete set of credentials are persisted behind a function call
%% that returns an AuthProps.
%% If the user was first authenticated by rabbit_auth_backend_http, there will be one property called
%% `rabbit_auth_backend_http` whose value is a function that returns a proplist with all the credentials used
%% on the first successful login.
%%
%% When rabbit_auth_backend_cache is involved,
%% the property `rabbit_auth_backend_cache` is a function which returns a proplist with all the credentials used
%% on the first successful login.
resolve_using_persisted_credentials(AuthProps) ->
  case proplists:get_value(rabbit_auth_backend_http, AuthProps, undefined) of
    undefined ->
      case proplists:get_value(rabbit_auth_backend_cache, AuthProps, undefined) of
          undefined -> AuthProps;
          CacheAuthPropsFun -> AuthProps ++ CacheAuthPropsFun()
      end;
    HttpAuthPropsFun -> AuthProps ++ HttpAuthPropsFun()
  end.


%% Some protocols may add additional credentials into the AuthProps that should be propagated to
%% the external authentication backends
%% This function excludes any attribute that starts with rabbit_auth_backend_
is_internal_property(rabbit_auth_backend_http) -> true;
is_internal_property(rabbit_auth_backend_cache) -> true;
is_internal_property(_Other) -> false.

is_internal_none_password(password, none) -> true;
is_internal_none_password(_, _) -> false.

is_sockOrAddr(sockOrAddr) -> true;
is_sockOrAddr(_) -> false.

extract_other_credentials(AuthProps) ->
  PublicAuthProps = [{K,V} || {K,V} <-AuthProps, not is_internal_property(K) and
                                                  not is_internal_none_password(K, V) and
                                                  not is_sockOrAddr(K)],
  case PublicAuthProps of
    [] -> resolve_using_persisted_credentials(AuthProps);
    _ -> PublicAuthProps
  end.


user_login_authorization(Username, AuthProps) ->
    case user_login_authentication(Username, AuthProps) of
        {ok, #auth_user{impl = Impl}} -> {ok, Impl};
        Else                          -> Else
    end.

check_vhost_access(#auth_user{username = Username, tags = Tags}, VHost, undefined) ->
    do_check_vhost_access(Username, Tags, VHost, "", undefined);
check_vhost_access(#auth_user{username = Username, tags = Tags}, VHost,
                   AuthzData = #{peeraddr := PeerAddr}) when is_map(AuthzData) ->
    AuthzData1 = maps:remove(peeraddr, AuthzData),
    Ip = parse_peeraddr(PeerAddr),
    do_check_vhost_access(Username, Tags, VHost, Ip, AuthzData1).

do_check_vhost_access(Username, Tags, VHost, Ip, AuthzData) ->
    OptionsParameters = context_as_parameters(AuthzData),
    bool_req(vhost_path, [{username, Username},
                          {vhost,    VHost},
                          {ip,       Ip},
                          {tags,     join_tags(Tags)}] ++ OptionsParameters).

check_resource_access(#auth_user{username = Username, tags = Tags},
                      #resource{virtual_host = VHost, kind = Type, name = Name},
                      Permission,
                      AuthzContext) ->
    OptionsParameters = context_as_parameters(AuthzContext),
    bool_req(resource_path, [{username,   Username},
                             {vhost,      VHost},
                             {resource,   Type},
                             {name,       Name},
                             {permission, Permission},
                             {tags, join_tags(Tags)}] ++ OptionsParameters).

check_topic_access(#auth_user{username = Username, tags = Tags},
                   #resource{virtual_host = VHost, kind = topic = Type, name = Name},
                   Permission,
                   Context) ->
    OptionsParameters = context_as_parameters(Context),
    bool_req(topic_path, [{username,   Username},
        {vhost,      VHost},
        {resource,   Type},
        {name,       Name},
        {permission, Permission},
        {tags, join_tags(Tags)}] ++ OptionsParameters).

expiry_timestamp(_) -> never.

%%--------------------------------------------------------------------

context_as_parameters(Options) when is_map(Options) ->
    % filter keys that would erase fixed parameters
    [{rabbit_data_coercion:to_atom(Key), maps:get(Key, Options)}
        || Key <- maps:keys(Options),
        lists:member(
            rabbit_data_coercion:to_atom(Key),
            ?RESOURCE_REQUEST_PARAMETERS) =:= false];
context_as_parameters(_) ->
    [].

bool_req(PathName, Props) ->
    Path = p(PathName),
    Query = q(Props),
    case http_req(Path, Query) of
        {error, _} = Err ->
            Err;
        "deny " ++ Reason ->
            ?LOG_INFO("HTTP authorisation denied for path ~ts with query ~ts: ~ts",
                      [Path, Query, Reason]),
            false;
        Body ->
            case string:lowercase(Body) of
                "deny" ->
                    false;
                "allow" ->
                    true
            end
    end.

http_req(Path, Query) ->
    http_req(Path, Query, ?RETRY_ON_KEEPALIVE_CLOSED).

http_req(Path, Query, Retry) ->
    case do_http_req(Path, Query) of
        {error, socket_closed_remotely} ->
            %% HTTP keepalive connection can no longer be used. Retry the request.
            case Retry > 0 of
                true  -> http_req(Path, Query, Retry - 1);
                false -> {error, socket_closed_remotely}
            end;
        Other -> Other
    end.

do_http_req(Path0, Query) ->
    URI = uri_parser:parse(Path0, [{port, 80}]),
    {host, Host} = lists:keyfind(host, 1, URI),
    {port, Port} = lists:keyfind(port, 1, URI),
    HostHdr = rabbit_misc:format("~ts:~b", [Host, Port]),
    {ok, Method} = application:get_env(rabbitmq_auth_backend_http, http_method),
    Request = case rabbit_data_coercion:to_atom(Method) of
        get  ->
            Path = Path0 ++ "?" ++ Query,
            ?LOG_DEBUG("auth_backend_http: GET ~ts", [Path]),
            {Path, [{"Host", HostHdr}]};
        post ->
            ?LOG_DEBUG("auth_backend_http: POST ~ts", [Path0]),
            {Path0, [{"Host", HostHdr}], "application/x-www-form-urlencoded", Query}
    end,
    RequestTimeout =
        case application:get_env(rabbitmq_auth_backend_http, request_timeout) of
            {ok, Val1} -> Val1;
            _ -> infinity
        end,
    ConnectionTimeout =
        case application:get_env(rabbitmq_auth_backend_http, connection_timeout) of
            {ok, Val2} -> Val2;
            _ -> RequestTimeout
        end,
    ?LOG_DEBUG("auth_backend_http: request timeout: ~tp, connection timeout: ~tp", [RequestTimeout, ConnectionTimeout]),
    HttpOpts = [{timeout, RequestTimeout},
                {connect_timeout, ConnectionTimeout}] ++ ssl_options(),
    case httpc:request(Method, Request, HttpOpts, []) of
        {ok, {{_HTTP, Code, _}, _Headers, Body}} ->
            ?LOG_DEBUG("auth_backend_http: response code is ~tp, body: ~tp", [Code, Body]),
            case lists:member(Code, ?SUCCESSFUL_RESPONSE_CODES) of
                true ->
                    string:strip(Body);
                false ->
                    {error, {Code, Body}}
            end;
        {error, _} = E ->
            E
    end.

ssl_options() ->
    case application:get_env(rabbitmq_auth_backend_http, ssl_options) of
        {ok, Opts0} when is_list(Opts0) ->
            Opts1 = [{ssl, rabbit_ssl_options:fix_client(Opts0)}],
            case application:get_env(rabbitmq_auth_backend_http, ssl_hostname_verification) of
                {ok, wildcard} ->
                    ?LOG_DEBUG("Enabling wildcard-aware hostname verification for HTTP client connections"),
                    %% Needed for HTTPS connections that connect to servers that use wildcard certificates.
                    %% See https://erlang.org/doc/man/public_key.html#pkix_verify_hostname_match_fun-1.
                    [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | Opts1];
                _ ->
                    Opts1
            end;
        _ -> []
    end.

p(PathName) ->
    {ok, Path} = application:get_env(rabbitmq_auth_backend_http, PathName),
    Path.

q(Args) ->
    string:join([escape(K, V) || {K, V} <- Args, not is_function(V)], "&").

escape(K, Map) when is_map(Map) ->
    string:join([escape(rabbit_data_coercion:to_list(K) ++ "." ++ rabbit_data_coercion:to_list(Key), Value)
        || {Key, Value} <- maps:to_list(Map), not is_function(Value)], "&");
escape(K, V) ->
    rabbit_data_coercion:to_list(K) ++ "=" ++ rabbit_http_util:quote_plus(V).

join_tags([])   -> "";
join_tags(Tags) ->
  Strings = [rabbit_data_coercion:to_list(T) || T <- Tags],
  string:join(Strings, " ").

-spec parse_peeraddr(inet:ip_address() | unknown) -> string().
parse_peeraddr(unknown) ->
    rabbit_data_coercion:to_list(unknown);
parse_peeraddr(PeerAddr) ->
    handle_inet_ntoa_peeraddr(inet:ntoa(PeerAddr), PeerAddr).

-spec handle_inet_ntoa_peeraddr({'error', term()} | string(), inet:ip_address() | unknown) -> string().
handle_inet_ntoa_peeraddr({error, einval}, PeerAddr) ->
    rabbit_data_coercion:to_list(PeerAddr);
handle_inet_ntoa_peeraddr(PeerAddrStr, _PeerAddr0) ->
    PeerAddrStr.
