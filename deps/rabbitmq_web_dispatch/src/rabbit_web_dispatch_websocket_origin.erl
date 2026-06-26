%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% WebSocket `Origin` header validation shared by the Web-STOMP and Web-MQTT
%% plugins. The policy decision (`validate/4`) is pure: the caller passes in the
%% configuration and the request's own origin, so it can be tested without a
%% running broker.
-module(rabbit_web_dispatch_websocket_origin).

-include_lib("kernel/include/logger.hrl").

-export([config/1, validate/4, permissive_warning/2, warn_if_permissive/2]).

-export_type([allow_origins/0, server_origin/0]).

%% Resolved value of a plugin's `allow_origins` setting:
%%   `[]`            - no restriction; any Origin is accepted (the default)
%%   `same_origin`   - only an Origin equal to the request's own origin is accepted
%%   list of origins - only an Origin present in the list is accepted
-type allow_origins() :: [] | same_origin | [string()].

%% The request's own origin as seen by the server: scheme, host, and port.
-type server_origin() :: {Scheme :: binary(),
                          Host :: binary(),
                          Port :: inet:port_number()}.

%% Reads both origin-validation settings from a plugin's application
%% environment. Centralised here so the two plugins and their listeners share
%% one definition of the setting names and defaults.
-spec config(App :: atom()) -> {allow_origins(), Strict :: boolean()}.
config(App) ->
    {application:get_env(App, allow_origins, []),
     application:get_env(App, strict_origin_validation, false)}.

-spec validate(Origin, ServerOrigin, AllowOrigins, Strict) ->
          ok | {error, origin_not_allowed} when
      Origin :: binary() | undefined,
      ServerOrigin :: server_origin(),
      AllowOrigins :: allow_origins(),
      Strict :: boolean().
%% No restriction configured: accept every Origin, including a missing one.
validate(_Origin, _ServerOrigin, [], _Strict) ->
    ok;
%% A policy is in effect but the request carries no Origin header. Such a
%% request cannot come from a same-origin-bound browser, so it is accepted
%% to keep non-browser WebSocket clients working unless strict mode is on.
validate(undefined, _ServerOrigin, _AllowOrigins, Strict) ->
    case Strict of
        true  -> {error, origin_not_allowed};
        false -> ok
    end;
validate(Origin, ServerOrigin, same_origin, _Strict) when is_binary(Origin) ->
    case is_same_origin(Origin, ServerOrigin) of
        true  -> ok;
        false -> {error, origin_not_allowed}
    end;
validate(Origin, _ServerOrigin, AllowOrigins, _Strict)
  when is_binary(Origin), is_list(AllowOrigins) ->
    case lists:member(binary_to_list(Origin), AllowOrigins) of
        true  -> ok;
        false -> {error, origin_not_allowed}
    end.

%% Returns a warning to log at startup when a listener accepts any Origin, or
%% `none` when a restriction is in place. Pure so the message text can be
%% asserted in tests.
-spec permissive_warning(PluginLabel :: iodata(), allow_origins()) ->
          iodata() | none.
permissive_warning(PluginLabel, []) ->
    [PluginLabel,
     " WebSocket Origin validation is disabled: 'allow_origins' is not set, so "
     "WebSocket upgrade requests from any Origin are accepted. If browser "
     "clients connect to this node, restrict them by setting 'allow_origins' to "
     "the trusted origins, or to 'same_origin'."];
permissive_warning(_PluginLabel, _AllowOrigins) ->
    none.

%% Logs the permissive-default warning for App (labelled PluginLabel) when no
%% Origin restriction is configured; a no-op otherwise. Called once per plugin
%% at listener startup.
-spec warn_if_permissive(App :: atom(), PluginLabel :: iodata()) -> ok.
warn_if_permissive(App, PluginLabel) ->
    {AllowOrigins, _Strict} = config(App),
    case permissive_warning(PluginLabel, AllowOrigins) of
        none -> ok;
        Msg  -> ?LOG_WARNING("~ts", [Msg])
    end.

%%
%% Implementation
%%

-spec is_same_origin(binary(), server_origin()) -> boolean().
is_same_origin(Origin, {Scheme, Host, Port}) ->
    case parse_origin(Origin) of
        {ok, {OScheme, OHost, OPort}} ->
            %% Scheme and host are compared case-insensitively per RFC 6454;
            %% the port must match exactly after default-port normalisation.
            string:equal(OScheme, Scheme, true) andalso
                string:equal(OHost, Host, true) andalso
                OPort =:= Port;
        error ->
            false
    end.

-spec parse_origin(binary()) ->
          {ok, {binary(), binary(), inet:port_number()}} | error.
parse_origin(Origin) ->
    try uri_string:parse(Origin) of
        #{scheme := Scheme, host := Host} = Parsed ->
            case maps:get(port, Parsed, default_port(Scheme)) of
                undefined -> error;
                Port      -> {ok, {Scheme, Host, Port}}
            end;
        _ ->
            %% Opaque Origins such as "null" carry no scheme/host and are
            %% never treated as same-origin.
            error
    catch
        _:_ -> error
    end.

-spec default_port(binary()) -> inet:port_number() | undefined.
default_port(Scheme) ->
    case string:lowercase(Scheme) of
        <<"https">> -> 443;
        <<"http">>  -> 80;
        _           -> undefined
    end.
