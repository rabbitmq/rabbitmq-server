%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbit_peer_discovery_httpc).

-include_lib("kernel/include/logger.hrl").
-include("include/rabbit_peer_discovery.hrl").

%%
%% API
%%

-export([build_query/1,
         build_uri/5,
         build_path/1,
         delete/6,
         get/5,
         get/7,
         post/6,
         post/8,
         put/6,
         put/7,
         maybe_configure_proxy/0,
         maybe_configure_inet6/0]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.


-define(CONFIG_MODULE, rabbit_peer_discovery_config).
-define(CONFIG_KEY, proxy).

-define(CONFIG_MAPPING,
        #{
          http_proxy   => #peer_discovery_config_entry_meta{
                             type          = string,
                             env_variable  = "HTTP_PROXY",
                             default_value = undefined
                            },
          https_proxy  => #peer_discovery_config_entry_meta{
                             type          = string,
                             env_variable  = "HTTPS_PROXY",
                             default_value = undefined
                            },
          proxy_exclusions => #peer_discovery_config_entry_meta{
                                 type          = list,
                                 env_variable  = "PROXY_EXCLUSIONS",
                                 default_value = []
                                }
         }).


-define(CONTENT_JSON, "application/json").
-define(CONTENT_JSON_WITH_CHARSET, "application/json; charset=utf-8").
-define(CONTENT_URLENCODED, "application/x-www-form-urlencoded").


%% @public
%% @spec build_uri(Scheme, Host, Port, Path, QArgs) -> string()
%% where Scheme = string()
%%       Host = string()
%%       Port = integer()
%%       Path = string()
%%       QArgs = proplist()
%% @doc Build the request URI
%% @end
%%
build_uri(Scheme, Host, Port, Path0, []) ->
  Path = case string:slice(Path0, 0, 1) of
    "/" -> Path0;
    _   -> "/" ++ Path0
  end,
  uri_string:recompose(#{scheme => Scheme,
                         host => Host,
                         port => rabbit_data_coercion:to_integer(Port),
                         path => Path});
build_uri(Scheme, Host, Port, Path0, QArgs) ->
  Path = case string:slice(Path0, 0, 1) of
    "/" -> Path0;
    _   -> "/" ++ Path0
  end,
  uri_string:recompose(#{scheme => Scheme,
                         host   => Host,
                         port   => rabbit_data_coercion:to_integer(Port),
                         path   => Path,
                         query  => build_query(QArgs)}).

-spec build_path(list()) -> string().

build_path(Segments0) when is_list(Segments0) ->
  Segments = [rabbit_data_coercion:to_list(PS) || PS <- Segments0],
  uri_string:recompose(#{path => string:join(Segments, "/")}).

%% @public
%% @spec build_query(proplist()) -> string()
%% @doc Build the query parameters string from a proplist
%% @end
%%
build_query(Args) when is_list(Args) ->
  Normalized = [{rabbit_data_coercion:to_list(K), rabbit_data_coercion:to_list(V)} || {K, V} <- Args],
  uri_string:compose_query(Normalized).


%% @public
%% @spec get(Scheme, Host, Port, Path, Args) -> Result
%% @where Scheme = string(),
%%        Host   = string(),
%%        Port   = integer(),
%%        Path   = string(),
%%        Args   = proplist(),
%%        Result = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP GET request
%% @end
%%
get(Scheme, Host, Port, Path, Args) ->
    get(Scheme, Host, Port, Path, Args, [], []).


%% @public
%% @spec get(Scheme, Host, Port, Path, Args, Headers) -> Result
%% @where Scheme  = string(),
%%        Host    = string(),
%%        Port    = integer(),
%%        Path    = string(),
%%        Args    = proplist(),
%%        Headers = proplist(),
%%        HttpOpts = proplist(),
%%        Result  = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP GET request
%% @end
%%
get(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  ?LOG_DEBUG("GET ~s", [URL], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  HttpOpts1 = ensure_timeout(HttpOpts),
  Response = httpc:request(get, {URL, Headers}, HttpOpts1, []),
  ?LOG_DEBUG("Response: ~p", [Response], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  parse_response(Response).


%% @public
%% @spec post(Scheme, Host, Port, Path, Args, Body) -> Result
%% @where Scheme = string(),
%%        Host   = string(),
%%        Port   = integer(),
%%        Path   = string(),
%%        Args   = proplist(),
%%        Body   = string(),
%%        Result = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP POST request
%% @end
%%
post(Scheme, Host, Port, Path, Args, Body) ->
    post(Scheme, Host, Port, Path, Args, [], [], Body).

%% @public
%% @spec post(Scheme, Host, Port, Path, Headers, HttpOpts, Args, Body) -> Result
%% @where Scheme = string(),
%%        Host   = string(),
%%        Port   = integer(),
%%        Path   = string(),
%%        Args   = proplist(),
%%        Headers = proplist(),
%%        HttpOpts = proplist(),
%%        Body   = string(),
%%        Result = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP POST request
%% @end
%%
post(Scheme, Host, Port, Path, Args, Headers, HttpOpts, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  ?LOG_DEBUG("POST ~s [~p]", [URL, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  HttpOpts1 = ensure_timeout(HttpOpts),
  Response = httpc:request(post, {URL, Headers, ?CONTENT_JSON, Body}, HttpOpts1, []),
  ?LOG_DEBUG("Response: [~p]", [Response], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  parse_response(Response).


%% @public
%% @spec put(Scheme, Host, Port, Path, Args, Body) -> Result
%% @where Scheme = string(),
%%        Host   = string(),
%%        Port   = integer(),
%%        Path   = string(),
%%        Args   = proplist(),
%%        Body   = string(),
%%        Result = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP PUT request
%% @end
%%

-spec put(Scheme, Host, Port, Path, Args, Body) -> {ok, string()} | {error, any()} when
  Scheme :: atom() | string(),
  Host :: string() | binary(),
  Port :: integer(),
  Path :: string() | binary(),
  Args :: list(),
  Body :: string() | binary() | tuple().
put(Scheme, Host, Port, Path, Args, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  ?LOG_DEBUG("PUT ~s [~p]", [URL, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(put, {URL, [], ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  ?LOG_DEBUG("Response: [~p]", [Response], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  parse_response(Response).


%% @spec put(Scheme, Host, Port, Path, Args, Headers, Body) -> Result
%% @where Scheme  = string(),
%%        Host    = string(),
%%        Port    = integer(),
%%        Path    = string(),
%%        Args    = proplist(),
%%        Headers = proplist(),
%%        Body    = string(),
%%        Result  = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP PUT request
%% @end
%%
-spec put(Scheme, Host, Port, Path, Args, Headers, Body) -> {ok, string()} | {error, any()} when
  Scheme :: atom() | string(),
  Host :: string() | binary(),
  Port :: integer(),
  Path :: string() | binary(),
  Args :: list(),
  Headers :: list(),
  Body :: string() | binary() | tuple().
put(Scheme, Host, Port, Path, Args, Headers, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  ?LOG_DEBUG("PUT ~s [~p] [~p]", [URL, Headers, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(put, {URL, Headers, ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  ?LOG_DEBUG("Response: [~p]", [Response], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  parse_response(Response).


%% @public
%% @spec delete(Scheme, Host, Port, Path, Args, Body) -> Result
%% @where Scheme = string(),
%%        Host   = string(),
%%        Port   = integer(),
%%        Path   = string(),
%%        Args   = proplist(),
%%        Body   = string(),
%%        Result = {ok, mixed}|{error, Reason::string()}
%% @doc Perform a HTTP DELETE request
%% @end
%%
delete(Scheme, Host, Port, PathSegments, Args, Body) when is_list(PathSegments) ->
  Path = uri_string:recompose(#{path => lists:join("/", [rabbit_data_coercion:to_list(PS) || PS <- PathSegments])}),
  delete(Scheme, Host, Port, Path, Args, Body);
delete(Scheme, Host, Port, Path, Args, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  ?LOG_DEBUG("DELETE ~s [~p]", [URL, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(delete, {URL, [], ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  ?LOG_DEBUG("Response: [~p]", [Response], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  parse_response(Response).


%% @public
%% @spec maybe_configure_proxy() -> Result
%% @where Result = ok.
%% @doc Configures HTTP[S] proxy settings in httpc, if necessary
%% @end
-spec maybe_configure_proxy() -> ok.
maybe_configure_proxy() ->
  Map = ?CONFIG_MODULE:config_map(?CONFIG_KEY),
  case map_size(Map) of
      0 ->
          ?LOG_DEBUG(
             "HTTP client proxy is not configured",
             #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
          ok;
      _ ->
          HttpProxy       = ?CONFIG_MODULE:get(http_proxy,  ?CONFIG_MAPPING, Map),
          HttpsProxy      = ?CONFIG_MODULE:get(https_proxy, ?CONFIG_MAPPING, Map),
          ProxyExclusions = ?CONFIG_MODULE:get(proxy_exclusions, ?CONFIG_MAPPING, Map),
          ?LOG_DEBUG(
             "Configured HTTP proxy: ~p, HTTPS proxy: ~p, exclusions: ~p",
             [HttpProxy, HttpsProxy, ProxyExclusions],
             #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
          maybe_set_proxy(proxy, HttpProxy, ProxyExclusions),
          maybe_set_proxy(https_proxy, HttpsProxy, ProxyExclusions),
          ok
  end.

%% @public
%% @spec maybe_configure_inet6() -> Result
%% @where Result = ok.
%% @doc Configures HTTP[S] inet6 settings in httpc, if necessary
%% @end
-spec maybe_configure_inet6() -> ok.
maybe_configure_inet6() ->
    IpFamily = case proplists:get_value(inet6, inet:get_rc(), false) of
                   true -> inet6;
                   false -> inet
               end,
    httpc:set_option(ipfamily, IpFamily).

%%--------------------------------------------------------------------
%% @private
%% @doc Set httpc proxy options.
%% @end
%%--------------------------------------------------------------------
-spec maybe_set_proxy(Option :: atom(),
                      ProxyUrl :: string(),
                      ProxyExclusions :: list()) -> ok | {error, Reason :: term()}.
maybe_set_proxy(_Option, "undefined", _ProxyExclusions) -> ok;
maybe_set_proxy(Option, ProxyUrl, ProxyExclusions) ->
  case parse_proxy_uri(Option, ProxyUrl) of
    UriMap ->
      Host = maps:get(host, UriMap),
      Port = maps:get(port, UriMap, 80),
      ?LOG_DEBUG(
        "Configuring HTTP client's ~s setting: ~p, exclusions: ~p",
        [Option, {Host, Port}, ProxyExclusions],
        #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
      httpc:set_option(Option, {{Host, Port}, ProxyExclusions})
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc Support defining proxy address with or without uri scheme.
%% @end
%%--------------------------------------------------------------------
-spec parse_proxy_uri(ProxyType :: atom(), ProxyUrl :: string()) -> uri_string:uri_map().
parse_proxy_uri(ProxyType, ProxyUrl) ->
  case ProxyType of
    proxy       ->
      case string:slice(ProxyUrl, 0, 4) of
        "http" -> uri_string:parse(ProxyUrl);
        _      -> uri_string:parse("http://" ++ ProxyUrl)
      end;
    https_proxy ->
      case string:slice(ProxyUrl, 0, 5) of
        "https" -> uri_string:parse(ProxyUrl);
        _       -> uri_string:parse("https://" ++ ProxyUrl)
      end
  end.



%% @private
%% @spec decode_body(mixed) -> list()
%% @doc Decode the response body and return a list
%% @end
%%

-spec decode_body(string(), string() | binary() | term()) -> 'false' | 'null' | 'true' |
                                                              binary() | [any()] | number() | map().

decode_body(_, []) -> [];
decode_body(?CONTENT_JSON_WITH_CHARSET, Body) ->
    decode_body(?CONTENT_JSON, Body);
decode_body(?CONTENT_JSON, Body) ->
    case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body)) of
        {ok, Value} ->
            Value;
        {error, Err}  ->
            ?LOG_ERROR(
               "HTTP client could not decode a JSON payload "
               "(JSON parser returned an error): ~p.",
               [Err],
               #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
            []
    end.

%% @private
%% @spec parse_response(Response) -> {ok, string()} | {error, mixed}
%% @where Response = {status_line(), headers(), Body} | {status_code(), Body}
%% @doc Decode the response body and return a list
%% @end
%%
-spec parse_response({ok, integer(), string()} | {error, any()}) -> {ok, string()} | {error, any()}.

parse_response({error, Reason}) ->
  ?LOG_DEBUG("HTTP error ~p", [Reason], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  {error, lists:flatten(io_lib:format("~p", [Reason]))};

parse_response({ok, 200, Body})  -> {ok, decode_body(?CONTENT_JSON, Body)};
parse_response({ok, 201, Body})  -> {ok, decode_body(?CONTENT_JSON, Body)};
parse_response({ok, 204, _})     -> {ok, []};
parse_response({ok, Code, Body}) ->
  ?LOG_DEBUG("HTTP Response (~p) ~s", [Code, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  {error, integer_to_list(Code)};

parse_response({ok, {{_,200,_}, Headers, Body}}) ->
  {ok, decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body)};
parse_response({ok,{{_,201,_}, Headers, Body}}) ->
  {ok, decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body)};
parse_response({ok,{{_,204,_}, _, _}}) -> {ok, []};
parse_response({ok,{{_Vsn,Code,_Reason},_,Body}}) ->
  ?LOG_DEBUG("HTTP Response (~p) ~s", [Code, Body], #{domain => ?RMQLOG_DOMAIN_PEER_DIS}),
  {error, integer_to_list(Code)}.

%% @private
%% @doc Ensure that the timeout option is set.
%% @end
%%
-spec ensure_timeout() -> list({atom(), term()}).
ensure_timeout() ->
    [{timeout, ?DEFAULT_HTTP_TIMEOUT}].

%% @private
%% @doc Ensure that the timeout option is set, and does not exceed
%%      about 1/2 of the default gen_server:call timeout. This gives
%%      enough time for a long connect and request phase to succeed.
%% @end
%%
-spec ensure_timeout(Options :: list({atom(), term()})) -> list({atom(), term()}).
ensure_timeout(Options) ->
    case proplists:get_value(timeout, Options) of
        undefined ->
            Options ++ [{timeout, ?DEFAULT_HTTP_TIMEOUT}];
        Value when is_integer(Value) andalso Value >= 0 andalso Value =< ?DEFAULT_HTTP_TIMEOUT ->
            Options;
        _ ->
            Options1 = proplists:delete(timeout, Options),
            Options1 ++ [{timeout, ?DEFAULT_HTTP_TIMEOUT}]
    end.
