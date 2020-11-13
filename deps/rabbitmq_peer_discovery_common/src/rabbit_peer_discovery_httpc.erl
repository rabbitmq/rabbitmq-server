%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2017 Pivotal Software, Inc. All rights reserved.
%%

-module(rabbit_peer_discovery_httpc).

-include("include/rabbit_peer_discovery.hrl").

%%
%% API
%%

-export([build_query/1,
         build_path/1,
         build_uri/5,
         delete/6,
         get/5,
         get/7,
         post/6,
         put/6,
         put/7,
         maybe_configure_proxy/0,
         maybe_configure_inet6/0]).

%% Export all for unit tests
-ifdef(TEST).
-compile(export_all).
-endif.

-ifdef (OTP_RELEASE).
  -if(?OTP_RELEASE >= 23).
    -compile({nowarn_deprecated_function, [{http_uri, encode, 1},
                                           {http_uri, parse,  1}]}).

    -ignore_xref([{http_uri, encode, 1},
                  {http_uri, parse,  1}]).
  -endif.
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
-define(CONTENT_URLENCODED, "application/x-www-form-urlencoded").

-type path_component() :: atom() | binary() | integer() | string().
-export_type([path_component/0]).

%% @public
%% @doc Build the path from a list of segments
%% @end
%%
-spec build_path([path_component()]) -> string().
build_path(Args) ->
  build_path(Args, []).


%% @public
%% @spec build_path(string(), string()) -> string()
%% @doc Build the path from a list of segments
%% @end
%%
build_path([Part | Parts], Path) ->
  build_path(Parts, string:join([Path, percent_encode(Part)], "/"));
build_path([], Path) -> Path.


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
build_uri(Scheme, Host, Port, Path, QArgs) ->
  build_uri(string:join([Scheme, "://", Host, ":", rabbit_peer_discovery_util:as_string(Port)], ""), Path, QArgs).


%% @public
%% @spec build_uri(string(), string(), proplist()) -> string()
%% @doc Build the requst URI for the given base URI, path and query args
%% @end
%%
build_uri(Base, Path, []) ->
  string:join([Base, build_path(Path)], "");
build_uri(Base, Path, QArgs) ->
  string:join([Base, string:join([build_path(Path),
                                  build_query(QArgs)], "?")], "").


%% @public
%% @spec build_query(proplist()) -> string()
%% @doc Build the query parameters string from a proplist
%% @end
%%
build_query(Args) ->
  build_query(Args, []).


%% @public
%% @spec build_query(proplist(), string()) -> string()
%% @doc Build the query parameters string from a proplist
%% @end
%%
build_query([{Key, Value} | Args], Parts) when is_atom(Key) =:= true ->
  build_query(Args, lists:merge(Parts, [string:join([percent_encode(Key),
                                                     percent_encode(Value)], "=")]));
build_query([{Key, Value} | Args], Parts) ->
  build_query(Args, lists:merge(Parts, [string:join([percent_encode(Key),
                                                     percent_encode(Value)], "=")]));
build_query([Key | Args], Parts) ->
  build_query(Args, lists:merge(Parts, [percent_encode(Key)]));
build_query([], Parts) ->
  string:join(Parts, "&").


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
  rabbit_log:debug("GET ~s", [URL]),
  HttpOpts1 = ensure_timeout(HttpOpts),
  Response = httpc:request(get, {URL, Headers}, HttpOpts1, []),
  rabbit_log:debug("Response: ~p", [Response]),
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
  URL = build_uri(Scheme, Host, Port, Path, Args),
  rabbit_log:debug("POST ~s [~p]", [URL, Body]),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(post, {URL, [], ?CONTENT_JSON, Body}, HttpOpts, []),
  rabbit_log:debug("Response: [~p]", [Response]),
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
put(Scheme, Host, Port, Path, Args, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  rabbit_log:debug("PUT ~s [~p]", [URL, Body]),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(put, {URL, [], ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  rabbit_log:debug("Response: [~p]", [Response]),
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
put(Scheme, Host, Port, Path, Args, Headers, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  rabbit_log:debug("PUT ~s [~p] [~p]", [URL, Headers, Body]),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(put, {URL, Headers, ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  rabbit_log:debug("Response: [~p]", [Response]),
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
delete(Scheme, Host, Port, Path, Args, Body) ->
  URL = build_uri(Scheme, Host, Port, Path, Args),
  rabbit_log:debug("DELETE ~s [~p]", [URL, Body]),
  HttpOpts = ensure_timeout(),
  Response = httpc:request(delete, {URL, [], ?CONTENT_URLENCODED, Body}, HttpOpts, []),
  rabbit_log:debug("Response: [~p]", [Response]),
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
          rabbit_log:debug("HTTP client proxy is not configured"),
          ok;
      _ ->
          HttpProxy       = ?CONFIG_MODULE:get(http_proxy,  ?CONFIG_MAPPING, Map),
          HttpsProxy      = ?CONFIG_MODULE:get(https_proxy, ?CONFIG_MAPPING, Map),
          ProxyExclusions = ?CONFIG_MODULE:get(proxy_exclusions, ?CONFIG_MAPPING, Map),
          rabbit_log:debug("Configured HTTP proxy: ~p, HTTPS proxy: ~p, exclusions: ~p",
                           [HttpProxy, HttpsProxy, ProxyExclusions]),
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
    {ok, {_Scheme, _UserInfo, Host, Port, _Path, _Query}} ->
      rabbit_log:debug(
        "Configuring HTTP client's ~s setting: ~p, exclusions: ~p",
        [Option, {Host, Port}, ProxyExclusions]),
      httpc:set_option(Option, {{Host, Port}, ProxyExclusions});
    {error, Reason} ->
          rabbit_log:error("Failed to set HTTP client setting ~p"
                           " with value of ~p, exclusions of ~p: ~p",
                           [Option, ProxyUrl, ProxyExclusions, {error, Reason}])
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc Support defining proxy address with or without uri scheme.
%% @end
%%--------------------------------------------------------------------
-spec parse_proxy_uri(ProxyType :: atom(), ProxyUrl :: string()) -> tuple().
parse_proxy_uri(ProxyType, ProxyUrl) ->
  case http_uri:parse(ProxyUrl) of
    {ok, Result} -> {ok, Result};
    {error, _} ->
      case ProxyType of
        proxy       -> http_uri:parse("http://" ++ ProxyUrl);
        https_proxy -> http_uri:parse("https://" ++ ProxyUrl)
      end
  end.



%% @private
%% @spec decode_body(mixed) -> list()
%% @doc Decode the response body and return a list
%% @end
%%
decode_body(_, []) -> [];
decode_body(?CONTENT_JSON, Body) ->
    case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body)) of
        {ok, Value} -> Value;
        {error, Err}  ->
            rabbit_log:error("HTTP client could not decode a JSON payload "
                                  "(JSON parser returned an error): ~p.~n",
                                  [Err]),
            {ok, []}
    end.

%% @private
%% @spec parse_response(Response) -> {ok, string()} | {error, mixed}
%% @where Response = {status_line(), headers(), Body} | {status_code(), Body}
%% @doc Decode the response body and return a list
%% @end
%%
parse_response({error, Reason}) ->
  rabbit_log:debug("HTTP Error ~p", [Reason]),
  {error, lists:flatten(io_lib:format("~p", [Reason]))};

parse_response({ok, 200, Body})  -> {ok, decode_body(?CONTENT_JSON, Body)};
parse_response({ok, 201, Body})  -> {ok, decode_body(?CONTENT_JSON, Body)};
parse_response({ok, 204, _})     -> {ok, []};
parse_response({ok, Code, Body}) ->
  rabbit_log:debug("HTTP Response (~p) ~s", [Code, Body]),
  {error, integer_to_list(Code)};

parse_response({ok, {{_,200,_},Headers,Body}}) ->
  {ok, decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body)};
parse_response({ok,{{_,201,_},Headers,Body}}) ->
  {ok, decode_body(proplists:get_value("content-type", Headers, ?CONTENT_JSON), Body)};
parse_response({ok,{{_,204,_},_,_}}) -> {ok, []};
parse_response({ok,{{_Vsn,Code,_Reason},_,Body}}) ->
  rabbit_log:debug("HTTP Response (~p) ~s", [Code, Body]),
  {error, integer_to_list(Code)}.

%% @private
%% @spec percent_encode(Value) -> string()
%% @where
%%       Value = atom() or binary() or integer() or list()
%% @doc Percent encode the query value, automatically
%%      converting atoms, binaries, or integers
%% @end
%%
percent_encode(Value) ->
  http_uri:encode(rabbit_peer_discovery_util:as_string(Value)).

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
