-module(trust_store_http_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    init_config(),
    Directory = get_directory(),
    Port = get_port(),
    Dispatch = cowboy_router:compile([
        {'_', [
               {"/",        trust_store_list_handler, []},
               {"/invalid", trust_store_invalid_handler, []},
               {"/certs/[...]", cowboy_static,
                {dir, Directory, [{mimetypes, {<<"text">>, <<"html">>, []}}]}}]}
    ]),
    _ = case get_ssl_options() of
        undefined  -> start_http(Dispatch, Port);
        SslOptions -> start_https(Dispatch, Port, SslOptions)
    end,
    trust_store_http_sup:start_link().

stop(_State) ->
    ok.

start_http(Dispatch, undefined) ->
    start_http(Dispatch, 8080);
start_http(Dispatch, Port) ->
    {ok, _} = cowboy:start_clear(trust_store_http_listener,
                                 [{port, Port}],
                                 #{env => #{dispatch => Dispatch}}).

start_https(Dispatch, undefined, SslOptions) ->
    start_https(Dispatch, 8443, SslOptions);
start_https(Dispatch, Port, SslOptions) ->
    {ok, _} = cowboy:start_tls(trust_store_https_listener,
                               [{port, Port}] ++ SslOptions,
                               #{env => #{dispatch => Dispatch}}).

get_directory() ->
    Dir = case os:getenv("CERT_DIR") of
        false ->
            case application:get_env(trust_store_http, directory, undefined) of
                undefined ->
                    {ok, CurrentDir} = file:get_cwd(),
                    CurrentDir;
                AppDir -> AppDir
            end;
        EnvDir -> EnvDir
    end,
    application:set_env(trust_store_http, directory, Dir),
    Dir.

get_ssl_options() ->
    application:get_env(trust_store_http, ssl_options, undefined).

get_port() ->
    case os:getenv("PORT") of
        false   ->
            application:get_env(trust_store_http, port, undefined);
        PortEnv ->
            Port = list_to_integer(PortEnv),
            application:set_env(trust_store_http, port, Port),
            Port
    end.

init_config() ->
    case os:getenv("CONFIG_FILE") of
        false -> ok;
        ConfigFile ->
            case file:consult(ConfigFile) of
                {ok, [Config]} ->
                    lists:foreach(fun({App, Conf}) ->
                        [application:set_env(App, Key, Val)
                         || {Key, Val} <- Conf]
                    end,
                    Config);
                _ -> ok
            end
    end.
