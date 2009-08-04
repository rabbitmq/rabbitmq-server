-module(rabbitmq_http_server).

-export([start/0, stop/0]).
-export([register_handler/2]).
-export([register_global_handler/1, register_context_handler/2,
         register_static_context/2, register_static_context/3]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.
        
%% @spec start() -> ok
%% @doc Start the rabbitmq_http_server server.
start() ->
    ensure_started(crypto),
    application:start(rabbitmq_http_server).

%% @spec stop() -> ok
%% @doc Stop the rabbitmq_http_server server.
stop() ->
    Res = application:stop(rabbitmq_http_server),
    application:stop(crypto),
    Res.

%% Handler Registration

register_handler(Selector, Handler) ->
    rabbitmq_http_server_registry:add(Selector, Handler).

%% Utility Methods for standard use cases

register_global_handler(Handler) ->
    rabbitmq_http_server_registry:add(fun(_) -> true end, Handler).

register_context_handler(Context, Handler) ->
    rabbitmq_http_server_registry:add(
        fun(Req) ->
            "/" ++ Path = Req:get(raw_path),
            (Path == Context) or (string:str(Path, Context ++ "/") == 1)
        end,
        Handler).

%% @spec register_static_context(Context, Module, Path) -> ok
%% @doc Registers a static docroot under the given context path.
register_static_context(Context, Module, Path) ->
    {file, Here} = code:is_loaded(Module),
    ModuleRoot = filename:dirname(filename:dirname(Here)),
    LocalPath = filename:join(ModuleRoot, Path),
    register_static_context(Context, LocalPath).

register_static_context(Context, LocalPath) ->
    rabbitmq_http_server_registry:add(
        fun(Req) ->
            "/" ++ Path = Req:get(raw_path),
            case Req:get(method) of
                Method when Method =:= 'GET'; Method =:= 'HEAD' ->
                    (Path == Context) or (string:str(Path, Context ++ "/") == 1);
                _ ->
                    false
            end        
        end,
        fun(Req) ->
            "/" ++ Path = Req:get(raw_path),
            case string:substr(Path, length(Context) + 1) of
                ""        -> Req:respond({301, [{"Location", "/" ++ Context ++ "/"}], ""});
                "/" ++ P  -> Req:serve_file(P, LocalPath)
            end
        end
    ).
