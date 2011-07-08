-module(rabbit_mochiweb).

-export([all_listeners/0]).
-export([register_context_handler/4, register_static_context/5]).
-export([register_authenticated_static_context/6]).
-export([context_listener/1, context_path/2]).

-define(APP, rabbitmq_mochiweb).

all_listeners() ->
    {ok, Listeners} = application:get_env(?APP, listeners),
    {ok, Default} = application:get_env(?APP, default_listener),
    [{'*', Default} | Listeners].

%% @doc Get the path for a context; if not configured then use the
%% default given.
context_path(Context, Default) ->
    {ok, Contexts} = application:get_env(?APP, contexts),
    case proplists:get_value(Context, Contexts) of
        undefined         -> Default;
        {_Listener, Path} -> Path;
        _Listener         -> Default
    end.

%% @doc Get the listener and its options, for a given context.
context_listener(Context) ->
    {ok, Contexts} = application:get_env(?APP, contexts),
    L = case proplists:get_value(Context, Contexts) of
            undefined         -> '*';
            {Listener, _Path} -> Listener;
            Listener          -> Listener
        end,
    proplists:lookup(L, all_listeners()).

%% Handler Registration

%% Registers a dynamic selector and handler combination, with a link
%% to display in lists. Assumes that context is configured; check with
%% context_path first to make sure.
register_handler(Context, Selector, Handler, Link) ->
    rabbit_mochiweb_registry:add(Context, Selector, Handler, Link).

%% Methods for standard use cases

%% @spec register_context_handler(Context, Path, Handler, LinkText) ->
%% {ok, Path}
%% @doc Registers a dynamic handler under a fixed context path, with
%% link to display in the global context. The path may be overidden by
%% rabbitmq_mochiweb's configuration.
register_context_handler(Context, Prefix0, Handler, LinkText) ->
    Prefix = context_path(Context, Prefix0),
    Listener = context_listener(Context),
    register_handler(
      Context, context_selector(Prefix),
      fun (Req) -> Handler({Prefix, Listener}, Req) end,
      {Prefix, LinkText}),
    {ok, Prefix}.

%% @doc Convenience function registering a fully static context to
%% serve content from a module-relative directory, with
%% link to display in the global context.
register_static_context(Context, Prefix0, Module, FSPath, LinkText) ->
    Prefix = context_path(Context, Prefix0),
    register_handler(Context,
                     context_selector(Prefix),
                     static_context_handler(Prefix, Module, FSPath),
                     {Prefix, LinkText}),
    {ok, Prefix}.

%% Register a fully static but HTTP-authenticated context to
%% serve content from a module-relative directory, with link to
%% display in the global context.
register_authenticated_static_context(Context, Prefix0, Module, FSPath,
                                      LinkDesc, AuthFun) ->
    Prefix = context_path(Context, Prefix0),
    RawHandler = static_context_handler(Prefix, Module, FSPath),
    Unauthorized = {401, [{"WWW-Authenticate",
                           "Basic realm=\"" ++ LinkDesc ++ "\""}], ""},
    Handler =
        fun (Req) ->
                case rabbit_mochiweb_util:parse_auth_header(
                       Req:get_header_value("authorization")) of
                    [Username, Password] ->
                        case AuthFun(Username, Password) of
                            true -> RawHandler(Req);
                            _    -> Req:respond(Unauthorized)
                        end;
                    _ ->
                        Req:respond(Unauthorized)
                end
        end,
    register_handler(Context, context_selector(Prefix), Handler,
                     {Prefix, LinkDesc}),
    {ok, Prefix}.

context_selector("") ->
    fun(_Req) -> true end;
context_selector(Prefix) ->
    Prefix1 = "/" ++ Prefix,
    fun(Req) ->
            Path = Req:get(raw_path),
            (Path == Prefix1) orelse (string:str(Path, Prefix1 ++ "/") == 1)
    end.

%% Produces a handler for use with register_handler that serves
%% up static content from a directory specified relative to the
%% directory containing the ebin directory containing the named
%% module's beam file.
static_context_handler(Prefix, Module, FSPath) ->
    {file, Here} = code:is_loaded(Module),
    ModuleRoot = filename:dirname(filename:dirname(Here)),
    LocalPath = filename:join(ModuleRoot, FSPath),
    static_context_handler(Prefix, LocalPath).

%% @doc Produces a handler for use with register_handler that serves
%% up static content from a specified directory.
static_context_handler("", LocalPath) ->
    fun(Req) ->
            "/" ++ Path = Req:get(raw_path),
            serve_file(Req, Path, LocalPath)
    end;
static_context_handler(Prefix, LocalPath) ->
    fun(Req) ->
            "/" ++ Path = Req:get(raw_path),
            case string:substr(Path, length(Prefix) + 1) of
                ""        -> Req:respond({301, [{"Location", "/" ++ Prefix ++ "/"}], ""});
                "/" ++ P  -> serve_file(Req, P, LocalPath)
            end
    end.

serve_file(Req, Path, LocalPath) ->
    case Req:get(method) of
        Method when Method =:= 'GET'; Method =:= 'HEAD' ->
            Req:serve_file(Path, LocalPath);
        _ ->
            Req:respond({405, [{"Allow", "GET, HEAD"}],
                         "Only GET or HEAD supported for static content"})
    end.
