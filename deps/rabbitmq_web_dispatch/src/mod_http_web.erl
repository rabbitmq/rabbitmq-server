-module(mod_http_web).

-export([start/1, stop/0, loop/2]).
-export([install_static/2]).

%% External API

start(Options) ->
    {ok, DocRoot} = application:get_env(docroot),
    {ok, Port} = application:get_env(port),
    Loop = fun (Req) ->
                   ?MODULE:loop(Req, DocRoot)
           end,
    mochiweb_http:start([{name, ?MODULE}, {port, Port}, {loop, Loop}]).

stop() ->
    mochiweb_http:stop(?MODULE).

loop(Req, DocRoot) ->
    "/" ++ Path = Req:get(path),
    case Req:get(method) of
        Method when Method =:= 'GET'; Method =:= 'HEAD' ->
            case Path of
                _ ->
                    Req:serve_file(Path, DocRoot)
            end;
        'POST' ->
            case Path of
                _ ->
                    Req:not_found()
            end;
        _ ->
            Req:respond({501, [], []})
    end.

%% The idea here is for mod_http to put all static content into this
%% directory when an application deploys a zip file containing static content
%% and to key it based on the name of the app
install_static(ModuleName, Archive) when is_list(Archive) ->
    {ok, DocRoot} = application:get_env(mod_http, docroot),
    %% TODO This should be cleaned down before any new stuff is deployed
    %% Might be an idea to do this on application startup though
    Parent = filename:join(DocRoot, ModuleName),
    case filelib:is_dir(Parent) of
        true  -> ok;
        false ->
            ok = filelib:ensure_dir(Parent),
            ok = file:make_dir(Parent)
    end,
    zip:unzip(Archive, [{cwd, Parent}]).


%% Internal API

get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.
