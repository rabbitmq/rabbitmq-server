-module(mod_http_web).

-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/zip.hrl").

-export([start/1, stop/0, loop/2]).
-export([install_static/1]).

%% ----------------------------------------------------------------------
%% HTTPish API
%% ----------------------------------------------------------------------

start(_Options) ->
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
            %% TODO The path is hard coded to handle mod_http_test
            case rfc4627_jsonrpc_mochiweb:handle("/mod_http_test/rpc", Req) of
                 no_match ->
                     io:format("PROCESSING REQUEST ~p~n",[Req]),
                     Req:not_found();
                 {ok, Response} ->
                     Req:respond(Response)
            end;
        _ ->
            Req:respond({501, [], []})
    end.
    
%% ----------------------------------------------------------------------
%% NON-HTTP API - maybe this should go in some other module
%% ----------------------------------------------------------------------

%% The idea here is for mod_http to put all static content into this
%% directory when an application deploys a zip file containing static content
%% and to key it based on the name of the app
install_static(Module) when is_atom(Module) ->
    install_static(atom_to_list(Module));

install_static(Module) when is_list(Module) ->
    {ok, ServerRoot} = application:get_env(mod_http, docroot),
    %% TODO This should be cleaned down before any new stuff is deployed
    %% Might be an idea to do this on application startup though
    Parent = filename:join(ServerRoot, Module),
    assert_dir(Parent),
    case filelib:is_dir(Parent) of
        true  -> ok;
        false ->
            ok = filelib:ensure_dir(Parent),
            ok = file:make_dir(Parent)
    end,
    Path = code:where_is_file(Module ++ ".app"),
    process_docroot(ServerRoot, filename:split(Path)).

assert_dir(Dir) ->
    case filelib:is_dir(Dir) of
        true  -> ok;
        false ->
            ok = filelib:ensure_dir(Dir),
            ok = file:make_dir(Dir)
    end.

process_docroot(ServerRoot, [Base, Archive, _Module, _Ebin_, _App]) ->
    Source = filename:join([Base, Archive]),
    io:format("Extracting static files from ~p archive....~n", [Source]),
    case zip:zip_open(Source, [{cwd, ServerRoot}]) of
        {ok, Handle} ->
            case zip:zip_list_dir(Handle) of
                {ok, [_Comment | Files]} ->
                    [extract(Handle, ServerRoot, F) || F <- Files];
                {error, Reason} ->
                    error_logger:error_msg("~p whilst extracting files~n",
                                           [Reason])
            end;
        {error, Reason} ->
            error_logger:error_msg("~p whilst extracting files~n", [Reason])
    end;

process_docroot(ServerRoot, Path) ->
    exit(could_not_find_doc_root, {ServerRoot, Path}).

extract(Handle, Target, #zip_file{name = Name}) ->
    case string:right(Name,1) of
        "/" -> ok;
        _ ->
            case filename:split(Name) of
                [_Module, "priv", "www" | _Rest] ->
                    case zip:zip_get(Name, Handle) of
                        {error, Reason} ->
                            error_logger:error_msg("~p whilst extracting: ~p~n",
                                                   [Reason, Name]);
                        {ok, _} ->
                            {ok, [Prefix, Suffix]} = regexp:split(Name,"priv/www/"),
                            OldName = Target ++ "/" ++ Name,
                            NewName = Target ++ "/" ++ Prefix ++ Suffix,
                            assert_dir(filename:dirname(NewName)),
                            ok = file:rename(OldName, NewName),
                            io:format("...extracted ~p to ~p~n", [Name, NewName])
                    end;
                _ ->
                    ok 
            end
    end.
