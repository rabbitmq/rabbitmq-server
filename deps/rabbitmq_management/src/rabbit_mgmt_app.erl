%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2010-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_app).

-behaviour(application).
-export([start/2, stop/1]).

-include("rabbit_mgmt.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% Dummy supervisor - see Ulf Wiger's comment at
%% http://erlang.2086793.n4.nabble.com/initializing-library-applications-without-processes-td2094473.html

%% All of our actual server processes are supervised by rabbit_mgmt_sup, which
%% is started by a rabbit_boot_step (since it needs to start up before queue
%% recovery or the network being up, so it can't be part of our application).
%%
%% However, we still need an application behaviour since we need to depend on
%% the rabbit_mochiweb application and call into it once it's running. Since
%% the application behaviour needs a tree of processes to supervise, this is
%% it...
-behaviour(supervisor).
-export([init/1]).

-define(CONTEXT, rabbit_mgmt).
-define(STATIC_PATH, "priv/www").

-ifdef(trace).
-define(SETUP_WM_TRACE, true).
-else.
-define(SETUP_WM_TRACE, false).
-endif.

%% Make sure our database is hooked in *before* listening on the network or
%% recovering queues (i.e. so there can't be any events fired before it starts).
-rabbit_boot_step({rabbit_mgmt_database,
                   [{description, "management statistics database"},
                    {mfa,         {rabbit_sup, start_child,
                                   [rabbit_mgmt_global_sup]}},
                    {requires,    rabbit_event},
                    {requires,    database},
                    {enables,     recovery}]}).

start(_Type, _StartArgs) ->
    setup_wm_logging(),
    register_context(),
    case ?SETUP_WM_TRACE of
        true -> setup_wm_trace_app();
        _    -> ok
    end,
    log_startup(),
    supervisor:start_link({local,?MODULE},?MODULE,[]).

stop(_State) ->
    ok.

register_context() ->
    rabbit_mochiweb:register_context_handler(
      ?CONTEXT, "", make_loop(), "RabbitMQ Management").

make_loop() ->
    Dispatch = rabbit_mgmt_dispatcher:build_dispatcher(),
    WMLoop = rabbit_webmachine:makeloop(Dispatch),
    LocalPaths = [filename:join(module_path(M), ?STATIC_PATH) ||
                     M <- rabbit_mgmt_dispatcher:modules()],
    fun(PL, Req) ->
            Unauthorized = {401, [{"WWW-Authenticate", ?AUTH_REALM}], ""},
            Auth = Req:get_header_value("authorization"),
            case rabbit_mochiweb_util:parse_auth_header(Auth) of
                [Username, Password] ->
                    case rabbit_access_control:check_user_pass_login(
                           Username, Password) of
                        {ok, _} -> respond(Req, LocalPaths, PL, WMLoop);
                        _       -> Req:respond(Unauthorized)
                    end;
                _ ->
                    Req:respond(Unauthorized)
            end

    end.

module_path(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

respond(Req, LocalPaths, PL = {Prefix, _}, WMLoop) ->
    %% To get here we know Prefix matches the beginning of the path
    RawPath = Req:get(raw_path),
    Path = case Prefix of
               "" -> RawPath;
               _  -> string:substr(RawPath, string:len(Prefix) + 2)
           end,
    Redirect = fun(L) -> {301, [{"Location", Prefix ++ L}], ""} end,
    case Path of
        "/api/" ++ Rest when length(Rest) > 0 ->
            WMLoop(PL, Req);
        "" ->
            Req:respond(Redirect("/"));
        "/mgmt/" ->
            Req:respond(Redirect("/"));
        "/mgmt" ->
            Req:respond(Redirect("/"));
        "/" ++ Stripped ->
            serve_file(Req, Stripped, LocalPaths, Redirect)
    end.

serve_file(Req, Path, [LocalPath], _Redirect) ->
    Req:serve_file(Path, LocalPath);
serve_file(Req, Path, [LocalPath | Others], Redirect) ->
    Path1 = filename:join([LocalPath, Path]),
    case filelib:is_regular(Path1) of
        true  -> Req:serve_file(Path, LocalPath);
        false -> case filelib:is_dir(Path1) of
                     true  -> index(Req, Path, LocalPath, Redirect);
                     false -> serve_file(Req, Path, Others, Redirect)
                 end
    end.

index(Req, Path, LocalPath, Redirect) ->
    case lists:reverse(Path) of
        ""       -> Req:serve_file("index.html", LocalPath);
        "/" ++ _ -> Req:serve_file(Path ++ "index.html", LocalPath);
        _        -> Req:respond(Redirect(Path ++ "/"))
    end.

setup_wm_logging() ->
    {ok, LogDir} = application:get_env(rabbitmq_management, http_log_dir),
    case LogDir of
        none ->
            rabbit_webmachine:setup(none);
        _ ->
            rabbit_webmachine:setup(webmachine_logger),
            webmachine_sup:start_logger(LogDir)
    end.

%% This doesn't *entirely* seem to work. It fails to load a non-existent
%% image which seems to partly break it, but some stuff is usable.
setup_wm_trace_app() ->
    Loop = rabbit_webmachine:makeloop([{["wmtrace", '*'],
                                       wmtrace_resource,
                                       [{trace_dir, "/tmp"}]}]),
    rabbit_mochiweb:register_static_context(
      rabbit_mgmt_trace_static, "wmtrace/static", ?MODULE,
      "deps/webmachine/webmachine/priv/trace", none),
    rabbit_mochiweb:register_context_handler(rabbit_mgmt_trace,
                                             "wmtrace", Loop,
                                             "Webmachine tracer").

log_startup() ->
    rabbit_log:info(
      "Management plugin started. Port: ~w, path: ~s~n",
      [get_port(), rabbit_mochiweb:context_path(?CONTEXT, "/")]).

get_port() ->
    case rabbit_mochiweb:context_listener(?CONTEXT) of
        undefined ->
            exit(rabbit_mochiweb_listener_not_configured);
        {_Instance, Options} ->
            proplists:get_value(port, Options)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one,3,10},[]}}.
