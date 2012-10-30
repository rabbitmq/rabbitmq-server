%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mochiweb).

-export([register_context_handler/5, register_static_context/6]).
-export([unregister_context/1]).

-define(APP, rabbitmq_mochiweb).

%% Handler Registration

%% Registers a dynamic selector and handler combination, with a link
%% to display in lists. Assumes that context is configured; check with
%% context_path first to make sure.
register_handler(Name, Listener, Selector, Handler, Link) ->
    rabbit_mochiweb_registry:add(Name, Listener, Selector, Handler, Link).

%% Methods for standard use cases

%% @spec register_context_handler(Context, Path, Handler, LinkText) ->
%% {ok, Path}
%% @doc Registers a dynamic handler under a fixed context path, with
%% link to display in the global context.
register_context_handler(Name, Listener, Prefix, Handler, LinkText) ->
    register_handler(
      Name, Listener, context_selector(Prefix), Handler, {Prefix, LinkText}),
    {ok, Prefix}.

%% @doc Convenience function registering a fully static context to
%% serve content from a module-relative directory, with
%% link to display in the global context.
register_static_context(Name, Listener, Prefix, Module, FSPath, LinkText) ->
    register_handler(Name, Listener,
                     context_selector(Prefix),
                     static_context_handler(Prefix, Module, FSPath),
                     {Prefix, LinkText}),
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

unregister_context(Context) ->
    rabbit_mochiweb_registry:remove(Context).

