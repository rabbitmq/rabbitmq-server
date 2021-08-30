%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_dispatch).

-export([register_context_handler/5, register_static_context/6]).
-export([register_port_redirect/4]).
-export([unregister_context/1]).

%% Handler Registration

%% Registers a dynamic selector and handler combination, with a link
%% to display in lists.
register_handler(Name, Listener, Selector, Handler, Link) ->
    rabbit_web_dispatch_registry:add(Name, Listener, Selector, Handler, Link).

%% Methods for standard use cases

%% Registers a dynamic handler under a fixed context path, with link
%% to display in the global context.
register_context_handler(Name, Listener, Prefix, Handler, LinkText) ->
    register_handler(
      Name, Listener, context_selector(Prefix), Handler, {Prefix, LinkText}),
    {ok, Prefix}.

%% Convenience function registering a fully static context to serve
%% content from a module-relative directory, with link to display in
%% the global context.
register_static_context(Name, Listener, Prefix, Module, FSPath, LinkText) ->
    register_handler(Name, Listener,
                     context_selector(Prefix),
                     static_context_handler(Prefix, Module, FSPath),
                     {Prefix, LinkText}),
    {ok, Prefix}.

%% A context which just redirects the request to a different port.
register_port_redirect(Name, Listener, Prefix, RedirectPort) ->
    register_context_handler(
      Name, Listener, Prefix,
      cowboy_router:compile([{'_', [{'_', rabbit_cowboy_redirect, RedirectPort}]}]),
      rabbit_misc:format("Redirect to port ~B", [RedirectPort])).

context_selector("") ->
    fun(_Req) -> true end;
context_selector(Prefix) ->
    Prefix1 = list_to_binary("/" ++ Prefix),
    fun(Req) ->
            Path = cowboy_req:path(Req),
            (Path == Prefix1) orelse (binary:match(Path, << Prefix1/binary, $/ >>) =/= nomatch)
    end.

%% Produces a handler for use with register_handler that serves up
%% static content from a directory specified relative to the application
%% (owning the module) priv directory, or to the directory containing
%% the ebin directory containing the named module's beam file.
static_context_handler(Prefix, Module, FSPath) ->
    FSPathInPriv = re:replace(FSPath, "^priv/?", "", [{return, list}]),
    case application:get_application(Module) of
        {ok, App} when FSPathInPriv =/= FSPath ->
            %% The caller indicated a file in the application's `priv`
            %% dir.
            cowboy_router:compile([{'_', [
                {"/" ++ Prefix, cowboy_static,
                 {priv_file, App, filename:join(FSPathInPriv, "index.html")}},
                {"/" ++ Prefix ++ "/[...]", cowboy_static,
                 {priv_dir, App, FSPathInPriv}}
            ]}]);
        _ ->
            %% The caller indicated a file we should access directly.
            {file, Here} = code:is_loaded(Module),
            ModuleRoot = filename:dirname(filename:dirname(Here)),
            LocalPath = filename:join(ModuleRoot, FSPath),
            cowboy_router:compile([{'_', [
                {"/" ++ Prefix, cowboy_static,
                 {file, LocalPath ++ "/index.html"}},
                {"/" ++ Prefix ++ "/[...]", cowboy_static,
                 {dir, LocalPath}}
            ]}])
    end.

%% The opposite of all those register_* functions.
unregister_context(Name) ->
    rabbit_web_dispatch_registry:remove(Name).

