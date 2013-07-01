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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
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
      fun (Req) ->
              Host = case Req:get_header_value("host") of
                         undefined -> {ok, {IP, _Port}} = rabbit_net:sockname(
                                                            Req:get(socket)),
                                      rabbit_misc:ntoa(IP);
                         Header    -> hd(string:tokens(Header, ":"))
                     end,
              URL = rabbit_misc:format(
                      "~s://~s:~B~s",
                      [Req:get(scheme), Host, RedirectPort, Req:get(raw_path)]),
              Req:respond({301, [{"Location", URL}], ""})
      end,
      rabbit_misc:format("Redirect to port ~B", [RedirectPort])).

context_selector("") ->
    fun(_Req) -> true end;
context_selector(Prefix) ->
    Prefix1 = "/" ++ Prefix,
    fun(Req) ->
            Path = Req:get(raw_path),
            (Path == Prefix1) orelse (string:str(Path, Prefix1 ++ "/") == 1)
    end.

%% Produces a handler for use with register_handler that serves up
%% static content from a directory specified relative to the directory
%% containing the ebin directory containing the named module's beam
%% file.
static_context_handler(Prefix, Module, FSPath) ->
    {file, Here} = code:is_loaded(Module),
    ModuleRoot = filename:dirname(filename:dirname(Here)),
    LocalPath = filename:join(ModuleRoot, FSPath),
    static_context_handler(Prefix, LocalPath).

%% Produces a handler for use with register_handler that serves up
%% static content from a specified directory.
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

%% The opposite of all those register_* functions.
unregister_context(Name) ->
    rabbit_web_dispatch_registry:remove(Name).

