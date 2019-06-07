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
%% Copyright (c) 2012-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_web_stomp_middleware).
-behavior(cowboy_middleware).

-export([execute/2]).

execute(Req, Env) ->
    #{keepalive_sup := KeepaliveSup} = Env,
    Sock = maps:get(socket, Env),
    case maps:get(handler_opts, Env, undefined) of
        undefined -> {ok, Req, Env};
        Opts when is_list(Opts) ->
            {ok, Req, Env#{handler_opts => [{keepalive_sup, KeepaliveSup},
                                            {socket, Sock}
                                            |Opts]}}
    end.
