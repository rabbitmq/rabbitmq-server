%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream).
-behaviour(application).

-export([start/2, port/0]).
-export([stop/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

start(_Type, _Args) ->
    rabbit_stream_sup:start_link().

port() ->
    Listeners = rabbit_networking:node_listeners(node()),
    Port = lists:foldl(fun(#listener{port = Port, protocol = stream}, _Acc) ->
        Port;
        (_, Acc) ->
            Acc
                       end, undefined, Listeners),
    Port.


stop(_State) ->
    ok.