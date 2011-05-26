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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_links).

-include("rabbit_federation.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([go_all/0, add_binding/3, remove_binding/3, stop/1, join/1]).

go_all() -> cast_all(go).

add_binding(Serial, X, B) -> call(X, {enqueue, Serial, {add_binding, B}}).

remove_binding(Serial, X, B) -> call(X, {enqueue, Serial, {remove_binding, B}}).

stop(X) -> call(X, stop).

join(Name) ->
    pg2_fixed:create(Name),
    ok = pg2_fixed:join(Name, self()).

%%----------------------------------------------------------------------------

call(X = #exchange{}, Msg) ->
    [gen_server2:call(Pid, Msg, infinity) || Pid <- x(X)].

cast_all(Msg) ->
    [gen_server2:cast(Pid, Msg) || Pid <- all()].

all() ->
    pg2_fixed:create(rabbit_federation_exchanges),
    pg2_fixed:get_members(rabbit_federation_exchanges).

x(X) ->
    pg2_fixed:create({rabbit_federation_exchange, X}),
    pg2_fixed:get_members({rabbit_federation_exchange, X}).
