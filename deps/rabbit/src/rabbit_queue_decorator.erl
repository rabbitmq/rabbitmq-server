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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_queue_decorator).

-include("rabbit.hrl").

-export([select/1, set/1, register/2, unregister/1]).

-behaviour(rabbit_registry_class).

-export([added_to_rabbit_registry/2, removed_from_rabbit_registry/1]).

%%----------------------------------------------------------------------------

-callback startup(rabbit_types:amqqueue()) -> 'ok'.

-callback shutdown(rabbit_types:amqqueue()) -> 'ok'.

-callback policy_changed(rabbit_types:amqqueue(), rabbit_types:amqqueue()) ->
    'ok'.

-callback active_for(rabbit_types:amqqueue()) -> boolean().

%% called with Queue, MaxActivePriority, IsEmpty
-callback consumer_state_changed(
            rabbit_types:amqqueue(), integer(), boolean()) -> 'ok'.

%%----------------------------------------------------------------------------

added_to_rabbit_registry(_Type, _ModuleName) -> ok.
removed_from_rabbit_registry(_Type) -> ok.

select(Modules) ->
    [M || M <- Modules, code:which(M) =/= non_existing].

set(Q) -> Q#amqqueue{decorators = [D || D <- list(), D:active_for(Q)]}.

list() -> [M || {_, M} <- rabbit_registry:lookup_all(queue_decorator)].

register(TypeName, ModuleName) ->
    rabbit_registry:register(queue_decorator, TypeName, ModuleName),
    [maybe_recover(Q) || Q <- rabbit_amqqueue:list()],
    ok.

unregister(TypeName) ->
    rabbit_registry:unregister(queue_decorator, TypeName),
    [maybe_recover(Q) || Q <- rabbit_amqqueue:list()],
    ok.

maybe_recover(Q = #amqqueue{name       = Name,
                            decorators = Decs}) ->
    #amqqueue{decorators = Decs1} = set(Q),
    Old = lists:sort(select(Decs)),
    New = lists:sort(select(Decs1)),
    case New of
        Old -> ok;
        _   -> [M:startup(Q) || M <- New -- Old],
               rabbit_amqqueue:update_decorators(Name)
    end.
