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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_decorator).

%% This is like an exchange type except that:
%%
%% 1) It applies to all exchanges as soon as it is installed, therefore
%% 2) It is not allowed to affect validation, so no validate/1 or
%%    assert_args_equivalence/2
%%
%% It's possible in the future we might make decorators
%% able to manipulate messages as they are published.

-ifdef(use_specs).

-type(tx() :: 'transaction' | 'none').
-type(serial() :: pos_integer() | tx()).

-callback description() -> [proplists:property()].

%% Should Rabbit ensure that all binding events that are
%% delivered to an individual exchange can be serialised? (they
%% might still be delivered out of order, but there'll be a
%% serial number).
-callback serialise_events(rabbit_types:exchange()) -> boolean().

%% called after declaration and recovery
-callback create(tx(), rabbit_types:exchange()) -> 'ok'.

%% called after exchange (auto)deletion.
-callback delete(tx(), rabbit_types:exchange(), [rabbit_types:binding()]) ->
    'ok'.

%% called when the policy attached to this exchange changes.
-callback policy_changed(rabbit_types:exchange(), rabbit_types:exchange()) ->
    'ok'.

%% called after a binding has been added or recovered
-callback add_binding(serial(), rabbit_types:exchange(),
                      rabbit_types:binding()) -> 'ok'.

%% called after bindings have been deleted.
-callback remove_bindings(serial(), rabbit_types:exchange(),
                          [rabbit_types:binding()]) -> 'ok'.

%% %% called after exchange routing
%% %% return value is a list of queues to be added to the list of
%% %% destination queues. decorators must register separately for
%% %% this callback using exchange_decorator_route.
%% -callback route(rabbit_types:exchange(), rabbit_types:delivery()) ->
%%     [rabbit_amqqueue:name()].

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{description, 0}, {serialise_events, 1}, {create, 2}, {delete, 3},
     {policy_changed, 2}, {add_binding, 3}, {remove_bindings, 3}];
behaviour_info(_Other) ->
    undefined.

-endif.
