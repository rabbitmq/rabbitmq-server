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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

%% TODO rename this
-module(rabbit_federation_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {cleanup, {rabbit_registry, unregister,
                               [exchange_decorator, <<"federation">>]}},
                    {enables, recovery}]}).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, route/2, active_for/1]).

%%----------------------------------------------------------------------------

description() ->
    [{description, <<"Federation exchange decorator">>}].

serialise_events(X) -> federate(X).

create(transaction, _X) ->
    ok;
create(none, X) ->
    maybe_start(X).

delete(transaction, _X, _Bs) ->
    ok;
delete(none, X, _Bs) ->
    maybe_stop(X).

policy_changed(OldX, NewX) ->
    maybe_stop(OldX),
    maybe_start(NewX).

add_binding(transaction, _X, _B) ->
    ok;
add_binding(Serial, X = #exchange{name = XName}, B) ->
    case federate(X) of
        true  -> rabbit_federation_exchange_link:add_binding(Serial, XName, B),
                 ok;
        false -> ok
    end.

remove_bindings(transaction, _X, _Bs) ->
    ok;
remove_bindings(Serial, X = #exchange{name = XName}, Bs) ->
    case federate(X) of
        true  -> rabbit_federation_exchange_link:remove_bindings(Serial, XName, Bs),
                 ok;
        false -> ok
    end.

route(_, _) -> [].

active_for(X) ->
    case federate(X) of
        true  -> noroute;
        false -> none
    end.

%%----------------------------------------------------------------------------

%% Don't federate default exchange, we can't bind to it
federate(#exchange{name = #resource{name = <<"">>}}) ->
    false;

%% Don't federate any of our intermediate exchanges. Note that we use
%% internal=true since older brokers may not declare
%% x-federation-upstream on us. Also other internal exchanges should
%% probably not be federated.
federate(#exchange{internal = true}) ->
    false;

federate(X) ->
    rabbit_federation_upstream:federate(X).

maybe_start(X = #exchange{name = XName})->
    case federate(X) of
        true  -> ok = rabbit_federation_db:prune_scratch(
                        XName, rabbit_federation_upstream:for(X)),
                 ok = rabbit_federation_exchange_link_sup_sup:start_child(X),
                 ok;
        false -> ok
    end.

maybe_stop(X = #exchange{name = XName}) ->
    case federate(X) of
        true  -> ok = rabbit_federation_exchange_link_sup_sup:stop_child(X),
                 rabbit_federation_status:remove_exchange_or_queue(XName);
        false -> ok
    end.
