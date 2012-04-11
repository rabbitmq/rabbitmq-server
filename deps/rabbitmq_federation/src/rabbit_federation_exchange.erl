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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, create/2, delete/3,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

serialise_events() -> true.

route(X, Delivery) -> with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(#exchange{arguments = Args} = X) ->
    rabbit_federation_util:validate_arg(<<"upstream-set">>, longstr, Args),
    rabbit_federation_util:validate_arg(<<"type">>,         longstr, Args),
    {longstr, TypeBin} = rabbit_misc:table_lookup(Args, <<"type">>),
    case rabbit_exchange:check_type(TypeBin) of
        'x-federation' -> rabbit_federation_util:fail(
                            "Type argument must not be x-federation.", []);
        _              -> ok
    end,
    with_module(X, fun (M) -> M:validate(X) end).

create(transaction, X) ->
    with_module(X, fun (M) -> M:create(transaction, X) end);
create(none, X = #exchange{name      = XName,
                           arguments = Args}) ->
    {longstr, Set} = rabbit_misc:table_lookup(Args, <<"upstream-set">>),
    Upstreams = rabbit_federation_upstream:from_set(Set, XName),
    ok = rabbit_federation_db:prune_scratch(XName, Upstreams),
    {ok, _} = rabbit_federation_link_sup_sup:start_child(XName, {Set, XName}),
    with_module(X, fun (M) -> M:create(none, X) end).

delete(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:delete(transaction, X, Bs) end);
delete(none, X = #exchange{name = XName}, Bs) ->
    rabbit_federation_link:stop(XName),
    ok = rabbit_federation_link_sup_sup:stop_child(XName),
    rabbit_federation_status:remove_exchange(XName),
    with_module(X, fun (M) -> M:delete(none, X, Bs) end).

add_binding(transaction, X, B) ->
    with_module(X, fun (M) -> M:add_binding(transaction, X, B) end);
add_binding(Serial, X = #exchange{name = XName}, B) ->
    rabbit_federation_link:add_binding(Serial, XName, B),
    with_module(X, fun (M) -> M:add_binding(serial(Serial, X), X, B) end).

remove_bindings(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(transaction, X, Bs) end);
remove_bindings(Serial, X = #exchange{name = XName}, Bs) ->
    rabbit_federation_link:remove_bindings(Serial, XName, Bs),
    with_module(X, fun (M) -> M:remove_bindings(serial(Serial, X), X, Bs) end).

assert_args_equivalence(X = #exchange{name = XName, arguments = Args},
                        NewArgs) ->
    rabbit_misc:assert_args_equivalence(Args, NewArgs, XName,
                                        [<<"upstream">>, <<"type">>]),
    with_module(X, fun (M) -> M:assert_args_equivalence(X, Args) end).

%%----------------------------------------------------------------------------

serial(Serial, X) ->
    case with_module(X, fun (M) -> M:serialise_events() end) of
        true  -> Serial;
        false -> none
    end.

with_module(#exchange{arguments = Args}, Fun) ->
    %% TODO should this be cached? It's on the publish path.
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, list_to_existing_atom(binary_to_list(Type))),
    Fun(Module).
