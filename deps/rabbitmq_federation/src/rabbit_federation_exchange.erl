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

-export([fail/2]).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

serialise_events() -> true.

route(X, Delivery) -> with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(X = #exchange{arguments = Args}) ->
    validate_arg(<<"upstream_set">>, longstr, Args),
    validate_arg(<<"type">>,         longstr, Args),
    {longstr, SetName} = rabbit_misc:table_lookup(Args, <<"upstream_set">>),
    case rabbit_federation_upstream:from_set_name(SetName, "", "") of
        {error, {F, A}} -> fail("upstream_set ~s: " ++ F, [SetName | A]);
        _               -> ok
    end,
    {longstr, TypeBin} = rabbit_misc:table_lookup(Args, <<"type">>),
    case rabbit_exchange:check_type(TypeBin) of
        'x-federation' -> fail("Type argument must not be x-federation.", []);
        _              -> ok
    end,
    with_module(X, fun (M) -> M:validate(X) end).

create(transaction, X) ->
    with_module(X, fun (M) -> M:create(transaction, X) end);
create(none, X) ->
    {ok, _} = rabbit_federation_sup:start_child(X, {upstreams(X), X}),
    with_module(X, fun (M) -> M:create(none, X) end).

delete(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:delete(transaction, X, Bs) end);
delete(none, X, Bs) ->
    rabbit_federation_link:stop(X),
    ok = rabbit_federation_sup:stop_child(X),
    with_module(X, fun (M) -> M:delete(none, X, Bs) end).

add_binding(transaction, X, B) ->
    with_module(X, fun (M) -> M:add_binding(transaction, X, B) end);
add_binding(Serial, X, B) ->
    rabbit_federation_link:add_binding(Serial, X, B),
    with_module(X, fun (M) -> M:add_binding(serial(Serial, X), X, B) end).

remove_bindings(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(transaction, X, Bs) end);
remove_bindings(Serial, X, Bs) ->
    rabbit_federation_link:remove_bindings(Serial, X, Bs),
    with_module(X, fun (M) -> M:remove_bindings(serial(Serial, X), X, Bs) end).

assert_args_equivalence(X = #exchange{name = Name, arguments = Args},
                        NewArgs) ->
    rabbit_misc:assert_args_equivalence(Args, NewArgs, Name,
                                        [<<"upstream">>, <<"type">>]),
    with_module(X, fun (M) -> M:assert_args_equivalence(X, Args) end).

%%----------------------------------------------------------------------------

serial(Serial, X) ->
    case with_module(X, fun (M) -> M:serialise_events() end) of
        true  -> Serial;
        false -> none
    end.

with_module(#exchange{ arguments = Args }, Fun) ->
    %% TODO should this be cached? It's on the publish path.
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, list_to_existing_atom(binary_to_list(Type))),
    Fun(Module).

upstreams(#exchange{name      = #resource{name = XName, virtual_host = VHost},
                    arguments = Args}) ->
    {longstr, Set} = rabbit_misc:table_lookup(Args, <<"upstream_set">>),
    rabbit_federation_upstream:from_set_name(
      Set, binary_to_list(XName), binary_to_list(VHost)).

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

fail(Fmt, Args) -> rabbit_misc:protocol_error(precondition_failed, Fmt, Args).
