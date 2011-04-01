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
                           [exchange, <<"x-federation">>,
                            rabbit_federation_exchange]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, route/2, serialise_events/0]).
-export([validate/1, start/3, delete/3,
         add_bindings/3, remove_bindings/3, assert_args_equivalence/2]).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

serialise_events() -> true.

route(X, Delivery) ->
    with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(X = #exchange{arguments = Args}) ->
    validate_arg(<<"upstreams">>, array,   Args),
    validate_arg(<<"type">>,      longstr, Args),
    {array, Upstreams} = rabbit_misc:table_lookup(Args, <<"upstreams">>),
    [validate_upstream(U) || U <- Upstreams],
    {longstr, TypeBin} = rabbit_misc:table_lookup(Args, <<"type">>),
    Type = rabbit_exchange:check_type(TypeBin),
    case Type of
        'x-federation' -> fail("Type argument must not be x-federation.", []);
        _              -> ok
    end,
    with_module(X, fun (M) -> M:validate(X) end).

start(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:start(transaction, X, Bs) end);
start(none, X, Bs) ->
    {ok, _} = rabbit_federation_sup:start_child(exchange_to_sup_args(X, Bs)),
    with_module(X, fun (M) -> M:start(none, X, Bs) end).

delete(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:delete(transaction, X, Bs) end);
delete(none, X, Bs) ->
    rabbit_federation_links:stop(X),
    ok = rabbit_federation_sup:stop_child(exchange_to_sup_args(X, Bs)),
    with_module(X, fun (M) -> M:delete(none, X, Bs) end).

add_bindings(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:add_bindings(transaction, X, Bs) end);
add_bindings(Serial, X, Bs) ->
    %% TODO add bindings only if needed.
    [case is_federation_exchange(Dest) of
         true  -> ok;
         false -> rabbit_federation_links:add_binding(Serial, X, B)
     end || B = #binding{destination = Dest} <- Bs],
    with_module(X, fun (M) -> M:add_bindings(serial(Serial, X), X, Bs) end).

remove_bindings(transaction, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(transaction, X, Bs) end);
remove_bindings(Serial, X, Bs) ->
    [case is_federation_exchange(Dest) of
         true  -> ok;
         false -> rabbit_federation_links:remove_binding(Serial, X, B)
     end || B = #binding{destination = Dest} <- Bs],
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

%%----------------------------------------------------------------------------

with_module(#exchange{ arguments = Args }, Fun) ->
    %% TODO should this be cached? It's on the publish path.
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, rabbit_exchange:check_type(Type)),
    Fun(Module).

%%----------------------------------------------------------------------------

is_federation_exchange(Name = #resource{kind = exchange}) ->
    rabbit_federation_util:has_purpose_arg(Name);
is_federation_exchange(_) ->
    false.

%%----------------------------------------------------------------------------

exchange_to_sup_args(X = #exchange{name = Name, arguments = Args}, Bindings) ->
    {array, UpstreamTables} = rabbit_misc:table_lookup(Args, <<"upstreams">>),
    Upstreams = [rabbit_federation_util:upstream_from_table(U, Name) ||
                    {table, U} <- UpstreamTables],
    {Upstreams, X, Bindings}.

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

validate_upstream({table, Table}) ->
    Args = [{<<"host">>,         [longstr],                      true},
            {<<"protocol">>,     [longstr],                      false},
            {<<"port">>,         [byte, short, signedint, long], false},
            {<<"virtual_host">>, [longstr],                      false},
            {<<"exchange">>,     [longstr],                      false}],
    [check_arg(Table, K, Ts, M) || {K, Ts, M} <- Args];
validate_upstream({Type, Obj}) ->
    fail("Upstream ~w was of type ~s, not table", [Obj, Type]).

check_arg(Table, K, Ts, Mandatory) ->
    case {rabbit_misc:table_lookup(Table, K), Mandatory} of
        {{T,  _}, _}     -> case lists:member(T, Ts) of
                                true  -> ok;
                                false -> fail("~s should have type in ~p, "
                                              "but ~s was received",
                                              [K, Ts, T])
                            end;
        {_,       true}  -> fail("~s is mandatory", [K]);
        {_,       false} -> ok
    end.

fail(Fmt, Args) ->
    rabbit_misc:protocol_error(precondition_failed, Fmt, Args).
