%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange_type_local).
-behaviour(rabbit_exchange_type).
-include_lib("rabbit_common/include/rabbit.hrl").

-rabbit_boot_step({?MODULE, [
  {description,   "exchange type local"},
  {mfa,           {rabbit_registry, register, [exchange, <<"x-local">>, ?MODULE]}},
  {requires,      rabbit_registry},
  {enables,       kernel_ready}
]}).

-export([
  add_binding/3,
  assert_args_equivalence/2,
  create/2,
  delete/2,
  policy_changed/2,
  description/0,
  recover/2,
  remove_bindings/3,
  validate_binding/2,
  route/2,
  serialise_events/0,
  validate/1,
  info/1,
  info/2
]).

description() ->
    [{name, <<"x-local">>}, {description, <<"Picks a local binding (queue) to route via (to).">>}].

filter_local_queue(Q) ->
  {ok, Queue} = rabbit_amqqueue:lookup(Q),
  Qpid = amqqueue:get_pid(Queue),
  erlang:node(Qpid) =:= node().

route(_X=#exchange{name = Name}, _Delivery) ->
    Matches = rabbit_router:match_routing_key(Name, ['_']),
    case lists:filter(fun filter_local_queue/1, Matches) of
      [ One ] ->
        [ One ];
      [ One | Tail ] ->
          Rand = rand:uniform(length(Tail)+1),
          case Rand of
            1 ->
              [One];
            _ ->
              [lists:nth(Rand-1, Tail)]
          end;
      [] ->
        case length(Matches) of
          Len when Len < 2 -> Matches;
          Len ->
            Rand = rand:uniform(Len),
            [lists:nth(Rand, Matches)]
        end
    end.

info(_X) -> [].
info(_X, _) -> [].
serialise_events() -> false.
validate(_X) -> ok.
create(_Serial, _X) -> ok.
recover(_X, _Bs) -> ok.
delete(_Serial, _X) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Serial, _X, _B) -> ok.
remove_bindings(_Serial, _X, _Bs) -> ok.
validate_binding(_X, _B) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
