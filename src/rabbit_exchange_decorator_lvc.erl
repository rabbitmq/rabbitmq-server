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
%% Copyright (c) 2013-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_decorator_lvc).
-include("rabbit.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "LVC exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"lvc">>, ?MODULE]}},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator_route, <<"lvc">>, ?MODULE]}},
                    {mfa, {rabbit_registry, register,
                           [policy_validator, <<"lvc">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-behaviour(rabbit_exchange_decorator).
-behaviour(rabbit_policy_validator).

-export([description/0, serialise_events/1]).
-export([create/2, delete/3, add_binding/3, remove_bindings/3,
         policy_changed/3, route/2]).
-export([validate_policy/1]).

%%----------------------------------------------------------------------------

description() ->
    [{description, <<"LVC exchange decorator">>}].

serialise_events(_) -> false.

create(_Tx, _X) -> ok.

delete(_Tx, _X, _Bs) -> ok.

add_binding(none, X, #binding{key         = Key,
                              destination = #resource{kind = queue} = Queue}) ->
    case policy(X) of
        false ->
            ok;
        _ ->
            rabbit_amqqueue:with(
              queue_name(X, Key),
              fun (#amqqueue{pid = QPid}) ->
                              gen_server2:call(QPid, {copy, Queue})
              end),
             ok
    end;
add_binding(_Tx, _X, _B) ->
    ok.

remove_bindings(transaction, _X, _Bs) ->
    ok;
remove_bindings(none, X = #exchange{name = _XName}, _Bs) ->
    case policy(X) of
        false -> ok;
        _Max  -> ok % TODO: no key in binding?
    end.

route(X, #delivery{message = #basic_message{routing_keys = RKs}}) ->
    case policy(X) of
        false ->
            [];
        Max   ->
            [begin
               rabbit_amqqueue:declare(queue_name(X, RK), false, false,
                                       [{<<"x-max-length">>, long, Max}],
                                       none),
               queue_name(X, RK)
             end || RK <- RKs]
    end.

policy_changed(_Tx, _OldX, _NewX) -> ok.

%%----------------------------------------------------------------------------

validate_policy([{<<"lvc">>, MaxCache}]) when is_integer(MaxCache) ->
    ok;
validate_policy(Invalid) ->
    {error, "~p invalid LVC policy", [Invalid]}.

%%----------------------------------------------------------------------------

queue_name(#exchange{name = #resource{virtual_host = VHostPath,
                                      kind         = exchange,
                                      name         = Name}}, Key) ->
    rabbit_misc:r(
      VHostPath, queue,
      list_to_binary([Name, ".", Key, ".",
                     base64:encode(erlang:md5(term_to_binary({Name, Key})))])).


policy(#exchange{} = X) ->
    case rabbit_policy:get(<<"lvc">>, X) of
        {ok, Max}          -> Max;
        {error, not_found} -> false
    end.
