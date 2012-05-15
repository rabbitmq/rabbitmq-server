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

%% TODO rename this
-module(rabbit_federation_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation exchange decorator"},
                    {mfa, {rabbit_registry, register,
                           [exchange_decorator, <<"federation">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_decorator).

-export([description/0, serialise_events/1, route/2]).
-export([create/2, delete/3, add_binding/3, remove_bindings/3]).

%%----------------------------------------------------------------------------

description() ->
    [{name, <<"federation">>},
     {description, <<"Federation exchange decorator">>}].

serialise_events(#exchange{name = XName}) -> federate(XName).

route(_X, _Delivery) -> ok.

create(transaction, _X) ->
    ok;
create(none, X = #exchange{name = XName}) ->
    case federate(XName) of
        true ->
            Set = <<"all">>,
            Upstreams = rabbit_federation_upstream:from_set(Set, X),
            ok = rabbit_federation_db:prune_scratch(XName, Upstreams),
            {ok, _} = rabbit_federation_link_sup_sup:start_child(X, {Set, X}),
            ok;
        false ->
            ok
    end.

delete(transaction, _X, _Bs) ->
    ok;
delete(none, X = #exchange{name = XName}, _Bs) ->
    case federate(XName) of
        true ->
            rabbit_federation_link:stop(XName),
            ok = rabbit_federation_link_sup_sup:stop_child(X),
            rabbit_federation_status:remove_exchange(XName),
            ok;
        false ->
            ok
    end.

add_binding(transaction, _X, _B) ->
    ok;
add_binding(Serial, #exchange{name = XName}, B) ->
    case federate(XName) of
        true ->
            rabbit_federation_link:add_binding(Serial, XName, B),
            ok;
        false ->
            ok
    end.

remove_bindings(transaction, _X, _Bs) ->
    ok;
remove_bindings(Serial, #exchange{name = XName}, Bs) ->
    case federate(XName) of
        true ->
            rabbit_federation_link:remove_bindings(Serial, XName, Bs),
            ok;
        false ->
            ok
    end.

%%----------------------------------------------------------------------------
%% TODO this is a bit noddy. OK, a lot noddy.
federate(#resource{name = <<"fed.", _/binary>>}) -> true;
federate(_)                                      -> false.
