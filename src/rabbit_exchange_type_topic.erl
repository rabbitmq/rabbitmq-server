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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_exchange_type_topic).
-include("rabbit.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, route/2]).
-export([validate/1, create/2, recover/2, delete/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).
-include("rabbit_exchange_type_spec.hrl").

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type topic"},
                    {mfa,         {rabbit_registry, register,
                                   [exchange, <<"topic">>, ?MODULE]}},
                    {requires,    rabbit_registry},
                    {enables,     kernel_ready}]}).

-export([topic_matches/2]).

-ifdef(use_specs).

-spec(topic_matches/2 :: (binary(), binary()) -> boolean()).

-endif.

description() ->
    [{name, <<"topic">>},
     {description, <<"AMQP topic exchange, as per the AMQP specification">>}].

route(#exchange{name = Name},
        #delivery{message = #basic_message{routing_key = RoutingKey}}) ->
    rabbit_router:match_bindings(Name,
                                 fun (#binding{key = BindingKey}) ->
                                         topic_matches(BindingKey, RoutingKey)
                                 end).

split_topic_key(Key) ->
    string:tokens(binary_to_list(Key), ".").

topic_matches(PatternKey, RoutingKey) ->
    P = split_topic_key(PatternKey),
    R = split_topic_key(RoutingKey),
    topic_matches1(P, R).

topic_matches1(["#"], _R) ->
    true;
topic_matches1(["#" | PTail], R) ->
    last_topic_match(PTail, [], lists:reverse(R));
topic_matches1([], []) ->
    true;
topic_matches1(["*" | PatRest], [_ | ValRest]) ->
    topic_matches1(PatRest, ValRest);
topic_matches1([PatElement | PatRest], [ValElement | ValRest])
  when PatElement == ValElement ->
    topic_matches1(PatRest, ValRest);
topic_matches1(_, _) ->
    false.

last_topic_match(P, R, []) ->
    topic_matches1(P, R);
last_topic_match(P, R, [BacktrackNext | BacktrackList]) ->
    topic_matches1(P, R) or
        last_topic_match(P, [BacktrackNext | R], BacktrackList).

validate(_X) -> ok.
create(_Tx, _X) -> ok.
recover(_X, _Bs) -> ok.
delete(_Tx, _X, _Bs) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.
assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).
