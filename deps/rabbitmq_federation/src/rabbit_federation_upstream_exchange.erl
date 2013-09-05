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
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_federation_upstream_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation upstream exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-federation-upstream">>, ?MODULE]}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_federation.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

%%----------------------------------------------------------------------------

description() ->
    [{description,      <<"Federation upstream helper exchange">>},
     {internal_purpose, federation}].

serialise_events() -> false.

route(X = #exchange{arguments = Args},
      D = #delivery{message = #basic_message{content = Content}}) ->
    {long, MaxHops} = rabbit_misc:table_lookup(Args, ?MAX_HOPS_ARG),
    Headers = rabbit_basic:extract_headers(Content),
    case rabbit_federation_util:should_forward(Headers, MaxHops) of
        true  -> rabbit_exchange_type_fanout:route(X, D);
        false -> []
    end.

validate(#exchange{arguments = Args}) ->
    rabbit_federation_util:validate_arg(?MAX_HOPS_ARG, long, Args).

validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.
delete(_Tx, _X, _Bs) -> ok.
policy_changed(_X1, _X2) -> ok.
add_binding(_Tx, _X, _B) -> ok.
remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X = #exchange{name      = Name,
                                      arguments = Args}, ReqArgs) ->
    rabbit_misc:assert_args_equivalence(Args, ReqArgs, Name, [?MAX_HOPS_ARG]),
    rabbit_exchange:assert_args_equivalence(X, Args).
