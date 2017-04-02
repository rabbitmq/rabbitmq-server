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
%% The Original Code is RabbitMQ Sharding Plugin
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_sharding_policy_validator).

-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/0, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding parameters"},
                    {mfa, {?MODULE, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{policy_validator,  <<"shards-per-node">>},
                          {policy_validator,  <<"routing-key">>}]],
    ok.

validate_policy(KeyList) ->
    SPN = proplists:get_value(<<"shards-per-node">>, KeyList, none),
    RKey = proplists:get_value(<<"routing-key">>, KeyList, none),
    case {SPN, RKey} of
        {none, none} ->
            ok;
        {none, _} ->
            {error, "shards-per-node must be specified", []};
        {SPN, none} ->
            validate_shards_per_node(SPN);
        {SPN, RKey} ->
            case validate_shards_per_node(SPN) of
                ok   -> validate_routing_key(RKey);
                Else -> Else
            end
    end.

%%----------------------------------------------------------------------------

validate_shards_per_node(Term) when is_number(Term) ->
    case Term >= 0 of
        true  ->
            ok;
        false ->
            {error, "shards-per-node should be greater than 0, actually was ~p",
             [Term]}
    end;
validate_shards_per_node(Term) ->
    {error, "shards-per-node should be a number, actually was ~p", [Term]}.

validate_routing_key(Term) when is_binary(Term) ->
    ok;
validate_routing_key(Term) ->
    {error, "routing-key should be binary, actually was ~p", [Term]}.
