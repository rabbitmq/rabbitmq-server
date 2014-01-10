-module(rabbit_sharding_parameters).

-behaviour(rabbit_runtime_parameter).
-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([validate/4, notify/4, notify_clear/3]).
-export([register/0, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding parameters"},
                    {mfa, {rabbit_sharding_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{runtime_parameter, <<"shard">>},
                          {runtime_parameter, <<"shard-definition">>},
                          {policy_validator,  <<"shard-definition">>}]],
    ok.

validate(_VHost, <<"shard">>, <<"shards-per-node">>, Term) ->
    validate_shards_per_node(<<"shards-per-node">>, Term);

validate(_VHost, <<"shard">>, <<"routing-key">>, Term) ->
    rabbit_parameter_validation:binary(<<"routing-key">>, Term);

validate(_VHost, <<"shard">>, <<"local-username">>, Term) ->
    rabbit_parameter_validation:binary(<<"local-username">>, Term);

validate(_VHost, <<"shard-definition">>, Name, Term) ->
    rabbit_parameter_validation:proplist(
       Name,
       [{<<"local-username">>, fun rabbit_parameter_validation:binary/2, mandatory},
        {<<"shards-per-node">>, fun validate_shards_per_node/2, optional},
        {<<"routing-key">>, fun rabbit_parameter_validation:binary/2, optional}],
      Term);

validate(_VHost, _Component, Name, _Term) ->
    {error, "name not recognised: ~p", [Name]}.

%% If the user wants to reduce the shards number, we can't
%% delete queues, but when adding new nodes, those nodes will
%% have the new parameter.
notify(VHost, <<"shard">>, <<"shards-per-node">>, _Term) ->
    rabbit_sharding_shard:update_shards(VHost, shards_per_node),
    ok;

notify(VHost, <<"shard">>, <<"routing-key">>, _Term) ->
    rabbit_sharding_shard:update_shards(VHost, all),
    ok;

notify(_VHost, <<"shard">>, _Name, _Term) ->
    ok;

%% Maybe increase shard number by declaring new queues 
%% in case shards-per-node increased.
%% We can't delete extra queues because the user might have messages on them.
%% We just ensure that there are SPN number of queues.
notify(VHost, <<"shard-definition">>, Name, _Term) ->
    rabbit_sharding_shard:update_named_shard(VHost, Name),
    ok.

notify_clear(VHost, <<"shard">>, <<"shards-per-node">>) ->
    rabbit_sharding_shard:update_shards(VHost, shards_per_node),
    ok;

notify_clear(VHost, <<"shard">>, <<"routing-key">>) ->
    rabbit_sharding_shard:update_shards(VHost, all),
    ok;

%% A shard definition is gone. We can't remove queues so
%% we resort to defaults when declaring queues or while
%% intercepting channel methods.
%% 1) don't care about connection param changes. They will be
%%    used automatically next time we need to declare a queue.
%% 2) we need to bind the queues using the new routing key
%%    and unbind them from the old one.
notify_clear(VHost, <<"shard-definition">>, Name) ->
    rabbit_sharding_shard:update_named_shard(VHost, Name),
    ok.

validate_shards_per_node(Name, Term) when is_number(Term) ->
    case Term >= 0 of
        true  ->
            ok;
        false ->
            {error, "~s should be greater than 0, actually was ~p", [Name, Term]}
    end;
validate_shards_per_node(Name, Term) ->
    {error, "~s should be number, actually was ~p", [Name, Term]}.

%%----------------------------------------------------------------------------

validate_policy([{<<"shard-definition">>, Value}])
  when is_binary(Value) ->
    ok;
validate_policy([{<<"shard-definition">>, Value}]) ->
    {error, "~p is not a valid shard name", [Value]}.
