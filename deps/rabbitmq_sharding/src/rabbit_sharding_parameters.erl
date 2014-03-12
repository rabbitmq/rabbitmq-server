-module(rabbit_sharding_parameters).

-behaviour(rabbit_policy_validator).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([register/0, validate_policy/1]).

-rabbit_boot_step({?MODULE,
                   [{description, "sharding parameters"},
                    {mfa, {rabbit_sharding_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    [rabbit_registry:register(Class, Name, ?MODULE) ||
        {Class, Name} <- [{policy_validator,  <<"sharded">>},
                          {policy_validator,  <<"shards-per-node">>},
                          {policy_validator,  <<"routing-key">>}]],
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

validate_policy(KeyList) ->
    rabbit_parameter_validation:proplist(
      <<"sharding policy definition">>,
      [{<<"sharded">>, fun rabbit_parameter_validation:boolean/2, mandatory},
       {<<"shards-per-node">>, fun validate_shards_per_node/2, mandatory},
       {<<"routing-key">>, fun rabbit_parameter_validation:binary/2, mandatory}],
      KeyList).
