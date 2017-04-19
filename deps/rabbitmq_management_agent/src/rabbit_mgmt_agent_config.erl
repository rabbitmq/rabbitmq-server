-module(rabbit_mgmt_agent_config).

-export([get_env/1, get_env/2]).

%% some people have reasons to only run with the agent enabled:
%% make it possible for them to configure key management app
%% settings such as rates_mode.
get_env(Key) ->
    rabbit_misc:get_env(rabbitmq_management, Key,
                        rabbit_misc:get_env(rabbitmq_management_agent, Key,
                                            undefined)).

get_env(Key, Default) ->
    rabbit_misc:get_env(rabbitmq_management, Key,
                        rabbit_misc:get_env(rabbitmq_management_agent, Key,
                                            Default)).
