-module(rabbit_rh_plugin).

-include("rabbit_rh_plugin.hrl").

-export([setup_schema/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "recent history exchange type"},
                    {mfa, {rabbit_rh_plugin, setup_schema, []}},
                    {mfa, {rabbit_registry, register, [exchange, <<"x-recent-history">>, rabbit_exchange_type_rh]}},
                    {requires, rabbit_registry},
                    {enables, exchange_recovery}]}).

%% private

setup_schema() ->
    case mnesia:create_table(?RH_TABLE,
                             [{attributes, record_info(fields, cached)},
                              {record_name, cached},
                              {type, set}]) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, ?RH_TABLE}} -> ok
    end.