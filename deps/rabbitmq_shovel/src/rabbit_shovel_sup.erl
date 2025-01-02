%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

-import(rabbit_shovel_config, []).

start_link() ->
    case parse_configuration(application:get_env(shovels)) of
        {ok, Configurations} ->
            supervisor:start_link({local, ?MODULE}, ?MODULE, [Configurations]);
        {error, Reason} ->
            {error, Reason}
    end.

init([Configurations]) ->
    Len = dict:size(Configurations),
    ChildSpecs = [
        #{
            id => rabbit_shovel_status,
            start => {rabbit_shovel_status, start_link, []},
            restart => transient,
            shutdown => 16#ffffffff,
            type => worker,
            modules => [rabbit_shovel_status]
        },
        #{
            id => rabbit_shovel_dyn_worker_sup_sup,
            start => {rabbit_shovel_dyn_worker_sup_sup, start_link, []},
            restart => transient,
            shutdown => 16#ffffffff,
            type => supervisor,
            modules => [rabbit_shovel_dyn_worker_sup_sup]
        }
        | make_child_specs(Configurations)
    ],
    {ok, {#{strategy => one_for_one, intensity => 2 * Len, period => 2}, ChildSpecs}}.

make_child_specs(Configurations) ->
    dict:fold(
      fun (ShovelName, ShovelConfig, Acc) ->
            [
                #{
                    id => ShovelName,
                    start => {rabbit_shovel_worker_sup, start_link, [ShovelName, ShovelConfig]},
                    restart => permanent,
                    shutdown => 16#ffffffff,
                    type => supervisor,
                    modules => [rabbit_shovel_worker_sup]
                }
                | Acc
            ]
        end, [], Configurations).

parse_configuration(undefined) ->
    {ok, dict:new()};
parse_configuration({ok, Env}) ->
    {ok, Defaults} = application:get_env(defaults),
    parse_configuration(Defaults, Env, dict:new()).

parse_configuration(_Defaults, [], Acc) ->
    {ok, Acc};
parse_configuration(Defaults, [{ShovelName, ShovelConfig} | Env], Acc) when
    is_atom(ShovelName) andalso is_list(ShovelConfig)
->
    case dict:is_key(ShovelName, Acc) of
        true ->
            {error, {duplicate_shovel_definition, ShovelName}};
        false ->
            case validate_shovel_config(ShovelName, ShovelConfig) of
                {ok, Shovel} ->
                    %% make sure the config we accumulate has any
                    %% relevant default values (discovered during
                    %% validation), applied back to it
                    Acc2 = dict:store(ShovelName, Shovel, Acc),
                    parse_configuration(Defaults, Env, Acc2);
                Error ->
                    Error
            end
    end;
parse_configuration(_Defaults, _, _Acc) ->
    {error, require_list_of_shovel_configurations}.

validate_shovel_config(ShovelName, ShovelConfig) ->
    rabbit_shovel_config:parse(ShovelName, ShovelConfig).
