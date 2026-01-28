%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_shovel_sup).
-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").
-include_lib("logging.hrl").

-export([start_link/0, init/1]).

start_link() ->
    case parse_configuration(application:get_env(shovels)) of
        {ok, Configurations} ->
            supervisor:start_link({local, ?MODULE}, ?MODULE, [Configurations]);
        {error, Reason} ->
            {error, Reason}
    end.

init([Configurations]) ->
    OpMode = rabbit_shovel_operating_mode:operating_mode(),
    ?LOG_DEBUG("Shovel: operating mode set to ~ts", [OpMode]),

    ok = maybe_register_khepri_projection(),

    Len = dict:size(Configurations),
    SupTreeSpecs = supervisor_tree_child_specs(OpMode),
    StaticShovelSpecs = static_shovel_child_specs(OpMode, Configurations),

    ChildSpecs = SupTreeSpecs ++ StaticShovelSpecs,
    Opts = #{strategy => one_for_one, intensity => 2 * Len, period => 2},
    {ok, {Opts, ChildSpecs}}.

maybe_register_khepri_projection() ->
    case rabbit_khepri:is_enabled() of
        true ->
            case rabbit_shovel_index:register_projection() of
                ok -> ok;
                {error, exists} -> ok;
                {error, {khepri, projection_already_exists, _}} -> ok;
                {error, Reason} ->
                    ?LOG_WARNING("Shovel: failed to register Khepri projection: ~tp", [Reason]),
                    ok
            end;
        false ->
            ok
    end.

supervisor_tree_child_specs(standard) ->
    [
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
    ];
supervisor_tree_child_specs(_NonStandardOpMode) ->
    [].


static_shovel_child_specs(standard, Configurations) ->
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
        end, [], Configurations);
static_shovel_child_specs(_NonStandardOpMode, _Configurations) ->
    %% when operating in a non-standard mode, do not start any shovels
    [].

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
