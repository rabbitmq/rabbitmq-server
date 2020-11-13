%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_shovel_sup).
-behaviour(supervisor2).

-export([start_link/0, init/1]).

-import(rabbit_shovel_config, []).

-include("rabbit_shovel.hrl").

start_link() ->
    case parse_configuration(application:get_env(shovels)) of
        {ok, Configurations} ->
            supervisor2:start_link({local, ?MODULE}, ?MODULE, [Configurations]);
        {error, Reason} ->
            {error, Reason}
    end.

init([Configurations]) ->
    Len = dict:size(Configurations),
    ChildSpecs = [{rabbit_shovel_status,
                   {rabbit_shovel_status, start_link, []},
                   transient, 16#ffffffff, worker,
                   [rabbit_shovel_status]},
                  {rabbit_shovel_dyn_worker_sup_sup,
                   {rabbit_shovel_dyn_worker_sup_sup, start_link, []},
                   transient, 16#ffffffff, supervisor,
                   [rabbit_shovel_dyn_worker_sup_sup]} |
                  make_child_specs(Configurations)],
    {ok, {{one_for_one, 2*Len, 2}, ChildSpecs}}.

make_child_specs(Configurations) ->
    dict:fold(
      fun (ShovelName, ShovelConfig, Acc) ->
              [{ShovelName,
                {rabbit_shovel_worker_sup, start_link,
                    [ShovelName, ShovelConfig]},
                permanent,
                16#ffffffff,
                supervisor,
                [rabbit_shovel_worker_sup]} | Acc]
      end, [], Configurations).

parse_configuration(undefined) ->
    {ok, dict:new()};
parse_configuration({ok, Env}) ->
    {ok, Defaults} = application:get_env(defaults),
    parse_configuration(Defaults, Env, dict:new()).

parse_configuration(_Defaults, [], Acc) ->
    {ok, Acc};
parse_configuration(Defaults, [{ShovelName, ShovelConfig} | Env], Acc)
  when is_atom(ShovelName) andalso is_list(ShovelConfig) ->
    case dict:is_key(ShovelName, Acc) of
        true  -> {error, {duplicate_shovel_definition, ShovelName}};
        false -> case validate_shovel_config(ShovelName, ShovelConfig) of
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
