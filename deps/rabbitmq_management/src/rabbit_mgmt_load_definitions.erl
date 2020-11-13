%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_mgmt_load_definitions).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([maybe_load_definitions/0, maybe_load_definitions_from/2]).

%% We want to A) make sure we apply definitions before being open for
%% business (hence why we don't do this in the mgmt app startup) and
%% B) in fact do it before empty_db_check (so the defaults will not
%% get created if we don't need 'em).

-rabbit_boot_step({load_definitions,
                   [{description, "configured definitions"},
                    {mfa,         {rabbit_mgmt_load_definitions,
                                   maybe_load_definitions,
                                   []}},
                    {requires,    recovery},
                    {enables,     empty_db_check}]}).

maybe_load_definitions() ->
    case application:get_env(rabbitmq_management, load_definitions) of
        {ok, none} ->
            ok;
        {ok, FileOrDir} ->
            IsDir = filelib:is_dir(FileOrDir),
            maybe_load_definitions_from(IsDir, FileOrDir)
    end.

maybe_load_definitions_from(true, Dir) ->
    rabbit_log:info("Applying definitions from directory ~s", [Dir]),
    load_definitions_from_files(file:list_dir(Dir), Dir);
maybe_load_definitions_from(false, File) ->
    load_definitions_from_file(File).

load_definitions_from_files({ok, Filenames0}, Dir) ->
    Filenames1 = lists:sort(Filenames0),
    Filenames2 = [filename:join(Dir, F) || F <- Filenames1],
    load_definitions_from_filenames(Filenames2);
load_definitions_from_files({error, E}, Dir) ->
    rabbit_log:error("Could not read definitions from directory ~s, Error: ~p", [Dir, E]),
    {error, {could_not_read_defs, E}}.

load_definitions_from_filenames([]) ->
    ok;
load_definitions_from_filenames([File|Rest]) ->
    case load_definitions_from_file(File) of
        ok         -> load_definitions_from_filenames(Rest);
        {error, E} -> {error, {failed_to_import_definitions, File, E}}
    end.

load_definitions_from_file(File) ->
    case file:read_file(File) of
        {ok, Body} ->
            rabbit_log:info("Applying definitions from ~s", [File]),
            load_definitions(Body);
        {error, E} ->
            rabbit_log:error("Could not read definitions from ~s, Error: ~p", [File, E]),
            {error, {could_not_read_defs, {File, E}}}
    end.

load_definitions(Body) ->
    rabbit_mgmt_wm_definitions:apply_defs(
        Body, ?INTERNAL_USER,
        fun () -> ok end,
        fun (E) -> {error, E} end).
