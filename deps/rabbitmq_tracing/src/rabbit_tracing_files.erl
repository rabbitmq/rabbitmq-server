%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_files).

-include_lib("kernel/include/file.hrl").

-export([list/0, exists/1, delete/1, full_path/1]).

%%--------------------------------------------------------------------

list() ->
    {ok, Dir} = application:get_env(rabbitmq_tracing, directory),
    ok = filelib:ensure_dir(Dir ++ "/a"),
    {ok, Names} = file:list_dir(Dir),
    [file_info(Name) || Name <- Names].

exists(Name) ->
    filelib:is_regular(full_path(Name)).

delete(Name) ->
    ok = file:delete(full_path(Name)).

full_path(Name0) when is_binary(Name0) ->
    full_path(binary_to_list(Name0));
full_path(Name0) ->
    {ok, Dir} = application:get_env(rabbitmq_tracing, directory),
    case rabbit_http_util:safe_relative_path(Name0) of
        undefined -> exit(how_rude);
        Name      -> Dir ++ "/" ++ Name
    end.

%%--------------------------------------------------------------------

file_info(Name) ->
    Size = case file:read_file_info(full_path(Name), [raw]) of
               {ok, Info} ->
                   Info#file_info.size;
               {error, Error} ->
                   _ = rabbit_log:warning("error getting file info for ~s: ~p",
                                      [Name, Error]),
                   0
           end,
    [{name, list_to_binary(Name)}, {size, Size}].
