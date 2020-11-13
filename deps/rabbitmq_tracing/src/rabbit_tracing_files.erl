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
%%  Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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
    Size = case file:read_file_info(full_path(Name)) of
               {ok, Info} ->
                   Info#file_info.size;
               {error, Error} ->
                   rabbit_log:warning("error getting file info for ~s: ~p",
                                      [Name, Error]),
                   0
           end,
    [{name, list_to_binary(Name)}, {size, Size}].
