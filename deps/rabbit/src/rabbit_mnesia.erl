%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mnesia).

-include_lib("rabbit_common/include/logging.hrl").
-include_lib("kernel/include/logger.hrl").

-export([is_running/0,
         is_virgin_node/0,
         dir/0,

         ensure_mnesia_running/0,
         ensure_mnesia_not_running/0,
         wait/2
        ]).

%% Used internally in `rabbit_khepri'.
-export([mnesia_and_msg_store_files/0]).

-ifdef(TEST).
-compile(export_all).
-endif.

%%----------------------------------------------------------------------------

-export_type([cluster_status/0]).

-type cluster_status() :: {[node()], [node()], [node()]}.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

-rabbit_deprecated_feature(
   {ram_node_type,
    #{deprecation_phase => removed,
      doc_url => "https://blog.rabbitmq.com/posts/2021/08/4.0-deprecation-announcements/#removal-of-ram-nodes"
     }}).

is_running() -> mnesia:system_info(is_running) =:= yes.

-spec dir() -> file:filename().

dir() -> mnesia:system_info(directory).

ensure_mnesia_running() ->
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        starting ->
            wait_for(mnesia_running),
            ensure_mnesia_running();
        Reason when Reason =:= no; Reason =:= stopping ->
            throw({error, mnesia_not_running})
    end.

ensure_mnesia_not_running() ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            wait_for(mnesia_not_running),
            ensure_mnesia_not_running();
        Reason when Reason =:= yes; Reason =:= starting ->
            throw({error, mnesia_unexpectedly_running})
    end.

-spec wait([atom()], boolean()) -> 'ok'.

wait(TableNames, Retry) ->
    {Timeout, Retries} = retry_timeout(Retry),
    wait(TableNames, Timeout, Retries).

wait(TableNames, Timeout, Retries) ->
    %% Wait for tables must only wait for tables that have already been declared.
    %% Otherwise, node boot returns a timeout when the Khepri ff is enabled from the start
    ExistingTables = mnesia:system_info(tables),
    MissingTables = TableNames -- ExistingTables,
    TablesToMigrate = TableNames -- MissingTables,
    wait1(TablesToMigrate, Timeout, Retries).

wait1(TableNames, Timeout, Retries) ->
    %% We might be in ctl here for offline ops, in which case we can't
    %% get_env() for the rabbit app.
    ?LOG_INFO("Waiting for Mnesia tables for ~tp ms, ~tp retries left",
              [Timeout, Retries - 1]),
    Result = case mnesia:wait_for_tables(TableNames, Timeout) of
                 ok ->
                     ok;
                 {timeout, BadTabs} ->
                     AllNodes = rabbit_nodes:list_members(),
                     {error, {timeout_waiting_for_tables, AllNodes, BadTabs}};
                 {error, Reason} ->
                     AllNodes = rabbit_nodes:list_members(),
                     {error, {failed_waiting_for_tables, AllNodes, Reason}}
             end,
    case {Retries, Result} of
        {_, ok} ->
            ?LOG_INFO("Successfully synced tables from a peer"),
            ok;
        {1, {error, _} = Error} ->
            throw(Error);
        {_, {error, Error}} ->
            ?LOG_WARNING("Error while waiting for Mnesia tables: ~tp", [Error]),
            wait1(TableNames, Timeout, Retries - 1)
    end.

retry_timeout(_Retry = false) ->
    {retry_timeout(), 1};
retry_timeout(_Retry = true) ->
    Retries = case application:get_env(rabbit, mnesia_table_loading_retry_limit) of
                  {ok, T}   -> T;
                  undefined -> 10
              end,
    {retry_timeout(), Retries}.

-spec retry_timeout() -> non_neg_integer() | infinity.

retry_timeout() ->
    case application:get_env(rabbit, mnesia_table_loading_retry_timeout) of
        {ok, T}   -> T;
        undefined -> 30000
    end.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

wait_for(Condition) ->
    ?LOG_INFO("Waiting for ~tp...", [Condition]),
    timer:sleep(1000).

%% This is fairly tricky.  We want to know if the node is in the state
%% that a `reset' would leave it in.  We cannot simply check if the
%% mnesia tables aren't there because restarted RAM nodes won't have
%% tables while still being non-virgin.  What we do instead is to
%% check if the mnesia directory is non existent or empty, with the
%% exception of certain files and directories, which can be there very early
%% on node boot.
is_virgin_node() ->
    mnesia_and_msg_store_files() =:= [].

mnesia_and_msg_store_files() ->
    case rabbit_file:list_dir(dir()) of
        {error, enoent} ->
            [];
        {ok, []} ->
            [];
        {ok, List0} ->
            IgnoredFiles0 =
            [rabbit_node_monitor:cluster_status_filename(),
             rabbit_node_monitor:running_nodes_filename(),
             rabbit_node_monitor:coordination_filename(),
             rabbit_node_monitor:stream_filename(),
             rabbit_node_monitor:default_quorum_filename(),
             rabbit_node_monitor:classic_filename(),
             rabbit_node_monitor:quorum_filename(),
             rabbit_feature_flags:enabled_feature_flags_list_file(),
             rabbit_khepri:dir(),
             rabbit_plugins:user_provided_plugins_data_dir()],
            IgnoredFiles = [filename:basename(File) || File <- IgnoredFiles0],
            ?LOG_DEBUG("Files and directories found in node's data directory: ~ts, of them to be ignored: ~ts",
                            [string:join(lists:usort(List0), ", "), string:join(lists:usort(IgnoredFiles), ", ")]),
            List = List0 -- IgnoredFiles,
            ?LOG_DEBUG("Files and directories found in node's data directory sans ignored ones: ~ts", [string:join(lists:usort(List), ", ")]),
            List
    end.
