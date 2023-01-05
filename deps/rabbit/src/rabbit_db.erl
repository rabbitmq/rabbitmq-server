%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db).

-include_lib("khepri/include/khepri.hrl").

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([init/0,
         reset/0,
         force_reset/0,
         force_load_on_next_boot/0,
         is_virgin_node/0, is_virgin_node/1,
         dir/0,
         ensure_dir_exists/0]).
-export([run/1]).

-export([set_migration_flag/1, is_migration_done/1]).

%% Exported to be used by various rabbit_db_* modules
-export([
         list_in_mnesia/2,
         list_in_khepri/1,
         list_in_khepri/2
        ]).

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

%% -------------------------------------------------------------------
%% DB initialization.
%% -------------------------------------------------------------------

-spec init() -> Ret when
      Ret :: ok | {error, any()}.
%% @doc Initializes the DB layer.

init() ->
    rabbit_db:run(
      #{mnesia => fun() -> init_in_mnesia() end,
        khepri => fun() ->
                          init_in_khepri(),
                          init_in_mnesia()
                  end
       }).

init_in_mnesia() ->
    IsVirgin = is_virgin_node(),
    ?LOG_DEBUG(
       "DB: this node is virgin: ~ts", [IsVirgin],
       #{domain => ?RMQLOG_DOMAIN_DB}),

    ensure_dir_exists(),
    rabbit_peer_discovery:log_configured_backend(),
    rabbit_peer_discovery:maybe_init(),

    pre_init(IsVirgin),

    case init_using_mnesia() of
        ok ->
            ?LOG_DEBUG(
               "DB: initialization successeful",
               #{domain => ?RMQLOG_DOMAIN_DB}),

            rabbit_node_monitor:update_cluster_status(),
            post_init(IsVirgin),

            ok;
        Error ->
            ?LOG_DEBUG(
               "DB: initialization failed: ~0p", [Error],
               #{domain => ?RMQLOG_DOMAIN_DB}),
            Error
    end.

pre_init(IsVirgin) ->
    Members = rabbit_db_cluster:members(),
    OtherMembers = rabbit_nodes:nodes_excl_me(Members),
    rabbit_db_cluster:ensure_feature_flags_are_in_sync(OtherMembers, IsVirgin).

post_init(false = _IsVirgin) ->
    rabbit_peer_discovery:maybe_register();
post_init(true = _IsVirgin) ->
    %% Registration handled by rabbit_peer_discovery.
    ok.

init_using_mnesia() ->
    ?LOG_DEBUG(
      "DB: initialize Mnesia",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    ok = recover_mnesia_tables(),
    ok = rabbit_mnesia:init(),
    ?assertEqual(rabbit:data_dir(), mnesia_dir()),
    rabbit_sup:start_child(mnesia_sync).

-spec reset() -> Ret when
      Ret :: ok.
%% @doc Resets the database and the node.

reset() ->
    ?LOG_INFO(
       "Resetting Rabbit", [],
      #{domain => ?RMQLOG_DOMAIN_DB}),
    ok = run(
           #{mnesia => fun() -> reset_using_mnesia() end,
             khepri => fun() -> reset_using_khepri() end
            }),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

reset_using_mnesia() ->
    ?LOG_INFO(
      "DB: resetting node",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_mnesia:reset().

reset_using_khepri() ->
    ?LOG_DEBUG(
      "DB: resetting node",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_khepri:reset().

-spec force_reset() -> Ret when
      Ret :: ok.
%% @doc Resets the database and the node.

force_reset() ->
    ?LOG_DEBUG(
       "DB: resetting node forcefully",
       #{domain => ?RMQLOG_DOMAIN_DB}),
    ok = run(
           #{mnesia => fun() -> force_reset_using_mnesia() end,
             khepri => fun() -> force_reset_using_khepri() end
            }),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

force_reset_using_mnesia() ->
    rabbit_mnesia:force_reset().

force_reset_using_khepri() ->
    rabbit_khepri:force_reset().

-spec force_load_on_next_boot() -> Ret when
      Ret :: ok.
%% @doc Requests that the database to be forcefully loaded during next boot.
%%
%% This is necessary when a node refuses to boot when the cluster is in a bad
%% state, like if critical members are MIA.

force_load_on_next_boot() ->
    run(
      #{mnesia => fun() -> force_load_on_next_boot_using_mnesia() end,
        khepri => fun() ->
                          %% TODO force load using Khepri might need to be implemented
                          %% for disaster recovery scenarios where just a minority of
                          %% nodes are accessible. Potentially, it could also be replaced
                          %% with a way to export all the data.
                          force_load_on_next_boot_using_mnesia()
                  end
       }).

force_load_on_next_boot_using_mnesia() ->
    ?LOG_DEBUG(
      "DB: resetting node forcefully",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_mnesia:force_load_next_boot().

recover_mnesia_tables() ->
    %% A failed migration can leave tables in read-only mode before enabling
    %% the feature flag. See rabbit_core_ff:final_sync_from_mnesia_to_khepri/2
    %% Unlock them here as mnesia is still fully functional.
    Tables = [Table || {Table, _} <- rabbit_table:definitions()],
    _ = [mnesia:change_table_access_mode(Table, read_write) || Table <- Tables],
    ok.

init_in_khepri() ->
    case rabbit_khepri:members() of
        [] ->
            timer:sleep(1000),
            init_in_khepri();
        Members ->
            rabbit_log:warning("Found the following metadata store members: ~p", [Members])
    end.

%% -------------------------------------------------------------------
%% is_virgin_node().
%% -------------------------------------------------------------------

-spec is_virgin_node() -> IsVirgin when
      IsVirgin :: boolean().
%% @doc Indicates if this RabbitMQ node is virgin.
%%
%% @returns `true' if the node is virgin, `false' if it is not.
%%
%% @see is_virgin_node/1.

is_virgin_node() ->
    ThisNode = node(),
    is_virgin_node(ThisNode).

-spec is_virgin_node(Node) -> IsVirgin | undefined when
      Node :: node(),
      IsVirgin :: boolean().
%% @doc Indicates if the given RabbitMQ node is virgin.
%%
%% A virgin node is a node starting for the first time. It could be a brand
%% new node or a node having been reset.
%%
%% @returns `true' if the node is virgin, `false' if it is not, or `undefined'
%% if the given node is remote and we couldn't determine it.

is_virgin_node(Node) when is_atom(Node) ->
    is_virgin_node_with_mnesia(Node).

is_virgin_node_with_mnesia(Node) when Node =:= node() ->
    rabbit_mnesia:is_virgin_node();
is_virgin_node_with_mnesia(Node) ->
    try
        erpc:call(Node, rabbit_mnesia, is_virgin_node, [], ?TIMEOUT)
    catch
        _:_ ->
            undefined
    end.

%% -------------------------------------------------------------------
%% dir().
%% -------------------------------------------------------------------

-spec dir() -> DBDir when
      DBDir :: file:filename().
%% @doc Returns the directory where the database stores its data.
%%
%% @returns the directory path.

dir() ->
    mnesia_dir().

mnesia_dir() ->
    rabbit_mnesia:dir().

%% -------------------------------------------------------------------
%% ensure_dir_exists().
%% -------------------------------------------------------------------

-spec ensure_dir_exists() -> ok | no_return().
%% @doc Ensures the database directory exists.
%%
%% @returns `ok' if it exists or throws an exception if it does not.

ensure_dir_exists() ->
    DBDir = dir() ++ "/",
    case filelib:ensure_dir(DBDir) of
        ok ->
            ok;
        {error, Reason} ->
            throw({error, {cannot_create_db_dir, DBDir, Reason}})
    end.

%% -------------------------------------------------------------------
%% run().
%% -------------------------------------------------------------------

-spec run(Funs) -> Ret when
      Funs :: #{mnesia := Fun, khepri := Fun},
      Fun :: fun(() -> Ret),
      Ret :: any().
%% @doc Runs the function corresponding to the used database engine.
%%
%% @returns the return value of `Fun'.

run(Funs)
  when is_map(Funs) andalso is_map_key(mnesia, Funs)
       andalso is_map_key(khepri, Funs) ->
    #{mnesia := MnesiaFun,
      khepri := KhepriFun} = Funs,
    case rabbit_khepri:use_khepri() of
        true ->
            run_with_khepri(KhepriFun);
        false ->
            try
                run_with_mnesia(MnesiaFun)
            catch
                _:{Type, {no_exists, Table}}
                  when Type =:= aborted orelse Type =:= error ->
                    %% We wait for the feature flag(s) to be enabled
                    %% or disabled (this is a blocking call) and
                    %% retry.
                    ?LOG_DEBUG(
                       "Mnesia function failed because table ~s "
                       "is gone or read-only; checking if the new "
                       "metadata store was enabled in parallel and "
                       "retry",
                       [Table]),
                    _ = rabbit_khepri:is_enabled(),
                    run(Funs)
            end
    end.

run_with_mnesia(Fun) ->
    Fun().

run_with_khepri(Fun) ->
    Fun().

%% -------------------------------------------------------------------
%% set_migration_flag().
%% -------------------------------------------------------------------

-spec set_migration_flag(FeatureName) -> ok when
      FeatureName :: atom().

set_migration_flag(FeatureName) ->
    rabbit_khepri:put([?MODULE, migration_done, FeatureName], true).

%% -------------------------------------------------------------------
%% is_migration_done().
%% -------------------------------------------------------------------

-spec is_migration_done(FeatureName) -> boolean() when
      FeatureName :: atom().

is_migration_done(FeatureName) ->
    case rabbit_khepri:get([?MODULE, migration_done, FeatureName]) of
        {ok, Flag} ->
            Flag;
        _ ->
            false
    end.

%% -------------------------------------------------------------------
%% list_in_mnesia().
%% -------------------------------------------------------------------

-spec list_in_mnesia(Table, Match) -> Objects when
      Table :: atom(),
      Match :: any(),
      Objects :: [any()].

list_in_mnesia(Table, Match) ->
    %% Not dirty_match_object since that would not be transactional when used in a
    %% tx context
    mnesia:async_dirty(fun () -> mnesia:match_object(Table, Match, read) end).

%% -------------------------------------------------------------------
%% list_in_khepri().
%% -------------------------------------------------------------------

-spec list_in_khepri(Path) -> Objects when
      Path :: khepri_path:pattern(),
      Objects :: [term()].

list_in_khepri(Path) ->
    list_in_khepri(Path, #{}).

-spec list_in_khepri(Path, Options) -> Objects when
      Path :: khepri_path:pattern(),
      Options :: map(),
      Objects :: [term()].

list_in_khepri(Path, Options) ->
    case rabbit_khepri:match(Path, Options) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.
