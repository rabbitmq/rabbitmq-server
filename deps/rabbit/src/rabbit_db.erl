%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-define(PT_KEY_INIT_FINISHED, {?MODULE, node(), initialisation_finished}).

-export([init/0,
         reset/0,
         force_reset/0,
         force_load_on_next_boot/0,
         is_virgin_node/0, is_virgin_node/1,
         dir/0,
         ensure_dir_exists/0,
         is_init_finished/0,
         clear_init_finished/0]).

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
    IsVirgin = is_virgin_node(),
    ?LOG_DEBUG(
       "DB: this node is virgin: ~ts", [IsVirgin],
       #{domain => ?RMQLOG_DOMAIN_DB}),

    ensure_dir_exists(),
    rabbit_peer_discovery:maybe_init(),
    rabbit_peer_discovery:maybe_register(),

    pre_init(IsVirgin),

    case IsVirgin of
        true ->
            %% At this point, the database backend could change if the node
            %% joins a cluster and that cluster uses a different database.
            ?LOG_INFO(
               "DB: virgin node -> run peer discovery",
               #{domain => ?RMQLOG_DOMAIN_DB}),
            rabbit_peer_discovery:sync_desired_cluster();
        false ->
            ok
    end,

    Ret = case rabbit_khepri:is_enabled() of
              true  -> init_using_khepri(IsVirgin);
              false -> init_using_mnesia(IsVirgin)
          end,
    case Ret of
        ok ->
            ?LOG_DEBUG(
               "DB: initialization successeful",
               #{domain => ?RMQLOG_DOMAIN_DB}),

            init_finished(),

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

init_using_mnesia(_IsVirgin) ->
    ?LOG_DEBUG(
      "DB: initialize Mnesia",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    ok = rabbit_mnesia:init(),
    ?assertEqual(rabbit:data_dir(), mnesia_dir()),
    rabbit_sup:start_child(mnesia_sync).

init_using_khepri(IsVirgin) ->
    ?LOG_DEBUG(
      "DB: initialize Khepri",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_khepri:init(IsVirgin).

init_finished() ->
    %% Used during initialisation by rabbit_logger_exchange_h.erl
    %% If an exchange logger is configured, it needs to declare the
    %% exchange. For this, it requires the metadata store to be
    %% initialised. The initialisation happens on a rabbit boot step,
    %% after the second phase of the prelaunch where the logger is
    %% configured.
    %% Using this persistent term the logger exchange can delay
    %% declaring the exchange until the metadata store is ready.
    persistent_term:put(?PT_KEY_INIT_FINISHED, true).

is_init_finished() ->
    persistent_term:get(?PT_KEY_INIT_FINISHED, false).

clear_init_finished() ->
    _ = persistent_term:erase(?PT_KEY_INIT_FINISHED),
    ok.

-spec reset() -> Ret when
      Ret :: ok.
%% @doc Resets the database and the node.

reset() ->
    ok = case rabbit_khepri:is_enabled() of
             true  -> reset_using_khepri();
             false -> reset_using_mnesia()
         end,
    post_reset().

reset_using_mnesia() ->
    ?LOG_INFO(
      "DB: resetting node (using Mnesia)",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_mnesia:reset().

reset_using_khepri() ->
    ?LOG_DEBUG(
      "DB: resetting node (using Khepri)",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_khepri:reset().

-spec force_reset() -> Ret when
      Ret :: ok.
%% @doc Resets the database and the node.

force_reset() ->
    ok = case rabbit_khepri:is_enabled() of
             true  -> force_reset_using_khepri();
             false -> force_reset_using_mnesia()
         end,
    post_reset().

force_reset_using_mnesia() ->
    ?LOG_DEBUG(
      "DB: resetting node forcefully (using Mnesia)",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_mnesia:force_reset().

force_reset_using_khepri() ->
    ?LOG_DEBUG(
      "DB: resetting node forcefully (using Khepri)",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_khepri:force_reset().

-spec force_load_on_next_boot() -> Ret when
      Ret :: ok.
%% @doc Requests that the database to be forcefully loaded during next boot.
%%
%% This is necessary when a node refuses to boot when the cluster is in a bad
%% state, like if critical members are MIA.

force_load_on_next_boot() ->
    %% TODO force load using Khepri might need to be implemented for disaster
    %% recovery scenarios where just a minority of nodes are accessible.
    %% Potentially, it could also be replaced with a way to export all the
    %% data.
    case rabbit_khepri:is_enabled() of
        true  -> ok;
        false -> force_load_on_next_boot_using_mnesia()
    end.

force_load_on_next_boot_using_mnesia() ->
    ?LOG_DEBUG(
      "DB: force load on next boot (using Mnesia)",
      #{domain => ?RMQLOG_DOMAIN_DB}),
    rabbit_mnesia:force_load_next_boot().

post_reset() ->
    rabbit_feature_flags:reset(),

    %% The cluster status files that RabbitMQ uses when Mnesia is the database
    %% are initially created from rabbit_prelaunch_cluster. However, it will
    %% only be done once the `rabbit` app is restarted. Meanwhile, they are
    %% missing and the CLI or the testsuite may rely on them. Indeed, after
    %% the reset, Mnesia is assumed to be the database and the cluster status
    %% files should have been created the first time the application was
    %% started already.
    ThisNode = node(),
    rabbit_node_monitor:write_cluster_status({[ThisNode], [ThisNode], []}),

    ok.

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
    case rabbit_khepri:is_enabled() of
        true  -> is_virgin_node_using_khepri();
        false -> is_virgin_node_using_mnesia()
    end.

is_virgin_node_using_mnesia() ->
    rabbit_mnesia:is_virgin_node().

is_virgin_node_using_khepri() ->
    case rabbit_khepri:is_empty() of
        {error, _} -> true;
        IsEmpty    -> IsEmpty
    end.

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

is_virgin_node(Node) when Node =:= node() ->
    is_virgin_node();
is_virgin_node(Node) when is_atom(Node) ->
    try
        erpc:call(Node, ?MODULE, is_virgin_node, [], ?TIMEOUT)
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
    case rabbit_khepri:is_enabled() of
        true  -> khepri_dir();
        false -> mnesia_dir()
    end.

mnesia_dir() ->
    rabbit_mnesia:dir().

khepri_dir() ->
    rabbit_khepri:dir().

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
%% list_in_mnesia().
%% -------------------------------------------------------------------

-spec list_in_mnesia(Table, Match) -> Objects when
      Table :: atom(),
      Match :: any(),
      Objects :: [any()].

list_in_mnesia(Table, Match) ->
    %% Not dirty_match_object since that would not be transactional when used
    %% in a tx context
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
