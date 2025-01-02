%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_rtparams).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

-export([set/2, set/4,
         get/1,
         get_all/0, get_all/2,
         delete/1, delete/3,
         delete_vhost/1]).

-export([khepri_vhost_rp_path/3,
         khepri_global_rp_path/1
        ]).

-define(MNESIA_TABLE, rabbit_runtime_parameters).
-define(KHEPRI_GLOBAL_PROJECTION, rabbit_khepri_global_rtparam).
-define(KHEPRI_VHOST_PROJECTION, rabbit_khepri_per_vhost_rtparam).
-define(any(Value), case Value of
                        '_' -> ?KHEPRI_WILDCARD_STAR;
                        _ -> Value
                    end).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Key, Term) -> Ret when
      Key :: atom(),
      Term :: any(),
      Ret :: new | {old, Term}.
%% @doc Sets the new value of the global runtime parameter named `Key'.
%%
%% @returns `new' if the runtime parameter was not set before, or `{old,
%% OldTerm}' with the old value.
%%
%% @private

set(Key, Term) when is_atom(Key) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> set_in_mnesia(Key, Term) end,
        khepri => fun() -> set_in_khepri(Key, Term) end}).

set_in_mnesia(Key, Term) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> set_in_mnesia_tx(Key, Term) end).

set_in_khepri(Key, Term) ->
    Path = khepri_rp_path(Key),
    Record = #runtime_parameters{key   = Key,
                                 value = Term},
    case rabbit_khepri:adv_put(Path, Record) of
        {ok, #{data := Params}} ->
            {old, Params#runtime_parameters.value};
        {ok, _} ->
            new
    end.

-spec set(VHostName, Comp, Name, Term) -> Ret when
      VHostName :: vhost:name(),
      Comp :: binary(),
      Name :: binary() | atom(),
      Term :: any(),
      Ret :: new | {old, Term}.
%% @doc Checks the existence of `VHostName' and sets the new value of the
%% non-global runtime parameter named `Key'.
%%
%% @returns `new' if the runtime parameter was not set before, or `{old,
%% OldTerm}' with the old value.
%%
%% @private

set(VHostName, Comp, Name, Term)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse is_atom(Name)) ->
    Key = {VHostName, Comp, Name},
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> set_in_mnesia(VHostName, Key, Term) end,
        khepri => fun() -> set_in_khepri(VHostName, Key, Term) end}).

set_in_mnesia(VHostName, Key, Term) ->
    rabbit_mnesia:execute_mnesia_transaction(
      rabbit_db_vhost:with_fun_in_mnesia_tx(
        VHostName,
        fun() -> set_in_mnesia_tx(Key, Term) end)).

set_in_mnesia_tx(Key, Term) ->
    Res = case mnesia:read(?MNESIA_TABLE, Key, read) of
              [Params] -> {old, Params#runtime_parameters.value};
              []       -> new
          end,
    Record = #runtime_parameters{key   = Key,
                                 value = Term},
    mnesia:write(?MNESIA_TABLE, Record, write),
    Res.

set_in_khepri(VHostName, Key, Term) ->
    rabbit_khepri:transaction(
      rabbit_db_vhost:with_fun_in_khepri_tx(
        VHostName, fun() -> set_in_khepri_tx(Key, Term) end), rw).

set_in_khepri_tx(Key, Term) ->
    Path = khepri_rp_path(Key),
    Record = #runtime_parameters{key   = Key,
                                 value = Term},
    case khepri_tx_adv:put(Path, Record) of
        {ok, #{data := Params}} ->
            {old, Params#runtime_parameters.value};
        {ok, _} ->
            new
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(Key) -> Ret when
      Key :: atom() | {vhost:name(), binary(), binary()},
      Ret :: #runtime_parameters{} | undefined.
%% @doc Returns a runtime parameter.
%%
%% @returns the value of the runtime parameter if it exists, or `undefined'
%% otherwise.
%%
%% @private

get({VHostName, Comp, Name} = Key)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse is_atom(Name)) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(Key) end,
        khepri => fun() -> get_in_khepri(Key) end});
get(Key) when is_atom(Key) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(Key) end,
        khepri => fun() -> get_in_khepri(Key) end}).

get_in_mnesia(Key) ->
    case mnesia:dirty_read(?MNESIA_TABLE, Key) of
        []       -> undefined;
        [Record] -> Record
    end.

get_in_khepri(Key) when is_atom(Key) ->
    try ets:lookup(?KHEPRI_GLOBAL_PROJECTION, Key) of
        []       -> undefined;
        [Record] -> Record
    catch
        error:badarg ->
            undefined
    end;
get_in_khepri(Key) when is_tuple(Key) ->
    try ets:lookup(?KHEPRI_VHOST_PROJECTION, Key) of
        []       -> undefined;
        [Record] -> Record
    catch
        error:badarg ->
            undefined
    end.

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> Ret when
      Ret :: [#runtime_parameters{}].
%% @doc Gets all runtime parameters.
%%
%% @returns a list of runtime parameters records.
%%
%% @private

get_all() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia() end,
        khepri => fun() -> get_all_in_khepri() end}).

get_all_in_mnesia() ->
    rabbit_mnesia:dirty_read_all(?MNESIA_TABLE).

get_all_in_khepri() ->
    try
        ets:tab2list(?KHEPRI_GLOBAL_PROJECTION) ++
        ets:tab2list(?KHEPRI_VHOST_PROJECTION)
    catch
        error:badarg ->
            []
    end.

-spec get_all(VHostName, Comp) -> Ret when
      VHostName :: vhost:name() | '_',
      Comp :: binary() | '_',
      Ret :: [#runtime_parameters{}].
%% @doc Gets all non-global runtime parameters matching the given virtual host
%% and component.
%%
%% @returns a list of runtime parameters records.
%%
%% @private

get_all(VHostName, Comp)
  when (is_binary(VHostName) orelse VHostName =:= '_') andalso
       (is_binary(Comp) orelse Comp =:= '_') ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia(VHostName, Comp) end,
        khepri => fun() -> get_all_in_khepri(VHostName, Comp) end}).

get_all_in_mnesia(VHostName, Comp) ->
    mnesia:async_dirty(
      fun () ->
              case VHostName of
                  '_' -> ok;
                  _   -> rabbit_vhost:assert(VHostName)
              end,
              Match = #runtime_parameters{key = {VHostName, Comp, '_'},
                                          _   = '_'},
              mnesia:match_object(?MNESIA_TABLE, Match, read)
      end).

get_all_in_khepri(VHostName, Comp) ->
    case VHostName of
        '_' -> ok;
        _   -> rabbit_vhost:assert(VHostName)
    end,
    try
        Match = #runtime_parameters{key = {VHostName, Comp, '_'},
                                    _   = '_'},
        ets:match_object(?KHEPRI_VHOST_PROJECTION, Match)
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Key) -> ok when
      Key :: atom().
%% @doc Deletes the global runtime parameter named `Key'.
%%
%% @private

delete(Key) when is_atom(Key) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(Key) end,
        khepri => fun() -> delete_in_khepri(Key) end}).

-spec delete(VHostName, Comp, Name) -> ok when
      VHostName :: vhost:name() | '_',
      Comp :: binary() | '_',
      Name :: binary() | atom() | '_'.
%% @doc Deletes the non-global runtime parameter named `Name' for the given
%% virtual host and component.
%%
%% @private

delete(VHostName, Comp, Name)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse (is_atom(Name) andalso Name =/= '_')) ->
    Key = {VHostName, Comp, Name},
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(Key) end,
        khepri => fun() -> delete_in_khepri(Key) end});
delete(VHostName, Comp, Name)
  when VHostName =:= '_' orelse Comp =:= '_' orelse Name =:= '_' ->
    rabbit_khepri:handle_fallback(
      #{mnesia =>
        fun() -> delete_matching_in_mnesia(VHostName, Comp, Name) end,
        khepri =>
        fun() -> delete_matching_in_khepri(VHostName, Comp, Name) end}).

delete_in_mnesia(Key) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(Key) end).

delete_in_mnesia_tx(Key) ->
    mnesia:delete(?MNESIA_TABLE, Key, write).

delete_matching_in_mnesia(VHostName, Comp, Name) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_matching_in_mnesia_tx(VHostName, Comp, Name) end).

delete_matching_in_mnesia_tx(VHostName, Comp, Name) ->
    Match = #runtime_parameters{key = {VHostName, Comp, Name},
                                _   = '_'},
    _ = [ok = mnesia:delete(?MNESIA_TABLE, Key, write)
         || #runtime_parameters{key = Key} <-
            mnesia:match_object(?MNESIA_TABLE, Match, write)],
    ok.

delete_in_khepri(Key) ->
    Path = khepri_rp_path(Key),
    ok = rabbit_khepri:delete(Path).

delete_matching_in_khepri(VHostName, Comp, Name) ->
    Key = {?any(VHostName), ?any(Comp), ?any(Name)},
    delete_in_khepri(Key).

%% -------------------------------------------------------------------
%% delete_vhost().
%% -------------------------------------------------------------------

-spec delete_vhost(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Ret :: {ok, Deletions} | {error, Reason :: any()},
      Deletions :: [#runtime_parameters{}].
%% @doc Deletes all runtime parameters belonging to the given virtual host.
%%
%% @returns an OK tuple containing the deleted runtime parameters if
%% successful, or an error tuple otherwise.
%%
%% @private

delete_vhost(VHostName) when is_binary(VHostName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_vhost_in_mnesia(VHostName) end,
        khepri => fun() -> delete_vhost_in_khepri(VHostName) end}).

delete_vhost_in_mnesia(VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              Deletions = delete_vhost_in_mnesia_tx(VHostName),
              {ok, Deletions}
      end).

delete_vhost_in_mnesia_tx(VHostName) ->
    Match = #runtime_parameters{key = {VHostName, '_', '_'},
                                _   = '_'},
    [begin
         mnesia:delete(?MNESIA_TABLE, Key, write),
         Record
     end
     || #runtime_parameters{key = Key} = Record
        <- mnesia:match_object(?MNESIA_TABLE, Match, read)].

delete_vhost_in_khepri(VHostName) ->
    Path = khepri_vhost_rp_path(
             VHostName, ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:adv_delete_many(Path) of
        {ok, Props} ->
            {ok, rabbit_khepri:collect_payloads(Props)};
        {error, _} = Err ->
            Err
    end.

%% -------------------------------------------------------------------

khepri_rp_path({VHost, Component, Name}) ->
    khepri_vhost_rp_path(VHost, Component, Name);
khepri_rp_path(Key) ->
    khepri_global_rp_path(Key).

khepri_global_rp_path(Key) when ?IS_KHEPRI_PATH_CONDITION(Key) ->
    ?RABBITMQ_KHEPRI_GLOBAL_RUNTIME_PARAM_PATH(Key).

khepri_vhost_rp_path(VHost, Component, Name)
  when ?IS_KHEPRI_PATH_CONDITION(Component) andalso
       ?IS_KHEPRI_PATH_CONDITION(Name) ->
    ?RABBITMQ_KHEPRI_VHOST_RUNTIME_PARAM_PATH(VHost, Component, Name).
