%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_rtparams).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([set/2, set/4,
         get/1,
         get_or_set/2,
         get_all/0, get_all/2,
         delete/1, delete/3]).

-define(MNESIA_TABLE, rabbit_runtime_parameters).

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
    rabbit_db:run(
      #{mnesia => fun() -> set_in_mnesia(Key, Term) end}).

set_in_mnesia(Key, Term) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> set_in_mnesia_tx(Key, Term) end).

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
    rabbit_db:run(
      #{mnesia => fun() -> set_in_mnesia(VHostName, Key, Term) end}).

set_in_mnesia(VHostName, Key, Term) ->
    rabbit_misc:execute_mnesia_transaction(
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
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Key) end});
get(Key) when is_atom(Key) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Key) end}).

get_in_mnesia(Key) ->
    case mnesia:dirty_read(?MNESIA_TABLE, Key) of
        []       -> undefined;
        [Record] -> Record
    end.

%% -------------------------------------------------------------------
%% get_or_set().
%% -------------------------------------------------------------------

-spec get_or_set(Key, Default) -> Ret when
      Key :: atom() | {vhost:name(), binary(), binary()},
      Default :: any(),
      Ret :: #runtime_parameters{}.
%% @doc Returns a runtime parameter or sets its value if it does not exist.
%%
%% @private

get_or_set({VHostName, Comp, Name} = Key, Default)
  when is_binary(VHostName) andalso
       is_binary(Comp) andalso
       (is_binary(Name) orelse is_atom(Name)) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_or_set_in_mnesia(Key, Default) end});
get_or_set(Key, Default) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_or_set_in_mnesia(Key, Default) end}).

get_or_set_in_mnesia(Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> get_or_set_in_mnesia_tx(Key, Default) end).

get_or_set_in_mnesia_tx(Key, Default) ->
    case mnesia:read(?MNESIA_TABLE, Key, read) of
        [Record] ->
            Record;
        [] ->
            Record = #runtime_parameters{key   = Key,
                                         value = Default},
            mnesia:write(?MNESIA_TABLE, Record, write),
            Record
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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia() end}).

get_all_in_mnesia() ->
    rabbit_misc:dirty_read_all(?MNESIA_TABLE).

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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia(VHostName, Comp) end}).

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

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Key) -> ok when
      Key :: atom().
%% @doc Deletes the global runtime parameter named `Key'.
%%
%% @private

delete(Key) when is_atom(Key) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(Key) end}).

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
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(Key) end});
delete(VHostName, Comp, Name)
  when VHostName =:= '_' orelse Comp =:= '_' orelse Name =:= '_' ->
    rabbit_db:run(
      #{mnesia =>
        fun() -> delete_matching_in_mnesia(VHostName, Comp, Name) end}).

delete_in_mnesia(Key) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(Key) end).

delete_in_mnesia_tx(Key) ->
    mnesia:delete(?MNESIA_TABLE, Key, write).

delete_matching_in_mnesia(VHostName, Comp, Name) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() -> delete_matching_in_mnesia_tx(VHostName, Comp, Name) end).

delete_matching_in_mnesia_tx(VHostName, Comp, Name) ->
    Match = #runtime_parameters{key = {VHostName, Comp, Name},
                                _   = '_'},
    _ = [ok = mnesia:delete(?MNESIA_TABLE, Key, write)
         || #runtime_parameters{key = Key} <-
            mnesia:match_object(?MNESIA_TABLE, Match, write)],
    ok.
