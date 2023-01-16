%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_vhost).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include("vhost.hrl").

-export([create_or_get/3,
         merge_metadata/2,
         set_tags/2,
         exists/1,
         get/1,
         get_all/0,
         list/0,
         update/2,
         with_fun_in_mnesia_tx/2,
         delete/1]).

-export([clear/0]).

-define(MNESIA_TABLE, rabbit_vhost).

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(VHostName, Limits, Metadata) -> Ret when
      VHostName :: vhost:name(),
      Limits :: vhost:limits(),
      Metadata :: vhost:metadata(),
      Ret :: {existing | new, VHost},
      VHost :: vhost:vhost().
%% @doc Writes a virtual host record if it doesn't exist already or returns
%% the existing one.
%%
%% @returns the existing record if there is one in the database already, or
%% the newly created record. It throws an exception if the transaction fails.
%%
%% @private

create_or_get(VHostName, Limits, Metadata)
  when is_binary(VHostName) andalso
       is_list(Limits) andalso
       is_map(Metadata) ->
    VHost = vhost:new(VHostName, Limits, Metadata),
    rabbit_db:run(
      #{mnesia => fun() -> create_or_get_in_mnesia(VHostName, VHost) end}).

create_or_get_in_mnesia(VHostName, VHost) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> create_or_get_in_mnesia_tx(VHostName, VHost) end).

create_or_get_in_mnesia_tx(VHostName, VHost) ->
    case mnesia:wread({?MNESIA_TABLE, VHostName}) of
        [] ->
            mnesia:write(?MNESIA_TABLE, VHost, write),
            {new, VHost};

        %% the vhost already exists
        [ExistingVHost] ->
            {existing, ExistingVHost}
    end.

%% -------------------------------------------------------------------
%% merge_metadata().
%% -------------------------------------------------------------------

-spec merge_metadata(VHostName, Metadata) -> Ret when
      VHostName :: vhost:name(),
      Metadata :: vhost:metadata(),
      Ret :: {ok, VHost} | {error, {no_such_vhost, VHostName}},
      VHost :: vhost:vhost().
%% @doc Updates the metadata of an existing virtual host record.
%%
%% @returns `{ok, VHost}' with the updated record if the record existed and
%% the update succeeded or an error tuple if the record didn't exist. It
%% throws an exception if the transaction fails.
%%
%% @private

merge_metadata(VHostName, Metadata)
  when is_binary(VHostName) andalso is_map(Metadata) ->
    case do_merge_metadata(VHostName, Metadata) of
        {ok, VHost} when ?is_vhost(VHost) ->
            rabbit_log:debug("Updated a virtual host record ~tp", [VHost]),
            {ok, VHost};
        {error, _} = Error ->
            Error
    end.

do_merge_metadata(VHostName, Metadata) ->
    rabbit_db:run(
      #{mnesia => fun() -> merge_metadata_in_mnesia(VHostName, Metadata) end}).

merge_metadata_in_mnesia(VHostName, Metadata) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> merge_metadata_in_mnesia_tx(VHostName, Metadata) end).

merge_metadata_in_mnesia_tx(VHostName, Metadata) ->
    case mnesia:wread({?MNESIA_TABLE, VHostName}) of
        [] ->
            {error, {no_such_vhost, VHostName}};
        [VHost0] when ?is_vhost(VHost0) ->
            VHost1 = vhost:merge_metadata(VHost0, Metadata),
            ?assert(?is_vhost(VHost1)),
            mnesia:write(?MNESIA_TABLE, VHost1, write),
            {ok, VHost1}
    end.

%% -------------------------------------------------------------------
%% set_tags().
%% -------------------------------------------------------------------

-spec set_tags(VHostName, Tags) -> VHost when
      VHostName :: vhost:name(),
      Tags :: [vhost:tag() | binary() | string()],
      VHost :: vhost:vhost().
%% @doc Sets the tags of an existing virtual host record.
%%
%% @returns the updated virtual host record if the record existed and the
%% update succeeded. It throws an exception if the transaction fails.
%%
%% @private

set_tags(VHostName, Tags)
  when is_binary(VHostName) andalso is_list(Tags) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(Tag) || Tag <- Tags],
    rabbit_db:run(
      #{mnesia => fun() -> set_tags_in_mnesia(VHostName, ConvertedTags) end}).

set_tags_in_mnesia(VHostName, Tags) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> set_tags_in_mnesia_tx(VHostName, Tags) end).

set_tags_in_mnesia_tx(VHostName, Tags) ->
    UpdateFun = fun(VHost) -> do_set_tags(VHost, Tags) end,
    update_in_mnesia_tx(VHostName, UpdateFun).

do_set_tags(VHost, Tags) when ?is_vhost(VHost) andalso is_list(Tags) ->
    Metadata0 = vhost:get_metadata(VHost),
    Metadata1 = Metadata0#{tags => Tags},
    vhost:set_metadata(VHost, Metadata1).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(VHostName) -> Exists when
      VHostName :: vhost:name(),
      Exists :: boolean().
%% @doc Indicates if the virtual host named `VHostName' exists.
%%
%% @returns true if the virtual host exists, false otherwise.
%%
%% @private

exists(VHostName) when is_binary(VHostName) ->
    rabbit_db:run(
      #{mnesia => fun() -> exists_in_mnesia(VHostName) end}).

exists_in_mnesia(VHostName) ->
    mnesia:dirty_read({?MNESIA_TABLE, VHostName}) /= [].

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(VHostName) -> VHost | undefined when
      VHostName :: vhost:name(),
      VHost :: vhost:vhost().
%% @doc Returns the record of the virtual host named `VHostName'.
%%
%% @returns the virtual host record or `undefined' if no virtual host is named
%% `VHostName'.
%%
%% @private

get(VHostName) when is_binary(VHostName) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(VHostName) end}).

get_in_mnesia(VHostName) ->
    case mnesia:dirty_read({?MNESIA_TABLE, VHostName}) of
        [VHost] when ?is_vhost(VHost) -> VHost;
        []                            -> undefined
    end.

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [VHost] when
      VHost :: vhost:vhost().
%% @doc Returns all virtual host records.
%%
%% @returns the list of all virtual host records.
%%
%% @private

get_all() ->
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia() end}).

get_all_in_mnesia() ->
    mnesia:dirty_match_object(?MNESIA_TABLE, vhost:pattern_match_all()).

%% -------------------------------------------------------------------
%% list().
%% -------------------------------------------------------------------

-spec list() -> [VHostName] when
      VHostName :: vhost:name().
%% @doc Lists the names of all virtual hosts.
%%
%% @returns a list of virtual host names.
%%
%% @private

list() ->
    rabbit_db:run(
      #{mnesia => fun() -> list_in_mnesia() end}).

list_in_mnesia() ->
    mnesia:dirty_all_keys(?MNESIA_TABLE).

%% -------------------------------------------------------------------
%% update_in_*tx().
%% -------------------------------------------------------------------

-spec update(VHostName, UpdateFun) -> VHost when
      VHostName :: vhost:name(),
      UpdateFun :: fun((VHost) -> VHost),
      VHost :: vhost:vhost().
%% @doc Updates an existing virtual host record using the result of
%% `UpdateFun'.
%%
%% @returns the updated virtual host record if the record existed and the
%% update succeeded. It throws an exception if the transaction fails.
%%
%% @private

update(VHostName, UpdateFun)
  when is_binary(VHostName) andalso is_function(UpdateFun, 1) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_in_mnesia(VHostName, UpdateFun) end}).

update_in_mnesia(VHostName, UpdateFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> update_in_mnesia_tx(VHostName, UpdateFun) end).

update_in_mnesia_tx(VHostName, UpdateFun)
  when is_binary(VHostName) andalso is_function(UpdateFun, 1) ->
    case mnesia:wread({?MNESIA_TABLE, VHostName}) of
        [VHost0] when ?is_vhost(VHost0) ->
            VHost1 = UpdateFun(VHost0),
            mnesia:write(?MNESIA_TABLE, VHost1, write),
            VHost1;
        [] ->
            mnesia:abort({no_such_vhost, VHostName})
    end.

%% -------------------------------------------------------------------
%% with_fun_in_*_tx().
%% -------------------------------------------------------------------

-spec with_fun_in_mnesia_tx(VHostName, TxFun) -> Fun when
      VHostName :: vhost:name(),
      TxFun :: fun(() -> Ret),
      Fun :: fun(() -> Ret),
      Ret :: any().
%% @doc Returns a function, calling `TxFun' only if the virtual host named
%% `VHostName' exists.
%%
%% The returned function must be used inside a Mnesia transaction.
%%
%% @returns a function calling `TxFun' only if the virtual host exists.
%%
%% @private

with_fun_in_mnesia_tx(VHostName, TxFun)
  when is_binary(VHostName) andalso is_function(TxFun, 0) ->
    fun() ->
            ?assert(mnesia:is_transaction()),
            case mnesia:read({?MNESIA_TABLE, VHostName}) of
                [_VHost] -> TxFun();
                []       -> mnesia:abort({no_such_vhost, VHostName})
            end
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(VHostName) -> Existed when
      VHostName :: vhost:name(),
      Existed :: boolean().
%% @doc Deletes a virtual host record from the database.
%%
%% @returns a boolean indicating if the vhost existed or not. It throws an
%% exception if the transaction fails.
%%
%% @private

delete(VHostName) when is_binary(VHostName) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(VHostName) end}).

delete_in_mnesia(VHostName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() -> delete_in_mnesia_tx(VHostName) end).

delete_in_mnesia_tx(VHostName) ->
    Existed = mnesia:wread({?MNESIA_TABLE, VHostName}) =/= [],
    mnesia:delete({?MNESIA_TABLE, VHostName}),
    Existed.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all vhosts.
%%
%% @private

clear() ->
    rabbit_db:run(
      #{mnesia => fun() -> clear_in_mnesia() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    ok.
