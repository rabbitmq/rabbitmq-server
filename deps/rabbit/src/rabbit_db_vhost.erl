%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_vhost).

-include_lib("stdlib/include/assert.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("khepri/include/khepri.hrl").

-include("include/rabbit_khepri.hrl").
-include("vhost.hrl").
-include_lib("kernel/include/logger.hrl").

-export([create_or_get/3,
         merge_metadata/2,
         set_tags/2,
         exists/1,
         exists_uncached/1,
         get/1,
         get_all/0,
         count_all/0,
         list/0,
         update/2,
         enable_protection_from_deletion/1,
         disable_protection_from_deletion/1,
         with_fun_in_khepri_tx/2,
         delete/1, delete_with_id/1,
         get_id/1, get_id_for_deletion/1,
         list_ids/0, cleanup_snapshot/0, identity/1, storage_id/1, storage_epoch/0,
         assign_ids/1]).

-export([khepri_vhost_path/1, khepri_vhost_id_path/0, khepri_storage_epoch_path/0]).

%% For testing
-export([clear/0]).

-define(KHEPRI_PROJECTION, rabbit_khepri_vhost).

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(VHostName, Limits, Metadata) -> Ret when
      VHostName :: vhost:name(),
      Limits :: vhost:limits(),
      Metadata :: vhost:metadata(),
      Ret :: {existing | new, VHost} | no_return(),
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
    case rabbit_feature_flags:is_enabled('rabbitmq_5.0.0') of
        true -> create_or_get_with_id(VHostName, Limits, Metadata);
        false -> create_or_get_without_id(VHostName, Limits, Metadata)
    end.

create_or_get_with_id(VHostName, Limits, Metadata) ->
    Path = khepri_vhost_path(VHostName),
    Counter = khepri_vhost_id_path(),
    Now = {binary:decode_unsigned(crypto:strong_rand_bytes(16)), os:system_time(microsecond)},
    ?LOG_DEBUG("Inserting a virtual host record for ~tp", [VHostName]),
    rabbit_khepri:transaction(
      fun() ->
              case khepri_tx:get(Path) of
                  {ok, ExistingVHost} ->
                      {existing, ExistingVHost};
                  {error, ?khepri_error(node_not_found, _)} ->
                      VHost = vhost:new(VHostName, Limits, Metadata, next_id(Counter, Now)),
                      ok = khepri_tx:put(Path, VHost),
                      {new, VHost}
              end
      end, rw).

create_or_get_without_id(VHostName, Limits, Metadata) ->
    VHost = vhost:new(VHostName, Limits, Metadata),
    Path = khepri_vhost_path(VHostName),
    ?LOG_DEBUG("Inserting a virtual host record ~tp", [VHost]),
    case rabbit_khepri:create(Path, VHost) of
        ok ->
            {new, VHost};
        {error, ?khepri_error(mismatching_node,
                              #{node_path := Path,
                                node_props := #{data := ExistingVHost}})} ->
            {existing, ExistingVHost};
        Error ->
            throw(Error)
    end.

%% -------------------------------------------------------------------
%% merge_metadata().
%% -------------------------------------------------------------------

-spec merge_metadata(VHostName, Metadata) -> Ret when
      VHostName :: vhost:name(),
      Metadata :: vhost:metadata(),
      Ret :: {ok, VHost} |
             {error, {no_such_vhost, VHostName}} |
             rabbit_khepri:timeout_error(),
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
            ?LOG_DEBUG("Updated a virtual host record ~tp", [VHost]),
            {ok, VHost};
        {error, _} = Error ->
            Error
    end.

do_merge_metadata(VHostName, Metadata) ->
    Path = khepri_vhost_path(VHostName),
    Ret1 = rabbit_khepri:adv_get(Path),
    case Ret1 of
        {ok, #{Path := #{data := VHost0, payload_version := DVersion}}} ->
            VHost = vhost:merge_metadata(VHost0, Metadata),
            ?LOG_DEBUG("Updating a virtual host record ~p", [VHost]),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = DVersion}]),
            Ret2 = rabbit_khepri:put(Path1, VHost),
            case Ret2 of
                ok ->
                    {ok, VHost};
                {error, ?khepri_error(mismatching_node, _)} ->
                    merge_metadata(VHostName, Metadata);
                {error, _} = Error ->
                    Error
            end;
        {error, ?khepri_error(node_not_found, _)} ->
            {error, {no_such_vhost, VHostName}};
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% set_tags().
%% -------------------------------------------------------------------

-spec set_tags(VHostName, Tags) -> VHost when
      VHostName :: vhost:name(),
      Tags :: [atom() | binary() | string()],
      VHost :: vhost:vhost() | no_return().
%% @doc Sets the tags of an existing virtual host record.
%%
%% @returns the updated virtual host record if the record existed and the
%% update succeeded. It throws an exception if the transaction fails.
%%
%% @private

set_tags(VHostName, Tags)
  when is_binary(VHostName) andalso is_list(Tags) ->
    ConvertedTags = lists:usort([vhost:tag_to_binary(Tag) || Tag <- Tags]),
    UpdateFun = fun(VHost) -> do_set_tags(VHost, ConvertedTags) end,
    update(VHostName, UpdateFun).

do_set_tags(VHost, Tags) when ?is_vhost(VHost) andalso is_list(Tags) ->
    Metadata0 = vhost:get_metadata(VHost),
    Metadata1 = Metadata0#{tags => Tags},
    vhost:set_metadata(VHost, Metadata1).

%% -------------------------------------------------------------------
%% Deletion protection
%% -------------------------------------------------------------------

-spec enable_protection_from_deletion(VHostName) -> Ret when
    VHostName :: vhost:name(),
    Ret :: {ok, VHost} |
    {error, {no_such_vhost, VHostName}} |
    rabbit_khepri:timeout_error(),
    VHost :: vhost:vhost().
enable_protection_from_deletion(VHostName) ->
    MetadataPatch = #{
        protected_from_deletion => true
    },
    ?LOG_INFO("Enabling deletion protection for virtual host '~ts'", [VHostName]),
    merge_metadata(VHostName, MetadataPatch).

-spec disable_protection_from_deletion(VHostName) -> Ret when
    VHostName :: vhost:name(),
    Ret :: {ok, VHost} |
    {error, {no_such_vhost, VHostName}} |
    rabbit_khepri:timeout_error(),
    VHost :: vhost:vhost().
disable_protection_from_deletion(VHostName) ->
    MetadataPatch = #{
        protected_from_deletion => false
    },
    ?LOG_INFO("Disabling deletion protection for virtual host '~ts'", [VHostName]),
    merge_metadata(VHostName, MetadataPatch).

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
    try
        ets:member(?KHEPRI_PROJECTION, VHostName)
    catch
        error:badarg ->
            false
    end.

%% -------------------------------------------------------------------
%% exists_uncached().
%% -------------------------------------------------------------------

-spec exists_uncached(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Ret :: boolean() | {error, any()}.
%% @doc Checks if the virtual host named `VHostName' exists by querying
%% Khepri directly, bypassing the ETS-backed projection.
%%
%% @returns `true' if the virtual host exists in Khepri, `false' if it does
%% not exist, or `{error, Reason}' if Khepri is unavailable.
%%
%% @private

exists_uncached(VHostName) when is_binary(VHostName) ->
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get(Path, #{timeout => 5000}) of
        {ok, _} ->
            true;
        {error, ?khepri_error(node_not_found, _)} ->
            false;
        {error, _} = Error ->
            Error
    end.

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
    try ets:lookup(?KHEPRI_PROJECTION, VHostName) of
        [Record] -> Record;
        _        -> undefined
    catch
        error:badarg ->
            undefined
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
    try
        ets:tab2list(?KHEPRI_PROJECTION)
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% count_all().
%% -------------------------------------------------------------------

-spec count_all() -> {ok, Count} | {error, any()} when
      Count :: non_neg_integer().
%% @doc Returns all virtual host records.
%%
%% @returns the count of virtual host records.
%%
%% @private

count_all() ->
    Path = khepri_vhost_path(?KHEPRI_WILDCARD_STAR),
    rabbit_khepri:count(Path).

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
    try
        ets:select(
          ?KHEPRI_PROJECTION, [{Pattern, [], ['$1']} || Pattern <- vhost:pattern_match_names()])
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% update_in_*tx().
%% -------------------------------------------------------------------

-spec update(VHostName, UpdateFun) -> VHost when
      VHostName :: vhost:name(),
      UpdateFun :: fun((VHost) -> VHost),
      VHost :: vhost:vhost() | no_return().
%% @doc Updates an existing virtual host record using the result of
%% `UpdateFun'.
%%
%% @returns the updated virtual host record if the record existed and the
%% update succeeded. It throws an exception if the transaction fails.
%%
%% @private

update(VHostName, UpdateFun)
  when is_binary(VHostName) andalso is_function(UpdateFun, 1) ->
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:adv_get(Path) of
        {ok, #{Path := #{data := V, payload_version := Vsn}}} ->
            V1 = UpdateFun(V),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = Vsn}]),
            case rabbit_khepri:put(Path1, V1) of
                ok ->
                    V1;
                {error, ?khepri_error(mismatching_node, _)} ->
                    update(VHostName, UpdateFun);
                Error ->
                    throw(Error)
            end;
        {error, ?khepri_error(node_not_found, _)} ->
            throw({error, {no_such_vhost, VHostName}});
        Error ->
            throw(Error)
    end.

%% -------------------------------------------------------------------
%% with_fun_in_*_tx().
%% -------------------------------------------------------------------

with_fun_in_khepri_tx(VHostName, Thunk) ->
    fun() ->
            Path = khepri_vhost_path(VHostName),
            case khepri_tx:exists(Path) of
                true  -> Thunk();
                false -> khepri_tx:abort({no_such_vhost, VHostName})
            end
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Existed :: boolean(),
      Ret :: Existed | rabbit_khepri:timeout_error().
%% @doc Deletes a virtual host record from the database.
%%
%% @returns a boolean indicating if the vhost existed or not. It throws an
%% exception if the transaction fails.
%%
%% @private

delete(VHostName) when is_binary(VHostName) ->
    case delete_with_id(VHostName) of
        {ok, _} -> true;
        {error, not_found} -> false;
        {error, _} = Error -> Error
    end.

-spec delete_with_id(vhost:name()) -> {ok, vhost:id() | undefined} | {error, term()}.
delete_with_id(VHostName) ->
    Path = khepri_vhost_path(VHostName),
    deletion_read(fun() ->
        case khepri_tx:get(Path) of
            {ok, VHost} ->
                ok = khepri_tx:delete(Path),
                {ok, vhost:get_id(VHost)};
            {error, ?khepri_error(node_not_found, _)} -> {error, not_found}
        end
    end).

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all vhosts.
%%
%% @private

clear() ->
    Path = khepri_vhost_path(?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:delete_many(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Paths
%% --------------------------------------------------------------

khepri_vhost_path(VHost) when ?IS_KHEPRI_PATH_CONDITION(VHost) ->
    ?RABBITMQ_KHEPRI_VHOST_PATH(VHost).

khepri_vhost_id_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([vhost_id]).

%% -------------------------------------------------------------------
%% Vhost ids.
%% -------------------------------------------------------------------

-spec get_id(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Ret :: {ok, vhost:id()} | {error, not_found | feature_flag_disabled | term()}.
%% @doc Returns the id of the virtual host named `VHostName', read with a
%% consistent query.
%%
%% A virtual host created before `rabbitmq_5.0.0' was enabled gets its id here
%% if enabling the feature flag did not assign it.
%%
%% @private

get_id(VHostName) when is_binary(VHostName) ->
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get(Path, #{favor => consistency, timeout => 5000}) of
        {ok, VHost} ->
            case vhost:get_id(VHost) of
                undefined -> assign_id(VHostName);
                Id -> {ok, Id}
            end;
        {error, ?khepri_error(node_not_found, _)} ->
            {error, not_found};
        {error, _} = Error ->
            Error
    end.

assign_id(VHostName) ->
    case rabbit_feature_flags:is_enabled('rabbitmq_5.0.0') of
        true ->
            Path = khepri_vhost_path(VHostName),
            Counter = khepri_vhost_id_path(),
            Now = {binary:decode_unsigned(crypto:strong_rand_bytes(16)), os:system_time(microsecond)},
            try
                rabbit_khepri:transaction(
                  fun() ->
                          case khepri_tx:get(Path) of
                              {ok, VHost} -> {ok, with_id(VHost, Counter, Now)};
                              {error, ?khepri_error(node_not_found, _)} -> {error, not_found}
                          end
                  end, rw, #{timeout => 5000})
            catch throw:{error, _} = Error -> Error
            end;
        false ->
            {error, feature_flag_disabled}
    end.

-spec list_ids() -> {ok, #{vhost:name() => vhost:id() | undefined}} | {error, term()}.
%% @doc Lists virtual host ids through a quorum.
%% @private
list_ids() ->
    case cleanup_snapshot() of
        {ok, _Epoch, Live} -> {ok, Live};
        {error, _} = Error -> Error
    end.

-spec cleanup_snapshot() ->
    {ok, non_neg_integer() | undefined, #{vhost:name() => vhost:id() | undefined}} | {error, term()}.
cleanup_snapshot() ->
    Path = khepri_vhost_path(?KHEPRI_WILDCARD_STAR),
    Counter = khepri_vhost_id_path(),
    deletion_read(fun() ->
        {ok, VHosts} = khepri_tx:get_many(Path),
        Epoch = case khepri_tx:get(Counter) of
                    {ok, Last} -> vhost:id_epoch(Last);
                    {error, ?khepri_error(node_not_found, _)} -> undefined
                end,
        {ok, Epoch, maps:from_list([{vhost:get_name(V), vhost:get_id(V)} || V <- maps:values(VHosts)])}
    end).

-spec identity(vhost:name()) -> {ok, vhost:id() | undefined, vhost:id() | legacy} | {error, term()}.
identity(Name) ->
    Path = khepri_vhost_path(Name),
    deletion_read(fun() ->
        case khepri_tx:get(Path) of
            {ok, VHost} ->
                Id = vhost:get_id(VHost),
                {ok, Id, storage_id_in_tx(Id)};
            {error, ?khepri_error(node_not_found, _)} -> {error, not_found}
        end
    end).

-spec storage_id(vhost:id() | undefined) -> {ok, vhost:id() | legacy} | {error, term()}.
storage_id(Id) ->
    deletion_read(fun() -> {ok, storage_id_in_tx(Id)} end).

storage_id_in_tx(undefined) -> legacy;
storage_id_in_tx(Id) ->
    case khepri_tx:get(khepri_legacy_storage_path(Id)) of
        {ok, legacy} -> legacy;
        {error, ?khepri_error(node_not_found, _)} -> Id
    end.

khepri_legacy_storage_path(Id) ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([legacy_vhost_storage, integer_to_binary(Id)]).

khepri_storage_epoch_path() ->
    ?RABBITMQ_KHEPRI_ROOT_PATH([vhost_storage_epoch]).

-spec storage_epoch() -> {ok, non_neg_integer()} | {error, term()}.
storage_epoch() ->
    Random = binary:decode_unsigned(crypto:strong_rand_bytes(16)),
    deletion_read(fun() -> {ok, storage_epoch_in_tx(Random)} end).

storage_epoch_in_tx(Random) ->
    Path = khepri_storage_epoch_path(),
    case khepri_tx:get(Path) of
        {ok, Epoch} -> Epoch;
        {error, ?khepri_error(node_not_found, _)} ->
            Epoch = case khepri_tx:get(khepri_vhost_id_path()) of
                        {ok, Last} -> vhost:id_epoch(Last);
                        {error, ?khepri_error(node_not_found, _)} -> Random
                    end,
            ok = khepri_tx:put(Path, Epoch),
            Epoch
    end.

-spec get_id_for_deletion(vhost:name()) ->
    {ok, vhost:id() | undefined} | {error, term()}.
%% @doc Reads the incarnation through a quorum before destructive cleanup.
%% @private
get_id_for_deletion(VHostName) ->
    Path = khepri_vhost_path(VHostName),
    case deletion_read(fun() -> khepri_tx:get(Path) end) of
        {ok, VHost} -> {ok, vhost:get_id(VHost)};
        {error, ?khepri_error(node_not_found, _)} -> {error, not_found};
        {error, _} = Error -> Error
    end.

deletion_read(Fun) ->
    %% A read fenced against a cached leader does not establish a quorum.
    try rabbit_khepri:transaction(Fun, rw, #{timeout => 5000})
    catch throw:{error, _} = Error -> Error
    end.

-spec assign_ids(rabbit_feature_flags:enable_callback_args()) -> ok | {error, term()}.
%% @doc Gives an id to every virtual host when `rabbitmq_5.0.0' is enabled.
%%
%% @private

assign_ids(#{feature_name := FeatureName}) ->
    ?LOG_DEBUG("Feature flag `~s`: assign virtual host ids", [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_DB}),
    Path = khepri_vhost_path(?KHEPRI_WILDCARD_STAR),
    Counter = khepri_vhost_id_path(),
    Now = {binary:decode_unsigned(crypto:strong_rand_bytes(16)), os:system_time(microsecond)},
    try
        rabbit_khepri:transaction(
          fun() ->
                  {ok, VHosts} = khepri_tx:get_many(Path),
                  _ = [with_id(VHost, Counter, Now)
                       || VHost <- lists:sort(maps:values(VHosts)), vhost:get_id(VHost) =:= undefined],
                  ok
          end, rw)
    catch throw:{error, _} = Error -> Error
    end.

%% Runs in a transaction. The time is taken before the transaction, which must
%% be deterministic.
with_id(VHost0, Counter, Now) ->
    case vhost:get_id(VHost0) of
        undefined ->
            Id = next_id(Counter, Now),
            VHost = vhost:new(vhost:get_name(VHost0), vhost:get_limits(VHost0),
                              vhost:get_metadata(VHost0), Id),
            ok = khepri_tx:put(khepri_vhost_path(vhost:get_name(VHost)), VHost),
            ok = khepri_tx:put(khepri_legacy_storage_path(Id), legacy),
            Id;
        Id ->
            Id
    end.

%% Ids increase even if the clock of the node creating a vhost is behind.
next_id(Counter, {Epoch, Now}) ->
    Id = case khepri_tx:get(Counter) of
             {ok, Last} ->
                 (vhost:id_epoch(Last) bsl 64) bor max(vhost:id_time(Last) + 1, Now);
             {error, ?khepri_error(node_not_found, _)} -> (storage_epoch_in_tx(Epoch) bsl 64) bor Now
         end,
    ok = khepri_tx:put(Counter, Id),
    Id.
