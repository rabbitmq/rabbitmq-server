%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vhost).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include_lib("khepri/include/khepri.hrl").

-include("vhost.hrl").

-export([recover/0, recover/1, read_config/1]).
-export([add/2, add/4, delete/2, exists/1, with/2, with_user_and_vhost/3, assert/1, update/2,
         set_limits/2, vhost_cluster_state/1, is_running_on_all_nodes/1, await_running_on_all_nodes/2,
        list/0, count/0, list_names/0, all/0]).
-export([parse_tags/1, update_metadata/2, tag_with/2, untag_from/2, update_tags/2, update_tags/3]).
-export([lookup/1]).
-export([info/1, info/2, info_all/0, info_all/1, info_all/2, info_all/3]).
-export([dir/1, msg_store_dir_path/1, msg_store_dir_wildcard/0, config_file_path/1, ensure_config_file/1]).
-export([delete_storage/1]).
-export([vhost_down/1]).
-export([put_vhost/5]).
-export([khepri_vhosts_path/0, khepri_vhost_path/1]).

%%
%% API
%%

-type vhost_tag() :: atom() | string() | binary().
-export_type([vhost_tag/0]).

recover() ->
    %% Clear out remnants of old incarnation, in case we restarted
    %% faster than other nodes handled DOWN messages from us.
    rabbit_amqqueue:on_node_down(node()),

    rabbit_amqqueue:warn_file_limit(),

    %% Prepare rabbit_semi_durable_route table
    rabbit_binding:recover(),

    %% rabbit_vhost_sup_sup will start the actual recovery.
    %% So recovery will be run every time a vhost supervisor is restarted.
    ok = rabbit_vhost_sup_sup:start(),

    [ok = rabbit_vhost_sup_sup:init_vhost(VHost) || VHost <- list_names()],
    ok.

recover(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Making sure data directory '~ts' for vhost '~s' exists",
                    [VHostDir, VHost]),
    VHostStubFile = filename:join(VHostDir, ".vhost"),
    ok = rabbit_file:ensure_dir(VHostStubFile),
    ok = file:write_file(VHostStubFile, VHost),
    ok = ensure_config_file(VHost),
    {Recovered, Failed} = rabbit_amqqueue:recover(VHost),
    AllQs = Recovered ++ Failed,
    QNames = [amqqueue:get_name(Q) || Q <- AllQs],
    ok = rabbit_binding:recover(rabbit_exchange:recover(VHost), QNames),
    ok = rabbit_amqqueue:start(Recovered),
    %% Start queue mirrors.
    ok = rabbit_mirror_queue_misc:on_vhost_up(VHost),
    ok.

ensure_config_file(VHost) ->
    Path = config_file_path(VHost),
    case filelib:is_regular(Path) of
        %% The config file exists. Do nothing.
        true ->
            ok;
        %% The config file does not exist.
        %% Check if there are queues in this vhost.
        false ->
            QueueDirs = rabbit_queue_index:all_queue_directory_names(VHost),
            SegmentEntryCount = case QueueDirs of
                %% There are no queues. Write the configured value for
                %% the segment entry count, or the new RabbitMQ default
                %% introduced in v3.8.17. The new default provides much
                %% better memory footprint when many queues are used.
                [] ->
                    application:get_env(rabbit, queue_index_segment_entry_count,
                        2048);
                %% There are queues already. Write the historic RabbitMQ
                %% default of 16384 for forward compatibility. Historic
                %% default calculated as trunc(math:pow(2,?REL_SEQ_BITS)).
                _ ->
                    ?LEGACY_INDEX_SEGMENT_ENTRY_COUNT
            end,
            rabbit_log:info("Setting segment_entry_count for vhost '~s' with ~b queues to '~b'",
                            [VHost, length(QueueDirs), SegmentEntryCount]),
            file:write_file(Path, io_lib:format(
                "%% This file is auto-generated! Edit at your own risk!~n"
                "{segment_entry_count, ~b}.",
                [SegmentEntryCount]))
    end.

read_config(VHost) ->
    Config = case file:consult(config_file_path(VHost)) of
        {ok, Val}       -> Val;
        %% the file does not exist yet, likely due to an upgrade from a pre-3.7
        %% message store layout so use the history default.
        {error, _}      -> #{
            segment_entry_count => ?LEGACY_INDEX_SEGMENT_ENTRY_COUNT
        }
    end,
    rabbit_data_coercion:to_map(Config).

-define(INFO_KEYS, vhost:info_keys()).

-spec parse_tags(binary() | string() | atom()) -> [atom()].
parse_tags(undefined) ->
    [];
parse_tags(<<"">>) ->
    [];
parse_tags([]) ->
    [];
parse_tags(Val) when is_binary(Val) ->
    SVal = rabbit_data_coercion:to_list(Val),
    [trim_tag(Tag) || Tag <- re:split(SVal, ",", [{return, list}])];
parse_tags(Val) when is_list(Val) ->
    case hd(Val) of
      Bin when is_binary(Bin) ->
        %% this is a list of binaries
        [trim_tag(Tag) || Tag <- Val];
      Int when is_integer(Int) ->
        %% this is a string/charlist
        [trim_tag(Tag) || Tag <- re:split(Val, ",", [{return, list}])]
    end.

-spec add(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(VHost, ActingUser) ->
    case exists(VHost) of
        true  -> ok;
        false -> do_add(VHost, <<"">>, [], ActingUser)
    end.

-spec add(vhost:name(), binary(), [atom()], rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(Name, Description, Tags, ActingUser) ->
    case exists(Name) of
        true  -> ok;
        false -> do_add(Name, Description, Tags, ActingUser)
    end.

do_add(Name, Description, Tags, ActingUser) ->
    case Description of
        undefined ->
            rabbit_log:info("Adding vhost '~s' without a description", [Name]);
        Value ->
            rabbit_log:info("Adding vhost '~s' (description: '~s', tags: ~p)", [Name, Value, Tags])
    end,
    %% MNESIA VHost = rabbit_misc:execute_mnesia_transaction(
    %% MNESIA       fun () ->
    %% MNESIA               case mnesia:wread({rabbit_vhost, Name}) of
    %% MNESIA                   [] ->
    %% MNESIA                     Row = vhost:new(Name, [], #{description => Description, tags => Tags}),
    %% MNESIA                     rabbit_log:debug("Inserting a virtual host record ~p", [Row]),
    %% MNESIA                     ok = mnesia:write(rabbit_vhost, Row, write),
    %% MNESIA                     Row;
    %% MNESIA                   %% the vhost already exists
    %% MNESIA                   [Row] ->
    %% MNESIA                     Row
    %% MNESIA               end
    %% MNESIA       end,
    %% MNESIA       fun (VHost1, true) ->
    %% MNESIA               VHost1;
    %% MNESIA           (VHost1, false) ->
    %% MNESIA               [begin
    %% MNESIA                 Resource = rabbit_misc:r(Name, exchange, ExchangeName),
    %% MNESIA                 rabbit_log:debug("Will declare an exchange ~p", [Resource]),
    %% MNESIA                 _ = rabbit_exchange:declare(Resource, Type, true, false, Internal, [], ActingUser)
    %% MNESIA               end || {ExchangeName, Type, Internal} <-
    %% MNESIA                       [{<<"">>,                   direct,  false},
    %% MNESIA                        {<<"amq.direct">>,         direct,  false},
    %% MNESIA                        {<<"amq.topic">>,          topic,   false},
    %% MNESIA                        %% per 0-9-1 pdf
    %% MNESIA                        {<<"amq.match">>,          headers, false},
    %% MNESIA                        %% per 0-9-1 xml
    %% MNESIA                        {<<"amq.headers">>,        headers, false},
    %% MNESIA                        {<<"amq.fanout">>,         fanout,  false},
    %% MNESIA                        {<<"amq.rabbitmq.trace">>, topic,   true}]],
    %% MNESIA               VHost1
    %% MNESIA       end),

    Path = khepri_vhost_path(Name),
    NewVHost = vhost:new(
                 Name, [], #{description => Description, tags => Tags}),
    Ret = rabbit_khepri:insert(
            Path, NewVHost, [#if_node_exists{exists = false}]),
    VHost = case Ret of
                {ok, _} ->
                    NewVHost;
                {error, {mismatching_node, Path, ExistingVHost, _}} ->
                    ExistingVHost;
                Error ->
                    throw(Error)
            end,
    [begin
         Resource = rabbit_misc:r(Name, exchange, ExchangeName),
         rabbit_log:debug("Will declare an exchange ~p", [Resource]),
         _ = rabbit_exchange:declare(Resource, Type, true, false, Internal, [], ActingUser)
     end || {ExchangeName, Type, Internal} <-
            [{<<"">>,                   direct,  false},
             {<<"amq.direct">>,         direct,  false},
             {<<"amq.topic">>,          topic,   false},
             %% per 0-9-1 pdf
             {<<"amq.match">>,          headers, false},
             %% per 0-9-1 xml
             {<<"amq.headers">>,        headers, false},
             {<<"amq.fanout">>,         fanout,  false},
             {<<"amq.rabbitmq.trace">>, topic,   true}]],
    case rabbit_vhost_sup_sup:start_on_all_nodes(Name) of
        ok ->
            rabbit_event:notify(vhost_created, info(VHost)
                                ++ [{user_who_performed_action, ActingUser},
                                    {description, Description},
                                    {tags, Tags}]),
            ok;
        {error, Reason} ->
            Msg = rabbit_misc:format("failed to set up vhost '~s': ~p",
                                     [Name, Reason]),
            {error, Msg}
    end.

-spec delete(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

delete(VHost, ActingUser) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further mnesia actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~s'", [VHost]),
    QDelFun = fun (Q) -> rabbit_amqqueue:delete(Q, false, false, ActingUser) end,
    [begin
         Name = amqqueue:get_name(Q),
         assert_benign(rabbit_amqqueue:with(Name, QDelFun), ActingUser)
     end || Q <- rabbit_amqqueue:list(VHost)],
    [assert_benign(rabbit_exchange:delete(Name, false, ActingUser), ActingUser) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHost)],
    Funs = rabbit_misc:execute_mnesia_transaction(
          with(VHost, fun () -> internal_delete(VHost, ActingUser) end)),
    ok = rabbit_event:notify(vhost_deleted, [{name, VHost},
                                             {user_who_performed_action, ActingUser}]),
    [case Fun() of
         ok                                  -> ok;
         {error, {no_such_vhost, VHost}} -> ok
     end || Fun <- Funs],
    %% After vhost was deleted from mnesia DB, we try to stop vhost supervisors
    %% on all the nodes.
    rabbit_vhost_sup_sup:delete_on_all_nodes(VHost),
    ok.

put_vhost(Name, Description, Tags0, Trace, Username) ->
    Tags = case Tags0 of
      undefined   -> <<"">>;
      null        -> <<"">>;
      "undefined" -> <<"">>;
      "null"      -> <<"">>;
      Other       -> Other
    end,
    Result = case exists(Name) of
        true  -> ok;
        false ->
            ParsedTags = parse_tags(Tags),
            rabbit_log:debug("Parsed tags ~p to ~p", [Tags, ParsedTags]),
            add(Name, Description, ParsedTags, Username),
             %% wait for up to 45 seconds for the vhost to initialise
             %% on all nodes
             case await_running_on_all_nodes(Name, 45000) of
                 ok               ->
                     maybe_grant_full_permissions(Name, Username);
                 {error, timeout} ->
                     {error, timeout}
             end
    end,
    case Trace of
        true      -> rabbit_trace:start(Name);
        false     -> rabbit_trace:stop(Name);
        undefined -> ok
    end,
    Result.

%% when definitions are loaded on boot, Username here will be ?INTERNAL_USER,
%% which does not actually exist
maybe_grant_full_permissions(_Name, ?INTERNAL_USER) ->
    ok;
maybe_grant_full_permissions(Name, Username) ->
    U = rabbit_auth_backend_internal:lookup_user(Username),
    maybe_grant_full_permissions(U, Name, Username).

maybe_grant_full_permissions({ok, _}, Name, Username) ->
    rabbit_auth_backend_internal:set_permissions(
      Username, Name, <<".*">>, <<".*">>, <<".*">>, Username);
maybe_grant_full_permissions(_, _Name, _Username) ->
    ok.


%% 50 ms
-define(AWAIT_SAMPLE_INTERVAL, 50).

-spec await_running_on_all_nodes(vhost:name(), integer()) -> ok | {error, timeout}.
await_running_on_all_nodes(VHost, Timeout) ->
    Attempts = round(Timeout / ?AWAIT_SAMPLE_INTERVAL),
    await_running_on_all_nodes0(VHost, Attempts).

await_running_on_all_nodes0(_VHost, 0) ->
    {error, timeout};
await_running_on_all_nodes0(VHost, Attempts) ->
    case is_running_on_all_nodes(VHost) of
        true  -> ok;
        _     ->
            timer:sleep(?AWAIT_SAMPLE_INTERVAL),
            await_running_on_all_nodes0(VHost, Attempts - 1)
    end.

-spec is_running_on_all_nodes(vhost:name()) -> boolean().
is_running_on_all_nodes(VHost) ->
    States = vhost_cluster_state(VHost),
    lists:all(fun ({_Node, State}) -> State =:= running end,
              States).

-spec vhost_cluster_state(vhost:name()) -> [{atom(), atom()}].
vhost_cluster_state(VHost) ->
    Nodes = rabbit_nodes:all_running(),
    lists:map(fun(Node) ->
        State = case rabbit_misc:rpc_call(Node,
                                          rabbit_vhost_sup_sup, is_vhost_alive,
                                          [VHost]) of
            {badrpc, nodedown} -> nodedown;
            true               -> running;
            false              -> stopped
        end,
        {Node, State}
    end,
    Nodes).

vhost_down(VHost) ->
    ok = rabbit_event:notify(vhost_down,
                             [{name, VHost},
                              {node, node()},
                              {user_who_performed_action, ?INTERNAL_USER}]).

delete_storage(VHost) ->
    VhostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Deleting message store directory for vhost '~s' at '~s'", [VHost, VhostDir]),
    %% Message store should be closed when vhost supervisor is closed.
    case rabbit_file:recursive_delete([VhostDir]) of
        ok                   -> ok;
        {error, {_, enoent}} ->
            %% a concurrent delete did the job for us
            rabbit_log:warning("Tried to delete storage directories for vhost '~s', it failed with an ENOENT", [VHost]),
            ok;
        Other                ->
            rabbit_log:warning("Tried to delete storage directories for vhost '~s': ~p", [VHost, Other]),
            Other
    end.

assert_benign(ok, _)                 -> ok;
assert_benign({ok, _}, _)            -> ok;
assert_benign({ok, _, _}, _)         -> ok;
assert_benign({error, not_found}, _) -> ok;
assert_benign({error, {absent, Q, _}}, ActingUser) ->
    %% Removing the mnesia entries here is safe. If/when the down node
    %% restarts, it will clear out the on-disk storage of the queue.
    QName = amqqueue:get_name(Q),
    rabbit_amqqueue:internal_delete(QName, ActingUser).

internal_delete(VHost, ActingUser) ->
    %% MNESIA [ok = rabbit_auth_backend_internal:clear_permissions(
    %% MNESIA         proplists:get_value(user, Info), VHost, ActingUser)
    %% MNESIA  || Info <- rabbit_auth_backend_internal:list_vhost_permissions(VHost)],
    %% MNESIA TopicPermissions = rabbit_auth_backend_internal:list_vhost_topic_permissions(VHost),
    %% MNESIA [ok = rabbit_auth_backend_internal:clear_topic_permissions(
    %% MNESIA     proplists:get_value(user, TopicPermission), VHost, ActingUser)
    %% MNESIA  || TopicPermission <- TopicPermissions],
    Fs1 = [rabbit_runtime_parameters:clear(VHost,
                                           proplists:get_value(component, Info),
                                           proplists:get_value(name, Info),
                                           ActingUser)
     || Info <- rabbit_runtime_parameters:list(VHost)],
    Fs2 = [rabbit_policy:delete(VHost, proplists:get_value(name, Info), ActingUser)
           || Info <- rabbit_policy:list(VHost)],
    %% MNESIA ok = mnesia:delete({rabbit_vhost, VHost}),
    Path = khepri_vhost_path(VHost),
    {ok, _} = rabbit_khepri:delete(Path),
    Fs1 ++ Fs2.

-spec exists(vhost:name()) -> boolean().

exists(VHost) ->
    %% MNESIA mnesia:dirty_read({rabbit_vhost, VHost}) /= [].
    Path = khepri_vhost_path(VHost),
    rabbit_khepri:exists(Path).

-spec list_names() -> [vhost:name()].
list_names() ->
    %% MNESIA mnesia:dirty_all_keys(rabbit_vhost).
    Path = khepri_vhosts_path(),
    case rabbit_khepri:list_with_props(Path) of
        {ok, Result} -> [lists:last(P) || P <- maps:keys(Result)];
        _            -> []
    end.

%% Exists for backwards compatibility, prefer list_names/0.
-spec list() -> [vhost:name()].
list() -> list_names().

-spec all() -> [vhost:vhost()].
all() ->
    %% MNESIA mnesia:dirty_match_object(rabbit_vhost, vhost:pattern_match_all()).
    Path = khepri_vhosts_path(),
    case rabbit_khepri:list_matching(Path, vhost:pattern_match_all()) of
        {ok, List} -> List;
        _          -> []
    end.

-spec count() -> non_neg_integer().
count() ->
    length(list()).

-spec lookup(vhost:name()) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
lookup(VHostName) ->
    %% MNESIA case rabbit_misc:dirty_read({rabbit_vhost, VHostName}) of
    %% MNESIA     {error, not_found} -> {error, {no_such_vhost, VHostName}};
    %% MNESIA     {ok, Record}       -> Record
    %% MNESIA end.
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get(Path) of
        {ok, Record} -> Record;
        _            -> {error, {no_such_vhost, VHostName}}
    end.

-spec with(vhost:name(), rabbit_misc:thunk(A)) -> A.
with(VHostName, Thunk) ->
    %% MNESIA fun () ->
    %% MNESIA     case mnesia:read({rabbit_vhost, VHostName}) of
    %% MNESIA         []   -> mnesia:abort({no_such_vhost, VHostName});
    %% MNESIA         [_V] -> Thunk()
    %% MNESIA     end
    %% MNESIA end.
    fun() ->
            Path = khepri_vhost_path(VHostName),
            case rabbit_khepri:get(Path) of
                {ok, _} -> Thunk();
                _       -> mnesia:abort({no_such_vhost, VHostName})
            end
    end.

-spec with_user_and_vhost(rabbit_types:username(), vhost:name(), rabbit_misc:thunk(A)) -> A.
with_user_and_vhost(Username, VHostName, Thunk) ->
    %% MNESIA rabbit_misc:with_user(Username, with(VHostName, Thunk)).
    rabbit_auth_backend_internal:with_user(Username, with(VHostName, Thunk)).

%% Like with/2 but outside an Mnesia tx

-spec assert(vhost:name()) -> 'ok'.
assert(VHostName) ->
    case exists(VHostName) of
        true  -> ok;
        false -> throw({error, {no_such_vhost, VHostName}})
    end.

-spec update(vhost:name(), fun((vhost:vhost()) -> vhost:vhost())) -> vhost:vhost().
update(VHostName, Fun) ->
    %% MNESIA case mnesia:read({rabbit_vhost, VHostName}) of
    %% MNESIA     [] ->
    %% MNESIA         mnesia:abort({no_such_vhost, VHostName});
    %% MNESIA     [V] ->
    %% MNESIA         V1 = Fun(V),
    %% MNESIA         ok = mnesia:write(rabbit_vhost, V1, write),
    %% MNESIA         V1
    %% MNESIA end.
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get_with_props(Path) of
        {ok, #{Path := #{data := V, data_version := DVersion}}} ->
            V1 = Fun(V),
            Ret = rabbit_khepri:insert(
                    Path, V1, #if_data_version{version = DVersion}),
            case Ret of
                {ok, _} ->
                    V1;
                {error, {mismatching_node, _, _, _}} ->
                    update(VHostName, Fun);
                Error ->
                    throw(Error)
            end;
        _ ->
            mnesia:abort({no_such_vhost, VHostName})
    end.

-spec update_metadata(vhost:name(), fun((map())-> map())) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_metadata(VHostName, Fun) ->
    update(VHostName, fun(Record) ->
        Meta = Fun(vhost:get_metadata(Record)),
        vhost:set_metadata(Record, Meta)
    end).

-spec update_tags(vhost:name(), [vhost_tag()], rabbit_types:username()) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_tags(VHostName, Tags, ActingUser) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    try
        R = rabbit_misc:execute_mnesia_transaction(fun() ->
            update_tags(VHostName, ConvertedTags)
        end),
        rabbit_log:info("Successfully set tags for virtual host '~s' to ~p", [VHostName, ConvertedTags]),
        rabbit_event:notify(vhost_tags_set, [{name, VHostName},
                                             {tags, ConvertedTags},
                                             {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': the virtual host does not exist", [VHostName]),
            throw(Error);
        throw:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': ~p", [VHostName, Error]),
            throw(Error);
        exit:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': ~p", [VHostName, Error]),
            exit(Error)
    end.

-spec update_tags(vhost:name(), [vhost_tag()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_tags(VHostName, Tags) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    update(VHostName, fun(Record) ->
        Meta0 = vhost:get_metadata(Record),
        Meta  = maps:update(tags, ConvertedTags, Meta0),
        vhost:set_metadata(Record, Meta)
    end).

-spec tag_with(vhost:name(), [atom()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
tag_with(VHostName, Tags) when is_list(Tags) ->
    update_metadata(VHostName, fun(#{tags := Tags0} = Meta) ->
        maps:update(tags, lists:usort(Tags0 ++ Tags), Meta)
    end).

-spec untag_from(vhost:name(), [atom()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
untag_from(VHostName, Tags) when is_list(Tags) ->
    update_metadata(VHostName, fun(#{tags := Tags0} = Meta) ->
        maps:update(tags, lists:usort(Tags0 -- Tags), Meta)
    end).

set_limits(VHost, undefined) ->
    vhost:set_limits(VHost, []);
set_limits(VHost, Limits) ->
    vhost:set_limits(VHost, Limits).


dir(Vhost) ->
    <<Num:128>> = erlang:md5(Vhost),
    rabbit_misc:format("~.36B", [Num]).

msg_store_dir_path(VHost) ->
    EncodedName = dir(VHost),
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), EncodedName])).

msg_store_dir_wildcard() ->
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), "*"])).

msg_store_dir_base() ->
    Dir = rabbit_mnesia:dir(),
    filename:join([Dir, "msg_stores", "vhosts"]).

config_file_path(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    filename:join(VHostDir, ".config").

-spec trim_tag(list() | binary() | atom()) -> atom().
trim_tag(Val) ->
    rabbit_data_coercion:to_atom(string:trim(rabbit_data_coercion:to_list(Val))).

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> vhost:get_name(VHost);
i(tracing, VHost) -> rabbit_trace:enabled(vhost:get_name(VHost));
i(cluster_state, VHost) -> vhost_cluster_state(vhost:get_name(VHost));
i(description, VHost) -> vhost:get_description(VHost);
i(tags, VHost) -> vhost:get_tags(VHost);
i(metadata, VHost) -> vhost:get_metadata(VHost);
i(Item, VHost)     ->
  rabbit_log:error("Don't know how to compute a virtual host info item '~s' for virtual host '~p'", [Item, VHost]),
  throw({bad_argument, Item}).

-spec info(vhost:vhost() | vhost:name()) -> rabbit_types:infos().

info(VHost) when ?is_vhost(VHost) ->
    infos(?INFO_KEYS, VHost);
info(Key) ->
    %% MNESIA case mnesia:dirty_read({rabbit_vhost, Key}) of
    %% MNESIA     [] -> [];
    %% MNESIA     [VHost] -> infos(?INFO_KEYS, VHost)
    %% MNESIA end.
    Path = khepri_vhost_path(Key),
    case rabbit_khepri:get(Path) of
        {ok, VHost} -> infos(?INFO_KEYS, VHost);
        _           -> [] 
    end.

-spec info(vhost:vhost(), rabbit_types:info_keys()) -> rabbit_types:infos().
info(VHost, Items) -> infos(Items, VHost).

-spec info_all() -> [rabbit_types:infos()].
info_all()       -> info_all(?INFO_KEYS).

-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].
info_all(Items) -> [info(VHost, Items) || VHost <- all()].

info_all(Ref, AggregatorPid)        -> info_all(?INFO_KEYS, Ref, AggregatorPid).

-spec info_all(rabbit_types:info_keys(), reference(), pid()) -> 'ok'.
info_all(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
       AggregatorPid, Ref, fun(VHost) -> info(VHost, Items) end, all()).

khepri_vhosts_path()     -> [?MODULE].
khepri_vhost_path(VHost) -> [?MODULE, VHost].
