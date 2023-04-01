%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vhost).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("vhost.hrl").

-export([recover/0, recover/1, read_config/1]).
-export([add/2, add/3, add/4, delete/2, exists/1, assert/1,
         set_limits/2, vhost_cluster_state/1, is_running_on_all_nodes/1, await_running_on_all_nodes/2,
        list/0, count/0, list_names/0, all/0, all_tagged_with/1]).
-export([parse_tags/1, update_tags/3]).
-export([lookup/1, default_name/0]).
-export([info/1, info/2, info_all/0, info_all/1, info_all/2, info_all/3]).
-export([dir/1, msg_store_dir_path/1, msg_store_dir_wildcard/0, config_file_path/1, ensure_config_file/1]).
-export([delete_storage/1]).
-export([vhost_down/1]).
-export([put_vhost/5,
         put_vhost/6]).

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
    {Time, _} = timer:tc(fun() ->
                                 rabbit_binding:recover()
                         end),
    rabbit_log:debug("rabbit_binding:recover/0 completed in ~fs", [Time/1000000]),

    %% rabbit_vhost_sup_sup will start the actual recovery.
    %% So recovery will be run every time a vhost supervisor is restarted.
    ok = rabbit_vhost_sup_sup:start(),

    [ok = rabbit_vhost_sup_sup:init_vhost(VHost) || VHost <- list_names()],
    ok.

recover(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Making sure data directory '~ts' for vhost '~ts' exists",
                    [VHostDir, VHost]),
    VHostStubFile = filename:join(VHostDir, ".vhost"),
    ok = rabbit_file:ensure_dir(VHostStubFile),
    ok = file:write_file(VHostStubFile, VHost),
    ok = ensure_config_file(VHost),
    {Recovered, Failed} = rabbit_amqqueue:recover(VHost),
    AllQs = Recovered ++ Failed,
    QNames = [amqqueue:get_name(Q) || Q <- AllQs],
    {Time, ok} = timer:tc(fun() ->
                                  rabbit_binding:recover(rabbit_exchange:recover(VHost), QNames)
                          end),
    rabbit_log:debug("rabbit_binding:recover/2 for vhost ~ts completed in ~fs", [VHost, Time/1000000]),

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
            rabbit_log:info("Setting segment_entry_count for vhost '~ts' with ~b queues to '~b'",
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
    ValUnicode = rabbit_data_coercion:to_unicode_charlist(Val),
    [trim_tag(Tag) || Tag <- re:split(ValUnicode, ",", [unicode, {return, list}])];
parse_tags(Val) when is_list(Val) ->
    case hd(Val) of
      Bin when is_binary(Bin) ->
        %% this is a list of binaries
        [trim_tag(Tag) || Tag <- Val];
      Atom when is_atom(Atom) ->
        %% this is a list of atoms
        [trim_tag(Tag) || Tag <- Val];
      Int when is_integer(Int) ->
        %% this is a string/charlist
        ValUnicode = rabbit_data_coercion:to_unicode_charlist(Val),
        [trim_tag(Tag) || Tag <- re:split(ValUnicode, ",", [unicode, {return, list}])]
    end.

-spec add(vhost:name(), rabbit_types:username()) ->
    rabbit_types:ok_or_error(any()).
add(VHost, ActingUser) ->
    add(VHost, #{}, ActingUser).

-spec add(vhost:name(), binary(), [atom()], rabbit_types:username()) ->
    rabbit_types:ok_or_error(any()).
add(Name, Description, Tags, ActingUser) ->
    add(Name, #{description => Description,
                tags => Tags}, ActingUser).

-spec add(vhost:name(), vhost:metadata(), rabbit_types:username()) ->
    rabbit_types:ok_or_error(any()).
add(Name, Metadata, ActingUser) ->
    case exists(Name) of
        true  -> ok;
        false ->
            catch(do_add(Name, Metadata, ActingUser))
    end.

do_add(Name, Metadata, ActingUser) ->
    ok = is_over_vhost_limit(Name),
    Description = maps:get(description, Metadata, undefined),
    Tags = maps:get(tags, Metadata, []),

    %% validate default_queue_type
    case Metadata of
        #{default_queue_type := DQT} ->
            %% check that the queue type is known
            try rabbit_queue_type:discover(DQT) of
                _ ->
                    case rabbit_queue_type:feature_flag_name(DQT) of
                        undefined -> ok;
                        Flag when is_atom(Flag) ->
                            case rabbit_feature_flags:is_enabled(Flag) of
                                true  -> ok;
                                false -> throw({error, queue_type_feature_flag_is_not_enabled})
                            end
                    end
            catch _:_ ->
                      throw({error, invalid_queue_type})
            end;
        _ ->
            ok
    end,

    case Description of
        undefined ->
            rabbit_log:info("Adding vhost '~ts' without a description", [Name]);
        Description ->
            rabbit_log:info("Adding vhost '~ts' (description: '~ts', tags: ~tp)",
                            [Name, Description, Tags])
    end,
    DefaultLimits = rabbit_db_vhost_defaults:list_limits(Name),

    {NewOrNot, VHost} = rabbit_db_vhost:create_or_get(Name, DefaultLimits, Metadata),
    case NewOrNot of
        new ->
            rabbit_log:info("Inserted a virtual host record ~tp", [VHost]);
        existing ->
            ok
    end,
    rabbit_db_vhost_defaults:apply(Name, ActingUser),
    _ = [begin
         Resource = rabbit_misc:r(Name, exchange, ExchangeName),
         rabbit_log:debug("Will declare an exchange ~tp", [Resource]),
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
            Msg = rabbit_misc:format("failed to set up vhost '~ts': ~tp",
                                     [Name, Reason]),
            {error, Msg}
    end.

-spec update(vhost:name(), binary(), [atom()], rabbit_types:username()) -> rabbit_types:ok_or_error(any()).
update(Name, Description, Tags, ActingUser) ->
    Metadata = #{description => Description, tags => Tags},
    case rabbit_db_vhost:merge_metadata(Name, Metadata) of
        {ok, VHost} ->
            rabbit_event:notify(
              vhost_updated,
              info(VHost) ++ [{user_who_performed_action, ActingUser},
                              {description, Description},
                              {tags, Tags}]),
            ok;
        {error, _} = Error ->
            Error
    end.

-spec delete(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

delete(VHost, ActingUser) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further database actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~ts'", [VHost]),
    %% TODO: This code does a lot of "list resources, walk through the list to
    %% delete each resource". This feature should be provided by each called
    %% modules, like `rabbit_amqqueue:delete_all_for_vhost(VHost)'. These new
    %% calls would be responsible for the atomicity, not this code.
    %% Clear the permissions first to prohibit new incoming connections when deleting a vhost
    _ = rabbit_auth_backend_internal:clear_permissions_for_vhost(VHost, ActingUser),
    _ = rabbit_auth_backend_internal:clear_topic_permissions_for_vhost(VHost, ActingUser),
    QDelFun = fun (Q) -> rabbit_amqqueue:delete(Q, false, false, ActingUser) end,
    [begin
         Name = amqqueue:get_name(Q),
         assert_benign(rabbit_amqqueue:with(Name, QDelFun), ActingUser)
     end || Q <- rabbit_amqqueue:list(VHost)],
    [assert_benign(rabbit_exchange:delete(Name, false, ActingUser), ActingUser) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHost)],
    _ = rabbit_runtime_parameters:clear_vhost(VHost, ActingUser),
    _ = [rabbit_policy:delete(VHost, proplists:get_value(name, Info), ActingUser)
         || Info <- rabbit_policy:list(VHost)],
    case rabbit_db_vhost:delete(VHost) of
        true ->
            ok = rabbit_event:notify(
                   vhost_deleted,
                   [{name, VHost},
                    {user_who_performed_action, ActingUser}]);
        false ->
            ok
    end,
    %% After vhost was deleted from the database, we try to stop vhost
    %% supervisors on all the nodes.
    rabbit_vhost_sup_sup:delete_on_all_nodes(VHost),
    ok.

-spec put_vhost(vhost:name(),
    binary(),
    vhost:tags(),
    boolean(),
    rabbit_types:username()) ->
    'ok' | {'error', any()} | {'EXIT', any()}.
put_vhost(Name, Description, Tags0, Trace, Username) ->
    put_vhost(Name, Description, Tags0, undefined, Trace, Username).

-spec put_vhost(vhost:name(),
    binary(),
    vhost:unparsed_tags() | vhost:tags(),
    rabbit_queue_type:queue_type() | 'undefined',
    boolean(),
    rabbit_types:username()) ->
    'ok' | {'error', any()} | {'EXIT', any()}.
put_vhost(Name, Description, Tags0, DefaultQueueType, Trace, Username) ->
    Tags = case Tags0 of
      undefined   -> <<"">>;
      null        -> <<"">>;
      "undefined" -> <<"">>;
      "null"      -> <<"">>;
      Other       -> Other
    end,
    ParsedTags = parse_tags(Tags),
    rabbit_log:debug("Parsed tags ~tp to ~tp", [Tags, ParsedTags]),
    Result = case exists(Name) of
                 true  ->
                     update(Name, Description, ParsedTags, Username);
                 false ->
                     Metadata0 = #{description => Description,
                                   tags => ParsedTags},
                     Metadata = case DefaultQueueType of
                                    undefined ->
                                        Metadata0;
                                    _ ->
                                        Metadata0#{default_queue_type =>
                                                       DefaultQueueType}
                                end,
                     case add(Name, Metadata, Username) of
                         ok ->
                             %% wait for up to 45 seconds for the vhost to initialise
                             %% on all nodes
                             case await_running_on_all_nodes(Name, 45000) of
                                 ok               ->
                                     maybe_grant_full_permissions(Name, Username);
                                 {error, timeout} ->
                                     {error, timeout}
                             end;
                         Err ->
                             Err
                     end
             end,
    case Trace of
        true      -> rabbit_trace:start(Name);
        false     -> rabbit_trace:stop(Name);
        undefined -> ok
    end,
    Result.

-spec is_over_vhost_limit(vhost:name()) -> 'ok' | no_return().
is_over_vhost_limit(Name) ->
    Limit = rabbit_misc:get_env(rabbit, vhost_max, infinity),
    is_over_vhost_limit(Name, Limit).

-spec is_over_vhost_limit(vhost:name(), 'infinity' | non_neg_integer())
        -> 'ok' | no_return().
is_over_vhost_limit(_Name, infinity) ->
    ok;
is_over_vhost_limit(Name, Limit) when is_integer(Limit) ->
    case length(rabbit_db_vhost:list()) >= Limit of
        false ->
            ok;
        true ->
            ErrorMsg = rabbit_misc:format("cannot create vhost '~ts': "
                                          "vhost limit of ~tp is reached",
                                          [Name, Limit]),
            exit({vhost_limit_exceeded, ErrorMsg})
    end.

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
    Nodes = rabbit_nodes:list_running(),
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
    rabbit_log:info("Deleting message store directory for vhost '~ts' at '~ts'", [VHost, VhostDir]),
    %% Message store should be closed when vhost supervisor is closed.
    case rabbit_file:recursive_delete([VhostDir]) of
        ok                   -> ok;
        {error, {_, enoent}} ->
            %% a concurrent delete did the job for us
            rabbit_log:warning("Tried to delete storage directories for vhost '~ts', it failed with an ENOENT", [VHost]),
            ok;
        Other                ->
            rabbit_log:warning("Tried to delete storage directories for vhost '~ts': ~tp", [VHost, Other]),
            Other
    end.

assert_benign(ok, _)                 -> ok;
assert_benign({ok, _}, _)            -> ok;
assert_benign({ok, _, _}, _)         -> ok;
assert_benign({error, not_found}, _) -> ok;
assert_benign({error, {absent, Q, _}}, ActingUser) ->
    %% Removing the database entries here is safe. If/when the down node
    %% restarts, it will clear out the on-disk storage of the queue.
    rabbit_amqqueue:internal_delete(Q, ActingUser).

-spec exists(vhost:name()) -> boolean().

exists(VHost) ->
    rabbit_db_vhost:exists(VHost).

-spec list_names() -> [vhost:name()].
list_names() -> rabbit_db_vhost:list().

%% Exists for backwards compatibility, prefer list_names/0.
-spec list() -> [vhost:name()].
list() -> list_names().

-spec all() -> [vhost:vhost()].
all() -> rabbit_db_vhost:get_all().

-spec all_tagged_with(atom()) -> [vhost:vhost()].
all_tagged_with(TagName) ->
    lists:filter(
        fun(VHost) ->
            Meta = vhost:get_metadata(VHost),
            case Meta of
                #{tags := Tags} ->
                    lists:member(rabbit_data_coercion:to_atom(TagName), Tags);
                _ -> false
            end
        end, all()).

-spec count() -> non_neg_integer().
count() ->
    length(list()).

-spec default_name() -> vhost:name().
default_name() ->
    case application:get_env(default_vhost) of
        {ok, Value} -> Value;
        undefined   -> <<"/">>
    end.

-spec lookup(vhost:name()) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
lookup(VHostName) ->
    case rabbit_db_vhost:get(VHostName) of
        undefined -> {error, {no_such_vhost, VHostName}};
        VHost     -> VHost
    end.

-spec assert(vhost:name()) -> 'ok'.
assert(VHostName) ->
    case exists(VHostName) of
        true  -> ok;
        false -> throw({error, {no_such_vhost, VHostName}})
    end.

are_different0([], []) ->
    false;
are_different0([], [_ | _]) ->
    true;
are_different0([_ | _], []) ->
    true;
are_different0([E], [E]) ->
    false;
are_different0([E | R1], [E | R2]) ->
    are_different0(R1, R2);
are_different0(_, _) ->
    true.

are_different(L1, L2) ->
    are_different0(lists:usort(L1), lists:usort(L2)).

-spec update_tags(vhost:name(), [vhost_tag()], rabbit_types:username()) -> vhost:vhost().
update_tags(VHostName, Tags, ActingUser) ->
    try
        CurrentTags = case rabbit_db_vhost:get(VHostName) of
                          undefined -> [];
                          V -> vhost:get_tags(V)
                      end,
        VHost = rabbit_db_vhost:set_tags(VHostName, Tags),
        ConvertedTags = vhost:get_tags(VHost),
        rabbit_log:info("Successfully set tags for virtual host '~ts' to ~tp", [VHostName, ConvertedTags]),
        rabbit_event:notify_if(are_different(CurrentTags, ConvertedTags),
                               vhost_tags_set, [{name, VHostName},
                                                {tags, ConvertedTags},
                                                {user_who_performed_action, ActingUser}]),
        VHost
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~ts': the virtual host does not exist", [VHostName]),
            throw(Error);
        throw:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~ts': ~tp", [VHostName, Error]),
            throw(Error);
        exit:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~ts': ~tp", [VHostName, Error]),
            exit(Error)
    end.

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
    Dir = rabbit:data_dir(),
    filename:join([Dir, "msg_stores", "vhosts"]).

config_file_path(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    filename:join(VHostDir, ".config").

-spec trim_tag(list() | binary() | atom()) -> atom().
trim_tag(Val) when is_atom(Val) ->
    trim_tag(rabbit_data_coercion:to_binary(Val));
trim_tag(Val) when is_list(Val) ->
    trim_tag(rabbit_data_coercion:to_utf8_binary(Val));
trim_tag(Val) when is_binary(Val) ->
    ValTrimmed = string:trim(Val),
    rabbit_data_coercion:to_atom(ValTrimmed).

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> vhost:get_name(VHost);
i(tracing, VHost) -> rabbit_trace:enabled(vhost:get_name(VHost));
i(cluster_state, VHost) -> vhost_cluster_state(vhost:get_name(VHost));
i(description, VHost) -> vhost:get_description(VHost);
i(tags, VHost) -> vhost:get_tags(VHost);
i(default_queue_type, VHost) -> vhost:get_default_queue_type(VHost);
i(metadata, VHost) -> vhost:get_metadata(VHost);
i(Item, VHost)     ->
  rabbit_log:error("Don't know how to compute a virtual host info item '~ts' for virtual host '~tp'", [Item, VHost]),
  throw({bad_argument, Item}).

-spec info(vhost:vhost() | vhost:name()) -> rabbit_types:infos().

info(VHost) when ?is_vhost(VHost) ->
    infos(?INFO_KEYS, VHost);
info(Key) ->
    case rabbit_db_vhost:get(Key) of
        undefined -> [];
        VHost     -> infos(?INFO_KEYS, VHost)
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
