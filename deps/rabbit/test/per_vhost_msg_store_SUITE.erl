%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_vhost_msg_store_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(MSGS_COUNT, 100).

all() ->
    [
      publish_to_different_dirs,
      storage_deleted_on_vhost_delete,
      single_vhost_storage_delete_is_safe
    ].



init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, ?MODULE}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit, [{queue_index_embed_msgs_below, 100}]}),
    rabbit_ct_helpers:run_setup_steps(
        Config2,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
        Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(_, Config) ->
    Vhost1 = <<"vhost1">>,
    Vhost2 = <<"vhost2">>,
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost1),
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost2),
    Chan1 = open_channel(Vhost1, Config),
    Chan2 = open_channel(Vhost2, Config),
    rabbit_ct_helpers:set_config(
        Config,
        [{vhost1, Vhost1}, {vhost2, Vhost2},
         {channel1, Chan1}, {channel2, Chan2}]).

end_per_testcase(single_vhost_storage_delete_is_safe, Config) ->
    Config;
end_per_testcase(_, Config) ->
    Vhost1 = ?config(vhost1, Config),
    Vhost2 = ?config(vhost2, Config),
    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost2),
    Config.

publish_to_different_dirs(Config) ->
    Vhost1 = ?config(vhost1, Config),
    Vhost2 = ?config(vhost2, Config),
    Channel1 = ?config(channel1, Config),
    Channel2 = ?config(channel2, Config),
    Queue1 = declare_durable_queue(Channel1),
    Queue2 = declare_durable_queue(Channel2),
    FolderSize1 = get_folder_size(Vhost1, Config),
    FolderSize2 = get_folder_size(Vhost2, Config),

    %% Publish message to a queue index
    publish_persistent_messages(index, Channel1, Queue1),
    %% First storage increased
    FolderSize11 = get_folder_size(Vhost1, Config),
    true = (FolderSize1 < FolderSize11),
    %% Second storage didn't increased
    FolderSize2 = get_folder_size(Vhost2, Config),

    %% Publish message to a message store
    publish_persistent_messages(store, Channel1, Queue1),
    %% First storage increased
    FolderSize12 = get_folder_size(Vhost1, Config),
    true = (FolderSize11 < FolderSize12),
    %% Second storage didn't increased
    FolderSize2 = get_folder_size(Vhost2, Config),

    %% Publish message to a queue index
    publish_persistent_messages(index, Channel2, Queue2),
    %% First storage increased
    FolderSize21 = get_folder_size(Vhost2, Config),
    true = (FolderSize2 < FolderSize21),
    %% Second storage didn't increased
    FolderSize12 = get_folder_size(Vhost1, Config),

    %% Publish message to a message store
    publish_persistent_messages(store, Channel2, Queue2),
    %% Second storage increased
    FolderSize22 = get_folder_size(Vhost2, Config),
    true = (FolderSize21 < FolderSize22),
    %% First storage didn't increased
    FolderSize12 = get_folder_size(Vhost1, Config).

storage_deleted_on_vhost_delete(Config) ->
    Vhost1 = ?config(vhost1, Config),
    Channel1 = ?config(channel1, Config),
    Queue1 = declare_durable_queue(Channel1),
    FolderSize = get_global_folder_size(Config),

    publish_persistent_messages(index, Channel1, Queue1),
    publish_persistent_messages(store, Channel1, Queue1),
    FolderSizeAfterPublish = get_global_folder_size(Config),

    %% Total storage size increased
    true = (FolderSize < FolderSizeAfterPublish),

    ok = rabbit_ct_broker_helpers:delete_vhost(Config, Vhost1),

    %% Total memory reduced
    FolderSizeAfterDelete = get_global_folder_size(Config),
    true = (FolderSizeAfterPublish > FolderSizeAfterDelete),

    %% There is no Vhost1 folder
    0 = get_folder_size(Vhost1, Config).


single_vhost_storage_delete_is_safe(Config) ->
ct:pal("Start test 3", []),
    Vhost1 = ?config(vhost1, Config),
    Vhost2 = ?config(vhost2, Config),
    Channel1 = ?config(channel1, Config),
    Channel2 = ?config(channel2, Config),
    Queue1 = declare_durable_queue(Channel1),
    Queue2 = declare_durable_queue(Channel2),

    %% Publish messages to both stores
    publish_persistent_messages(index, Channel1, Queue1),
    publish_persistent_messages(store, Channel1, Queue1),
    publish_persistent_messages(index, Channel2, Queue2),
    publish_persistent_messages(store, Channel2, Queue2),

    queue_is_not_empty(Channel2, Queue2),
    % Vhost2Dir = vhost_dir(Vhost2, Config),
    % [StoreFile] = filelib:wildcard(binary_to_list(filename:join([Vhost2Dir, "msg_store_persistent_*", "0.rdq"]))),
    % ct:pal("Store file ~p~n", [file:read_file(StoreFile)]).
% ok.
    rabbit_ct_broker_helpers:stop_broker(Config, 0),
    delete_vhost_data(Vhost1, Config),
    rabbit_ct_broker_helpers:start_broker(Config, 0),

    Channel11 = open_channel(Vhost1, Config),
    Channel21 = open_channel(Vhost2, Config),

    %% There are no Vhost1 messages
    queue_is_empty(Channel11, Queue1),

    %% But Vhost2 messages are in place
    queue_is_not_empty(Channel21, Queue2),
    consume_messages(index, Channel21, Queue2),
    consume_messages(store, Channel21, Queue2).

declare_durable_queue(Channel) ->
    QName = list_to_binary(erlang:ref_to_list(make_ref())),
    #'queue.declare_ok'{queue = QName} =
        amqp_channel:call(Channel,
                          #'queue.declare'{queue = QName, durable = true}),
    QName.

publish_persistent_messages(Storage, Channel, Queue) ->
    MessagePayload = case Storage of
        index -> binary:copy(<<"=">>, 50);
        store -> binary:copy(<<"-">>, 150)
    end,
    amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(Channel,
                       #'basic.publish'{routing_key = Queue},
                       #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                 payload = MessagePayload})
     || _ <- lists:seq(1, ?MSGS_COUNT)],
    amqp_channel:wait_for_confirms(Channel).


get_folder_size(Vhost, Config) ->
    Dir = vhost_dir(Vhost, Config),
    folder_size(Dir).

folder_size(Dir) ->
    filelib:fold_files(Dir, ".*", true,
                       fun(F,Acc) -> filelib:file_size(F) + Acc end, 0).

get_global_folder_size(Config) ->
    BaseDir = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_mnesia, dir, []),
    folder_size(BaseDir).

vhost_dir(Vhost, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 rabbit_vhost, msg_store_dir_path, [Vhost]).

delete_vhost_data(Vhost, Config) ->
    Dir = vhost_dir(Vhost, Config),
    rabbit_file:recursive_delete([Dir]).

queue_is_empty(Channel, Queue) ->
    #'queue.declare_ok'{queue = Queue, message_count = 0} =
        amqp_channel:call(Channel,
                          #'queue.declare'{ queue = Queue,
                                            durable = true,
                                            passive = true}).

queue_is_not_empty(Channel, Queue) ->
    #'queue.declare_ok'{queue = Queue, message_count = MsgCount} =
        amqp_channel:call(Channel,
                          #'queue.declare'{ queue = Queue,
                                            durable = true,
                                            passive = true}),
    ExpectedCount = ?MSGS_COUNT * 2,
    ExpectedCount = MsgCount.

consume_messages(Storage, Channel, Queue) ->
    MessagePayload = case Storage of
        index -> binary:copy(<<"=">>, 50);
        store -> binary:copy(<<"-">>, 150)
    end,
    lists:foreach(
        fun(I) ->
            ct:pal("Consume message ~p~n ~p~n", [I, MessagePayload]),
            {#'basic.get_ok'{}, Content} =
                amqp_channel:call(Channel,
                                  #'basic.get'{queue = Queue,
                                  no_ack = true}),
            #amqp_msg{payload = MessagePayload} = Content
        end,
        lists:seq(1, ?MSGS_COUNT)),
    ok.

open_channel(Vhost, Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    {ok, Conn} = amqp_connection:start(
        #amqp_params_direct{node = Node, virtual_host = Vhost}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Chan.
