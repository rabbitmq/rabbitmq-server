%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_default_queue_type_SUITE).

-include_lib("common_test/include/ct.hrl").

-import(rabbit_ct_broker_helpers, [rpc/5]).

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
     inject_dqt_undefined_binary,
     inject_dqt_null,
     inject_dqt_nil,
     inject_dqt_preserves_existing_metadata,
     new_metadata_sanitizes_undefined_binary,
     new_metadata_sanitizes_null,
     new_metadata_sanitizes_nil,
     vhost_import_with_undefined_dqt,
     vhost_import_with_null_dqt,
     vhost_import_with_nil_dqt,
     update_metadata_sanitizes_undefined_binary,
     update_metadata_sanitizes_null,
     update_metadata_sanitizes_nil
    ].

%% -------------------------------------------------------------------
%% Test suite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Testcase},
        {rmq_nodes_count, 1}
    ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_helpers:testcase_started(Config2, Testcase).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

%% When default_queue_type is the binary <<"undefined">> (exported from older versions),
%% inject the default. See rabbitmq/rabbitmq-server#10469
inject_dqt_undefined_binary(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{default_queue_type => <<"undefined">>, name => <<"/">>},
    #{default_queue_type := Expected,
      metadata := #{default_queue_type := Expected}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.

inject_dqt_null(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{default_queue_type => null, name => <<"/">>},
    #{default_queue_type := Expected,
      metadata := #{default_queue_type := Expected}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.

inject_dqt_nil(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{default_queue_type => nil, name => <<"/">>},
    #{default_queue_type := Expected,
      metadata := #{default_queue_type := Expected}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.

%% Existing metadata keys should be preserved when injecting DQT
inject_dqt_preserves_existing_metadata(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    Input = #{
        default_queue_type => undefined,
        name => <<"/">>,
        metadata => #{
            description => <<"test vhost">>,
            tags => [replicate]
        }
    },
    #{metadata := #{default_queue_type := Expected,
                    description := <<"test vhost">>,
                    tags := [replicate]}} =
        rpc(Config, 0, rabbit_queue_type, inject_dqt, [Input]),
    ok.

new_metadata_sanitizes_undefined_binary(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    #{default_queue_type := Expected} =
        rpc(Config, 0, vhost, new_metadata, [<<"description">>, [], <<"undefined">>]),
    #{default_queue_type := Expected} =
        rpc(Config, 0, vhost, new_metadata, [<<"description">>, [], undefined]),
    #{default_queue_type := <<"quorum">>} =
        rpc(Config, 0, vhost, new_metadata, [<<"description">>, [], <<"quorum">>]),
    ok.

new_metadata_sanitizes_null(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    #{default_queue_type := Expected} =
        rpc(Config, 0, vhost, new_metadata, [<<"description">>, [], null]),
    ok.

new_metadata_sanitizes_nil(Config) ->
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    #{default_queue_type := Expected} =
        rpc(Config, 0, vhost, new_metadata, [<<"description">>, [], nil]),
    ok.

vhost_import_with_undefined_dqt(Config) ->
    VHost = <<"import-undefined-dqt-test">>,
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test description">>, [], <<"undefined">>, false, <<"test-user">>]),
        V = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        Metadata = rpc(Config, 0, vhost, get_metadata, [V]),
        #{default_queue_type := StoredDQT} = Metadata,
        Expected = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.

vhost_import_with_null_dqt(Config) ->
    VHost = <<"import-null-dqt-test">>,
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test description">>, [], null, false, <<"test-user">>]),
        V = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        Metadata = rpc(Config, 0, vhost, get_metadata, [V]),
        #{default_queue_type := StoredDQT} = Metadata,
        Expected = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.

vhost_import_with_nil_dqt(Config) ->
    VHost = <<"import-nil-dqt-test">>,
    Expected = rpc(Config, 0, rabbit_queue_type, default_alias, []),
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test description">>, [], nil, false, <<"test-user">>]),
        V = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        Metadata = rpc(Config, 0, vhost, get_metadata, [V]),
        #{default_queue_type := StoredDQT} = Metadata,
        Expected = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.

update_metadata_sanitizes_undefined_binary(Config) ->
    VHost = <<"update-metadata-undefined-dqt-test">>,
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test">>, [], <<"classic">>, false, <<"test-user">>]),
        V1 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := <<"classic">>} = rpc(Config, 0, vhost, get_metadata, [V1]),
        %% update_metadata should not overwrite with <<"undefined">>
        ok = rpc(Config, 0, rabbit_vhost, update_metadata,
                 [VHost, #{default_queue_type => <<"undefined">>}, <<"test-user">>]),
        V2 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := StoredDQT} = rpc(Config, 0, vhost, get_metadata, [V2]),
        <<"classic">> = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.

update_metadata_sanitizes_null(Config) ->
    VHost = <<"update-metadata-null-dqt-test">>,
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test">>, [], <<"classic">>, false, <<"test-user">>]),
        V1 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := <<"classic">>} = rpc(Config, 0, vhost, get_metadata, [V1]),
        ok = rpc(Config, 0, rabbit_vhost, update_metadata,
                 [VHost, #{default_queue_type => null}, <<"test-user">>]),
        V2 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := StoredDQT} = rpc(Config, 0, vhost, get_metadata, [V2]),
        <<"classic">> = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.

update_metadata_sanitizes_nil(Config) ->
    VHost = <<"update-metadata-nil-dqt-test">>,
    try
        ok = rpc(Config, 0, rabbit_vhost, put_vhost,
                 [VHost, <<"test">>, [], <<"classic">>, false, <<"test-user">>]),
        V1 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := <<"classic">>} = rpc(Config, 0, vhost, get_metadata, [V1]),
        ok = rpc(Config, 0, rabbit_vhost, update_metadata,
                 [VHost, #{default_queue_type => nil}, <<"test-user">>]),
        V2 = rpc(Config, 0, rabbit_vhost, lookup, [VHost]),
        #{default_queue_type := StoredDQT} = rpc(Config, 0, vhost, get_metadata, [V2]),
        <<"classic">> = StoredDQT
    after
        rpc(Config, 0, rabbit_vhost, delete, [VHost, <<"test-user">>])
    end,
    ok.
