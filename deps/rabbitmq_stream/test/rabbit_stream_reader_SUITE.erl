%% The contents of this file are subject to the Mozilla Public License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stream_reader_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_stream/src/rabbit_stream_reader.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-import(rabbit_stream_reader, [ensure_token_expiry_timer/2]).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

%% replicate eunit like test resolution
all_tests() ->
    [F
     || {F, _} <- ?MODULE:module_info(functions),
        re:run(atom_to_list(F), "_test$") /= nomatch].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

ensure_token_expiry_timer_test(_) ->
    ok = meck:new(rabbit_access_control),

    meck:expect(rabbit_access_control, permission_cache_can_expire, fun (_) -> false end),
    {_, #stream_connection{token_expiry_timer = TR1}} = ensure_token_expiry_timer(#user{}, #stream_connection{}),
    ?assertEqual(undefined, TR1),

    meck:expect(rabbit_access_control, permission_cache_can_expire, fun (_) -> true end),
    meck:expect(rabbit_access_control, expiry_timestamp, fun (_) -> never end),
    {_, #stream_connection{token_expiry_timer = TR2}} = ensure_token_expiry_timer(#user{}, #stream_connection{}),
    ?assertEqual(undefined, TR2),

    Now = os:system_time(second),
    meck:expect(rabbit_access_control, expiry_timestamp, fun (_) -> Now + 60 end),
    {_, #stream_connection{token_expiry_timer = TR3}} = ensure_token_expiry_timer(#user{}, #stream_connection{}),
    Cancel3 = erlang:cancel_timer(TR3, [{async, false}, {info, true}]),
    ?assert(is_integer(Cancel3)),

    meck:expect(rabbit_access_control, expiry_timestamp, fun (_) -> Now - 60 end),
    {_, #stream_connection{token_expiry_timer = TR4}} = ensure_token_expiry_timer(#user{}, #stream_connection{}),
    ?assertEqual(undefined, TR4),

    DummyTRef = erlang:send_after(1_000 * 1_000, self(), dummy),
    meck:expect(rabbit_access_control, permission_cache_can_expire, fun (_) -> false end),
    {Cancel5, #stream_connection{token_expiry_timer = TR5}} = ensure_token_expiry_timer(#user{},
                                                                                        #stream_connection{token_expiry_timer = DummyTRef}),
    ?assertEqual(undefined, TR5),
    ?assert(is_integer(Cancel5)),

    ok.

evaluate_state_after_secret_update_test(_) ->
    Mod = rabbit_stream_reader,
    meck:new(Mod, [passthrough]),

    ModUtils = rabbit_stream_utils,
    meck:new(ModUtils, [passthrough]),
    CheckFun = fun(N) ->
                       case binary:match(N, <<"ok_">>) of
                           nomatch ->
                               error;
                           _ ->
                               ok
                       end
               end,
    meck:expect(ModUtils, check_write_permitted, fun(#resource{name = N}, _) -> CheckFun(N) end),
    meck:expect(ModUtils, check_read_permitted, fun(#resource{name = N}, _, _) -> CheckFun(N) end),

    ModAccess = rabbit_access_control,
    meck:new(ModAccess),
    meck:expect(ModAccess, permission_cache_can_expire, 1, false),

    meck:new(rabbit_stream_metrics, [stub_all]),
    meck:new(rabbit_global_counters, [stub_all]),

    ModTransport = dummy_transport,
    meck:new(ModTransport, [non_strict]),
    meck:expect(ModTransport, send, 2, ok),

    ModLog = osiris_log,
    meck:new(ModLog),
    meck:expect(ModLog, init, 1, ok),
    put(close_log_count, 0),
    meck:expect(ModLog, close, fun(_) -> put(close_log_count, get(close_log_count) + 1) end),

    ModCore = rabbit_stream_core,
    meck:new(ModCore),
    put(metadata_update, []),
    meck:expect(ModCore, frame, fun(Cmd) -> put(metadata_update, [Cmd | get(metadata_update)]) end),

    Publishers = #{0 => #publisher{stream = <<"ok_publish">>},
                   1 => #publisher{stream = <<"ko_publish">>},
                   2 => #publisher{stream = <<"ok_publish_consume">>},
                   3 => #publisher{stream = <<"ko_publish_consume">>}},
    Subscriptions = #{<<"ok_consume">> => [0],
                      <<"ko_consume">> => [1],
                      <<"ok_publish_consume">> => [2],
                      <<"ko_publish_consume">> => [3]},
    Consumers = #{0 => consumer(<<"ok_consume">>),
                  1 => consumer(<<"ko_consume">>),
                  2 => consumer(<<"ok_publish_consume">>),
                  3 => consumer(<<"ko_publish_consume">>)},

    {C1, S1} = Mod:evaluate_state_after_secret_update(ModTransport, #user{},
                                                      #stream_connection{publishers = Publishers,
                                                                         stream_subscriptions = Subscriptions,
                                                                         user = #user{}},
                                                      #stream_connection_state{consumers = Consumers}),

    meck:validate(ModLog),
    ?assertEqual(2, get(close_log_count)),
    erase(close_log_count),

    Cmds = get(metadata_update),
    ?assertEqual(3, length(Cmds)),
    ?assert(lists:member({metadata_update, <<"ko_publish">>, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE}, Cmds)),
    ?assert(lists:member({metadata_update, <<"ko_consume">>, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE}, Cmds)),
    ?assert(lists:member({metadata_update, <<"ko_publish_consume">>, ?RESPONSE_CODE_STREAM_NOT_AVAILABLE}, Cmds)),
    erase(metadata_update),

    #stream_connection{token_expiry_timer = TRef1,
                       publishers = Pubs1,
                       stream_subscriptions = Subs1} = C1,
    ?assertEqual(undefined, TRef1), %% no expiry set in the mock
    ?assertEqual(2, maps:size(Pubs1)),
    ?assertEqual(#publisher{stream = <<"ok_publish">>}, maps:get(0, Pubs1)),
    ?assertEqual(#publisher{stream = <<"ok_publish_consume">>}, maps:get(2, Pubs1)),

    #stream_connection_state{consumers = Cons1} = S1,
    ?assertEqual([0], maps:get(<<"ok_consume">>, Subs1)),
    ?assertEqual([2], maps:get(<<"ok_publish_consume">>, Subs1)),
    ?assertEqual(consumer(<<"ok_consume">>), maps:get(0, Cons1)),
    ?assertEqual(consumer(<<"ok_publish_consume">>), maps:get(2, Cons1)),

    %% making sure the token expiry timer is set if the token expires
    meck:expect(ModAccess, permission_cache_can_expire, 1, true),
    Now = os:system_time(second),
    meck:expect(rabbit_access_control, expiry_timestamp, fun (_) -> Now + 60 end),
    {C2, _} = Mod:evaluate_state_after_secret_update(ModTransport, #user{},
                                                     #stream_connection{user = #user{}},
                                                     #stream_connection_state{}),
    #stream_connection{token_expiry_timer = TRef2} = C2,
    Cancel2 = erlang:cancel_timer(TRef2, [{async, false}, {info, true}]),
    ?assert(is_integer(Cancel2)),
    ok.

clean_subscriptions_should_remove_only_affected_subscriptions_test(_) ->
    Mod = rabbit_stream_reader,
    meck:new(Mod, [passthrough]),
    meck:new(rabbit_stream_metrics, [stub_all]),
    meck:new(rabbit_stream_sac_coordinator, [stub_all]),

    S = <<"s1">>,
    Pid1 = new_process(),
    Pid2 = new_process(),
    StreamSubs = #{S => [0, 1]},
    Consumers = #{0 => consumer(S, Pid1),
                  1 => consumer(S, Pid2)},

    C0 = #stream_connection{stream_subscriptions = StreamSubs,
                            user = #user{}},
    S0 = #stream_connection_state{consumers = Consumers},
    {Cleaned1, C1, S1} = Mod:clean_subscriptions(Pid1, S, C0, S0),
    ?assert(Cleaned1),
    ?assertEqual(#{S => [1]},
                 C1#stream_connection.stream_subscriptions),
    ?assertEqual(#{1 => consumer(S, Pid2)},
                 S1#stream_connection_state.consumers),

    {Cleaned2, C2, S2} = Mod:clean_subscriptions(Pid2, S, C1, S1),
    ?assert(Cleaned2),
    ?assertEqual(#{}, C2#stream_connection.stream_subscriptions),
    ?assertEqual(#{}, S2#stream_connection_state.consumers),

    ok.

consumer(S, Pid) ->
    #consumer{configuration = #consumer_configuration{stream = S,
                                                      member_pid = Pid}}.

consumer(S) ->
    #consumer{configuration = #consumer_configuration{stream = S},
              log = osiris_log:init(#{})}.

new_process() ->
    spawn(node(), fun() -> ok end).

