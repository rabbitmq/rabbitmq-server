%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          amqp_connection_refusal,
          configurable_server_properties,
          confirms,
          credit_flow_settings,
          dynamic_mirroring,
          gen_server2_with_state,
          list_operations_timeout_pass,
          mcall,
          {password_hashing, [], [
              password_hashing,
              change_password
            ]},
          {policy_validation, [parallel, {repeat, 20}], [
              ha_policy_validation,
              policy_validation,
              policy_opts_validation,
              queue_master_location_policy_validation,
              queue_modes_policy_validation,
              vhost_removed_while_updating_policy
            ]},
          runtime_parameters,
          set_disk_free_limit_command,
          topic_matching,
          user_management
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            ClusterSize = 2,
            Config1 = rabbit_ct_helpers:set_config(Config, [
                {rmq_nodename_suffix, Group},
                {rmq_nodes_count, ClusterSize}
              ]),
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps() ++ [
                fun setup_file_handle_cache/1
              ]);
        false ->
            rabbit_ct_helpers:run_steps(Config, [])
    end.

setup_file_handle_cache(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_file_handle_cache1, []),
    Config.

setup_file_handle_cache1() ->
    %% FIXME: Why are we doing this?
    application:set_env(rabbit, file_handles_high_watermark, 10),
    ok = file_handle_cache:set_limit(10),
    ok.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

msg_id_bin(X) ->
    erlang:md5(term_to_binary(X)).

on_disk_capture() ->
    receive
        {await, MsgIds, Pid} -> on_disk_capture([], MsgIds, Pid);
        stop                 -> done
    end.

on_disk_capture([_|_], _Awaiting, Pid) ->
    Pid ! {self(), surplus};
on_disk_capture(OnDisk, Awaiting, Pid) ->
    receive
        {on_disk, MsgIdsS} ->
            MsgIds = gb_sets:to_list(MsgIdsS),
            on_disk_capture(OnDisk ++ (MsgIds -- Awaiting), Awaiting -- MsgIds,
                            Pid);
        stop ->
            done
    after (case Awaiting of [] -> 200; _ -> ?TIMEOUT end) ->
            case Awaiting of
                [] -> Pid ! {self(), arrived}, on_disk_capture();
                _  -> Pid ! {self(), timeout}
            end
    end.

on_disk_await(Pid, MsgIds) when is_list(MsgIds) ->
    Pid ! {await, MsgIds, self()},
    receive
        {Pid, arrived} -> ok;
        {Pid, Error}   -> Error
    end.

on_disk_stop(Pid) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! stop,
    receive {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end.

queue_name(Config, Name) ->
    Name1 = rabbit_ct_helpers:config_to_testcase_name(Config, Name),
    queue_name(Name1).

queue_name(Name) ->
    rabbit_misc:r(<<"/">>, queue, Name).

test_queue() ->
    queue_name(<<"test">>).

verify_read_with_published(_Delivered, _Persistent, [], _) ->
    ok;
verify_read_with_published(Delivered, Persistent,
                           [{MsgId, SeqId, _Props, Persistent, Delivered}|Read],
                           [{SeqId, MsgId}|Published]) ->
    verify_read_with_published(Delivered, Persistent, Read, Published);
verify_read_with_published(_Delivered, _Persistent, _Read, _Published) ->
    ko.

nop(_) -> ok.
nop(_, _) -> ok.

variable_queue_init(Q, Recover) ->
    rabbit_variable_queue:init(
      Q, case Recover of
             true  -> non_clean_shutdown;
             false -> new
         end, fun nop/2, fun nop/2, fun nop/1, fun nop/1).

publish_and_confirm(Q, Payload, Count) ->
    Seqs = lists:seq(1, Count),
    [begin
         Msg = rabbit_basic:message(rabbit_misc:r(<<>>, exchange, <<>>),
                                    <<>>, #'P_basic'{delivery_mode = 2},
                                    Payload),
         Delivery = #delivery{mandatory = false, sender = self(),
                              confirm = true, message = Msg, msg_seq_no = Seq,
                              flow = noflow},
         _QPids = rabbit_amqqueue:deliver([Q], Delivery)
     end || Seq <- Seqs],
    wait_for_confirms(gb_sets:from_list(Seqs)).

wait_for_confirms(Unconfirmed) ->
    case gb_sets:is_empty(Unconfirmed) of
        true  -> ok;
        false -> receive {'$gen_cast', {confirm, Confirmed, _}} ->
                         wait_for_confirms(
                           rabbit_misc:gb_sets_difference(
                             Unconfirmed, gb_sets:from_list(Confirmed)))
                 after ?TIMEOUT -> exit(timeout_waiting_for_confirm)
                 end
    end.

test_amqqueue(Durable) ->
    (rabbit_amqqueue:pseudo_queue(test_queue(), self()))
        #amqqueue { durable = Durable }.

assert_prop(List, Prop, Value) ->
    case proplists:get_value(Prop, List)of
        Value -> ok;
        _     -> {exit, Prop, exp, Value, List}
    end.

assert_props(List, PropVals) ->
    [assert_prop(List, Prop, Value) || {Prop, Value} <- PropVals].

variable_queue_wait_for_shuffling_end(VQ) ->
    case credit_flow:blocked() of
        false -> VQ;
        true  -> receive
                     {bump_credit, Msg} ->
                         credit_flow:handle_bump_msg(Msg),
                         variable_queue_wait_for_shuffling_end(
                           rabbit_variable_queue:resume(VQ))
                 end
    end.

msg2int(#basic_message{content = #content{ payload_fragments_rev = P}}) ->
    binary_to_term(list_to_binary(lists:reverse(P))).

ack_subset(AckSeqs, Interval, Rem) ->
    lists:filter(fun ({_Ack, N}) -> (N + Rem) rem Interval == 0 end, AckSeqs).

requeue_one_by_one(Acks, VQ) ->
    lists:foldl(fun (AckTag, VQN) ->
                        {_MsgId, VQM} = rabbit_variable_queue:requeue(
                                          [AckTag], VQN),
                        VQM
                end, VQ, Acks).

%% ---------------------------------------------------------------------------
%% Credit flow.
%% ---------------------------------------------------------------------------

credit_flow_settings(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, credit_flow_settings1, [Config]).

credit_flow_settings1(_Config) ->
    passed = test_proc(400, 200, {400, 200}),
    passed = test_proc(600, 300),
    passed.

test_proc(InitialCredit, MoreCreditAfter) ->
    test_proc(InitialCredit, MoreCreditAfter, {InitialCredit, MoreCreditAfter}).
test_proc(InitialCredit, MoreCreditAfter, Settings) ->
    Pid = spawn(?MODULE, dummy, [Settings]),
    Pid ! {credit, self()},
    {InitialCredit, MoreCreditAfter} =
        receive
            {credit, Val} -> Val
        end,
    passed.

dummy(Settings) ->
    credit_flow:send(self()),
    receive
        {credit, From} ->
            From ! {credit, Settings};
        _      ->
            dummy(Settings)
    end.

%% -------------------------------------------------------------------
%% dynamic_mirroring.
%% -------------------------------------------------------------------

dynamic_mirroring(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, dynamic_mirroring1, [Config]).

dynamic_mirroring1(_Config) ->
    %% Just unit tests of the node selection logic, see multi node
    %% tests for the rest...
    Test = fun ({NewM, NewSs, ExtraSs}, Policy, Params,
                {MNode, SNodes, SSNodes}, All) ->
                   {ok, M} = rabbit_mirror_queue_misc:module(Policy),
                   {NewM, NewSs0} = M:suggested_queue_nodes(
                                      Params, MNode, SNodes, SSNodes, All),
                   NewSs1 = lists:sort(NewSs0),
                   case dm_list_match(NewSs, NewSs1, ExtraSs) of
                       ok    -> ok;
                       error -> exit({no_match, NewSs, NewSs1, ExtraSs})
                   end
           end,

    Test({a,[b,c],0},<<"all">>,'_',{a,[],   []},   [a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[b,c],[b,c]},[a,b,c]),
    Test({a,[b,c],0},<<"all">>,'_',{a,[d],  [d]},  [a,b,c]),

    N = fun (Atoms) -> [list_to_binary(atom_to_list(A)) || A <- Atoms] end,

    %% Add a node
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[b],[b]},[a,b,c,d]),
    Test({b,[a,c],0},<<"nodes">>,N([a,b,c]),{b,[a],[a]},[a,b,c,d]),
    %% Add two nodes and drop one
    Test({a,[b,c],0},<<"nodes">>,N([a,b,c]),{a,[d],[d]},[a,b,c,d]),
    %% Don't try to include nodes that are not running
    Test({a,[b],  0},<<"nodes">>,N([a,b,f]),{a,[b],[b]},[a,b,c,d]),
    %% If we can't find any of the nodes listed then just keep the master
    Test({a,[],   0},<<"nodes">>,N([f,g,h]),{a,[b],[b]},[a,b,c,d]),
    %% And once that's happened, still keep the master even when not listed,
    %% if nothing is synced
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[], []}, [a,b,c,d]),
    Test({a,[b,c],0},<<"nodes">>,N([b,c]),  {a,[b],[]}, [a,b,c,d]),
    %% But if something is synced we can lose the master - but make
    %% sure we pick the new master from the nodes which are synced!
    Test({b,[c],  0},<<"nodes">>,N([b,c]),  {a,[b],[b]},[a,b,c,d]),
    Test({b,[c],  0},<<"nodes">>,N([c,b]),  {a,[b],[b]},[a,b,c,d]),

    Test({a,[],   1},<<"exactly">>,2,{a,[],   []},   [a,b,c,d]),
    Test({a,[],   2},<<"exactly">>,3,{a,[],   []},   [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  1},<<"exactly">>,3,{a,[c],  [c]},  [a,b,c,d]),
    Test({a,[c],  0},<<"exactly">>,2,{a,[c,d],[c,d]},[a,b,c,d]),
    Test({a,[c,d],0},<<"exactly">>,3,{a,[c,d],[c,d]},[a,b,c,d]),

    passed.

%% Does the first list match the second where the second is required
%% to have exactly Extra superfluous items?
dm_list_match([],     [],      0)     -> ok;
dm_list_match(_,      [],     _Extra) -> error;
dm_list_match([H|T1], [H |T2], Extra) -> dm_list_match(T1, T2, Extra);
dm_list_match(L1,     [_H|T2], Extra) -> dm_list_match(L1, T2, Extra - 1).

override_group_leader() ->
    %% Override group leader, otherwise SASL fake events are ignored by
    %% the error_logger local to RabbitMQ.
    {group_leader, Leader} = erlang:process_info(whereis(rabbit), group_leader),
    erlang:group_leader(Leader, self()).

empty_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo} -> FInfo#file_info.size == 0;
         Error       -> Error
     end || File <- Files].

non_empty_files(Files) ->
    [case EmptyFile of
         {error, Reason} -> {error, Reason};
         _               -> not(EmptyFile)
     end || EmptyFile <- empty_files(Files)].

test_logs_working(MainLogFile, SaslLogFile) ->
    ok = rabbit_log:error("Log a test message~n"),
    ok = error_logger:error_report(crash_report, [fake_crash_report, ?MODULE]),
    %% give the error loggers some time to catch up
    timer:sleep(100),
    [true, true] = non_empty_files([MainLogFile, SaslLogFile]),
    ok.

set_permissions(Path, Mode) ->
    case file:read_file_info(Path) of
        {ok, FInfo} -> file:write_file_info(
                         Path,
                         FInfo#file_info{mode=Mode});
        Error       -> Error
    end.

clean_logs(Files, Suffix) ->
    [begin
         ok = delete_file(File),
         ok = delete_file([File, Suffix])
     end || File <- Files],
    ok.

assert_ram_node() ->
    case rabbit_mnesia:node_type() of
        disc -> exit('not_ram_node');
        ram  -> ok
    end.

assert_disc_node() ->
    case rabbit_mnesia:node_type() of
        disc -> ok;
        ram  -> exit('not_disc_node')
    end.

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=8#444}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

%% sasl_report_file_h returns [] during terminate
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
%%
%% error_logger_file_h returns ok since OTP 18.1
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
delete_log_handlers(Handlers) ->
    [ok_or_empty_list(error_logger:delete_report_handler(Handler))
     || Handler <- Handlers],
    ok.

ok_or_empty_list([]) ->
    [];
ok_or_empty_list(ok) ->
    ok.

%% ---------------------------------------------------------------------------
%% Password hashing.
%% ---------------------------------------------------------------------------

password_hashing(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, password_hashing1, [Config]).

password_hashing1(_Config) ->
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    = rabbit_password:hashing_mod(),
    application:set_env(rabbit, password_hashing_module,
                        rabbit_password_hashing_sha256),
    rabbit_password_hashing_sha256 = rabbit_password:hashing_mod(),

    rabbit_password_hashing_sha256 =
        rabbit_password:hashing_mod(rabbit_password_hashing_sha256),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(rabbit_password_hashing_md5),
    rabbit_password_hashing_md5    =
        rabbit_password:hashing_mod(undefined),

    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{}),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = undefined
            }),
    rabbit_password_hashing_md5    =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_md5
            }),

    rabbit_password_hashing_sha256 =
        rabbit_auth_backend_internal:hashing_module_for_user(
          #internal_user{
             hashing_algorithm = rabbit_password_hashing_sha256
            }),

    passed.

change_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, change_password1, [Config]).

change_password1(_Config) ->
    UserName = <<"test_user">>,
    Password = <<"test_password">>,
    case rabbit_auth_backend_internal:lookup_user(UserName) of
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.

%% -------------------------------------------------------------------
%% rabbitmqctl.
%% -------------------------------------------------------------------

list_operations_timeout_pass(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, list_operations_timeout_pass1, [Config]).

list_operations_timeout_pass1(Config) ->
    %% create a few things so there is some useful information to list
    {_Writer1, Limiter1, Ch1} = rabbit_ct_broker_helpers:test_channel(),
    {_Writer2, Limiter2, Ch2} = rabbit_ct_broker_helpers:test_channel(),

    [Q, Q2] = [Queue || Name <- [<<"list_operations_timeout_pass-q1">>,
                                 <<"list_operations_timeout_pass-q2">>],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], none)]],

    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch1, Limiter1, false, 0, <<"ctag1">>, true, [],
           undefined),
    ok = rabbit_amqqueue:basic_consume(
           Q2, true, Ch2, Limiter2, false, 0, <<"ctag2">>, true, [],
           undefined),

    %% list users
    ok = control_action(add_user,
      ["list_operations_timeout_pass-user",
       "list_operations_timeout_pass-password"]),
    {error, {user_already_exists, _}} =
        control_action(add_user,
          ["list_operations_timeout_pass-user",
           "list_operations_timeout_pass-password"]),
    ok = control_action_t(list_users, [], ?TIMEOUT_LIST_OPS_PASS),

    %% list parameters
    ok = dummy_runtime_parameters:register(),
    ok = control_action(set_parameter, ["test", "good", "123"]),
    ok = control_action_t(list_parameters, [], ?TIMEOUT_LIST_OPS_PASS),
    ok = control_action(clear_parameter, ["test", "good"]),
    dummy_runtime_parameters:unregister(),

    %% list vhosts
    ok = control_action(add_vhost, ["/list_operations_timeout_pass-vhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost, ["/list_operations_timeout_pass-vhost"]),
    ok = control_action_t(list_vhosts, [], ?TIMEOUT_LIST_OPS_PASS),

    %% list permissions
    ok = control_action(set_permissions,
      ["list_operations_timeout_pass-user", ".*", ".*", ".*"],
      [{"-p", "/list_operations_timeout_pass-vhost"}]),
    ok = control_action_t(list_permissions, [],
      [{"-p", "/list_operations_timeout_pass-vhost"}],
      ?TIMEOUT_LIST_OPS_PASS),

    %% list user permissions
    ok = control_action_t(list_user_permissions,
      ["list_operations_timeout_pass-user"],
      ?TIMEOUT_LIST_OPS_PASS),

    %% list policies
    ok = control_action_opts(
      ["set_policy", "list_operations_timeout_pass-policy", ".*",
       "{\"ha-mode\":\"all\"}"]),
    ok = control_action_t(list_policies, [], ?TIMEOUT_LIST_OPS_PASS),
    ok = control_action(clear_policy, ["list_operations_timeout_pass-policy"]),

    %% list queues
    ok = info_action_t(list_queues,
      rabbit_amqqueue:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list exchanges
    ok = info_action_t(list_exchanges,
      rabbit_exchange:info_keys(), true,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list bindings
    ok = info_action_t(list_bindings,
      rabbit_binding:info_keys(), true,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list connections
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, C1} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C1, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C1, 3, 100),

    {ok, C2} = gen_tcp:connect(H, P, [binary, {active, false}]),
    gen_tcp:send(C2, <<"AMQP", 0, 0, 9, 1>>),
    {ok, <<1,0,0>>} = gen_tcp:recv(C2, 3, 100),

    ok = info_action_t(
      list_connections, rabbit_networking:connection_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list consumers
    ok = info_action_t(
      list_consumers, rabbit_amqqueue:consumer_info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% list channels
    ok = info_action_t(
      list_channels, rabbit_channel:info_keys(), false,
      ?TIMEOUT_LIST_OPS_PASS),

    %% do some cleaning up
    ok = control_action(delete_user, ["list_operations_timeout_pass-user"]),
    {error, {no_such_user, _}} =
        control_action(delete_user, ["list_operations_timeout_pass-user"]),

    ok = control_action(delete_vhost, ["/list_operations_timeout_pass-vhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/list_operations_timeout_pass-vhost"]),

    %% close_connection
    Conns = rabbit_ct_broker_helpers:get_connection_pids([C1, C2]),
    [ok, ok] = [ok = control_action(
        close_connection, [rabbit_misc:pid_to_string(ConnPid), "go away"])
     || ConnPid <- Conns],

    %% cleanup queues
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    [begin
         unlink(Chan),
         ok = rabbit_channel:shutdown(Chan)
     end || Chan <- [Ch1, Ch2]],
    passed.

user_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, user_management1, [Config]).

user_management1(_Config) ->

    %% lots if stuff that should fail
    {error, {no_such_user, _}} =
        control_action(delete_user,
          ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(change_password,
          ["user_management-user", "user_management-password"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost,
          ["/user_management-vhost"]),
    {error, {no_such_user, _}} =
        control_action(set_permissions,
          ["user_management-user", ".*", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(clear_permissions,
          ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(list_user_permissions,
          ["user_management-user"]),
    {error, {no_such_vhost, _}} =
        control_action(list_permissions, [],
          [{"-p", "/user_management-vhost"}]),
    {error, {invalid_regexp, _, _}} =
        control_action(set_permissions,
          ["guest", "+foo", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(set_user_tags,
          ["user_management-user", "bar"]),

    %% user creation
    ok = control_action(add_user,
      ["user_management-user", "user_management-password"]),
    {error, {user_already_exists, _}} =
        control_action(add_user,
          ["user_management-user", "user_management-password"]),
    ok = control_action(clear_password,
      ["user_management-user"]),
    ok = control_action(change_password,
      ["user_management-user", "user_management-newpassword"]),

    TestTags = fun (Tags) ->
                       Args = ["user_management-user" | [atom_to_list(T) || T <- Tags]],
                       ok = control_action(set_user_tags, Args),
                       {ok, #internal_user{tags = Tags}} =
                           rabbit_auth_backend_internal:lookup_user(
                             <<"user_management-user">>),
                       ok = control_action(list_users, [])
               end,
    TestTags([foo, bar, baz]),
    TestTags([administrator]),
    TestTags([]),

    %% user authentication
    ok = control_action(authenticate_user,
      ["user_management-user", "user_management-newpassword"]),
    {refused, _User, _Format, _Params} =
        control_action(authenticate_user,
          ["user_management-user", "user_management-password"]),

    %% vhost creation
    ok = control_action(add_vhost,
      ["/user_management-vhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost,
          ["/user_management-vhost"]),
    ok = control_action(list_vhosts, []),

    %% user/vhost mapping
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_permissions, [],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_permissions, [],
      [{"-p", "/user_management-vhost"}]),
    ok = control_action(list_user_permissions,
      ["user_management-user"]),

    %% user/vhost unmapping
    ok = control_action(clear_permissions,
      ["user_management-user"], [{"-p", "/user_management-vhost"}]),
    ok = control_action(clear_permissions,
      ["user_management-user"], [{"-p", "/user_management-vhost"}]),

    %% vhost deletion
    ok = control_action(delete_vhost,
      ["/user_management-vhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost,
          ["/user_management-vhost"]),

    %% deleting a populated vhost
    ok = control_action(add_vhost,
      ["/user_management-vhost"]),
    ok = control_action(set_permissions,
      ["user_management-user", ".*", ".*", ".*"],
      [{"-p", "/user_management-vhost"}]),
    {new, _} = rabbit_amqqueue:declare(
                 rabbit_misc:r(<<"/user_management-vhost">>, queue,
                               <<"user_management-vhost-queue">>),
                 true, false, [], none),
    ok = control_action(delete_vhost,
      ["/user_management-vhost"]),

    %% user deletion
    ok = control_action(delete_user,
      ["user_management-user"]),
    {error, {no_such_user, _}} =
        control_action(delete_user,
          ["user_management-user"]),

    passed.

runtime_parameters(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, runtime_parameters1, [Config]).

runtime_parameters1(_Config) ->
    dummy_runtime_parameters:register(),
    Good = fun(L) -> ok                = control_action(set_parameter, L) end,
    Bad  = fun(L) -> {error_string, _} = control_action(set_parameter, L) end,

    %% Acceptable for bijection
    Good(["test", "good", "\"ignore\""]),
    Good(["test", "good", "123"]),
    Good(["test", "good", "true"]),
    Good(["test", "good", "false"]),
    Good(["test", "good", "null"]),
    Good(["test", "good", "{\"key\": \"value\"}"]),

    %% Invalid json
    Bad(["test", "good", "atom"]),
    Bad(["test", "good", "{\"foo\": \"bar\""]),
    Bad(["test", "good", "{foo: \"bar\"}"]),

    %% Test actual validation hook
    Good(["test", "maybe", "\"good\""]),
    Bad(["test", "maybe", "\"bad\""]),
    Good(["test", "admin", "\"ignore\""]), %% ctl means 'user' -> none

    ok = control_action(list_parameters, []),

    ok = control_action(clear_parameter, ["test", "good"]),
    ok = control_action(clear_parameter, ["test", "maybe"]),
    ok = control_action(clear_parameter, ["test", "admin"]),
    {error_string, _} =
        control_action(clear_parameter, ["test", "neverexisted"]),

    %% We can delete for a component that no longer exists
    Good(["test", "good", "\"ignore\""]),
    dummy_runtime_parameters:unregister(),
    ok = control_action(clear_parameter, ["test", "good"]),
    passed.

policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, policy_validation1, [Config]).

policy_validation1(_Config) ->
    PolicyName = "runtime_parameters-policy",
    dummy_runtime_parameters:register_policy_validator(),
    SetPol = fun (Key, Val) ->
                     control_action_opts(
                       ["set_policy", PolicyName, ".*",
                        rabbit_misc:format("{\"~s\":~p}", [Key, Val])])
             end,
    OK     = fun (Key, Val) ->
                 ok = SetPol(Key, Val),
                 true = does_policy_exist(PolicyName,
                   [{definition, [{list_to_binary(Key), Val}]}])
             end,

    OK("testeven", []),
    OK("testeven", [1, 2]),
    OK("testeven", [1, 2, 3, 4]),
    OK("testpos",  [2, 5, 5678]),

    {error_string, _} = SetPol("testpos",  [-1, 0, 1]),
    {error_string, _} = SetPol("testeven", [ 1, 2, 3]),

    ok = control_action(clear_policy, [PolicyName]),
    dummy_runtime_parameters:unregister_policy_validator(),
    passed.

policy_opts_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, policy_opts_validation1, [Config]).

policy_opts_validation1(_Config) ->
    PolicyName = "policy_opts_validation-policy",
    Set  = fun (Extra) -> control_action_opts(
                            ["set_policy", PolicyName,
                             ".*", "{\"ha-mode\":\"all\"}"
                             | Extra]) end,
    OK   = fun (Extra, Props) ->
               ok = Set(Extra),
               true = does_policy_exist(PolicyName, Props)
           end,
    Fail = fun (Extra) ->
            case Set(Extra) of
                {error_string, _} -> ok;
                no_command when Extra =:= ["--priority"] -> ok;
                no_command when Extra =:= ["--apply-to"] -> ok;
                {'EXIT',
                 {function_clause,
                  [{rabbit_control_main,action, _, _} | _]}}
                when Extra =:= ["--offline"] -> ok
          end
    end,

    OK  ([], [{priority, 0}, {'apply-to', <<"all">>}]),

    OK  (["--priority", "0"], [{priority, 0}]),
    OK  (["--priority", "3"], [{priority, 3}]),
    Fail(["--priority", "banana"]),
    Fail(["--priority"]),

    OK  (["--apply-to", "all"],    [{'apply-to', <<"all">>}]),
    OK  (["--apply-to", "queues"], [{'apply-to', <<"queues">>}]),
    Fail(["--apply-to", "bananas"]),
    Fail(["--apply-to"]),

    OK  (["--priority", "3",      "--apply-to", "queues"], [{priority, 3}, {'apply-to', <<"queues">>}]),
    Fail(["--priority", "banana", "--apply-to", "queues"]),
    Fail(["--priority", "3",      "--apply-to", "bananas"]),

    Fail(["--offline"]),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

ha_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, ha_policy_validation1, [Config]).

ha_policy_validation1(_Config) ->
    PolicyName = "ha_policy_validation-policy",
    Set  = fun (JSON) -> control_action_opts(
                           ["set_policy", PolicyName,
                            ".*", JSON]) end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"ha-mode\":\"all\"}", [{<<"ha-mode">>, <<"all">>}]),
    Fail("{\"ha-mode\":\"made_up\"}"),

    Fail("{\"ha-mode\":\"nodes\"}"),
    Fail("{\"ha-mode\":\"nodes\",\"ha-params\":2}"),
    Fail("{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",2]}"),
    OK  ("{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",\"b\"]}",
      [{<<"ha-mode">>, <<"nodes">>}, {<<"ha-params">>, [<<"a">>, <<"b">>]}]),
    Fail("{\"ha-params\":[\"a\",\"b\"]}"),

    Fail("{\"ha-mode\":\"exactly\"}"),
    Fail("{\"ha-mode\":\"exactly\",\"ha-params\":[\"a\",\"b\"]}"),
    OK  ("{\"ha-mode\":\"exactly\",\"ha-params\":2}",
      [{<<"ha-mode">>, <<"exactly">>}, {<<"ha-params">>, 2}]),
    Fail("{\"ha-params\":2}"),

    OK  ("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"manual\"}",
      [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"manual">>}]),
    OK  ("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"automatic\"}",
      [{<<"ha-mode">>, <<"all">>}, {<<"ha-sync-mode">>, <<"automatic">>}]),
    Fail("{\"ha-mode\":\"all\",\"ha-sync-mode\":\"made_up\"}"),
    Fail("{\"ha-sync-mode\":\"manual\"}"),
    Fail("{\"ha-sync-mode\":\"automatic\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

queue_master_location_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, queue_master_location_policy_validation1, [Config]).

queue_master_location_policy_validation1(_Config) ->
    PolicyName = "queue_master_location_policy_validation-policy",
    Set  = fun (JSON) ->
                   control_action_opts(
                     ["set_policy", PolicyName, ".*", JSON])
           end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"queue-master-locator\":\"min-masters\"}",
      [{<<"queue-master-locator">>, <<"min-masters">>}]),
    OK  ("{\"queue-master-locator\":\"client-local\"}",
      [{<<"queue-master-locator">>, <<"client-local">>}]),
    OK  ("{\"queue-master-locator\":\"random\"}",
      [{<<"queue-master-locator">>, <<"random">>}]),
    Fail("{\"queue-master-locator\":\"made_up\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

queue_modes_policy_validation(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, queue_modes_policy_validation1, [Config]).

queue_modes_policy_validation1(_Config) ->
    PolicyName = "queue_modes_policy_validation-policy",
    Set  = fun (JSON) ->
                   control_action_opts(
                     ["set_policy", PolicyName, ".*", JSON])
           end,
    OK   = fun (JSON, Def) ->
               ok = Set(JSON),
               true = does_policy_exist(PolicyName, [{definition, Def}])
           end,
    Fail = fun (JSON) -> {error_string, _} = Set(JSON) end,

    OK  ("{\"queue-mode\":\"lazy\"}",
      [{<<"queue-mode">>, <<"lazy">>}]),
    OK  ("{\"queue-mode\":\"default\"}",
      [{<<"queue-mode">>, <<"default">>}]),
    Fail("{\"queue-mode\":\"wrong\"}"),

    ok = control_action(clear_policy, [PolicyName]),
    passed.

vhost_removed_while_updating_policy(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, vhost_removed_while_updating_policy1, [Config]).

vhost_removed_while_updating_policy1(_Config) ->
    VHost = "/vhost_removed_while_updating_policy-vhost",
    PolicyName = "vhost_removed_while_updating_policy-policy",

    ok = control_action(add_vhost, [VHost]),
    ok = control_action_opts(
      ["set_policy", "-p", VHost, PolicyName, ".*", "{\"ha-mode\":\"all\"}"]),
    true = does_policy_exist(PolicyName, []),

    %% Removing the vhost triggers the deletion of the policy. Once
    %% the policy and the vhost are actually removed, RabbitMQ calls
    %% update_policies() which lists policies on the given vhost. This
    %% obviously fails because the vhost is gone, but the call should
    %% still succeed.
    ok = control_action(delete_vhost, [VHost]),
    false = does_policy_exist(PolicyName, []),

    passed.

does_policy_exist(PolicyName, Props) ->
    PolicyNameBin = list_to_binary(PolicyName),
    Policies = lists:filter(
      fun(Policy) ->
          lists:member({name, PolicyNameBin}, Policy)
      end, rabbit_policy:list()),
    case Policies of
        [Policy] -> check_policy_props(Policy, Props);
        []       -> false;
        _        -> false
    end.

check_policy_props(Policy, [Prop | Rest]) ->
    case lists:member(Prop, Policy) of
        true  -> check_policy_props(Policy, Rest);
        false -> false
    end;
check_policy_props(_Policy, []) ->
    true.

amqp_connection_refusal(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, amqp_connection_refusal1, [Config]).

amqp_connection_refusal1(Config) ->
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    [passed = test_amqp_connection_refusal(H, P, V) ||
        V <- [<<"AMQP",9,9,9,9>>, <<"AMQP",0,1,0,0>>, <<"XXXX",0,0,9,1>>]],
    passed.

test_amqp_connection_refusal(H, P, Header) ->
    {ok, C} = gen_tcp:connect(H, P, [binary, {active, false}]),
    ok = gen_tcp:send(C, Header),
    {ok, <<"AMQP",0,0,9,1>>} = gen_tcp:recv(C, 8, 100),
    ok = gen_tcp:close(C),
    passed.

rabbitmqctl_list_consumers(Config, Node) ->
    {ok, StdOut} = rabbit_ct_broker_helpers:rabbitmqctl(Config, Node,
      ["list_consumers"]),
    [<<"Listing consumers", _/binary>> | ConsumerRows] = re:split(StdOut, <<"\n">>, [trim]),
    CTags = [ lists:nth(3, re:split(Row, <<"\t">>)) || Row <- ConsumerRows ],
    CTags.

test_ch_metrics(Fun, Timeout) when Timeout =< 0 ->
    Fun();
test_ch_metrics(Fun, Timeout) ->
    try
        Fun()
    catch
        _:{badmatch, _} ->
            timer:sleep(1000),
            test_ch_metrics(Fun, Timeout - 1000)
    end.

head_message_timestamp_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, head_message_timestamp1, [Config]).

head_message_timestamp1(_Config) ->
    %% Can't find a way to receive the ack here so can't test pending acks status

    application:set_env(rabbit, collect_statistics, fine),

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),

    {ok, Q1} = rabbit_amqqueue:lookup(QRes),
    QPid = Q1#amqqueue.pid,

    %% Set up event receiver for queue
    dummy_event_receiver:start(self(), [node()], [queue_stats]),

    %% Check timestamp is empty when queue is empty
    Event1 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event1),

    %% Publish two messages and check timestamp is that of first message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 1}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 2}, <<"">>)),
    Event2 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    1 = proplists:get_value(head_message_timestamp, Event2),

    %% Get first message and check timestamp is that of second message
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event3 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    2 = proplists:get_value(head_message_timestamp, Event3),

    %% Get second message and check timestamp is empty again
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event4 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event4),

    %% Teardown
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),

    passed.

test_queue_statistics_receive_event(Q, Matcher) ->
    %% Q ! emit_stats,
    test_queue_statistics_receive_event1(Q, Matcher).

test_queue_statistics_receive_event1(Q, Matcher) ->
    receive #event{type = queue_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_queue_statistics_receive_event1(Q, Matcher)
            end
    after ?TIMEOUT -> throw(failed_to_receive_event)
    end.

test_spawn() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

test_spawn(Node) ->
    rpc:call(Node, ?MODULE, test_spawn_remote, []).

%% Spawn an arbitrary long lived process, so we don't end up linking
%% the channel to the short-lived process (RPC, here) spun up by the
%% RPC server.
test_spawn_remote() ->
    RPC = self(),
    spawn(fun () ->
                  {Writer, Ch} = test_spawn(),
                  RPC ! {Writer, Ch},
                  link(Ch),
                  receive
                      _ -> ok
                  end
          end),
    receive Res -> Res
    after ?TIMEOUT  -> throw(failed_to_receive_result)
    end.

%% -------------------------------------------------------------------
%% Topic matching.
%% -------------------------------------------------------------------

topic_matching(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, topic_matching1, [Config]).

topic_matching1(_Config) ->
    XName = #resource{virtual_host = <<"/">>,
                      kind = exchange,
                      name = <<"topic_matching-exchange">>},
    X0 = #exchange{name = XName, type = topic, durable = false,
                   auto_delete = false, arguments = []},
    X = rabbit_exchange_decorator:set(X0),
    %% create
    rabbit_exchange_type_topic:validate(X),
    exchange_op_callback(X, create, []),

    %% add some bindings
    Bindings = [#binding{source = XName,
                         key = list_to_binary(Key),
                         destination = #resource{virtual_host = <<"/">>,
                                                 kind = queue,
                                                 name = list_to_binary(Q)},
                         args = Args} ||
                   {Key, Q, Args} <- [{"a.b.c",         "t1",  []},
                                      {"a.*.c",         "t2",  []},
                                      {"a.#.b",         "t3",  []},
                                      {"a.b.b.c",       "t4",  []},
                                      {"#",             "t5",  []},
                                      {"#.#",           "t6",  []},
                                      {"#.b",           "t7",  []},
                                      {"*.*",           "t8",  []},
                                      {"a.*",           "t9",  []},
                                      {"*.b.c",         "t10", []},
                                      {"a.#",           "t11", []},
                                      {"a.#.#",         "t12", []},
                                      {"b.b.c",         "t13", []},
                                      {"a.b.b",         "t14", []},
                                      {"a.b",           "t15", []},
                                      {"b.c",           "t16", []},
                                      {"",              "t17", []},
                                      {"*.*.*",         "t18", []},
                                      {"vodka.martini", "t19", []},
                                      {"a.b.c",         "t20", []},
                                      {"*.#",           "t21", []},
                                      {"#.*.#",         "t22", []},
                                      {"*.#.#",         "t23", []},
                                      {"#.#.#",         "t24", []},
                                      {"*",             "t25", []},
                                      {"#.b.#",         "t26", []},
                                      {"args-test",     "t27",
                                       [{<<"foo">>, longstr, <<"bar">>}]},
                                      {"args-test",     "t27", %% Note aliasing
                                       [{<<"foo">>, longstr, <<"baz">>}]}]],
    lists:foreach(fun (B) -> exchange_op_callback(X, add_binding, [B]) end,
                  Bindings),

    %% test some matches
    test_topic_expect_match(
      X, [{"a.b.c",               ["t1", "t2", "t5", "t6", "t10", "t11", "t12",
                                   "t18", "t20", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b",                 ["t3", "t5", "t6", "t7", "t8", "t9", "t11",
                                   "t12", "t15", "t21", "t22", "t23", "t24",
                                   "t26"]},
          {"a.b.b",               ["t3", "t5", "t6", "t7", "t11", "t12", "t14",
                                   "t18", "t21", "t22", "t23", "t24", "t26"]},
          {"",                    ["t5", "t6", "t17", "t24"]},
          {"b.c.c",               ["t5", "t6", "t18", "t21", "t22", "t23",
                                   "t24", "t26"]},
          {"a.a.a.a.a",           ["t5", "t6", "t11", "t12", "t21", "t22",
                                   "t23", "t24"]},
          {"vodka.gin",           ["t5", "t6", "t8", "t21", "t22", "t23",
                                   "t24"]},
          {"vodka.martini",       ["t5", "t6", "t8", "t19", "t21", "t22", "t23",
                                   "t24"]},
          {"b.b.c",               ["t5", "t6", "t10", "t13", "t18", "t21",
                                   "t22", "t23", "t24", "t26"]},
          {"nothing.here.at.all", ["t5", "t6", "t21", "t22", "t23", "t24"]},
          {"oneword",             ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25"]},
          {"args-test",           ["t5", "t6", "t21", "t22", "t23", "t24",
                                   "t25", "t27"]}]),
    %% remove some bindings
    RemovedBindings = [lists:nth(1, Bindings), lists:nth(5, Bindings),
                       lists:nth(11, Bindings), lists:nth(19, Bindings),
                       lists:nth(21, Bindings), lists:nth(28, Bindings)],
    exchange_op_callback(X, remove_bindings, [RemovedBindings]),
    RemainingBindings = ordsets:to_list(
                          ordsets:subtract(ordsets:from_list(Bindings),
                                           ordsets:from_list(RemovedBindings))),

    %% test some matches
    test_topic_expect_match(
      X,
      [{"a.b.c",               ["t2", "t6", "t10", "t12", "t18", "t20", "t22",
                                "t23", "t24", "t26"]},
       {"a.b",                 ["t3", "t6", "t7", "t8", "t9", "t12", "t15",
                                "t22", "t23", "t24", "t26"]},
       {"a.b.b",               ["t3", "t6", "t7", "t12", "t14", "t18", "t22",
                                "t23", "t24", "t26"]},
       {"",                    ["t6", "t17", "t24"]},
       {"b.c.c",               ["t6", "t18", "t22", "t23", "t24", "t26"]},
       {"a.a.a.a.a",           ["t6", "t12", "t22", "t23", "t24"]},
       {"vodka.gin",           ["t6", "t8", "t22", "t23", "t24"]},
       {"vodka.martini",       ["t6", "t8", "t22", "t23", "t24"]},
       {"b.b.c",               ["t6", "t10", "t13", "t18", "t22", "t23",
                                "t24", "t26"]},
       {"nothing.here.at.all", ["t6", "t22", "t23", "t24"]},
       {"oneword",             ["t6", "t22", "t23", "t24", "t25"]},
       {"args-test",           ["t6", "t22", "t23", "t24", "t25", "t27"]}]),

    %% remove the entire exchange
    exchange_op_callback(X, delete, [RemainingBindings]),
    %% none should match now
    test_topic_expect_match(X, [{"a.b.c", []}, {"b.b.c", []}, {"", []}]),
    passed.

exchange_op_callback(X, Fun, Args) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> rabbit_exchange:callback(X, Fun, transaction, [X] ++ Args) end),
    rabbit_exchange:callback(X, Fun, none, [X] ++ Args).

test_topic_expect_match(X, List) ->
    lists:foreach(
      fun ({Key, Expected}) ->
              BinKey = list_to_binary(Key),
              Message = rabbit_basic:message(X#exchange.name, BinKey,
                                             #'P_basic'{}, <<>>),
              Res = rabbit_exchange_type_topic:route(
                      X, #delivery{mandatory = false,
                                   sender    = self(),
                                   message   = Message}),
              ExpectedRes = lists:map(
                              fun (Q) -> #resource{virtual_host = <<"/">>,
                                                   kind = queue,
                                                   name = list_to_binary(Q)}
                              end, Expected),
              true = (lists:usort(ExpectedRes) =:= lists:usort(Res))
      end, List).

%% ---------------------------------------------------------------------------
%% Unordered tests (originally from rabbit_tests.erl).
%% ---------------------------------------------------------------------------

confirms(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, confirms1, [Config]).

confirms1(_Config) ->
    {_Writer, Ch} = test_spawn(),
    DeclareBindDurableQueue =
        fun() ->
                rabbit_channel:do(Ch, #'queue.declare'{durable = true}),
                receive #'queue.declare_ok'{queue = Q0} ->
                        rabbit_channel:do(Ch, #'queue.bind'{
                                                 queue = Q0,
                                                 exchange = <<"amq.direct">>,
                                                 routing_key = "confirms-magic" }),
                        receive #'queue.bind_ok'{} -> Q0
                        after ?TIMEOUT -> throw(failed_to_bind_queue)
                        end
                after ?TIMEOUT -> throw(failed_to_declare_queue)
                end
        end,
    %% Declare and bind two queues
    QName1 = DeclareBindDurableQueue(),
    QName2 = DeclareBindDurableQueue(),
    %% Get the first one's pid (we'll crash it later)
    {ok, Q1} = rabbit_amqqueue:lookup(rabbit_misc:r(<<"/">>, queue, QName1)),
    QPid1 = Q1#amqqueue.pid,
    %% Enable confirms
    rabbit_channel:do(Ch, #'confirm.select'{}),
    receive
        #'confirm.select_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_enable_confirms)
    end,
    %% Publish a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"amq.direct">>,
                                           routing_key = "confirms-magic"
                                          },
                      rabbit_basic:build_content(
                        #'P_basic'{delivery_mode = 2}, <<"">>)),
    %% We must not kill the queue before the channel has processed the
    %% 'publish'.
    ok = rabbit_channel:flush(Ch),
    %% Crash the queue
    QPid1 ! boom,
    %% Wait for a nack
    receive
        #'basic.nack'{} -> ok;
        #'basic.ack'{}  -> throw(received_ack_instead_of_nack)
    after ?TIMEOUT-> throw(did_not_receive_nack)
    end,
    receive
        #'basic.ack'{} -> throw(received_ack_when_none_expected)
    after 1000 -> ok
    end,
    %% Cleanup
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName2}),
    receive
        #'queue.delete_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_cleanup_queue)
    end,
    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

gen_server2_with_state(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, gen_server2_with_state1, [Config]).

gen_server2_with_state1(_Config) ->
    fhc_state = gen_server2:with_state(file_handle_cache,
                                       fun (S) -> element(1, S) end),
    passed.

mcall(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, mcall1, [Config]).

mcall1(_Config) ->
    P1 = spawn(fun gs2_test_listener/0),
    register(foo, P1),
    global:register_name(gfoo, P1),

    P2 = spawn(fun() -> exit(bang) end),
    %% ensure P2 is dead (ignore the race setting up the monitor)
    await_exit(P2),

    P3 = spawn(fun gs2_test_crasher/0),

    %% since P2 crashes almost immediately and P3 after receiving its first
    %% message, we have to spawn a few more processes to handle the additional
    %% cases we're interested in here
    register(baz, spawn(fun gs2_test_crasher/0)),
    register(bog, spawn(fun gs2_test_crasher/0)),
    global:register_name(gbaz, spawn(fun gs2_test_crasher/0)),

    NoNode = rabbit_nodes:make("nonode"),

    Targets =
        %% pids
        [P1, P2, P3]
        ++
        %% registered names
        [foo, bar, baz]
        ++
        %% {Name, Node} pairs
        [{foo, node()}, {bar, node()}, {bog, node()}, {foo, NoNode}]
        ++
        %% {global, Name}
        [{global, gfoo}, {global, gbar}, {global, gbaz}],

    GoodResults = [{D, goodbye} || D <- [P1, foo,
                                         {foo, node()},
                                         {global, gfoo}]],

    BadResults  = [{P2,             noproc},   % died before use
                   {P3,             boom},     % died on first use
                   {bar,            noproc},   % never registered
                   {baz,            boom},     % died on first use
                   {{bar, node()},  noproc},   % never registered
                   {{bog, node()},  boom},     % died on first use
                   {{foo, NoNode},  nodedown}, % invalid node
                   {{global, gbar}, noproc},   % never registered globally
                   {{global, gbaz}, boom}],    % died on first use

    {Replies, Errors} = gen_server2:mcall([{T, hello} || T <- Targets]),
    true = lists:sort(Replies) == lists:sort(GoodResults),
    true = lists:sort(Errors)  == lists:sort(BadResults),

    %% cleanup (ignore the race setting up the monitor)
    P1 ! stop,
    await_exit(P1),
    passed.

await_exit(Pid) ->
    MRef = erlang:monitor(process, Pid),
    receive
        {'DOWN', MRef, _, _, _} -> ok
    end.

gs2_test_crasher() ->
    receive
        {'$gen_call', _From, hello} -> exit(boom)
    end.

gs2_test_listener() ->
    receive
        {'$gen_call', From, hello} ->
            gen_server2:reply(From, goodbye),
            gs2_test_listener();
        stop ->
            ok
    end.

configurable_server_properties(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, configurable_server_properties1, [Config]).

configurable_server_properties1(_Config) ->
    %% List of the names of the built-in properties do we expect to find
    BuiltInPropNames = [<<"product">>, <<"version">>, <<"platform">>,
                        <<"copyright">>, <<"information">>],

    Protocol = rabbit_framing_amqp_0_9_1,

    %% Verify that the built-in properties are initially present
    ActualPropNames = [Key || {Key, longstr, _} <-
                                  rabbit_reader:server_properties(Protocol)],
    true = lists:all(fun (X) -> lists:member(X, ActualPropNames) end,
                     BuiltInPropNames),

    %% Get the initial server properties configured in the environment
    {ok, ServerProperties} = application:get_env(rabbit, server_properties),

    %% Helper functions
    ConsProp = fun (X) -> application:set_env(rabbit,
                                              server_properties,
                                              [X | ServerProperties]) end,
    IsPropPresent =
        fun (X) ->
                lists:member(X, rabbit_reader:server_properties(Protocol))
        end,

    %% Add a wholly new property of the simplified {KeyAtom, StringValue} form
    NewSimplifiedProperty = {NewHareKey, NewHareVal} = {hare, "soup"},
    ConsProp(NewSimplifiedProperty),
    %% Do we find hare soup, appropriately formatted in the generated properties?
    ExpectedHareImage = {list_to_binary(atom_to_list(NewHareKey)),
                         longstr,
                         list_to_binary(NewHareVal)},
    true = IsPropPresent(ExpectedHareImage),

    %% Add a wholly new property of the {BinaryKey, Type, Value} form
    %% and check for it
    NewProperty = {<<"new-bin-key">>, signedint, -1},
    ConsProp(NewProperty),
    %% Do we find the new property?
    true = IsPropPresent(NewProperty),

    %% Add a property that clobbers a built-in, and verify correct clobbering
    {NewVerKey, NewVerVal} = NewVersion = {version, "X.Y.Z."},
    {BinNewVerKey, BinNewVerVal} = {list_to_binary(atom_to_list(NewVerKey)),
                                    list_to_binary(NewVerVal)},
    ConsProp(NewVersion),
    ClobberedServerProps = rabbit_reader:server_properties(Protocol),
    %% Is the clobbering insert present?
    true = IsPropPresent({BinNewVerKey, longstr, BinNewVerVal}),
    %% Is the clobbering insert the only thing with the clobbering key?
    [{BinNewVerKey, longstr, BinNewVerVal}] =
        [E || {K, longstr, _V} = E <- ClobberedServerProps, K =:= BinNewVerKey],

    application:set_env(rabbit, server_properties, ServerProperties),
    passed.

set_disk_free_limit_command(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_disk_free_limit_command1, [Config]).

set_disk_free_limit_command1(_Config) ->
    ok = control_action(set_disk_free_limit,
      ["2000kiB"]),
    2048000 = rabbit_disk_monitor:get_disk_free_limit(),

    %% Use an integer
    ok = control_action(set_disk_free_limit,
      ["mem_relative", "1"]),
    check_limit(1),

    %% Use a float
    ok = control_action(set_disk_free_limit,
      ["mem_relative", "1.5"]),
    check_limit(1.5),

    ok = control_action(set_disk_free_limit, ["50MB"]),
    passed.

check_limit(Limit) ->
    ExpectedLimit = Limit * vm_memory_monitor:get_total_memory(),
    % Total memory is unstable, so checking order
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() < 1.2,
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() > 0.98.

%% ---------------------------------------------------------------------------
%% rabbitmqctl helpers.
%% ---------------------------------------------------------------------------

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed: ~p~n", [Other]),
            Other
    end.

control_action_t(Command, Args, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args, default_options(), Timeout).

control_action_t(Command, Args, NewOpts, Timeout) when is_number(Timeout) ->
    control_action_t(Command, node(), Args,
                     expand_options(default_options(), NewOpts),
                     Timeout).

control_action_t(Command, Node, Args, Opts, Timeout) when is_number(Timeout) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end, Timeout) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed: ~p~n", [Other]),
            Other
    end.

control_action_opts(Raw) ->
    NodeStr = atom_to_list(node()),
    case rabbit_control_main:parse_arguments(Raw, NodeStr) of
        {ok, {Cmd, Opts, Args}} ->
            case control_action(Cmd, node(), Args, Opts) of
                ok    -> ok;
                Error -> Error
            end;
        Error ->
            Error
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, [], ["-p", "/"]);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

info_action_t(Command, Args, CheckVHost, Timeout) when is_number(Timeout) ->
    if CheckVHost -> ok = control_action_t(Command, [], ["-p", "/"], Timeout);
       true       -> ok
    end,
    ok = control_action_t(Command, lists:map(fun atom_to_list/1, Args), Timeout),
    ok.

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).
