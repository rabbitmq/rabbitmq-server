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
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(TIMEOUT_LIST_OPS_PASS, 5000).
-define(TIMEOUT, 30000).
-define(TIMEOUT_CHANNEL_EXCEPTION, 5000).

-define(CLEANUP_QUEUE_NAME, <<"cleanup-queue">>).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    MaxLengthTests = [max_length_default,
                      max_length_bytes_default,
                      max_length_drop_head,
                      max_length_bytes_drop_head,
                      max_length_reject_confirm,
                      max_length_bytes_reject_confirm,
                      max_length_drop_publish,
                      max_length_drop_publish_requeue,
                      max_length_bytes_drop_publish],
    [
      {parallel_tests, [parallel], [
          amqp_connection_refusal,
          configurable_server_properties,
          credit_flow_settings,
          dynamic_mirroring,
          gen_server2_with_state,
          mcall,
          {password_hashing, [], [
              password_hashing,
              change_password
            ]},
          {auth_backend_internal, [parallel], [
              login_with_credentials_but_no_password,
              login_of_passwordless_user,
              set_tags_for_passwordless_user
            ]},
          set_disk_free_limit_command,
          set_vm_memory_high_watermark_command,
          topic_matching,
          max_message_size,

          {queue_max_length, [], [
                                  {max_length_classic, [], MaxLengthTests},
                                  {max_length_quorum, [], [max_length_default,
                                                           max_length_bytes_default]
                                  },
                                  {max_length_mirrored, [], MaxLengthTests}
                                 ]}
       ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(max_length_classic, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {queue_durable, false}]);
init_per_group(max_length_quorum, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, quorum_queue) of
        ok ->
            rabbit_ct_helpers:set_config(
              Config,
              [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
               {queue_durable, true}]);
        Skip ->
            Skip
    end;
init_per_group(max_length_mirrored, Config) ->
    rabbit_ct_broker_helpers:set_ha_policy(Config, 0, <<"^max_length.*queue">>,
        <<"all">>, [{<<"ha-sync-mode">>, <<"automatic">>}]),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{is_mirrored, true},
                         {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                         {queue_durable, false}]),
    rabbit_ct_helpers:run_steps(Config1, []);
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

end_per_group(max_length_mirrored, Config) ->
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"^max_length.*queue">>),
    Config1 = rabbit_ct_helpers:set_config(Config, [{is_mirrored, false}]),
    Config1;
end_per_group(queue_max_length, Config) ->
    Config;
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
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~p", [Group, Testcase])),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config)
  when Testcase == max_length_drop_publish; Testcase == max_length_bytes_drop_publish;
       Testcase == max_length_drop_publish_requeue;
       Testcase == max_length_reject_confirm; Testcase == max_length_bytes_reject_confirm;
       Testcase == max_length_drop_head; Testcase == max_length_bytes_drop_head;
       Testcase == max_length_default; Testcase == max_length_bytes_default ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);

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
    Name1 = iolist_to_binary(rabbit_ct_helpers:config_to_testcase_name(Config, Name)),
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
    rabbit_amqqueue:pseudo_queue(test_queue(), self(), Durable).

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
        {ok, _} -> rabbit_auth_backend_internal:delete_user(UserName, <<"acting-user">>);
        _       -> ok
    end,
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_md5),
    ok = rabbit_auth_backend_internal:add_user(UserName, Password, <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    ok = application:set_env(rabbit, password_hashing_module,
                             rabbit_password_hashing_sha256),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),

    NewPassword = <<"test_password1">>,
    ok = rabbit_auth_backend_internal:change_password(UserName, NewPassword,
                                                      <<"acting-user">>),
    {ok, #auth_user{username = UserName}} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, NewPassword}]),

    {refused, _, [UserName]} =
        rabbit_auth_backend_internal:user_login_authentication(
            UserName, [{password, Password}]),
    passed.


%% -------------------------------------------------------------------
%% rabbit_auth_backend_internal
%% -------------------------------------------------------------------

login_with_credentials_but_no_password(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_with_credentials_but_no_password1, [Config]).

login_with_credentials_but_no_password1(_Config) ->
    Username = <<"login_with_credentials_but_no_password-user">>,
    Password = <<"login_with_credentials_but_no_password-password">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    try
        rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{key, <<"value">>}]),
        ?assert(false)
    catch exit:{unknown_auth_props, Username, [{key, <<"value">>}]} ->
            ok
    end,

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.

%% passwordless users are not supposed to be used with
%% this backend (and PLAIN authentication mechanism in general)
login_of_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, login_of_passwordless_user1, [Config]).

login_of_passwordless_user1(_Config) ->
    Username = <<"login_of_passwordless_user-user">>,
    Password = <<"">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password, <<"acting-user">>),

    ?assertMatch(
       {refused, _Message, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, <<"">>}])),

    ?assertMatch(
       {refused, _Format, [Username]},
       rabbit_auth_backend_internal:user_login_authentication(Username,
                                                              [{password, ""}])),

    ok = rabbit_auth_backend_internal:delete_user(Username, <<"acting-user">>),

    passed.


set_tags_for_passwordless_user(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_tags_for_passwordless_user1, [Config]).

set_tags_for_passwordless_user1(_Config) ->
    Username = <<"set_tags_for_passwordless_user">>,
    Password = <<"set_tags_for_passwordless_user">>,
    ok = rabbit_auth_backend_internal:add_user(Username, Password,
                                               <<"acting-user">>),
    ok = rabbit_auth_backend_internal:clear_password(Username,
                                                     <<"acting-user">>),
    ok = rabbit_auth_backend_internal:set_tags(Username, [management],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = [management]}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [management, policymaker],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = [management, policymaker]}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:set_tags(Username, [],
                                               <<"acting-user">>),

    ?assertMatch(
       {ok, #internal_user{tags = []}},
       rabbit_auth_backend_internal:lookup_user(Username)),

    ok = rabbit_auth_backend_internal:delete_user(Username,
                                                  <<"acting-user">>),

    passed.


%% -------------------------------------------------------------------
%% rabbitmqctl.
%% -------------------------------------------------------------------

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
    QPid = amqqueue:get_pid(Q1),

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
    rabbit_disk_monitor:set_disk_free_limit("2000kiB"),
    2048000 = rabbit_disk_monitor:get_disk_free_limit(),

    %% Use an integer
    rabbit_disk_monitor:set_disk_free_limit({mem_relative, 1}),
    disk_free_limit_to_total_memory_ratio_is(1),

    %% Use a float
    rabbit_disk_monitor:set_disk_free_limit({mem_relative, 1.5}),
    disk_free_limit_to_total_memory_ratio_is(1.5),

    rabbit_disk_monitor:set_disk_free_limit("50MB"),
    passed.

disk_free_limit_to_total_memory_ratio_is(MemRatio) ->
    ExpectedLimit = MemRatio * vm_memory_monitor:get_total_memory(),
    % Total memory is unstable, so checking order
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() < 1.2,
    true = ExpectedLimit/rabbit_disk_monitor:get_disk_free_limit() > 0.98.

set_vm_memory_high_watermark_command(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, set_vm_memory_high_watermark_command1, [Config]).

set_vm_memory_high_watermark_command1(_Config) ->
    MemLimitRatio = 1.0,
    MemTotal = vm_memory_monitor:get_total_memory(),

    vm_memory_monitor:set_vm_memory_high_watermark(MemLimitRatio),
    MemLimit = vm_memory_monitor:get_memory_limit(),
    case MemLimit of
        MemTotal -> ok;
        _        -> MemTotalToMemLimitRatio = MemLimit * 100.0 / MemTotal / 100,
                    ct:fail(
                        "Expected memory high watermark to be ~p (~s), but it was ~p (~.1f)",
                        [MemTotal, MemLimitRatio, MemLimit, MemTotalToMemLimitRatio]
                    )
    end.

max_length_bytes_drop_head(Config) ->
    max_length_bytes_drop_head(Config, [{<<"x-overflow">>, longstr, <<"drop-head">>}]).

max_length_bytes_default(Config) ->
    max_length_bytes_drop_head(Config, []).

max_length_bytes_drop_head(Config, ExtraArgs) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),

    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthBytesArgs ++ Args ++ ExtraArgs, durable = Durable}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,
    check_max_length_drops_head(Config, QName, Ch, Payload1, Payload2, Payload3).

max_length_drop_head(Config) ->
    max_length_drop_head(Config, [{<<"x-overflow">>, longstr, <<"drop-head">>}]).

max_length_default(Config) ->
    %% Defaults to drop_head
    max_length_drop_head(Config, []).

max_length_drop_head(Config, ExtraArgs) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),

    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ Args ++ ExtraArgs, durable = Durable}),

    check_max_length_drops_head(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_reject_confirm(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    QName = ?config(queue_name, Config),
    Durable = ?config(queue_durable, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    check_max_length_drops_publish(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>),
    check_max_length_rejects(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_bytes_reject_confirm(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    QNameBytes = ?config(queue_name, Config),
    Durable = ?config(queue_durable, Config),
    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QNameBytes, arguments = MaxLengthBytesArgs ++ OverflowArgs ++ Args, durable = Durable}),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,

    check_max_length_drops_publish(Config, QNameBytes, Ch, Payload1, Payload2, Payload3),
    check_max_length_rejects(Config, QNameBytes, Ch, Payload1, Payload2, Payload3).

max_length_drop_publish(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    %% If confirms are not enable, publishes will still be dropped in reject-publish mode.
    check_max_length_drops_publish(Config, QName, Ch, <<"1">>, <<"2">>, <<"3">>).

max_length_drop_publish_requeue(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    MaxLengthArgs = [{<<"x-max-length">>, long, 1}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName, arguments = MaxLengthArgs ++ OverflowArgs ++ Args, durable = Durable}),
    %% If confirms are not enable, publishes will still be dropped in reject-publish mode.
    check_max_length_requeue(Config, QName, Ch, <<"1">>, <<"2">>).

check_max_length_requeue(Config, QName, Ch, Payload1, Payload2) ->
    sync_mirrors(QName, Config),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{delivery_tag = DeliveryTag},
     #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Another message is published
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    wait_for_consensus(QName, Config),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

max_length_bytes_drop_publish(Config) ->
    {_Conn, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QNameBytes = ?config(queue_name, Config),
    MaxLengthBytesArgs = [{<<"x-max-length-bytes">>, long, 100}],
    OverflowArgs = [{<<"x-overflow">>, longstr, <<"reject-publish">>}],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QNameBytes, arguments = MaxLengthBytesArgs ++ OverflowArgs ++ Args, durable = Durable}),

    %% 80 bytes payload
    Payload1 = << <<"1">> || _ <- lists:seq(1, 80) >>,
    Payload2 = << <<"2">> || _ <- lists:seq(1, 80) >>,
    Payload3 = << <<"3">> || _ <- lists:seq(1, 80) >>,

    check_max_length_drops_publish(Config, QNameBytes, Ch, Payload1, Payload2, Payload3).

check_max_length_drops_publish(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Message 2 is dropped, message 1 stays
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    wait_for_consensus(QName, Config),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Messages 2 and 3 are dropped, message 1 stays
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    wait_for_consensus(QName, Config),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    wait_for_consensus(QName, Config),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

wait_for_consensus(QName, Config) ->
    case lists:keyfind(<<"x-queue-type">>, 1, ?config(queue_args, Config)) of
        {_, _, <<"quorum">>} ->
            Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
            RaName = binary_to_atom(<<"%2F_", QName/binary>>, utf8),
            {ok, _, _} = ra:members({RaName, Server});
        _ ->
            ok
    end.

check_max_length_rejects(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),
    amqp_channel:register_confirm_handler(Ch, self()),
    flush(),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% First message can be enqueued and acks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    receive #'basic.ack'{} -> ok
    after 1000 -> error(expected_ack)
    end,

    %% The message cannot be enqueued and nacks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    receive #'basic.nack'{} -> ok
    after 1000 -> error(expected_nack)
    end,

    %% The message cannot be enqueued and nacks
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    receive #'basic.nack'{} -> ok
    after 1000 -> error(expected_nack)
    end,

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Now we can publish message 2.
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    receive #'basic.ack'{} -> ok
    after 1000 -> error(expected_ack)
    end,

    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

check_max_length_drops_head(Config, QName, Ch, Payload1, Payload2, Payload3) ->
    sync_mirrors(QName, Config),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    %% A single message is published and consumed
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload1}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Message 1 is replaced by message 2
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload2}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),

    %% Messages 1 and 2 are replaced
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload1}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload2}),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QName}, #amqp_msg{payload = Payload3}),
    wait_for_consensus(QName, Config),
    {#'basic.get_ok'{}, #amqp_msg{payload = Payload3}} = amqp_channel:call(Ch, #'basic.get'{queue = QName}),
    #'basic.get_empty'{} = amqp_channel:call(Ch, #'basic.get'{queue = QName}).

sync_mirrors(QName, Config) ->
    case ?config(is_mirrored, Config) of
        true ->
            rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, [<<"sync_queue">>, QName]);
        _ -> ok
    end.

gen_binary_mb(N) ->
    B1M = << <<"_">> || _ <- lists:seq(1, 1024 * 1024) >>,
    << B1M || _ <- lists:seq(1, N) >>.

assert_channel_alive(Ch) ->
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"nope">>},
                          #amqp_msg{payload = <<"HI">>}).

assert_channel_fail_max_size(Ch, Monitor) ->
    receive
        {'DOWN', Monitor, process, Ch,
            {shutdown,
                {server_initiated_close, 406, _Error}}} ->
            ok
    after ?TIMEOUT_CHANNEL_EXCEPTION ->
        error({channel_exception_expected, max_message_size})
    end.

max_message_size(Config) ->
    Binary2M  = gen_binary_mb(2),
    Binary4M  = gen_binary_mb(4),
    Binary6M  = gen_binary_mb(6),
    Binary10M = gen_binary_mb(10),

    Size2Mb = 1024 * 1024 * 2,
    Size2Mb = byte_size(Binary2M),

    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 3]),

    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    %% Binary is within the max size limit
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary2M}),
    %% The channel process is alive
    assert_channel_alive(Ch),

    Monitor = monitor(process, Ch),
    amqp_channel:call(Ch, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_fail_max_size(Ch, Monitor),

    %% increase the limit
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 8]),

    {_, Ch1} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary2M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary4M}),
    assert_channel_alive(Ch1),

    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"nope">>}, #amqp_msg{payload = Binary6M}),
    assert_channel_alive(Ch1),

    Monitor1 = monitor(process, Ch1),
    amqp_channel:call(Ch1, #'basic.publish'{routing_key = <<"none">>}, #amqp_msg{payload = Binary10M}),
    assert_channel_fail_max_size(Ch1, Monitor1),

    %% increase beyond the hard limit
    rabbit_ct_broker_helpers:rpc(Config, 0,
                                 application, set_env, [rabbit, max_message_size, 1024 * 1024 * 600]),
    Val = rabbit_ct_broker_helpers:rpc(Config, 0,
                                       rabbit_channel, get_max_message_size, []),

    ?assertEqual(?MAX_MSG_SIZE, Val).

%% ---------------------------------------------------------------------------
%% rabbitmqctl helpers.
%% ---------------------------------------------------------------------------

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).

flush() ->
    receive _ -> flush()
    after 10 -> ok
    end.
