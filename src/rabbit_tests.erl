%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_tests).

-compile(export_all).

-export([all_tests/0, test_parsing/0, test_disk_queue/0]).

-import(lists).

-include("rabbit.hrl").
-include_lib("kernel/include/file.hrl").

test_content_prop_roundtrip(Datum, Binary) ->
    Types =  [element(1, E) || E <- Datum],
    Values = [element(2, E) || E <- Datum],
    Values = rabbit_binary_parser:parse_properties(Types, Binary), %% assertion
    Binary = rabbit_binary_generator:encode_properties(Types, Values). %% assertion

all_tests() ->
    passed = test_priority_queue(),
    passed = test_parsing(),
    passed = test_topic_matching(),
    passed = test_log_management(),
    passed = test_app_management(),
    passed = test_log_management_during_startup(),
    passed = test_cluster_management(),
    passed = test_user_management(),
    passed = test_server_status(),
    passed = test_disk_queue(),
    passed.

test_priority_queue() ->

    false = priority_queue:is_queue(not_a_queue),

    %% empty Q
    Q = priority_queue:new(),
    {true, true, 0, [], []} = test_priority_queue(Q),

    %% 1-4 element no-priority Q
    true = lists:all(fun (X) -> X =:= passed end,
                     lists:map(fun test_simple_n_element_queue/1,
                               lists:seq(1, 4))),

    %% 1-element priority Q
    Q1 = priority_queue:in(foo, 1, priority_queue:new()),
    {true, false, 1, [{1, foo}], [foo]} = test_priority_queue(Q1),

    %% 2-element same-priority Q
    Q2 = priority_queue:in(bar, 1, Q1),
    {true, false, 2, [{1, foo}, {1, bar}], [foo, bar]} =
        test_priority_queue(Q2),

    %% 2-element different-priority Q
    Q3 = priority_queue:in(bar, 2, Q1),
    {true, false, 2, [{2, bar}, {1, foo}], [bar, foo]} =
        test_priority_queue(Q3),

    %% 1-element negative priority Q
    Q4 = priority_queue:in(foo, -1, priority_queue:new()),
    {true, false, 1, [{-1, foo}], [foo]} = test_priority_queue(Q4),

    passed.

priority_queue_in_all(Q, L) ->
    lists:foldl(fun (X, Acc) -> priority_queue:in(X, Acc) end, Q, L).

priority_queue_out_all(Q) ->
    case priority_queue:out(Q) of
        {empty, _}       -> [];
        {{value, V}, Q1} -> [V | priority_queue_out_all(Q1)]
    end.

test_priority_queue(Q) ->
    {priority_queue:is_queue(Q),
     priority_queue:is_empty(Q),
     priority_queue:len(Q),
     priority_queue:to_list(Q),
     priority_queue_out_all(Q)}.

test_simple_n_element_queue(N) ->
    Items = lists:seq(1, N),
    Q = priority_queue_in_all(priority_queue:new(), Items),
    ToListRes = [{0, X} || X <- Items],
    {true, false, N, ToListRes, Items} = test_priority_queue(Q),
    passed.

test_parsing() ->
    passed = test_content_properties(),
    passed.

test_content_properties() ->
    test_content_prop_roundtrip([], <<0, 0>>),
    test_content_prop_roundtrip([{bit, true}, {bit, false}, {bit, true}, {bit, false}],
                                <<16#A0, 0>>),
    test_content_prop_roundtrip([{bit, true}, {octet, 123}, {bit, true}, {octet, undefined},
                                 {bit, true}],
                                <<16#E8,0,123>>),
    test_content_prop_roundtrip([{bit, true}, {octet, 123}, {octet, 123}, {bit, true}],
                                <<16#F0,0,123,123>>),
    test_content_prop_roundtrip([{bit, true}, {shortstr, <<"hi">>}, {bit, true},
                                 {shortint, 54321}, {bit, true}],
                                <<16#F8,0,2,"hi",16#D4,16#31>>),
    test_content_prop_roundtrip([{bit, true}, {shortstr, undefined}, {bit, true},
                                 {shortint, 54321}, {bit, true}],
                                <<16#B8,0,16#D4,16#31>>),
    test_content_prop_roundtrip([{table, [{<<"a signedint">>, signedint, 12345678},
                                          {<<"a longstr">>, longstr, <<"yes please">>},
                                          {<<"a decimal">>, decimal, {123, 12345678}},
                                          {<<"a timestamp">>, timestamp, 123456789012345},
                                          {<<"a nested table">>, table,
                                           [{<<"one">>, signedint, 1},
                                            {<<"two">>, signedint, 2}]}]}],
                                <<
                                 16#8000:16,                % flags
                                 % properties:

                                 117:32,                % table length in bytes

                                 11,"a signedint",        % name
                                 "I",12345678:32,        % type and value

                                 9,"a longstr",
                                 "S",10:32,"yes please",

                                 9,"a decimal",
                                 "D",123,12345678:32,

                                 11,"a timestamp",
                                 "T", 123456789012345:64,

                                 14,"a nested table",
                                 "F",
                                        18:32,

                                        3,"one",
                                        "I",1:32,

                                        3,"two",
                                        "I",2:32 >>),
    case catch rabbit_binary_parser:parse_properties([bit, bit, bit, bit], <<16#A0,0,1>>) of
        {'EXIT', content_properties_binary_overflow} -> passed;
        V -> exit({got_success_but_expected_failure, V})
    end.

test_topic_match(P, R) ->
    test_topic_match(P, R, true).

test_topic_match(P, R, Expected) ->
    case rabbit_exchange:topic_matches(list_to_binary(P), list_to_binary(R)) of
        Expected ->
            passed;
        _ ->
            {topic_match_failure, P, R}
    end.

test_topic_matching() ->
    passed = test_topic_match("#", "test.test"),
    passed = test_topic_match("#", ""),
    passed = test_topic_match("#.T.R", "T.T.R"),
    passed = test_topic_match("#.T.R", "T.R.T.R"),
    passed = test_topic_match("#.Y.Z", "X.Y.Z.X.Y.Z"),
    passed = test_topic_match("#.test", "test"),
    passed = test_topic_match("#.test", "test.test"),
    passed = test_topic_match("#.test", "ignored.test"),
    passed = test_topic_match("#.test", "more.ignored.test"),
    passed = test_topic_match("#.test", "notmatched", false),
    passed = test_topic_match("#.z", "one.two.three.four", false),
    passed.

test_app_management() ->
    %% starting, stopping, status
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    ok = control_action(start_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    passed.

test_log_management() ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),
    Suffix = ".1",

    %% prepare basic logs
    file:delete([MainLog, Suffix]),
    file:delete([SaslLog, Suffix]),

    %% simple logs reopening
    ok = control_action(rotate_logs, []),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% simple log rotation
    ok = control_action(rotate_logs, [Suffix]),
    [true, true] = non_empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% reopening logs with log rotation performed first
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = file:rename(MainLog, [MainLog, Suffix]),
    ok = file:rename(SaslLog, [SaslLog, Suffix]),
    ok = test_logs_working([MainLog, Suffix], [SaslLog, Suffix]),
    ok = control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% log rotation on empty file
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = control_action(rotate_logs, [Suffix]),
    [true, true] = empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),

    %% original main log file is not writable
    ok = make_files_non_writable([MainLog]),
    {error, {cannot_rotate_main_logs, _}} = control_action(rotate_logs, []),
    ok = clean_logs([MainLog], Suffix),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog}]),

    %% original sasl log file is not writable
    ok = make_files_non_writable([SaslLog]),
    {error, {cannot_rotate_sasl_logs, _}} = control_action(rotate_logs, []),
    ok = clean_logs([SaslLog], Suffix),
    ok = add_log_handlers([{rabbit_sasl_report_file_h, SaslLog}]),

    %% logs with suffix are not writable
    ok = control_action(rotate_logs, [Suffix]),
    ok = make_files_non_writable([[MainLog, Suffix], [SaslLog, Suffix]]),
    ok = control_action(rotate_logs, [Suffix]),
    ok = test_logs_working(MainLog, SaslLog),

    %% original log files are not writable
    ok = make_files_non_writable([MainLog, SaslLog]),
    {error, {{cannot_rotate_main_logs, _},
             {cannot_rotate_sasl_logs, _}}} = control_action(rotate_logs, []),

    %% logging directed to tty (handlers were removed in last test)
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = application:set_env(sasl, sasl_error_logger, tty),
    ok = application:set_env(kernel, error_logger, tty),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% rotate logs when logging is turned off
    ok = application:set_env(sasl, sasl_error_logger, false),
    ok = application:set_env(kernel, error_logger, silent),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% cleanup
    ok = application:set_env(sasl, sasl_error_logger, {file, SaslLog}),
    ok = application:set_env(kernel, error_logger, {file, MainLog}),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog},
                           {rabbit_sasl_report_file_h, SaslLog}]),
    passed.

test_log_management_during_startup() ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),

    %% start application with simple tty logging
    ok = control_action(stop_app, []),
    ok = application:set_env(kernel, error_logger, tty),
    ok = application:set_env(sasl, sasl_error_logger, tty),
    ok = add_log_handlers([{error_logger_tty_h, []},
                           {sasl_report_tty_h, []}]),
    ok = control_action(start_app, []),

    %% start application with tty logging and 
    %% proper handlers not installed
    ok = control_action(stop_app, []),
    ok = error_logger:tty(false),
    ok = delete_log_handlers([sasl_report_tty_h]),
    ok = case catch control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                        log_rotation_tty_no_handlers_test});
             {error, {cannot_log_to_tty, _, _}} -> ok
         end,

    %% fix sasl logging
    ok = application:set_env(sasl, sasl_error_logger,
                             {file, SaslLog}),

    %% start application with logging to non-existing directory
    TmpLog = "/tmp/rabbit-tests/test.log",
    delete_file(TmpLog),
    ok = application:set_env(kernel, error_logger, {file, TmpLog}),

    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = control_action(start_app, []),

    %% start application with logging to directory with no
    %% write permissions
    TmpDir = "/tmp/rabbit-tests",
    ok = set_permissions(TmpDir, 8#00400),
    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                        log_rotation_no_write_permission_dir_test}); 
            {error, {cannot_log_to_file, _, _}} -> ok
         end,

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    TmpTestDir = "/tmp/rabbit-tests/no-permission/test/log",
    ok = application:set_env(kernel, error_logger, {file, TmpTestDir}),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                        log_rotatation_parent_dirs_test});
             {error, {cannot_log_to_file, _,
               {error, {cannot_create_parent_dirs, _, eacces}}}} -> ok
         end,
    ok = set_permissions(TmpDir, 8#00700),
    ok = set_permissions(TmpLog, 8#00600),
    ok = delete_file(TmpLog),
    ok = file:del_dir(TmpDir),

    %% start application with standard error_logger_file_h
    %% handler not installed 
    ok = application:set_env(kernel, error_logger, {file, MainLog}),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% start application with standard sasl handler not installed
    %% and rabbit main log handler installed correctly
    ok = delete_log_handlers([rabbit_sasl_report_file_h]),
    ok = control_action(start_app, []),
    passed.

test_cluster_management() ->

    %% 'cluster' and 'reset' should only work if the app is stopped
    {error, _} = control_action(cluster, []),
    {error, _} = control_action(reset, []),
    {error, _} = control_action(force_reset, []),

    ok = control_action(stop_app, []),

    %% various ways of creating a standalone node
    NodeS = atom_to_list(node()),
    ClusteringSequence = [[],
                          [NodeS],
                          ["invalid@invalid", NodeS],
                          [NodeS, "invalid@invalid"]],

    ok = control_action(reset, []),
    lists:foreach(fun (Arg) ->
                          ok = control_action(cluster, Arg),
                          ok
                  end,
                  ClusteringSequence),
    lists:foreach(fun (Arg) ->
                          ok = control_action(reset, []),
                          ok = control_action(cluster, Arg),
                          ok
                  end,
                  ClusteringSequence),
    ok = control_action(reset, []),
    lists:foreach(fun (Arg) ->
                          ok = control_action(cluster, Arg),
                          ok = control_action(start_app, []),
                          ok = control_action(stop_app, []),
                          ok
                  end,
                  ClusteringSequence),
    lists:foreach(fun (Arg) ->
                          ok = control_action(reset, []),
                          ok = control_action(cluster, Arg),
                          ok = control_action(start_app, []),
                          ok = control_action(stop_app, []),
                          ok
                  end,
                  ClusteringSequence),

    %% convert a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(cluster, ["invalid1@invalid",
                                  "invalid2@invalid"]),

    %% join a non-existing cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, ["invalid1@invalid",
                                  "invalid2@invalid"]),

    SecondaryNode = rabbit_misc:localnode(hare),
    case net_adm:ping(SecondaryNode) of
        pong -> passed = test_cluster_management2(SecondaryNode);
        pang -> io:format("Skipping clustering tests with node ~p~n",
                          [SecondaryNode])
    end,

    ok = control_action(start_app, []),
    passed.

test_cluster_management2(SecondaryNode) ->
    NodeS = atom_to_list(node()),
    SecondaryNodeS = atom_to_list(SecondaryNode),

    %% make a disk node
    ok = control_action(reset, []),
    ok = control_action(cluster, [NodeS]),
    %% make a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),

    %% join cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS, "invalid1@invalid"]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% change cluster config while remaining in same cluster
    ok = control_action(cluster, ["invalid2@invalid", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% join non-existing cluster as a ram node
    ok = control_action(cluster, ["invalid1@invalid",
                                  "invalid2@invalid"]),
    %% turn ram node into disk node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    
    %% convert a disk node into a ram node
    ok = control_action(cluster, ["invalid1@invalid",
                                  "invalid2@invalid"]),

    %% turn a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% NB: this will log an inconsistent_database error, which is harmless
    true = disconnect_node(SecondaryNode),
    pong = net_adm:ping(SecondaryNode),

    %% leaving a cluster as a ram node
    ok = control_action(reset, []),
    %% ...and as a disk node
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(reset, []),

    %% attempt to leave cluster when no other node is alive
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, SecondaryNode, []),
    ok = control_action(stop_app, []),
    {error, {no_running_cluster_nodes, _, _}} =
        control_action(reset, []),

    %% leave system clustered, with the secondary node as a ram node
    ok = control_action(force_reset, []),
    ok = control_action(start_app, []),
    ok = control_action(force_reset, SecondaryNode, []),
    ok = control_action(cluster, SecondaryNode, [NodeS]),
    ok = control_action(start_app, SecondaryNode, []),

    passed.

test_user_management() ->

    %% lots if stuff that should fail
    {error, {no_such_user, _}} =
        control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(change_password, ["foo", "baz"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_user, _}} =
        control_action(set_permissions, ["foo", ".*", ".*", ".*"]),
    {error, {no_such_user, _}} =
        control_action(clear_permissions, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(list_user_permissions, ["foo"]),
    {error, {no_such_vhost, _}} =
        control_action(list_permissions, ["-p", "/testhost"]),
    {error, {invalid_regexp, _, _}} =
        control_action(set_permissions, ["guest", "+foo", ".*", ".*"]),

    %% user creation
    ok = control_action(add_user, ["foo", "bar"]),
    {error, {user_already_exists, _}} =
        control_action(add_user, ["foo", "bar"]),
    ok = control_action(change_password, ["foo", "baz"]),
    ok = control_action(list_users, []),

    %% vhost creation
    ok = control_action(add_vhost, ["/testhost"]),
    {error, {vhost_already_exists, _}} =
        control_action(add_vhost, ["/testhost"]),
    ok = control_action(list_vhosts, []),

    %% user/vhost mapping
    ok = control_action(set_permissions, ["-p", "/testhost",
                                          "foo", ".*", ".*", ".*"]),
    ok = control_action(set_permissions, ["-p", "/testhost",
                                          "foo", ".*", ".*", ".*"]),
    ok = control_action(list_permissions, ["-p", "/testhost"]),
    ok = control_action(list_user_permissions, ["foo"]),

    %% user/vhost unmapping
    ok = control_action(clear_permissions, ["-p", "/testhost", "foo"]),
    ok = control_action(clear_permissions, ["-p", "/testhost", "foo"]),

    %% vhost deletion
    ok = control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/testhost"]),

    %% deleting a populated vhost
    ok = control_action(add_vhost, ["/testhost"]),
    ok = control_action(set_permissions, ["-p", "/testhost",
                                          "foo", ".*", ".*", ".*"]),
    ok = control_action(delete_vhost, ["/testhost"]),

    %% user deletion
    ok = control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(delete_user, ["foo"]),

    passed.

test_server_status() ->

    %% create a queue so we have something to list
    Q = #amqqueue{} = rabbit_amqqueue:declare(
                        rabbit_misc:r(<<"/">>, queue, <<"foo">>),
                        false, false, []),

    %% list queues
    ok = info_action(
           list_queues,
           [name, durable, auto_delete, arguments, pid,
            messages_ready, messages_unacknowledged, messages_uncommitted,
            messages, acks_uncommitted, consumers, transactions, memory],
           true),

    %% list exchanges
    ok = info_action(
           list_exchanges,
           [name, type, durable, auto_delete, arguments],
           true),

    %% list bindings
    ok = control_action(list_bindings, []),

    %% cleanup
    {ok, _} = rabbit_amqqueue:delete(Q, false, false),

    %% list connections
    [#listener{host = H, port = P} | _] = rabbit_networking:active_listeners(),
    {ok, C} = gen_tcp:connect(H, P, []),
    timer:sleep(100),
    ok = info_action(
           list_connections,
           [pid, address, port, peer_address, peer_port, state,
            channels, user, vhost, timeout, frame_max,
            recv_oct, recv_cnt, send_oct, send_cnt, send_pend],
           false),
    ok = gen_tcp:close(C),

    passed.

%---------------------------------------------------------------------

control_action(Command, Args) -> control_action(Command, node(), Args).

control_action(Command, Node, Args) ->
    case catch rabbit_control:action(
                 Command, Node, Args,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        Other -> 
            io:format("failed.~n"),
            Other
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, ["-p", "/"]);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

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
    ok = rabbit_log:error("foo bar"),
    ok = error_logger:error_report(crash_report, [foo, bar]),
    %% give the error loggers some time to catch up
    timer:sleep(50),
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

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=0}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

delete_log_handlers(Handlers) ->
    [[] = error_logger:delete_report_handler(Handler) ||
        Handler <- Handlers],
    ok.

test_disk_queue() ->
    rdq_stop(),
    rdq_virgin(),
    passed = rdq_stress_gc(10000),
    passed = rdq_test_startup_with_queue_gaps(),
    passed = rdq_test_redeliver(),
    passed = rdq_test_purge(),
    passed = rdq_test_dump_queue(),
    passed = rdq_test_mixed_queue_modes(),
    passed = rdq_test_mode_conversion_mid_txn(),
    rdq_virgin(),
    ok = control_action(stop_app, []),
    ok = control_action(start_app, []),
    passed.

benchmark_disk_queue() ->
    rdq_stop(),
    % unicode chars are supported properly from r13 onwards
    io:format("Msg Count\t| Msg Size\t| Queue Count\t| Startup mu s\t| Publish mu s\t| Pub mu s/msg\t| Pub mu s/byte\t| Deliver mu s\t| Del mu s/msg\t| Del mu s/byte~n", []),
    [begin rdq_time_tx_publish_commit_deliver_ack(Qs, MsgCount, MsgSize),
           timer:sleep(1000) end || % 1000 milliseconds
        MsgSize <- [512, 8192, 32768, 131072],
        Qs <- [[1], lists:seq(1,10)], %, lists:seq(1,100), lists:seq(1,1000)],
        MsgCount <- [1024, 4096, 16384]
    ],
    rdq_virgin(),
    ok = control_action(stop_app, []),
    ok = control_action(start_app, []),
    passed.

rdq_message(MsgId, MsgBody) ->
    rabbit_basic:message(x, <<>>, <<>>, MsgBody, MsgId).

rdq_match_message(
  #basic_message { guid = MsgId, content =
                   #content { payload_fragments_rev = [MsgBody] }},
  MsgId, MsgBody, Size) when size(MsgBody) =:= Size ->
    ok.

rdq_time_tx_publish_commit_deliver_ack(Qs, MsgCount, MsgSizeBytes) ->
    Startup = rdq_virgin(),
    rdq_start(),
    QCount = length(Qs),
    Msg = <<0:(8*MsgSizeBytes)>>,
    List = lists:seq(1, MsgCount),
    {Publish, ok} =
        timer:tc(?MODULE, rdq_time_commands,
                 [[fun() -> [rabbit_disk_queue:tx_publish(rdq_message(N, Msg))
                             || N <- List, _ <- Qs] end,
                   fun() -> [ok = rabbit_disk_queue:tx_commit(Q, List, [])
                             || Q <- Qs] end
                  ]]),
    {Deliver, ok} =
        timer:tc(
          ?MODULE, rdq_time_commands,
          [[fun() -> [begin SeqIds =
                                [begin
                                     Remaining = MsgCount - N,
                                     {Message, _TSize, false, SeqId,
                                      Remaining} = rabbit_disk_queue:deliver(Q),
                                     ok = rdq_match_message(Message, N, Msg, MsgSizeBytes),
                                     SeqId
                                 end || N <- List],
                            ok = rabbit_disk_queue:tx_commit(Q, [], SeqIds)
                      end || Q <- Qs]
            end]]),
    io:format(" ~15.10B| ~14.10B| ~14.10B| ~14.1f| ~14.1f| ~14.6f| ~14.10f| ~14.1f| ~14.6f| ~14.10f~n",
              [MsgCount, MsgSizeBytes, QCount, float(Startup),
               float(Publish), (Publish / (MsgCount * QCount)),
               (Publish / (MsgCount * QCount * MsgSizeBytes)),
               float(Deliver), (Deliver / (MsgCount * QCount)),
               (Deliver / (MsgCount * QCount * MsgSizeBytes))]),
    rdq_stop().

% we know each file is going to be 1024*1024*10 bytes in size (10MB),
% so make sure we have several files, and then keep punching holes in
% a reasonably sensible way.
rdq_stress_gc(MsgCount) ->
    rdq_virgin(),
    rdq_start(),
    MsgSizeBytes = 256*1024,
    Msg = <<0:(8*MsgSizeBytes)>>, % 256KB
    List = lists:seq(1, MsgCount),
    [rabbit_disk_queue:tx_publish(rdq_message(N, Msg)) || N <- List],
    rabbit_disk_queue:tx_commit(q, List, []),
    StartChunk = round(MsgCount / 20), % 5%
    AckList =
        lists:foldl(
          fun (E, Acc) ->
                  case lists:member(E, Acc) of
                      true -> Acc;
                      false -> [E|Acc]
                  end
          end, [], lists:flatten(
                     lists:reverse(
                       [ lists:seq(N, MsgCount, N)
                         || N <- lists:seq(1, round(MsgCount / 2), 1)
                       ]))),
    {Start, End} = lists:split(StartChunk, AckList),
    AckList2 = End ++ Start,
    MsgIdToSeqDict =
        lists:foldl(
          fun (MsgId, Acc) ->
                  Remaining = MsgCount - MsgId,
                  {Message, _TSize, false, SeqId, Remaining} =
                      rabbit_disk_queue:deliver(q),
                  ok = rdq_match_message(Message, MsgId, Msg, MsgSizeBytes),
                  dict:store(MsgId, SeqId, Acc)
          end, dict:new(), List),
    %% we really do want to ack each of this individually
    [begin {ok, SeqId} = dict:find(MsgId, MsgIdToSeqDict),
           rabbit_disk_queue:ack(q, [SeqId])
     end || MsgId <- AckList2],
    rabbit_disk_queue:tx_commit(q, [], []),
    empty = rabbit_disk_queue:deliver(q),
    rdq_stop(),
    passed.

rdq_test_startup_with_queue_gaps() ->
    rdq_virgin(),
    rdq_start(),
    Msg = <<0:(8*256)>>,
    Total = 1000,
    Half = round(Total/2),
    All = lists:seq(1,Total),
    [rabbit_disk_queue:tx_publish(rdq_message(N, Msg)) || N <- All],
    rabbit_disk_queue:tx_commit(q, All, []),
    io:format("Publish done~n", []),
    %% deliver first half
    Seqs = [begin
                Remaining = Total - N,
                {Message, _TSize, false, SeqId, Remaining} =
                    rabbit_disk_queue:deliver(q),
                ok = rdq_match_message(Message, N, Msg, 256),
                SeqId
            end || N <- lists:seq(1,Half)],
    io:format("Deliver first half done~n", []),
    %% ack every other message we have delivered (starting at the _first_)
    lists:foldl(fun (SeqId2, true) ->
                        rabbit_disk_queue:ack(q, [SeqId2]),
                        false;
                    (_SeqId2, false) ->
                        true
                end, true, Seqs),
    rabbit_disk_queue:tx_commit(q, [], []),
    io:format("Acked every other message delivered done~n", []),
    rdq_stop(),
    rdq_start(),
    io:format("Startup (with shuffle) done~n", []),
    %% should have shuffled up. So we should now get
    %% lists:seq(2,500,2) already delivered
    Seqs2 = [begin
                 Remaining = round(Total - ((Half + N)/2)),
                 {Message, _TSize, true, SeqId, Remaining} =
                     rabbit_disk_queue:deliver(q),
                 ok = rdq_match_message(Message, N, Msg, 256),
                 SeqId
             end || N <- lists:seq(2,Half,2)],
    rabbit_disk_queue:tx_commit(q, [], Seqs2),
    io:format("Reread non-acked messages done~n", []),
    %% and now fetch the rest
    Seqs3 = [begin
                 Remaining = Total - N,
                 {Message, _TSize, false, SeqId, Remaining} =
                     rabbit_disk_queue:deliver(q),
                 ok = rdq_match_message(Message, N, Msg, 256),
                 SeqId
             end || N <- lists:seq(1 + Half,Total)],
    rabbit_disk_queue:tx_commit(q, [], Seqs3),
    io:format("Read second half done~n", []),
    empty = rabbit_disk_queue:deliver(q),
    rdq_stop(),
    passed.

rdq_test_redeliver() ->
    rdq_virgin(),
    rdq_start(),
    Msg = <<0:(8*256)>>,
    Total = 1000,
    Half = round(Total/2),
    All = lists:seq(1,Total),
    [rabbit_disk_queue:tx_publish(rdq_message(N, Msg)) || N <- All],
    rabbit_disk_queue:tx_commit(q, All, []),
    io:format("Publish done~n", []),
    %% deliver first half
    Seqs = [begin
                Remaining = Total - N,
                {Message, _TSize, false, SeqId, Remaining} =
                    rabbit_disk_queue:deliver(q),
                ok = rdq_match_message(Message, N, Msg, 256),
                SeqId
            end || N <- lists:seq(1,Half)],
    io:format("Deliver first half done~n", []),
    %% now requeue every other message (starting at the _first_)
    %% and ack the other ones
    lists:foldl(fun (SeqId2, true) ->
                        rabbit_disk_queue:requeue(q, [SeqId2]),
                        false;
                    (SeqId2, false) ->
                        rabbit_disk_queue:ack(q, [SeqId2]),
                        true
                end, true, Seqs),
    rabbit_disk_queue:tx_commit(q, [], []),
    io:format("Redeliver and acking done~n", []),
    %% we should now get the 2nd half in order, followed by
    %% every-other-from-the-first-half
    Seqs2 = [begin
                 Remaining = round(Total - N + (Half/2)),
                 {Message, _TSize, false, SeqId, Remaining} =
                     rabbit_disk_queue:deliver(q),
                 ok = rdq_match_message(Message, N, Msg, 256),
                 SeqId
             end || N <- lists:seq(1+Half, Total)],
    rabbit_disk_queue:tx_commit(q, [], Seqs2),
    Seqs3 = [begin
                 Remaining = round((Half - N) / 2) - 1,
                 {Message, _TSize, true, SeqId, Remaining} =
                     rabbit_disk_queue:deliver(q),
                 ok = rdq_match_message(Message, N, Msg, 256),
                 SeqId
             end || N <- lists:seq(1, Half, 2)],
    rabbit_disk_queue:tx_commit(q, [], Seqs3),
    empty = rabbit_disk_queue:deliver(q),
    rdq_stop(),
    passed.

rdq_test_purge() ->
    rdq_virgin(),
    rdq_start(),
    Msg = <<0:(8*256)>>,
    Total = 1000,
    Half = round(Total/2),
    All = lists:seq(1,Total),
    [rabbit_disk_queue:tx_publish(rdq_message(N, Msg)) || N <- All],
    rabbit_disk_queue:tx_commit(q, All, []),
    io:format("Publish done~n", []),
    %% deliver first half
    Seqs = [begin
                Remaining = Total - N,
                {Message, _TSize, false, SeqId, Remaining} =
                    rabbit_disk_queue:deliver(q),
                ok = rdq_match_message(Message, N, Msg, 256),
                SeqId
            end || N <- lists:seq(1,Half)],
    io:format("Deliver first half done~n", []),
    rabbit_disk_queue:purge(q),
    io:format("Purge done~n", []),
    rabbit_disk_queue:tx_commit(q, [], Seqs),
    io:format("Ack first half done~n", []),
    empty = rabbit_disk_queue:deliver(q),
    rdq_stop(),
    passed.    

rdq_test_dump_queue() ->
    rdq_virgin(),
    rdq_start(),
    Msg = <<0:(8*256)>>,
    Total = 1000,
    All = lists:seq(1,Total),
    [rabbit_disk_queue:tx_publish(rdq_message(N, Msg)) || N <- All],
    rabbit_disk_queue:tx_commit(q, All, []),
    io:format("Publish done~n", []),
    QList = [begin Message = rdq_message(N, Msg),
                   Size = size(term_to_binary(Message)),
                   {Message, Size, false, {N, (N-1)}, (N-1)}
             end || N <- All],
    QList = rabbit_disk_queue:dump_queue(q),
    rdq_stop(),
    io:format("dump ok undelivered~n", []),
    rdq_start(),
    lists:foreach(
      fun (N) ->
              Remaining = Total - N,
              {Message, _TSize, false, _SeqId, Remaining} =
                  rabbit_disk_queue:deliver(q),
              ok = rdq_match_message(Message, N, Msg, 256)
      end, All),
    [] = rabbit_disk_queue:dump_queue(q),
    rdq_stop(),
    io:format("dump ok post delivery~n", []),
    rdq_start(),
    QList2 = [begin Message = rdq_message(N, Msg),
                    Size = size(term_to_binary(Message)),
                    {Message, Size, true, {N, (N-1)}, (N-1)}
              end || N <- All],
    QList2 = rabbit_disk_queue:dump_queue(q),
    io:format("dump ok post delivery + restart~n", []),
    rdq_stop(),
    passed.

rdq_test_mixed_queue_modes() ->
    rdq_virgin(),
    rdq_start(),
    Payload = <<0:(8*256)>>,
    {ok, MS} = rabbit_mixed_queue:init(q, true, mixed),
    MS2 = lists:foldl(
            fun (_N, MS1) ->
                    Msg = rabbit_basic:message(x, <<>>, <<>>, Payload),
                    {ok, MS1a} = rabbit_mixed_queue:publish(Msg, MS1),
                    MS1a
            end, MS, lists:seq(1,10)),
    MS4 = lists:foldl(
            fun (_N, MS3) ->
                    Msg = (rabbit_basic:message(x, <<>>, <<>>, Payload))
                        #basic_message { is_persistent = true },
                    {ok, MS3a} = rabbit_mixed_queue:publish(Msg, MS3),
                    MS3a
            end, MS2, lists:seq(1,10)),
    MS6 = lists:foldl(
            fun (_N, MS5) ->
                    Msg = rabbit_basic:message(x, <<>>, <<>>, Payload),
                    {ok, MS5a} = rabbit_mixed_queue:publish(Msg, MS5),
                    MS5a
            end, MS4, lists:seq(1,10)),
    30 = rabbit_mixed_queue:length(MS6),
    io:format("Published a mixture of messages~n"),
    {ok, MS7} = rabbit_mixed_queue:to_disk_only_mode([], MS6),
    30 = rabbit_mixed_queue:length(MS7),
    io:format("Converted to disk only mode~n"),
    {ok, MS8} = rabbit_mixed_queue:to_mixed_mode([], MS7),
    30 = rabbit_mixed_queue:length(MS8),
    io:format("Converted to mixed mode~n"),
    MS10 =
        lists:foldl(
          fun (N, MS9) ->
                  Rem = 30 - N,
                  {{#basic_message { is_persistent = false },
                    false, _AckTag, Rem},
                   MS9a} = rabbit_mixed_queue:deliver(MS9),
                  MS9a
          end, MS8, lists:seq(1,10)),
    20 = rabbit_mixed_queue:length(MS10),
    io:format("Delivered initial non persistent messages~n"),
    {ok, MS11} = rabbit_mixed_queue:to_disk_only_mode([], MS10),
    20 = rabbit_mixed_queue:length(MS11),
    io:format("Converted to disk only mode~n"),
    rdq_stop(),
    rdq_start(),
    {ok, MS12} = rabbit_mixed_queue:init(q, true, mixed),
    10 = rabbit_mixed_queue:length(MS12),
    io:format("Recovered queue~n"),
    {MS14, AckTags} =
        lists:foldl(
          fun (N, {MS13, AcksAcc}) ->
                  Rem = 10 - N,
                  {{#basic_message { is_persistent = true },
                    false, AckTag, Rem},
                   MS13a} = rabbit_mixed_queue:deliver(MS13),
                  {MS13a, [AckTag | AcksAcc]}
          end, {MS12, []}, lists:seq(1,10)),
    0 = rabbit_mixed_queue:length(MS14),
    {ok, MS15} = rabbit_mixed_queue:ack(AckTags, MS14),
    io:format("Delivered and acked all messages~n"),
    {ok, MS16} = rabbit_mixed_queue:to_disk_only_mode([], MS15),
    0 = rabbit_mixed_queue:length(MS16),
    io:format("Converted to disk only mode~n"),
    rdq_stop(),
    rdq_start(),
    {ok, MS17} = rabbit_mixed_queue:init(q, true, mixed),
    0 = rabbit_mixed_queue:length(MS17),
    io:format("Recovered queue~n"),
    rdq_stop(),
    passed.

rdq_test_mode_conversion_mid_txn() ->
    Payload = <<0:(8*256)>>,
    MsgIdsA = lists:seq(0,9),
    MsgsA = [ rabbit_basic:message(x, <<>>, <<>>, Payload, MsgId,
                                   (0 == MsgId rem 2))
            || MsgId <- MsgIdsA ],
    MsgIdsB = lists:seq(10,20),
    MsgsB = [ rabbit_basic:message(x, <<>>, <<>>, Payload, MsgId,
                                   (0 == MsgId rem 2))
            || MsgId <- MsgIdsB ],

    rdq_virgin(),
    rdq_start(),
    {ok, MS0} = rabbit_mixed_queue:init(q, true, mixed),
    passed = rdq_tx_publish_mixed_alter_commit_get(
               MS0, MsgsA, MsgsB, fun rabbit_mixed_queue:to_disk_only_mode/2, commit),

    rdq_stop_virgin_start(),
    {ok, MS1} = rabbit_mixed_queue:init(q, true, mixed),
    passed = rdq_tx_publish_mixed_alter_commit_get(
               MS1, MsgsA, MsgsB, fun rabbit_mixed_queue:to_disk_only_mode/2, cancel),


    rdq_stop_virgin_start(),
    {ok, MS2} = rabbit_mixed_queue:init(q, true, disk),
    passed = rdq_tx_publish_mixed_alter_commit_get(
               MS2, MsgsA, MsgsB, fun rabbit_mixed_queue:to_mixed_mode/2, commit),

    rdq_stop_virgin_start(),
    {ok, MS3} = rabbit_mixed_queue:init(q, true, disk),
    passed = rdq_tx_publish_mixed_alter_commit_get(
               MS3, MsgsA, MsgsB, fun rabbit_mixed_queue:to_mixed_mode/2, cancel),

    rdq_stop(),
    passed.

rdq_tx_publish_mixed_alter_commit_get(MS0, MsgsA, MsgsB, ChangeFun, CommitOrCancel) ->
    0 = rabbit_mixed_queue:length(MS0),
    MS2 = lists:foldl(
            fun (Msg, MS1) ->
                    {ok, MS1a} = rabbit_mixed_queue:publish(Msg, MS1),
                    MS1a
            end, MS0, MsgsA),
    Len0 = length(MsgsA),
    Len0 = rabbit_mixed_queue:length(MS2),
    MS4 = lists:foldl(
            fun (Msg, MS3) ->
                    {ok, MS3a} = rabbit_mixed_queue:tx_publish(Msg, MS3),
                    MS3a
            end, MS2, MsgsB),
    Len0 = rabbit_mixed_queue:length(MS4),
    {ok, MS5} = ChangeFun(MsgsB, MS4),
    Len0 = rabbit_mixed_queue:length(MS5),
    {ok, MS9} =
        case CommitOrCancel of
            commit ->
                {ok, MS6} = rabbit_mixed_queue:tx_commit(MsgsB, [], MS5),
                Len1 = Len0 + length(MsgsB),
                Len1 = rabbit_mixed_queue:length(MS6),
                {AckTags, MS8} =
                    lists:foldl(
                      fun (Msg, {Acc, MS7}) ->
                              Rem = Len1 - (Msg #basic_message.guid) - 1,
                              {{Msg, false, AckTag, Rem}, MS7a} =
                                  rabbit_mixed_queue:deliver(MS7),
                              {[AckTag | Acc], MS7a}
                      end, {[], MS6}, MsgsA ++ MsgsB),
                0 = rabbit_mixed_queue:length(MS8),
                rabbit_mixed_queue:ack(lists:reverse(AckTags), MS8);
            cancel ->
                {ok, MS6} = rabbit_mixed_queue:tx_cancel(MsgsB, MS5),
                Len0 = rabbit_mixed_queue:length(MS6),
                {AckTags, MS8} =
                    lists:foldl(
                      fun (Msg, {Acc, MS7}) ->
                              Rem = Len0 - (Msg #basic_message.guid) - 1,
                              {{Msg, false, AckTag, Rem}, MS7a} =
                                  rabbit_mixed_queue:deliver(MS7),
                              {[AckTag | Acc], MS7a}
                      end, {[], MS6}, MsgsA),
                0 = rabbit_mixed_queue:length(MS8),
                rabbit_mixed_queue:ack(lists:reverse(AckTags), MS8)
        end,
    0 = rabbit_mixed_queue:length(MS9),
    passed.

rdq_time_commands(Funcs) ->
    lists:foreach(fun (F) -> F() end, Funcs).

rdq_virgin() ->
    {Micros, {ok, _}} =
        timer:tc(rabbit_disk_queue, start_link, []),
    ok = rabbit_disk_queue:stop_and_obliterate(),
    timer:sleep(1000),
    Micros.

rdq_start() ->
    {ok, _} = rabbit_disk_queue:start_link(),
    ok = rabbit_disk_queue:to_ram_disk_mode(),
    ok.

rdq_stop() ->
    rabbit_disk_queue:stop(),
    timer:sleep(1000).

rdq_stop_virgin_start() ->
    rdq_stop(),
    rdq_virgin(),
    rdq_start().
