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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial Technologies
%%   LLC., and Rabbit Technologies Ltd. are Copyright (C) 2007-2008
%%   LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_tests).

-export([all_tests/0, test_parsing/0]).

-import(lists).

test_content_prop_roundtrip(Datum, Binary) ->
    Types =  [element(1, E) || E <- Datum],
    Values = [element(2, E) || E <- Datum],
    Values = rabbit_binary_parser:parse_properties(Types, Binary), %% assertion
    Binary = rabbit_binary_generator:encode_properties(Types, Values). %% assertion

all_tests() ->
    passed = test_parsing(),
    passed = test_topic_matching(),
    passed = test_app_management(),
    passed = test_cluster_management(),
    passed = test_user_management(),
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

    %% attempt to convert a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    {error, {cannot_convert_disk_node_to_ram_node, _}} =
        control_action(cluster, ["invalid1@invalid",
                                 "invalid2@invalid"]),

    %% attempt to join a non-existing cluster as a ram node
    ok = control_action(reset, []),
    {error, {unable_to_contact_cluster_nodes, _}} =
        control_action(cluster, ["invalid1@invalid",
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

    %% attempt to convert a disk node into a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [NodeS]),
    {error, {unable_to_join_cluster, _, _}} =
        control_action(cluster, [SecondaryNodeS]),

    %% join cluster as a ram node
    ok = control_action(reset, []),
    ok = control_action(cluster, [SecondaryNodeS, "invalid1@invalid"]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% change cluster config while remaining in same cluster
    ok = control_action(cluster, ["invalid2@invalid", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),

    %% attempt to join non-existing cluster as a ram node
    {error, _} = control_action(cluster, ["invalid1@invalid",
                                          "invalid2@invalid"]),

    %% turn ram node into disk node
    ok = control_action(cluster, [SecondaryNodeS, NodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    
    %% attempt to convert a disk node into a ram node
    {error, {cannot_convert_disk_node_to_ram_node, _}} =
        control_action(cluster, ["invalid1@invalid",
                                 "invalid2@invalid"]),

    %% turn a disk node into a ram node
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
    ok = control_action(force_reset, []),

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
        control_action(map_user_vhost, ["foo", "/"]),
    {error, {no_such_user, _}} =
        control_action(unmap_user_vhost, ["foo", "/"]),
    {error, {no_such_user, _}} =
        control_action(list_user_vhosts, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(set_permissions, ["foo", "/", "/data"]),
    {error, {no_such_user, _}} =
        control_action(list_permissions, ["foo", "/"]),
    {error, {no_such_vhost, _}} =
        control_action(map_user_vhost, ["guest", "/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(unmap_user_vhost, ["guest", "/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(list_vhost_users, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(set_permissions, ["guest", "/testhost", "/data"]),
    {error, {no_such_vhost, _}} =
        control_action(list_permissions, ["guest", "/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(add_realm, ["/testhost", "/data/test"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_realm, ["/testhost", "/data/test"]),
    {error, {no_such_vhost, _}} =
        control_action(list_realms, ["/testhost"]),
    {error, {no_such_realm, _}} =
        control_action(set_permissions, ["guest", "/", "/data/test"]),
    {error, {no_such_realm, _}} =
        control_action(delete_realm, ["/", "/data/test"]),

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
    ok = control_action(map_user_vhost, ["foo", "/testhost"]),
    ok = control_action(map_user_vhost, ["foo", "/testhost"]),
    ok = control_action(list_user_vhosts, ["foo"]),

    %% realm creation
    ok = control_action(add_realm, ["/testhost", "/data/test"]),
    {error, {realm_already_exists, _}} =
        control_action(add_realm, ["/testhost", "/data/test"]),
    ok = control_action(list_realms, ["/testhost"]),

    %% user permissions
    ok = control_action(set_permissions,
                        ["foo", "/testhost", "/data/test",
                         "passive", "active", "write", "read"]),
    ok = control_action(list_permissions, ["foo", "/testhost"]),
    ok = control_action(set_permissions,
                        ["foo", "/testhost", "/data/test", "all"]),
    ok = control_action(set_permissions,
                        ["foo", "/testhost", "/data/test"]),
    {error, not_mapped_to_vhost} =
        control_action(set_permissions,
                       ["guest", "/testhost", "/data/test"]),
    {error, not_mapped_to_vhost} =
        control_action(list_permissions, ["guest", "/testhost"]),

    %% realm deletion
    ok = control_action(delete_realm, ["/testhost", "/data/test"]),
    {error, {no_such_realm, _}} =
        control_action(delete_realm, ["/testhost", "/data/test"]),

    %% user/vhost unmapping
    ok = control_action(unmap_user_vhost, ["foo", "/testhost"]),
    ok = control_action(unmap_user_vhost, ["foo", "/testhost"]),

    %% vhost deletion
    ok = control_action(delete_vhost, ["/testhost"]),
    {error, {no_such_vhost, _}} =
        control_action(delete_vhost, ["/testhost"]),

    %% deleting a populated vhost
    ok = control_action(add_vhost, ["/testhost"]),
    ok = control_action(add_realm, ["/testhost", "/data/test"]),
    ok = control_action(map_user_vhost, ["foo", "/testhost"]),
    ok = control_action(set_permissions,
                        ["foo", "/testhost", "/data/test", "all"]),
    _ = rabbit_amqqueue:declare(
          rabbit_misc:r(<<"/testhost">>, realm, <<"/data/test">>),
          <<"bar">>, true, false, []),
    ok = control_action(delete_vhost, ["/testhost"]),

    %% user deletion
    ok = control_action(delete_user, ["foo"]),
    {error, {no_such_user, _}} =
        control_action(delete_user, ["foo"]),

    passed.

control_action(Command, Args) -> control_action(Command, node(), Args).

control_action(Command, Node, Args) ->
    case catch rabbit_control:action(Command, Node, Args) of
        ok ->
            io:format("done.~n"),
            ok;
        Other -> 
            io:format("failed.~n"),
            Other
    end.
