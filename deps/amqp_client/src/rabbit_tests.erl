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
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_tests).

-export([all_tests/0, test_parsing/0]).

-import(lists).

test_roundtrip(Datum, Binary) ->
    Types = lists:map(fun (E) -> element(1, E) end, Datum),
    Values = lists:map(fun (E) -> element(2, E) end, Datum),
    {ParsedValues, <<>>} = rabbit_binary_parser:parse_binary(Types, Binary),
    ParsedValues = Values, %% assertion
    Binary = list_to_binary(rabbit_binary_generator:encode_method_fields(Datum)). %% assertion

test_content_prop_roundtrip(Datum, Binary) ->
    Types = lists:map(fun (E) -> element(1, E) end, Datum),
    Values = lists:map(fun (E) -> element(2, E) end, Datum),
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
    passed = test_short_binary(),
    passed = test_long_binary(),
    passed = test_bits(),
    passed = test_content_properties(),
    passed.

test_short_binary() ->
    syntax_error = rabbit_binary_parser:parse_binary([octet], <<>>),
    passed.

test_long_binary() ->
    {[123], <<234>>} = rabbit_binary_parser:parse_binary([octet], <<123, 234>>),
    passed.

test_bits() ->
    test_roundtrip([{octet, 1}, {bit, false}, {bit, true}, {bit, true}, {octet, 2}], <<1, 6, 2>>),
    test_roundtrip([{octet, 1}, {bit, true}, {bit, true}, {bit, false}, {octet, 2}], <<1, 3, 2>>),
    test_roundtrip([{octet, 1}, {bit, true}, {octet, 2}], <<1, 1, 2>>),
    test_roundtrip([{octet, 1}, {bit, false}, {octet, 2}], <<1, 0, 2>>),
    test_roundtrip([{octet, 1},

                    {bit, true}, {bit, false}, {bit, false}, {bit, true},
                    {bit, true}, {bit, false}, {bit, true}, {bit, false},

                    {bit, true}, {bit, false}, {bit, true}, {bit, false},
                    {bit, true}, {bit, false}, {bit, false}, {bit, true},

                    {bit, true}, {bit, false},
                    {octet, 2}],
                   <<1, 16#59, 16#95, 1, 2>>),
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
    {error, _} = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    {error, _} = control_action(stop_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    passed.

test_cluster_management() ->

    % lots of stuff that (mostly) should fail
    {error, _} = control_action(clear_db, []),
    {error, _} = control_action(join_cluster, ["ram", "invalid@invalid"]),
    {error, _} = control_action(leave_cluster, []),
    ok = control_action(stop_app, []),
    {error, node_is_not_clustered} =
        control_action(leave_cluster, []),
    {error, {db_found, _}} =
        control_action(join_cluster, ["ram", "invalid@invalid"]),
    ok = control_action(clear_db, []),
    {error, {db_not_found, _}} =
        control_action(clear_db, []),
    {error, {unable_to_contact_cluster_nodes, _}} =
        control_action(join_cluster, ["ram", "invalid@invalid"]),

    SecondaryNode = rabbit_misc:localnode(hare),
    case net_adm:ping(SecondaryNode) of
        pong -> passed = test_cluster_management2(SecondaryNode);
        pang -> io:format("Skipping clustering tests with node ~p~n",
                          [SecondaryNode])
    end,

    ok = control_action(start_app, []),

    passed.

test_cluster_management2(SecondaryNode) ->
    SecondaryNodeS = atom_to_list(SecondaryNode),
    ok = control_action(join_cluster, ["ram", SecondaryNodeS]),
    {error, {node_is_clustered, _}} =
        control_action(join_cluster, ["ram", SecondaryNodeS]),
    ok = control_action(start_app, []),
    %% NB: this will log an inconsistent_database error, which is harmless
    true = disconnect_node(SecondaryNode),
    pong = net_adm:ping(SecondaryNode),
    ok = control_action(stop_app, []),
    {error, {node_is_clustered, _}} =
        control_action(clear_db, []),
    ok = control_action(stop_app, SecondaryNode, []),
    {error, {unable_to_contact_cluster_nodes, _}} =
        control_action(leave_cluster, []),
    ok = control_action(start_app, SecondaryNode, []),
    ok = control_action(leave_cluster, []),
    ok = control_action(join_cluster, ["disc_only", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(leave_cluster, []),
    ok = control_action(join_cluster, ["disc", SecondaryNodeS]),
    ok = control_action(start_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(leave_cluster, []),

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
