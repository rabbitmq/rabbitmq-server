%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(amqp10_dynamic_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          simple,
          % set_properties,
          % headers,
          % exchange,
          % restart,
          change_definition,
          autodelete
          % validation,
          % security_validation,
          % get_connection_name
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    application:stop(amqp10_client),
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

simple(Config) ->
    with_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(
                Config,
                <<"test">>, [{<<"src-protocol">>, <<"amqp10">>},
                             {<<"src-address">>,  <<"src">>},
                             {<<"dest-protocol">>, <<"amqp10">>},
                             {<<"dest-address">>, <<"dest">>},
                             {<<"dest-add-forward-headers">>, true},
                             {<<"dest-add-timestamp-header">>, true},
                             {<<"dest-application-properties">>,
                              [{<<"app-prop-key">>, <<"app-prop-value">>}]},
                             {<<"dest-properties">>,
                              [{<<"user_id">>, <<"guest">>}]},
                             {<<"dest-message-annotations">>,
                              [{<<"message-ann-key">>, <<"message-ann-value">>}]}
                            ]),
              Msg = publish_expect(Sess, <<"src">>, <<"dest">>, <<"tag1">>,
                                   <<"hello">>),
              ?assertMatch(#{user_id := <<"guest">>,
                             creation_time := _}, amqp10_msg:properties(Msg)),
              ?assertMatch(#{<<"shovel-name">> := <<"test">>,
                             <<"shovel-type">> := <<"dynamic">>,
                             <<"shovelled-by">> := _,
                             <<"app-prop-key">> := <<"app-prop-value">>
                            }, amqp10_msg:application_properties(Msg)),
              ?assertMatch(#{<<"message-ann-key">> := <<"message-ann-value">>},
                           amqp10_msg:message_annotations(Msg))
      end).


change_definition(Config) ->
    with_session(Config,
      fun (Sess) ->
              shovel_test_utils:set_param(Config, <<"test">>,
                                          [{<<"src-address">>,  <<"src">>},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, <<"dest">>}]),
              publish_expect(Sess, <<"src">>, <<"dest">>, <<"tag2">>,<<"hello">>),
              shovel_test_utils:set_param(Config, <<"test">>,
                                          [{<<"src-address">>,  <<"src">>},
                                           {<<"src-protocol">>, <<"amqp10">>},
                                           {<<"dest-protocol">>, <<"amqp10">>},
                                           {<<"dest-address">>, <<"dest2">>}]),
              publish_expect(Sess, <<"src">>, <<"dest2">>, <<"tag3">>, <<"hello">>),
              expect_empty(Sess, <<"dest">>),
              shovel_test_utils:clear_param(Config, <<"test">>),
              publish_expect(Sess, <<"src">>, <<"src">>, <<"tag4">>, <<"hello2">>),
              expect_empty(Sess, <<"dest">>),
              expect_empty(Sess, <<"dest2">>)
      end).

autodelete(Config) ->
    autodelete_case(Config, {<<"on-confirm">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"on-publish">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"no-ack">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"on-publish">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"on-publish">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"on-publish">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"no-ack">>, 50, 50, 50}),
    % autodelete_case(Config, {<<"on-confirm">>, <<"queue-length">>,  0, 100}),
    % autodelete_case(Config, {<<"on-publish">>, <<"queue-length">>,  0, 100}),
    % autodelete_case(Config, {<<"on-publish">>, 50,                 50,  50}),
    % %% no-ack is not compatible with explicit count
    % autodelete_case(Config, {<<"no-ack">>,     <<"queue-length">>,  0, 100}),
    ok.

autodelete_case(Config, Args) ->
    with_session(Config, autodelete_do(Config, Args)).

autodelete_do(Config, {AckMode, After, ExpSrc, ExpDest}) ->
    fun (Session) ->
            % amqp_channel:call(Ch, #'confirm.select'{}),
            % amqp_channel:call(Ch, #'queue.declare'{queue = <<"src">>}),
            publish_count(Session, <<"src">>, <<"hello">>, 100),
            shovel_test_utils:set_param_nowait(
              Config,
              <<"test">>, [{<<"src-address">>,    <<"src">>},
                           {<<"src-protocol">>,   <<"amqp10">>},
                           {<<"src-delete-after">>, After},
                           {<<"dest-address">>,   <<"dest">>},
                           {<<"dest-protocol">>,   <<"amqp10">>},
                           {<<"src-prefetch-count">>, 50},
                           {<<"ack-mode">>,     AckMode}
                          ]),
            await_autodelete(Config, <<"test">>),
            expect_count(Session, <<"dest">>, <<"hello">>, ExpDest),
            expect_count(Session, <<"src">>, <<"hello">>, ExpSrc)
    end.


% security_validation(Config) ->
%     ok = rabbit_ct_broker_helpers:rpc(Config, 0,
%       ?MODULE, security_validation_add_user, []),

%     Qs = [{<<"src-queue">>, <<"test">>},
%           {<<"dest-queue">>, <<"test2">>}],

%     A = lookup_user(Config, <<"a">>),
%     valid_param(Config, [{<<"src-uri">>,  <<"amqp:///a">>},
%                  {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),
%     invalid_param(Config,
%                   [{<<"src-uri">>,  <<"amqp:///a">>},
%                    {<<"dest-uri">>, <<"amqp:///b">>} | Qs], A),
%     invalid_param(Config,
%                   [{<<"src-uri">>,  <<"amqp:///b">>},
%                    {<<"dest-uri">>, <<"amqp:///a">>} | Qs], A),

%     ok = rabbit_ct_broker_helpers:rpc(Config, 0,
%       ?MODULE, security_validation_remove_user, []),
%     ok.

% security_validation_add_user() ->
%     [begin
%          rabbit_vhost:add(U, <<"acting-user">>),
%          rabbit_auth_backend_internal:add_user(U, <<>>, <<"acting-user">>),
%          rabbit_auth_backend_internal:set_permissions(
%            U, U, <<".*">>, <<".*">>, <<".*">>, <<"acting-user">>)
%      end || U <- [<<"a">>, <<"b">>]],
%     ok.

% security_validation_remove_user() ->
%     [begin
%          rabbit_vhost:delete(U, <<"acting-user">>),
%          rabbit_auth_backend_internal:delete_user(U, <<"acting-user">>)
%      end || U <- [<<"a">>, <<"b">>]],
%     ok.

% get_connection_name(_Config) ->
%     <<"Shovel static_shovel_name_as_atom">> = rabbit_shovel_worker:get_connection_name(static_shovel_name_as_atom),
%     <<"Shovel dynamic_shovel_name_as_binary">> = rabbit_shovel_worker:get_connection_name({<<"/">>, <<"dynamic_shovel_name_as_binary">>}),
%     <<"Shovel">> = rabbit_shovel_worker:get_connection_name({<<"/">>, {unexpected, tuple}}),
%     <<"Shovel">> = rabbit_shovel_worker:get_connection_name({one, two, three}),
%     <<"Shovel">> = rabbit_shovel_worker:get_connection_name(<<"anything else">>).


%%----------------------------------------------------------------------------

with_session(Config, Fun) ->
    Hostname = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, Conn} = amqp10_client:open_connection(Hostname, Port),
    {ok, Sess} = amqp10_client:begin_session(Conn),
    Fun(Sess),
    amqp10_client:close_connection(Conn),
    cleanup(Config),
    ok.

publish(Sender, Tag, Payload) when is_binary(Payload) ->
    Headers = #{durable => true},
    Msg = amqp10_msg:set_headers(Headers,
                                 amqp10_msg:new(Tag, Payload, false)),
    ok = amqp10_client:send_msg(Sender, Msg),
    receive
        {amqp10_disposition, {accepted, Tag}} -> ok
    after 3000 ->
              exit(publish_disposition_not_received)
    end.

publish_expect(Session, Source, Dest, Tag, Payload) ->
    LinkName = <<"dynamic-sender-", Dest/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName,
                                                    Source),
    ok = await_amqp10_event(link, Sender, attached),
    publish(Sender, Tag, Payload),
    amqp10_client:detach_link(Sender),
    expect_one(Session, Dest, Payload).

await_amqp10_event(On, Ref, Evt) ->
    receive
        {amqp10_event, {On, Ref, Evt}} -> ok
    after 5000 ->
          exit({amqp10_event_timeout, On, Ref, Evt})
    end.

expect_one(Session, Dest, Payload) ->
    LinkName = <<"dynamic-receiver-", Dest/binary>>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, LinkName,
                                                        Dest),
    ok = amqp10_client:flow_link_credit(Receiver, 5, never),
    expect(Receiver, Payload).

expect(Receiver, Payload) ->
    receive
        {amqp10_msg, Receiver, InMsg} ->
            [Payload] = amqp10_msg:body(InMsg),
            % #{content_type := ?UNSHOVELLED} = amqp10_msg:properties(InMsg),
            % #{durable := true} = amqp10_msg:headers(InMsg),
            amqp10_client:detach_link(Receiver),
            InMsg
    after 4000 ->
              throw(timeout_in_expect_waiting_for_delivery)
    end.

expect_empty(Session, Dest) ->
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"dynamic-receiver">>,
                                                        Dest),
    % probably good enough given we don't currently have a means of
    % echoing flow state
    {error, timeout} = amqp10_client:get_msg(Receiver, 250),
    amqp10_client:detach_link(Receiver).

publish_count(Session, Address, Payload, Count) ->
    LinkName = <<"dynamic-sender-", Address/binary>>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session, LinkName,
                                                    Address),
    ok = await_amqp10_event(link, Sender, attached),
    [begin

         Tag = rabbit_data_coercion:to_binary(I),
         publish(Sender, Tag, Payload)
     end || I <- lists:seq(1, Count)],
     amqp10_client:detach_link(Sender).

expect_count(Session, Address, Payload, Count) ->
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                                                        <<"dynamic-receiver">>,
                                                        Address),
    [begin
         expect(Receiver, Payload)
     end || _ <- lists:seq(1, Count)],
    expect_empty(Session, Address),
    amqp10_client:detach_link(Receiver).


invalid_param(Config, Value, User) ->
    {error_string, _} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_runtime_parameters, set,
      [<<"/">>, <<"shovel">>, <<"invalid">>, Value, User]).

valid_param(Config, Value, User) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, valid_param1, [Config, Value, User]).

valid_param1(_Config, Value, User) ->
    ok = rabbit_runtime_parameters:set(
           <<"/">>, <<"shovel">>, <<"a">>, Value, User),
    ok = rabbit_runtime_parameters:clear(<<"/">>, <<"shovel">>, <<"a">>, <<"acting-user">>).

invalid_param(Config, Value) -> invalid_param(Config, Value, none).
valid_param(Config, Value) -> valid_param(Config, Value, none).

lookup_user(Config, Name) ->
    {ok, User} = rabbit_ct_broker_helpers:rpc(Config, 0,
      rabbit_access_control, check_user_login, [Name, []]),
    User.

cleanup(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, cleanup1, [Config]).

cleanup1(_Config) ->
    [rabbit_runtime_parameters:clear(rabbit_misc:pget(vhost, P),
                                     rabbit_misc:pget(component, P),
                                     rabbit_misc:pget(name, P),
                                     <<"acting-user">>) ||
        P <- rabbit_runtime_parameters:list()],
    [rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>)
     || Q <- rabbit_amqqueue:list()].

await_autodelete(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, await_autodelete1, [Config, Name], 10000).

await_autodelete1(_Config, Name) ->
    shovel_test_utils:await(
      fun () -> not lists:member(Name, shovels_from_parameters()) end),
    shovel_test_utils:await(
      fun () ->
              not lists:member(Name,
                               shovel_test_utils:shovels_from_status())
      end).

shovels_from_parameters() ->
    L = rabbit_runtime_parameters:list(<<"/">>, <<"shovel">>),
    [rabbit_misc:pget(name, Shovel) || Shovel <- L].
