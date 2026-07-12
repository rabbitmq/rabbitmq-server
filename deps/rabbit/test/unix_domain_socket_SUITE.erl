-module(unix_domain_socket_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [test_unix_domain_socket,
     test_uds_publish_consume,
     test_uds_connection_failure].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, rabbit_ct_broker_helpers:setup_steps() ++ rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config, rabbit_ct_client_helpers:teardown_steps() ++ rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Name = case Testcase of
               test_unix_domain_socket -> "uds_test.sock";
               test_uds_publish_consume -> "uds_pubsub.sock";
               test_uds_connection_failure -> "uds_failure.sock";
               _ -> atom_to_list(Testcase) ++ ".sock"
           end,
    SocketPath = uds_socket_path(Config, Name),
    [{uds_socket_path, SocketPath} | Config].

end_per_testcase(Testcase, Config) ->
    case ?config(uds_socket_path, Config) of
        undefined -> ok;
        SocketPath ->
            _ = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
            _ = file:delete(SocketPath)
    end,
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

uds_socket_path(Config, Name) ->
    PrivDir = ?config(priv_dir, Config),
    Path = filename:join(PrivDir, Name),
    case length(Path) > 104 of
        true ->
            %% Path exceeds sun_path limits (e.g. deeply nested CI or WSL environments).
            %% Fall back to /tmp with a unique identifier to avoid collisions.
            UniqueId = integer_to_list(erlang:unique_integer([positive])),
            filename:join("/tmp", "rmq-uds-" ++ UniqueId ++ "-" ++ Name);
        false ->
            Path
    end.

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

test_unix_domain_socket(Config) ->
    %% Path for our temporary unix domain socket
    SocketPath = ?config(uds_socket_path, Config),

    %% Ensure it doesn't already exist from a crashed run
    file:delete(SocketPath),

    %% Dynamically start a new listener on the local unix socket
    ok = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),

    %% Let the listener start up
    timer:sleep(500),

    %% Connect using the Erlang AMQP Client over the Unix socket
    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),

    %% Perform a basic AMQP operation (declare a queue) to verify the socket works
    Queue = <<"test_uds_queue">>,
    Declare = #'queue.declare'{queue = Queue, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),

    %% Clean up
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn),

    %% Stop the listener
    ok = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
    file:delete(SocketPath),
    ok.

test_uds_publish_consume(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),
    timer:sleep(500),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),

    Queue = <<"test_uds_pubsub_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable = true}),

    %% Publish a message
    Payload = <<"Hello over Unix Sockets">>,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Queue},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}),

    %% Consume the message
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:call(Channel, #'basic.consume'{queue = Queue, no_ack = true}),
    receive
        {#'basic.deliver'{consumer_tag = CTag}, #amqp_msg{payload = MsgPayload}} ->
            Payload = MsgPayload
    after 5000 ->
        ct:fail(message_not_received)
    end,

    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn),
    ok = rabbit_ct_broker_helpers:rpc(Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
    file:delete(SocketPath),
    ok.

test_uds_connection_failure(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    case amqp_connection:start(ConnParams) of
        {error, _} -> ok;
        Other -> ct:fail({expected_error, Other})
    end,
    ok.

