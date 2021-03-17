-module(rabbit_prelaunch_errors).

-export([format_error/1,
         format_exception/3,
         log_error/1,
         log_exception/3]).

-define(BOOT_FAILED_HEADER,
        "\n"
        "BOOT FAILED\n"
        "===========\n").

-define(BOOT_FAILED_FOOTER,
        "\n").

log_error(Error) ->
    Message = format_error(Error),
    log_message(Message).

format_error({error, {duplicate_node_name, NodeName, NodeHost}}) ->
    rabbit_misc:format(
      "ERROR: node with name ~p is already running on host ~p",
      [NodeName, NodeHost]);
format_error({error, {epmd_error, NodeHost, EpmdReason}}) ->
    rabbit_misc:format(
      "ERROR: epmd error for host ~s: ~s",
      [NodeHost, rabbit_misc:format_inet_error(EpmdReason)]);
format_error({error, {invalid_dist_port_range, DistTcpPort}}) ->
    rabbit_misc:format(
      "Invalid Erlang distribution TCP port: ~b", [DistTcpPort]);
format_error({error, {dist_port_already_used, Port, not_erlang, Host}}) ->
    rabbit_misc:format(
      "ERROR: distribution port ~b in use on host ~s "
      "(by non-Erlang process?)", [Port, Host]);
format_error({error, {dist_port_already_used, Port, Name, Host}}) ->
    rabbit_misc:format(
      "ERROR: distribution port ~b in use by another node: ~s@~s", [Port, Name, Host]);
format_error({error, {erlang_dist_running_with_unexpected_nodename,
                      Unexpected, Node}}) ->
    rabbit_misc:format(
      "Erlang distribution running with another node name (~s) "
      "than the configured one (~s)",
      [Unexpected, Node]);
format_error({bad_config_entry_decoder, missing_passphrase}) ->
    rabbit_misc:format(
      "Missing passphrase or missing passphrase read method in "
      "`config_entry_decoder`", []);
format_error({config_decryption_error, {key, Key}, _Msg}) ->
    rabbit_misc:format(
      "Error while decrypting key '~p'. Please check encrypted value, "
      "passphrase, and encryption configuration~n",
      [Key]);
format_error({error, {timeout_waiting_for_tables, AllNodes, _}}) ->
    Suffix =
    "~nBACKGROUND~n==========~n~n"
    "This cluster node was shut down while other nodes were still running.~n"
    "To avoid losing data, you should start the other nodes first, then~n"
    "start this one. To force this node to start, first invoke~n"
    "\"rabbitmqctl force_boot\". If you do so, any changes made on other~n"
    "cluster nodes after this one was shut down may be lost.",
    {Message, Nodes} =
    case AllNodes -- [node()] of
        [] -> {rabbit_misc:format(
                 "Timeout contacting cluster nodes. Since RabbitMQ was"
                 " shut down forcefully~nit cannot determine which nodes"
                 " are timing out.~n" ++ Suffix, []),
               []};
        Ns -> {rabbit_misc:format(
                 "Timeout contacting cluster nodes: ~p.~n" ++ Suffix,
                 [Ns]),
               Ns}
    end,
    Message ++ "\n" ++ rabbit_nodes_common:diagnostics(Nodes);
format_error({error, {cannot_log_to_file, unknown, Reason}}) ->
    rabbit_misc:format(
      "failed to initialised logger: ~p~n",
      [Reason]);
format_error({error, {cannot_log_to_file, LogFile,
                      {cannot_create_parent_dirs, _, Reason}}}) ->
    rabbit_misc:format(
      "failed to create parent directory for log file at '~s', reason: ~s~n",
      [LogFile, file:format_error(Reason)]);
format_error({error, {cannot_log_to_file, LogFile, Reason}}) ->
    rabbit_misc:format(
      "failed to open log file at '~s', reason: ~s",
      [LogFile, file:format_error(Reason)]);
format_error(Error) ->
    rabbit_misc:format("Error during startup: ~p", [Error]).

log_exception(Class, Exception, Stacktrace) ->
    Message = format_exception(Class, Exception, Stacktrace),
    log_message(Message).

format_exception(Class, Exception, Stacktrace) ->
    rabbit_misc:format(
      "Exception during startup:~n~s",
      [lager:pr_stacktrace(Stacktrace, {Class, Exception})]).

log_message(Message) ->
    Lines = string:split(
              ?BOOT_FAILED_HEADER ++
              Message ++
              ?BOOT_FAILED_FOOTER,
              [$\n],
              all),
    lists:foreach(
      fun(Line) ->
              _ = rabbit_log_prelaunch:error("~s", [Line]),
              io:format(standard_error, "~s~n", [Line])
      end, Lines),
    timer:sleep(1000),
    ok.
