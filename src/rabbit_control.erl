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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_control).
-include("rabbit.hrl").

-export([start/0, stop/0, action/5]).

-define(RPC_TIMEOUT, infinity).

-define(QUIET_OPT, "-q").
-define(NODE_OPT, "-n").
-define(VHOST_OPT, "-p").
-define(SCOPE_OPT, "-s").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start/0 :: () -> no_return()).
-spec(stop/0 :: () -> 'ok').
-spec(action/5 ::
        (atom(), node(), [string()], [{string(), any()}],
         fun ((string(), [any()]) -> 'ok'))
        -> 'ok').
-spec(usage/0 :: () -> no_return()).

-endif.

%%----------------------------------------------------------------------------

start() ->
    {ok, [[NodeStr|_]|_]} = init:get_argument(nodename),
    FullCommand = init:get_plain_arguments(),
    case FullCommand of
        [] -> usage();
        _ -> ok
    end,
    {[Command0 | Args], Opts} =
        rabbit_misc:get_options(
          [{flag, ?QUIET_OPT}, {option, ?NODE_OPT, NodeStr},
           {option, ?VHOST_OPT, "/"}, {option, ?SCOPE_OPT, "client"}],
          FullCommand),
    Opts1 = lists:map(fun({K, V}) ->
                              case K of
                                  ?NODE_OPT -> {?NODE_OPT, rabbit_misc:makenode(V)};
                                  _    -> {K, V}
                              end
                      end, Opts),
    Command = list_to_atom(Command0),
    Quiet = proplists:get_bool(?QUIET_OPT, Opts1),
    Node = proplists:get_value(?NODE_OPT, Opts1),
    Inform = case Quiet of
                 true  -> fun (_Format, _Args1) -> ok end;
                 false -> fun (Format, Args1) ->
                                  io:format(Format ++ " ...~n", Args1)
                          end
             end,
    %% The reason we don't use a try/catch here is that rpc:call turns
    %% thrown errors into normal return values
    case catch action(Command, Node, Args, Opts, Inform) of
        ok ->
            case Quiet of
                true  -> ok;
                false -> io:format("...done.~n")
            end,
            halt();
        {'EXIT', {function_clause, [{?MODULE, action, _} | _]}} ->
            error("invalid command '~s'",
                  [lists:flatten(
                     rabbit_misc:intersperse(
                       " ", [atom_to_list(Command) | Args]))]),
            usage();
        {error, Reason} ->
            error("~p", [Reason]),
            halt(2);
        {badrpc, {'EXIT', Reason}} ->
            error("~p", [Reason]),
            halt(2);
        {badrpc, Reason} ->
            error("unable to connect to node ~w: ~w", [Node, Reason]),
            print_badrpc_diagnostics(Node),
            halt(2);
        Other ->
            error("~p", [Other]),
            halt(2)
    end.

fmt_stderr(Format, Args) -> rabbit_misc:format_stderr(Format ++ "~n", Args).

error(Format, Args) -> fmt_stderr("Error: " ++ Format, Args).

print_badrpc_diagnostics(Node) ->
    fmt_stderr("diagnostics:", []),
    {_NodeName, NodeHost} = rabbit_misc:nodeparts(Node),
    case net_adm:names(NodeHost) of
        {error, EpmdReason} ->
            fmt_stderr("- unable to connect to epmd on ~s: ~w",
                       [NodeHost, EpmdReason]);
        {ok, NamePorts} ->
            fmt_stderr("- nodes and their ports on ~s: ~p",
                       [NodeHost, [{list_to_atom(Name), Port} ||
                                      {Name, Port} <- NamePorts]])
            end,
    fmt_stderr("- current node: ~w", [node()]),
    case init:get_argument(home) of
        {ok, [[Home]]} -> fmt_stderr("- current node home dir: ~s", [Home]);
        Other          -> fmt_stderr("- no current node home dir: ~p", [Other])
    end,
    fmt_stderr("- current node cookie hash: ~s", [rabbit_misc:cookie_hash()]),
    ok.

stop() ->
    ok.

usage() ->
    io:format("~s", [rabbit_ctl_usage:usage()]),
    halt(1).

action(stop, Node, [], _Opts, Inform) ->
    Inform("Stopping and halting node ~p", [Node]),
    call(Node, {rabbit, stop_and_halt, []});

action(stop_app, Node, [], _Opts, Inform) ->
    Inform("Stopping node ~p", [Node]),
    call(Node, {rabbit, stop, []});

action(start_app, Node, [], _Opts, Inform) ->
    Inform("Starting node ~p", [Node]),
    call(Node, {rabbit, start, []});

action(reset, Node, [], _Opts, Inform) ->
    Inform("Resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, reset, []});

action(force_reset, Node, [], _Opts, Inform) ->
    Inform("Forcefully resetting node ~p", [Node]),
    call(Node, {rabbit_mnesia, force_reset, []});

action(cluster, Node, ClusterNodeSs, _Opts, Inform) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    Inform("Clustering node ~p with ~p",
              [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, cluster, [ClusterNodes]);

action(force_cluster, Node, ClusterNodeSs, _Opts, Inform) ->
    ClusterNodes = lists:map(fun list_to_atom/1, ClusterNodeSs),
    Inform("Forcefully clustering node ~p with ~p (ignoring offline nodes)",
              [Node, ClusterNodes]),
    rpc_call(Node, rabbit_mnesia, force_cluster, [ClusterNodes]);

action(status, Node, [], _Opts, Inform) ->
    Inform("Status of node ~p", [Node]),
    case call(Node, {rabbit, status, []}) of
        {badrpc, _} = Res -> Res;
        Res               -> io:format("~p~n", [Res]),
                             ok
    end;

action(rotate_logs, Node, [], _Opts, Inform) ->
    Inform("Reopening logs for node ~p", [Node]),
    call(Node, {rabbit, rotate_logs, [""]});
action(rotate_logs, Node, Args = [Suffix], _Opts, Inform) ->
    Inform("Rotating logs to files with suffix ~p", [Suffix]),
    call(Node, {rabbit, rotate_logs, Args});

action(close_connection, Node, [PidStr, Explanation], _Opts, Inform) ->
    Inform("Closing connection ~s", [PidStr]),
    rpc_call(Node, rabbit_networking, close_connection,
             [rabbit_misc:string_to_pid(PidStr), Explanation]);

action(add_user, Node, Args = [Username, _Password], _Opts, Inform) ->
    Inform("Creating user ~p", [Username]),
    call(Node, {rabbit_access_control, add_user, Args});

action(delete_user, Node, Args = [_Username], _Opts, Inform) ->
    Inform("Deleting user ~p", Args),
    call(Node, {rabbit_access_control, delete_user, Args});

action(change_password, Node, Args = [Username, _Newpassword], _Opts, Inform) ->
    Inform("Changing password for user ~p", [Username]),
    call(Node, {rabbit_access_control, change_password, Args});

action(list_users, Node, [], _Opts, Inform) ->
    Inform("Listing users", []),
    display_list(call(Node, {rabbit_access_control, list_users, []}));

action(add_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Creating vhost ~p", Args),
    call(Node, {rabbit_access_control, add_vhost, Args});

action(delete_vhost, Node, Args = [_VHostPath], _Opts, Inform) ->
    Inform("Deleting vhost ~p", Args),
    call(Node, {rabbit_access_control, delete_vhost, Args});

action(list_vhosts, Node, [], _Opts, Inform) ->
    Inform("Listing vhosts", []),
    display_list(call(Node, {rabbit_access_control, list_vhosts, []}));

action(list_user_permissions, Node, Args = [_Username], _Opts, Inform) ->
    Inform("Listing permissions for user ~p", Args),
    display_list(call(Node, {rabbit_access_control, list_user_permissions,
                             Args}));

action(list_queues, Node, Args, Opts, Inform) ->
    Inform("Listing queues", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [name, messages]),
    display_info_list(rpc_call(Node, rabbit_amqqueue, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_exchanges, Node, Args, Opts, Inform) ->
    Inform("Listing exchanges", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    ArgAtoms = default_if_empty(Args, [name, type]),
    display_info_list(rpc_call(Node, rabbit_exchange, info_all,
                               [VHostArg, ArgAtoms]),
                      ArgAtoms);

action(list_bindings, Node, _Args, Opts, Inform) ->
    Inform("Listing bindings", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    InfoKeys = [exchange_name, queue_name, routing_key, args],
    display_info_list(
      [lists:zip(InfoKeys, tuple_to_list(X)) ||
          X <- rpc_call(Node, rabbit_exchange, list_bindings, [VHostArg])],
      InfoKeys);

action(list_connections, Node, Args, _Opts, Inform) ->
    Inform("Listing connections", []),
    ArgAtoms = default_if_empty(Args, [user, peer_address, peer_port, state]),
    display_info_list(rpc_call(Node, rabbit_networking, connection_info_all,
                               [ArgAtoms]),
                      ArgAtoms);

action(list_channels, Node, Args, _Opts, Inform) ->
    Inform("Listing channels", []),
    ArgAtoms = default_if_empty(Args, [pid, user, transactional, consumer_count,
                                       messages_unacknowledged]),
    display_info_list(rpc_call(Node, rabbit_channel, info_all, [ArgAtoms]),
                      ArgAtoms);

action(list_consumers, Node, _Args, Opts, Inform) ->
    Inform("Listing consumers", []),
    VHostArg = list_to_binary(proplists:get_value(?VHOST_OPT, Opts)),
    InfoKeys = [queue_name, channel_pid, consumer_tag, ack_required],
    display_info_list(
      [lists:zip(InfoKeys, tuple_to_list(X)) ||
          X <- rpc_call(Node, rabbit_amqqueue, consumers_all, [VHostArg])],
      InfoKeys);

action(set_permissions, Node, [Username, CPerm, WPerm, RPerm], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Scope = proplists:get_value(?SCOPE_OPT, Opts),
    Inform("Setting permissions for user ~p in vhost ~p", [Username, VHost]),
    call(Node, {rabbit_access_control, set_permissions,
                [Scope, Username, VHost, CPerm, WPerm, RPerm]});

action(clear_permissions, Node, [Username], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Clearing permissions for user ~p in vhost ~p", [Username, VHost]),
    call(Node, {rabbit_access_control, clear_permissions, [Username, VHost]});

action(list_permissions, Node, [], Opts, Inform) ->
    VHost = proplists:get_value(?VHOST_OPT, Opts),
    Inform("Listing permissions in vhost ~p", [VHost]),
    display_list(call(Node, {rabbit_access_control, list_vhost_permissions,
                             [VHost]})).

default_if_empty(List, Default) when is_list(List) ->
    if List == [] ->
        Default;
       true ->
        [list_to_atom(X) || X <- List]
    end.

display_info_list(Results, InfoItemKeys) when is_list(Results) ->
    lists:foreach(fun (Result) -> display_row([format_info_item(X, Result) ||
                                                  X <- InfoItemKeys])
                  end, Results),
    ok;
display_info_list(Other, _) ->
    Other.

display_row(Row) ->
    io:fwrite(lists:flatten(rabbit_misc:intersperse("\t", Row))),
    io:nl().

format_info_item(Key, Items) ->
    case proplists:get_value(Key, Items) of
        #resource{name = Name} ->
            escape(Name);
        Value when Key =:= address; Key =:= peer_address andalso
                   is_tuple(Value) ->
            inet_parse:ntoa(Value);
        Value when is_pid(Value) ->
            rabbit_misc:pid_to_string(Value);
        Value when is_binary(Value) ->
            escape(Value);
        Value when is_atom(Value) ->
            escape(atom_to_list(Value));
        Value = [{TableEntryKey, TableEntryType, _TableEntryValue} | _]
        when is_binary(TableEntryKey) andalso is_atom(TableEntryType) ->
            io_lib:format("~1000000000000p", [prettify_amqp_table(Value)]);
        Value ->
            io_lib:format("~w", [Value])
    end.

display_list(L) when is_list(L) ->
    lists:foreach(fun (I) when is_binary(I) ->
                          io:format("~s~n", [escape(I)]);
                      (I) when is_tuple(I) ->
                          display_row([escape(V)
                                       || V <- tuple_to_list(I)])
                  end,
                  lists:sort(L)),
    ok;
display_list(Other) -> Other.

call(Node, {Mod, Fun, Args}) ->
    rpc_call(Node, Mod, Fun, lists:map(fun list_to_binary/1, Args)).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).

%% escape does C-style backslash escaping of non-printable ASCII
%% characters.  We don't escape characters above 127, since they may
%% form part of UTF-8 strings.

escape(Atom) when is_atom(Atom) ->
    escape(atom_to_list(Atom));
escape(Bin) when is_binary(Bin) ->
    escape(binary_to_list(Bin));
escape(L) when is_list(L) ->
    escape_char(lists:reverse(L), []).

escape_char([$\\ | T], Acc) ->
    escape_char(T, [$\\, $\\ | Acc]);
escape_char([X | T], Acc) when X >= 32, X /= 127 ->
    escape_char(T, [X | Acc]);
escape_char([X | T], Acc) ->
    escape_char(T, [$\\, $0 + (X bsr 6), $0 + (X band 8#070 bsr 3),
                    $0 + (X band 7) | Acc]);
escape_char([], Acc) ->
    Acc.

prettify_amqp_table(Table) ->
    [{escape(K), prettify_typed_amqp_value(T, V)} || {K, T, V} <- Table].

prettify_typed_amqp_value(Type, Value) ->
    case Type of
        longstr -> escape(Value);
        table   -> prettify_amqp_table(Value);
        array   -> [prettify_typed_amqp_value(T, V) || {T, V} <- Value];
        _       -> Value
    end.
