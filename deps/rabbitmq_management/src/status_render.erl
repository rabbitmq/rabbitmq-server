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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(status_render).

-export([render_conns/0, render_queues/0]).
-export([escape/1, format_info_item/2, format_info/2, print/2]).

-include_lib("rabbit_common/include/rabbit.hrl").


%%--------------------------------------------------------------------

render_conns() ->
    ConnKeys = [pid, address, port, peer_address, peer_port, recv_oct, recv_cnt,
                send_oct, send_cnt, send_pend, state,
                channels, user, vhost, timeout, frame_max],
    Conns = rabbit_networking:connection_info_all(),
    [[{Key, format_info_item(Key, Conn)} || Key <- ConnKeys] || Conn <- Conns].

render_queues() ->
    QueueKeys = [name, durable, auto_delete, arguments, pid, messages_ready,
                 messages_unacknowledged, messages, consumers, memory],

    Queues = lists:flatten([
                    [{Vhost, Queue} || Queue <- rabbit_amqqueue:info_all(Vhost)]
                        || Vhost <- rabbit_access_control:list_vhosts()]),
    [[{vhost, format_info(vhost, Vhost)}] ++
             [{Key, format_info_item(Key, Queue)} || Key <- QueueKeys]
                                                  || {Vhost, Queue} <- Queues].



print(Fmt, Val) when is_list(Val) ->
    escape(lists:flatten(io_lib:format(Fmt, Val)));
print(Fmt, Val) ->
    print(Fmt, [Val]).

print_no_escape(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val))).



escape(A) ->
    mochiweb_html:escape(A).

format_info_item(Key, Items) ->
    format_info(Key, proplists:get_value(Key, Items)).


format_info(Key, Value) ->
    case Value of
        #resource{name = Name} ->       %% queue name
            Name;
        Value when (Key =:= address orelse Key =:= peer_address) andalso
                   is_tuple(Value) ->
            list_to_binary(inet_parse:ntoa(Value));
        Value when is_number(Value) ->  %% memory stats, counters
            Value;
        Value when is_binary(Value) ->  %% vhost, username
            Value;
        Value ->                        %% queue arguments
            print_no_escape("~w", [Value])
    end.

