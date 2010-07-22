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

-export([print/2, format_pid/1, format_ip/1]).

-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

print(Fmt, Val) when is_list(Val) ->
    list_to_binary(lists:flatten(io_lib:format(Fmt, Val)));
print(Fmt, Val) ->
    print(Fmt, [Val]).

format_pid(Pid) when is_pid(Pid) ->
    list_to_binary(io_lib:format("~w", [Pid]));
format_pid('') ->
    <<"">>;
format_pid(unknown) ->
    unknown.

format_ip(unknown) ->
    unknown;
format_ip(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).
