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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_upgrade_functions).

-include("rabbit.hrl").

-compile([export_all]).

-rabbit_upgrade({test_add_column, []}).
-rabbit_upgrade({test_remove_column, [test_add_column]}).
-rabbit_upgrade({remove_user_scope, []}).

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_user_scope/0 :: () -> 'ok').
-spec(test_add_column/0 :: () -> 'ok').
-spec(test_remove_column/0 :: () -> 'ok').

-endif.

%%--------------------------------------------------------------------

%% TODO this is just a hack, when branch bug23319 is merged this should use
%% the real permission record
-record(permission2, {configure, write, read}).

remove_user_scope() ->
    {atomic, ok} = mnesia:transform_table(
                     rabbit_user_permission,
                     fun (Perm = #user_permission{
                            permission = {permission,
                                          _Scope, Conf, Write, Read}}) ->
                             Perm#user_permission{
                               permission = #permission2{configure = Conf,
                                                         write = Write,
                                                         read = Read}}
                     end,
                     record_info(fields, user_permission)),
    ok.

test_add_column() ->
    {atomic, ok} = mnesia:transform_table(
                     rabbit_user,
                     fun ({user, Username, Password, Admin}) ->
                             {user, Username, Password, Admin, something_else}
                     end,
                     [username, password, is_admin, something]),
    ok.

test_remove_column() ->
    {atomic, ok} = mnesia:transform_table(
                     rabbit_user,
                     fun ({user, Username, Password, Admin, _SomethingElse}) ->
                             {user, Username, Password, Admin}
                     end,
                     record_info(fields, user)),
    ok.
