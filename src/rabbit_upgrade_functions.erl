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

-rabbit_upgrade({remove_user_scope,  []}).
-rabbit_upgrade({hash_passwords,     []}).
-rabbit_upgrade({add_ip_to_listener, []}).

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_user_scope/0  :: () -> 'ok').
-spec(hash_passwords/0     :: () -> 'ok').
-spec(add_ip_to_listener/0 :: () -> 'ok').

-endif.

%%--------------------------------------------------------------------

remove_user_scope() ->
    mnesia(
      rabbit_user_permission,
      fun (Perm = #user_permission{
             permission = {permission,
                           _Scope, Conf, Write, Read}}) ->
              Perm#user_permission{
                permission = #permission{configure = Conf,
                                         write     = Write,
                                         read      = Read}}
      end,
      record_info(fields, user_permission)).

hash_passwords() ->
    mnesia(
      rabbit_user,
      fun ({user, Username, Password, IsAdmin}) ->
              Hash = rabbit_access_control:hash_password(Password),
              #user{username      = Username,
                    password_hash = Hash,
                    is_admin      = IsAdmin}
      end,
      record_info(fields, user)).

add_ip_to_listener() ->
    mnesia(
      rabbit_listener,
      fun ({listener, Node, Protocol, Host, Port}) ->
              #listener{node       = Node,
                        protocol   = Protocol,
                        host       = Host,
                        ip_address = {0,0,0,0},
                        port       = Port}
      end,
      record_info(fields, listener)).

%%--------------------------------------------------------------------

mnesia(TableName, Fun, RecordInfo) ->
    {atomic, ok} = mnesia:transform_table(TableName, Fun, RecordInfo),
    ok.
