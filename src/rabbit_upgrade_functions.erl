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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_upgrade_functions).

%% If you are tempted to add include("rabbit.hrl"). here, don't. Using record
%% defs here leads to pain later.

-compile([export_all]).

-rabbit_upgrade({remove_user_scope,     mnesia, []}).
-rabbit_upgrade({hash_passwords,        mnesia, []}).
-rabbit_upgrade({add_ip_to_listener,    mnesia, []}).
-rabbit_upgrade({internal_exchanges,    mnesia, []}).
-rabbit_upgrade({user_to_internal_user, mnesia, [hash_passwords]}).
-rabbit_upgrade({topic_trie,            mnesia, []}).
-rabbit_upgrade({semi_durable_route,    mnesia, []}).
-rabbit_upgrade({exchange_event_serial, mnesia, []}).
-rabbit_upgrade({trace_exchanges,       mnesia, [internal_exchanges]}).
-rabbit_upgrade({user_admin_to_tags,    mnesia, [user_to_internal_user]}).
-rabbit_upgrade({ha_mirrors,            mnesia, []}).
-rabbit_upgrade({gm,                    mnesia, []}).
-rabbit_upgrade({exchange_scratch,      mnesia, [trace_exchanges]}).
-rabbit_upgrade({mirrored_supervisor,   mnesia, []}).
-rabbit_upgrade({topic_trie_node,       mnesia, []}).
-rabbit_upgrade({runtime_parameters,    mnesia, []}).

%% -------------------------------------------------------------------

-ifdef(use_specs).

-spec(remove_user_scope/0     :: () -> 'ok').
-spec(hash_passwords/0        :: () -> 'ok').
-spec(add_ip_to_listener/0    :: () -> 'ok').
-spec(internal_exchanges/0    :: () -> 'ok').
-spec(user_to_internal_user/0 :: () -> 'ok').
-spec(topic_trie/0            :: () -> 'ok').
-spec(semi_durable_route/0    :: () -> 'ok').
-spec(exchange_event_serial/0 :: () -> 'ok').
-spec(trace_exchanges/0       :: () -> 'ok').
-spec(user_admin_to_tags/0    :: () -> 'ok').
-spec(ha_mirrors/0            :: () -> 'ok').
-spec(gm/0                    :: () -> 'ok').
-spec(exchange_scratch/0      :: () -> 'ok').
-spec(mirrored_supervisor/0   :: () -> 'ok').
-spec(topic_trie_node/0       :: () -> 'ok').
-spec(runtime_parameters/0    :: () -> 'ok').

-endif.

%%--------------------------------------------------------------------

%% It's a bad idea to use records or record_info here, even for the
%% destination form. Because in the future, the destination form of
%% your current transform may not match the record any more, and it
%% would be messy to have to go back and fix old transforms at that
%% point.

remove_user_scope() ->
    transform(
      rabbit_user_permission,
      fun ({user_permission, UV, {permission, _Scope, Conf, Write, Read}}) ->
              {user_permission, UV, {permission, Conf, Write, Read}}
      end,
      [user_vhost, permission]).

hash_passwords() ->
    transform(
      rabbit_user,
      fun ({user, Username, Password, IsAdmin}) ->
              Hash = rabbit_auth_backend_internal:hash_password(Password),
              {user, Username, Hash, IsAdmin}
      end,
      [username, password_hash, is_admin]).

add_ip_to_listener() ->
    transform(
      rabbit_listener,
      fun ({listener, Node, Protocol, Host, Port}) ->
              {listener, Node, Protocol, Host, {0,0,0,0}, Port}
      end,
      [node, protocol, host, ip_address, port]).

internal_exchanges() ->
    Tables = [rabbit_exchange, rabbit_durable_exchange],
    AddInternalFun =
        fun ({exchange, Name, Type, Durable, AutoDelete, Args}) ->
                {exchange, Name, Type, Durable, AutoDelete, false, Args}
        end,
    [ ok = transform(T,
                     AddInternalFun,
                     [name, type, durable, auto_delete, internal, arguments])
      || T <- Tables ],
    ok.

user_to_internal_user() ->
    transform(
      rabbit_user,
      fun({user, Username, PasswordHash, IsAdmin}) ->
              {internal_user, Username, PasswordHash, IsAdmin}
      end,
      [username, password_hash, is_admin], internal_user).

topic_trie() ->
    create(rabbit_topic_trie_edge, [{record_name, topic_trie_edge},
                                    {attributes, [trie_edge, node_id]},
                                    {type, ordered_set}]),
    create(rabbit_topic_trie_binding, [{record_name, topic_trie_binding},
                                       {attributes, [trie_binding, value]},
                                       {type, ordered_set}]).

semi_durable_route() ->
    create(rabbit_semi_durable_route, [{record_name, route},
                                       {attributes, [binding, value]}]).

exchange_event_serial() ->
    create(rabbit_exchange_serial, [{record_name, exchange_serial},
                                    {attributes, [name, next]}]).

trace_exchanges() ->
    [declare_exchange(
       rabbit_misc:r(VHost, exchange, <<"amq.rabbitmq.trace">>), topic) ||
        VHost <- rabbit_vhost:list()],
    ok.

user_admin_to_tags() ->
    transform(
      rabbit_user,
      fun({internal_user, Username, PasswordHash, true}) ->
              {internal_user, Username, PasswordHash, [administrator]};
         ({internal_user, Username, PasswordHash, false}) ->
              {internal_user, Username, PasswordHash, [management]}
      end,
      [username, password_hash, tags], internal_user).

ha_mirrors() ->
    Tables = [rabbit_queue, rabbit_durable_queue],
    AddMirrorPidsFun =
        fun ({amqqueue, Name, Durable, AutoDelete, Owner, Arguments, Pid}) ->
                {amqqueue, Name, Durable, AutoDelete, Owner, Arguments, Pid,
                 [], undefined}
        end,
    [ ok = transform(T,
                     AddMirrorPidsFun,
                     [name, durable, auto_delete, exclusive_owner, arguments,
                      pid, slave_pids, mirror_nodes])
      || T <- Tables ],
    ok.

gm() ->
    create(gm_group, [{record_name, gm_group},
                      {attributes, [name, version, members]}]).

exchange_scratch() ->
    ok = exchange_scratch(rabbit_exchange),
    ok = exchange_scratch(rabbit_durable_exchange).

exchange_scratch(Table) ->
    transform(
      Table,
      fun ({exchange, Name, Type, Dur, AutoDel, Int, Args}) ->
              {exchange, Name, Type, Dur, AutoDel, Int, Args, undefined}
      end,
      [name, type, durable, auto_delete, internal, arguments, scratch]).

mirrored_supervisor() ->
    create(mirrored_sup_childspec,
           [{record_name, mirrored_sup_childspec},
            {attributes, [key, mirroring_pid, childspec]}]).

topic_trie_node() ->
    create(rabbit_topic_trie_node,
           [{record_name, topic_trie_node},
            {attributes, [trie_node, edge_count, binding_count]},
            {type, ordered_set}]).

runtime_parameters() ->
    create(rabbit_runtime_parameters,
           [{record_name, runtime_parameters},
            {attributes, [key, value]},
            {disc_copies, [node()]}]).

%%--------------------------------------------------------------------

transform(TableName, Fun, FieldList) ->
    rabbit_mnesia:wait_for_tables([TableName]),
    {atomic, ok} = mnesia:transform_table(TableName, Fun, FieldList),
    ok.

transform(TableName, Fun, FieldList, NewRecordName) ->
    rabbit_mnesia:wait_for_tables([TableName]),
    {atomic, ok} = mnesia:transform_table(TableName, Fun, FieldList,
                                          NewRecordName),
    ok.

create(Tab, TabDef) ->
    {atomic, ok} = mnesia:create_table(Tab, TabDef),
    ok.

%% Dumb replacement for rabbit_exchange:declare that does not require
%% the exchange type registry or worker pool to be running by dint of
%% not validating anything and assuming the exchange type does not
%% require serialisation.
%% NB: this assumes the pre-exchange-scratch-space format
declare_exchange(XName, Type) ->
    X = {exchange, XName, Type, true, false, false, []},
    ok = mnesia:dirty_write(rabbit_durable_exchange, X).
