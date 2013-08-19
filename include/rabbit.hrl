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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-record(user, {username,
               tags,
               auth_backend, %% Module this user came from
               impl          %% Scratch space for that module
              }).

-record(internal_user, {username, password_hash, tags}).
-record(permission, {configure, write, read}).
-record(user_vhost, {username, virtual_host}).
-record(user_permission, {user_vhost, permission}).

-record(vhost, {virtual_host, dummy}).

-record(content,
        {class_id,
         properties, %% either 'none', or a decoded record/tuple
         properties_bin, %% either 'none', or an encoded properties binary
         %% Note: at most one of properties and properties_bin can be
         %% 'none' at once.
         protocol, %% The protocol under which properties_bin was encoded
         payload_fragments_rev %% list of binaries, in reverse order (!)
         }).

-record(resource, {virtual_host, kind, name}).

-record(exchange, {name, type, durable, auto_delete, internal, arguments,
                   scratches, policy, decorators}).
-record(exchange_serial, {name, next}).

-record(amqqueue, {name, durable, auto_delete, exclusive_owner = none,
                   arguments, pid, slave_pids, sync_slave_pids, policy,
                   gm_pids}).

%% mnesia doesn't like unary records, so we add a dummy 'value' field
-record(route, {binding, value = const}).
-record(reverse_route, {reverse_binding, value = const}).

-record(binding, {source, key, destination, args = []}).
-record(reverse_binding, {destination, key, source, args = []}).

-record(topic_trie_node, {trie_node, edge_count, binding_count}).
-record(topic_trie_edge, {trie_edge, node_id}).
-record(topic_trie_binding, {trie_binding, value = const}).

-record(trie_node, {exchange_name, node_id}).
-record(trie_edge, {exchange_name, node_id, word}).
-record(trie_binding, {exchange_name, node_id, destination}).

-record(listener, {node, protocol, host, ip_address, port}).

-record(runtime_parameters, {key, value}).

-record(basic_message, {exchange_name, routing_keys = [], content, id,
                        is_persistent}).

-record(ssl_socket, {tcp, ssl}).
-record(delivery, {mandatory, sender, message, msg_seq_no}).
-record(amqp_error, {name, explanation = "", method = none}).

-record(event, {type, props, timestamp}).

-record(message_properties, {expiry, needs_confirming = false}).

-record(plugin, {name,          %% atom()
                 version,       %% string()
                 description,   %% string()
                 type,          %% 'ez' or 'dir'
                 dependencies,  %% [{atom(), string()}]
                 location}).    %% string()

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2013 GoPivotal, Inc.").
-define(INFORMATION_MESSAGE, "Licensed under the MPL.  See http://www.rabbitmq.com/").
-define(ERTS_MINIMUM, "5.6.3").

%% EMPTY_FRAME_SIZE, 8 = 1 + 2 + 4 + 1
%%  - 1 byte of frame type
%%  - 2 bytes of channel number
%%  - 4 bytes of frame payload length
%%  - 1 byte of payload trailer FRAME_END byte
%% See rabbit_binary_generator:check_empty_frame_size/0, an assertion
%% called at startup.
-define(EMPTY_FRAME_SIZE, 8).

-define(MAX_WAIT, 16#ffffffff).

-define(HIBERNATE_AFTER_MIN,        1000).
-define(DESIRED_HIBERNATE,         10000).
-define(CREDIT_DISC_BOUND,   {2000, 500}).

-define(INVALID_HEADERS_KEY, <<"x-invalid-headers">>).
-define(ROUTING_HEADERS, [<<"CC">>, <<"BCC">>]).
-define(DELETED_HEADER, <<"BCC">>).
