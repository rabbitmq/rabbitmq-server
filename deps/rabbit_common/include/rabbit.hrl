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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

%% Passed around most places
-record(user, {username,
               tags,
               authz_backends}). %% List of {Module, AuthUserImpl} pairs

%% Passed to auth backends
-record(auth_user, {username,
                    tags,
                    impl}).
%% Passed to authz backends.
-record(authz_socket_info, {sockname, peername}).

%% Implementation for the internal auth backend
-record(internal_user, {
    username,
    password_hash,
    tags,
    %% password hashing implementation module,
    %% typically rabbit_password_hashing_* but can
    %% come from a plugin
    hashing_algorithm}).
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

-record(resource, {
    virtual_host,
    %% exchange, queue, ...
    kind,
    %% name as a binary
    name
}).

%% fields described as 'transient' here are cleared when writing to
%% rabbit_durable_<thing>
-record(exchange, {
          name, type, durable, auto_delete, internal, arguments, %% immutable
          scratches,    %% durable, explicitly updated via update_scratch/3
          policy,       %% durable, implicitly updated when policy changes
          decorators}). %% transient, recalculated in store/1 (i.e. recovery)

-record(amqqueue, {
          name, durable, auto_delete, exclusive_owner = none, %% immutable
          arguments,                   %% immutable
          pid,                         %% durable (just so we know home node)
          slave_pids, sync_slave_pids, %% transient
          recoverable_slaves,          %% durable
          policy,                      %% durable, implicit update as above
          gm_pids,                     %% transient
          decorators,                  %% transient, recalculated as above
          state}).                     %% durable (have we crashed?)

-record(exchange_serial, {name, next}).

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
-record(trie_binding, {exchange_name, node_id, destination, arguments}).

-record(listener, {node, protocol, host, ip_address, port}).

-record(runtime_parameters, {key, value}).

-record(basic_message,
        {exchange_name,     %% The exchange where the message was received
         routing_keys = [], %% Routing keys used during publish
         content,           %% The message content
         id,                %% A `rabbit_guid:gen()` generated id
         is_persistent}).   %% Whether the message was published as persistent

-record(delivery,
        {mandatory,  %% Whether the message was published as mandatory
         confirm,    %% Whether the message needs confirming
         sender,     %% The pid of the process that created the delivery
         message,    %% The #basic_message record
         msg_seq_no, %% Msg Sequence Number from the channel publish_seqno field
         flow}).     %% Should flow control be used for this delivery

-record(amqp_error, {name, explanation = "", method = none}).

-record(event, {type, props, reference = undefined, timestamp}).

-record(message_properties, {expiry, needs_confirming = false, size}).

-record(plugin, {name,          %% atom()
                 version,       %% string()
                 description,   %% string()
                 type,          %% 'ez' or 'dir'
                 dependencies,  %% [{atom(), string()}]
                 location}).    %% string()

%%----------------------------------------------------------------------------

-define(COPYRIGHT_MESSAGE, "Copyright (C) 2007-2016 Pivotal Software, Inc.").
-define(INFORMATION_MESSAGE, "Licensed under the MPL.  See http://www.rabbitmq.com/").
-define(OTP_MINIMUM, "R16B03").
-define(ERTS_MINIMUM, "5.10.4").

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
%% When we discover that we should write some indices to disk for some
%% betas, the IO_BATCH_SIZE sets the number of betas that we must be
%% due to write indices for before we do any work at all.
-define(IO_BATCH_SIZE, 2048). %% next power-of-2 after ?CREDIT_DISC_BOUND

-define(INVALID_HEADERS_KEY, <<"x-invalid-headers">>).
-define(ROUTING_HEADERS, [<<"CC">>, <<"BCC">>]).
-define(DELETED_HEADER, <<"BCC">>).

-define(EXCHANGE_DELETE_IN_PROGRESS_COMPONENT, <<"exchange-delete-in-progress">>).

-define(CHANNEL_OPERATION_TIMEOUT, rabbit_misc:get_channel_operation_timeout()).

%% Trying to send a term across a cluster larger than 2^31 bytes will
%% cause the VM to exit with "Absurdly large distribution output data
%% buffer". So we limit the max message size to 2^31 - 10^6 bytes (1MB
%% to allow plenty of leeway for the #basic_message{} and #content{}
%% wrapping the message body).
-define(MAX_MSG_SIZE, 2147383648).

%% First number is maximum size in bytes before we start to
%% truncate. The following 4-tuple is:
%%
%% 1) Maximum size of printable lists and binaries.
%% 2) Maximum size of any structural term.
%% 3) Amount to decrease 1) every time we descend while truncating.
%% 4) Amount to decrease 2) every time we descend while truncating.
%%
%% Whole thing feeds into truncate:log_event/2.
-define(LOG_TRUNC, {100000, {2000, 100, 50, 5}}).

-define(store_proc_name(N), rabbit_misc:store_proc_name(?MODULE, N)).
