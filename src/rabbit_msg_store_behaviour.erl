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
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%


-module(rabbit_msg_store_behaviour).

-type msg_ref_delta_gen(A) ::
        fun ((A) -> 'finished' |
                    {rabbit_types:msg_id(), non_neg_integer(), A}).
-type maybe_msg_id_fun() ::
        'undefined' | fun ((gb_sets:set(), 'written' | 'ignored') -> any()).

-type maybe_close_fds_fun() :: 'undefined' | fun (() -> 'ok').

-type server() :: atom() | pid().

-type client_ref() :: binary().

-type msg() :: any().

-export_type([msg_ref_delta_gen/1,
              maybe_msg_id_fun/0,
              maybe_close_fds_fun/0,
              server/0,
              client_ref/0,
              msg/0]).

-callback start_link (atom(), file:filename(),
                      [binary()] | 'undefined',
                      {msg_ref_delta_gen(A), A}) -> rabbit_types:ok_pid_or_error().

-callback successfully_recovered_state(server()) -> boolean().

-callback client_init(server(), client_ref(), maybe_msg_id_fun(), maybe_close_fds_fun()) -> term().

-callback client_terminate(term()) -> 'ok'.

-callback client_delete_and_terminate(term()) -> 'ok'.

-callback client_ref(term()) -> client_ref().

-callback close_all_indicated (term()) -> rabbit_types:ok(term()).

-callback write(rabbit_types:msg_id(), msg(), term()) -> 'ok'.

-callback write_flow(rabbit_types:msg_id(), msg(), term()) -> 'ok'.

-callback read(rabbit_types:msg_id(), term()) -> {rabbit_types:ok(msg()) | 'not_found', term()}.

-callback contains(rabbit_types:msg_id(), term()) -> boolean().

-callback remove([rabbit_types:msg_id()], term()) -> 'ok'.
