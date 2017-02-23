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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_index).

-include("rabbit_msg_store.hrl").

-type(dir() :: string()).
-type(index_state() :: any()).
-type(fieldpos() :: non_neg_integer()).
-type(fieldvalue() :: any()).
-type(msg_location() :: #msg_location{}).

%% Initialize a fresh index state for msg store directory.
-callback new(dir()) -> index_state().
%% Try to recover gracefully stopped index state.
-callback recover(dir()) -> rabbit_types:ok_or_error2(index_state(), any()).
%% Lookup an entry in the index.
%% Is called concurrently by msg_store, it's clients and GC processes.
-callback lookup(rabbit_types:msg_id(), index_state()) -> ('not_found' | msg_location()).
%% Insert an entry into the index.
%% Is called by a msg_store process only.
%% This function can exit if there is already an entry with the same ID
-callback insert(msg_location(), index_state()) -> 'ok'.
%% Update an entry in the index.
%% Is called by a msg_store process only.
-callback update(msg_location(), index_state()) -> 'ok'.
%% Update positional fields in the entry tuple.
%% Is called by msg_store and GC processes concurrently.
%% This function can exit if there is no entry with specified ID
-callback update_fields(rabbit_types:msg_id(), ({fieldpos(), fieldvalue()} |
                                                [{fieldpos(), fieldvalue()}]),
                        index_state()) -> 'ok'.
%% Delete an entry from the index by ID.
%% Is called from a msg_store process only.
-callback delete(rabbit_types:msg_id(), index_state()) -> 'ok'.
%% Delete an exactly matching entry from the index.
%% Is called by GC process only.
-callback delete_object(msg_location(), index_state()) -> 'ok'.
%% Delete temporary reference count entries with the 'file' record field equal to 'undefined'.
%% Is called during index rebuild from scratch (e.g. after non-clean stop)
-callback clean_up_temporary_reference_count_entries_without_file(index_state()) -> 'ok'.
%% Gracefully shutdown the index.
%% Should save the index state, which will be loaded by the 'recover' function.
-callback terminate(index_state()) -> any().
