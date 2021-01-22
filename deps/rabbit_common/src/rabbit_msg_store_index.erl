%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_msg_store_index).

-include("rabbit_msg_store.hrl").

%% Behaviour module to provide pluggable message store index.
%% The index is used to locate message on disk and for reference-counting.

%% Message store have several additional assumptions about performance and
%% atomicity of some operations. See comments for each callback.

-type(dir() :: string()).
-type(index_state() :: any()).
-type(fieldpos() :: non_neg_integer()).
-type(fieldvalue() :: any()).
-type(msg_location() :: #msg_location{}).


%% There are two ways of starting an index:
%% - `new` - starts a clean index
%% - `recover` - attempts to read a saved index
%% In both cases the old saved state should be deleted from directory.

%% Initialize a fresh index state for msg store directory.
-callback new(dir()) -> index_state().
%% Try to recover gracefully stopped index state.
-callback recover(dir()) -> rabbit_types:ok_or_error2(index_state(), any()).
%% Gracefully shutdown the index.
%% Should save the index state, which will be loaded by the 'recover' function.
-callback terminate(index_state()) -> any().

%% Lookup an entry in the index.
%% Is called concurrently by msg_store, it's clients and GC processes.
%% This function is called multiple times for each message store operation.
%% Message store tries to avoid writing messages on disk if consumers can
%% process them fast, so there will be a lot of lookups for non-existent
%% entries, which should be as fast as possible.
-callback lookup(rabbit_types:msg_id(), index_state()) -> ('not_found' | msg_location()).

%% Insert an entry into the index.
%% Is called by a msg_store process only.
%% This function can exit if there is already an entry with the same ID
-callback insert(msg_location(), index_state()) -> 'ok'.

%% Update an entry in the index.
%% Is called by a msg_store process only.
%% The function is called during message store recovery after crash.
%% The difference between update and insert functions, is that update
%% should not fail if entry already exist, and should be atomic.
-callback update(msg_location(), index_state()) -> 'ok'.

%% Update positional fields in the entry tuple.
%% Is called by msg_store and GC processes concurrently.
%% This function can exit if there is no entry with specified ID
%% This function is called to update reference-counters and file locations.
%% File locations are updated from a GC process, reference-counters are
%% updated from a message store process.
%% This function should be atomic.
-callback update_fields(rabbit_types:msg_id(), ({fieldpos(), fieldvalue()} |
                                                [{fieldpos(), fieldvalue()}]),
                        index_state()) -> 'ok'.

%% Delete an entry from the index by ID.
%% Is called from a msg_store process only.
%% This function should be atomic.
-callback delete(rabbit_types:msg_id(), index_state()) -> 'ok'.

%% Delete an exactly matching entry from the index.
%% Is called by GC process only.
%% This function should match exact object to avoid deleting a zero-reference
%% object, which reference-counter is being concurrently updated.
%% This function should be atomic.
-callback delete_object(msg_location(), index_state()) -> 'ok'.

%% Delete temporary reference count entries with the 'file' record field equal to 'undefined'.
%% Is called during index rebuild from scratch (e.g. after non-clean stop)
%% During recovery after non-clean stop or file corruption, reference-counters
%% are added to the index with `undefined` value for the `file` field.
%% If message is found in a message store file, it's file field is updated.
%% If some reference-counters miss the message location after recovery - they
%% should be deleted.
-callback clean_up_temporary_reference_count_entries_without_file(index_state()) -> 'ok'.

