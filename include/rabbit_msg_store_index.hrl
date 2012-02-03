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

-include("rabbit_msg_store.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(dir() :: any()).
-type(index_state() :: any()).
-type(keyvalue() :: any()).
-type(fieldpos() :: non_neg_integer()).
-type(fieldvalue() :: any()).

-spec(new/1 :: (dir()) -> index_state()).
-spec(recover/1 :: (dir()) -> rabbit_types:ok_or_error2(index_state(), any())).
-spec(lookup/2 ::
        (rabbit_types:msg_id(), index_state()) -> ('not_found' | keyvalue())).
-spec(insert/2 :: (keyvalue(), index_state()) -> 'ok').
-spec(update/2 :: (keyvalue(), index_state()) -> 'ok').
-spec(update_fields/3 :: (rabbit_types:msg_id(), ({fieldpos(), fieldvalue()} |
                                                  [{fieldpos(), fieldvalue()}]),
                          index_state()) -> 'ok').
-spec(delete/2 :: (rabbit_types:msg_id(), index_state()) -> 'ok').
-spec(delete_object/2 :: (keyvalue(), index_state()) -> 'ok').
-spec(delete_by_file/2 :: (fieldvalue(), index_state()) -> 'ok').
-spec(terminate/1 :: (index_state()) -> any()).

-endif.

%%----------------------------------------------------------------------------
