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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-type(fetch_result() ::
                 %% Message,  IsDelivered,  AckTag,  Remaining_Len
        ('empty'|{basic_message(), boolean(), ack(), non_neg_integer()})).

-spec(start/1 :: ([queue_name()]) -> 'ok').
-spec(init/2 :: (queue_name(), boolean()) -> state()).
-spec(terminate/1 :: (state()) -> state()).
-spec(delete_and_terminate/1 :: (state()) -> state()).
-spec(purge/1 :: (state()) -> {non_neg_integer(), state()}).
-spec(publish/2 :: (basic_message(), state()) -> state()).
-spec(publish_delivered/3 ::
        (boolean(), basic_message(), state()) -> {ack(), state()}).
-spec(fetch/2 :: (boolean(), state()) -> {fetch_result(), state()}).
-spec(ack/2 :: ([ack()], state()) -> state()).
-spec(tx_publish/3 :: (txn(), basic_message(), state()) -> state()).
-spec(tx_ack/3 :: (txn(), [ack()], state()) -> state()).
-spec(tx_rollback/2 :: (txn(), state()) -> {[ack()], state()}).
-spec(tx_commit/3 :: (txn(), {pid(), any()}, state()) -> {[ack()], state()}).
-spec(requeue/2 :: ([ack()], state()) -> state()).
-spec(len/1 :: (state()) -> non_neg_integer()).
-spec(is_empty/1 :: (state()) -> boolean()).
-spec(set_ram_duration_target/2 ::
      (('undefined' | 'infinity' | number()), state()) -> state()).
-spec(ram_duration/1 :: (state()) -> {number(), state()}).
-spec(sync_callback/1 :: (state()) ->
                              ('undefined' | (fun ((A) -> {boolean(), A})))).
-spec(handle_pre_hibernate/1 :: (state()) -> state()).
-spec(status/1 :: (state()) -> [{atom(), any()}]).
