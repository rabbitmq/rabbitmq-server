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

-module(rabbit_variable_queue).

-export([init/1, in/3]).

-record(vqstate,
        { q1,
          q2,
          gamma,
          q3,
          q4,
          egress_rate,
          target_ram_msg_count,
          ram_msg_count,
          queue,
          index_state,
          next_seq_id
        }).

-include("rabbit.hrl").

init(QueueName) ->
    {NextSeqId, IndexState} = rabbit_queue_index:init(QueueName),
    #vqstate { q1 = queue:new(), q2 = queue:new(),
               gamma = 0,
               q3 = queue:new(), q4 = queue:new(),
               egress_rate = 0,
               target_ram_msg_count = undefined,
               ram_msg_count = 0,
               queue = QueueName,
               index_state = IndexState,
               next_seq_id = NextSeqId
             }.

maybe_write_msg_to_disk(Bool, Msg = #basic_message {
                                guid = MsgId, is_persistent = IsPersistent })
  when Bool orelse IsPersistent ->
    ok = rabbit_msg_store:write(MsgId, ensure_binary_properties(Msg)),
    true;
maybe_write_msg_to_disk(_Bool, _Msg) ->
    false.

maybe_write_index_to_disk(Bool, IsPersistent, MsgId, SeqId, IsDelivered,
                          IndexState) when Bool orelse IsPersistent ->
    IndexState1 = rabbit_queue_index:write_published(
                    MsgId, SeqId, IsPersistent, IndexState),
    {true, case IsDelivered of
               true  -> rabbit_queue_index:write_delivered(SeqId, IndexState1);
               false -> IndexState1
           end};
maybe_write_index_to_disk(_Bool, _IsPersistent, _MsgId, _SeqId, _IsDelivered,
                          IndexState) ->
    {false, IndexState}.

in(Msg = #basic_message {}, IsDelivered, State) ->
    in(test_keep_msg_in_ram(State), Msg, IsDelivered, State).

in(msg_and_index, Msg = #basic_message { guid = MsgId,
                                         is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId,
                                   ram_msg_count = RamMsgCount
                                 }) ->
    MsgOnDisk = maybe_write_msg_to_disk(false, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry =
        {msg_and_index, Msg, SeqId, IsDelivered, MsgOnDisk, IndexOnDisk},
    State1 = State #vqstate { next_seq_id = SeqId + 1,
                              ram_msg_count = RamMsgCount + 1,
                              index_state = IndexState1 },
    store_alpha_entry(Entry, State1);

in(just_index, Msg = #basic_message { guid = MsgId,
                                      is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId, q1 = Q1 }) ->
    true = maybe_write_msg_to_disk(true, Msg),
    {IndexOnDisk, IndexState1} =
        maybe_write_index_to_disk(false, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    Entry = {index, MsgId, SeqId, IsDelivered, true, IndexOnDisk},
    State1 = State #vqstate { next_seq_id = SeqId + 1,
                              index_state = IndexState1 },
    true = queue:is_empty(Q1), %% ASSERTION
    store_beta_entry(Entry, State1);

in(neither, Msg = #basic_message { guid = MsgId,
                                   is_persistent = IsPersistent },
   IsDelivered, State = #vqstate { index_state = IndexState,
                                   next_seq_id = SeqId,
                                   q1 = Q1, q2 = Q2, gamma = Gamma }) ->
    true = maybe_write_msg_to_disk(true, Msg),
    {true, IndexState1} =
        maybe_write_index_to_disk(true, IsPersistent, MsgId, SeqId,
                                  IsDelivered, IndexState),
    true = queue:is_empty(Q1) andalso queue:is_empty(Q2), %% ASSERTION
    State #vqstate { next_seq_id = SeqId + 1,
                     index_state = IndexState1,
                     gamma = Gamma + 1 }.

test_keep_msg_in_ram(#vqstate { target_ram_msg_count = TargetRamMsgCount,
                                ram_msg_count = RamMsgCount,
                                q1 = Q1 }) ->
    case TargetRamMsgCount of
        undefined -> msg_and_index;
        0         -> neither;
        _ when TargetRamMsgCount > RamMsgCount ->
                     msg_and_index;
        _         -> case queue:is_empty(Q1) of
                         true -> just_index;
                         false -> msg_and_index %% can push out elders to disk
                     end
    end.

ensure_binary_properties(Msg = #basic_message { content = Content }) ->
    Msg #basic_message {
      content = rabbit_binary_parser:clear_decoded_content(
                  rabbit_binary_generator:ensure_content_encoded(Content)) }.

store_alpha_entry(Entry, State = #vqstate { q1 = Q1, q2 = Q2, gamma = Gamma,
                                            q3 = Q3, q4 = Q4 }) ->
    case queue:is_empty(Q1) andalso queue:is_empty(Q2) andalso 
        Gamma == 0 andalso queue:is_empty(Q3) of
        true ->
            State #vqstate { q4 = queue:in(Entry, Q4) };
        false ->
            maybe_push_q1_out(State #vqstate { q1 = queue:in(Entry, Q1) })
    end.

store_beta_entry(Entry, State = #vqstate { q2 = Q2, gamma = Gamma, q3 = Q3 }) ->
    case queue:is_empty(Q2) andalso Gamma == 0 of
        true  -> State #vqstate { q3 = queue:in(Entry, Q3) };
        false -> State #vqstate { q2 = queue:in(Entry, Q2) }
    end.

maybe_push_q1_out(State = #vqstate { ram_msg_count = RamMsgCount,
                                     target_ram_msg_count = TargetRamMsgCount
                                    }) when TargetRamMsgCount > RamMsgCount ->
    State;
maybe_push_q1_out(State = #vqstate { ram_msg_count = RamMsgCount, q1 = Q1 }) ->
    {{value, {msg_and_index, Msg = #basic_message { guid = MsgId }, SeqId,
              IsDelivered, MsgOnDisk, IndexOnDisk}}, Q1a} = queue:out(Q1),
    true = case MsgOnDisk of
               true -> true;
               false -> maybe_write_msg_to_disk(true, Msg)
           end,
    maybe_push_q1_out(
      store_beta_entry({index, MsgId, SeqId, IsDelivered, true, IndexOnDisk},
                       State #vqstate { ram_msg_count = RamMsgCount - 1,
                                        q1 = Q1a })).
