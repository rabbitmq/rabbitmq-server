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

-module(rabbit_queue_index3).

%% Combine what we have just read from a segment file with what we're
%% holding for that segment in memory. There must be no
%% duplicates. Used when providing segment entries to the variable
%% queue.
journal_plus_segment(JEntries, SegDict) ->
    dict:fold(fun (RelSeq, JObj, SegDictOut) ->
                      SegEntry = case dict:find(RelSeq, SegDictOut) of
                                     error -> not_found;
                                     {ok, SObj = {_, _, _}} -> SObj
                                 end,
                      journal_plus_segment(JObj, SegEntry, RelSeq, SegDictOut)
              end, SegDict, JEntries).

%% Here, the OutDict is the SegDict which we may be adding to (for
%% items only in the journal), modifying (bits in both), or erasing
%% from (ack in journal, not segment).
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_plus_segment(Obj = {{_MsgId, _IsPersistent}, del, ack},
                     not_found,
                     RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);

journal_plus_segment({no_pub, del, no_ack},
                     {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, OutDict) ->
    dict:store(RelSeq, {PubRecord, del, no_ack}, OutDict);

journal_plus_segment({no_pub, del, ack},
                     {{_MsgId, _IsPersistent}, no_del, no_ack},
                     RelSeq, OutDict) ->
    dict:erase(RelSeq, OutDict);
journal_plus_segment({no_pub, no_del, ack},
                     {{_MsgId, _IsPersistent}, del, no_ack},
                     RelSeq, OutDict) ->
    dict:erase(RelSeq, OutDict).


%% Remove from the journal entries for a segment, items that are
%% duplicates of entries found in the segment itself. Used on start up
%% to clean up the journal.
journal_minus_segment(JEntries, SegDict) ->
    dict:fold(fun (RelSeq, JObj, JEntriesOut) ->
                      SegEntry = case dict:find(RelSeq, SegDict) of
                                     error -> not_found;
                                     {ok, SObj = {_, _, _}} -> SObj
                                 end,
                      journal_minus_segment(JObj, SegEntry, RelSeq, JEntriesOut)
              end, dict:new(), JEntries).

%% Here, the OutDict is a fresh journal that we're filling with valid
%% entries.
%% Both the same
journal_minus_segment(_RelSeq, Obj, Obj, OutDict) ->
    OutDict;

%% Just publish in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, no_del, no_ack},
                      not_found,
                      RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);

%% Just deliver in journal
journal_minus_segment(Obj = {no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_minus_segment({no_pub, del, no_ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      _RelSeq, OutDict) ->
    OutDict;

%% Just ack in journal
journal_minus_segment(Obj = {no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_minus_segment({no_pub, no_del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, OutDict) ->
    OutDict;

%% Publish and deliver in journal
journal_minus_segment(Obj = {{_MsgId, _IsPersistent}, del, no_ack},
                      not_found,
                      RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_minus_segment({PubRecord, del, no_ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, {no_pub, del, no_ack}, OutDict);

%% Deliver and ack in journal
journal_minus_segment(Obj = {no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, Obj, OutDict);
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, {no_pub, no_del, ack}, OutDict);
journal_minus_segment({no_pub, del, ack},
                      {{_MsgId, _IsPersistent}, del, ack},
                      _RelSeq, OutDict) ->
    OutDict;

%% Publish, deliver and ack in journal
journal_minus_segment({{_MsgId, _IsPersistent}, del, ack},
                      not_found,
                      _RelSeq, OutDict) ->
    OutDict;
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, no_del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, {no_pub, del, ack}, OutDict);
journal_minus_segment({PubRecord, del, ack},
                      {PubRecord = {_MsgId, _IsPersistent}, del, no_ack},
                      RelSeq, OutDict) ->
    dict:store(RelSeq, {no_pub, no_del, ack}, OutDict).

