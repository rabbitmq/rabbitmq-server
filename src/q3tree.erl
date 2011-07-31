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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(q3tree).

%% A less general random access variation of bpqueue for message status records

-export([new/0, is_empty/1, len/1, in/3, in_r/3, out/1, out_r/1, least_key/1,
         join/2, join_bpqueue/2, foldr/3, from_batch/1, map_fold_filter_r/4]).

-include("rabbit.hrl").
-include("rabbit_backing_queue.hrl").

new() ->
    gb_trees:empty().

is_empty(T) ->
    gb_trees:is_empty(T).

len(T) ->
    gb_trees:size(T).

in(IndexOnDisk, MsgStatus, Tree) -> in_r(IndexOnDisk, MsgStatus, Tree).

in_r(IndexOnDisk,
     #msg_status { seq_id = SeqId, index_on_disk = IndexOnDisk } = MsgStatus,
     Tree) ->
    gb_trees:insert(SeqId, MsgStatus, Tree);
in_r(IndexOnDisk, _Msgstatus, _Tree) ->
    throw({prefix_and_msg_disagree, IndexOnDisk}).

out(Tree)   -> out1(Tree, fun gb_trees:take_smallest/1).
out_r(Tree) -> out1(Tree, fun gb_trees:take_largest/1).

out1(Tree, TakeFun) ->
   case gb_trees:is_empty(Tree) of
       true  -> {empty, Tree};
       false -> {_Key, #msg_status { index_on_disk = IndexOnDisk } = MsgStatus,
                 Tree2} = TakeFun(Tree),
                {{value, IndexOnDisk, MsgStatus}, Tree2}
   end.

least_key(Tree) ->
    {Least, _} = gb_trees:smallest(Tree),
    Least.

join(T1, T2) ->
   join1(gb_trees:iterator(T1), T2).
join1(Iter, T) ->
    case gb_trees:next(Iter) of
        none    -> T;
        {_SeqId,
         #msg_status { index_on_disk = IndexOnDisk } = MsgStatus,
         Iter1} -> join1(Iter1, in_r(IndexOnDisk, MsgStatus, T))
    end.

join_bpqueue(T, Q) ->
   bpqueue:foldr(fun (IndexOnDisk, MsgStatus, Tree) ->
                     in_r(IndexOnDisk, MsgStatus, Tree)
                 end, T, Q).

foldr(Fun, Acc, Tree) ->
   lists:foldr(Fun, Acc, gb_trees:to_list(Tree)).

from_batch({IndexOnDisk, L}) ->
    lists:foldl(fun (MsgStatus, Tree) ->
                    in_r(IndexOnDisk, MsgStatus, Tree)
                end, new(), L).

map_fold_filter_r(PFilter, Fun, Acc, Tree) ->
    map_fold_filter_r1(PFilter, Fun, Acc, Tree, new()).

map_fold_filter_r1(PFilter, Fun, Acc, TreeOld, TreeNew) ->
    case out_r(TreeOld) of
        {empty, _T} -> {TreeNew, Acc};
        {{value,
          IndexOnDisk, #msg_status{index_on_disk = IndexOnDisk} = MsgStatus},
         TreeOld1} ->
            case PFilter(IndexOnDisk) of
                false ->
                   map_fold_filter_r1(PFilter, Fun, Acc, TreeOld1,
                                      in_r(IndexOnDisk, MsgStatus, TreeNew));
                true ->
                   case Fun(MsgStatus, Acc) of
                       stop ->
                           {join(TreeOld, TreeNew), Acc};
                       {IndexOnDisk1, MsgStatus1, Acc1} ->
                           map_fold_filter_r1(PFilter, Fun, Acc1, TreeOld1,
                                              in_r(IndexOnDisk1, MsgStatus1,
                                              TreeNew))
                   end
            end
   end.

