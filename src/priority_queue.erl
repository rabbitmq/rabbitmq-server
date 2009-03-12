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

%% Priority queues have essentially the same interface as ordinary
%% queues, except that a) there is an in/3 that takes a priority, and
%% b) we have only implemented the core API we need.
%%
%% Priorities should be integers - the higher the value the higher the
%% priority - but we don't actually check that.
%%
%% in/2 inserts items with the highest priority.
%%
%% We optimise the case where a priority queue is being used just like
%% an ordinary queue. When that is the case we represent the priority
%% queue as an ordinary queue. We could just call into the 'queue'
%% module for that, but for efficiency we implement the relevant
%% functions directly in here, thus saving on inter-module calls and
%% eliminating a level of boxing.
%%
%% When in/3 is invoked for the first time we change the
%% representation from an ordinary queue to a gb_tree with {Priority,
%% Counter} as the key. Counter is incremented with for every 'in',

-module(priority_queue).

-export([new/0, is_queue/1, is_empty/1, len/1, to_list/1, in/2, in/3, out/1]).

new() ->
    {queue, [], []}.

is_queue({queue, R, F}) when is_list(R), is_list(F) ->
    true;
is_queue({pqueue, Counter, _Tree}) when is_integer(Counter), Counter >= 0 ->
    true;
is_queue(_) ->
    false.

is_empty({queue, [], []}) ->
    true;
is_empty({queue, In,Out}) when is_list(In), is_list(Out) ->
    false;
is_empty({pqueue, _, Tree}) ->
    gb_trees:is_empty(Tree).

len({queue, R, F}) when is_list(R), is_list(F) ->
    length(R) + length(F);
len({pqueue, _, Tree}) ->
    gb_trees:size(Tree).

to_list({queue, In, Out}) when is_list(In), is_list(Out) ->
    Out ++ lists:reverse(In, []);
to_list({pqueue, _, Tree}) ->
    gb_trees:to_list(Tree).

in(X, {queue, [_] = In, []}) ->
    {queue, [X], In};
in(X, {queue, In, Out}) when is_list(In), is_list(Out) ->
    {queue, [X|In], Out};
in(Item, Other) ->
    in(Item, infinity, Other).

in(Item, Priority, {queue, In, Out}) ->
    {Counter, Tree} = to_tree(In, Out),
    in(Item, Priority, {pqueue, Counter, Tree});
in(Item, Priority, {pqueue, Counter, Tree}) ->
    {pqueue, Counter + 1, gb_trees:insert({Priority, Counter}, Item, Tree)}.

out({queue, [], []} = Q) ->
    {empty, Q};
out({queue, [V], []}) ->
    {{value, V}, {queue, [], []}};
out({queue, [Y|In], []}) ->
    [V|Out] = lists:reverse(In, []),
    {{value, V}, {queue, [Y], Out}};
out({queue, In, [V]}) when is_list(In) ->
    {{value,V}, r2f(In)};
out({queue, In,[V|Out]}) when is_list(In) ->
    {{value, V}, {queue, In, Out}};
out({pqueue, Counter, Tree}) ->
    {_, Item, Tree1} = gb_trees:take_smallest(Tree),
    {{value, Item}, case gb_trees:is_empty(Tree1) of
                        true  -> {queue, queue:new()};
                        false -> {pqueue, Counter, Tree1}
                    end}.

to_tree(In, Out) ->
    lists:foldl(fun (V, {C, T}) ->
                        {C + 1, gb_trees:insert({infinity, C}, V, T)}
                end, {0, gb_trees:empty()}, Out ++ lists:reverse(In, [])).

r2f([]) ->
    {queue, [], []};
r2f([_] = R) ->
    {queue, [], R};
r2f([X,Y]) ->
    {queue, [X], [Y]};
r2f([X,Y|R]) ->
    {queue, [X,Y], lists:reverse(R, [])}.
