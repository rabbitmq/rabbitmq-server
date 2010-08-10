-module(rabbit_ram_backed_queue_nogen).

-export([new/1, destroy/1,
	 dequeue/1, pushback/2, enqueue/2, enqueue_list/2,
	 foreach/2, foldl/3,
	 clear/1,
	 is_empty/1,
	 len/1]).

new(_Options) ->
    spawn_link(fun () -> mainloop(queue:new()) end).

destroy(P) -> rpc(P, destroy).

dequeue(P) -> rpc(P, dequeue).
pushback(Item, P) -> rpc(P, {pushback, Item}).
enqueue(Item, P) -> rpc(P, {enqueue, Item}).
enqueue_list(Items, P) -> rpc(P, {enqueue_list, Items}).
foreach(F, P) -> rpc(P, {foreach, F}).
foldl(F, Acc, P) -> rpc(P, {foldl, F, Acc}).
clear(P) -> rpc(P, clear). 
is_empty(P) -> rpc(P, is_empty). 
len(P) -> rpc(P, len).

rpc(P, Request) ->
    K = make_ref(),
    P ! {self(), K, Request},
    receive {K, Reply} -> Reply end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

mainloop(Q) ->
    receive
	{Requestor, Key, destroy} ->
	    Requestor ! {Key, ok},
	    ok;
	{Requestor, Key, Request} ->
	    {Reply, NewQ} = handle(Request, Q),
	    Requestor ! {Key, Reply},
	    mainloop(NewQ)
    end.

handle(dequeue, Q) ->
    case queue:out(Q) of
	{{value, Item}, NextQ} ->
	    {{ok, Item}, NextQ};
	{empty, _} ->
	    {empty, Q}
    end;
handle({pushback, Item}, Q) ->
    {ok, queue:in_r(Item, Q)};
handle({enqueue, Item}, Q) ->
    {ok, queue:in(Item, Q)};
handle({enqueue_list, Items}, Q) ->
    {ok, queue:join(Q, queue:from_list(Items))};
handle({foreach, F}, Q) ->
    ok = lists:foreach(F, queue:to_list(Q)),
    {ok, Q};
handle({foldl, F, Acc0}, Q) ->
    Acc1 = lists:foldl(F, Acc0, queue:to_list(Q)),
    {Acc1, Q};
handle(clear, _Q) ->
    {ok, queue:new()};
handle(is_empty, Q) ->
    {queue:is_empty(Q), Q};
handle(len, Q) ->
    {queue:len(Q), Q};
handle(_Request, Q) ->
    exit({?MODULE, unexpected_call, _Request, Q}).
