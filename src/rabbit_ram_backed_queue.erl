-module(rabbit_ram_backed_queue).

-behaviour(gen_server).

-export([new/1, destroy/1,
	 dequeue/1, pushback/2, enqueue/2, enqueue_list/2,
	 foreach/2, foldl/3,
	 clear/1,
	 is_empty/1,
	 len/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([test/0]).

new(_Options) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [], []),
    Pid.

destroy(P) -> gen_server:call(P, destroy). 

dequeue(P) -> gen_server:call(P, dequeue).
pushback(Item, P) -> gen_server:call(P, {pushback, Item}).
enqueue(Item, P) -> gen_server:call(P, {enqueue, Item}).
enqueue_list(Items, P) -> gen_server:call(P, {enqueue_list, Items}).
foreach(F, P) -> gen_server:call(P, {foreach, F}, infinity).
foldl(F, Acc, P) -> gen_server:call(P, {foldl, F, Acc}, infinity).
clear(P) -> gen_server:call(P, clear). 
is_empty(P) -> gen_server:call(P, is_empty). 
len(P) -> gen_server:call(P, len).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([]) ->
    {ok, queue:new()}.

handle_call(destroy, _From, Q) ->
    {stop, normal, ok, Q};
handle_call(dequeue, _From, Q) ->
    case queue:out(Q) of
	{{value, Item}, NextQ} ->
	    {reply, {ok, Item}, NextQ};
	{empty, _} ->
	    {reply, empty, Q}
    end;
handle_call({pushback, Item}, _From, Q) ->
    {reply, ok, queue:in_r(Item, Q)};
handle_call({enqueue, Item}, _From, Q) ->
    {reply, ok, queue:in(Item, Q)};
handle_call({enqueue_list, Items}, _From, Q) ->
    {reply, ok, queue:join(Q, queue:from_list(Items))};
handle_call({foreach, F}, _From, Q) ->
    ok = lists:foreach(F, queue:to_list(Q)),
    {reply, ok, Q};
handle_call({foldl, F, Acc0}, _From, Q) ->
    Acc1 = lists:foldl(F, Acc0, queue:to_list(Q)),
    {reply, Acc1, Q};
handle_call(clear, _From, _Q) ->
    {reply, ok, queue:new()};
handle_call(is_empty, _From, Q) ->
    {reply, queue:is_empty(Q), Q};
handle_call(len, _From, Q) ->
    {reply, queue:len(Q), Q};
handle_call(_Request, _From, Q) ->
    exit({?MODULE, unexpected_call, _Request, _From, Q}).

handle_cast(_Msg, Q) ->
    exit({?MODULE, unexpected_cast, _Msg, Q}).

handle_info(_Info, Q) ->
    exit({?MODULE, unexpected_info, _Info, Q}).

terminate(_Reason, _Q) ->
    ok.

code_change(_OldVsn, Q, _Extra) ->
    {ok, Q}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

test_insert_upto(_Pid, Lo, Hi)
  when Lo >= Hi ->
    ok;
test_insert_upto(Pid, Lo, Hi) ->
    ok = ?MODULE:enqueue(Lo, Pid),
    test_insert_upto(Pid, Lo + 1, Hi).

test_remove_upto(Pid, Lo, Hi)
  when Lo >= Hi ->
    empty = ?MODULE:dequeue(Pid),
    ok;
test_remove_upto(Pid, Lo, Hi) ->
    {ok, Lo} = ?MODULE:dequeue(Pid),
    test_remove_upto(Pid, Lo + 1, Hi).

test() ->
    Pid = ?MODULE:new([]),
    Max = 11,
    Mid = trunc(Max / 2),
    ok = test_insert_upto(Pid, 0, Max),
    AllItems = lists:seq(0, Max - 1),
    AllItems = lists:reverse(?MODULE:foldl(fun (X, Acc) -> [X | Acc] end, [], Pid)),
    ok = test_remove_upto(Pid, 0, Max),

    ok = test_insert_upto(Pid, 0, Mid),
    {ok, 0} = ?MODULE:dequeue(Pid),
    ok = ?MODULE:pushback(abc, Pid),
    ok = test_insert_upto(Pid, Mid, Max),
    {ok, abc} = ?MODULE:dequeue(Pid),
    ok = test_remove_upto(Pid, 1, Max),

    %% ok = ?MODULE:destroy(Pid),
    ok.
