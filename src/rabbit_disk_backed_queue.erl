-module(rabbit_disk_backed_queue).

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

new([{backing_filename, Filename}]) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [Filename], []),
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

-define(THRESHOLD, 100).

-define(MAGIC_HEADER, <<"DBQ0">>).

-record(state, {filename, dev, gaps, read_pos, tail_pos,
		pushback,
		buffer, buffer_length,
		total_length}).

init([Filename]) ->
    {ok, Dev} = file:open(Filename, [read, write, raw, binary, delayed_write, read_ahead]),
    ok = reset_dev(Dev),
    fresh_state(Filename, Dev).

reset_dev(Dev) ->
    {ok, 0} = file:position(Dev, 0),
    ok = file:truncate(Dev),
    ok = file:write(Dev, ?MAGIC_HEADER),
    ok.

fresh_state(Filename, Dev) ->
    {ok, #state{filename = Filename,
		dev = Dev,
		gaps = intervals:range(size(?MAGIC_HEADER), inf),
		read_pos = 0,
		tail_pos = 0,
		pushback = [],
		buffer = [],
		buffer_length = 0,
		total_length = 0}}.

chunk_at(Dev, ReadPos)
  when ReadPos > 0 ->
    {ok, ReadPos} = file:position(Dev, ReadPos),
    {ok, <<ChunkLen:64/unsigned, NextPos:64/unsigned>>} = file:read(Dev, 16),
    {ok, ChunkBin} = file:read(Dev, ChunkLen),
    {binary_to_term(ChunkBin), ChunkLen, NextPos}.

pop_chunk(State = #state{dev = Dev,
			 pushback = [],
			 read_pos = ReadPos,
			 tail_pos = OldTailPos,
			 gaps = OldGaps,
			 total_length = OldLen}) ->
    {[Item | Chunk], ChunkLen, NextPos} = chunk_at(Dev, ReadPos),
    NewTailPos = if 
		     OldTailPos == ReadPos + 8 -> 0;
		     true -> OldTailPos
		 end,
    NewGaps = intervals:union(OldGaps,
			      intervals:range(ReadPos, ReadPos + ChunkLen + 16)),
    case intervals:first_fit(inf, NewGaps) of
	none -> ok;
	{ok, FirstUnusedByte} ->
	    {ok, FirstUnusedByte} = file:position(Dev, FirstUnusedByte),
	    ok = file:truncate(Dev)
    end,
    {Item, State#state{pushback = Chunk,
		       read_pos = NextPos,
		       tail_pos = NewTailPos,
		       gaps = NewGaps,
		       total_length = OldLen - 1}}.

maybe_evict(State = #state{buffer_length = BufLen})
  when BufLen < ?THRESHOLD ->
    State;
maybe_evict(State = #state{dev = Dev,
			   gaps = OldGaps,
			   read_pos = OldReadPos,
			   tail_pos = OldTailPos,
			   buffer = Buffer}) ->
    ChunkBin = term_to_binary(lists:reverse(Buffer)),
    RequiredSpace = size(ChunkBin) + 16,
    {ok, FirstFit} = intervals:first_fit(RequiredSpace, OldGaps),
    NewGaps = intervals:difference(OldGaps, intervals:range(FirstFit, FirstFit + RequiredSpace)),
    {ok, FirstFit} = file:position(Dev, FirstFit),
    ok = file:write(Dev, [<<(size(ChunkBin)):64/unsigned, 0:64/unsigned>>, ChunkBin]),
    case OldTailPos of
	0 -> ok;
	_ ->
	    {ok, OldTailPos} = file:position(Dev, OldTailPos),
	    ok = file:write(Dev, <<FirstFit:64/unsigned>>)
    end,
    NewReadPos = if
		     OldReadPos == 0 -> FirstFit;
		     true -> OldReadPos
		 end,
    State#state{gaps = NewGaps,
		read_pos = NewReadPos,
		tail_pos = FirstFit + 8,
		buffer = [],
		buffer_length = 0}.

foldl_chunk(_ChunkFun, Acc, _Dev, 0) ->
    Acc;
foldl_chunk(ChunkFun, Acc, Dev, ReadPos) ->
    {Chunk, _ChunkLen, NextPos} = chunk_at(Dev, ReadPos),
    NewAcc = ChunkFun(Chunk, Acc),
    foldl_chunk(ChunkFun, NewAcc, Dev, NextPos).

handle_call(destroy, _From, State) ->
    {stop, normal, ok, State};
handle_call(dequeue, _From, State = #state{total_length = 0}) ->
    {reply, empty, State};
handle_call(dequeue, _From, State = #state{pushback = [Item | Rest],
					   total_length = OldLen}) ->
    {reply, {ok, Item}, State#state{pushback = Rest, total_length = OldLen - 1}};
handle_call(dequeue, _From, State = #state{read_pos = 0,
					   buffer = OldBuf,
					   total_length = OldLen}) ->
    [Item | NewPushback] = lists:reverse(OldBuf),
    {reply, {ok, Item}, State#state{pushback = NewPushback,
				    buffer = [],
				    buffer_length = 0,
				    total_length = OldLen - 1}};
handle_call(dequeue, _From, State) ->
    {Item, NewState} = pop_chunk(State),
    {reply, {ok, Item}, NewState};

handle_call({pushback, Item}, _From, State = #state{pushback = Rest, total_length = OldLen}) ->
    {reply, ok, State#state{pushback = [Item | Rest], total_length = OldLen + 1}};
handle_call({enqueue, Item}, _From, State = #state{buffer = OldBuf,
						   buffer_length = OldBufLen,
						   total_length = OldLen}) ->
    {reply, ok, maybe_evict(State#state{buffer = [Item | OldBuf],
					buffer_length = OldBufLen + 1,
					total_length = OldLen + 1})};
handle_call({enqueue_list, Items}, _From, State = #state{buffer = OldBuf,
							 buffer_length = OldBufLen,
							 total_length = OldLen}) ->
    NItems = length(Items),
    {reply, ok, maybe_evict(State#state{buffer = lists:reverse(Items, OldBuf),
					buffer_length = OldBufLen + NItems,
					total_length = OldLen + NItems})};
handle_call({foreach, F}, _From, State = #state{dev = Dev,
						read_pos = ReadPos,
						pushback = Pushback,
						buffer = Buffer}) ->
    ok = lists:foreach(F, Pushback),
    ok = foldl_chunk(fun (Value, ok) -> ok = lists:foreach(F, Value) end, ok, Dev, ReadPos),
    ok = lists:foreach(F, lists:reverse(Buffer)),
    {reply, ok, State};
handle_call({foldl, F, Acc0}, _From, State = #state{dev = Dev,
						    read_pos = ReadPos,
						    pushback = Pushback,
						    buffer = Buffer}) ->
    Acc1 = lists:foldl(F, Acc0, Pushback),
    Acc2 = foldl_chunk(fun (Value, AccN) -> lists:foldl(F, AccN, Value) end, Acc1, Dev, ReadPos),
    Acc3 = lists:foldl(F, Acc2, lists:reverse(Buffer)),
    {reply, Acc3, State};
handle_call(clear, _From, #state{filename = Filename, dev = Dev}) ->
    ok = reset_dev(Dev),
    {reply, ok, fresh_state(Filename, Dev)};
handle_call(is_empty, _From, State = #state{total_length = Len}) ->
    {reply, case Len of
		0 -> true;
		_ -> false
	    end, State};
handle_call(len, _From, State = #state{total_length = Len}) ->
    {reply, Len, State};
handle_call(_Request, _From, State) ->
    exit({?MODULE, unexpected_call, _Request, _From, State}).

handle_cast(_Msg, State) ->
    exit({?MODULE, unexpected_cast, _Msg, State}).

handle_info(_Info, State) ->
    exit({?MODULE, unexpected_info, _Info, State}).

terminate(_Reason, _State = #state{filename = Filename, dev = Dev}) ->
    _ProbablyOk = file:close(Dev),
    ok = file:delete(Filename),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(TESTFILE, "qqq.tmp").

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
    Pid = ?MODULE:new([{backing_filename, ?TESTFILE}]),
    Max = trunc(?THRESHOLD * 2.5),
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
