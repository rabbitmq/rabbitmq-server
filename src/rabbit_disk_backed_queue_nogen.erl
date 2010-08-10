-module(rabbit_disk_backed_queue_nogen).

-export([new/1, destroy/1,
	 dequeue/1, pushback/2, enqueue/2, enqueue_list/2,
	 foreach/2, foldl/3,
	 clear/1,
	 is_empty/1,
	 len/1]).

new([{backing_filename, Filename}]) ->
    spawn_link(fun () -> init([Filename]) end).

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

-define(THRESHOLD, 100).

-define(MAGIC_HEADER, <<"DBQ0">>).

-record(state, {filename, dev, gaps, read_pos, tail_pos,
		pushback,
		buffer, buffer_length,
		total_length}).

init([Filename]) ->
    {ok, Dev} = file:open(Filename, [read, write, raw, binary, delayed_write, read_ahead]),
    ok = reset_dev(Dev),
    mainloop(fresh_state(Filename, Dev)).

mainloop(State) ->
    receive
	{Requestor, Key, destroy} ->
	    Requestor ! {Key, ok},
	    ok;
	{Requestor, Key, Request} ->
	    {reply, Reply, NewState} = handle(Request, State),
	    Requestor ! {Key, Reply},
	    mainloop(NewState)
    end.

reset_dev(Dev) ->
    {ok, 0} = file:position(Dev, 0),
    ok = file:truncate(Dev),
    ok = file:write(Dev, ?MAGIC_HEADER),
    ok.

fresh_state(Filename, Dev) ->
    #state{filename = Filename,
	   dev = Dev,
	   gaps = intervals:range(size(?MAGIC_HEADER), inf),
	   read_pos = 0,
	   tail_pos = 0,
	   pushback = [],
	   buffer = [],
	   buffer_length = 0,
	   total_length = 0}.

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

handle(dequeue, State = #state{total_length = 0}) ->
    {reply, empty, State};
handle(dequeue, State = #state{pushback = [Item | Rest],
					   total_length = OldLen}) ->
    {reply, {ok, Item}, State#state{pushback = Rest, total_length = OldLen - 1}};
handle(dequeue, State = #state{read_pos = 0,
					   buffer = OldBuf,
					   total_length = OldLen}) ->
    [Item | NewPushback] = lists:reverse(OldBuf),
    {reply, {ok, Item}, State#state{pushback = NewPushback,
				    buffer = [],
				    buffer_length = 0,
				    total_length = OldLen - 1}};
handle(dequeue, State) ->
    {Item, NewState} = pop_chunk(State),
    {reply, {ok, Item}, NewState};

handle({pushback, Item}, State = #state{pushback = Rest, total_length = OldLen}) ->
    {reply, ok, State#state{pushback = [Item | Rest], total_length = OldLen + 1}};
handle({enqueue, Item}, State = #state{buffer = OldBuf,
						   buffer_length = OldBufLen,
						   total_length = OldLen}) ->
    {reply, ok, maybe_evict(State#state{buffer = [Item | OldBuf],
					buffer_length = OldBufLen + 1,
					total_length = OldLen + 1})};
handle({enqueue_list, Items}, State = #state{buffer = OldBuf,
							 buffer_length = OldBufLen,
							 total_length = OldLen}) ->
    NItems = length(Items),
    {reply, ok, maybe_evict(State#state{buffer = lists:reverse(Items, OldBuf),
					buffer_length = OldBufLen + NItems,
					total_length = OldLen + NItems})};
handle({foreach, F}, State = #state{dev = Dev,
						read_pos = ReadPos,
						pushback = Pushback,
						buffer = Buffer}) ->
    ok = lists:foreach(F, Pushback),
    ok = foldl_chunk(fun (Value, ok) -> ok = lists:foreach(F, Value) end, ok, Dev, ReadPos),
    ok = lists:foreach(F, lists:reverse(Buffer)),
    {reply, ok, State};
handle({foldl, F, Acc0}, State = #state{dev = Dev,
						    read_pos = ReadPos,
						    pushback = Pushback,
						    buffer = Buffer}) ->
    Acc1 = lists:foldl(F, Acc0, Pushback),
    Acc2 = foldl_chunk(fun (Value, AccN) -> lists:foldl(F, AccN, Value) end, Acc1, Dev, ReadPos),
    Acc3 = lists:foldl(F, Acc2, lists:reverse(Buffer)),
    {reply, Acc3, State};
handle(clear, #state{filename = Filename, dev = Dev}) ->
    ok = reset_dev(Dev),
    {reply, ok, fresh_state(Filename, Dev)};
handle(is_empty, State = #state{total_length = Len}) ->
    {reply, case Len of
		0 -> true;
		_ -> false
	    end, State};
handle(len, State = #state{total_length = Len}) ->
    {reply, Len, State};
handle(_Request, State) ->
    exit({?MODULE, unexpected_call, _Request, State}).
