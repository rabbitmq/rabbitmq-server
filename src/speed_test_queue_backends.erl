-module(speed_test_queue_backends).

-compile([export_all]).

-define(M, rabbit_disk_backed_queue).
%%-define(M, rabbit_disk_backed_queue_nogen).
%%-define(M, rabbit_ram_backed_queue).
%%-define(M, rabbit_ram_backed_queue_nogen).

fill_drain_noproc(N, Size) ->
    summarize(fill_drain_noproc, N, Size,
	      fun (_Q) ->
		      drain_f(enqueue_n_f(queue:new(), N, blob_for_size(Size)))
	      end).

fill_drain(N, Size) ->
    summarize(fill_drain, N, Size, fun (Q) ->
					   enqueue_n(Q, N, blob_for_size(Size)),
					   drain(Q)
				   end).

fill_destroy(N, Size) ->
    summarize(fill_destroy, N, Size, fun (Q) ->
					     enqueue_n(Q, N, blob_for_size(Size))
				     end).

simultaneous_drain(N, Size) ->
    summarize(simultaneous_drain, N, Size,
	      fun (Q) ->
		      Parent = self(),
		      spawn_link(fun () ->
					 enqueue_n(Q, N, blob_for_size(Size)),
					 ?M:enqueue(done, Q),
					 Parent ! done1
				 end),
		      spawn_link(fun () ->
					 drain_until(done, Q),
					 Parent ! done2
				 end),
		      receive done1 -> ok end,
		      receive done2 -> ok end
	      end).

blob_for_size(Size) ->
    SizeBits = Size * 8,
    <<99:SizeBits/integer>>.

enqueue_n_f(Q, 0, _Blob) ->
    Q;
enqueue_n_f(Q, N, Blob) ->
    enqueue_n_f(queue:in(Blob, Q), N - 1, Blob).

drain_f(Q) ->
    case queue:out(Q) of
	{{value, _}, Q1} ->
	    drain_f(Q1);
	{empty, _} ->
	    ok
    end.

enqueue_n(_Q, 0, _Blob) ->
    ok;
enqueue_n(Q, N, Blob) ->
    ?M:enqueue(Blob, Q),
    enqueue_n(Q, N - 1, Blob).

drain_until(What, Q) ->
    case ?M:dequeue(Q) of
	empty ->
	    drain_until(What, Q);
	{ok, What} ->
	    ok;
	{ok, _Other} ->
	    drain_until(What, Q)
    end.

drain(Q) ->
    case ?M:dequeue(Q) of
	empty ->
	    ok;
	{ok, _Item} ->
	    drain(Q)
    end.

summarize(Kind, N, Size, F) ->
    TimeMicrosec = with_q(F),
    io:format("~p(~p, ~p) using ~p: ~p microsec, ~p Hz~n",
	      [Kind, N, Size, ?M,
	       TimeMicrosec,
	       float(N) / (TimeMicrosec / 1000000.0)]),
    ok.

with_q(F) ->
    Q = ?M:new([{backing_filename, "/tmp/speed_test_queue_backends.tmp"}]),
    {TimeMicrosec, _Result} = timer:tc(erlang, apply, [F, [Q]]),
    ok = ?M:destroy(Q),
    TimeMicrosec.
