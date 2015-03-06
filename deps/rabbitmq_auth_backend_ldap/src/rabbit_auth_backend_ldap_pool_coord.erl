-module(rabbit_auth_backend_ldap_pool_coord).

%% 'public' exports
-export([start_link/0,
         do_work/2,
         get_work/1,
         done_work/3,
         stop/1,
         hello_from_worker/1]).

-behaviour(gen_server).

%% exports to satisfy gen_server
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {idle_workers=queue:new(),
                work_queue=queue:new(),
                busy_workers=dict:new()}).

%% testing stuff ---------------------------------------------------------------

-export([get_info_for_testing/0]).

get_info_for_testing() ->
  gen_server:call(?MODULE, get_info_for_testing).

%% 'public' interface ----------------------------------------------------------

%% called as part of managing the supervision tree:

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Timeout) ->
  try
    gen_server:call(?MODULE, stop, Timeout)
  catch
    exit: {normal, _} -> ok
  end.

%% called by 'client' (process that wants work to be done):

do_work(Work, Timeout) ->
  case gen_server:call(?MODULE, {do_work, Work}, Timeout) of
    {ok, Answer} -> Answer;
    Error -> exit(Error)
  end.

%% called by worker:

get_work(Timeout) ->
  gen_server:call(?MODULE, get_work, Timeout).

done_work(Client={_, _}, Answer, Timeout) ->
  well_done = gen_server:call(?MODULE, {done_work, Client, Answer}, Timeout),
  ok.

hello_from_worker(Timeout) ->
  hello_from_coord = gen_server:call(?MODULE, hello_from_worker, Timeout).

%% gen_server callbacks --------------------------------------------------------

init(Args) ->
  {ok, #state{}}.

handle_call(get_work, Worker, State) ->
  State1 = enqueue_worker(Worker, State),
  State2 = distribute_work(State1),
  {noreply, State2};

handle_call({do_work, Work}, Client, State) ->
  State1 = enqueue_work(Client, Work, State),
  State2 = distribute_work(State1),
  {noreply, State2};

handle_call({done_work, Client={_,_}, Answer}, {WorkerPid, _}, State) ->
  State1 = done_work_impl(Client, Answer, WorkerPid, State),
  {reply, well_done, State1};

handle_call(get_info_for_testing, _,
            State=#state{idle_workers=Workers, work_queue=WorkQueue}) ->
  Info = [{idle_worker_count, queue:len(Workers)},
          {work_queue_count, queue:len(WorkQueue)}],
  {reply, Info, State};

handle_call(stop, _, State) ->
  {stop, normal, State};

handle_call(hello_from_worker, {Worker, _}, State) ->
  erlang:monitor(process, Worker),
  {reply, hello_from_coord, State}.

handle_cast(Request, State) ->
  {noreply, State}.

handle_info(Info={'DOWN', Ref, process, Pid, Reason}, State) ->
  State1 = notify_client_of_worker_failure(Pid, State),
  {noreply, State1};

handle_info(Info, State) ->
  {noreply, State}.

terminate(Reason,State) ->
  ok.

code_change(OldVsn, State, Extra) ->
  {ok, State}.

%% internal stuff --------------------------------------------------------------

enqueue_work(Client={Pid, _}, Work, State=#state{work_queue=WorkQueue}) ->
  %% note that we're not monitor'ing the client; if it dies, then the worker
  %% will die when it tries to reply.
  State1 = State#state{work_queue=queue:in({Client, Work}, WorkQueue)},
  check_invariants(State1),
  State1.

enqueue_worker(Worker={Pid,_}, State=#state{idle_workers=Workers}) ->
  State1 = State#state{idle_workers=queue:in(Worker,Workers)},
  check_invariants(State1),
  State1.

distribute_work(State=#state{idle_workers=IdleWorkers,
                             work_queue=WorkQueue,
                             busy_workers=BusyWorkers}) ->
  State1 = case {queue:out(IdleWorkers), queue:out(WorkQueue)} of
    {{{value, Worker}, Workers1}, {{value, Work}, WorkQueue1}} ->
      try
        gen_server:reply(Worker, Work)
      catch
        E -> io:format("exception: ~p~n", [E])
      end,
      {WorkerPid, _} = Worker,
      BusyWorkers1 = dict:store(WorkerPid, Work, BusyWorkers),
      distribute_work(State#state{idle_workers=Workers1,
                                  work_queue=WorkQueue1,
                                  busy_workers=BusyWorkers1});
    _ -> State
  end,
  check_invariants(State1),
  State1.

check_invariants(State=#state{idle_workers=IdleWorkers,
                              busy_workers=BusyWorkers}) ->
  IdlePidSet = sets:from_list(lists:map(fun ({Pid, _}) -> Pid end,
                                        queue:to_list(IdleWorkers))),
  BusyPidSet = sets:from_list(dict:fetch_keys(BusyWorkers)),
  BothPidList = sets:to_list(sets:intersection(IdlePidSet, BusyPidSet)),
  if
    length(BothPidList) > 0 ->
      exit({invariant_violated, State});
    true ->
      ok
  end,
  State.

done_work_impl(Client, Answer, WorkerPid,
               State=#state{busy_workers=BusyWorkers}) when is_pid(WorkerPid) ->
  gen_server:reply(Client, {ok, Answer}),
  State1 = State#state{busy_workers=dict:erase(WorkerPid, BusyWorkers)},
  check_invariants(State1),
  State1.

notify_client_of_worker_failure(Worker,
                                State=#state{idle_workers=IdleWorkers,
                                             busy_workers=BusyWorkers}) ->
  case dict:find(Worker, BusyWorkers) of
    {ok, Work = {Client, _}} ->
      try
        R = gen_server:reply(Client, {error, worker_failed})
      catch
        E -> io:format(user,"exception: ~p~n", [E])
      end,
      State#state{busy_workers=dict:erase(Worker, BusyWorkers)};
    error ->
      IdleWorkers1 = queue:filter(fun ({Pid, _}) ->
                                      case Pid of
                                        Worker -> false;
                                        _ -> true
                                      end
                                  end, IdleWorkers),
      State#state{idle_workers=IdleWorkers1}
  end.
