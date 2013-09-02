%% This file is a copy of supervisor.erl from the R16B Erlang/OTP
%% distribution, with the following modifications:
%%
%% 1) the module name is supervisor2
%%
%% 2) a find_child/2 utility function has been added
%%
%% 3) Added an 'intrinsic' restart type. Like the transient type, this
%%    type means the child should only be restarted if the child exits
%%    abnormally. Unlike the transient type, if the child exits
%%    normally, the supervisor itself also exits normally. If the
%%    child is a supervisor and it exits normally (i.e. with reason of
%%    'shutdown') then the child's parent also exits normally.
%%
%% 4) child specifications can contain, as the restart type, a tuple
%%    {permanent, Delay} | {transient, Delay} | {intrinsic, Delay}
%%    where Delay >= 0 (see point (4) below for intrinsic). The delay,
%%    in seconds, indicates what should happen if a child, upon being
%%    restarted, exceeds the MaxT and MaxR parameters. Thus, if a
%%    child exits, it is restarted as normal. If it exits sufficiently
%%    quickly and often to exceed the boundaries set by the MaxT and
%%    MaxR parameters, and a Delay is specified, then rather than
%%    stopping the supervisor, the supervisor instead continues and
%%    tries to start up the child again, Delay seconds later.
%%
%%    Note that you can never restart more frequently than the MaxT
%%    and MaxR parameters allow: i.e. you must wait until *both* the
%%    Delay has passed *and* the MaxT and MaxR parameters allow the
%%    child to be restarted.
%%
%%    Also note that the Delay is a *minimum*. There is no guarantee
%%    that the child will be restarted within that time, especially if
%%    other processes are dying and being restarted at the same time -
%%    essentially we have to wait for the delay to have passed and for
%%    the MaxT and MaxR parameters to permit the child to be
%%    restarted. This may require waiting for longer than Delay.
%%
%%    Sometimes, you may wish for a transient or intrinsic child to
%%    exit abnormally so that it gets restarted, but still log
%%    nothing. gen_server will log any exit reason other than
%%    'normal', 'shutdown' or {'shutdown', _}. Thus the exit reason of
%%    {'shutdown', 'restart'} is interpreted to mean you wish the
%%    child to be restarted according to the delay parameters, but
%%    gen_server will not log the error. Thus from gen_server's
%%    perspective it's a normal exit, whilst from supervisor's
%%    perspective, it's an abnormal exit.
%%
%% 5) normal, and {shutdown, _} exit reasons are all treated the same
%%    (i.e. are regarded as normal exits)
%%
%% All modifications are (C) 2010-2013 GoPivotal, Inc.
%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2012. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(supervisor2).

-behaviour(gen_server).

%% External exports
-export([start_link/2, start_link/3,
	 start_child/2, restart_child/2,
	 delete_child/2, terminate_child/2,
	 which_children/1, count_children/1,
	 find_child/2, check_childspecs/1]).

%% Internal exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-export([try_again_restart/3]).

%%--------------------------------------------------------------------------
-ifdef(use_specs).
-export_type([child_spec/0, startchild_ret/0, strategy/0, sup_name/0]).
-endif.
%%--------------------------------------------------------------------------

-ifdef(use_specs).
-type child()    :: 'undefined' | pid().
-type child_id() :: term().
-type mfargs()   :: {M :: module(), F :: atom(), A :: [term()] | undefined}.
-type modules()  :: [module()] | 'dynamic'.
-type delay()    :: non_neg_integer().
-type restart()  :: 'permanent' | 'transient' | 'temporary' | 'intrinsic' | {'permanent', delay()} | {'transient', delay()} | {'intrinsic', delay()}.
-type shutdown() :: 'brutal_kill' | timeout().
-type worker()   :: 'worker' | 'supervisor'.
-type sup_name() :: {'local', Name :: atom()} | {'global', Name :: atom()}.
-type sup_ref()  :: (Name :: atom())
                  | {Name :: atom(), Node :: node()}
                  | {'global', Name :: atom()}
                  | pid().
-type child_spec() :: {Id :: child_id(),
                       StartFunc :: mfargs(),
                       Restart :: restart(),
                       Shutdown :: shutdown(),
                       Type :: worker(),
                       Modules :: modules()}.

-type strategy() :: 'one_for_all' | 'one_for_one'
                  | 'rest_for_one' | 'simple_one_for_one'.
-endif.

%%--------------------------------------------------------------------------

-ifdef(use_specs).
-record(child, {% pid is undefined when child is not running
	        pid = undefined :: child() | {restarting,pid()} | [pid()],
		name            :: child_id(),
		mfargs          :: mfargs(),
		restart_type    :: restart(),
		shutdown        :: shutdown(),
		child_type      :: worker(),
		modules = []    :: modules()}).
-type child_rec() :: #child{}.
-else.
-record(child, {
	        pid = undefined,
		name,
		mfargs,
		restart_type,
		shutdown,
		child_type,
		modules = []}).
-endif.

-define(DICT, dict).
-define(SETS, sets).
-define(SET, set).

-ifdef(use_specs).
-record(state, {name,
		strategy               :: strategy(),
		children = []          :: [child_rec()],
		dynamics               :: ?DICT() | ?SET(),
		intensity              :: non_neg_integer(),
		period                 :: pos_integer(),
		restarts = [],
	        module,
	        args}).
-type state() :: #state{}.
-else.
-record(state, {name,
		strategy,
		children = [],
		dynamics,
		intensity,
		period,
		restarts = [],
	        module,
	        args}).
-endif.

-define(is_simple(State), State#state.strategy =:= simple_one_for_one).
-define(is_permanent(R), ((R =:= permanent) orelse
                          (is_tuple(R) andalso
                           tuple_size(R) == 2 andalso
                           element(1, R) =:= permanent))).
-define(is_explicit_restart(R),
        R == {shutdown, restart}).

-ifdef(use_specs).
-callback init(Args :: term()) ->
    {ok, {{RestartStrategy :: strategy(),
           MaxR            :: non_neg_integer(),
           MaxT            :: non_neg_integer()},
           [ChildSpec :: child_spec()]}}
    | ignore.
-endif.
-define(restarting(_Pid_), {restarting,_Pid_}).

%%% ---------------------------------------------------
%%% This is a general process supervisor built upon gen_server.erl.
%%% Servers/processes should/could also be built using gen_server.erl.
%%% SupName = {local, atom()} | {global, atom()}.
%%% ---------------------------------------------------
-ifdef(use_specs).
-type startlink_err() :: {'already_started', pid()}
                         | {'shutdown', term()}
                         | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link(Module, Args) -> startlink_ret() when
      Module :: module(),
      Args :: term().

-endif.
start_link(Mod, Args) ->
    gen_server:start_link(?MODULE, {self, Mod, Args}, []).
 
-ifdef(use_specs).
-spec start_link(SupName, Module, Args) -> startlink_ret() when
      SupName :: sup_name(),
      Module :: module(),
      Args :: term().
-endif.
start_link(SupName, Mod, Args) ->
    gen_server:start_link(SupName, ?MODULE, {SupName, Mod, Args}, []).
 
%%% ---------------------------------------------------
%%% Interface functions.
%%% ---------------------------------------------------
-ifdef(use_specs).
-type startchild_err() :: 'already_present'
			| {'already_started', Child :: child()} | term().
-type startchild_ret() :: {'ok', Child :: child()}
                        | {'ok', Child :: child(), Info :: term()}
			| {'error', startchild_err()}.

-spec start_child(SupRef, ChildSpec) -> startchild_ret() when
      SupRef :: sup_ref(),
      ChildSpec :: child_spec() | (List :: [term()]).
-endif.
start_child(Supervisor, ChildSpec) ->
    call(Supervisor, {start_child, ChildSpec}).

-ifdef(use_specs).
-spec restart_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: {'ok', Child :: child()}
              | {'ok', Child :: child(), Info :: term()}
              | {'error', Error},
      Error :: 'running' | 'restarting' | 'not_found' | 'simple_one_for_one' |
	       term().
-endif.
restart_child(Supervisor, Name) ->
    call(Supervisor, {restart_child, Name}).

-ifdef(use_specs).
-spec delete_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'running' | 'restarting' | 'not_found' | 'simple_one_for_one'.
-endif.
delete_child(Supervisor, Name) ->
    call(Supervisor, {delete_child, Name}).

%%-----------------------------------------------------------------
%% Func: terminate_child/2
%% Returns: ok | {error, Reason}
%%          Note that the child is *always* terminated in some
%%          way (maybe killed).
%%-----------------------------------------------------------------
-ifdef(use_specs).
-spec terminate_child(SupRef, Id) -> Result when
      SupRef :: sup_ref(),
      Id :: pid() | child_id(),
      Result :: 'ok' | {'error', Error},
      Error :: 'not_found' | 'simple_one_for_one'.
-endif.
terminate_child(Supervisor, Name) ->
    call(Supervisor, {terminate_child, Name}).

-ifdef(use_specs).
-spec which_children(SupRef) -> [{Id,Child,Type,Modules}] when
      SupRef :: sup_ref(),
      Id :: child_id() | undefined,
      Child :: child() | 'restarting',
      Type :: worker(),
      Modules :: modules().
-endif.
which_children(Supervisor) ->
    call(Supervisor, which_children).

-ifdef(use_specs).
-spec count_children(SupRef) -> PropListOfCounts when
      SupRef :: sup_ref(),
      PropListOfCounts :: [Count],
      Count :: {specs, ChildSpecCount :: non_neg_integer()}
             | {active, ActiveProcessCount :: non_neg_integer()}
             | {supervisors, ChildSupervisorCount :: non_neg_integer()}
             |{workers, ChildWorkerCount :: non_neg_integer()}.
-endif.
count_children(Supervisor) ->
    call(Supervisor, count_children).

-ifdef(use_specs).
-spec find_child(Supervisor, Name) -> [pid()] when
      Supervisor :: sup_ref(),
      Name :: child_id().
-endif.
find_child(Supervisor, Name) ->
    [Pid || {Name1, Pid, _Type, _Modules} <- which_children(Supervisor),
            Name1 =:= Name].

call(Supervisor, Req) ->
    gen_server:call(Supervisor, Req, infinity).

-ifdef(use_specs).
-spec check_childspecs(ChildSpecs) -> Result when
      ChildSpecs :: [child_spec()],
      Result :: 'ok' | {'error', Error :: term()}.
-endif.
check_childspecs(ChildSpecs) when is_list(ChildSpecs) ->
    case check_startspec(ChildSpecs) of
	{ok, _} -> ok;
	Error -> {error, Error}
    end;
check_childspecs(X) -> {error, {badarg, X}}.

%%%-----------------------------------------------------------------
%%% Called by timer:apply_after from restart/2
-ifdef(use_specs).
-spec try_again_restart(SupRef, Child, Reason) -> ok when
      SupRef :: sup_ref(),
      Child :: child_id() | pid(),
      Reason :: term().
-endif.
try_again_restart(Supervisor, Child, Reason) ->
    cast(Supervisor, {try_again_restart, Child, Reason}).

cast(Supervisor, Req) ->
    gen_server:cast(Supervisor, Req).

%%% ---------------------------------------------------
%%% 
%%% Initialize the supervisor.
%%% 
%%% ---------------------------------------------------
-ifdef(use_specs).
-type init_sup_name() :: sup_name() | 'self'.

-type stop_rsn() :: {'shutdown', term()}
                  | {'bad_return', {module(),'init', term()}}
                  | {'bad_start_spec', term()}
                  | {'start_spec', term()}
                  | {'supervisor_data', term()}.

-spec init({init_sup_name(), module(), [term()]}) ->
        {'ok', state()} | 'ignore' | {'stop', stop_rsn()}.
-endif.
init({SupName, Mod, Args}) ->
    process_flag(trap_exit, true),
    case Mod:init(Args) of
	{ok, {SupFlags, StartSpec}} ->
	    case init_state(SupName, SupFlags, Mod, Args) of
		{ok, State} when ?is_simple(State) ->
		    init_dynamic(State, StartSpec);
		{ok, State} ->
		    init_children(State, StartSpec);
		Error ->
		    {stop, {supervisor_data, Error}}
	    end;
	ignore ->
	    ignore;
	Error ->
	    {stop, {bad_return, {Mod, init, Error}}}
    end.

init_children(State, StartSpec) ->
    SupName = State#state.name,
    case check_startspec(StartSpec) of
        {ok, Children} ->
            case start_children(Children, SupName) of
                {ok, NChildren} ->
                    {ok, State#state{children = NChildren}};
                {error, NChildren, Reason} ->
                    terminate_children(NChildren, SupName),
                    {stop, {shutdown, Reason}}
            end;
        Error ->
            {stop, {start_spec, Error}}
    end.

init_dynamic(State, [StartSpec]) ->
    case check_startspec([StartSpec]) of
        {ok, Children} ->
	    {ok, State#state{children = Children}};
        Error ->
            {stop, {start_spec, Error}}
    end;
init_dynamic(_State, StartSpec) ->
    {stop, {bad_start_spec, StartSpec}}.

%%-----------------------------------------------------------------
%% Func: start_children/2
%% Args: Children = [child_rec()] in start order
%%       SupName = {local, atom()} | {global, atom()} | {pid(), Mod}
%% Purpose: Start all children.  The new list contains #child's
%%          with pids.
%% Returns: {ok, NChildren} | {error, NChildren, Reason}
%%          NChildren = [child_rec()] in termination order (reversed
%%                        start order)
%%-----------------------------------------------------------------
start_children(Children, SupName) -> start_children(Children, [], SupName).

start_children([Child|Chs], NChildren, SupName) ->
    case do_start_child(SupName, Child) of
	{ok, undefined} when Child#child.restart_type =:= temporary ->
	    start_children(Chs, NChildren, SupName);
	{ok, Pid} ->
	    start_children(Chs, [Child#child{pid = Pid}|NChildren], SupName);
	{ok, Pid, _Extra} ->
	    start_children(Chs, [Child#child{pid = Pid}|NChildren], SupName);
	{error, Reason} ->
	    report_error(start_error, Reason, Child, SupName),
	    {error, lists:reverse(Chs) ++ [Child | NChildren],
	     {failed_to_start_child,Child#child.name,Reason}}
    end;
start_children([], NChildren, _SupName) ->
    {ok, NChildren}.

do_start_child(SupName, Child) ->
    #child{mfargs = {M, F, Args}} = Child,
    case catch apply(M, F, Args) of
	{ok, Pid} when is_pid(Pid) ->
	    NChild = Child#child{pid = Pid},
	    report_progress(NChild, SupName),
	    {ok, Pid};
	{ok, Pid, Extra} when is_pid(Pid) ->
	    NChild = Child#child{pid = Pid},
	    report_progress(NChild, SupName),
	    {ok, Pid, Extra};
	ignore ->
	    {ok, undefined};
	{error, What} -> {error, What};
	What -> {error, What}
    end.

do_start_child_i(M, F, A) ->
    case catch apply(M, F, A) of
	{ok, Pid} when is_pid(Pid) ->
	    {ok, Pid};
	{ok, Pid, Extra} when is_pid(Pid) ->
	    {ok, Pid, Extra};
	ignore ->
	    {ok, undefined};
	{error, Error} ->
	    {error, Error};
	What ->
	    {error, What}
    end.

%%% ---------------------------------------------------
%%% 
%%% Callback functions.
%%% 
%%% ---------------------------------------------------
-ifdef(use_specs).
-type call() :: 'which_children' | 'count_children' | {_, _}.	% XXX: refine
-spec handle_call(call(), term(), state()) -> {'reply', term(), state()}.
-endif.
handle_call({start_child, EArgs}, _From, State) when ?is_simple(State) ->
    Child = hd(State#state.children),
    #child{mfargs = {M, F, A}} = Child,
    Args = A ++ EArgs,
    case do_start_child_i(M, F, Args) of
	{ok, undefined} when Child#child.restart_type =:= temporary ->
	    {reply, {ok, undefined}, State};
	{ok, Pid} ->
	    NState = save_dynamic_child(Child#child.restart_type, Pid, Args, State),
	    {reply, {ok, Pid}, NState};
	{ok, Pid, Extra} ->
	    NState = save_dynamic_child(Child#child.restart_type, Pid, Args, State),
	    {reply, {ok, Pid, Extra}, NState};
	What ->
	    {reply, What, State}
    end;

%% terminate_child for simple_one_for_one can only be done with pid
handle_call({terminate_child, Name}, _From, State) when not is_pid(Name),
							?is_simple(State) ->
    {reply, {error, simple_one_for_one}, State};

handle_call({terminate_child, Name}, _From, State) ->
    case get_child(Name, State, ?is_simple(State)) of
	{value, Child} ->
	    case do_terminate(Child, State#state.name) of
		#child{restart_type=RT} when RT=:=temporary; ?is_simple(State) ->
		    {reply, ok, state_del_child(Child, State)};
		NChild ->
		    {reply, ok, replace_child(NChild, State)}
		end;
	false ->
	    {reply, {error, not_found}, State}
    end;

%%% The requests delete_child and restart_child are invalid for
%%% simple_one_for_one supervisors.
handle_call({_Req, _Data}, _From, State) when ?is_simple(State) ->
    {reply, {error, simple_one_for_one}, State};

handle_call({start_child, ChildSpec}, _From, State) ->
    case check_childspec(ChildSpec) of
	{ok, Child} ->
	    {Resp, NState} = handle_start_child(Child, State),
	    {reply, Resp, NState};
	What ->
	    {reply, {error, What}, State}
    end;

handle_call({restart_child, Name}, _From, State) ->
    case get_child(Name, State) of
	{value, Child} when Child#child.pid =:= undefined ->
	    case do_start_child(State#state.name, Child) of
		{ok, Pid} ->
		    NState = replace_child(Child#child{pid = Pid}, State),
		    {reply, {ok, Pid}, NState};
		{ok, Pid, Extra} ->
		    NState = replace_child(Child#child{pid = Pid}, State),
		    {reply, {ok, Pid, Extra}, NState};
		Error ->
		    {reply, Error, State}
	    end;
	{value, #child{pid=?restarting(_)}} ->
	    {reply, {error, restarting}, State};
	{value, _} ->
	    {reply, {error, running}, State};
	_ ->
	    {reply, {error, not_found}, State}
    end;

handle_call({delete_child, Name}, _From, State) ->
    case get_child(Name, State) of
	{value, Child} when Child#child.pid =:= undefined ->
	    NState = remove_child(Child, State),
	    {reply, ok, NState};
	{value, #child{pid=?restarting(_)}} ->
	    {reply, {error, restarting}, State};
	{value, _} ->
	    {reply, {error, running}, State};
	_ ->
	    {reply, {error, not_found}, State}
    end;

handle_call(which_children, _From, #state{children = [#child{restart_type = temporary,
							     child_type = CT,
							     modules = Mods}]} =
		State) when ?is_simple(State) ->
    Reply = lists:map(fun(Pid) -> {undefined, Pid, CT, Mods} end,
                      ?SETS:to_list(dynamics_db(temporary, State#state.dynamics))),
    {reply, Reply, State};

handle_call(which_children, _From, #state{children = [#child{restart_type = RType,
							 child_type = CT,
							 modules = Mods}]} =
		State) when ?is_simple(State) ->
    Reply = lists:map(fun({?restarting(_),_}) -> {undefined,restarting,CT,Mods};
			 ({Pid, _}) -> {undefined, Pid, CT, Mods} end,
		      ?DICT:to_list(dynamics_db(RType, State#state.dynamics))),
    {reply, Reply, State};

handle_call(which_children, _From, State) ->
    Resp =
	lists:map(fun(#child{pid = ?restarting(_), name = Name,
			     child_type = ChildType, modules = Mods}) ->
			  {Name, restarting, ChildType, Mods};
		     (#child{pid = Pid, name = Name,
			     child_type = ChildType, modules = Mods}) ->
			  {Name, Pid, ChildType, Mods}
		  end,
		  State#state.children),
    {reply, Resp, State};


handle_call(count_children, _From, #state{children = [#child{restart_type = temporary,
							     child_type = CT}]} = State)
  when ?is_simple(State) ->
    {Active, Count} =
	?SETS:fold(fun(Pid, {Alive, Tot}) ->
			   case is_pid(Pid) andalso is_process_alive(Pid) of
			       true ->{Alive+1, Tot +1};
			       false ->
				   {Alive, Tot + 1}
			   end
		   end, {0, 0}, dynamics_db(temporary, State#state.dynamics)),
    Reply = case CT of
		supervisor -> [{specs, 1}, {active, Active},
			       {supervisors, Count}, {workers, 0}];
		worker -> [{specs, 1}, {active, Active},
			   {supervisors, 0}, {workers, Count}]
	    end,
    {reply, Reply, State};

handle_call(count_children, _From,  #state{children = [#child{restart_type = RType,
							      child_type = CT}]} = State)
  when ?is_simple(State) ->
    {Active, Count} =
	?DICT:fold(fun(Pid, _Val, {Alive, Tot}) ->
			   case is_pid(Pid) andalso is_process_alive(Pid) of
			       true ->
				   {Alive+1, Tot +1};
			       false ->
				   {Alive, Tot + 1}
			   end
		   end, {0, 0}, dynamics_db(RType, State#state.dynamics)),
    Reply = case CT of
		supervisor -> [{specs, 1}, {active, Active},
			       {supervisors, Count}, {workers, 0}];
		worker -> [{specs, 1}, {active, Active},
			   {supervisors, 0}, {workers, Count}]
	    end,
    {reply, Reply, State};

handle_call(count_children, _From, State) ->
    %% Specs and children are together on the children list...
    {Specs, Active, Supers, Workers} =
	lists:foldl(fun(Child, Counts) ->
			   count_child(Child, Counts)
		   end, {0,0,0,0}, State#state.children),

    %% Reformat counts to a property list.
    Reply = [{specs, Specs}, {active, Active},
	     {supervisors, Supers}, {workers, Workers}],
    {reply, Reply, State}.


count_child(#child{pid = Pid, child_type = worker},
	    {Specs, Active, Supers, Workers}) ->
    case is_pid(Pid) andalso is_process_alive(Pid) of
	true ->  {Specs+1, Active+1, Supers, Workers+1};
	false -> {Specs+1, Active, Supers, Workers+1}
    end;
count_child(#child{pid = Pid, child_type = supervisor},
	    {Specs, Active, Supers, Workers}) ->
    case is_pid(Pid) andalso is_process_alive(Pid) of
	true ->  {Specs+1, Active+1, Supers+1, Workers};
	false -> {Specs+1, Active, Supers+1, Workers}
    end.


%%% If a restart attempt failed, this message is sent via
%%% timer:apply_after(0,...) in order to give gen_server the chance to
%%% check it's inbox before trying again.
-ifdef(use_specs).
-spec handle_cast({try_again_restart, child_id() | pid(), term()}, state()) ->
			 {'noreply', state()} | {stop, shutdown, state()}.
-endif.
handle_cast({try_again_restart,Pid,Reason}, #state{children=[Child]}=State)
  when ?is_simple(State) ->
    RT = Child#child.restart_type,
    RPid = restarting(Pid),
    case dynamic_child_args(RPid, dynamics_db(RT, State#state.dynamics)) of
	{ok, Args} ->
	    {M, F, _} = Child#child.mfargs,
	    NChild = Child#child{pid = RPid, mfargs = {M, F, Args}},
            try_restart(Child#child.restart_type, Reason, NChild, State);
	error ->
            {noreply, State}
    end;

handle_cast({try_again_restart,Name,Reason}, State) ->
    %% we still support >= R12-B3 in which lists:keyfind/3 doesn't exist
    case lists:keysearch(Name,#child.name,State#state.children) of
	{value, Child = #child{pid=?restarting(_), restart_type=RestartType}} ->
            try_restart(RestartType, Reason, Child, State);
	_ ->
	    {noreply,State}
    end.

%%
%% Take care of terminated children.
%%
-ifdef(use_specs).
-spec handle_info(term(), state()) ->
        {'noreply', state()} | {'stop', 'shutdown', state()}.
-endif.
handle_info({'EXIT', Pid, Reason}, State) ->
    case restart_child(Pid, Reason, State) of
	{ok, State1} ->
	    {noreply, State1};
	{shutdown, State1} ->
	    {stop, shutdown, State1}
    end;

handle_info({delayed_restart, {RestartType, Reason, Child}}, State)
  when ?is_simple(State) ->
    try_restart(RestartType, Reason, Child, State);
handle_info({delayed_restart, {RestartType, Reason, Child}}, State) ->
    case get_child(Child#child.name, State) of
        {value, Child1} ->
            try_restart(RestartType, Reason, Child1, State);
        _What ->
            {noreply, State}
    end;

handle_info(Msg, State) ->
    error_logger:error_msg("Supervisor received unexpected message: ~p~n", 
			   [Msg]),
    {noreply, State}.

%%
%% Terminate this server.
%%
-ifdef(use_specs).
-spec terminate(term(), state()) -> 'ok'.
-endif.
terminate(_Reason, #state{children=[Child]} = State) when ?is_simple(State) ->
    terminate_dynamic_children(Child, dynamics_db(Child#child.restart_type,
                                                  State#state.dynamics),
                               State#state.name);
terminate(_Reason, State) ->
    terminate_children(State#state.children, State#state.name).

%%
%% Change code for the supervisor.
%% Call the new call-back module and fetch the new start specification.
%% Combine the new spec. with the old. If the new start spec. is
%% not valid the code change will not succeed.
%% Use the old Args as argument to Module:init/1.
%% NOTE: This requires that the init function of the call-back module
%%       does not have any side effects.
%%
-ifdef(use_specs).
-spec code_change(term(), state(), term()) ->
        {'ok', state()} | {'error', term()}.
-endif.
code_change(_, State, _) ->
    case (State#state.module):init(State#state.args) of
	{ok, {SupFlags, StartSpec}} ->
	    case catch check_flags(SupFlags) of
		ok ->
		    {Strategy, MaxIntensity, Period} = SupFlags,
                    update_childspec(State#state{strategy = Strategy,
                                                 intensity = MaxIntensity,
                                                 period = Period},
                                     StartSpec);
		Error ->
		    {error, Error}
	    end;
	ignore ->
	    {ok, State};
	Error ->
	    Error
    end.

check_flags({Strategy, MaxIntensity, Period}) ->
    validStrategy(Strategy),
    validIntensity(MaxIntensity),
    validPeriod(Period),
    ok;
check_flags(What) ->
    {bad_flags, What}.

update_childspec(State, StartSpec) when ?is_simple(State) ->
    case check_startspec(StartSpec) of
        {ok, [Child]} ->
            {ok, State#state{children = [Child]}};
        Error ->
            {error, Error}
    end;
update_childspec(State, StartSpec) ->
    case check_startspec(StartSpec) of
	{ok, Children} ->
	    OldC = State#state.children, % In reverse start order !
	    NewC = update_childspec1(OldC, Children, []),
	    {ok, State#state{children = NewC}};
        Error ->
	    {error, Error}
    end.

update_childspec1([Child|OldC], Children, KeepOld) ->
    case update_chsp(Child, Children) of
	{ok,NewChildren} ->
	    update_childspec1(OldC, NewChildren, KeepOld);
	false ->
	    update_childspec1(OldC, Children, [Child|KeepOld])
    end;
update_childspec1([], Children, KeepOld) ->
    %% Return them in (kept) reverse start order.
    lists:reverse(Children ++ KeepOld).

update_chsp(OldCh, Children) ->
    case lists:map(fun(Ch) when OldCh#child.name =:= Ch#child.name ->
			   Ch#child{pid = OldCh#child.pid};
		      (Ch) ->
			   Ch
		   end,
		   Children) of
	Children ->
	    false;  % OldCh not found in new spec.
	NewC ->
	    {ok, NewC}
    end.
    
%%% ---------------------------------------------------
%%% Start a new child.
%%% ---------------------------------------------------

handle_start_child(Child, State) ->
    case get_child(Child#child.name, State) of
	false ->
	    case do_start_child(State#state.name, Child) of
		{ok, undefined} when Child#child.restart_type =:= temporary ->
		    {{ok, undefined}, State};
		{ok, Pid} ->
		    {{ok, Pid}, save_child(Child#child{pid = Pid}, State)};
		{ok, Pid, Extra} ->
		    {{ok, Pid, Extra}, save_child(Child#child{pid = Pid}, State)};
		{error, What} ->
		    {{error, {What, Child}}, State}
	    end;
	{value, OldChild} when is_pid(OldChild#child.pid) ->
	    {{error, {already_started, OldChild#child.pid}}, State};
	{value, _OldChild} ->
	    {{error, already_present}, State}
    end.

%%% ---------------------------------------------------
%%% Restart. A process has terminated.
%%% Returns: {ok, state()} | {shutdown, state()}
%%% ---------------------------------------------------

restart_child(Pid, Reason, #state{children = [Child]} = State) when ?is_simple(State) ->
    RestartType = Child#child.restart_type,
    case dynamic_child_args(Pid, dynamics_db(RestartType, State#state.dynamics)) of
	{ok, Args} ->
	    {M, F, _} = Child#child.mfargs,
	    NChild = Child#child{pid = Pid, mfargs = {M, F, Args}},
	    do_restart(RestartType, Reason, NChild, State);
	error ->
            {ok, State}
    end;

restart_child(Pid, Reason, State) ->
    Children = State#state.children,
    %% we still support >= R12-B3 in which lists:keyfind/3 doesn't exist
    case lists:keysearch(Pid, #child.pid, Children) of
	{value, #child{restart_type = RestartType} = Child} ->
	    do_restart(RestartType, Reason, Child, State);
	false ->
	    {ok, State}
    end.

try_restart(RestartType, Reason, Child, State) ->
    case handle_restart(RestartType, Reason, Child, State) of
        {ok, NState}       -> {noreply, NState};
        {shutdown, State2} -> {stop, shutdown, State2}
    end.

do_restart(RestartType, Reason, Child, State) ->
    maybe_report_error(RestartType, Reason, Child, State),
    handle_restart(RestartType, Reason, Child, State).

maybe_report_error(permanent, Reason, Child, State) ->
    report_child_termination(Reason, Child, State);
maybe_report_error({permanent, _}, Reason, Child, State) ->
    report_child_termination(Reason, Child, State);
maybe_report_error(_Type, Reason, Child, State) ->
    case is_abnormal_termination(Reason) of
        true  -> report_child_termination(Reason, Child, State);
        false -> ok
    end.

report_child_termination(Reason, Child, State) ->
    report_error(child_terminated, Reason, Child, State#state.name).

handle_restart(permanent, _Reason, Child, State) ->
    restart(Child, State);
handle_restart(transient, Reason, Child, State) ->
    restart_if_explicit_or_abnormal(fun restart/2,
                                    fun delete_child_and_continue/2,
                                    Reason, Child, State);
handle_restart(intrinsic, Reason, Child, State) ->
    restart_if_explicit_or_abnormal(fun restart/2,
                                    fun delete_child_and_stop/2,
                                    Reason, Child, State);
handle_restart(temporary, _Reason, Child, State) ->
    delete_child_and_continue(Child, State);
handle_restart({permanent, _Delay}=Restart, Reason, Child, State) ->
    do_restart_delay(Restart, Reason, Child, State);
handle_restart({transient, _Delay}=Restart, Reason, Child, State) ->
    restart_if_explicit_or_abnormal(defer_to_restart_delay(Restart, Reason),
                                    fun delete_child_and_continue/2,
                                    Reason, Child, State);
handle_restart({intrinsic, _Delay}=Restart, Reason, Child, State) ->
    restart_if_explicit_or_abnormal(defer_to_restart_delay(Restart, Reason),
                                    fun delete_child_and_stop/2,
                                    Reason, Child, State).

restart_if_explicit_or_abnormal(RestartHow, Otherwise, Reason, Child, State) ->
    case ?is_explicit_restart(Reason) orelse is_abnormal_termination(Reason) of
        true  -> RestartHow(Child, State);
        false -> Otherwise(Child, State)
    end.

defer_to_restart_delay(Restart, Reason) ->
    fun(Child, State) -> do_restart_delay(Restart, Reason, Child, State) end.

delete_child_and_continue(Child, State) ->
    {ok, state_del_child(Child, State)}.

delete_child_and_stop(Child, State) ->
    {shutdown, state_del_child(Child, State)}.

is_abnormal_termination(normal)        -> false;
is_abnormal_termination(shutdown)      -> false;
is_abnormal_termination({shutdown, _}) -> false;
is_abnormal_termination(_Other)        -> true.

do_restart_delay({RestartType, Delay}, Reason, Child, State) ->
    case add_restart(State) of
        {ok, NState} ->
            maybe_restart(NState#state.strategy, Child, NState);
        {terminate, _NState} ->
            %% we've reached the max restart intensity, but the
            %% add_restart will have added to the restarts
            %% field. Given we don't want to die here, we need to go
            %% back to the old restarts field otherwise we'll never
            %% attempt to restart later, which is why we ignore
            %% NState for this clause.
            _TRef = erlang:send_after(trunc(Delay*1000), self(),
                                      {delayed_restart,
                                       {{RestartType, Delay}, Reason, Child}}),
            {ok, state_del_child(Child, State)}
    end.

restart(Child, State) ->
    case add_restart(State) of
	{ok, NState} ->
	    maybe_restart(NState#state.strategy, Child, NState);
	{terminate, NState} ->
	    report_error(shutdown, reached_max_restart_intensity,
			 Child, State#state.name),
	    {shutdown, remove_child(Child, NState)}
    end.

maybe_restart(Strategy, Child, State) ->
    case restart(Strategy, Child, State) of
        {try_again, Reason, NState2} ->
            %% Leaving control back to gen_server before
            %% trying again. This way other incoming requsts
            %% for the supervisor can be handled - e.g. a
            %% shutdown request for the supervisor or the
            %% child.
            Id = if ?is_simple(State) -> Child#child.pid;
                    true -> Child#child.name
                 end,
            timer:apply_after(0,?MODULE,try_again_restart,[self(),Id,Reason]),
            {ok,NState2};
        Other ->
            Other
    end.

restart(simple_one_for_one, Child, State) ->
    #child{pid = OldPid, mfargs = {M, F, A}} = Child,
    Dynamics = ?DICT:erase(OldPid, dynamics_db(Child#child.restart_type,
					       State#state.dynamics)),
    case do_start_child_i(M, F, A) of
	{ok, Pid} ->
	    NState = State#state{dynamics = ?DICT:store(Pid, A, Dynamics)},
	    {ok, NState};
	{ok, Pid, _Extra} ->
	    NState = State#state{dynamics = ?DICT:store(Pid, A, Dynamics)},
	    {ok, NState};
	{error, Error} ->
	    NState = State#state{dynamics = ?DICT:store(restarting(OldPid), A,
							Dynamics)},
	    report_error(start_error, Error, Child, State#state.name),
	    {try_again, Error, NState}
    end;
restart(one_for_one, Child, State) ->
    OldPid = Child#child.pid,
    case do_start_child(State#state.name, Child) of
	{ok, Pid} ->
	    NState = replace_child(Child#child{pid = Pid}, State),
	    {ok, NState};
	{ok, Pid, _Extra} ->
	    NState = replace_child(Child#child{pid = Pid}, State),
	    {ok, NState};
	{error, Reason} ->
	    NState = replace_child(Child#child{pid = restarting(OldPid)}, State),
	    report_error(start_error, Reason, Child, State#state.name),
	    {try_again, Reason, NState}
    end;
restart(rest_for_one, Child, State) ->
    {ChAfter, ChBefore} = split_child(Child#child.pid, State#state.children),
    ChAfter2 = terminate_children(ChAfter, State#state.name),
    case start_children(ChAfter2, State#state.name) of
	{ok, ChAfter3} ->
	    {ok, State#state{children = ChAfter3 ++ ChBefore}};
	{error, ChAfter3, Reason} ->
	    NChild = Child#child{pid=restarting(Child#child.pid)},
	    NState = State#state{children = ChAfter3 ++ ChBefore},
	    {try_again, Reason, replace_child(NChild,NState)}
    end;
restart(one_for_all, Child, State) ->
    Children1 = del_child(Child#child.pid, State#state.children),
    Children2 = terminate_children(Children1, State#state.name),
    case start_children(Children2, State#state.name) of
	{ok, NChs} ->
	    {ok, State#state{children = NChs}};
	{error, NChs, Reason} ->
	    NChild = Child#child{pid=restarting(Child#child.pid)},
	    NState = State#state{children = NChs},
	    {try_again, Reason, replace_child(NChild,NState)}
    end.

restarting(Pid) when is_pid(Pid) -> ?restarting(Pid);
restarting(RPid) -> RPid.

%%-----------------------------------------------------------------
%% Func: terminate_children/2
%% Args: Children = [child_rec()] in termination order
%%       SupName = {local, atom()} | {global, atom()} | {pid(),Mod}
%% Returns: NChildren = [child_rec()] in
%%          startup order (reversed termination order)
%%-----------------------------------------------------------------
terminate_children(Children, SupName) ->
    terminate_children(Children, SupName, []).

%% Temporary children should not be restarted and thus should
%% be skipped when building the list of terminated children, although
%% we do want them to be shut down as many functions from this module
%% use this function to just clear everything.
terminate_children([Child = #child{restart_type=temporary} | Children], SupName, Res) ->
    do_terminate(Child, SupName),
    terminate_children(Children, SupName, Res);
terminate_children([Child | Children], SupName, Res) ->
    NChild = do_terminate(Child, SupName),
    terminate_children(Children, SupName, [NChild | Res]);
terminate_children([], _SupName, Res) ->
    Res.

do_terminate(Child, SupName) when is_pid(Child#child.pid) ->
    case shutdown(Child#child.pid, Child#child.shutdown) of
        ok ->
            ok;
        {error, normal} when not ?is_permanent(Child#child.restart_type) ->
            ok;
        {error, OtherReason} ->
            report_error(shutdown_error, OtherReason, Child, SupName)
    end,
    Child#child{pid = undefined};
do_terminate(Child, _SupName) ->
    Child#child{pid = undefined}.

%%-----------------------------------------------------------------
%% Shutdowns a child. We must check the EXIT value 
%% of the child, because it might have died with another reason than
%% the wanted. In that case we want to report the error. We put a 
%% monitor on the child an check for the 'DOWN' message instead of 
%% checking for the 'EXIT' message, because if we check the 'EXIT' 
%% message a "naughty" child, who does unlink(Sup), could hang the 
%% supervisor. 
%% Returns: ok | {error, OtherReason}  (this should be reported)
%%-----------------------------------------------------------------
shutdown(Pid, brutal_kill) ->
    case monitor_child(Pid) of
	ok ->
	    exit(Pid, kill),
	    receive
		{'DOWN', _MRef, process, Pid, killed} ->
		    ok;
		{'DOWN', _MRef, process, Pid, OtherReason} ->
		    {error, OtherReason}
	    end;
	{error, Reason} ->      
	    {error, Reason}
    end;
shutdown(Pid, Time) ->
    case monitor_child(Pid) of
	ok ->
	    exit(Pid, shutdown), %% Try to shutdown gracefully
	    receive 
		{'DOWN', _MRef, process, Pid, shutdown} ->
		    ok;
		{'DOWN', _MRef, process, Pid, OtherReason} ->
		    {error, OtherReason}
	    after Time ->
		    exit(Pid, kill),  %% Force termination.
		    receive
			{'DOWN', _MRef, process, Pid, OtherReason} ->
			    {error, OtherReason}
		    end
	    end;
	{error, Reason} ->      
	    {error, Reason}
    end.

%% Help function to shutdown/2 switches from link to monitor approach
monitor_child(Pid) ->
    
    %% Do the monitor operation first so that if the child dies 
    %% before the monitoring is done causing a 'DOWN'-message with
    %% reason noproc, we will get the real reason in the 'EXIT'-message
    %% unless a naughty child has already done unlink...
    erlang:monitor(process, Pid),
    unlink(Pid),

    receive
	%% If the child dies before the unlik we must empty
	%% the mail-box of the 'EXIT'-message and the 'DOWN'-message.
	{'EXIT', Pid, Reason} -> 
	    receive 
		{'DOWN', _, process, Pid, _} ->
		    {error, Reason}
	    end
    after 0 -> 
	    %% If a naughty child did unlink and the child dies before
	    %% monitor the result will be that shutdown/2 receives a 
	    %% 'DOWN'-message with reason noproc.
	    %% If the child should die after the unlink there
	    %% will be a 'DOWN'-message with a correct reason
	    %% that will be handled in shutdown/2. 
	    ok   
    end.


%%-----------------------------------------------------------------
%% Func: terminate_dynamic_children/3
%% Args: Child    = child_rec()
%%       Dynamics = ?DICT() | ?SET()
%%       SupName  = {local, atom()} | {global, atom()} | {pid(),Mod}
%% Returns: ok
%%
%%
%% Shutdown all dynamic children. This happens when the supervisor is
%% stopped. Because the supervisor can have millions of dynamic children, we
%% can have an significative overhead here.
%%-----------------------------------------------------------------
terminate_dynamic_children(Child, Dynamics, SupName) ->
    {Pids, EStack0} = monitor_dynamic_children(Child, Dynamics),
    Sz = ?SETS:size(Pids),
    EStack = case Child#child.shutdown of
                 brutal_kill ->
                     ?SETS:fold(fun(P, _) -> exit(P, kill) end, ok, Pids),
                     wait_dynamic_children(Child, Pids, Sz, undefined, EStack0);
                 infinity ->
                     ?SETS:fold(fun(P, _) -> exit(P, shutdown) end, ok, Pids),
                     wait_dynamic_children(Child, Pids, Sz, undefined, EStack0);
                 Time ->
                     ?SETS:fold(fun(P, _) -> exit(P, shutdown) end, ok, Pids),
                     TRef = erlang:start_timer(Time, self(), kill),
                     wait_dynamic_children(Child, Pids, Sz, TRef, EStack0)
             end,
    %% Unroll stacked errors and report them
    ?DICT:fold(fun(Reason, Ls, _) ->
                       report_error(shutdown_error, Reason,
                                    Child#child{pid=Ls}, SupName)
               end, ok, EStack).


monitor_dynamic_children(#child{restart_type=temporary}, Dynamics) ->
    ?SETS:fold(fun(P, {Pids, EStack}) ->
                       case monitor_child(P) of
                           ok ->
                               {?SETS:add_element(P, Pids), EStack};
                           {error, normal} ->
                               {Pids, EStack};
                           {error, Reason} ->
                               {Pids, ?DICT:append(Reason, P, EStack)}
                       end
               end, {?SETS:new(), ?DICT:new()}, Dynamics);
monitor_dynamic_children(#child{restart_type=RType}, Dynamics) ->
    ?DICT:fold(fun(P, _, {Pids, EStack}) when is_pid(P) ->
                       case monitor_child(P) of
                           ok ->
                               {?SETS:add_element(P, Pids), EStack};
                           {error, normal} when not ?is_permanent(RType) ->
                               {Pids, EStack};
                           {error, Reason} ->
                               {Pids, ?DICT:append(Reason, P, EStack)}
                       end;
		  (?restarting(_), _, {Pids, EStack}) ->
		       {Pids, EStack}
               end, {?SETS:new(), ?DICT:new()}, Dynamics).

wait_dynamic_children(_Child, _Pids, 0, undefined, EStack) ->
    EStack;
wait_dynamic_children(_Child, _Pids, 0, TRef, EStack) ->
	%% If the timer has expired before its cancellation, we must empty the
	%% mail-box of the 'timeout'-message.
    erlang:cancel_timer(TRef),
    receive
        {timeout, TRef, kill} ->
            EStack
    after 0 ->
            EStack
    end;
wait_dynamic_children(#child{shutdown=brutal_kill} = Child, Pids, Sz,
                      TRef, EStack) ->
    receive
        {'DOWN', _MRef, process, Pid, killed} ->
            wait_dynamic_children(Child, ?SETS:del_element(Pid, Pids), Sz-1,
                                  TRef, EStack);

        {'DOWN', _MRef, process, Pid, Reason} ->
            wait_dynamic_children(Child, ?SETS:del_element(Pid, Pids), Sz-1,
                                  TRef, ?DICT:append(Reason, Pid, EStack))
    end;
wait_dynamic_children(#child{restart_type=RType} = Child, Pids, Sz,
                      TRef, EStack) ->
    receive
        {'DOWN', _MRef, process, Pid, shutdown} ->
            wait_dynamic_children(Child, ?SETS:del_element(Pid, Pids), Sz-1,
                                  TRef, EStack);

        {'DOWN', _MRef, process, Pid, normal} when not ?is_permanent(RType) ->
            wait_dynamic_children(Child, ?SETS:del_element(Pid, Pids), Sz-1,
                                  TRef, EStack);

        {'DOWN', _MRef, process, Pid, Reason} ->
            wait_dynamic_children(Child, ?SETS:del_element(Pid, Pids), Sz-1,
                                  TRef, ?DICT:append(Reason, Pid, EStack));

        {timeout, TRef, kill} ->
            ?SETS:fold(fun(P, _) -> exit(P, kill) end, ok, Pids),
            wait_dynamic_children(Child, Pids, Sz-1, undefined, EStack)
    end.

%%-----------------------------------------------------------------
%% Child/State manipulating functions.
%%-----------------------------------------------------------------

%% Note we do not want to save the parameter list for temporary processes as
%% they will not be restarted, and hence we do not need this information.
%% Especially for dynamic children to simple_one_for_one supervisors
%% it could become very costly as it is not uncommon to spawn
%% very many such processes.
save_child(#child{restart_type = temporary,
		  mfargs = {M, F, _}} = Child, #state{children = Children} = State) ->
    State#state{children = [Child#child{mfargs = {M, F, undefined}} |Children]};
save_child(Child, #state{children = Children} = State) ->
    State#state{children = [Child |Children]}.

save_dynamic_child(temporary, Pid, _, #state{dynamics = Dynamics} = State) ->
    State#state{dynamics = ?SETS:add_element(Pid, dynamics_db(temporary, Dynamics))};
save_dynamic_child(RestartType, Pid, Args, #state{dynamics = Dynamics} = State) ->
    State#state{dynamics = ?DICT:store(Pid, Args, dynamics_db(RestartType, Dynamics))}.

dynamics_db(temporary, undefined) ->
    ?SETS:new();
dynamics_db(_, undefined) ->
    ?DICT:new();
dynamics_db(_,Dynamics) ->
    Dynamics.

dynamic_child_args(Pid, Dynamics) ->
    case ?SETS:is_set(Dynamics) of
        true ->
            {ok, undefined};
        false ->
            ?DICT:find(Pid, Dynamics)
    end.

state_del_child(#child{pid = Pid, restart_type = temporary}, State) when ?is_simple(State) ->
    NDynamics = ?SETS:del_element(Pid, dynamics_db(temporary, State#state.dynamics)),
    State#state{dynamics = NDynamics};
state_del_child(#child{pid = Pid, restart_type = RType}, State) when ?is_simple(State) ->
    NDynamics = ?DICT:erase(Pid, dynamics_db(RType, State#state.dynamics)),
    State#state{dynamics = NDynamics};
state_del_child(Child, State) ->
    NChildren = del_child(Child#child.name, State#state.children),
    State#state{children = NChildren}.

del_child(Name, [Ch=#child{pid = ?restarting(_)}|_]=Chs) when Ch#child.name =:= Name ->
    Chs;
del_child(Name, [Ch|Chs]) when Ch#child.name =:= Name, Ch#child.restart_type =:= temporary ->
    Chs;
del_child(Name, [Ch|Chs]) when Ch#child.name =:= Name ->
    [Ch#child{pid = undefined} | Chs];
del_child(Pid, [Ch|Chs]) when Ch#child.pid =:= Pid, Ch#child.restart_type =:= temporary ->
    Chs;
del_child(Pid, [Ch|Chs]) when Ch#child.pid =:= Pid ->
    [Ch#child{pid = undefined} | Chs];
del_child(Name, [Ch|Chs]) ->
    [Ch|del_child(Name, Chs)];
del_child(_, []) ->
    [].

%% Chs = [S4, S3, Ch, S1, S0]
%% Ret: {[S4, S3, Ch], [S1, S0]}
split_child(Name, Chs) ->
    split_child(Name, Chs, []).

split_child(Name, [Ch|Chs], After) when Ch#child.name =:= Name ->
    {lists:reverse([Ch#child{pid = undefined} | After]), Chs};
split_child(Pid, [Ch|Chs], After) when Ch#child.pid =:= Pid ->
    {lists:reverse([Ch#child{pid = undefined} | After]), Chs};
split_child(Name, [Ch|Chs], After) ->
    split_child(Name, Chs, [Ch | After]);
split_child(_, [], After) ->
    {lists:reverse(After), []}.

get_child(Name, State) ->
    get_child(Name, State, false).
get_child(Pid, State, AllowPid) when AllowPid, is_pid(Pid) ->
    get_dynamic_child(Pid, State);
get_child(Name, State, _) ->
    lists:keysearch(Name, #child.name, State#state.children).

get_dynamic_child(Pid, #state{children=[Child], dynamics=Dynamics}) ->
    DynamicsDb = dynamics_db(Child#child.restart_type, Dynamics),
    case is_dynamic_pid(Pid, DynamicsDb) of
	true ->
	    {value, Child#child{pid=Pid}};
	false ->
	    RPid = restarting(Pid),
	    case is_dynamic_pid(RPid, DynamicsDb) of
		true ->
		    {value, Child#child{pid=RPid}};
		false ->
		    case erlang:is_process_alive(Pid) of
			true -> false;
			false -> {value, Child}
		    end
	    end
    end.

is_dynamic_pid(Pid, Dynamics) ->
    case ?SETS:is_set(Dynamics) of
	true ->
	    ?SETS:is_element(Pid, Dynamics);
	false ->
	    ?DICT:is_key(Pid, Dynamics)
    end.

replace_child(Child, State) ->
    Chs = do_replace_child(Child, State#state.children),
    State#state{children = Chs}.

do_replace_child(Child, [Ch|Chs]) when Ch#child.name =:= Child#child.name ->
    [Child | Chs];
do_replace_child(Child, [Ch|Chs]) ->
    [Ch|do_replace_child(Child, Chs)].

remove_child(Child, State) ->
    Chs = lists:keydelete(Child#child.name, #child.name, State#state.children),
    State#state{children = Chs}.

%%-----------------------------------------------------------------
%% Func: init_state/4
%% Args: SupName = {local, atom()} | {global, atom()} | self
%%       Type = {Strategy, MaxIntensity, Period}
%%         Strategy = one_for_one | one_for_all | simple_one_for_one |
%%                    rest_for_one
%%         MaxIntensity = integer() >= 0
%%         Period = integer() > 0
%%       Mod :== atom()
%%       Args :== term()
%% Purpose: Check that Type is of correct type (!)
%% Returns: {ok, state()} | Error
%%-----------------------------------------------------------------
init_state(SupName, Type, Mod, Args) ->
    case catch init_state1(SupName, Type, Mod, Args) of
	{ok, State} ->
	    {ok, State};
	Error ->
	    Error
    end.

init_state1(SupName, {Strategy, MaxIntensity, Period}, Mod, Args) ->
    validStrategy(Strategy),
    validIntensity(MaxIntensity),
    validPeriod(Period),
    {ok, #state{name = supname(SupName,Mod),
		strategy = Strategy,
		intensity = MaxIntensity,
		period = Period,
		module = Mod,
		args = Args}};
init_state1(_SupName, Type, _, _) ->
    {invalid_type, Type}.

validStrategy(simple_one_for_one) -> true;
validStrategy(one_for_one)        -> true;
validStrategy(one_for_all)        -> true;
validStrategy(rest_for_one)       -> true;
validStrategy(What)               -> throw({invalid_strategy, What}).

validIntensity(Max) when is_integer(Max),
                         Max >=  0 -> true;
validIntensity(What)               -> throw({invalid_intensity, What}).

validPeriod(Period) when is_integer(Period),
                         Period > 0 -> true;
validPeriod(What)                   -> throw({invalid_period, What}).

supname(self, Mod) -> {self(), Mod};
supname(N, _)      -> N.

%%% ------------------------------------------------------
%%% Check that the children start specification is valid.
%%% Shall be a six (6) tuple
%%%    {Name, Func, RestartType, Shutdown, ChildType, Modules}
%%% where Name is an atom
%%%       Func is {Mod, Fun, Args} == {atom(), atom(), list()}
%%%       RestartType is permanent | temporary | transient |
%%%                      intrinsic | {permanent, Delay} |
%%%                      {transient, Delay} | {intrinsic, Delay}
%%                       where Delay >= 0
%%%       Shutdown = integer() > 0 | infinity | brutal_kill
%%%       ChildType = supervisor | worker
%%%       Modules = [atom()] | dynamic
%%% Returns: {ok, [child_rec()]} | Error
%%% ------------------------------------------------------

check_startspec(Children) -> check_startspec(Children, []).

check_startspec([ChildSpec|T], Res) ->
    case check_childspec(ChildSpec) of
	{ok, Child} ->
	    case lists:keymember(Child#child.name, #child.name, Res) of
		true -> {duplicate_child_name, Child#child.name};
		false -> check_startspec(T, [Child | Res])
	    end;
	Error -> Error
    end;
check_startspec([], Res) ->
    {ok, lists:reverse(Res)}.

check_childspec({Name, Func, RestartType, Shutdown, ChildType, Mods}) ->
    catch check_childspec(Name, Func, RestartType, Shutdown, ChildType, Mods);
check_childspec(X) -> {invalid_child_spec, X}.

check_childspec(Name, Func, RestartType, Shutdown, ChildType, Mods) ->
    validName(Name),
    validFunc(Func),
    validRestartType(RestartType),
    validChildType(ChildType),
    validShutdown(Shutdown, ChildType),
    validMods(Mods),
    {ok, #child{name = Name, mfargs = Func, restart_type = RestartType,
		shutdown = Shutdown, child_type = ChildType, modules = Mods}}.

validChildType(supervisor) -> true;
validChildType(worker) -> true;
validChildType(What) -> throw({invalid_child_type, What}).

validName(_Name) -> true.

validFunc({M, F, A}) when is_atom(M), 
                          is_atom(F), 
                          is_list(A) -> true;
validFunc(Func)                      -> throw({invalid_mfa, Func}).

validRestartType(permanent)          -> true;
validRestartType(temporary)          -> true;
validRestartType(transient)          -> true;
validRestartType(intrinsic)          -> true;
validRestartType({permanent, Delay}) -> validDelay(Delay);
validRestartType({intrinsic, Delay}) -> validDelay(Delay);
validRestartType({transient, Delay}) -> validDelay(Delay);
validRestartType(RestartType)        -> throw({invalid_restart_type,
                                               RestartType}).

validDelay(Delay) when is_number(Delay),
                       Delay >= 0 -> true;
validDelay(What)                  -> throw({invalid_delay, What}).

validShutdown(Shutdown, _) 
  when is_integer(Shutdown), Shutdown > 0 -> true;
validShutdown(infinity, _)             -> true;
validShutdown(brutal_kill, _)          -> true;
validShutdown(Shutdown, _)             -> throw({invalid_shutdown, Shutdown}).

validMods(dynamic) -> true;
validMods(Mods) when is_list(Mods) ->
    lists:foreach(fun(Mod) ->
		    if
			is_atom(Mod) -> ok;
			true -> throw({invalid_module, Mod})
		    end
		  end,
		  Mods);
validMods(Mods) -> throw({invalid_modules, Mods}).

%%% ------------------------------------------------------
%%% Add a new restart and calculate if the max restart
%%% intensity has been reached (in that case the supervisor
%%% shall terminate).
%%% All restarts accured inside the period amount of seconds
%%% are kept in the #state.restarts list.
%%% Returns: {ok, State'} | {terminate, State'}
%%% ------------------------------------------------------

add_restart(State) ->  
    I = State#state.intensity,
    P = State#state.period,
    R = State#state.restarts,
    Now = erlang:now(),
    R1 = add_restart([Now|R], Now, P),
    State1 = State#state{restarts = R1},
    case length(R1) of
	CurI when CurI  =< I ->
	    {ok, State1};
	_ ->
	    {terminate, State1}
    end.

add_restart([R|Restarts], Now, Period) ->
    case inPeriod(R, Now, Period) of
	true ->
	    [R|add_restart(Restarts, Now, Period)];
	_ ->
	    []
    end;
add_restart([], _, _) ->
    [].

inPeriod(Time, Now, Period) ->
    case difference(Time, Now) of
	T when T > Period ->
	    false;
	_ ->
	    true
    end.

%%
%% Time = {MegaSecs, Secs, MicroSecs} (NOTE: MicroSecs is ignored)
%% Calculate the time elapsed in seconds between two timestamps.
%% If MegaSecs is equal just subtract Secs.
%% Else calculate the Mega difference and add the Secs difference,
%% note that Secs difference can be negative, e.g.
%%      {827, 999999, 676} diff {828, 1, 653753} == > 2 secs.
%%
difference({TimeM, TimeS, _}, {CurM, CurS, _}) when CurM > TimeM ->
    ((CurM - TimeM) * 1000000) + (CurS - TimeS);
difference({_, TimeS, _}, {_, CurS, _}) ->
    CurS - TimeS.

%%% ------------------------------------------------------
%%% Error and progress reporting.
%%% ------------------------------------------------------

report_error(Error, Reason, Child, SupName) ->
    ErrorMsg = [{supervisor, SupName},
		{errorContext, Error},
		{reason, Reason},
		{offender, extract_child(Child)}],
    error_logger:error_report(supervisor_report, ErrorMsg).


extract_child(Child) when is_list(Child#child.pid) ->
    [{nb_children, length(Child#child.pid)},
     {name, Child#child.name},
     {mfargs, Child#child.mfargs},
     {restart_type, Child#child.restart_type},
     {shutdown, Child#child.shutdown},
     {child_type, Child#child.child_type}];
extract_child(Child) ->
    [{pid, Child#child.pid},
     {name, Child#child.name},
     {mfargs, Child#child.mfargs},
     {restart_type, Child#child.restart_type},
     {shutdown, Child#child.shutdown},
     {child_type, Child#child.child_type}].

report_progress(Child, SupName) ->
    Progress = [{supervisor, SupName},
		{started, extract_child(Child)}],
    error_logger:info_report(progress, Progress).
