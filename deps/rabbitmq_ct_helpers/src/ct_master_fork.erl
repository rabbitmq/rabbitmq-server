%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2006-2024. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

-module(ct_master_fork).
%-moduledoc """
%Distributed test execution control for `Common Test`.
%
%This module exports functions for running `Common Test` nodes on multiple hosts
%in parallel.
%""".

-export([run/1,run/3,run/4]).
-export([run_on_node/2,run_on_node/3]).
-export([run_test/1,run_test/2]).
-export([get_event_mgr_ref/0]).
-export([basic_html/1,esc_chars/1]).

-export([abort/0,abort/1,progress/0]).

-export([init_master/7, init_node_ctrl/3]).

-export([status/2]).

-include_lib("common_test/include/ct_event.hrl").
-include_lib("common_test/src/ct_util.hrl").

%-doc "Filename of test spec to be executed.".
-type test_spec() :: file:name_all().

-record(state, {node_ctrl_pids=[],
		logdirs=[],
		results=[],
		locks=[],
		blocked=[]
		}).

-export_type([test_spec/0]).

%-doc """
%Tests are spawned on `Node` using `ct:run_test/1`
%""".
-spec run_test(Node, Opts) -> 'ok'
        when Node :: node(),
            Opts :: [OptTuples],
            OptTuples :: {'dir', TestDirs}
                        | {'suite', Suites}
                        | {'group', Groups}
                        | {'testcase', Cases}
                        | {'spec', TestSpecs}
                        | {'join_specs', boolean()}
                        | {'label', Label}
                        | {'config', CfgFiles}
                        | {'userconfig', UserConfig}
                        | {'allow_user_terms', boolean()}
                        | {'logdir', LogDir}
                        | {'silent_connections', Conns}
                        | {'stylesheet', CSSFile}
                        | {'cover', CoverSpecFile}
                        | {'cover_stop', boolean()}
                        | {'step', StepOpts}
                        | {'event_handler', EventHandlers}
                        | {'include', InclDirs}
                        | {'auto_compile', boolean()}
                        | {'abort_if_missing_suites', boolean()}
                        | {'create_priv_dir', CreatePrivDir}
                        | {'multiply_timetraps', M}
                        | {'scale_timetraps', boolean()}
                        | {'repeat', N}
                        | {'duration', DurTime}
                        | {'until', StopTime}
                        | {'force_stop', ForceStop}
                        | {'decrypt', DecryptKeyOrFile}
                        | {'refresh_logs', LogDir}
                        | {'logopts', LogOpts}
                        | {'verbosity', VLevels}
                        | {'basic_html', boolean()}
                        | {'esc_chars', boolean()}
                        | {'keep_logs',KeepSpec}
                        | {'ct_hooks', CTHs}
                        | {'ct_hooks_order', CTHsOrder}
                        | {'enable_builtin_hooks', boolean()}
                        | {'release_shell', boolean()},
            TestDirs :: [string()] | string(),
            Suites :: [string()] | [atom()] | string() | atom(),
            Cases :: [atom()] | atom(),
            Groups :: GroupNameOrPath | [GroupNameOrPath],
            GroupNameOrPath :: [atom()] | atom() | 'all',
            TestSpecs :: [string()] | string(),
            Label :: string() | atom(),
            CfgFiles :: [string()] | string(),
            UserConfig :: [{CallbackMod, CfgStrings}] | {CallbackMod, CfgStrings},
            CallbackMod :: atom(),
            CfgStrings :: [string()] | string(),
            LogDir :: string(),
            Conns :: 'all' | [atom()],
            CSSFile :: string(),
            CoverSpecFile :: string(),
            StepOpts :: [StepOpt],
            StepOpt :: 'config' | 'keep_inactive',
            EventHandlers :: EH | [EH],
            EH :: atom() | {atom(), InitArgs} | {[atom()], InitArgs},
            InitArgs :: [term()],
            InclDirs :: [string()] | string(),
            CreatePrivDir :: 'auto_per_run' | 'auto_per_tc' | 'manual_per_tc',
            M :: integer(),
            N :: integer(),
            DurTime :: HHMMSS,
            HHMMSS :: string(),
            StopTime :: YYMoMoDDHHMMSS | HHMMSS,
            YYMoMoDDHHMMSS :: string(),
            ForceStop :: 'skip_rest' | boolean(),
            DecryptKeyOrFile :: {'key', DecryptKey} | {'file', DecryptFile},
            DecryptKey :: string(),
            DecryptFile :: string(),
            LogOpts :: [LogOpt],
            LogOpt :: 'no_nl' | 'no_src',
            VLevels :: VLevel | [{Category, VLevel}],
            VLevel :: integer(),
            Category :: atom(),
            KeepSpec :: 'all' | pos_integer(),
            CTHs :: [CTHModule | {CTHModule, CTHInitArgs}],
            CTHsOrder :: atom(),
            CTHModule :: atom(),
            CTHInitArgs :: term().
run_test(Node,Opts) ->
    run_test([{Node,Opts}]).

%-doc false.
run_test({Node,Opts}) ->
    run_test([{Node,Opts}]);
run_test(NodeOptsList) when is_list(NodeOptsList) ->
    start_master(NodeOptsList).

%-doc """
%Tests are spawned on the nodes as specified in `TestSpecs`. Each specification
%in `TestSpec` is handled separately. However, it is also possible to specify a
%list of specifications to be merged into one specification before the tests are
%executed. Any test without a particular node specification is also executed on
%the nodes in `InclNodes`. Nodes in the `ExclNodes` list are excluded from the
%test.
%""".
-spec run(TestSpecs, AllowUserTerms, InclNodes, ExclNodes) -> [{Specs, 'ok'} | {'error', Reason}]
              when TestSpecs :: TestSpec | [TestSpec] | [[TestSpec]],
                   TestSpec :: test_spec(),
                   AllowUserTerms :: boolean(),
                   InclNodes :: [node()],
                   ExclNodes :: [node()],
                   Specs :: [file:filename_all()],
                   Reason :: term().
run([TS|TestSpecs],AllowUserTerms,InclNodes,ExclNodes) when is_list(TS),
							    is_list(InclNodes),
							    is_list(ExclNodes) ->
    %% Note: [Spec] means run one test with Spec
    %%       [Spec1,Spec2] means run two tests separately
    %%       [[Spec1,Spec2]] means run one test, with the two specs merged
    case catch ct_testspec:collect_tests_from_file([TS],InclNodes,
						   AllowUserTerms) of
	{error,Reason} ->
	    [{error,Reason} | run(TestSpecs,AllowUserTerms,InclNodes,ExclNodes)];
	Tests ->
	    RunResult =
		lists:map(
		  fun({Specs,TSRec=#testspec{logdir=AllLogDirs,
					      config=StdCfgFiles,
					      userconfig=UserCfgFiles,
					      include=AllIncludes,
					      init=AllInitOpts,
					      event_handler=AllEvHs}}) ->
			  AllCfgFiles =
			      {StdCfgFiles,UserCfgFiles},
			  RunSkipPerNode =
			      ct_testspec:prepare_tests(TSRec),
			  RunSkipPerNode2 =
			      exclude_nodes(ExclNodes,RunSkipPerNode),
			  TSList = if is_integer(hd(TS)) -> [TS];
				      true -> TS end,
			  {Specs,run_all(RunSkipPerNode2,AllLogDirs,
					 AllCfgFiles,AllEvHs,
					 AllIncludes,[],[],AllInitOpts,TSList)}
		  end, Tests),
	    RunResult ++ run(TestSpecs,AllowUserTerms,InclNodes,ExclNodes)
    end;
run([],_,_,_) ->
    [];
run(TS,AllowUserTerms,InclNodes,ExclNodes) when is_list(InclNodes),
						is_list(ExclNodes) ->
    run([TS],AllowUserTerms,InclNodes,ExclNodes).

%-doc(#{equiv => run(TestSpecs, false, InclNodes, ExclNodes)}).
-spec run(TestSpecs, InclNodes, ExclNodes) -> [{Specs, 'ok'} | {'error', Reason}]
              when TestSpecs :: TestSpec | [TestSpec] | [[TestSpec]],
                   TestSpec :: test_spec(),
                   InclNodes :: [node()],
                   ExclNodes :: [node()],
                   Specs :: [file:filename_all()],
                   Reason :: term().
run(TestSpecs,InclNodes,ExclNodes) ->
    run(TestSpecs,false,InclNodes,ExclNodes).

%-doc """
%Run tests on spawned nodes as specified in `TestSpecs` (see `run/4`).
%
%Equivalent to [`run(TestSpecs, false, [], [])`](`run/4`) if
%called with TestSpecs being list of strings;
%
%Equivalent to [`run([TS], false, [], [])`](`run/4`) if
%called with TS being string.
%""".
-spec run(TestSpecs) -> [{Specs, 'ok'} | {'error', Reason}]
              when TestSpecs :: TestSpec | [TestSpec] | [[TestSpec]],
                   TestSpec :: test_spec(),
                   Specs :: [file:filename_all()],
                   Reason :: term().
run(TestSpecs=[TS|_]) when is_list(TS) ->
    run(TestSpecs,false,[],[]);
run(TS) ->
    run([TS],false,[],[]).


exclude_nodes([ExclNode|ExNs],RunSkipPerNode) ->
    exclude_nodes(ExNs,lists:keydelete(ExclNode,1,RunSkipPerNode));
exclude_nodes([],RunSkipPerNode) ->
    RunSkipPerNode.


%-doc """
%Tests are spawned on `Node` according to `TestSpecs`.
%""".
-spec run_on_node(TestSpecs, AllowUserTerms, Node) -> [{Specs, 'ok'} | {'error', Reason}]
              when TestSpecs :: TestSpec | [TestSpec] | [[TestSpec]],
                   TestSpec :: test_spec(),
                   AllowUserTerms :: boolean(),
                   Node :: node(),
                   Specs :: [file:filename_all()],
                   Reason :: term().
run_on_node([TS|TestSpecs],AllowUserTerms,Node) when is_list(TS),is_atom(Node) ->
    case catch ct_testspec:collect_tests_from_file([TS],[Node],
						   AllowUserTerms) of
	{error,Reason} ->
	    [{error,Reason} | run_on_node(TestSpecs,AllowUserTerms,Node)];
	Tests ->
	    RunResult =
		lists:map(
		  fun({Specs,TSRec=#testspec{logdir=AllLogDirs,
					     config=StdCfgFiles,
					     init=AllInitOpts,
					     include=AllIncludes,
					     userconfig=UserCfgFiles,
					     event_handler=AllEvHs}}) ->
			  AllCfgFiles = {StdCfgFiles,UserCfgFiles},
			  {Run,Skip} = ct_testspec:prepare_tests(TSRec,Node),
			  TSList = if is_integer(hd(TS)) -> [TS];
				      true -> TS end,			  
			  {Specs,run_all([{Node,Run,Skip}],AllLogDirs,
					 AllCfgFiles,AllEvHs,
					 AllIncludes, [],[],AllInitOpts,TSList)}
		  end, Tests),
	    RunResult ++ run_on_node(TestSpecs,AllowUserTerms,Node)
    end;
run_on_node([],_,_) ->
    [];
run_on_node(TS,AllowUserTerms,Node) when is_atom(Node) ->
    run_on_node([TS],AllowUserTerms,Node).

%-doc(#{equiv => run_on_node(TestSpecs, false, Node)}).
-spec run_on_node(TestSpecs, Node) -> [{Specs, 'ok'} | {'error', Reason}]
              when TestSpecs :: TestSpec | [TestSpec] | [[TestSpec]],
                   TestSpec :: test_spec(),
                   Node :: node(),
                   Specs :: [file:filename_all()],
                   Reason :: term().
run_on_node(TestSpecs,Node) ->
    run_on_node(TestSpecs,false,Node).



run_all([{Node,Run,Skip}|Rest],AllLogDirs,
	{AllStdCfgFiles, AllUserCfgFiles}=AllCfgFiles,
	AllEvHs,AllIncludes,NodeOpts,LogDirs,InitOptions,Specs) ->
    LogDir =
	lists:foldl(fun({N,Dir},_Found) when N == Node ->
			    Dir;
		       ({_N,_Dir},Found) ->
			    Found;
		       (Dir,".") ->
			    Dir;
		       (_Dir,Found) ->
			    Found
		    end,".",AllLogDirs),

    StdCfgFiles =
	lists:foldr(fun({N,F},Fs) when N == Node -> [F|Fs];
		       ({_N,_F},Fs) -> Fs;
		       (F,Fs) -> [F|Fs]
		    end,[],AllStdCfgFiles),
    UserCfgFiles =
         lists:foldr(fun({N,F},Fs) when N == Node -> [{userconfig, F}|Fs];
		       ({_N,_F},Fs) -> Fs;
		       (F,Fs) -> [{userconfig, F}|Fs]
		    end,[],AllUserCfgFiles),
    
    Includes = lists:foldr(fun({N,I},Acc) when N =:= Node ->
				   [I|Acc];
			      ({_,_},Acc) ->
				   Acc;
			      (I,Acc) ->
				   [I | Acc]
			   end, [], AllIncludes),
    EvHs =
	lists:foldr(fun({N,H,A},Hs) when N == Node -> [{H,A}|Hs];
		       ({_N,_H,_A},Hs) -> Hs;
		       ({H,A},Hs) -> [{H,A}|Hs]
		    end,[],AllEvHs),

    NO = {Node,[{prepared_tests,{Run,Skip},Specs},
        {ct_hooks, [cth_parallel_ct_detect_failure]},
		{logdir,LogDir},
		{include, Includes},
		{config,StdCfgFiles},
		{event_handler,EvHs}] ++ UserCfgFiles},
    run_all(Rest,AllLogDirs,AllCfgFiles,AllEvHs,AllIncludes,
	    [NO|NodeOpts],[LogDir|LogDirs],InitOptions,Specs);
run_all([],AllLogDirs,_,AllEvHs,_AllIncludes,
	NodeOpts,LogDirs,InitOptions,Specs) ->
    Handlers = [{H,A} || {Master,H,A} <- AllEvHs, Master == master],
    MasterLogDir = case lists:keysearch(master,1,AllLogDirs) of
		       {value,{_,Dir}} -> Dir;
		       false -> "."
		   end,
    log(tty,"Master Logdir","~ts",[MasterLogDir]),
    start_master(lists:reverse(NodeOpts),Handlers,MasterLogDir,
		 LogDirs,InitOptions,Specs),
    ok.
    

%-doc """
%Stops all running tests.
%""".
-spec abort() -> 'ok'.
abort() ->
    call(abort).

%-doc """
%Stops tests on specified nodes.
%""".
-spec abort(Nodes) -> 'ok'
              when Nodes :: Node | [Node],
                   Node :: node().
abort(Nodes) when is_list(Nodes) ->
    call({abort,Nodes});

abort(Node) when is_atom(Node) ->
    abort([Node]).
    
%-doc """
%Returns test progress. If `Status` is `ongoing`, tests are running on the node
%and are not yet finished.
%""".
-spec progress() -> [{Node, Status}]
              when Node :: node(),
                   Status :: atom().
progress() ->
    call(progress).

%-doc """
%Gets a reference to the `Common Test` master event manager. The reference can be
%used to, for example, add a user-specific event handler while tests are running.
%
%_Example:_
%
%```erlang
%gen_event:add_handler(ct_master:get_event_mgr_ref(), my_ev_h, [])
%```
%""".
%-doc(#{since => <<"OTP 17.5">>}).
-spec get_event_mgr_ref() -> atom().
get_event_mgr_ref() ->
    ?CT_MEVMGR_REF.

%-doc """
%If set to `true`, the `ct_master logs` are written on a primitive HTML format,
%not using the `Common Test` CSS style sheet.
%""".
%-doc(#{since => <<"OTP R15B01">>}).
-spec basic_html(Bool) -> 'ok'
              when Bool :: boolean().
basic_html(Bool) ->
    application:set_env(common_test_master, basic_html, Bool),
    ok.

%-doc false.
esc_chars(Bool) ->
    application:set_env(common_test_master, esc_chars, Bool),
    ok.

%%%-----------------------------------------------------------------
%%% MASTER, runs on central controlling node.
%%%-----------------------------------------------------------------
start_master(NodeOptsList) ->
    start_master(NodeOptsList,[],".",[],[],[]).

start_master(NodeOptsList,EvHandlers,MasterLogDir,LogDirs,InitOptions,Specs) ->
    Master = spawn_link(?MODULE,init_master,[self(),NodeOptsList,EvHandlers,
					     MasterLogDir,LogDirs,
					     InitOptions,Specs]),
    receive 
	{Master,Result} -> Result
    end.	    

%-doc false.
init_master(Parent,NodeOptsList,EvHandlers,MasterLogDir,LogDirs,
	    InitOptions,Specs) ->
    case whereis(ct_master) of
	undefined ->
	    register(ct_master,self()),
            ct_util:mark_process(),
	    ok;
	_Pid ->
	    io:format("~nWarning: ct_master already running!~n"),
	    exit(aborted)
%	    case io:get_line('[y/n]>') of
%		"y\n" ->
%		    ok;
%		"n\n" ->
%		    exit(aborted);
%		_ ->
%		    init_master(NodeOptsList,LogDirs)
%	    end
    end,

    %% start master logger
    {MLPid,_} = ct_master_logs:start(MasterLogDir,
				     [N || {N,_} <- NodeOptsList]),
    log(all,"Master Logger process started","~w",[MLPid]),

    case Specs of
	[] -> ok;
	_ ->
	    SpecsStr = lists:map(fun(Name) ->
					 Name ++ " "
				 end,Specs), 
	    ct_master_logs:log("Test Specification file(s)","~ts",
			       [lists:flatten(SpecsStr)])
    end,

    %% start master event manager and add default handler
    {ok, _}  = start_ct_master_event(),
    ct_master_event:add_handler(),
    %% add user handlers for master event manager
    Add = fun({H,Args}) ->
		  log(all,"Adding Event Handler","~w",[H]),
		  case gen_event:add_handler(?CT_MEVMGR_REF,H,Args) of
		      ok -> ok;
		      {'EXIT',Why} -> exit(Why);
		      Other -> exit({event_handler,Other})
		  end
	  end,
    lists:foreach(Add,EvHandlers),

    %% double check event manager is started and registered
    case whereis(?CT_MEVMGR) of
	undefined ->
	    exit({?CT_MEVMGR,undefined});
	Pid when is_pid(Pid) ->
	    ok
    end,
    init_master1(Parent,NodeOptsList,InitOptions,LogDirs).

start_ct_master_event() ->
    case ct_master_event:start_link() of
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Else ->
            Else
    end.

init_master1(Parent,NodeOptsList,InitOptions,LogDirs) ->
    {Inaccessible,NodeOptsList1,InitOptions1} = init_nodes(NodeOptsList,
							   InitOptions),
    case Inaccessible of
	[] ->
	    init_master2(Parent,NodeOptsList,LogDirs);
	_ ->
	    io:format("~nThe following nodes are inaccessible: ~p~n~n",
		      [Inaccessible]),
	    io:format("Proceed(p), Rescan(r) or Abort(a)? "),
	    case io:get_line('[p/r/a]>') of
		"p\n" ->
		    log(html,"Inaccessible Nodes",
			"Proceeding without: ~p",[Inaccessible]),
		    init_master2(Parent,NodeOptsList1,LogDirs);
		"r\n" ->
		    init_master1(Parent,NodeOptsList,InitOptions1,LogDirs);
		_ ->
		    log(html,"Aborting Tests","",[]),
		    ct_master_event:stop(),
		    ct_master_logs:stop(),
		    exit(aborted)
	    end
    end.

init_master2(Parent,NodeOptsList,LogDirs) ->
    process_flag(trap_exit,true),
    Cookie = erlang:get_cookie(),
    log(all,"Cookie","~tw",[Cookie]),
    log(all,"Starting Tests",
	"Tests starting on: ~p",[[N || {N,_} <- NodeOptsList]]),
    SpawnAndMon = 
	fun({Node,Opts}) ->
		monitor_node(Node,true),
		log(all,"Test Info","Starting test(s) on ~w...",[Node]),
		{spawn_link(Node,?MODULE,init_node_ctrl,[self(),Cookie,Opts]),
		 Node}
	end,
    NodeCtrlPids = lists:map(SpawnAndMon,NodeOptsList),
    Result = master_loop(#state{node_ctrl_pids=NodeCtrlPids,
				logdirs=LogDirs}),
    Parent ! {self(),Result}.

master_loop(#state{node_ctrl_pids=[],
		   logdirs=LogDirs,
		   results=Finished}) ->
    Str =
	lists:map(fun({Node,Result}) ->
			  io_lib:format("~-40.40.*ts~tp\n",
					[$_,atom_to_list(Node),Result])
		  end,lists:reverse(Finished)),
    log(all,"TEST RESULTS","~ts", [Str]),
    log(all,"Info","Updating log files",[]),
    refresh_logs(LogDirs,[]),
    
    ct_master_event:stop(),
    ct_master_logs:stop(),
    ok;

master_loop(State=#state{node_ctrl_pids=NodeCtrlPids,
			 results=Results,
			 locks=Locks,
			 blocked=Blocked}) ->
    receive
	{'EXIT',Pid,Reason} ->
	    case get_node(Pid,NodeCtrlPids) of
		{Node,NodeCtrlPids1} ->
		    monitor_node(Node,false),
		    case Reason of 
			normal ->
			    log(all,"Test Info",
				"Test(s) on node ~w finished.",[Node]),
			    master_loop(State#state{node_ctrl_pids=NodeCtrlPids1});
			Bad ->
			    Error =
				case Bad of
				    What when What=/=killed,is_atom(What) -> 
					{error,Bad};
				    _ -> 
					Bad
				end,
			    log(all,"Test Info",
				"Test on node ~w failed! Reason: ~tp",
				[Node,Error]),
			    {Locks1,Blocked1} = 
				update_queue(exit,Node,Locks,Blocked),
			    master_loop(State#state{node_ctrl_pids=NodeCtrlPids1,
						    results=[{Node,
							      Error}|Results],
						    locks=Locks1,
						    blocked=Blocked1})
		    end;
		undefined ->
		    %% ignore (but report) exit from master_logger etc
		    log(all,"Test Info",
			"Warning! Process ~w has terminated. Reason: ~tp",
			[Pid,Reason]),
			master_loop(State)
	    end;

	{nodedown,Node} ->
	    case get_pid(Node,NodeCtrlPids) of
		{_Pid,NodeCtrlPids1} ->
		    monitor_node(Node,false),
		    log(all,"Test Info","No connection to testnode ~w!",[Node]),
		    {Locks1,Blocked1} = 
			update_queue(exit,Node,Locks,Blocked),
		    master_loop(State#state{node_ctrl_pids=NodeCtrlPids1,
					    results=[{Node,nodedown}|Results],
					    locks=Locks1,
					    blocked=Blocked1});
		undefined ->
		    master_loop(State)
	    end;

	{Pid,{result,Result}} ->
	    {Node,_} = get_node(Pid,NodeCtrlPids),
	    master_loop(State#state{results=[{Node,Result}|Results]});
	
	{call,progress,From} ->
	    reply(master_progress(NodeCtrlPids,Results),From),
	    master_loop(State);

	{call,abort,From} ->
	    lists:foreach(fun({Pid,Node}) ->
				  log(all,"Test Info",
				      "Aborting tests on ~w",[Node]),
				  exit(Pid,kill)
			  end,NodeCtrlPids),
	    reply(ok,From),
	    master_loop(State);

	 {call,{abort,Nodes},From} ->
	    lists:foreach(fun(Node) ->
				  case lists:keysearch(Node,2,NodeCtrlPids) of
				      {value,{Pid,Node}} ->
					  log(all,"Test Info",
					      "Aborting tests on ~w",[Node]),
					  exit(Pid,kill);
				      false ->
					  ok
				  end
			  end,Nodes),
	    reply(ok,From),
	    master_loop(State);

	{call,#event{name=Name,node=Node,data=Data},From} ->
	    {Op,Lock} =
		case Name of
		    start_make ->
			{take,{make,Data}};
		    finished_make ->
			{release,{make,Data}};
		    start_write_file ->
			{take,{write_file,Data}};
		    finished_write_file ->
			{release,{write_file,Data}}
		end,
	    {Locks1,Blocked1} =
		update_queue(Op,Node,From,Lock,Locks,Blocked),
	    if Op == release -> reply(ok,From);
	       true -> ok
	    end,
	    master_loop(State#state{locks=Locks1,
				    blocked=Blocked1});

	{cast,Event} when is_record(Event,event) ->
	    ct_master_event:notify(Event),
	    master_loop(State)

    end.


update_queue(take,Node,From,Lock={Op,Resource},Locks,Blocked) ->
    %% Locks: [{{Operation,Resource},Node},...]
    %% Blocked: [{{Operation,Resource},Node,WaitingPid},...]
    case lists:keysearch(Lock,1,Locks) of
	{value,{_Lock,Owner}} ->		% other node has lock
	    log(html,"Lock Info","Node ~w blocked on ~w by ~w. Resource: ~tp",
		[Node,Op,Owner,Resource]),
	    Blocked1 = Blocked ++ [{Lock,Node,From}],
	    {Locks,Blocked1};
	false ->				% go ahead
	    Locks1 = [{Lock,Node}|Locks],
	    reply(ok,From),
	    {Locks1,Blocked}
    end;

update_queue(release,Node,_From,Lock={Op,Resource},Locks,Blocked) ->
    Locks1 = lists:delete({Lock,Node},Locks),
    case lists:keysearch(Lock,1,Blocked) of
	{value,E={Lock,SomeNode,WaitingPid}} ->
	    Blocked1 = lists:delete(E,Blocked),
	    log(html,"Lock Info","Node ~w proceeds with ~w. Resource: ~tp",
		[SomeNode,Op,Resource]),
	    reply(ok,WaitingPid),		% waiting process may start
	    {Locks1,Blocked1};
	false ->
	    {Locks1,Blocked}
    end.

update_queue(exit,Node,Locks,Blocked) ->
    NodeLocks = lists:foldl(fun({L,N},Ls) when N == Node ->
				     [L|Ls];
				(_,Ls) ->
				     Ls
			     end,[],Locks),
    release_locks(Node,NodeLocks,Locks,Blocked).

release_locks(Node,[Lock|Ls],Locks,Blocked) ->
    {Locks1,Blocked1} = update_queue(release,Node,undefined,Lock,Locks,Blocked),
    release_locks(Node,Ls,Locks1,Blocked1);
release_locks(_,[],Locks,Blocked) ->
    {Locks,Blocked}.

get_node(Pid,NodeCtrlPids) ->
    case lists:keysearch(Pid,1,NodeCtrlPids) of
	{value,{Pid,Node}} ->
	    {Node,lists:keydelete(Pid,1,NodeCtrlPids)};
	false ->
	    undefined
    end.

get_pid(Node,NodeCtrlPids) ->
    case lists:keysearch(Node,2,NodeCtrlPids) of
	{value,{Pid,Node}} ->
	    {Pid,lists:keydelete(Node,2,NodeCtrlPids)};
	false ->
	    undefined
    end.

ping_nodes(NodeOptions)->
    ping_nodes(NodeOptions, [], []).

ping_nodes([NO={Node,_Opts}|NOs],Inaccessible,NodeOpts) ->
    case net_adm:ping(Node) of
	pong ->
	    ping_nodes(NOs,Inaccessible,[NO|NodeOpts]);
	_ ->
	    ping_nodes(NOs,[Node|Inaccessible],NodeOpts)
    end;
ping_nodes([],Inaccessible,NodeOpts) ->
    {lists:reverse(Inaccessible),lists:reverse(NodeOpts)}.

master_progress(NodeCtrlPids,Results) ->
    Results ++ lists:map(fun({_Pid,Node}) ->
				 {Node,ongoing}
			 end,NodeCtrlPids).    
    
%% refresh those dirs where more than one node has written logs
refresh_logs([D|Dirs],Refreshed) ->
    case lists:member(D,Dirs) of
	true ->
	    case lists:keymember(D,1,Refreshed) of
		true ->
		    refresh_logs(Dirs,Refreshed);
		false ->
		    {ok,Cwd} = file:get_cwd(),
		    case catch ct_run:refresh_logs(D, unknown) of
			{'EXIT',Reason} ->
			    ok = file:set_cwd(Cwd),
			    refresh_logs(Dirs,[{D,{error,Reason}}|Refreshed]);
			Result -> 
			    refresh_logs(Dirs,[{D,Result}|Refreshed])
		    end
	    end;
	false ->
	    refresh_logs(Dirs,Refreshed)
    end;
refresh_logs([],Refreshed) ->
    Str =
	lists:map(fun({D,Result}) ->
			  io_lib:format("Refreshing logs in ~tp... ~tp",
					[D,Result])
		  end,Refreshed),
    log(all,"Info","~ts", [Str]).

%%%-----------------------------------------------------------------
%%% NODE CONTROLLER, runs and controls tests on a test node.
%%%-----------------------------------------------------------------
%-doc false.
init_node_ctrl(MasterPid,Cookie,Opts) ->
    %% make sure tests proceed even if connection to master is lost
    process_flag(trap_exit, true),
    ct_util:mark_process(),
    MasterNode = node(MasterPid),
    group_leader(whereis(user),self()),
    io:format("~n********** node_ctrl process ~w started on ~w **********~n",
	      [self(),node()]),
    %% initially this node must have the same cookie as the master node
    %% but now we set it explicitly for the connection so that test suites
    %% can change the cookie for the node if they wish
    case erlang:get_cookie() of
	Cookie ->			% first time or cookie not changed
	    erlang:set_cookie(node(MasterPid),Cookie);
	_ ->
	    ok
    end,    
    case whereis(ct_util_server) of
	undefined -> ok;
	Pid -> exit(Pid,kill)
    end,
    
    %% start a local event manager
    {ok, _} = start_ct_event(),
    ct_event:add_handler([{master,MasterPid}]),

    %% log("Running test with options: ~tp~n", [Opts]),
    Result = case (catch ct:run_test(Opts)) of
		 ok -> finished_ok;
		 Other -> Other
	     end,

    %% stop local event manager
    ct_event:stop(),

    case net_adm:ping(MasterNode) of
	pong ->
	    MasterPid ! {self(),{result,Result}};
	pang ->
	    io:format("Warning! Connection to master node ~w is lost. "
		      "Can't report result!~n~n", [MasterNode])
    end.

start_ct_event() ->
    case ct_event:start_link() of
        {error, {already_started, Pid}} ->
            {ok, Pid};
        Else ->
            Else
    end.

%%%-----------------------------------------------------------------
%%% Event handling
%%%-----------------------------------------------------------------
%-doc false.
status(MasterPid,Event=#event{name=start_make}) ->
    call(MasterPid,Event);
status(MasterPid,Event=#event{name=finished_make}) ->
    call(MasterPid,Event);
status(MasterPid,Event=#event{name=start_write_file}) ->
    call(MasterPid,Event);
status(MasterPid,Event=#event{name=finished_write_file}) ->
    call(MasterPid,Event);
status(MasterPid,Event) ->
    cast(MasterPid,Event).

%%%-----------------------------------------------------------------
%%% Internal
%%%-----------------------------------------------------------------

log(To,Heading,Str,Args) ->
    if To == all ; To == tty ->
	    Chars = ["=== ",Heading," ===\n",
		     io_lib:format(Str,Args),"\n"],
	    io:put_chars(Chars);
       true ->
	    ok
    end,
    if To == all ; To == html ->
	    ct_master_logs:log(Heading,Str,Args);
       true ->
	    ok
    end.    
	    

call(Msg) ->
    call(whereis(ct_master),Msg).

call(undefined,_Msg) ->
    {error,not_running};

call(Pid,Msg) ->
    Ref = erlang:monitor(process,Pid),
    Pid ! {call,Msg,self()},
    Return = receive
		 {Pid,Result} ->
		     Result;
		 {'DOWN', Ref, _, _, _} ->
		     {error,master_died}
	     end,	    
    erlang:demonitor(Ref, [flush]),
    Return.

reply(Result,To) ->
    To ! {self(),Result},
    ok.

init_nodes(NodeOptions, InitOptions)->
    _ = ping_nodes(NodeOptions),
    start_nodes(InitOptions),
    eval_on_nodes(InitOptions),
    {Inaccessible, NodeOptions1}=ping_nodes(NodeOptions),
    InitOptions1 = filter_accessible(InitOptions, Inaccessible),
    {Inaccessible, NodeOptions1, InitOptions1}.

% only nodes which are inaccessible now, should be initiated later
filter_accessible(InitOptions, Inaccessible)->
    [{Node,Option}||{Node,Option}<-InitOptions, lists:member(Node, Inaccessible)].

start_nodes(InitOptions)->
    lists:foreach(fun({NodeName, Options})->
	[NodeS,HostS]=string:lexemes(atom_to_list(NodeName), "@"),
	Node=list_to_atom(NodeS),
	Host=list_to_atom(HostS),
	HasNodeStart = lists:keymember(node_start, 1, Options),
	IsAlive = lists:member(NodeName, nodes()),
	case {HasNodeStart, IsAlive} of
	    {false, false}->
		io:format("WARNING: Node ~w is not alive but has no "
			  "node_start option~n", [NodeName]);
	    {false, true}->
		io:format("Node ~w is alive~n", [NodeName]);
	    {true, false}->
		{node_start, NodeStart} = lists:keyfind(node_start, 1, Options),
		{value, {callback_module, Callback}, NodeStart2}=
		    lists:keytake(callback_module, 1, NodeStart),
		case Callback:start(Host, Node, NodeStart2) of
		    {ok, NodeName} ->
			io:format("Node ~w started successfully "
				  "with callback ~w~n", [NodeName,Callback]);
		    {error, Reason, _NodeName} ->
			io:format("Failed to start node ~w with callback ~w! "
				  "Reason: ~tp~n", [NodeName, Callback, Reason])
		end;
	    {true, true}->
		io:format("WARNING: Node ~w is alive but has node_start "
			  "option~n", [NodeName])
	end
    end,
    InitOptions).

eval_on_nodes(InitOptions)->
    lists:foreach(fun({NodeName, Options})->
	HasEval = lists:keymember(eval, 1, Options),
	IsAlive = lists:member(NodeName, nodes()),
	case {HasEval, IsAlive} of
	    {false,_}->
		ok;
	    {true,false}->
		io:format("WARNING: Node ~w is not alive but has eval "
			  "option~n", [NodeName]);
	    {true,true}->
		{eval, MFAs} = lists:keyfind(eval, 1, Options),
		evaluate(NodeName, MFAs)
        end
    end,
    InitOptions).

evaluate(Node, [{M,F,A}|MFAs])->
    case rpc:call(Node, M, F, A) of
        {badrpc,Reason}->
	    io:format("WARNING: Failed to call ~w:~tw/~w on node ~w "
		      "due to ~tp~n", [M,F,length(A),Node,Reason]);
	Result->
	    io:format("Called ~w:~tw/~w on node ~w, result: ~tp~n",
		      [M,F,length(A),Node,Result])
    end,
    evaluate(Node, MFAs);
evaluate(_Node, [])->
    ok.

%cast(Msg) ->
%    cast(whereis(ct_master),Msg).

cast(undefined,_Msg) ->
    {error,not_running};

cast(Pid,Msg) ->
    Pid ! {cast,Msg},
    ok.
