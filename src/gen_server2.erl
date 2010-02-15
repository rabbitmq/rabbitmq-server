%% This file is a copy of gen_server.erl from the R13B-1 Erlang/OTP
%% distribution, with the following modifications:
%%
%% 1) the module name is gen_server2
%%
%% 2) more efficient handling of selective receives in callbacks
%% gen_server2 processes drain their message queue into an internal
%% buffer before invoking any callback module functions. Messages are
%% dequeued from the buffer for processing. Thus the effective message
%% queue of a gen_server2 process is the concatenation of the internal
%% buffer and the real message queue.
%% As a result of the draining, any selective receive invoked inside a
%% callback is less likely to have to scan a large message queue.
%%
%% 3) gen_server2:cast is guaranteed to be order-preserving
%% The original code could reorder messages when communicating with a
%% process on a remote node that was not currently connected.
%%
%% 4) The new functions gen_server2:pcall/3, pcall/4, and pcast/3
%% allow callers to attach priorities to requests. Requests with
%% higher priorities are processed before requests with lower
%% priorities. The default priority is 0.
%%
%% 5) The callback module can optionally implement
%% handle_pre_hibernate/1 and handle_post_hibernate/1. These will be
%% called immediately prior to and post hibernation, respectively. If
%% handle_pre_hibernate returns {hibernate, NewState} then the process
%% will hibernate. If the module does not implement
%% handle_pre_hibernate/1 then the default action is to hibernate.
%%
%% 6) init can return a 4th arg, {backoff, InitialTimeout,
%% MinimumTimeout, DesiredHibernatePeriod} (all in
%% milliseconds). Then, on all callbacks which can return a timeout
%% (including init), timeout can be 'hibernate'. When this is the
%% case, the current timeout value will be used (initially, the
%% InitialTimeout supplied from init). After this timeout has
%% occurred, hibernation will occur as normal. Upon awaking, a new
%% current timeout value will be calculated.
%%
%% The purpose is that the gen_server2 takes care of adjusting the
%% current timeout value such that the process will increase the
%% timeout value repeatedly if it is unable to sleep for the
%% DesiredHibernatePeriod. If it is able to sleep for the
%% DesiredHibernatePeriod it will decrease the current timeout down to
%% the MinimumTimeout, so that the process is put to sleep sooner (and
%% hopefully stays asleep for longer). In short, should a process
%% using this receive a burst of messages, it should not hibernate
%% between those messages, but as the messages become less frequent,
%% the process will not only hibernate, it will do so sooner after
%% each message.
%%
%% When using this backoff mechanism, normal timeout values (i.e. not
%% 'hibernate') can still be used, and if they are used then the
%% handle_info(timeout, State) will be called as normal. In this case,
%% returning 'hibernate' from handle_info(timeout, State) will not
%% hibernate the process immediately, as it would if backoff wasn't
%% being used. Instead it'll wait for the current timeout as described
%% above.

%% All modifications are (C) 2009-2010 LShift Ltd.

%% ``The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved via the world wide web at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% The Initial Developer of the Original Code is Ericsson Utvecklings AB.
%% Portions created by Ericsson are Copyright 1999, Ericsson Utvecklings
%% AB. All Rights Reserved.''
%% 
%%     $Id$
%%
-module(gen_server2).

%%% ---------------------------------------------------
%%%
%%% The idea behind THIS server is that the user module
%%% provides (different) functions to handle different
%%% kind of inputs. 
%%% If the Parent process terminates the Module:terminate/2
%%% function is called.
%%%
%%% The user module should export:
%%%
%%%   init(Args)  
%%%     ==> {ok, State}
%%%         {ok, State, Timeout}
%%%         {ok, State, Timeout, Backoff}
%%%         ignore
%%%         {stop, Reason}
%%%
%%%   handle_call(Msg, {From, Tag}, State)
%%%
%%%    ==> {reply, Reply, State}
%%%        {reply, Reply, State, Timeout}
%%%        {noreply, State}
%%%        {noreply, State, Timeout}
%%%        {stop, Reason, Reply, State}  
%%%              Reason = normal | shutdown | Term terminate(State) is called
%%%
%%%   handle_cast(Msg, State)
%%%
%%%    ==> {noreply, State}
%%%        {noreply, State, Timeout}
%%%        {stop, Reason, State} 
%%%              Reason = normal | shutdown | Term terminate(State) is called
%%%
%%%   handle_info(Info, State) Info is e.g. {'EXIT', P, R}, {nodedown, N}, ...
%%%
%%%    ==> {noreply, State}
%%%        {noreply, State, Timeout}
%%%        {stop, Reason, State} 
%%%              Reason = normal | shutdown | Term, terminate(State) is called
%%%
%%%   terminate(Reason, State) Let the user module clean up
%%%        always called when server terminates
%%%
%%%    ==> ok
%%%
%%%   handle_pre_hibernate(State)
%%%
%%%    ==> {hibernate, State}
%%%        {stop, Reason, State}
%%%              Reason = normal | shutdown | Term, terminate(State) is called
%%%
%%%   handle_post_hibernate(State)
%%%
%%%    ==> {noreply, State}
%%%        {stop, Reason, State}
%%%              Reason = normal | shutdown | Term, terminate(State) is called
%%%
%%% The work flow (of the server) can be described as follows:
%%%
%%%   User module                          Generic
%%%   -----------                          -------
%%%     start            ----->             start
%%%     init             <-----              .
%%%
%%%                                         loop
%%%     handle_call      <-----              .
%%%                      ----->             reply
%%%
%%%     handle_cast      <-----              .
%%%
%%%     handle_info      <-----              .
%%%
%%%     terminate        <-----              .
%%%
%%%                      ----->             reply
%%%
%%%
%%% ---------------------------------------------------

%% API
-export([start/3, start/4,
	 start_link/3, start_link/4,
	 call/2, call/3, pcall/3, pcall/4,
	 cast/2, pcast/3, reply/2,
	 abcast/2, abcast/3,
	 multi_call/2, multi_call/3, multi_call/4,
	 enter_loop/3, enter_loop/4, enter_loop/5, wake_hib/7]).

-export([behaviour_info/1]).

%% System exports
-export([system_continue/3,
	 system_terminate/4,
	 system_code_change/4,
	 format_status/2]).

%% Internal exports
-export([init_it/6, print_event/3]).

-import(error_logger, [format/2]).

%%%=========================================================================
%%%  Specs. These exist only to shut up dialyzer's warnings
%%%=========================================================================

-ifdef(use_specs).

-spec(handle_common_termination/6 ::
      (any(), any(), any(), atom(), any(), any()) -> no_return()). 

-spec(hibernate/7 ::
      (pid(), any(), any(), atom(), any(), queue(), any()) -> no_return()).

-endif.

%%%=========================================================================
%%%  API
%%%=========================================================================

behaviour_info(callbacks) ->
    [{init,1},{handle_call,3},{handle_cast,2},{handle_info,2},
     {terminate,2},{code_change,3}];
behaviour_info(_Other) ->
    undefined.

%%%  -----------------------------------------------------------------
%%% Starts a generic server.
%%% start(Mod, Args, Options)
%%% start(Name, Mod, Args, Options)
%%% start_link(Mod, Args, Options)
%%% start_link(Name, Mod, Args, Options) where:
%%%    Name ::= {local, atom()} | {global, atom()}
%%%    Mod  ::= atom(), callback module implementing the 'real' server
%%%    Args ::= term(), init arguments (to Mod:init/1)
%%%    Options ::= [{timeout, Timeout} | {debug, [Flag]}]
%%%      Flag ::= trace | log | {logfile, File} | statistics | debug
%%%          (debug == log && statistics)
%%% Returns: {ok, Pid} |
%%%          {error, {already_started, Pid}} |
%%%          {error, Reason}
%%% -----------------------------------------------------------------
start(Mod, Args, Options) ->
    gen:start(?MODULE, nolink, Mod, Args, Options).

start(Name, Mod, Args, Options) ->
    gen:start(?MODULE, nolink, Name, Mod, Args, Options).

start_link(Mod, Args, Options) ->
    gen:start(?MODULE, link, Mod, Args, Options).

start_link(Name, Mod, Args, Options) ->
    gen:start(?MODULE, link, Name, Mod, Args, Options).


%% -----------------------------------------------------------------
%% Make a call to a generic server.
%% If the server is located at another node, that node will
%% be monitored.
%% If the client is trapping exits and is linked server termination
%% is handled here (? Shall we do that here (or rely on timeouts) ?).
%% ----------------------------------------------------------------- 
call(Name, Request) ->
    case catch gen:call(Name, '$gen_call', Request) of
	{ok,Res} ->
	    Res;
	{'EXIT',Reason} ->
	    exit({Reason, {?MODULE, call, [Name, Request]}})
    end.

call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$gen_call', Request, Timeout) of
	{ok,Res} ->
	    Res;
	{'EXIT',Reason} ->
	    exit({Reason, {?MODULE, call, [Name, Request, Timeout]}})
    end.

pcall(Name, Priority, Request) ->
    case catch gen:call(Name, '$gen_pcall', {Priority, Request}) of
	{ok,Res} ->
	    Res;
	{'EXIT',Reason} ->
	    exit({Reason, {?MODULE, pcall, [Name, Priority, Request]}})
    end.

pcall(Name, Priority, Request, Timeout) ->
    case catch gen:call(Name, '$gen_pcall', {Priority, Request}, Timeout) of
	{ok,Res} ->
	    Res;
	{'EXIT',Reason} ->
	    exit({Reason, {?MODULE, pcall, [Name, Priority, Request, Timeout]}})
    end.

%% -----------------------------------------------------------------
%% Make a cast to a generic server.
%% -----------------------------------------------------------------
cast({global,Name}, Request) ->
    catch global:send(Name, cast_msg(Request)),
    ok;
cast({Name,Node}=Dest, Request) when is_atom(Name), is_atom(Node) -> 
    do_cast(Dest, Request);
cast(Dest, Request) when is_atom(Dest) ->
    do_cast(Dest, Request);
cast(Dest, Request) when is_pid(Dest) ->
    do_cast(Dest, Request).

do_cast(Dest, Request) -> 
    do_send(Dest, cast_msg(Request)),
    ok.
    
cast_msg(Request) -> {'$gen_cast',Request}.

pcast({global,Name}, Priority, Request) ->
    catch global:send(Name, cast_msg(Priority, Request)),
    ok;
pcast({Name,Node}=Dest, Priority, Request) when is_atom(Name), is_atom(Node) -> 
    do_cast(Dest, Priority, Request);
pcast(Dest, Priority, Request) when is_atom(Dest) ->
    do_cast(Dest, Priority, Request);
pcast(Dest, Priority, Request) when is_pid(Dest) ->
    do_cast(Dest, Priority, Request).

do_cast(Dest, Priority, Request) -> 
    do_send(Dest, cast_msg(Priority, Request)),
    ok.
    
cast_msg(Priority, Request) -> {'$gen_pcast', {Priority, Request}}.

%% -----------------------------------------------------------------
%% Send a reply to the client.
%% -----------------------------------------------------------------
reply({To, Tag}, Reply) ->
    catch To ! {Tag, Reply}.

%% ----------------------------------------------------------------- 
%% Asyncronous broadcast, returns nothing, it's just send'n prey
%%-----------------------------------------------------------------  
abcast(Name, Request) when is_atom(Name) ->
    do_abcast([node() | nodes()], Name, cast_msg(Request)).

abcast(Nodes, Name, Request) when is_list(Nodes), is_atom(Name) ->
    do_abcast(Nodes, Name, cast_msg(Request)).

do_abcast([Node|Nodes], Name, Msg) when is_atom(Node) ->
    do_send({Name,Node},Msg),
    do_abcast(Nodes, Name, Msg);
do_abcast([], _,_) -> abcast.

%%% -----------------------------------------------------------------
%%% Make a call to servers at several nodes.
%%% Returns: {[Replies],[BadNodes]}
%%% A Timeout can be given
%%% 
%%% A middleman process is used in case late answers arrives after
%%% the timeout. If they would be allowed to glog the callers message
%%% queue, it would probably become confused. Late answers will 
%%% now arrive to the terminated middleman and so be discarded.
%%% -----------------------------------------------------------------
multi_call(Name, Req)
  when is_atom(Name) ->
    do_multi_call([node() | nodes()], Name, Req, infinity).

multi_call(Nodes, Name, Req) 
  when is_list(Nodes), is_atom(Name) ->
    do_multi_call(Nodes, Name, Req, infinity).

multi_call(Nodes, Name, Req, infinity) ->
    do_multi_call(Nodes, Name, Req, infinity);
multi_call(Nodes, Name, Req, Timeout) 
  when is_list(Nodes), is_atom(Name), is_integer(Timeout), Timeout >= 0 ->
    do_multi_call(Nodes, Name, Req, Timeout).


%%-----------------------------------------------------------------
%% enter_loop(Mod, Options, State, <ServerName>, <TimeOut>, <Backoff>) ->_ 
%%   
%% Description: Makes an existing process into a gen_server. 
%%              The calling process will enter the gen_server receive 
%%              loop and become a gen_server process.
%%              The process *must* have been started using one of the 
%%              start functions in proc_lib, see proc_lib(3). 
%%              The user is responsible for any initialization of the 
%%              process, including registering a name for it.
%%-----------------------------------------------------------------
enter_loop(Mod, Options, State) ->
    enter_loop(Mod, Options, State, self(), infinity, undefined).

enter_loop(Mod, Options, State, Backoff = {backoff, _, _ , _}) ->
    enter_loop(Mod, Options, State, self(), infinity, Backoff);

enter_loop(Mod, Options, State, ServerName = {_, _}) ->
    enter_loop(Mod, Options, State, ServerName, infinity, undefined);

enter_loop(Mod, Options, State, Timeout) ->
    enter_loop(Mod, Options, State, self(), Timeout, undefined).

enter_loop(Mod, Options, State, ServerName, Backoff = {backoff, _, _, _}) ->
    enter_loop(Mod, Options, State, ServerName, infinity, Backoff);

enter_loop(Mod, Options, State, ServerName, Timeout) ->
    enter_loop(Mod, Options, State, ServerName, Timeout, undefined).

enter_loop(Mod, Options, State, ServerName, Timeout, Backoff) ->
    Name = get_proc_name(ServerName),
    Parent = get_parent(),
    Debug = debug_options(Name, Options),
    Queue = priority_queue:new(),
    Backoff1 = extend_backoff(Backoff),
    loop(Parent, Name, State, Mod, Timeout, Backoff1, Queue, Debug).

%%%========================================================================
%%% Gen-callback functions
%%%========================================================================

%%% ---------------------------------------------------
%%% Initiate the new process.
%%% Register the name using the Rfunc function
%%% Calls the Mod:init/Args function.
%%% Finally an acknowledge is sent to Parent and the main
%%% loop is entered.
%%% ---------------------------------------------------
init_it(Starter, self, Name, Mod, Args, Options) ->
    init_it(Starter, self(), Name, Mod, Args, Options);
init_it(Starter, Parent, Name0, Mod, Args, Options) ->
    Name = name(Name0),
    Debug = debug_options(Name, Options),
    Queue = priority_queue:new(),
    case catch Mod:init(Args) of
	{ok, State} ->
	    proc_lib:init_ack(Starter, {ok, self()}), 	    
	    loop(Parent, Name, State, Mod, infinity, undefined, Queue, Debug);
	{ok, State, Timeout} ->
	    proc_lib:init_ack(Starter, {ok, self()}),
	    loop(Parent, Name, State, Mod, Timeout, undefined, Queue, Debug);
	{ok, State, Timeout, Backoff = {backoff, _, _, _}} ->
            Backoff1 = extend_backoff(Backoff),
	    proc_lib:init_ack(Starter, {ok, self()}),
	    loop(Parent, Name, State, Mod, Timeout, Backoff1, Queue, Debug);
	{stop, Reason} ->
	    %% For consistency, we must make sure that the
	    %% registered name (if any) is unregistered before
	    %% the parent process is notified about the failure.
	    %% (Otherwise, the parent process could get
	    %% an 'already_started' error if it immediately
	    %% tried starting the process again.)
	    unregister_name(Name0),
	    proc_lib:init_ack(Starter, {error, Reason}),
	    exit(Reason);
	ignore ->
	    unregister_name(Name0),
	    proc_lib:init_ack(Starter, ignore),
	    exit(normal);
	{'EXIT', Reason} ->
	    unregister_name(Name0),
	    proc_lib:init_ack(Starter, {error, Reason}),
	    exit(Reason);
	Else ->
	    Error = {bad_return_value, Else},
	    proc_lib:init_ack(Starter, {error, Error}),
	    exit(Error)
    end.

name({local,Name}) -> Name;
name({global,Name}) -> Name;
%% name(Pid) when is_pid(Pid) -> Pid;
%% when R11 goes away, drop the line beneath and uncomment the line above
name(Name) -> Name.

unregister_name({local,Name}) ->
    _ = (catch unregister(Name));
unregister_name({global,Name}) ->
    _ = global:unregister_name(Name);
unregister_name(Pid) when is_pid(Pid) ->
    Pid;
% Under R12 let's just ignore it, as we have a single term as Name.
% On R13 it will never get here, as we get tuple with 'local/global' atom.
unregister_name(_Name) -> ok.

extend_backoff(undefined) ->
    undefined;
extend_backoff({backoff, InitialTimeout, MinimumTimeout, DesiredHibPeriod}) ->
    {backoff, InitialTimeout, MinimumTimeout, DesiredHibPeriod, now()}.

%%%========================================================================
%%% Internal functions
%%%========================================================================
%%% ---------------------------------------------------
%%% The MAIN loop.
%%% ---------------------------------------------------
loop(Parent, Name, State, Mod, hibernate, undefined, Queue, Debug) ->
    pre_hibernate(Parent, Name, State, Mod, undefined, Queue, Debug);
loop(Parent, Name, State, Mod, Time, TimeoutState, Queue, Debug) ->
    process_next_msg(Parent, Name, State, Mod, Time, TimeoutState,
                     drain(Queue), Debug).

drain(Queue) ->
    receive
        Input -> drain(in(Input, Queue))
    after 0 -> Queue
    end.

process_next_msg(Parent, Name, State, Mod, Time, TimeoutState, Queue, Debug) ->
    case priority_queue:out(Queue) of
        {{value, Msg}, Queue1} ->
            process_msg(Parent, Name, State, Mod,
                        Time, TimeoutState, Queue1, Debug, Msg);
        {empty, Queue1} ->
            {Time1, HibOnTimeout}
                = case {Time, TimeoutState} of
                      {hibernate, {backoff, Current, _Min, _Desired, _RSt}} ->
                          {Current, true};
                      {hibernate, _} ->
                          %% wake_hib/7 will set Time to hibernate. If
                          %% we were woken and didn't receive a msg
                          %% then we will get here and need a sensible
                          %% value for Time1, otherwise we crash.
                          %% R13B1 always waits infinitely when waking
                          %% from hibernation, so that's what we do
                          %% here too.
                          {infinity, false};
                      _ -> {Time, false}
                  end,
            receive
                Input ->
                    %% Time could be 'hibernate' here, so *don't* call loop
                    process_next_msg(
                      Parent, Name, State, Mod, Time, TimeoutState,
                      drain(in(Input, Queue1)), Debug)
            after Time1 ->
                    case HibOnTimeout of
                        true ->
                            pre_hibernate(
                              Parent, Name, State, Mod, TimeoutState, Queue1,
                              Debug);
                        false ->
                            process_msg(
                              Parent, Name, State, Mod, Time, TimeoutState,
                              Queue1, Debug, timeout)
                    end
            end
    end.

wake_hib(Parent, Name, State, Mod, TS, Queue, Debug) ->
    TimeoutState1 = case TS of
                        undefined ->
                            undefined;
                        {SleptAt, TimeoutState} ->
                            adjust_timeout_state(SleptAt, now(), TimeoutState)
                    end,
    post_hibernate(Parent, Name, State, Mod, TimeoutState1,
                   drain(Queue), Debug).

hibernate(Parent, Name, State, Mod, TimeoutState, Queue, Debug) ->
    TS = case TimeoutState of
             undefined             -> undefined;
             {backoff, _, _, _, _} -> {now(), TimeoutState}
         end,
    proc_lib:hibernate(?MODULE, wake_hib, [Parent, Name, State, Mod,
                                           TS, Queue, Debug]).

pre_hibernate(Parent, Name, State, Mod, TimeoutState, Queue, Debug) ->
    case erlang:function_exported(Mod, handle_pre_hibernate, 1) of
        true ->
            case catch Mod:handle_pre_hibernate(State) of
                {hibernate, NState} ->
                    hibernate(Parent, Name, NState, Mod, TimeoutState, Queue,
                              Debug);
                Reply ->
                    handle_common_termination(Reply, Name, pre_hibernate,
                                              Mod, State, Debug)
            end;
        false ->
            hibernate(Parent, Name, State, Mod, TimeoutState, Queue, Debug)
    end.

post_hibernate(Parent, Name, State, Mod, TimeoutState, Queue, Debug) ->
    case erlang:function_exported(Mod, handle_post_hibernate, 1) of
        true ->
            case catch Mod:handle_post_hibernate(State) of
                {noreply, NState} ->
                    process_next_msg(Parent, Name, NState, Mod, infinity,
                                     TimeoutState, Queue, Debug);
                {noreply, NState, Time} ->
                    process_next_msg(Parent, Name, NState, Mod, Time,
                                     TimeoutState, Queue, Debug);
                Reply ->
                    handle_common_termination(Reply, Name, post_hibernate,
                                              Mod, State, Debug)
            end;
        false ->
            %% use hibernate here, not infinity. This matches
            %% R13B. The key is that we should be able to get through
            %% to process_msg calling sys:handle_system_msg with Time
            %% still set to hibernate, iff that msg is the very msg
            %% that woke us up (or the first msg we receive after
            %% waking up).
            process_next_msg(Parent, Name, State, Mod, hibernate,
                             TimeoutState, Queue, Debug)
    end.

adjust_timeout_state(SleptAt, AwokeAt, {backoff, CurrentTO, MinimumTO,
                                        DesiredHibPeriod, RandomState}) ->
    NapLengthMicros = timer:now_diff(AwokeAt, SleptAt),
    CurrentMicros = CurrentTO * 1000,
    MinimumMicros = MinimumTO * 1000,
    DesiredHibMicros = DesiredHibPeriod * 1000,
    GapBetweenMessagesMicros = NapLengthMicros + CurrentMicros,
    Base =
        %% If enough time has passed between the last two messages then we
        %% should consider sleeping sooner. Otherwise stay awake longer.
        case GapBetweenMessagesMicros > (MinimumMicros + DesiredHibMicros) of
            true -> lists:max([MinimumTO, CurrentTO div 2]);
            false -> CurrentTO
        end,
    {Extra, RandomState1} = random:uniform_s(Base, RandomState),
    CurrentTO1 = Base + Extra,
    {backoff, CurrentTO1, MinimumTO, DesiredHibPeriod, RandomState1}.

in({'$gen_pcast', {Priority, Msg}}, Queue) ->
    priority_queue:in({'$gen_cast', Msg}, Priority, Queue);
in({'$gen_pcall', From, {Priority, Msg}}, Queue) ->
    priority_queue:in({'$gen_call', From, Msg}, Priority, Queue);
in(Input, Queue) ->
    priority_queue:in(Input, Queue).

process_msg(Parent, Name, State, Mod, Time, TimeoutState, Queue,
            Debug, Msg) ->
    case Msg of
	{system, From, Req} ->
	    sys:handle_system_msg
              (Req, From, Parent, ?MODULE, Debug,
               [Name, State, Mod, Time, TimeoutState, Queue]);
        %% gen_server puts Hib on the end as the 7th arg, but that
        %% version of the function seems not to be documented so
        %% leaving out for now.
	{'EXIT', Parent, Reason} ->
	    terminate(Reason, Name, Msg, Mod, State, Debug);
	_Msg when Debug =:= [] ->
	    handle_msg(Msg, Parent, Name, State, Mod, TimeoutState, Queue);
	_Msg ->
	    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event}, 
				      Name, {in, Msg}),
	    handle_msg(Msg, Parent, Name, State, Mod, TimeoutState, Queue,
                       Debug1)
    end.

%%% ---------------------------------------------------
%%% Send/recive functions
%%% ---------------------------------------------------
do_send(Dest, Msg) ->
    catch erlang:send(Dest, Msg).

do_multi_call(Nodes, Name, Req, infinity) ->
    Tag = make_ref(),
    Monitors = send_nodes(Nodes, Name, Tag, Req),
    rec_nodes(Tag, Monitors, Name, undefined);
do_multi_call(Nodes, Name, Req, Timeout) ->
    Tag = make_ref(),
    Caller = self(),
    Receiver =
	spawn(
	  fun() ->
		  %% Middleman process. Should be unsensitive to regular
		  %% exit signals. The sychronization is needed in case
		  %% the receiver would exit before the caller started
		  %% the monitor.
		  process_flag(trap_exit, true),
		  Mref = erlang:monitor(process, Caller),
		  receive
		      {Caller,Tag} ->
			  Monitors = send_nodes(Nodes, Name, Tag, Req),
			  TimerId = erlang:start_timer(Timeout, self(), ok),
			  Result = rec_nodes(Tag, Monitors, Name, TimerId),
			  exit({self(),Tag,Result});
		      {'DOWN',Mref,_,_,_} ->
			  %% Caller died before sending us the go-ahead.
			  %% Give up silently.
			  exit(normal)
		  end
	  end),
    Mref = erlang:monitor(process, Receiver),
    Receiver ! {self(),Tag},
    receive
	{'DOWN',Mref,_,_,{Receiver,Tag,Result}} ->
	    Result;
	{'DOWN',Mref,_,_,Reason} ->
	    %% The middleman code failed. Or someone did 
	    %% exit(_, kill) on the middleman process => Reason==killed
	    exit(Reason)
    end.

send_nodes(Nodes, Name, Tag, Req) ->
    send_nodes(Nodes, Name, Tag, Req, []).

send_nodes([Node|Tail], Name, Tag, Req, Monitors)
  when is_atom(Node) ->
    Monitor = start_monitor(Node, Name),
    %% Handle non-existing names in rec_nodes.
    catch {Name, Node} ! {'$gen_call', {self(), {Tag, Node}}, Req},
    send_nodes(Tail, Name, Tag, Req, [Monitor | Monitors]);
send_nodes([_Node|Tail], Name, Tag, Req, Monitors) ->
    %% Skip non-atom Node
    send_nodes(Tail, Name, Tag, Req, Monitors);
send_nodes([], _Name, _Tag, _Req, Monitors) -> 
    Monitors.

%% Against old nodes:
%% If no reply has been delivered within 2 secs. (per node) check that
%% the server really exists and wait for ever for the answer.
%%
%% Against contemporary nodes:
%% Wait for reply, server 'DOWN', or timeout from TimerId.

rec_nodes(Tag, Nodes, Name, TimerId) -> 
    rec_nodes(Tag, Nodes, Name, [], [], 2000, TimerId).

rec_nodes(Tag, [{N,R}|Tail], Name, Badnodes, Replies, Time, TimerId ) ->
    receive
	{'DOWN', R, _, _, _} ->
	    rec_nodes(Tag, Tail, Name, [N|Badnodes], Replies, Time, TimerId);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    unmonitor(R), 
	    rec_nodes(Tag, Tail, Name, Badnodes, 
		      [{N,Reply}|Replies], Time, TimerId);
	{timeout, TimerId, _} ->	
	    unmonitor(R),
	    %% Collect all replies that already have arrived
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes(Tag, [N|Tail], Name, Badnodes, Replies, Time, TimerId) ->
    %% R6 node
    receive
	{nodedown, N} ->
	    monitor_node(N, false),
	    rec_nodes(Tag, Tail, Name, [N|Badnodes], Replies, 2000, TimerId);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes(Tag, Tail, Name, Badnodes,
		      [{N,Reply}|Replies], 2000, TimerId);
	{timeout, TimerId, _} ->	
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    %% Collect all replies that already have arrived
	    rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
    after Time ->
	    case rpc:call(N, erlang, whereis, [Name]) of
		Pid when is_pid(Pid) -> % It exists try again.
		    rec_nodes(Tag, [N|Tail], Name, Badnodes,
			      Replies, infinity, TimerId);
		_ -> % badnode
		    receive {nodedown, N} -> ok after 0 -> ok end,
		    monitor_node(N, false),
		    rec_nodes(Tag, Tail, Name, [N|Badnodes],
			      Replies, 2000, TimerId)
	    end
    end;
rec_nodes(_, [], _, Badnodes, Replies, _, TimerId) ->
    case catch erlang:cancel_timer(TimerId) of
	false ->  % It has already sent it's message
	    receive
		{timeout, TimerId, _} -> ok
	    after 0 ->
		    ok
	    end;
	_ -> % Timer was cancelled, or TimerId was 'undefined'
	    ok
    end,
    {Replies, Badnodes}.

%% Collect all replies that already have arrived
rec_nodes_rest(Tag, [{N,R}|Tail], Name, Badnodes, Replies) ->
    receive
	{'DOWN', R, _, _, _} ->
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies);
	{{Tag, N}, Reply} -> %% Tag is bound !!!
	    unmonitor(R),
	    rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N,Reply}|Replies])
    after 0 ->
	    unmonitor(R),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes_rest(Tag, [N|Tail], Name, Badnodes, Replies) ->
    %% R6 node
    receive
	{nodedown, N} ->
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies);
	{{Tag, N}, Reply} ->  %% Tag is bound !!!
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N,Reply}|Replies])
    after 0 ->
	    receive {nodedown, N} -> ok after 0 -> ok end,
	    monitor_node(N, false),
	    rec_nodes_rest(Tag, Tail, Name, [N|Badnodes], Replies)
    end;
rec_nodes_rest(_Tag, [], _Name, Badnodes, Replies) ->
    {Replies, Badnodes}.


%%% ---------------------------------------------------
%%% Monitor functions
%%% ---------------------------------------------------

start_monitor(Node, Name) when is_atom(Node), is_atom(Name) ->
    if node() =:= nonode@nohost, Node =/= nonode@nohost ->
	    Ref = make_ref(),
	    self() ! {'DOWN', Ref, process, {Name, Node}, noconnection},
	    {Node, Ref};
       true ->
	    case catch erlang:monitor(process, {Name, Node}) of
		{'EXIT', _} ->
		    %% Remote node is R6
		    monitor_node(Node, true),
		    Node;
		Ref when is_reference(Ref) ->
		    {Node, Ref}
	    end
    end.

%% Cancels a monitor started with Ref=erlang:monitor(_, _).
unmonitor(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref),
    receive
	{'DOWN', Ref, _, _, _} ->
	    true
    after 0 ->
	    true
    end.

%%% ---------------------------------------------------
%%% Message handling functions
%%% ---------------------------------------------------

dispatch({'$gen_cast', Msg}, Mod, State) ->
    Mod:handle_cast(Msg, State);
dispatch(Info, Mod, State) ->
    Mod:handle_info(Info, State).

handle_msg({'$gen_call', From, Msg},
           Parent, Name, State, Mod, TimeoutState, Queue) ->
    case catch Mod:handle_call(Msg, From, State) of
	{reply, Reply, NState} ->
	    reply(From, Reply),
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue, []);
	{reply, Reply, NState, Time1} ->
	    reply(From, Reply),
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, []);
	{noreply, NState} ->
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue, []);
	{noreply, NState, Time1} ->
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, []);
	{stop, Reason, Reply, NState} ->
	    {'EXIT', R} = 
		(catch terminate(Reason, Name, Msg, Mod, NState, [])),
	    reply(From, Reply),
	    exit(R);
	Other -> handle_common_reply(Other, Parent, Name, Msg, Mod, State,
                                     TimeoutState, Queue)
    end;
handle_msg(Msg,
           Parent, Name, State, Mod, TimeoutState, Queue) ->
    Reply = (catch dispatch(Msg, Mod, State)),
    handle_common_reply(Reply, Parent, Name, Msg, Mod, State,
                        TimeoutState, Queue).

handle_msg({'$gen_call', From, Msg},
           Parent, Name, State, Mod, TimeoutState, Queue, Debug) ->
    case catch Mod:handle_call(Msg, From, State) of
	{reply, Reply, NState} ->
	    Debug1 = reply(Name, From, Reply, NState, Debug),
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue,
                 Debug1);
	{reply, Reply, NState, Time1} ->
	    Debug1 = reply(Name, From, Reply, NState, Debug),
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, Debug1);
	{noreply, NState} ->
	    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event}, Name,
				      {noreply, NState}),
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue,
                 Debug1);
	{noreply, NState, Time1} ->
	    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event}, Name,
				      {noreply, NState}),
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, Debug1);
	{stop, Reason, Reply, NState} ->
	    {'EXIT', R} = 
		(catch terminate(Reason, Name, Msg, Mod, NState, Debug)),
	    reply(Name, From, Reply, NState, Debug),
	    exit(R);
	Other ->
	    handle_common_reply(Other, Parent, Name, Msg, Mod, State,
                                TimeoutState, Queue, Debug)
    end;
handle_msg(Msg,
           Parent, Name, State, Mod, TimeoutState, Queue, Debug) ->
    Reply = (catch dispatch(Msg, Mod, State)),
    handle_common_reply(Reply, Parent, Name, Msg, Mod, State,
                        TimeoutState, Queue, Debug).

handle_common_reply(Reply, Parent, Name, Msg, Mod, State,
                    TimeoutState, Queue) ->
    case Reply of
	{noreply, NState} ->
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue, []);
	{noreply, NState, Time1} ->
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, []);
        _ ->
            handle_common_termination(Reply, Name, Msg, Mod, State, [])
    end.

handle_common_reply(Reply, Parent, Name, Msg, Mod, State, TimeoutState, Queue,
                    Debug) ->
    case Reply of
	{noreply, NState} ->
	    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event}, Name,
				      {noreply, NState}),
	    loop(Parent, Name, NState, Mod, infinity, TimeoutState, Queue,
                 Debug1);
	{noreply, NState, Time1} ->
	    Debug1 = sys:handle_debug(Debug, {?MODULE, print_event}, Name,
				      {noreply, NState}),
	    loop(Parent, Name, NState, Mod, Time1, TimeoutState, Queue, Debug1);
        _ ->
            handle_common_termination(Reply, Name, Msg, Mod, State, Debug)
    end.

handle_common_termination(Reply, Name, Msg, Mod, State, Debug) ->
    case Reply of
	{stop, Reason, NState} ->
	    terminate(Reason, Name, Msg, Mod, NState, Debug);
	{'EXIT', What} ->
	    terminate(What, Name, Msg, Mod, State, Debug);
	_ ->
	    terminate({bad_return_value, Reply}, Name, Msg, Mod, State, Debug)
    end.

reply(Name, {To, Tag}, Reply, State, Debug) ->
    reply({To, Tag}, Reply),
    sys:handle_debug(Debug, {?MODULE, print_event}, Name, 
		     {out, Reply, To, State} ).


%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
system_continue(Parent, Debug, [Name, State, Mod, Time, TimeoutState, Queue]) ->
    loop(Parent, Name, State, Mod, Time, TimeoutState, Queue, Debug).

-ifdef(use_specs).
-spec system_terminate(_, _, _, [_]) -> no_return().
-endif.

system_terminate(Reason, _Parent, Debug, [Name, State, Mod, _Time,
                                          _TimeoutState, _Queue]) ->
    terminate(Reason, Name, [], Mod, State, Debug).

system_code_change([Name, State, Mod, Time, TimeoutState, Queue], _Module,
                   OldVsn, Extra) ->
    case catch Mod:code_change(OldVsn, State, Extra) of
	{ok, NewState} ->
            {ok, [Name, NewState, Mod, Time, TimeoutState, Queue]};
	Else ->
            Else
    end.

%%-----------------------------------------------------------------
%% Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%-----------------------------------------------------------------
print_event(Dev, {in, Msg}, Name) ->
    case Msg of
	{'$gen_call', {From, _Tag}, Call} ->
	    io:format(Dev, "*DBG* ~p got call ~p from ~w~n",
		      [Name, Call, From]);
	{'$gen_cast', Cast} ->
	    io:format(Dev, "*DBG* ~p got cast ~p~n",
		      [Name, Cast]);
	_ ->
	    io:format(Dev, "*DBG* ~p got ~p~n", [Name, Msg])
    end;
print_event(Dev, {out, Msg, To, State}, Name) ->
    io:format(Dev, "*DBG* ~p sent ~p to ~w, new state ~w~n", 
	      [Name, Msg, To, State]);
print_event(Dev, {noreply, State}, Name) ->
    io:format(Dev, "*DBG* ~p new state ~w~n", [Name, State]);
print_event(Dev, Event, Name) ->
    io:format(Dev, "*DBG* ~p dbg  ~p~n", [Name, Event]).


%%% ---------------------------------------------------
%%% Terminate the server.
%%% ---------------------------------------------------

terminate(Reason, Name, Msg, Mod, State, Debug) ->
    case catch Mod:terminate(Reason, State) of
	{'EXIT', R} ->
	    error_info(R, Name, Msg, State, Debug),
	    exit(R);
	_ ->
	    case Reason of
		normal ->
		    exit(normal);
		shutdown ->
		    exit(shutdown);
		{shutdown,_}=Shutdown ->
		    exit(Shutdown);
		_ ->
		    error_info(Reason, Name, Msg, State, Debug),
		    exit(Reason)
	    end
    end.

error_info(_Reason, application_controller, _Msg, _State, _Debug) ->
    %% OTP-5811 Don't send an error report if it's the system process
    %% application_controller which is terminating - let init take care
    %% of it instead
    ok;
error_info(Reason, Name, Msg, State, Debug) ->
    Reason1 = 
	case Reason of
	    {undef,[{M,F,A}|MFAs]} ->
		case code:is_loaded(M) of
		    false ->
			{'module could not be loaded',[{M,F,A}|MFAs]};
		    _ ->
			case erlang:function_exported(M, F, length(A)) of
			    true ->
				Reason;
			    false ->
				{'function not exported',[{M,F,A}|MFAs]}
			end
		end;
	    _ ->
		Reason
	end,    
    format("** Generic server ~p terminating \n"
           "** Last message in was ~p~n"
           "** When Server state == ~p~n"
           "** Reason for termination == ~n** ~p~n",
	   [Name, Msg, State, Reason1]),
    sys:print_log(Debug),
    ok.

%%% ---------------------------------------------------
%%% Misc. functions.
%%% ---------------------------------------------------

opt(Op, [{Op, Value}|_]) ->
    {ok, Value};
opt(Op, [_|Options]) ->
    opt(Op, Options);
opt(_, []) ->
    false.

debug_options(Name, Opts) ->
    case opt(debug, Opts) of
	{ok, Options} -> dbg_options(Name, Options);
	_ -> dbg_options(Name, [])
    end.

dbg_options(Name, []) ->
    Opts = 
	case init:get_argument(generic_debug) of
	    error ->
		[];
	    _ ->
		[log, statistics]
	end,
    dbg_opts(Name, Opts);
dbg_options(Name, Opts) ->
    dbg_opts(Name, Opts).

dbg_opts(Name, Opts) ->
    case catch sys:debug_options(Opts) of
	{'EXIT',_} ->
	    format("~p: ignoring erroneous debug options - ~p~n",
		   [Name, Opts]),
	    [];
	Dbg ->
	    Dbg
    end.

get_proc_name(Pid) when is_pid(Pid) ->
    Pid;
get_proc_name({local, Name}) ->
    case process_info(self(), registered_name) of
	{registered_name, Name} ->
	    Name;
	{registered_name, _Name} ->
	    exit(process_not_registered);
	[] ->
	    exit(process_not_registered)
    end;    
get_proc_name({global, Name}) ->
    case global:safe_whereis_name(Name) of
	undefined ->
	    exit(process_not_registered_globally);
	Pid when Pid =:= self() ->
	    Name;
	_Pid ->
	    exit(process_not_registered_globally)
    end.

get_parent() ->
    case get('$ancestors') of
	[Parent | _] when is_pid(Parent)->
            Parent;
        [Parent | _] when is_atom(Parent)->
            name_to_pid(Parent);
	_ ->
	    exit(process_was_not_started_by_proc_lib)
    end.

name_to_pid(Name) ->
    case whereis(Name) of
	undefined ->
	    case global:safe_whereis_name(Name) of
		undefined ->
		    exit(could_not_find_registerd_name);
		Pid ->
		    Pid
	    end;
	Pid ->
	    Pid
    end.

%%-----------------------------------------------------------------
%% Status information
%%-----------------------------------------------------------------
format_status(Opt, StatusData) ->
    [PDict, SysState, Parent, Debug,
     [Name, State, Mod, _Time, _TimeoutState, Queue]] = StatusData,
    NameTag = if is_pid(Name) ->
		      pid_to_list(Name);
		 is_atom(Name) ->
		      Name
	      end,
    Header = lists:concat(["Status for generic server ", NameTag]),
    Log = sys:get_debug(log, Debug, []),
    Specfic = 
	case erlang:function_exported(Mod, format_status, 2) of
	    true ->
		case catch Mod:format_status(Opt, [PDict, State]) of
		    {'EXIT', _} -> [{data, [{"State", State}]}];
		    Else -> Else
		end;
	    _ ->
		[{data, [{"State", State}]}]
	end,
    [{header, Header},
     {data, [{"Status", SysState},
	     {"Parent", Parent},
	     {"Logged events", Log},
             {"Queued messages", priority_queue:to_list(Queue)}]} |
     Specfic].
