%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(mirrored_supervisor).

-define(SUPERVISOR, supervisor2).
-define(GEN_SERVER, gen_server2).
-define(TABLE, ?MODULE).

%% TODO documentation
%% We need a thing like a supervisor, except that it joins something
%% like a process group, and if a child process dies it can be
%% restarted under another supervisor (probably on another node).

-export([start_link/2,start_link/3,
	 start_child/2, restart_child/2,
	 delete_child/2, terminate_child/2,
	 which_children/1, find_child/2,
	 check_childspecs/1]).

-export([behaviour_info/1]).

-behaviour(?GEN_SERVER).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([start_internal/2]).

-record(state, {}).

%%----------------------------------------------------------------------------

-define(ID, ?MODULE).

start_link(_Mod, _Args) ->
    exit(mirrored_supervisors_must_be_locally_named).

start_link({local, SupName}, Mod, Args) ->
    {ok, SupPid} = ?SUPERVISOR:start_link({local, SupName}, Mod, Args),
    {ok, _Me} = ?SUPERVISOR:start_child(
                   SupPid, {?ID, {?MODULE, start_internal, [SupName, Args]},
                            transient, 16#ffffffff, supervisor, [?MODULE]}),
    {ok, SupPid};

start_link({_, _SupName}, _Mod, _Args) ->
    exit(mirrored_supervisors_must_be_locally_named).

start_child(Sup, ChildSpec)  -> call(Sup, {start_child,     [Sup, ChildSpec]}).
restart_child(Sup, Name)     -> call(Sup, {restart_child,   [Sup, Name]}).
delete_child(Sup, Name)      -> call(Sup, {delete_child,    [Sup, Name]}).
terminate_child(Sup, Name)   -> call(Sup, {terminate_child, [Sup, Name]}).
which_children(Sup)          -> call(Sup, {which_children,  [Sup]}).
find_child(Sup, Name)        -> call(Sup, {find_child,      [Sup, Name]}).
check_childspecs(ChildSpecs) -> ?SUPERVISOR:check_childspecs(ChildSpecs).

behaviour_info(callbacks) -> [{init,1}];
behaviour_info(_Other)    -> undefined.

call(SupName, Msg) ->
    [{SupName, Pid}] = ets:lookup(?TABLE, SupName),
    ?GEN_SERVER:call(Pid, {sup_msg, Msg}, infinity).

%%----------------------------------------------------------------------------

start_internal(SupName, Args) ->
    {ok, Pid} = ?GEN_SERVER:start_link(?MODULE, Args, [{timeout, infinity}]),
    Ins = fun() -> true = ets:insert(?TABLE, {SupName, Pid}) end,
    try
        Ins()
    catch error:badarg -> ets:new(?TABLE, [named_table]),
                          Ins()
    end,
    {ok, Pid}.

%%----------------------------------------------------------------------------

init(_Args) ->
    {ok, #state{}}.

handle_call({sup_msg, {F, A}}, _From, State) ->
    {reply, apply(?SUPERVISOR, F, A), State};

handle_call(Msg, _From, State) ->
    {reply, {unexpected_call, Msg}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%%-export([create_tables/0, table_definitions/0]).

%% -define(TABLE, {?GROUP_TABLE, [{record_name, gm_group},
%%                                {attributes, record_info(fields, gm_group)}]}).
%% -define(TABLE_MATCH, {match, #gm_group { _ = '_' }}).

%% -define(GROUP_TABLE, gm_group).


%% create_tables() ->
%%     create_tables([?TABLE]).

%% create_tables([]) ->
%%     ok;
%% create_tables([{Table, Attributes} | Tables]) ->
%%     case mnesia:create_table(Table, Attributes) of
%%         {atomic, ok}                          -> create_tables(Tables);
%%         {aborted, {already_exists, gm_group}} -> create_tables(Tables);
%%         Err                                   -> Err
%%     end.

%% table_definitions() ->
%%     {Name, Attributes} = ?TABLE,
%%     [{Name, [?TABLE_MATCH | Attributes]}].


