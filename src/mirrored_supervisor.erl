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

%% TODO documentation
%% We need a thing like a supervisor, except that it joins something
%% like a process group, and if a child process dies it can be
%% restarted under another supervisor (probably on another node).
%% For docs: start_link/2 and /3 become /3 and /4.

-define(SUPERVISOR, supervisor2).
-define(GEN_SERVER, gen_server2).
-define(ID, ?MODULE).

-define(MNESIA_TABLE, mirrored_sup_childspec).
-define(MNESIA_TABLE_DEF,
        {?MNESIA_TABLE,
         [{record_name, mirrored_sup_childspec},
          {attributes, record_info(fields, mirrored_sup_childspec)}]}).
-define(MNESIA_TABLE_MATCH, {match, #mirrored_sup_childspec{ _ = '_' }}).

-export([start_link/3, start_link/4,
	 start_child/2, restart_child/2,
	 delete_child/2, terminate_child/2,
	 which_children/1, check_childspecs/1]).

-export([behaviour_info/1]).

-behaviour(?GEN_SERVER).

-export([init/1, handle_call/3, handle_info/2, terminate/2, code_change/3,
         handle_cast/2]).

-export([start_internal/3]).
-export([create_tables/0, table_definitions/0]).

-record(mirrored_sup_childspec, {id, sup_pid, childspec}).

-record(state, {sup, group}).

%%----------------------------------------------------------------------------

start_link(_Group, _Mod, _Args) ->
    %% TODO this one is probably fixable.
    exit(mirrored_supervisors_must_be_locally_named).

start_link({local, SupName}, Group, Mod, Args) ->
    {ok, SupPid} = ?SUPERVISOR:start_link({local, SupName}, Mod, Args),
    {ok, _Me} = ?SUPERVISOR:start_child(
                   SupPid, {?ID, {?MODULE, start_internal,
                                  [SupName, Group, Args]},
                            transient, 16#ffffffff, supervisor, [?MODULE]}),
    {ok, SupPid};

start_link({global, _SupName}, _Group, _Mod, _Args) ->
    exit(mirrored_supervisors_must_be_locally_named).

start_child(Sup, ChildSpec)  -> call(Sup, {start_child,  ChildSpec}).
delete_child(Sup, Name)      -> call(Sup, {delete_child, Name}).
restart_child(Sup, Name)     -> call(Sup, {msg, restart_child,   [Sup, Name]}).
terminate_child(Sup, Name)   -> call(Sup, {msg, terminate_child, [Sup, Name]}).
which_children(Sup)          -> ?SUPERVISOR:which_children(Sup).
check_childspecs(ChildSpecs) -> ?SUPERVISOR:check_childspecs(ChildSpecs).

behaviour_info(callbacks) -> [{init,1}];
behaviour_info(_Other)    -> undefined.

call(Sup, Msg) ->
    [Pid] = [Pid || {Name, Pid, _, _} <- which_children(Sup), Name =:= ?ID],
    ?GEN_SERVER:call(Pid, Msg, infinity).

%%----------------------------------------------------------------------------

start_internal(Sup, Group, Args) ->
    ?GEN_SERVER:start_link(?MODULE, {Sup, Group, Args}, [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({Sup, Group, _Args}) ->
    pg2_fixed:create(Group),
    [begin
         io:format("Announce to ~p~n", [Pid]),
         gen_server2:call(Pid, {hello, self()}, infinity),
         erlang:monitor(process, Pid)
     end
     || Pid <- pg2_fixed:get_members(Group)],
    ok = pg2_fixed:join(Group, self()),
    {ok, #state{sup = Sup, group = Group}}.

handle_call({start_child, ChildSpec}, _From, State = #state{sup = Sup}) ->
    {reply, case mnesia:transaction(fun() -> check_start(ChildSpec) end) of
                {atomic, start}   -> io:format("Start ~p~n", [id(ChildSpec)]),
                                     start(Sup, ChildSpec);
                {atomic, already} -> io:format("Already ~p~n", [id(ChildSpec)]),
                                           {ok, already}
            end, State};

handle_call({delete_child, Id}, _From, State = #state{sup = Sup}) ->
    {atomic, ok} = mnesia:transaction(fun() -> delete(Id) end),
    {reply, stop(Sup, Id), State};

handle_call({msg, F, A}, _From, State) ->
    {reply, apply(?SUPERVISOR, F, A), State};

handle_call({hello, Pid}, _From, State) ->
    io:format("Hello from ~p~n", [Pid]),
    erlang:monitor(process, Pid),
    {reply, ok, State};

handle_call(alive, _From, State) ->
    {reply, true, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason},
            State = #state{sup = Sup, group = Group}) ->
    io:format("Pid ~p down!~n", [Pid]),
    %% TODO load balance this
    Self = self(),
    case lists:sort(pg2_fixed:get_members(Group)) of
        [Self | _] -> {atomic, ChildSpecs} =
                          mnesia:transaction(fun() -> update_all(Pid) end),
                      [begin
                           start(Sup, ChildSpec),
                           io:format("Restarted ~p~n", [id(ChildSpec)])
                       end || ChildSpec <- ChildSpecs];
        _          -> ok
    end,
    {noreply, State};

handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

check_start(ChildSpec) ->
    case mnesia:wread({?MNESIA_TABLE, id(ChildSpec)}) of
        []  -> write(ChildSpec),
               start;
        [S] -> #mirrored_sup_childspec{sup_pid = Pid} = S,
               case alive(Pid) of
                   true  -> already; %% TODO return real pid?
                   false -> delete(ChildSpec),
                            write(ChildSpec),
                            start
               end
    end.

alive(Pid) ->
    try
        gen_server:call(Pid, alive, infinity)
    catch
        exit:{noproc, _} -> false
    end.

write(ChildSpec) ->
    ok = mnesia:write(#mirrored_sup_childspec{id        = id(ChildSpec),
                                              sup_pid   = self(),
                                              childspec = ChildSpec}).

delete(Id) ->
    ok = mnesia:delete({?MNESIA_TABLE, Id}).

start(Sup, ChildSpec) ->
    apply(?SUPERVISOR, start_child, [Sup, ChildSpec]).

stop(Sup, Id) ->
    apply(?SUPERVISOR, delete_child, [Sup, Id]).

id({Id, _, _, _, _, _}) -> Id.

update(ChildSpec) ->
    delete(ChildSpec),
    write(ChildSpec),
    ChildSpec.

update_all(OldPid) ->
    MatchHead = #mirrored_sup_childspec{sup_pid   = OldPid,
                                        childspec = '$1',
                                        _         = '_'},
    [update(C) || C <- mnesia:select(?MNESIA_TABLE, [{MatchHead, [], ['$1']}])].

%%----------------------------------------------------------------------------

create_tables() ->
    create_tables([?MNESIA_TABLE_DEF]).

create_tables([]) ->
    ok;
create_tables([{Table, Attributes} | Ts]) ->
    case mnesia:create_table(Table, Attributes) of
        {atomic, ok}                               -> create_tables(Ts);
        {aborted, {already_exists, ?MNESIA_TABLE}} -> create_tables(Ts);
        Err                                        -> Err
    end.

table_definitions() ->
    {Name, Attributes} = ?MNESIA_TABLE_DEF,
    [{Name, [?MNESIA_TABLE_MATCH | Attributes]}].

%%----------------------------------------------------------------------------
