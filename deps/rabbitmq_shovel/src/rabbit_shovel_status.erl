%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_shovel_status).
-behaviour(gen_server).

-export([start_link/0]).

-export([report/3, remove/1, status/0, lookup/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-record(state, {}).
-record(entry, {name, type, info, timestamp}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

report(Name, Type, Info) ->
    gen_server:cast(?SERVER, {report, Name, Type, Info, calendar:local_time()}).

remove(Name) ->
    gen_server:cast(?SERVER, {remove, Name}).

status() ->
    gen_server:call(?SERVER, status, infinity).

lookup(Name) ->
    gen_server:call(?SERVER, {lookup, Name}, infinity).

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME,
                        [named_table, {keypos, #entry.name}, private]),
    {ok, #state{}}.

handle_call(status, _From, State) ->
    Entries = ets:tab2list(?ETS_NAME),
    {reply, [{Entry#entry.name, Entry#entry.type, Entry#entry.info,
              Entry#entry.timestamp}
             || Entry <- Entries], State};

handle_call({lookup, Name}, _From, State) ->
    Link = case ets:lookup(?ETS_NAME, Name) of
               [Entry] -> [{name, Name},
                           {type, Entry#entry.type},
                           {info, Entry#entry.info},
                           {timestamp, Entry#entry.timestamp}];
               [] -> not_found
           end,
    {reply, Link, State}.

handle_cast({report, Name, Type, Info, Timestamp}, State) ->
    true = ets:insert(?ETS_NAME, #entry{name = Name, type = Type, info = Info,
                                        timestamp = Timestamp}),
    rabbit_event:notify(shovel_worker_status,
                        split_name(Name) ++ split_status(Info)),
    {noreply, State};

handle_cast({remove, Name}, State) ->
    true = ets:delete(?ETS_NAME, Name),
    rabbit_event:notify(shovel_worker_removed, split_name(Name)),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

split_status({running, MoreInfo})         -> [{status, running} | MoreInfo];
split_status({terminated, Reason})        -> [{status, terminated},
                                              {reason, Reason}];
split_status(Status) when is_atom(Status) -> [{status, Status}].

split_name({VHost, Name})           -> [{name,  Name},
                                        {vhost, VHost}];
split_name(Name) when is_atom(Name) -> [{name, Name}].
