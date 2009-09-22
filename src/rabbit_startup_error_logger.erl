%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_startup_error_logger).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_event).

-export([init/1, terminate/2, code_change/3, handle_call/2, handle_event/2, 
	handle_info/2, get_errors/0]).

init([]) -> {ok, []}.

terminate(_Arg, _State) ->
    terminated_ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_errors() ->
    Events = get_events(),
    Errors = [string:strip(lists:flatten(
                    io_lib:format(Format, Data)), both, $\n)
                || {Kind, Format, Data} <- Events,
                       Kind == error],
    {ok, Errors}.

%% returns list of {Kind, Format, Data}
get_events() ->
    {ok, Events} = gen_event:call(error_logger, ?MODULE, get_events),
    Events.
    

handle_call(get_events, State) ->
    {ok, {ok, State}, State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.


handle_event({Kind, _Gleader, {_Pid, Format, Data}}, State) ->
    Msg = {Kind, Format, Data},
    {ok, [Msg|State]};

handle_event(_Event, State) ->
    {ok, State}.


handle_info(_Info, State) ->
    {ok, State}.

