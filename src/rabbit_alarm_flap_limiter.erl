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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_alarm_flap_limiter).

-export([init/0, set/1, clear/1]).

-define(MAX_INTENSITY,  2). %% 2 set->clear transitions
-define(MAX_PERIOD,    10). %% allowed within 10 seconds

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(state() :: any()).
-spec(init/0 :: () -> state()).
-spec(set/1 :: (state()) -> {boolean(), state()}).
-spec(clear/1 :: (state()) -> {boolean(), state()}).

-endif.

%%----------------------------------------------------------------------------

init() ->
    {false, []}.

%% already flapping too much, locked up
set(State = {true, _Restarts}) -> {false, State};
set(State)                     -> {true,  State}.

clear({_Locked, Restarts}) ->
    case add_transition(Restarts) of
        {true, Restarts1}   -> {true,  {false, Restarts1}};
        {false, _Restarts1} -> {false, {true,  Restarts}}
    end.

%%----------------------------------------------------------------------------
%% The following code is lifted from supervisor.erl in Erlang/OTP
%% R13B03 and lightly edited. The following license applies:

%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 1996-2009. All Rights Reserved.
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

add_transition(Restarts) ->
    Now = erlang:now(),
    Restarts1 = add_transition([Now|Restarts], Now, ?MAX_PERIOD),
    case length(Restarts1) of
	CurI when CurI  =< ?MAX_INTENSITY -> {true, Restarts1};
	_                                 -> {false, Restarts1}
    end.

add_transition([R|Restarts], Now, Period) ->
    case inPeriod(R, Now, Period) of
	true ->
	    [R|add_transition(Restarts, Now, Period)];
	_ ->
	    []
    end;
add_transition([], _, _) ->
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
