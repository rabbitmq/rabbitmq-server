%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 2014-2015. All Rights Reserved.
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

%%
%% If your code need to be able to execute on ERTS versions both
%% earlier and later than 7.0, the best approach is to use the new
%% time API introduced in ERTS 7.0 and implement a fallback
%% solution using the old primitives to be used on old ERTS
%% versions. This way your code can automatically take advantage
%% of the improvements in the API when available. This is an
%% example of how to implement such an API, but it can be used
%% as is if you want to. Just add (a preferrably renamed version of)
%% this module to your project, and call the API via this module
%% instead of calling the BIFs directly.
%%

-module(time_compat).

%% We don't want warnings about the use of erlang:now/0 in
%% this module.
-compile(nowarn_deprecated_function).
%%
%% We don't use
%%   -compile({nowarn_deprecated_function, [{erlang, now, 0}]}).
%% since this will produce warnings when compiled on systems
%% where it has not yet been deprecated.
%%

-export([monotonic_time/0,
	 monotonic_time/1,
	 erlang_system_time/0,
	 erlang_system_time/1,
	 os_system_time/0,
	 os_system_time/1,
	 time_offset/0,
	 time_offset/1,
	 convert_time_unit/3,
	 timestamp/0,
	 unique_integer/0,
	 unique_integer/1,
	 monitor/2,
	 system_info/1,
	 system_flag/2]).

monotonic_time() ->
    try
	erlang:monotonic_time()
    catch
	error:undef ->
	    %% Use Erlang system time as monotonic time
	    erlang_system_time_fallback()
    end.

monotonic_time(Unit) ->
    try
	erlang:monotonic_time(Unit)
    catch
	error:badarg ->
	    erlang:error(badarg, [Unit]);
	error:undef ->
	    %% Use Erlang system time as monotonic time
	    STime = erlang_system_time_fallback(),
	    try
		convert_time_unit_fallback(STime, native, Unit)
	    catch
		error:bad_time_unit -> erlang:error(badarg, [Unit])
	    end
    end.

erlang_system_time() ->
    try
	erlang:system_time()
    catch
	error:undef ->
	    erlang_system_time_fallback()
    end.

erlang_system_time(Unit) ->
    try
	erlang:system_time(Unit)
    catch
	error:badarg ->
	    erlang:error(badarg, [Unit]);
	error:undef ->
	    STime = erlang_system_time_fallback(),
	    try
		convert_time_unit_fallback(STime, native, Unit)
	    catch
		error:bad_time_unit -> erlang:error(badarg, [Unit])
	    end
    end.

os_system_time() ->
    try
	os:system_time()
    catch
	error:undef ->
	    os_system_time_fallback()
    end.

os_system_time(Unit) ->
    try
	os:system_time(Unit)
    catch
	error:badarg ->
	    erlang:error(badarg, [Unit]);
	error:undef ->
	    STime = os_system_time_fallback(),
	    try
		convert_time_unit_fallback(STime, native, Unit)
	    catch
		error:bad_time_unit -> erlang:error(badarg, [Unit])
	    end
    end.

time_offset() ->
    try
	erlang:time_offset()
    catch
	error:undef ->
	    %% Erlang system time and Erlang monotonic
	    %% time are always aligned
	    0
    end.

time_offset(Unit) ->
    try
	erlang:time_offset(Unit)
    catch
	error:badarg ->
	    erlang:error(badarg, [Unit]);
	error:undef ->
	    try
		_ = integer_time_unit(Unit)
	    catch
		error:bad_time_unit -> erlang:error(badarg, [Unit])
	    end,
	    %% Erlang system time and Erlang monotonic
	    %% time are always aligned
	    0
    end.

convert_time_unit(Time, FromUnit, ToUnit) ->
    try
	erlang:convert_time_unit(Time, FromUnit, ToUnit)
    catch
	error:undef ->
	    try
		convert_time_unit_fallback(Time, FromUnit, ToUnit)
	    catch
		_:_ ->
		    erlang:error(badarg, [Time, FromUnit, ToUnit])
	    end;
	error:Error ->
	    erlang:error(Error, [Time, FromUnit, ToUnit])
    end.

timestamp() ->
    try
	erlang:timestamp()
    catch
	error:undef ->
	    erlang:now()
    end.

unique_integer() ->
    try
	erlang:unique_integer()
    catch
	error:undef ->
	    {MS, S, US} = erlang:now(),
	    (MS*1000000+S)*1000000+US
    end.

unique_integer(Modifiers) ->
    try
	erlang:unique_integer(Modifiers)
    catch
	error:badarg ->
	    erlang:error(badarg, [Modifiers]);
	error:undef ->
	    case is_valid_modifier_list(Modifiers) of
		true ->
		    %% now() converted to an integer
		    %% fullfill the requirements of
		    %% all modifiers: unique, positive,
		    %% and monotonic...
		    {MS, S, US} = erlang:now(),
		    (MS*1000000+S)*1000000+US;
		false ->
		    erlang:error(badarg, [Modifiers])
	    end
    end.

monitor(Type, Item) ->
    try
	erlang:monitor(Type, Item)
    catch
	error:Error ->
	    case {Error, Type, Item} of
		{badarg, time_offset, clock_service} ->
		    %% Time offset is final and will never change.
		    %% Return a dummy reference, there will never
		    %% be any need for 'CHANGE' messages...
		    make_ref();
		_ ->
		    erlang:error(Error, [Type, Item])
	    end
    end.

system_info(Item) ->
    try
	erlang:system_info(Item)
    catch
	error:badarg ->
	    case Item of
		time_correction ->
		    case erlang:system_info(tolerant_timeofday) of
			enabled -> true;
			disabled -> false
		    end;
		time_warp_mode ->
		    no_time_warp;
		time_offset ->
		    final;
		NotSupArg when NotSupArg == os_monotonic_time_source;
			       NotSupArg == os_system_time_source;
			       NotSupArg == start_time;
			       NotSupArg == end_time ->
		    %% Cannot emulate this...
		    erlang:error(notsup, [NotSupArg]);
		_ ->
		    erlang:error(badarg, [Item])
	    end;
	error:Error ->
	    erlang:error(Error, [Item])
    end.

system_flag(Flag, Value) ->
    try
	erlang:system_flag(Flag, Value)
    catch
	error:Error ->
	    case {Error, Flag, Value} of
		{badarg, time_offset, finalize} ->
		    %% Time offset is final
		    final;
		_ ->
		    erlang:error(Error, [Flag, Value])
	    end
    end.

%%
%% Internal functions
%%

integer_time_unit(native) -> 1000*1000;
integer_time_unit(nano_seconds) -> 1000*1000*1000;
integer_time_unit(micro_seconds) -> 1000*1000;
integer_time_unit(milli_seconds) -> 1000;
integer_time_unit(seconds) -> 1;
integer_time_unit(I) when is_integer(I), I > 0 -> I;
integer_time_unit(BadRes) -> erlang:error(bad_time_unit, [BadRes]).

erlang_system_time_fallback() ->
    {MS, S, US} = erlang:now(),
    (MS*1000000+S)*1000000+US.

os_system_time_fallback() ->
    {MS, S, US} = os:timestamp(),
    (MS*1000000+S)*1000000+US.

convert_time_unit_fallback(Time, FromUnit, ToUnit) ->
    FU = integer_time_unit(FromUnit),
    TU = integer_time_unit(ToUnit),
    case Time < 0 of
	true -> TU*Time - (FU - 1);
	false -> TU*Time
    end div FU.

is_valid_modifier_list([positive|Ms]) ->
    is_valid_modifier_list(Ms);
is_valid_modifier_list([monotonic|Ms]) ->
    is_valid_modifier_list(Ms);
is_valid_modifier_list([]) ->
    true;
is_valid_modifier_list(_) ->
    false.
