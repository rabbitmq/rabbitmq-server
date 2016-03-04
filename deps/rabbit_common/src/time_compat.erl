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

%% Declare versioned functions to allow dynamic code loading,
%% depending on the Erlang version running. See 'code_version.erl' for details
-erlang_version_support(
   [{18,
     [{monotonic_time, 0, monotonic_time_pre_18, monotonic_time_post_18},
      {monotonic_time, 1, monotonic_time_pre_18, monotonic_time_post_18},
      {erlang_system_time, 0, erlang_system_time_pre_18, erlang_system_time_post_18},
      {erlang_system_time, 1, erlang_system_time_pre_18, erlang_system_time_post_18},
      {os_system_time, 0, os_system_time_pre_18, os_system_time_post_18},
      {os_system_time, 1, os_system_time_pre_18, os_system_time_post_18},
      {time_offset, 0, time_offset_pre_18, time_offset_post_18},
      {time_offset, 1, time_offset_pre_18, time_offset_post_18},
      {convert_time_unit, 3, convert_time_unit_pre_18, convert_time_unit_post_18},
      {timestamp, 0, timestamp_pre_18, timestamp_post_18},
      {unique_integer, 0, unique_integer_pre_18, unique_integer_post_18},
      {unique_integer, 1, unique_integer_pre_18, unique_integer_post_18}]}
   ]).

-export([monotonic_time/0,
         monotonic_time_pre_18/0,
         monotonic_time_post_18/0,
         monotonic_time/1,
         monotonic_time_pre_18/1,
         monotonic_time_post_18/1,
         erlang_system_time/0,
         erlang_system_time_pre_18/0,
         erlang_system_time_post_18/0,
         erlang_system_time/1,
         erlang_system_time_pre_18/1,
         erlang_system_time_post_18/1,
         os_system_time/0,
         os_system_time_pre_18/0,
         os_system_time_post_18/0,
         os_system_time/1,
         os_system_time_pre_18/1,
         os_system_time_post_18/1,
         time_offset/0,
         time_offset_pre_18/0,
         time_offset_post_18/0,
         time_offset/1,
         time_offset_pre_18/1,
         time_offset_post_18/1,
         convert_time_unit/3,
         convert_time_unit_pre_18/3,
         convert_time_unit_post_18/3,
         timestamp/0,
         timestamp_pre_18/0,
         timestamp_post_18/0,
         unique_integer/0,
         unique_integer_pre_18/0,
         unique_integer_post_18/0,
         unique_integer/1,
         unique_integer_pre_18/1,
         unique_integer_post_18/1,
         monitor/2,
         system_info/1,
         system_flag/2]).

monotonic_time() ->
    code_version:update(?MODULE),
    time_compat:monotonic_time().

monotonic_time_post_18() ->
	erlang:monotonic_time().

monotonic_time_pre_18() ->
    erlang_system_time_fallback().

monotonic_time(Unit) ->
    code_version:update(?MODULE),
    time_compat:monotonic_time(Unit).

monotonic_time_post_18(Unit) ->
    erlang:monotonic_time(Unit).

monotonic_time_pre_18(Unit) ->
    %% Use Erlang system time as monotonic time
    STime = erlang_system_time_fallback(),
    convert_time_unit_fallback(STime, native, Unit).

erlang_system_time() ->
    code_version:update(?MODULE),
    time_compat:erlang_system_time().

erlang_system_time_post_18() ->
	erlang:system_time().

erlang_system_time_pre_18() ->
    erlang_system_time_fallback().

erlang_system_time(Unit) ->
    code_version:update(?MODULE),
    time_compat:erlang_system_time(Unit).

erlang_system_time_post_18(Unit) ->
    erlang:system_time(Unit).

erlang_system_time_pre_18(Unit) ->
    STime = erlang_system_time_fallback(),
    convert_time_unit_fallback(STime, native, Unit).

os_system_time() ->
    code_version:update(?MODULE),
    time_compat:os_system_time().

os_system_time_post_18() ->
	os:system_time().

os_system_time_pre_18() ->
    os_system_time_fallback().

os_system_time(Unit) ->
    code_version:update(?MODULE),
    time_compat:os_system_time(Unit).

os_system_time_post_18(Unit) ->
    os:system_time(Unit).

os_system_time_pre_18(Unit) ->
    STime = os_system_time_fallback(),
    convert_time_unit_fallback(STime, native, Unit).

time_offset() ->
    code_version:update(?MODULE),
    time_compat:time_offset().

time_offset_post_18() ->
	erlang:time_offset().

time_offset_pre_18() ->
    %% Erlang system time and Erlang monotonic
    %% time are always aligned
    0.

time_offset(Unit) ->
    code_version:update(?MODULE),
    time_compat:time_offset(Unit).

time_offset_post_18(Unit) ->
    erlang:time_offset(Unit).

time_offset_pre_18(Unit) ->
    _ = integer_time_unit(Unit),
    %% Erlang system time and Erlang monotonic
    %% time are always aligned
    0.

convert_time_unit(Time, FromUnit, ToUnit) ->
    code_version:update(?MODULE),
    time_compat:convert_time_unit(Time, FromUnit, ToUnit).

convert_time_unit_post_18(Time, FromUnit, ToUnit) ->
    try
        erlang:convert_time_unit(Time, FromUnit, ToUnit)
    catch
        error:Error ->
	    erlang:error(Error, [Time, FromUnit, ToUnit])
    end.

convert_time_unit_pre_18(Time, FromUnit, ToUnit) ->
    try
        convert_time_unit_fallback(Time, FromUnit, ToUnit)
    catch
		_:_ ->
		    erlang:error(badarg, [Time, FromUnit, ToUnit])
    end.

timestamp() ->
    code_version:update(?MODULE),
    time_compat:timestamp().

timestamp_post_18() ->
	erlang:timestamp().

timestamp_pre_18() ->
    erlang:now().

unique_integer() ->
    code_version:update(?MODULE),
    time_compat:unique_integer().

unique_integer_post_18() ->
	erlang:unique_integer().

unique_integer_pre_18() ->
    {MS, S, US} = erlang:now(),
    (MS*1000000+S)*1000000+US.

unique_integer(Modifiers) ->
    code_version:update(?MODULE),
    time_compat:unique_integer(Modifiers).

unique_integer_post_18(Modifiers) ->
    erlang:unique_integer(Modifiers).

unique_integer_pre_18(Modifiers) ->
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
integer_time_unit(BadRes) -> erlang:error(badarg, [BadRes]).

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
