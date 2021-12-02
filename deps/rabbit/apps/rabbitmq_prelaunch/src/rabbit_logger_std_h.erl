%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2017-2020. All Rights Reserved.
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
-module(rabbit_logger_std_h).

-ifdef(TEST).
-define(io_put_chars(DEVICE, DATA), begin
                                        %% We log to Common Test log as well.
                                        %% This is the file we use to check
                                        %% the message made it to
                                        %% stdout/stderr.
                                        ct:log("~ts", [DATA]),
                                        io:put_chars(DEVICE, DATA)
                                    end).

-export([parse_date_spec/1, parse_day_of_week/2, parse_day_of_month/2, parse_hour/2, parse_minute/2]).
-else.
-define(io_put_chars(DEVICE, DATA), io:put_chars(DEVICE, DATA)).
-endif.
-define(file_write(DEVICE, DATA), file:write(DEVICE, DATA)).
-define(file_datasync(DEVICE), file:datasync(DEVICE)).

-include_lib("kernel/include/file.hrl").

%% API
-export([filesync/1]).
-export([is_date_based_rotation_needed/3]).

%% logger_h_common callbacks
-export([init/2, check_config/4, config_changed/3, reset_state/2,
         filesync/3, write/4, handle_info/3, terminate/3]).

%% logger callbacks
-export([log/2, adding_handler/1, removing_handler/1, changing_config/3,
         filter_config/1]).

-define(DEFAULT_CALL_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

%%%-----------------------------------------------------------------
%%%
-spec filesync(Name) -> ok | {error,Reason} when
      Name :: atom(),
      Reason :: handler_busy | {badarg,term()}.

filesync(Name) ->
    logger_h_common:filesync(?MODULE,Name).

%%%===================================================================
%%% logger callbacks - just forward to logger_h_common
%%%===================================================================

%%%-----------------------------------------------------------------
%%% Handler being added
-spec adding_handler(Config) -> {ok,Config} | {error,Reason} when
      Config :: logger:handler_config(),
      Reason :: term().

adding_handler(Config) ->
    logger_h_common:adding_handler(Config).

%%%-----------------------------------------------------------------
%%% Updating handler config
-spec changing_config(SetOrUpdate, OldConfig, NewConfig) ->
                              {ok,Config} | {error,Reason} when
      SetOrUpdate :: set | update,
      OldConfig :: logger:handler_config(),
      NewConfig :: logger:handler_config(),
      Config :: logger:handler_config(),
      Reason :: term().

changing_config(SetOrUpdate, OldConfig, NewConfig) ->
    logger_h_common:changing_config(SetOrUpdate, OldConfig, NewConfig).

%%%-----------------------------------------------------------------
%%% Handler being removed
-spec removing_handler(Config) -> ok when
      Config :: logger:handler_config().

removing_handler(Config) ->
    logger_h_common:removing_handler(Config).

%%%-----------------------------------------------------------------
%%% Log a string or report
-spec log(LogEvent, Config) -> ok when
      LogEvent :: logger:log_event(),
      Config :: logger:handler_config().

log(LogEvent, Config) ->
    logger_h_common:log(LogEvent, Config).

%%%-----------------------------------------------------------------
%%% Remove internal fields from configuration
-spec filter_config(Config) -> Config when
      Config :: logger:handler_config().

filter_config(Config) ->
    logger_h_common:filter_config(Config).

%%%===================================================================
%%% logger_h_common callbacks
%%%===================================================================
init(Name, Config) ->
    MyConfig = maps:with([type,file,modes,file_check,max_no_bytes,
                          rotate_on_date,max_no_files,compress_on_rotate],
                         Config),
    case file_ctrl_start(Name, MyConfig) of
        {ok,FileCtrlPid} ->
            {ok,MyConfig#{file_ctrl_pid=>FileCtrlPid}};
        Error ->
            Error
    end.

check_config(Name,set,undefined,NewHConfig) ->
    check_h_config(merge_default_config(Name,normalize_config(NewHConfig)));
check_config(Name,SetOrUpdate,OldHConfig,NewHConfig0) ->
    WriteOnce = maps:with([type,file,modes],OldHConfig),
    Default =
        case SetOrUpdate of
            set ->
                %% Do not reset write-once fields to defaults
                merge_default_config(Name,WriteOnce);
            update ->
                OldHConfig
        end,

    NewHConfig = maps:merge(Default, normalize_config(NewHConfig0)),

    %% Fail if write-once fields are changed
    case maps:with([type,file,modes],NewHConfig) of
        WriteOnce ->
            check_h_config(NewHConfig);
        Other ->
            {error,{illegal_config_change,?MODULE,WriteOnce,Other}}
    end.

check_h_config(HConfig) ->
    case check_h_config(maps:get(type,HConfig),maps:to_list(HConfig)) of
        ok ->
            {ok,fix_file_opts(HConfig)};
        {error,{Key,Value}} ->
            {error,{invalid_config,?MODULE,#{Key=>Value}}}
    end.

check_h_config(Type,[{type,Type} | Config]) when Type =:= standard_io;
                                                 Type =:= standard_error;
                                                 Type =:= file ->
    check_h_config(Type,Config);
check_h_config({device,Device},[{type,{device,Device}} | Config]) ->
    check_h_config({device,Device},Config);
check_h_config(file,[{file,File} | Config]) when is_list(File) ->
    check_h_config(file,Config);
check_h_config(file,[{modes,Modes} | Config]) when is_list(Modes) ->
    check_h_config(file,Config);
check_h_config(file,[{max_no_bytes,Size} | Config])
  when (is_integer(Size) andalso Size>0) orelse Size=:=infinity ->
    check_h_config(file,Config);
check_h_config(file,[{rotate_on_date,DateSpec}=Param | Config])
  when is_list(DateSpec) orelse DateSpec=:=false ->
    case parse_date_spec(DateSpec) of
        error -> {error,Param};
        _ -> check_h_config(file,Config)
    end;
check_h_config(file,[{max_no_files,Num} | Config]) when is_integer(Num), Num>=0 ->
    check_h_config(file,Config);
check_h_config(file,[{compress_on_rotate,Bool} | Config]) when is_boolean(Bool) ->
    check_h_config(file,Config);
check_h_config(file,[{file_check,FileCheck} | Config])
  when is_integer(FileCheck), FileCheck>=0 ->
    check_h_config(file,Config);
check_h_config(_Type,[Other | _]) ->
    {error,Other};
check_h_config(_Type,[]) ->
    ok.

normalize_config(#{type:={file,File}}=HConfig) ->
    normalize_config(HConfig#{type=>file,file=>File});
normalize_config(#{type:={file,File,Modes}}=HConfig) ->
    normalize_config(HConfig#{type=>file,file=>File,modes=>Modes});
normalize_config(#{file:=File}=HConfig) ->
    HConfig#{file=>filename:absname(File)};
normalize_config(HConfig) ->
    HConfig.

merge_default_config(Name,#{type:=Type}=HConfig) ->
    merge_default_config(Name,Type,HConfig);
merge_default_config(Name,#{file:=_}=HConfig) ->
    merge_default_config(Name,file,HConfig);
merge_default_config(Name,HConfig) ->
    merge_default_config(Name,standard_io,HConfig).

merge_default_config(Name,Type,HConfig) ->
    maps:merge(get_default_config(Name,Type),HConfig).

get_default_config(Name,file) ->
     #{type => file,
       file => filename:absname(atom_to_list(Name)),
       modes => [raw,append],
       file_check => 0,
       max_no_bytes => infinity,
       rotate_on_date => false,
       max_no_files => 0,
       compress_on_rotate => false};
get_default_config(_Name,Type) ->
     #{type => Type}.

fix_file_opts(#{modes:=Modes}=HConfig) ->
    HConfig#{modes=>fix_modes(Modes)};
fix_file_opts(HConfig) ->
    HConfig#{filesync_repeat_interval=>no_repeat}.

fix_modes(Modes) ->
    %% Ensure write|append|exclusive
    Modes1 =
        case [M || M <- Modes,
                   lists:member(M,[write,append,exclusive])] of
            [] -> [append|Modes];
            _ -> Modes
        end,
    %% Ensure raw
    Modes2 =
        case lists:member(raw,Modes) of
            false -> [raw|Modes1];
            true -> Modes1
        end,
    %% Ensure delayed_write
    case lists:partition(fun(delayed_write) -> true;
                            ({delayed_write,_,_}) -> true;
                            (_) -> false
                         end, Modes2) of
        {[],_} ->
            [delayed_write|Modes2];
        _ ->
            Modes2
    end.

config_changed(_Name,
               #{file_check:=FileCheck,
                 max_no_bytes:=Size,
                 rotate_on_date:=DateSpec,
                 max_no_files:=Count,
                 compress_on_rotate:=Compress},
               #{file_check:=FileCheck,
                 max_no_bytes:=Size,
                 rotate_on_date:=DateSpec,
                 max_no_files:=Count,
                 compress_on_rotate:=Compress}=State) ->
    State;
config_changed(_Name,
               #{file_check:=FileCheck,
                 max_no_bytes:=Size,
                 rotate_on_date:=DateSpec,
                 max_no_files:=Count,
                 compress_on_rotate:=Compress},
               #{file_ctrl_pid := FileCtrlPid} = State) ->
    FileCtrlPid ! {update_config,#{file_check=>FileCheck,
                                   max_no_bytes=>Size,
                                   rotate_on_date=>DateSpec,
                                   max_no_files=>Count,
                                   compress_on_rotate=>Compress}},
    State#{file_check:=FileCheck,
           max_no_bytes:=Size,
           rotate_on_date:=DateSpec,
           max_no_files:=Count,
           compress_on_rotate:=Compress};
config_changed(_Name,_NewHConfig,State) ->
    State.

filesync(_Name, SyncAsync, #{file_ctrl_pid := FileCtrlPid} = State) ->
    Result = file_ctrl_filesync(SyncAsync, FileCtrlPid),
    {Result,State}.

write(_Name, SyncAsync, Bin, #{file_ctrl_pid:=FileCtrlPid} = State) ->
    Result = file_write(SyncAsync, FileCtrlPid, Bin),
    {Result,State}.

reset_state(_Name, State) ->
    State.

handle_info(_Name, {'EXIT',Pid,Why}, #{file_ctrl_pid := Pid}=State) ->
    %% file_ctrl_pid died, file error, terminate handler
    exit({error,{write_failed,maps:with([type,file,modes],State),Why}});
handle_info(_, _, State) ->
    State.

terminate(_Name, _Reason, #{file_ctrl_pid:=FWPid}) ->
    case is_process_alive(FWPid) of
        true ->
            unlink(FWPid),
            _ = file_ctrl_stop(FWPid),
            MRef = erlang:monitor(process, FWPid),
            receive
                {'DOWN',MRef,_,_,_} ->
                    ok
            after
                ?DEFAULT_CALL_TIMEOUT ->
                    exit(FWPid, kill),
                    ok
            end;
        false ->
            ok
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%%-----------------------------------------------------------------
%%%
open_log_file(HandlerName,#{type:=file,
                            file:=FileName,
                            modes:=Modes,
                            file_check:=FileCheck}) ->
    try
        case filelib:ensure_dir(FileName) of
            ok ->
                case file:open(FileName, Modes) of
                    {ok, Fd} ->
                        {ok,#file_info{inode=INode}} =
                            file:read_file_info(FileName,[raw]),
                        UpdateModes = [append | Modes--[write,append,exclusive]],
                        {ok,#{handler_name=>HandlerName,
                              file_name=>FileName,
                              modes=>UpdateModes,
                              file_check=>FileCheck,
                              fd=>Fd,
                              inode=>INode,
                              last_check=>timestamp(),
                              synced=>false,
                              write_res=>ok,
                              sync_res=>ok}};
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        _:Reason -> {error,Reason}
    end.

close_log_file(#{fd:=Fd}) ->
    _ = file:datasync(Fd), %% file:datasync may return error as it will flush the delayed_write buffer
    _ = file:close(Fd),
    ok;
close_log_file(_) ->
    ok.

%% A special close that closes the FD properly when the delayed write close failed
delayed_write_close(#{fd:=Fd}) ->
    case file:close(Fd) of
        %% We got an error while closing, could be a delayed write failing
        %% So we close again in order to make sure the file is closed.
        {error, _} ->
            file:close(Fd);
        Res ->
            Res
    end.

%%%-----------------------------------------------------------------
%%% File control process

file_ctrl_start(HandlerName, HConfig) ->
    Starter = self(),
    FileCtrlPid =
        spawn_link(fun() ->
                           file_ctrl_init(HandlerName, HConfig, Starter)
                   end),
    receive
        {FileCtrlPid,ok} ->
            {ok,FileCtrlPid};
        {FileCtrlPid,Error} ->
            Error
    after
        ?DEFAULT_CALL_TIMEOUT ->
            {error,file_ctrl_process_not_started}
    end.

file_ctrl_stop(Pid) ->
    Pid ! stop.

file_write(async, Pid, Bin) ->
    Pid ! {log,Bin},
    ok;
file_write(sync, Pid, Bin) ->
    file_ctrl_call(Pid, {log,Bin}).

file_ctrl_filesync(async, Pid) ->
    Pid ! filesync,
    ok;
file_ctrl_filesync(sync, Pid) ->
    file_ctrl_call(Pid, filesync).

file_ctrl_call(Pid, Msg) ->
    MRef = monitor(process, Pid),
    Pid ! {Msg,{self(),MRef}},
    receive
        {MRef,Result} ->
            demonitor(MRef, [flush]),
            Result;
        {'DOWN',MRef,_Type,_Object,Reason} ->
            {error,Reason}
    after
        ?DEFAULT_CALL_TIMEOUT ->
            %% If this timeout triggers we will get a stray
            %% reply message in our mailbox eventually.
            %% That does not really matter though as it will
            %% end up in this module's handle_info and be ignored
            demonitor(MRef, [flush]),
            {error,{no_response,Pid}}
    end.

file_ctrl_init(HandlerName,
               #{type:=file,
                 max_no_bytes:=Size,
                 rotate_on_date:=DateSpec,
                 max_no_files:=Count,
                 compress_on_rotate:=Compress,
                 file:=FileName} = HConfig,
               Starter) ->
    process_flag(message_queue_data, off_heap),
    case open_log_file(HandlerName,HConfig) of
        {ok,State} ->
            Starter ! {self(),ok},
            %% Do the initial rotate (if any) after we ack the starting
            %% process as otherwise startup of the system will be
            %% delayed/crash
            case parse_date_spec(DateSpec) of
                error ->
                    Starter ! {self(),{error,{invalid_date_spec,DateSpec}}};
                ParsedDS ->
                    RotState = update_rotation({Size,ParsedDS,Count,Compress},State),
                    file_ctrl_loop(RotState)
            end;
        {error,Reason} ->
            Starter ! {self(),{error,{open_failed,FileName,Reason}}}
    end;
file_ctrl_init(HandlerName, #{type:={device,Dev}}, Starter) ->
    Starter ! {self(),ok},
    file_ctrl_loop(#{handler_name=>HandlerName,dev=>Dev});
file_ctrl_init(HandlerName, #{type:=StdDev}, Starter) ->
    Starter ! {self(),ok},
    file_ctrl_loop(#{handler_name=>HandlerName,dev=>StdDev}).

file_ctrl_loop(State) ->
    receive
        %% asynchronous event
        {log,Bin} ->
            State1 = write_to_dev(Bin,State),
            file_ctrl_loop(State1);

        %% synchronous event
        {{log,Bin},{From,MRef}} ->
            State1 = ensure_file(State),
            State2 = write_to_dev(Bin,State1),
            From ! {MRef,ok},
            file_ctrl_loop(State2);

        filesync ->
            State1 = sync_dev(State),
            file_ctrl_loop(State1);

        {filesync,{From,MRef}} ->
            State1 = ensure_file(State),
            State2 = sync_dev(State1),
            From ! {MRef,ok},
            file_ctrl_loop(State2);

        {update_config,#{file_check:=FileCheck,
                         max_no_bytes:=Size,
                         rotate_on_date:=DateSpec,
                         max_no_files:=Count,
                         compress_on_rotate:=Compress}} ->
            case parse_date_spec(DateSpec) of
                error ->
                    %% FIXME: Report parsing error?
                    file_ctrl_loop(State#{file_check=>FileCheck});
                ParsedDS ->
                    State1 = update_rotation({Size,ParsedDS,Count,Compress},State),
                    file_ctrl_loop(State1#{file_check=>FileCheck})
            end;

        stop ->
            close_log_file(State),
            stopped
    end.

maybe_ensure_file(#{file_check:=0}=State) ->
    ensure_file(State);
maybe_ensure_file(#{last_check:=T0,file_check:=CheckInt}=State)
  when is_integer(CheckInt) ->
    T = timestamp(),
    if T-T0 > CheckInt -> ensure_file(State);
       true -> State
    end;
maybe_ensure_file(State) ->
    State.

%% In order to play well with tools like logrotate, we need to be able
%% to re-create the file if it has disappeared (e.g. if rotated by
%% logrotate)
ensure_file(#{inode:=INode0,file_name:=FileName,modes:=Modes}=State) ->
    case file:read_file_info(FileName,[raw]) of
        {ok,#file_info{inode=INode0}} ->
            State#{last_check=>timestamp()};
        _ ->
            close_log_file(State),
            case file:open(FileName,Modes) of
                {ok,Fd} ->
                    {ok,#file_info{inode=INode}} =
                        file:read_file_info(FileName,[raw]),
                    State#{fd=>Fd,inode=>INode,
                           last_check=>timestamp(),
                           synced=>true,sync_res=>ok};
                Error ->
                    exit({could_not_reopen_file,Error})
            end
    end;
ensure_file(State) ->
    State.

write_to_dev(Bin,#{dev:=DevName}=State) ->
    ?io_put_chars(DevName, Bin),
    State;
write_to_dev(Bin, State) ->
    State1 = #{fd:=Fd} = maybe_ensure_file(State),
    Result = ?file_write(Fd, Bin),
    State2 = maybe_rotate_file(Bin,State1),
    maybe_notify_error(write,Result,State2),
    State2#{synced=>false,write_res=>Result}.

sync_dev(#{synced:=false}=State) ->
    State1 = #{fd:=Fd} = maybe_ensure_file(State),
    Result = ?file_datasync(Fd),
    maybe_notify_error(filesync,Result,State1),
    State1#{synced=>true,sync_res=>Result};
sync_dev(State) ->
    State.

update_rotation({infinity,false,_,_},State) ->
    maybe_remove_archives(0,State),
    maps:remove(rotation,State);
update_rotation({Size,DateSpec,Count,Compress},#{file_name:=FileName}=State) ->
    maybe_remove_archives(Count,State),
    {ok,#file_info{size=CurrSize}} = file:read_file_info(FileName,[raw]),
    State1 = State#{rotation=>#{size=>Size,
                                on_date=>DateSpec,
                                count=>Count,
                                compress=>Compress,
                                curr_size=>CurrSize}},
    maybe_update_compress(0,State1),
    maybe_rotate_file(0,State1).

%%
%% Date spec parser
%%

%% Some examples from Lager docs:
%%
%% $D0     rotate every night at midnight
%% $D23    rotate every day at 23:00 hr
%% $W0D23  rotate every week on Sunday at 23:00 hr
%% $W5D16  rotate every week on Friday at 16:00 hr
%% $M1D0   rotate on the first day of every month at
%%         midnight (i.e., the start of the day)
%% $M5D6   rotate on every 5th day of the month at
%%         6:00 hr

parse_date_spec(false) ->
    false;
parse_date_spec("") ->
    false;
parse_date_spec(Input) ->
    parse_date_spec(Input, #{}).

parse_date_spec("", Acc) ->
    Acc;
%% $D23
parse_date_spec([$$, $D, D1, D2 | Rest], Acc0) when D1 >= $0, D1 =< $9, D2 >= $0, D2 =< $9 ->
    Acc = parse_hour([D1, D2], Acc0#{every => day, hour => 0}),
    parse_date_spec(Rest, Acc);
%% D23
parse_date_spec([$D, D1, D2 | Rest], Acc0) when D1 >= $0, D1 =< $9, D2 >= $0, D2 =< $9 ->
    Acc = parse_hour([D1, D2], Acc0#{hour => 0}),
    parse_date_spec(Rest, Acc);
%% $D0
parse_date_spec([$$, $D, D1 | Rest], Acc0) when D1 >= $0, D1 =< $9 ->
    Acc = parse_hour([D1], Acc0#{every => day, hour => 0}),
    parse_date_spec(Rest, Acc);
%% D0
parse_date_spec([$D, D1 | Rest], Acc0) when D1 >= $0, D1 =< $9 ->
    Acc = parse_hour([D1], Acc0#{hour => 0}),
    parse_date_spec(Rest, Acc);
%% $H23
parse_date_spec([$$, $H, H1, H2 | Rest], Acc0) when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9 ->
    Acc = parse_minute([H1, H2], Acc0#{every => day, hour => 0}),
    parse_date_spec(Rest, Acc);
%% H23
parse_date_spec([$H, H1, H2 | Rest], Acc0) when H1 >= $0, H1 =< $9, H2 >= $0, H2 =< $9 ->
    Acc = parse_minute([H1, H2], Acc0#{hour => 0}),
    parse_date_spec(Rest, Acc);
%% $H0
parse_date_spec([$$, $H, H1 | Rest], Acc0) when H1 >= $0, H1 =< $9 ->
    Acc = parse_minute([H1], Acc0#{every => day, hour => 0}),
    parse_date_spec(Rest, Acc);
%% H0
parse_date_spec([$H, H1 | Rest], Acc0) when H1 >= $0, H1 =< $9 ->
    Acc = parse_minute([H1], Acc0#{hour => 0}),
    parse_date_spec(Rest, Acc);
%% $W0
parse_date_spec([$$, $W, W | Rest], Acc0) when W >= $0, W =< $6 ->
    Acc = parse_day_of_week([W], Acc0#{every => week, hour => 0}),
    parse_date_spec(Rest, Acc);
%% $M0
parse_date_spec([$$, $M, M | Rest], Acc0) when M >= $0, M =< $6 ->
    Acc = parse_day_of_month([M], Acc0#{every => month, hour => 0}),
    parse_date_spec(Rest, Acc);
%% all other inputs
parse_date_spec(Input, _Acc) ->
    io:format(standard_error, "Failed to parse rotation date spec: ~p (error)~n", [Input]),
    error.

parse_minute("", Acc) ->
    Acc;
parse_minute(Input, Acc) ->
    case string_to_int_within_range(Input, 0, 59) of
        {Val, _Rest} -> Acc#{minute => Val};
        error       ->  error
    end.

parse_hour("", Acc) ->
    Acc;
parse_hour(Input, Acc) ->
    case string_to_int_within_range(Input, 0, 23) of
        {Val, _Rest} -> Acc#{hour => Val};
        error       ->  error
    end.

parse_day_of_week("", Acc) ->
    Acc;
parse_day_of_week(Input, Acc) ->
    case string_to_int_within_range(Input, 0, 6) of
        {DayOfWeek, _Rest} -> Acc#{day_of_week => DayOfWeek};
        error              -> error
    end.

parse_day_of_month("", Acc) ->
    Acc;
parse_day_of_month([Last | _Rest], Acc)
  when Last=:=$l orelse Last=:=$L ->
    Acc#{day_of_month => last};
parse_day_of_month(Input, Acc) ->
    case string_to_int_within_range(Input, 1, 31) of
        {DayOfMonth, _Rest} -> Acc#{day_of_month => DayOfMonth};
        error               -> error
    end.

string_to_int_within_range(String, Min, Max) ->
    case string:to_integer(String) of
        {Int, Rest} when is_integer(Int) andalso Int >= Min andalso Int =< Max ->
            {Int, Rest};
        _ ->
            error
    end.

%%
%% End of Date spec parser
%%

maybe_remove_archives(Count,#{file_name:=FileName}=State) ->
    Archive = rot_file_name(FileName,Count,false),
    CompressedArchive = rot_file_name(FileName,Count,true),
    case {file:read_file_info(Archive,[raw]),
          file:read_file_info(CompressedArchive,[raw])} of
        {{error,enoent},{error,enoent}} ->
            ok;
        _ ->
            _ = file:delete(Archive),
            _ = file:delete(CompressedArchive),
            maybe_remove_archives(Count+1,State)
    end.

maybe_update_compress(Count,#{rotation:=#{count:=Count}}) ->
    ok;
maybe_update_compress(N,#{file_name:=FileName,
                          rotation:=#{compress:=Compress}}=State) ->
    Archive = rot_file_name(FileName,N,not Compress),
    case file:read_file_info(Archive,[raw]) of
        {ok,_} when Compress ->
            compress_file(Archive);
        {ok,_} ->
            decompress_file(Archive);
        _ ->
            ok
    end,
    maybe_update_compress(N+1,State).

maybe_rotate_file(Bin,#{rotation:=_}=State) when is_binary(Bin) ->
    maybe_rotate_file(byte_size(Bin),State);
maybe_rotate_file(AddSize,#{rotation:=#{size:=RotSize,
                                        curr_size:=CurrSize}=Rotation}=State) ->
    {DateBasedRotNeeded, Rotation1} = is_date_based_rotation_needed(Rotation),
    NewSize = CurrSize + AddSize,
    if NewSize>RotSize ->
            rotate_file(State#{rotation=>Rotation1#{curr_size=>NewSize}});
       DateBasedRotNeeded ->
            rotate_file(State#{rotation=>Rotation1#{curr_size=>NewSize}});
       true ->
            State#{rotation=>Rotation1#{curr_size=>NewSize}}
    end;
maybe_rotate_file(_Bin,State) ->
    State.

is_date_based_rotation_needed(#{last_rotation_ts:=PrevTimestamp,
                                on_date:=DateSpec}=Rotation) ->
    CurrTimestamp = rotation_timestamp(),
    case is_date_based_rotation_needed(DateSpec,PrevTimestamp,CurrTimestamp) of
        true -> {true,Rotation#{last_rotation_ts=>CurrTimestamp}};
        false -> {false,Rotation}
    end;
is_date_based_rotation_needed(Rotation) ->
    {false,Rotation#{last_rotation_ts=>rotation_timestamp()}}.

is_date_based_rotation_needed(#{every:=day,hour:=Hour},
                              {Date1,Time1},{Date2,Time2})
  when (Date1<Date2 orelse (Date1=:=Date2 andalso Time1<{Hour,0,0})) andalso
       Time2>={Hour,0,0} ->
    true;
is_date_based_rotation_needed(#{every:=day,hour:=Hour},
                              {Date1,_}=DateTime1,{Date2,Time2}=DateTime2)
  when Date1<Date2 andalso
       Time2<{Hour,0,0} ->
    GregDays2 = calendar:date_to_gregorian_days(Date2),
    TargetDate = calendar:gregorian_days_to_date(GregDays2 - 1),
    TargetDateTime = {TargetDate,{Hour,0,0}},
    DateTime1<TargetDateTime andalso DateTime2>=TargetDateTime;
is_date_based_rotation_needed(#{every:=week,day_of_week:=TargetDoW,hour:=Hour},
                              DateTime1,{Date2,_}=DateTime2) ->
    DoW2 = calendar:day_of_the_week(Date2) rem 7,
    DaysSinceTargetDoW = ((DoW2 - TargetDoW) + 7) rem 7,
    GregDays2 = calendar:date_to_gregorian_days(Date2),
    TargetGregDays = GregDays2 - DaysSinceTargetDoW,
    TargetDate = calendar:gregorian_days_to_date(TargetGregDays),
    TargetDateTime = {TargetDate,{Hour,0,0}},
    DateTime1<TargetDateTime andalso DateTime2>=TargetDateTime;
is_date_based_rotation_needed(#{every:=month,day_of_month:=last,hour:=Hour},
                              DateTime1,{{Year2,Month2,_}=Date2,_}=DateTime2) ->
    DoMA = calendar:last_day_of_the_month(Year2, Month2),
    DateA = {Year2,Month2,DoMA},
    TargetDate = if
                     DateA>Date2 ->
                         case Month2 - 1 of
                             0 ->
                                 {Year2-1,12,31};
                             MonthB ->
                                 {Year2,MonthB,
                                  calendar:last_day_of_the_month(Year2,MonthB)}
                         end;
                     true ->
                         DateA
                 end,
    TargetDateTime = {TargetDate,{Hour,0,0}},
    DateTime1<TargetDateTime andalso DateTime2>=TargetDateTime;
is_date_based_rotation_needed(#{every:=month,day_of_month:=DoM,hour:=Hour},
                              DateTime1,{{Year2,Month2,_}=Date2,_}=DateTime2) ->
    DateA = {Year2,Month2,adapt_day_of_month(Year2,Month2,DoM)},
    TargetDate = if
                     DateA>Date2 ->
                         case Month2 - 1 of
                             0 ->
                                 {Year2-1,12,31};
                             MonthB ->
                                 {Year2,MonthB,
                                  adapt_day_of_month(Year2,MonthB,DoM)}
                         end;
                     true ->
                         DateA
                 end,
    TargetDateTime = {TargetDate,{Hour,0,0}},
    DateTime1<TargetDateTime andalso DateTime2>=TargetDateTime;
is_date_based_rotation_needed(_,_,_) ->
    false.

adapt_day_of_month(Year,Month,Day) ->
    LastDay = calendar:last_day_of_the_month(Year,Month),
    erlang:min(Day,LastDay).

rotate_file(#{file_name:=FileName,modes:=Modes,rotation:=Rotation}=State) ->
    State1 = sync_dev(State),
    _ = delayed_write_close(State),
    rotate_files(FileName,maps:get(count,Rotation),maps:get(compress,Rotation)),
    case file:open(FileName,Modes) of
        {ok,Fd} ->
            {ok,#file_info{inode=INode}} = file:read_file_info(FileName,[raw]),
            CurrTimestamp = rotation_timestamp(),
            State1#{fd=>Fd,inode=>INode,
                    rotation=>Rotation#{curr_size=>0,
                                        last_rotation_ts=>CurrTimestamp}};
        Error ->
            exit({could_not_reopen_file,Error})
    end.

rotation_timestamp() ->
    calendar:now_to_local_time(erlang:timestamp()).

rotate_files(FileName,0,_Compress) ->
    _ = file:delete(FileName),
    ok;
rotate_files(FileName,1,Compress) ->
    FileName0 = FileName++".0",
    _ = file:rename(FileName,FileName0),
    if Compress -> compress_file(FileName0);
       true -> ok
    end,
    ok;
rotate_files(FileName,Count,Compress) ->
    _ = file:rename(rot_file_name(FileName,Count-2,Compress),
                    rot_file_name(FileName,Count-1,Compress)),
    rotate_files(FileName,Count-1,Compress).

rot_file_name(FileName,Count,false) ->
    FileName ++ "." ++ integer_to_list(Count);
rot_file_name(FileName,Count,true) ->
    rot_file_name(FileName,Count,false) ++ ".gz".

compress_file(FileName) ->
    {ok,In} = file:open(FileName,[read,binary]),
    {ok,Out} = file:open(FileName++".gz",[write]),
    Z = zlib:open(),
    zlib:deflateInit(Z, default, deflated, 31, 8, default),
    compress_data(Z,In,Out),
    zlib:deflateEnd(Z),
    zlib:close(Z),
    _ = file:close(In),
    _ = file:close(Out),
    _ = file:delete(FileName),
    ok.

compress_data(Z,In,Out) ->
    case file:read(In,100000) of
        {ok,Data} ->
            Compressed = zlib:deflate(Z, Data),
            _ = file:write(Out,Compressed),
            compress_data(Z,In,Out);
        eof ->
            Compressed = zlib:deflate(Z, <<>>, finish),
            _ = file:write(Out,Compressed),
            ok
    end.

decompress_file(FileName) ->
    {ok,In} = file:open(FileName,[read,binary]),
    {ok,Out} = file:open(filename:rootname(FileName,".gz"),[write]),
    Z = zlib:open(),
    zlib:inflateInit(Z, 31),
    decompress_data(Z,In,Out),
    zlib:inflateEnd(Z),
    zlib:close(Z),
    _ = file:close(In),
    _ = file:close(Out),
    _ = file:delete(FileName),
    ok.

decompress_data(Z,In,Out) ->
    case file:read(In,1000) of
        {ok,Data} ->
            Decompressed = zlib:inflate(Z, Data),
            _ = file:write(Out,Decompressed),
            decompress_data(Z,In,Out);
        eof ->
            ok
    end.

maybe_notify_error(_Op, ok, _State) ->
    ok;
maybe_notify_error(Op, Result, #{write_res:=WR,sync_res:=SR})
  when (Op==write andalso Result==WR) orelse
       (Op==filesync andalso Result==SR) ->
    %% don't report same error twice
    ok;
maybe_notify_error(Op, Error, #{handler_name:=HandlerName,file_name:=FileName}) ->
    logger_h_common:error_notify({HandlerName,Op,FileName,Error}),
    ok.

timestamp() ->
    erlang:monotonic_time(millisecond).
