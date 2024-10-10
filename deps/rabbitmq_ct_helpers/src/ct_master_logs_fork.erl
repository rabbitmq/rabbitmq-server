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

%%% Logging functionality for Common Test Master.
%%%
%%% This module implements a logger for the master
%%% node.
-module(ct_master_logs_fork).
-moduledoc false.

-export([start/2, make_all_runs_index/0, log/3, nodedir/2,
	 stop/0]).

-include_lib("common_test/src/ct_util.hrl").

-record(state, {log_fd, start_time, logdir, rundir, 
		nodedir_ix_fd, nodes, nodedirs=[]}).

-define(ct_master_log_name, "ct_master_log.html").
-define(all_runs_name, "master_runs.html").
-define(nodedir_index_name, "index.html").
-define(details_file_name,"details.info").
-define(table_color,"lightblue").

-define(now, os:timestamp()).

%%%--------------------------------------------------------------------
%%% API
%%%--------------------------------------------------------------------

start(LogDir,Nodes) ->
    Self = self(),
    Pid = spawn_link(fun() -> init(Self,LogDir,Nodes) end),
    MRef = erlang:monitor(process,Pid),
    receive 
	{started,Pid,Result} -> 
	    erlang:demonitor(MRef, [flush]),
	    {Pid,Result};
	{'DOWN',MRef,process,_,Reason} ->
	    exit({could_not_start_process,?MODULE,Reason})
    end.

log(Heading,Format,Args) ->
    cast({log,self(),[{int_header(),[log_timestamp(?now),Heading]},
		      {Format,Args},
		      {int_footer(),[]}]}),
    ok.

make_all_runs_index() ->
    call(make_all_runs_index).

nodedir(Node,RunDir) ->
    call({nodedir,Node,RunDir}).

stop() ->
    case whereis(?MODULE) of
	Pid when is_pid(Pid) ->
	    MRef = erlang:monitor(process,Pid),
	    ?MODULE ! stop,
	    receive
		{'DOWN',MRef,process,_,_} ->
		    ok
	    end;
	undefined ->
	    ok
    end,
    ok.

%%%--------------------------------------------------------------------
%%% Logger process
%%%--------------------------------------------------------------------

init(Parent,LogDir,Nodes) ->
    register(?MODULE,self()),
    ct_util:mark_process(),
    Time = calendar:local_time(),
    RunDir = make_dirname(Time),
    RunDirAbs = filename:join(LogDir,RunDir),
    ok = make_dir(RunDirAbs),
    _ = write_details_file(RunDirAbs,{node(),Nodes}),

    case basic_html() of
	true ->
	    put(basic_html, true);
	BasicHtml ->
	    put(basic_html, BasicHtml),
	    %% copy priv files to log dir (both top dir and test run
	    %% dir) so logs are independent of Common Test installation
	    CTPath = code:lib_dir(common_test),
	    PrivFiles = [?css_default,?jquery_script,?tablesorter_script],
	    PrivFilesSrc = [filename:join(filename:join(CTPath, "priv"), F) ||
			       F <- PrivFiles],
	    PrivFilesDestTop = [filename:join(LogDir, F) || F <- PrivFiles],
	    PrivFilesDestRun = [filename:join(RunDirAbs, F) || F <- PrivFiles],
	    case copy_priv_files(PrivFilesSrc, PrivFilesDestTop) of
		{error,Src1,Dest1,Reason1} ->
		    io:format(user, "ERROR! "++
			      "Priv file ~tp could not be copied to ~tp. "++
			      "Reason: ~tp~n",
			      [Src1,Dest1,Reason1]),
		    exit({priv_file_error,Dest1});
		ok ->
		    case copy_priv_files(PrivFilesSrc, PrivFilesDestRun) of
			{error,Src2,Dest2,Reason2} ->
			    io:format(user, "ERROR! "++
				      "Priv file ~tp could not be copied to ~tp. "++
				      "Reason: ~tp~n",
				      [Src2,Dest2,Reason2]),
			    exit({priv_file_error,Dest2});
			ok ->
			    ok
		    end
	    end
    end,

    {ok,Cwd} = file:get_cwd(),
    ok = file:set_cwd(LogDir),
    _ = make_all_runs_index(LogDir),
    CtLogFd = open_ct_master_log(RunDirAbs),
    ok = file:set_cwd(Cwd),

    NodeStr = 
	lists:flatten(lists:map(fun(N) ->
					atom_to_list(N) ++ " "
				end,Nodes)),

    io:format(CtLogFd,int_header(),[log_timestamp(?now),"Test Nodes\n"]),
    io:format(CtLogFd,"~ts\n",[NodeStr]),
    io:put_chars(CtLogFd,[int_footer(),"\n"]),

    NodeDirIxFd = open_nodedir_index(RunDirAbs,Time),
    Parent ! {started,self(),{Time,RunDirAbs}},
    loop(#state{log_fd=CtLogFd,
		start_time=Time,
		logdir=LogDir,
		rundir=RunDirAbs,
		nodedir_ix_fd=NodeDirIxFd,
		nodes=Nodes,
		nodedirs=lists:map(fun(N) ->
					   {N,""}
				   end,Nodes)}).

copy_priv_files([SrcF | SrcFs], [DestF | DestFs]) ->
    case file:copy(SrcF, DestF) of
	{error,Reason} ->
	    {error,SrcF,DestF,Reason};
	_ ->
	    copy_priv_files(SrcFs, DestFs)
    end;
copy_priv_files([], []) ->
    ok.

loop(State) ->
    receive
	{log,_From,List} ->
	    Fd = State#state.log_fd,
	    Fun = 
		fun({Str,Args}) ->
			case catch io:format(Fd,Str++"\n",Args) of
			    {'EXIT',Reason} ->
				io:format(Fd, 
					  "Logging fails! Str: ~tp, Args: ~tp~n",
					  [Str,Args]),
				exit({logging_failed,Reason}),
				ok;
			    _ ->
				ok
			end
		end,
	    lists:foreach(Fun,List),
	    loop(State);
	{make_all_runs_index,From} ->
        {ok,Cwd} = file:get_cwd(),
        ok = file:set_cwd(State#state.logdir),
	    _ = make_all_runs_index(State#state.logdir),
        ok = file:set_cwd(Cwd),
	    return(From,State#state.logdir),
	    loop(State);	
	{{nodedir,Node,RunDir},From} ->
	    print_nodedir(Node,RunDir,State#state.nodedir_ix_fd),
	    return(From,ok),
	    loop(State);	
	stop ->
        {ok,Cwd} = file:get_cwd(),
        ok = file:set_cwd(State#state.logdir),
	    _ = make_all_runs_index(State#state.logdir),
        ok = file:set_cwd(Cwd),
	    io:format(State#state.log_fd,
		      int_header()++int_footer(),
		      [log_timestamp(?now),"Finished!"]),
	    _ = close_ct_master_log(State#state.log_fd),
	    _ = close_nodedir_index(State#state.nodedir_ix_fd),
	    ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Master Log functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_ct_master_log(Dir) ->
    FullName = filename:join(Dir,?ct_master_log_name),
    {ok,Fd} = file:open(FullName,[write,{encoding,utf8}]),
    io:put_chars(Fd,header("Common Test Master Log", {[],[1,2],[]})),
    %% maybe add config info here later
    io:put_chars(Fd,config_table([])),
    io:put_chars(Fd,
		 "<style>\n"
		 "div.ct_internal { background:lightgrey; color:black }\n"
		 "div.default     { background:lightgreen; color:black }\n"
		 "</style>\n"),
    io:put_chars(Fd, 
		 xhtml("<br><h2>Progress Log</h2>\n<pre>\n",
		       "<br /><h2>Progress Log</h2>\n<pre>\n")),
    Fd.

close_ct_master_log(Fd) ->
    io:put_chars(Fd,["</pre>",footer()]),
    file:close(Fd).

config_table(Vars) ->
    [config_table_header()|config_table1(Vars)].

config_table_header() ->
    ["<h2>Configuration</h2>\n",
     xhtml(["<table border=\"3\" cellpadding=\"5\" "
	    "bgcolor=\"",?table_color,"\"\n"],
	   ["<table id=\"",?sortable_table_name,"\">\n",
	    "<thead>\n"]),
     "<tr><th>Key</th><th>Value</th></tr>\n",
     xhtml("", "</thead>\n<tbody>\n")].

config_table1([]) ->
    ["</tbody>\n</table>\n"].

int_header() ->
    "</pre>\n<div class=\"ct_internal\"><pre><b>*** CT MASTER ~s *** ~ts</b>".
int_footer() ->
    "</pre></div>\n<pre>".

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% NodeDir Index functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_nodedir_index(Dir,StartTime) ->
    FullName = filename:join(Dir,?nodedir_index_name),
    {ok,Fd} = file:open(FullName,[write,{encoding,utf8}]),
    io:put_chars(Fd,nodedir_index_header(StartTime)),
    Fd.

print_nodedir(Node,RunDir,Fd) ->
    Index = filename:join(RunDir,"index.html"),
    io:put_chars(Fd,
		 ["<tr>\n"
		  "<td align=center>",atom_to_list(Node),"</td>\n",
		  "<td align=left><a href=\"",ct_logs:uri(Index),"\">",Index,
		  "</a></td>\n",
		  "</tr>\n"]),
    ok.

close_nodedir_index(Fd) ->
    io:put_chars(Fd,index_footer()),
    file:close(Fd).

nodedir_index_header(StartTime) ->
    [header("Log Files " ++ format_time(StartTime), {[],[1,2],[]}) |
     ["<center>\n",
      "<p><a href=\"",?ct_master_log_name,"\">Common Test Master Log</a></p>",
      xhtml(["<table border=\"3\" cellpadding=\"5\" "
	     "bgcolor=\"",?table_color,"\">\n"],
	    ["<table id=\"",?sortable_table_name,"\">\n",
	     "<thead>\n<tr>\n"]),
      "<th><b>Node</b></th>\n",
      "<th><b>Log</b></th>\n",
      xhtml("", "</tr>\n</thead>\n<tbody>\n")]].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% All Run Index functions %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
make_all_runs_index(LogDir) ->
    FullName = filename:join(LogDir,?all_runs_name),
    Match = filename:join(LogDir,logdir_prefix()++"*.*"),
    Dirs = filelib:wildcard(Match),
    DirsSorted = (catch sort_all_runs(Dirs)),
    Header = all_runs_header(),
    Index = [runentry(Dir) || Dir <- DirsSorted],
    Result = file:write_file(FullName,
			     unicode:characters_to_binary(
			       Header++Index++index_footer())),
    Result.    

sort_all_runs(Dirs) ->
    %% sort on time string, always last and on the format:
    %% "YYYY-MM-DD_HH.MM.SS"
    KeyList =
	lists:map(fun(Dir) ->
			  case lists:reverse(string:lexemes(Dir,[$.,$_])) of
			      [SS,MM,HH,Date|_] ->
				  {{Date,HH,MM,SS},Dir};
			      _Other ->
				  throw(Dirs)
			  end
		  end,Dirs),
    lists:reverse(lists:map(fun({_,Dir}) ->
				    Dir
			    end,lists:keysort(1,KeyList))).

runentry(Dir) ->
    {MasterStr,NodesStr} = 
	case read_details_file(Dir) of
	    {Master,Nodes} when is_list(Nodes) ->
		[_,Host] = string:lexemes(atom_to_list(Master),"@"),
                {Host,lists:concat(lists:join(", ",Nodes))};
	    _Error ->
		{"unknown",""}
	end,
    Index = filename:join(Dir,?nodedir_index_name),
    ["<tr>\n"
     "<td align=center><a href=\"",ct_logs:uri(Index),"\">",
     timestamp(Dir),"</a></td>\n",
     "<td align=center>",MasterStr,"</td>\n",
     "<td align=center>",NodesStr,"</td>\n",
     "</tr>\n"].

all_runs_header() ->
    [header("Master Test Runs", {[1],[2,3],[]}) |
     ["<center>\n",
      xhtml(["<table border=\"3\" cellpadding=\"5\" "
	     "bgcolor=\"",?table_color,"\">\n"],
	    ["<table id=\"",?sortable_table_name,"\">\n",
	     "<thead>\n<tr>\n"]),
      "<th><b>History</b></th>\n"
      "<th><b>Master Host</b></th>\n"
      "<th><b>Test Nodes</b></th>\n",
      xhtml("", "</tr></thead>\n<tbody>\n")]].

timestamp(Dir) ->
    [S,Min,H,D,M,Y|_] = lists:reverse(string:lexemes(Dir,".-_")),
    [S1,Min1,H1,D1,M1,Y1] = [list_to_integer(N) || N <- [S,Min,H,D,M,Y]],
    format_time({{Y1,M1,D1},{H1,Min1,S1}}).

write_details_file(Dir,Details) ->
    FullName = filename:join(Dir,?details_file_name),
    force_write_file(FullName,term_to_binary(Details)).

read_details_file(Dir) ->
    FullName = filename:join(Dir,?details_file_name),
    case file:read_file(FullName) of
	{ok,Bin} ->
	    binary_to_term(Bin);
	Error ->
	    Error
    end.

%%%--------------------------------------------------------------------
%%% Internal functions
%%%--------------------------------------------------------------------

header(Title, TableCols) ->
    CSSFile = xhtml(fun() -> "" end, 
		    fun() -> make_relative(locate_priv_file(?css_default)) end),
    JQueryFile =
	xhtml(fun() -> "" end, 
	      fun() -> make_relative(locate_priv_file(?jquery_script)) end),
    TableSorterFile =
	xhtml(fun() -> "" end, 
	      fun() -> make_relative(locate_priv_file(?tablesorter_script)) end),

    [xhtml(["<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 3.2 Final//EN\">\n",
	    "<html>\n"],
	   ["<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n",
	    "\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n",
	    "<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n"]),
     "<!-- autogenerated by '"++atom_to_list(?MODULE)++"' -->\n",
     "<head>\n",
     "<title>" ++ Title ++ "</title>\n",
     "<meta http-equiv=\"cache-control\" content=\"no-cache\"></meta>\n",
     "<meta http-equiv=\"content-type\" content=\"text/html; ",
     "charset=utf-8\"></meta>\n",
     xhtml("",
	   ["<link rel=\"stylesheet\" href=\"",ct_logs:uri(CSSFile),
	    "\" type=\"text/css\"></link>\n"]),
     xhtml("",
	   ["<script type=\"text/javascript\" src=\"",JQueryFile,
	    "\"></script>\n"]),
     xhtml("",
	   ["<script type=\"text/javascript\" src=\"",TableSorterFile,
	    "\"></script>\n"]),
     xhtml(fun() -> "" end,
	   fun() -> ct_logs:insert_javascript({tablesorter,
					       ?sortable_table_name,
					       TableCols}) end),
     "</head>\n",
     body_tag(),
     "<center>\n",
     "<h1>" ++ Title ++ "</h1>\n",
     "</center>\n"].

index_footer() ->
    ["</tbody>\n</table>\n"
     "</center>\n" | footer()].

footer() ->
    ["<center>\n",
     xhtml("<br><hr>\n", "<br />\n"),
     xhtml("<p><font size=\"-1\">\n", "<div class=\"copyright\">"),
     "Copyright &copy; ", year(),
     " <a href=\"http://www.erlang.org\">Open Telecom Platform</a>",
     xhtml("<br>\n", "<br />\n"),
     "Updated: <!--date-->", current_time(), "<!--/date-->",
     xhtml("<br>\n", "<br />\n"),
     xhtml("</font></p>\n", "</div>\n"),
     "</center>\n"
     "</body>\n"].

body_tag() ->
    xhtml("<body bgcolor=\"#FFFFFF\" text=\"#000000\" link=\"#0000FF\" "
	  "vlink=\"#800080\" alink=\"#FF0000\">\n",
	  "<body>\n").

current_time() ->
    format_time(calendar:local_time()).

format_time({{Y, Mon, D}, {H, Min, S}}) ->
    Weekday = weekday(calendar:day_of_the_week(Y, Mon, D)),
    lists:flatten(io_lib:format("~s ~s ~2.2.0w ~w ~2.2.0w:~2.2.0w:~2.2.0w",
				[Weekday, month(Mon), D, Y, H, Min, S])).

weekday(1) -> "Mon";
weekday(2) -> "Tue";
weekday(3) -> "Wed";
weekday(4) -> "Thu";
weekday(5) -> "Fri";
weekday(6) -> "Sat";
weekday(7) -> "Sun".

month(1) -> "Jan";
month(2) -> "Feb";
month(3) -> "Mar";
month(4) -> "Apr";
month(5) -> "May";
month(6) -> "Jun";
month(7) -> "Jul";
month(8) -> "Aug";
month(9) -> "Sep";
month(10) -> "Oct";
month(11) -> "Nov";
month(12) -> "Dec".

year() ->
    {Y, _, _} = date(),
    integer_to_list(Y).


make_dirname({{YY,MM,DD},{H,M,S}}) -> 
    io_lib:format(logdir_prefix()++".~w-~2.2.0w-~2.2.0w_~2.2.0w.~2.2.0w.~2.2.0w",
		  [YY,MM,DD,H,M,S]).

logdir_prefix() ->
    "ct_master_run".

log_timestamp(Now) ->
    put(log_timestamp,Now),
    {_,{H,M,S}} = calendar:now_to_local_time(Now),
    lists:flatten(io_lib:format("~2.2.0w:~2.2.0w:~2.2.0w",
				[H,M,S])).

basic_html() ->
    case application:get_env(common_test_master, basic_html) of
	{ok,true} ->
	    true;
	_ ->
	    false
    end.

xhtml(HTML, XHTML) ->
    ct_logs:xhtml(HTML, XHTML).

locate_priv_file(File) ->
    ct_logs:locate_priv_file(File).

make_relative(Dir) ->
    ct_logs:make_relative(Dir).

force_write_file(Name,Contents) ->
    _ = force_delete(Name),
    file:write_file(Name,Contents).

force_delete(Name) ->
    case file:delete(Name) of
	{error,eacces} ->
	    force_rename(Name,Name++".old.",0);
	Other ->
	    Other
    end.

force_rename(From,To,Number) ->
    Dest = [To|integer_to_list(Number)],
    case file:read_file_info(Dest) of
	{ok,_} ->
	    force_rename(From,To,Number+1);
	{error,_} ->
	    file:rename(From,Dest)
    end.

call(Msg) ->
    case whereis(?MODULE) of
	undefined ->
	    {error,does_not_exist};
	Pid ->
	    MRef = erlang:monitor(process,Pid),
	    Ref = make_ref(),
	    ?MODULE ! {Msg,{self(),Ref}},
	    receive
		{Ref, Result} -> 
		    erlang:demonitor(MRef, [flush]),
		    Result;
		{'DOWN',MRef,process,_,Reason}  -> 
		    {error,{process_down,?MODULE,Reason}}
	    end
    end.

return({To,Ref},Result) ->
    To ! {Ref, Result},
    ok.

cast(Msg) ->
    case whereis(?MODULE) of
	undefined ->
	    io:format("Warning: ct_master_logs not started~n"),
	    {_,_,Content} = Msg,
	    FormatArgs = get_format_args(Content),
	    _ = [io:format(Format, Args) || {Format, Args} <- FormatArgs],
	    ok;
	_Pid ->
	    ?MODULE ! Msg,
	    ok
    end.

get_format_args(Content) ->
    lists:map(fun(C) ->
		  case C of
		      {_, FA, _} -> FA;
		      _ -> C
		  end
	      end, Content).

make_dir(Dir) ->
    case file:make_dir(Dir) of
	{error, eexist} ->
		ok;
	    Else ->
		Else
    end.
