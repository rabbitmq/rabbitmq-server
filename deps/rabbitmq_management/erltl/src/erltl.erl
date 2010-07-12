%% @author Yariv Sadan <yarivsblog@gmail.com> [http://yarivsblog.com]
%% @version 0.9.3
%% @copyright Yariv Sadan 2006-2007
%%
%% @doc ErlTL is a simple Erlang template language.
%% 
%% == Introduction ==
%% ErlTL is a template language used for creating Erlang modules that
%% programatically generate iolists (nested lists of strings and/or binaries)
%% whose contents are mostly known at compile time.
%% ErlTL templates are generally less verbose and more readable than
%% the equivalent hand-written Erlang code.
%% A common use-case for ErlTL is the generation of
%% dynamic HTML in web applications.
%%
%% ErlTL emphasizes speed, simplicity, reusability and good error reporting.
%% The ErlTL compiler transforms template files into Erlang modules whose
%% whose functions are exported so they can be used in other modules.
%% By compiling to BEAM, ErlTL doesn't add any overhead to writing a template's
%% logic in pure Erlang.
%%
%% == Tag Reference ==
%%
%% An ErlTL template can be composed of the following tags:
%%
%% `<% [Exprs] %>' <br/>Erlang expression tag. This tag contains one or more
%% Erlang expressions that are evaluated in runtime.
%% For the template to return a valid iolist, the results of embedded Erlang
%% expressions must be strings or binaries.
%%
%% `<%@ [FuncDecl] %>' <br/>Function declaration tag.
%% An ErlTL template compiles into an Erlang module containing one or more
%% functions. This module always contains a function named 'render'
%% that accepts a single parameter called 'Data'. The 'render' function
%% corresponds to the area at the top of an ErlTL file, above all other
%% function declarations.
%%
%% You can use the function declaration tag to add more functions to
%% an ErlTL template.
%% ErlTL functions return the iolist described by to the template region
%% between its declaration and the next function declaration, or the end of
%% the file. To facilitate pattern-matching, ErlTL translates consecutive
%% function declarations with the same name and arity into a single function
%% declaration with multiple clauses, as in the following example:
%%
%% ```
%% <%@ volume_desc(Val) when Val >= 20 %> Big
%% <%@ volume_desc(Val) when Val >= 10 %> Medium
%% <%@ volume_desc(Val) %> Small
%% '''
%%
%% Function declarations have 2 possible forms: basic and full.
%% A full function declaration contains a complete Erlang function
%% declaration up to the '->' symbol, e.g.
%% `"<%@ my_func(A, B = [1,2 | _]) when is_integer(A) %>"'.
%%
%% A basic function declaration contains only the name of the function,
%% e.g. "`<%@ my_func %>'". This declaration is equivalent to
%% `"<%@ my_func(Data) %>"'.
%%
%% `<%~ [TopForms] %>' <br/>Top-level forms tag.
%% This tag, which may appear only at the very top of an ErlTL file, can
%% contain any legal top-level Erlang form. This includes module attributes,
%% compiler directives, and even complete functions.
%%
%% `<%? [TopExprs] %>' <br/>Top-level expressions tag.
%% This tag, which may appear only at the top of ErlTL functions, contains
%% Erlang expressions whose result isn't part of the function's return value.
%% This is used primarily for "unpacking" the Data parameter and binding
%% its elements to local variables prior to using them in the body of a
%% function.
%%
%% `<%! [Comment] %>' <br/>Comment tag. The contents of this tag are
%% used for documentation only. They are discarded in compile-time.
%%
%% Following is an sample ErlTL template that uses the ErlTL tags above
%% (you can find this code under test/erltl):
%%
%% ```
%% <%~
%% %% date: 10/21/2006
%% -author("Yariv Sadan").
%% -import(widgets, [foo/1, bar/2, baz/3]).
%% %>
%% <%! This is a sample ErlTL template that renders a list of albums %>
%% <html>
%% <body>
%% <% [album(A) || A <- Data] %>
%% </body>
%% </html>
%%
%% <%@ album({Title, Artist, Songs}) %>
%% Title: <b><% Title %></b><br>
%% Artist: <b><% Artist %></b><br>
%% Songs: <br>
%% <table>
%% <% [song(Number, Name) || {Number, Name} <- Songs] %>
%% </table>
%%
%% <%@ song(Number, Name) when size(Name) > 15 %>
%% <%? <<First:13/binary, Rest/binary>> = Name %>
%% <% song(Number, [First, <<"...">>]) %>
%%
%% <%@ song(Number, Name) %>
%% <%?
%% Class =
%%   case Number rem 2 of
%%     0 -> <<"even">>;
%%     1 -> <<"odd">>
%%   end
%% %>
%% <tr>
%%   <td class="<% Class %>"><% integer_to_list(Number) %></td>
%%   <td class="<% Class %>"><% Name %></td>
%% </tr>
%% '''
%%
%% @end

%% Copyright (c) Yariv Sadan 2006-2007
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the
%% "Software"), to deal in the Software without restriction, including
%% without limitation the rights to use, copy, modify, merge, publish,
%% distribute, sublicense, and/or sell copies of the Software, and to
%% permit persons to whom the Software is furnished to do so, subject to
%% the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included
%% in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
%% OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
%% IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
%% CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
%% TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
%% SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-module(erltl).
-author("Yariv Sadan (yarivsblog@gmail.com, http://yarivsblog.com)").
-export([compile/1, compile/2, forms_for_file/1, 
	 forms_for_file/2, forms_for_data/2, forms_for_data/3]).

-define(L(Msg), io:format("~b ~p~n", [?LINE, Msg])).

%% @doc Compile the ErlTL file with the default options:
%%  `[{outdir, FileDir}, report_errors, report_warnings, nowarn_unused_vars]'.
%%  (FileDir is the directory of the source file.)
%%
%%  After compilation, the resulting BEAM is loaded into the VM
%%  (the old code is purged if necessary).
%%
%% @spec compile(FileName::string()) -> ok | {error, Err}
compile(FileName) ->
    compile(FileName, [{outdir, filename:dirname(FileName)},
		       report_errors, report_warnings, nowarn_unused_vars]).

%% @doc Compile the ErlTL file with user-defined options. The options are
%% described in the documentation for the 'compile' module.
%% For more information, visit
%% [http://erlang.org/doc/doc-5.5.1/lib/compiler-4.4.1/doc/html/compile.html]
%%
%% @spec compile(FileName::string(), Options::[option()]) -> ok | {error, Err}
compile(FileName, Options) ->
    IncludePaths = lists:foldl(
		     fun({i, Path}, Acc) ->
			     [Path | Acc];
			(_Other, Acc) ->
			     Acc
		     end, [], Options),
    case forms_for_file(FileName, IncludePaths) of
	{ok, Forms} ->
	    case compile:forms(Forms,
			       Options) of
		{ok, Module, Bin} ->
		    OutDir = case lists:keysearch(outdir, 1, Options)
				 of
				 {value, {outdir, Val}} -> Val;
				 false -> filename:dirname(FileName)
			     end,
		    BaseName = filename:rootname(filename:basename(FileName)),
		    case file:write_file(OutDir ++ ['/' | BaseName] ++
					 ".beam", Bin) of
			ok ->
			    code:purge(Module),
			    case code:load_binary(
				   Module, atom_to_list(Module), Bin)
				of
				{module, _Module} ->
				    ok;
				Err ->
				    Err
			    end;
			{error, _} = Err ->
			    Err
		    end;
		Err ->
		    Err
	    end;
	Err -> Err
    end.


%% @equiv forms_for_file(Filename, [])
forms_for_file(FileName) ->
    forms_for_file(FileName, []).

%% @doc Parse the ErlTL file and return its representation in Erlang
%%   abstract forms.
%% @spec forms_for_file(FileName::string(),
%%   IncludePaths::[string()]) -> {ok, [form()]} | {error, Err}
forms_for_file(FileName, IncludePaths) ->
    case file:read_file(FileName) of
	{ok, Binary} ->
	    BaseName = filename:rootname(filename:basename(FileName)),
	    forms_for_data(Binary, list_to_atom(BaseName), IncludePaths);
	Err ->
	    Err
    end.

%% @equiv forms_form_data(Data, ModuleName, [])
forms_for_data(Data, ModuleName) ->
    forms_for_data(Data, ModuleName, []).

%% @doc Parse the raw text of an ErlTL template and return its
%%   representation in abstract forms.
%% @spec forms_for_data(Data::binary() | string(), ModuleName::atom(),
%%   IncludePaths::[string()]) ->
%%   {ok, [form()]} | {error, Err}
forms_for_data(Data, ModuleName, IncludePaths) when is_binary(Data) ->
    forms_for_data(binary_to_list(Data), ModuleName, IncludePaths);
forms_for_data(Data, ModuleName, IncludePaths) ->
    Lines = make_lines(Data),
    case forms(Lines, ModuleName) of
	{ok, Forms} ->
	    case catch lists:map(
			 fun({attribute, _, include, Include}) ->
				 process_include(
				   Include, [[], ["."] | 
					     IncludePaths]);
			    (Form) ->
				 Form
			 end, Forms)
		       of 
			   {'EXIT', Err} ->
			 {error, Err};
		       Res ->
			 {ok, lists:flatten(Res)}
		 end;
	Err ->
	    Err
    end.

process_include(Include, []) ->
    exit({file_not_found, Include});
process_include(Include, [Path | Rest]) ->
    case epp:parse_file(Path ++ "/" ++ Include, [], []) of
	{error, enoent} ->
	    process_include(Include, Rest);
	{ok, IncludeForms} ->
	    lists:sublist(
	      IncludeForms,
	      2,
	      length(IncludeForms) - 2)
    end.

make_lines(Str) ->
    make_lines(Str, [], []).

make_lines([], [], Result) -> lists:reverse(Result);
make_lines([], Acc, Result) -> lists:reverse([lists:reverse(Acc) | Result]);
make_lines([10 | Tail], Acc, Result) ->
    make_lines(Tail, [], [lists:reverse(Acc) | Result]);
make_lines([Head | Tail], Acc, Result) ->
    make_lines(Tail, [Head | Acc], Result).
    
    

forms(Lines, Module) ->
    case catch parse(Lines) of
	{'EXIT', Err} ->
	    {error, Err};
	Forms ->
	    {ok, [{attribute,1,module,Module},
	     {attribute,2,file,{atom_to_list(Module),1}},
	     {attribute,3,compile,export_all}] ++ lists:reverse(Forms)}
    end.

parse(Lines) ->
    parse(Lines, binary, 1, [], [], [], []).

parse([], _State, _LineNo, TopExprs, Exprs, AllForms, ChunkAcc) ->
    [FirstForms | OtherForms] = initial_forms(AllForms),
    combine_forms(
      embed_exprs(FirstForms, TopExprs, last_exprs(ChunkAcc, Exprs)) ++
      OtherForms);
parse([Line | OtherLines], State, LineNo, TopExprs, Exprs,
	    AllForms, ChunkAcc) ->
    case scan(Line, State) of
	more ->
	    Line1 = case State of
			binary -> Line ++ "\n";
			_ -> Line
		    end,
	    parse(OtherLines, State, LineNo+1, TopExprs, Exprs,
			    AllForms, [{Line1, LineNo} | ChunkAcc]);
	{ok, Chunk, NextState, NextChunk} ->
	    Chunks = [{Chunk, LineNo} | ChunkAcc],
	    Result = case State of
			 func_decl -> {func_decl, parse_func_decl(Chunks)};
			 binary -> {exprs, parse_binary(Chunks)};
			 top_exprs -> {top_exprs, parse_exprs(Chunks)};
			 forms -> {forms, parse_forms(Chunks)};
			 erlang -> {exprs, parse_exprs(Chunks)};
			 comment -> comment
		     end,
	    case Result of
		comment ->
		    {NextLines, LineDiff} = 
			skip_line_break([NextChunk | OtherLines]),
		    parse(NextLines,
			  NextState, LineNo + LineDiff,
			  TopExprs, Exprs, AllForms, []);
		{func_decl, BasicForms} ->
		    [CurrentForms | PreviousForms] = initial_forms(AllForms),
		    AllForms1 = 
			embed_exprs(CurrentForms, TopExprs, Exprs) ++
			PreviousForms,
		    NewForms = combine_forms(AllForms1),
		    
		    {NextLines, LineDiff} = 
			skip_line_break([NextChunk | OtherLines]),
		    parse(NextLines,
			  NextState, LineNo + LineDiff, [], [],
			  [BasicForms | NewForms], []);
		{exprs, []} ->
		    NextLineNo = case Chunk of
				     [] -> LineNo;
				     _ -> LineNo + 1
				 end,
		    parse([NextChunk | OtherLines],
			  NextState, NextLineNo, TopExprs,
			  Exprs, AllForms, []);
		{exprs, Exprs1} ->
		    parse([NextChunk | OtherLines],
			  NextState, LineNo, TopExprs,
			  Exprs1 ++ Exprs,
			  initial_forms(AllForms), []);
		{top_exprs, TopExprs1} ->
		    case Exprs of
			[] ->
			    parse([NextChunk | OtherLines], NextState, LineNo,
				  TopExprs1 ++ TopExprs,
				  Exprs, AllForms, []);
			_ ->
			    error(misplaced_top_exprs, LineNo, Line,
				  "top expressions must appear before all "
				  "other expressions in a function")
		    end;
		{forms, Forms} ->
		    case AllForms of
			[] ->
			    {NextLines, LineDiff} = 
				skip_line_break([NextChunk | OtherLines]),
			    parse(NextLines,
				    NextState, LineNo + LineDiff, [], [],
				    initial_forms([]) ++ Forms, []);
			_ -> error(misplaced_top_declaration, LineNo, Line,
				   "top-level declarations must appear at the "
				   "top of a file")
		    end
	    end
    end.


combine_forms([{function,L,FuncName,Arity,Clauses},
	       {function,_,LastFuncName,LastArity,LastClauses} | 
	       PreviousForms]) when LastFuncName == FuncName,
				    LastArity == Arity ->
    [{function,L,FuncName,Arity, LastClauses ++ Clauses} | PreviousForms];
combine_forms(Forms) -> Forms.

scan(Line, State) ->
    Delim =
	case State of
	    binary -> {"<%",
		       [{$@, func_decl},
			{$!, comment},
			{$?, top_exprs},
			{$~, forms}],
		       erlang};
	    _ -> {"%>", [], binary}
	end,
    scan1(Line, Delim).

scan1(Line, {Delim, Options, Default}) ->
    case string:str(Line, Delim) of
	0 -> more;
	Pos ->
	    {First, [_,_ | Rest]} = lists:split(Pos-1, Line),
	    {NextState, NextChunk} =
		case Rest of
		    [] -> {Default, Rest};
		    [FirstChar | OtherChars] ->
			case lists:keysearch(FirstChar, 1, Options) of
			    {value, {_, NextState1}} ->
				{NextState1, OtherChars};
			    false -> {Default, Rest}
			end
		end,
	    {ok, First, NextState, NextChunk}
    end.

initial_forms([]) -> [parse_func_decl([{"render", 1}])];
initial_forms(AllForms) -> AllForms.

skip_line_break([[] | Lines]) -> {Lines, 1};
skip_line_break(Lines) -> {Lines, 0}.

last_exprs([], Exprs) -> Exprs;
last_exprs(ChunkAcc, Exprs) -> 
    parse_binary(ChunkAcc) ++ Exprs.
		       
parse_binary([]) -> [];
parse_binary([{[],_}]) -> [];
parse_binary(Fragments) ->
    BinElems =
	lists:foldl(
	  fun({Chars, LineNo}, Acc) ->
		  Elems = lists:foldl(
		    fun(Char, BinElems) ->
			    [{bin_element,LineNo,{integer,LineNo,Char},
			      default,default} | BinElems]
		    end, [], lists:reverse(Chars)),
		  Elems ++ Acc
	  end, [], Fragments),
    [{bin,1,BinElems}].

parse_exprs([]) -> [];
parse_exprs(Fragments) ->
    Tokens = lists:foldl(
	    fun({Frag, LineNo}, Acc) ->
		    case erl_scan:string(Frag, LineNo) of
			{ok, Toks, _} -> Toks ++ Acc;
			{error, Err, _} -> error(scan_error, LineNo, Frag, Err)
		    end
	    end, [{dot,1}], Fragments),
    Tokens1 = Tokens,
    case erl_parse:parse_exprs(Tokens1) of
	{ok, Exprs} -> [{block,1, Exprs}];
	{error, Msg} -> exit({parse_error, Msg})
    end.

parse_forms([]) -> [];
parse_forms(Fragments) ->
    FormsTokens =
	lists:foldl(
	  fun({Frag, LineNo}, Acc) ->
		  case erl_scan:string(Frag, LineNo) of
		      {ok, Toks, _} ->
			  lists:foldl(
			    fun({dot,_} = Tok, [Form | Rest]) ->
				    [[Tok] | [Form | Rest]];
			       (Tok, [Form | Rest]) ->
				    [[Tok | Form] | Rest]
			    end, Acc, lists:reverse(Toks));
		      {error, Err, _} ->
			  error(scan_error, LineNo, Frag, Err)
		  end
	  end, [[]], Fragments),
    lists:foldl(
      fun([], Acc) -> Acc;
	 (FormTokens, Acc) ->
	      case erl_parse:parse_form(FormTokens) of
		  {ok, Form} ->
		      [Form | Acc];
		  {error, Err} -> exit({parse_error, Err})
	      end
      end, [], FormsTokens).

parse_func_decl(Fragments) ->
    {FuncDecl, LineNo} = 
	lists:foldl(
	  fun({Chars, LineNo}, {Acc, FirstLine}) ->
		  Elems = lists:foldl(
		    fun(Char, Chars1) ->
			    [Char | Chars1]
		    end, [], lists:reverse(Chars)),
		  FirstLine1 = case FirstLine of
				   undefined -> LineNo;
				   _ -> FirstLine
			       end,
		  {Elems ++ Acc, FirstLine1}
	  end, {[], undefined}, Fragments),

    case erl_scan:string(FuncDecl) of
	{ok, [{atom,_,FuncName}], _} ->
	    [{full_form,
		   {function, LineNo, FuncName, 0,
		    [{clause, LineNo, [], [],
		      [{call,LineNo,{atom,LineNo,FuncName},
			[{atom,1,undefined}]}]
		     }]
		   }},
	     {empty_form,
	      {function, LineNo, FuncName, 1,
	       [{clause, LineNo, [{var,LineNo,'Data'}], [], []}]}}];
	{ok, _, _} ->
	    case erl_scan:string(FuncDecl ++ " -> funky_func.") of
		{ok, Toks, _} ->
		    case erl_parse:parse_form(Toks) of
			{ok, Form} ->
			    [{empty_form,
			      change_line_numbers(LineNo, Form)}];
			{error, Msg} ->
			    error(parse_error, LineNo, FuncDecl, Msg)
		    end;
		{error, Msg, _} ->
		    error(scan_error, LineNo, FuncDecl, Msg)
	    end;
	{error, Msg, _} -> error(scan_error, LineNo, FuncDecl, Msg)
    end.


error(Type, Line, Chunk, Msg) ->
    exit({Type, {{line, Line}, {chunk, Chunk}, {msg, Msg}}}).

embed_exprs(Forms, TopExprs, Exprs) when is_list(Forms) ->
    [embed_exprs(Form, TopExprs, Exprs) || Form <- Forms];
embed_exprs({full_form, Form}, _TopExprs, _Exprs) -> Form;
embed_exprs({empty_form,
	     {function,Line,FuncName,Arity,
	      [{clause, Line1, Params, Guards, _UnusedExprs}]}},
	    TopExprs, Exprs) ->
    {function,Line,FuncName,Arity,[{clause,Line1,Params,Guards,
				    lists:reverse(TopExprs) ++
				    [cons_exprs(lists:reverse(Exprs))]}]}.

cons_exprs([]) -> {nil,1};
cons_exprs([{bin,L,BinElems}, {bin,_,BinElems1} | Rest]) ->
    cons_exprs([{bin,L,BinElems ++ BinElems1} | Rest]);
cons_exprs([Expr|Rest]) ->
    {cons,1,Expr,cons_exprs(Rest)}.
    

change_line_numbers(L, Exprs) when is_list(Exprs) ->
    lists:foldl(
      fun(Expr, Acc) ->
	      [change_line_numbers(L, Expr) | Acc]
      end, [], lists:reverse(Exprs));
change_line_numbers(L, Expr) when is_tuple(Expr) ->
    Expr1 = case is_integer(element(2, Expr)) of
		true -> setelement(2, Expr, L);
		false -> Expr
	    end,
    Elems = tuple_to_list(Expr1),
    NewElems =
	lists:foldl(
	  fun(Elem, Acc) ->
		  NewElem = change_line_numbers(L, Elem),
		  [NewElem | Acc]
	  end, [], lists:reverse(Elems)),
    list_to_tuple(NewElems);
change_line_numbers(_L, Expr) ->
    Expr.
