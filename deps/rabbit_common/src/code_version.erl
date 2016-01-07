%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%
-module(code_version).

-export([update/1]).

update(Module) ->
    AbsCode = get_abs_code(Module),
    Forms = replace_forms(Module, get_otp_version() >= 18, AbsCode),
    Code = compile_forms(Forms),
    load_code(Module, Code).

load_code(Module, Code) ->
    unload(Module),
    case code:load_binary(Module, "loaded by rabbit_common", Code) of
        {module, _} ->
            ok;
        {error, _Reason} ->
            throw(cannot_load)
    end.

unload(Module) ->
    code:soft_purge(Module),
    code:delete(Module).

compile_forms(Forms) ->
    case compile:forms(Forms, [debug_info]) of
        {ok, _ModName, Code} ->
            Code;
        {ok, _ModName, Code, _Warnings} ->
            Code;
        _ ->
            throw(cannot_compile_forms)
    end.

get_abs_code(Module) ->
    get_forms(get_object_code(Module)).

get_object_code(Module) ->
    case code:get_object_code(Module) of
        {_Mod, Code, _File} ->
            Code;
        error ->
            throw(not_found)
    end.

get_forms(Code) ->
    case beam_lib:chunks(Code, [abstract_code]) of
        {ok, {_, [{abstract_code, {raw_abstract_v1, Forms}}]}} ->
            Forms;
        {ok, {_, [{abstract_code, no_abstract_code}]}} ->
            throw(no_abstract_code);
        {error, beam_lib, Reason} ->
            throw({no_abstract_code, Reason})
    end.

get_otp_version() ->
    Version = erlang:system_info(otp_release),
    case re:run(Version, "^[0-9][0-9]", [{capture, first, list}]) of
        {match, [V]} ->
            list_to_integer(V);
        _ ->
            %% Could be anything below R17, we are not interested
            0
    end.

get_original_pairs(VersionSupport) ->
    [{Orig, Arity} || {Orig, _Pre, _Post, Arity} <- VersionSupport].

get_delete_pairs(true, VersionSupport) ->
    [{Pre, Arity} || {_Orig, Pre, _Post, Arity} <- VersionSupport];
get_delete_pairs(false, VersionSupport) ->
    [{Post, Arity} || {_Orig, _Pre, Post, Arity} <- VersionSupport].

get_rename_pairs(true, VersionSupport) ->
    [{Post, Arity} || {_Orig, _Pre, Post, Arity} <- VersionSupport];
get_rename_pairs(false, VersionSupport) ->
    [{Pre, Arity} || {_Orig, Pre, _Post, Arity} <- VersionSupport].

get_name_pairs(true, VersionSupport) ->
    [{Post, Orig} || {Orig, _Pre, Post, _Arity} <- VersionSupport];
get_name_pairs(false, VersionSupport) ->
    [{Pre, Orig} || {Orig, Pre, _Post, _Arity} <- VersionSupport].

delete_abstract_functions(ToDelete) ->
    fun(Tree, Function) ->
            case lists:member(Function, ToDelete) of
                true ->
                    erl_syntax:comment(["Deleted unused function"]);
                false ->
                    Tree
            end
    end.

rename_abstract_functions(ToRename, ToName) ->
    fun(Tree, {Name, _Arity} = Function) ->
            case lists:member(Function, ToRename) of
                true ->
                    FunctionName = proplists:get_value(Name, ToName),
                    erl_syntax:function(
                      erl_syntax:atom(FunctionName),
                      erl_syntax:function_clauses(Tree));
                false ->
                    Tree
            end
    end.

replace_forms(Module, IsPost18, AbsCode) ->
    Attr = Module:module_info(attributes),
    VersionSupport = proplists:get_value(version_support, Attr),
    Original = get_original_pairs(VersionSupport),
    ToDelete = get_delete_pairs(IsPost18, VersionSupport),
    DeleteFun = delete_abstract_functions(ToDelete ++ Original),
    AbsCode0 = replace_function_forms(AbsCode, DeleteFun),
    ToRename = get_rename_pairs(IsPost18, VersionSupport),
    ToName = get_name_pairs(IsPost18, VersionSupport),
    RenameFun = rename_abstract_functions(ToRename, ToName),
    remove_exports(replace_function_forms(AbsCode0, RenameFun), ToDelete ++ ToRename).

replace_function_forms(AbsCode, Fun) ->
    ReplaceFunction =
        fun(Tree) ->
                case erl_syntax_lib:analyze_function(Tree) of
                    {_N, _A} = Function ->
                        Fun(Tree, Function);
                    _Other -> Tree
                end
        end,
    Filter = fun(Tree) ->
                     case erl_syntax:type(Tree) of
                         function -> ReplaceFunction(Tree);
                         _Other -> Tree
                     end
             end,
    fold_syntax_tree(Filter, AbsCode).

filter_export_pairs(Info, ToDelete) ->
    lists:filter(fun(Pair) ->
                         not lists:member(Pair, ToDelete)
                 end, Info).

remove_exports(AbsCode, ToDelete) ->
    RemoveExports =
        fun(Tree) ->
                case erl_syntax_lib:analyze_attribute(Tree) of
                    {export, Info} ->
                        Remaining = filter_export_pairs(Info, ToDelete),
                        rebuild_export(Remaining);
                    _Other -> Tree
                end
        end,
    Filter = fun(Tree) ->
                     case erl_syntax:type(Tree) of
                         attribute -> RemoveExports(Tree);
                         _Other -> Tree
                     end
             end,
    fold_syntax_tree(Filter, AbsCode).

rebuild_export(Args) ->
    erl_syntax:attribute(
      erl_syntax:atom(export),
      [erl_syntax:list(
         [erl_syntax:arity_qualifier(erl_syntax:atom(N),
                                     erl_syntax:integer(A))
          || {N, A} <- Args])]).

fold_syntax_tree(Filter, Forms) ->
    Tree = erl_syntax:form_list(Forms),
    NewTree = erl_syntax_lib:map(Filter, Tree),
    erl_syntax:revert_forms(NewTree).
