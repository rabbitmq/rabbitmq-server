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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2011 VMware, Inc.  All rights reserved.
%%

-module(state_error_monad).

-export([eval/2, exec/2, run/3]).

eval(Funs, State) ->
    case run(Funs, ok, State) of
        {error, _Err} = Error -> Error;
        {Result, _State}      -> Result
    end.

exec(Funs, State) ->
    case run(Funs, ok, State) of
        {error, _Err} = Error -> Error;
        {_Result, State1}     -> State1
    end.

run([], Result, State) ->
    {Result, State};
run([Fun|Funs], Result, State) ->
    case Fun(Result, State) of
        {error, Err}        -> {error, {State, Err}};
        {set_state, State1} -> run(Funs, ok, State1);
        {join, Funs1}       -> run(Funs1 ++ Funs, ok, State);
        Result1             -> run(Funs, Result1, State)
    end.
