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
%% Copyright Ericsson AB 2011-2023. All Rights Reserved.
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% This code originated here and has been modified to suit RabbitMQ:
%% https://github.com/erlang/otp/blob/2d43af53899d35423f1c83887026089c91bce010/lib/ssl/src/ssl_dist_sup.erl
%%

-module(rabbit_prelaunch_file).

-export([consult_file/1, consult_string/1]).

%%
%% consult_file/consult_string API
%%

-spec consult_file(atom() | string()) ->
    {'error',{'badfile',atom() | string() | {_,_} | {'scan_error',{_,_,_},non_neg_integer() | {_,_}}}} | {'ok',_}.
consult_file(Path) ->
    case erl_prim_loader:get_file(Path) of
        {ok, Binary, _FullName} ->
            Encoding =
                case epp:read_encoding_from_binary(Binary) of
                    none ->
                        latin1;
                    Enc ->
                        Enc
                end,
            case unicode:characters_to_list(Binary, Encoding) of
                {error, _String, Rest} ->
                    {error, {badfile, {encoding_error, Rest}}};
                {incomplete, _String, Rest} ->
                    {error, {badfile, {encoding_incomplete, Rest}}};
                String when is_list(String) ->
                    consult_string(String)
            end;
        error ->
            {error, {badfile, Path}}
    end.

-spec consult_string(string()) ->
    {'error',{'badfile',{_,_} | {'scan_error',{_,_,_},non_neg_integer() | {_,_}}}} | {'ok',_}.
consult_string(String) ->
    case erl_scan:string(String) of
        {error, Info, Location} ->
            {error, {badfile, {scan_error, Info, Location}}};
        {ok, Tokens, _EndLocation} ->
            consult_tokens(Tokens)
    end.

%%
%% consult_file / consult_string implementation
%%

consult_tokens(Tokens) ->
    case erl_parse:parse_exprs(Tokens) of
        {error, Info} ->
            {error, {badfile, {parse_error, Info}}};
        {ok, [Expr]} ->
            consult_expression(Expr);
        {ok, Other} ->
            {error, {badfile, {parse_error, Other}}}
    end.

consult_expression(Expr) ->
    Result = try
            erl_eval:expr(Expr, erl_eval:new_bindings())
        catch
            Class:Error0 ->
                {error, {badfile, {Class, Error0}}}
        end,
    case Result of
        {value, Value, Bs} ->
            case erl_eval:bindings(Bs) of
                [] ->
                    {ok, [Value]};
                Other ->
                    {error, {badfile, {bindings, Other}}}
            end;
        Error1 ->
            Error1
    end.

