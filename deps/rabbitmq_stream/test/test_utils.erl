%% The contents of this file are subject to the Mozilla Public License
%% Version 2.0 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/en-US/MPL/2.0/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(test_utils).

-export([wait_until/1]).

wait_until(Predicate) ->
    Fun = fun(Pid, Fun) ->
             case Predicate() of
                 true ->
                     Pid ! done,
                     ok;
                 _ ->
                     timer:sleep(100),
                     Fun(Pid, Fun)
             end
          end,
    CurrentPid = self(),
    Pid = spawn(fun() -> Fun(CurrentPid, Fun) end),
    Result =
        receive
            done ->
                ok
        after 5000 ->
            failed
        end,
    exit(Pid, kill),
    Result.
