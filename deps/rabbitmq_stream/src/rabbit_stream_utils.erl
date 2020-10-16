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
%% Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_stream_utils).

%% API
-export([enforce_correct_stream_name/1]).

enforce_correct_stream_name(Name) ->
  % from rabbit_channel
  StrippedName = binary:replace(Name, [<<"\n">>, <<"\r">>], <<"">>, [global]),
  case check_name(StrippedName) of
    ok ->
      {ok, StrippedName};
    error ->
      error
  end.

check_name(<<"amq.", _/binary>>) ->
  error;
check_name(<<"">>) ->
  error;
check_name(_Name) ->
  ok.
