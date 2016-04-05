%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(resource_monitor_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
      parse_information_unit
    ].

%% ---------------------------------------------------------------------------
%% Tests
%% ---------------------------------------------------------------------------

parse_information_unit(_Config) ->
    lists:foreach(fun ({S, V}) ->
                          V = rabbit_resource_monitor_misc:parse_information_unit(S)
                  end,
                  [
                   {"1000", {ok, 1000}},

                   {"10kB", {ok, 10000}},
                   {"10MB", {ok, 10000000}},
                   {"10GB", {ok, 10000000000}},

                   {"10kiB", {ok, 10240}},
                   {"10MiB", {ok, 10485760}},
                   {"10GiB", {ok, 10737418240}},

                   {"10k", {ok, 10240}},
                   {"10M", {ok, 10485760}},
                   {"10G", {ok, 10737418240}},

                   {"10KB", {ok, 10000}},
                   {"10K",  {ok, 10240}},
                   {"10m",  {ok, 10485760}},
                   {"10Mb", {ok, 10000000}},

                   {"0MB",  {ok, 0}},

                   {"10 k", {error, parse_error}},
                   {"MB", {error, parse_error}},
                   {"", {error, parse_error}},
                   {"0.5GB", {error, parse_error}},
                   {"10TB", {error, parse_error}}
                  ]),
    passed.
