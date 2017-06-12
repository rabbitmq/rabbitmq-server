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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(term_to_binary_compat).

-include("rabbit.hrl").

-export([queue_name_to_binary/1]).

queue_name_to_binary(#resource{kind = queue} = {resource, VHost, queue, Name}) ->
    VHostBSize = byte_size(VHost),
    NameBSize = byte_size(Name),
    <<131,                              %% Binary format "version"
      104, 4,                           %% 4-element tuple
      100, 0, 8, "resource",            %% `resource` atom
      109, VHostBSize:32, VHost/binary, %% Vhost binary
      100, 0, 5, "queue",               %% `queue` atom
      109, NameBSize:32, Name/binary>>. %% Name binary

