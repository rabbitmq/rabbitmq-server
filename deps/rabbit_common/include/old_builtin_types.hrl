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
%% The Initial Developer of the Original Code is Pivotal Software, Inc.
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

%% Old builtin types found in ERlang R16B03.
-ifdef(use_old_builtin_types).
-define(ARRAY_TYPE, array).
-define(DICT_TYPE, dict).
-define(GB_SET_TYPE, gb_set).
-define(QUEUE_TYPE, queue).
-define(SET_TYPE, set).
-else.
-define(ARRAY_TYPE, array:array).
-define(DICT_TYPE, dict:dict).
-define(GB_SET_TYPE, gb_sets:set).
-define(QUEUE_TYPE, queue:queue).
-define(SET_TYPE, sets:set).
-endif.
