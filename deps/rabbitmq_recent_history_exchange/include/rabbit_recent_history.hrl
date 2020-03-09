%% The contents of this file are subject to the mozilla public license
%% version 1.1 (the "license"); you may not use this file except in
%% compliance with the license. you may obtain a copy of the license at
%% https://www.mozilla.org/mpl/
%%
%% software distributed under the license is distributed on an "as is"
%% basis, without warranty of any kind, either express or implied. see the
%% license for the specific language governing rights and limitations
%% under the license.
%%
%% Copyright (c) 2007-2020 VMware, Inc and its affiliates. All rights reserved.

-define(KEEP_NB, 20).
-define(RH_TABLE, rh_exchange_table).
-record(cached, {key, content}).
