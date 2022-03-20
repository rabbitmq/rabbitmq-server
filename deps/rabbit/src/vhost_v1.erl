%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(vhost_v1).

-include("vhost.hrl").

-export([new/2,
         new/3,
         upgrade/1,
         upgrade_to/2,
         fields/0,
         fields/1,
         info_keys/0,
         field_name/0,
         record_version_to_use/0,
         pattern_match_all/0,
         get_name/1,
         get_limits/1,
         get_metadata/1,
         get_description/1,
         get_tags/1,
         set_limits/2
]).

-define(record_version, ?MODULE).

%% Represents a vhost.
%%
%% Historically this record had 2 arguments although the 2nd
%% was never used (`dummy`, always undefined). This is because
%% single field records were/are illegal in OTP.
%%
%% As of 3.6.x, the second argument is vhost limits,
%% which is actually used and has the same default.
%% Nonetheless, this required a migration, see rabbit_upgrade_functions.

-record(vhost, {
  %% name as a binary
  virtual_host :: vhost:name() | '_',
  %% proplist of limits configured, if any
  limits :: list() | '_'}).

-type vhost() :: vhost_v1().
-type vhost_v1() :: #vhost{
                          virtual_host :: vhost:name(),
                          limits :: list()
                         }.

-export_type([vhost/0,
              vhost_v1/0,
              vhost_pattern/0,
              vhost_v1_pattern/0]).


-spec new(vhost:name(), list()) -> vhost().
new(Name, Limits) ->
  #vhost{virtual_host = Name, limits = Limits}.

-spec new(vhost:name(), list(), map()) -> vhost().
new(Name, Limits, _Metadata) ->
  #vhost{virtual_host = Name, limits = Limits}.


-spec record_version_to_use() -> vhost_v1.
record_version_to_use() ->
    ?record_version.

-spec upgrade(vhost()) -> vhost().
upgrade(#vhost{} = VHost) -> VHost.

-spec upgrade_to(vhost_v1, vhost()) -> vhost().
upgrade_to(?record_version, #vhost{} = VHost) ->
    VHost.

fields() -> fields(?record_version).

fields(?record_version) -> record_info(fields, vhost).

field_name() -> #vhost.virtual_host.

info_keys() -> [name, tracing, cluster_state].

-type vhost_pattern() :: vhost_v1_pattern().
-type vhost_v1_pattern() :: #vhost{
                                  virtual_host :: vhost:name() | '_',
                                  limits :: '_'
                                 }.

-spec pattern_match_all() -> vhost_pattern().

pattern_match_all() -> #vhost{_ = '_'}.

get_name(#vhost{virtual_host = Value}) -> Value.
get_limits(#vhost{limits = Value}) -> Value.

get_metadata(_VHost) -> undefined.
get_description(_VHost) -> undefined.
get_tags(_VHost) -> undefined.

set_limits(VHost, Value) ->
    VHost#vhost{limits = Value}.
