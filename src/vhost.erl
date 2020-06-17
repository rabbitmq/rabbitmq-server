%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(vhost).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("vhost.hrl").

-export([
  new/2,
  new/3,
  fields/0,
  fields/1,
  info_keys/0,
  record_version_to_use/0,
  upgrade/1,
  upgrade_to/2,
  pattern_match_all/0,
  get_name/1,
  get_limits/1,
  get_metadata/1,
  get_description/1,
  get_tags/1,
  set_limits/2
]).

-define(record_version, vhost_v2).

-type(name() :: binary()).

-type(metadata_key() :: atom()).

-type(metadata() :: #{description => binary(),
                      tags => [atom()],
                      metadata_key() => any()} | undefined).

-type vhost() :: vhost_v1:vhost_v1() | vhost_v2().

-record(vhost, {
    %% name as a binary
    virtual_host :: name() | '_',
    %% proplist of limits configured, if any
    limits :: list() | '_',
    metadata :: metadata() | '_'
}).

-type vhost_v2() :: #vhost{
                          virtual_host :: name(),
                          limits :: list(),
                          metadata :: metadata()
                         }.

-type vhost_pattern() :: vhost_v1:vhost_v1_pattern() |
                         vhost_v2_pattern().
-type vhost_v2_pattern() :: #vhost{
                                  virtual_host :: name() | '_',
                                  limits :: '_',
                                  metadata :: '_'
                                 }.

-export_type([name/0,
              metadata_key/0,
              metadata/0,
              vhost/0,
              vhost_v2/0,
              vhost_pattern/0,
              vhost_v2_pattern/0]).

-spec new(name(), list()) -> vhost().
new(Name, Limits) ->
    case record_version_to_use() of
        ?record_version ->
          #vhost{virtual_host = Name, limits = Limits};
        _ ->
          vhost_v1:new(Name, Limits)
    end.

-spec new(rabbit_vhost:name(), list(), map()) -> vhost().
new(Name, Limits, Metadata) ->
    case record_version_to_use() of
        ?record_version ->
          #vhost{virtual_host = Name, limits = Limits, metadata = Metadata};
        _ ->
          vhost_v1:new(Name, Limits)
    end.

-spec record_version_to_use() -> vhost_v1 | vhost_v2.

record_version_to_use() ->
    case rabbit_feature_flags:is_enabled(virtual_host_metadata) of
        true  -> ?record_version;
        false -> vhost_v1:record_version_to_use()
    end.

-spec upgrade(vhost()) -> vhost().

upgrade(#vhost{} = VHost) -> VHost;
upgrade(OldVHost)         -> upgrade_to(record_version_to_use(), OldVHost).

-spec upgrade_to
(vhost_v2, vhost()) -> vhost_v2();
(vhost_v1, vhost_v1:vhost_v1()) -> vhost_v1:vhost_v1().

upgrade_to(?record_version, #vhost{} = VHost) ->
    VHost;
upgrade_to(?record_version, OldVHost) ->
    Fields = erlang:tuple_to_list(OldVHost) ++ [#{description => <<"">>, tags => []}],
    #vhost{} = erlang:list_to_tuple(Fields);
upgrade_to(Version, OldVHost) ->
    vhost_v1:upgrade_to(Version, OldVHost).


fields() ->
    case record_version_to_use() of
        ?record_version -> fields(?record_version);
        _               -> vhost_v1:fields()
    end.

fields(?record_version) -> record_info(fields, vhost);
fields(Version)         -> vhost_v1:fields(Version).

info_keys() ->
    case record_version_to_use() of
        %% note: this reports description and tags separately even though
        %% they are stored in the metadata map. MK.
        ?record_version -> [name, description, tags, metadata, tracing, cluster_state];
        _               -> vhost_v1:info_keys()
    end.

-spec pattern_match_all() -> vhost_pattern().

pattern_match_all() ->
    case record_version_to_use() of
        ?record_version -> #vhost{_ = '_'};
        _               -> vhost_v1:pattern_match_all()
    end.

-spec get_name(vhost()) -> name().
get_name(#vhost{virtual_host = Value}) -> Value;
get_name(VHost) -> vhost_v1:get_name(VHost).

-spec get_limits(vhost()) -> list().
get_limits(#vhost{limits = Value}) -> Value;
get_limits(VHost) -> vhost_v1:get_limits(VHost).

-spec get_metadata(vhost()) -> metadata().
get_metadata(#vhost{metadata = Value}) -> Value;
get_metadata(VHost) -> vhost_v1:get_limits(VHost).

-spec get_description(vhost()) -> binary().
get_description(#vhost{} = VHost) ->
    maps:get(description, get_metadata(VHost), undefined);
get_description(VHost) ->
    vhost_v1:get_description(VHost).

-spec get_tags(vhost()) -> [atom()].
get_tags(#vhost{} = VHost) ->
    maps:get(tags, get_metadata(VHost), undefined);
get_tags(VHost) ->
    vhost_v1:get_tags(VHost).

set_limits(VHost, Value) ->
    case record_version_to_use() of
      ?record_version ->
        VHost#vhost{limits = Value};
      _ ->
        vhost_v1:set_limits(VHost, Value)
    end.
