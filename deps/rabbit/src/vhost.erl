%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(vhost).

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
  pattern_match_names/0,
  get_name/1,
  get_limits/1,
  get_metadata/1,
  get_description/1,
  get_tags/1,
  get_default_queue_type/1,
  set_limits/2,
  set_metadata/2,
  merge_metadata/2,
  new_metadata/3,
  is_tagged_with/2
]).

-define(record_version, vhost_v2).

-type(name() :: rabbit_types:vhost()).

-type(limits() :: list()).

-type(metadata_key() :: atom()).

-type(metadata() :: #{description => description(),
                      tags => [tag()],
                      metadata_key() => any()} | undefined).

-type(description() :: binary()).
-type(tag() :: atom()).
-type(tags() :: [tag()]).
-type(unparsed_tags() :: binary() | string() | atom()).

-type vhost() :: vhost_v2().

-record(vhost, {
    %% name as a binary
    virtual_host :: name() | '_' | '$1',
    %% proplist of limits configured, if any
    limits :: limits() | '_',
    metadata :: metadata() | '_'
}).

-type vhost_v2() :: #vhost{
                          virtual_host :: name(),
                          limits :: limits(),
                          metadata :: metadata()
                         }.

-type vhost_pattern() :: vhost_v2_pattern().
-type vhost_v2_pattern() :: #vhost{
                                  virtual_host :: name() | '_' | '$1',
                                  limits :: '_',
                                  metadata :: '_'
                                 }.

-export_type([name/0,
              limits/0,
              metadata_key/0,
              metadata/0,
              description/0,
              tag/0,
              unparsed_tags/0,
              tags/0,
              vhost/0,
              vhost_v2/0,
              vhost_pattern/0,
              vhost_v2_pattern/0]).

-spec new(name(), limits()) -> vhost().
new(Name, Limits) ->
    #vhost{virtual_host = Name, limits = Limits}.

-spec new(name(), limits(), metadata()) -> vhost().
new(Name, Limits, Metadata) ->
    #vhost{virtual_host = Name, limits = Limits, metadata = Metadata}.

-spec record_version_to_use() -> vhost_v2.

record_version_to_use() ->
    ?record_version.

-spec upgrade(vhost()) -> vhost().

upgrade(#vhost{} = VHost) -> VHost.

-spec upgrade_to(vhost_v2, vhost()) -> vhost_v2().

upgrade_to(?record_version, #vhost{} = VHost) ->
    VHost.

fields() ->
    fields(?record_version).

fields(?record_version) -> record_info(fields, vhost).

info_keys() ->
    %% note: this reports description and tags separately even though
    %% they are stored in the metadata map. MK.
    [name,
     description,
     tags,
     default_queue_type,
     metadata,
     tracing,
     cluster_state].

-spec pattern_match_all() -> vhost_pattern().

pattern_match_all() ->
    #vhost{_ = '_'}.

-spec pattern_match_names() -> vhost_pattern().
pattern_match_names() ->
    #vhost{virtual_host = '$1', _ = '_'}.

-spec get_name(vhost()) -> name().
get_name(#vhost{virtual_host = Value}) -> Value.

-spec get_limits(vhost()) -> limits().
get_limits(#vhost{limits = Value}) -> Value.

-spec get_metadata(vhost()) -> metadata().
get_metadata(#vhost{metadata = Value}) -> Value.

-spec get_description(vhost()) -> binary().
get_description(#vhost{} = VHost) ->
    maps:get(description, get_metadata(VHost), undefined).

-spec get_tags(vhost()) -> [tag()].
get_tags(#vhost{} = VHost) ->
    maps:get(tags, get_metadata(VHost), []).

-spec get_default_queue_type(vhost()) -> binary() | undefined.
get_default_queue_type(#vhost{} = VHost) ->
    maps:get(default_queue_type, get_metadata(VHost), undefined);
get_default_queue_type(_VHost) ->
    undefined.

set_limits(VHost, Value) ->
    VHost#vhost{limits = Value}.

-spec set_metadata(vhost(), metadata()) -> vhost().
set_metadata(VHost, Value) ->
    VHost#vhost{metadata = Value}.

-spec merge_metadata(vhost(), metadata()) -> vhost().
merge_metadata(VHost, NewVHostMeta) ->
    CurrentVHostMeta = get_metadata(VHost),
    FinalMeta =  maps:merge_with(
                   fun metadata_merger/3, CurrentVHostMeta, NewVHostMeta),
    VHost#vhost{metadata = FinalMeta}.

%% This is the case where the existing VHost metadata has a default queue type
%% value and the proposed value is `undefined`. We do not want the proposed
%% value to overwrite the current value
metadata_merger(default_queue_type, CurrentDefaultQueueType, undefined) ->
    CurrentDefaultQueueType;
%% This is the case where the existing VHost metadata has any default queue
%% type value, and the proposed value is NOT `undefined`. It is OK for any
%% proposed value to be used.
metadata_merger(default_queue_type, _, NewVHostDefaultQueueType) ->
    NewVHostDefaultQueueType;
%% This is the case for all other VHost metadata keys.
metadata_merger(_, _, NewMetadataValue) ->
    NewMetadataValue.

-spec new_metadata(binary(), [atom()], rabbit_queue_type:queue_type() | 'undefined') -> metadata().
new_metadata(Description, Tags, undefined) ->
    #{description => Description,
      tags => Tags};
new_metadata(Description, Tags, DefaultQueueType) ->
    #{description => Description,
      tags => Tags,
      default_queue_type => DefaultQueueType}.

-spec is_tagged_with(vhost(), tag()) -> boolean().
is_tagged_with(VHost, Tag) ->
    lists:member(Tag, get_tags(VHost)).
