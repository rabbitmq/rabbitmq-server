%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(vhost).

-include("vhost.hrl").

-export([
  new/2,
  new/3,
  new/4,
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
  get_id/1,
  id_epoch/1,
  id_time/1,
  get_description/1,
  get_tags/1,
  tag_to_binary/1,
  get_default_queue_type/1,

  set_limits/2,
  set_metadata/2,
  merge_metadata/2,

  is_protected_from_deletion/1,
  enable_protection_from_deletion/1,
  disable_protection_from_deletion/1,

  new_metadata/3,
  is_tagged_with/2,

  to_map/1
]).

-define(record_version, vhost_v3).

-type(name() :: rabbit_types:vhost()).

-type(limits() :: list()).

-type(metadata_key() :: atom()).

-type(metadata() :: #{description => description(),
                      tags => [atom() | binary()],
                      metadata_key() => any()} | undefined).

-type(description() :: binary()).
-type(tag() :: binary()).
-type(tags() :: [tag()]).
-type(unparsed_tags() :: undefined | binary() | string() | [atom() | binary() | string()]).

%% The high bits identify the metadata store; the low 64 bits are a monotonic timestamp.
-type id() :: pos_integer().

-type vhost() :: vhost_v2() | vhost_v3().

-record(vhost, {
    %% name as a binary
    virtual_host :: name() | '_' | '$1',
    %% proplist of limits configured, if any
    limits :: limits() | undefined | '_',
    metadata :: metadata() | '_',
    id :: id() | '_'
}).

%% Written until `rabbitmq_5.0.0' is enabled.
-type vhost_v2() :: {vhost, name(), limits(), metadata()}.

-type vhost_v3() :: #vhost{
                          virtual_host :: name(),
                          limits :: limits(),
                          metadata :: metadata(),
                          id :: id()
                         }.

-type vhost_pattern() :: vhost_v2_pattern() | vhost_v3_pattern().
-type vhost_v2_pattern() :: {vhost, name() | '_' | '$1', '_', '_'}.
-type vhost_v3_pattern() :: #vhost{
                                  virtual_host :: name() | '_' | '$1',
                                  limits :: '_',
                                  metadata :: '_',
                                  id :: '_'
                                 }.

-export_type([name/0,
              limits/0,
              metadata_key/0,
              metadata/0,
              description/0,
              tag/0,
              unparsed_tags/0,
              tags/0,
              id/0,
              vhost/0,
              vhost_v2/0,
              vhost_v3/0,
              vhost_pattern/0,
              vhost_v2_pattern/0,
              vhost_v3_pattern/0]).

-define(DELETION_PROTECTION_KEY, protected_from_deletion).

-spec new(name(), limits()) -> vhost_v2().
new(Name, Limits) ->
    new(Name, Limits, undefined).

-spec new(name(), limits(), metadata()) -> vhost_v2().
new(Name, Limits, Metadata) ->
    {vhost, Name, Limits, Metadata}.

-spec new(name(), limits(), metadata(), id()) -> vhost_v3().
new(Name, Limits, Metadata, Id) when is_integer(Id), Id > 0 ->
    #vhost{virtual_host = Name, limits = Limits, metadata = Metadata, id = Id}.

-spec record_version_to_use() -> vhost_v2 | vhost_v3.

record_version_to_use() ->
    case rabbit_feature_flags:is_enabled('rabbitmq_5.0.0') of
        true -> ?record_version;
        false -> vhost_v2
    end.

-spec upgrade(vhost()) -> vhost().

upgrade(VHost) when ?is_vhost(VHost) -> VHost.

-spec upgrade_to(vhost_v2 | vhost_v3, vhost()) -> vhost().

upgrade_to(vhost_v2, VHost) when ?is_vhost_v2(VHost) ->
    VHost;
upgrade_to(vhost_v3, VHost) when ?is_vhost_v3(VHost) ->
    VHost.

fields() ->
    fields(?record_version).

fields(vhost_v2) -> [virtual_host, limits, metadata];
fields(?record_version) -> record_info(fields, vhost).

info_keys() ->
    %% note: this reports description and tags separately even though
    %% they are stored in the metadata map. MK.
    [name,
     description,
     tags,
     default_queue_type,
     protected_from_deletion,
     created_at,
     metadata,
     tracing,
     cluster_state].

-spec pattern_match_all() -> [vhost_pattern()].

pattern_match_all() ->
    [{vhost, '_', '_', '_'}, #vhost{_ = '_'}].

-spec pattern_match_names() -> [vhost_pattern()].
pattern_match_names() ->
    [{vhost, '$1', '_', '_'}, #vhost{virtual_host = '$1', _ = '_'}].

%% The fields that both versions have are at the same positions.
-spec get_name(vhost()) -> name().
get_name(VHost) when ?is_vhost(VHost) -> ?vhost_v2_field_name(VHost).

%% Records written by older versions can have `undefined` limits.
-spec get_limits(vhost()) -> limits().
get_limits(VHost) when ?is_vhost(VHost) ->
    case ?vhost_v2_field_limits(VHost) of
        undefined -> [];
        Limits -> Limits
    end.

-spec get_metadata(vhost()) -> metadata().
get_metadata(VHost) when ?is_vhost(VHost) -> ?vhost_v2_field_metadata(VHost).

-spec get_id(vhost()) -> id() | undefined.
get_id(VHost) when ?is_vhost_v3(VHost) -> ?vhost_v3_field_id(VHost);
get_id(VHost) when ?is_vhost_v2(VHost) -> undefined.

-spec id_epoch(id()) -> non_neg_integer().
id_epoch(Id) -> Id bsr 64.

-spec id_time(id()) -> non_neg_integer().
id_time(Id) -> Id band ((1 bsl 64) - 1).

-spec get_description(vhost()) -> binary().
get_description(VHost) when ?is_vhost(VHost) ->
    maps:get(description, get_metadata(VHost), undefined).

-spec get_tags(vhost()) -> [tag()].
get_tags(VHost) when ?is_vhost(VHost) ->
    [tag_to_binary(Tag) || Tag <- maps:get(tags, get_metadata(VHost), [])].

-spec tag_to_binary(atom() | string() | binary()) -> tag().
tag_to_binary(Tag) when is_atom(Tag) ->
    atom_to_binary(Tag, utf8);
tag_to_binary(Tag) when is_list(Tag) ->
    rabbit_data_coercion:to_utf8_binary(Tag);
tag_to_binary(Tag) when is_binary(Tag) ->
    Tag.

-spec get_default_queue_type(vhost()) -> binary() | undefined.
get_default_queue_type(VHost) when ?is_vhost(VHost) ->
    maps:get(default_queue_type, get_metadata(VHost), undefined);
get_default_queue_type(_VHost) ->
    undefined.

set_limits(VHost, Value) when ?is_vhost(VHost) ->
    setelement(3, VHost, Value).

-spec set_metadata(vhost(), metadata()) -> vhost().
set_metadata(VHost, Value) when ?is_vhost(VHost) ->
    setelement(4, VHost, Value).

-spec merge_metadata(vhost(), metadata()) -> vhost().
merge_metadata(VHost, NewVHostMeta) ->
    CurrentVHostMeta = get_metadata(VHost),
    FinalMeta =  maps:merge_with(
                   fun metadata_merger/3, CurrentVHostMeta, NewVHostMeta),
    set_metadata(VHost, FinalMeta).

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

-spec is_protected_from_deletion(vhost()) -> boolean().
is_protected_from_deletion(VHost) ->
    case get_metadata(VHost) of
        Map when map_size(Map) =:= 0 -> false;
        #{?DELETION_PROTECTION_KEY := true} -> true;
        #{?DELETION_PROTECTION_KEY := false} -> false;
        _ -> false
    end.

-spec enable_protection_from_deletion(vhost()) -> vhost().
enable_protection_from_deletion(VHost) ->
    merge_metadata(VHost, #{?DELETION_PROTECTION_KEY => true}).

-spec disable_protection_from_deletion(vhost()) -> vhost().
disable_protection_from_deletion(VHost) ->
    merge_metadata(VHost, #{?DELETION_PROTECTION_KEY => false}).

-spec new_metadata(binary(), [atom()], rabbit_queue_type:queue_type() | 'undefined') -> metadata().
new_metadata(Description, Tags, undefined) ->
    #{description => Description,
        default_queue_type => rabbit_queue_type:default_alias(),
        tags => Tags};
new_metadata(Description, Tags, <<"undefined">>) ->
    %% See rabbitmq/rabbitmq-server#10469
    new_metadata(Description, Tags, undefined);
new_metadata(Description, Tags, <<>>) ->
    %% See rabbitmq/rabbitmq-server#16481
    new_metadata(Description, Tags, undefined);
new_metadata(Description, Tags, null) ->
    %% JSON null, see rabbitmq/rabbitmq-server#10469
    new_metadata(Description, Tags, undefined);
new_metadata(Description, Tags, nil) ->
    %% JSON null (Elixir JSON), see rabbitmq/rabbitmq-server#10469
    new_metadata(Description, Tags, undefined);
new_metadata(Description, Tags, DefaultQueueType) ->
    #{description => Description,
      tags => Tags,
      default_queue_type => DefaultQueueType}.

-spec is_tagged_with(vhost(), atom() | string() | binary()) -> boolean().
is_tagged_with(VHost, Tag) ->
    lists:member(tag_to_binary(Tag), get_tags(VHost)).

-spec to_map(vhost()) -> map().
to_map(VHost) ->
    #{
        name               => get_name(VHost),
        description        => get_description(VHost),
        tags               => get_tags(VHost),
        default_queue_type => get_default_queue_type(VHost),
        metadata           => get_metadata(VHost)
    }.
