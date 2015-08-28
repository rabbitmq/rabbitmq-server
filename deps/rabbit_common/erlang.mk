# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

.PHONY: all app deps search rel docs install-docs check tests clean distclean help erlang-mk

ERLANG_MK_FILENAME := $(realpath $(lastword $(MAKEFILE_LIST)))

ERLANG_MK_VERSION = 1.2.0-657-ga84d0eb

# Core configuration.

PROJECT ?= $(notdir $(CURDIR))
PROJECT := $(strip $(PROJECT))

PROJECT_VERSION ?= rolling

# Verbosity.

V ?= 0

verbose_0 = @
verbose = $(verbose_$(V))

gen_verbose_0 = @echo " GEN   " $@;
gen_verbose = $(gen_verbose_$(V))

# Temporary files directory.

ERLANG_MK_TMP ?= $(CURDIR)/.erlang.mk
export ERLANG_MK_TMP

# "erl" command.

ERL = erl +A0 -noinput -boot start_clean

# Platform detection.
# @todo Add Windows/Cygwin detection eventually.

ifeq ($(PLATFORM),)
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Linux)
PLATFORM = linux
else ifeq ($(UNAME_S),Darwin)
PLATFORM = darwin
else ifeq ($(UNAME_S),SunOS)
PLATFORM = solaris
else ifeq ($(UNAME_S),GNU)
PLATFORM = gnu
else ifeq ($(UNAME_S),FreeBSD)
PLATFORM = freebsd
else ifeq ($(UNAME_S),NetBSD)
PLATFORM = netbsd
else ifeq ($(UNAME_S),OpenBSD)
PLATFORM = openbsd
else
$(error Unable to detect platform. Please open a ticket with the output of uname -a.)
endif

export PLATFORM
endif

# Core targets.

ifneq ($(words $(MAKECMDGOALS)),1)
.NOTPARALLEL:
endif

all:: deps
	$(verbose) $(MAKE) --no-print-directory app rel

# Noop to avoid a Make warning when there's nothing to do.
rel:: app
	$(verbose) echo -n

check:: clean app tests

clean:: clean-crashdump

clean-crashdump:
ifneq ($(wildcard erl_crash.dump),)
	$(gen_verbose) rm -f erl_crash.dump
endif

distclean:: clean

help::
	$(verbose) printf "%s\n" \
		"erlang.mk (version $(ERLANG_MK_VERSION)) is distributed under the terms of the ISC License." \
		"Copyright (c) 2013-2015 Loïc Hoguin <essen@ninenines.eu>" \
		"" \
		"Usage: [V=1] $(MAKE) [-jNUM] [target]..." \
		"" \
		"Core targets:" \
		"  all           Run deps, app and rel targets in that order" \
		"  app           Compile the project" \
		"  deps          Fetch dependencies (if needed) and compile them" \
		"  search q=...  Search for a package in the built-in index" \
		"  rel           Build a release for this project, if applicable" \
		"  docs          Build the documentation for this project" \
		"  install-docs  Install the man pages for this project" \
		"  check         Compile and run all tests and analysis for this project" \
		"  tests         Run the tests for this project" \
		"  clean         Delete temporary and output files from most targets" \
		"  distclean     Delete all temporary and output files" \
		"  help          Display this help and exit" \
		"  erlang-mk     Update erlang.mk to the latest version"

# Core functions.

empty :=
space := $(empty) $(empty)
tab := $(empty)	$(empty)
comma := ,

define newline


endef

define comma_list
$(subst $(space),$(comma),$(strip $(1)))
endef

# Adding erlang.mk to make Erlang scripts who call init:get_plain_arguments() happy.
define erlang
$(ERL) $(2) -pz $(ERLANG_MK_TMP)/rebar/ebin -eval "$(subst $(newline),,$(subst ",\",$(1)))" -- erlang.mk
endef

ifeq ($(shell which wget 2>/dev/null | wc -l), 1)
define core_http_get
	wget --no-check-certificate -O $(1) $(2)|| rm $(1)
endef
else
define core_http_get.erl
	ssl:start(),
	inets:start(),
	case httpc:request(get, {"$(2)", []}, [{autoredirect, true}], []) of
		{ok, {{_, 200, _}, _, Body}} ->
			case file:write_file("$(1)", Body) of
				ok -> ok;
				{error, R1} -> halt(R1)
			end;
		{error, R2} ->
			halt(R2)
	end,
	halt(0).
endef

define core_http_get
	$(call erlang,$(call core_http_get.erl,$(1),$(2)))
endef
endif

core_eq = $(and $(findstring $(1),$(2)),$(findstring $(2),$(1)))

core_find = $(if $(realpath $(1)),$(shell find $(1) '(' $(patsubst %,-name % -o,$(2)) -false ')' -type f))

core_lc = $(subst A,a,$(subst B,b,$(subst C,c,$(subst D,d,$(subst E,e,$(subst F,f,$(subst G,g,$(subst H,h,$(subst I,i,$(subst J,j,$(subst K,k,$(subst L,l,$(subst M,m,$(subst N,n,$(subst O,o,$(subst P,p,$(subst Q,q,$(subst R,r,$(subst S,s,$(subst T,t,$(subst U,u,$(subst V,v,$(subst W,w,$(subst X,x,$(subst Y,y,$(subst Z,z,$(1)))))))))))))))))))))))))))

# @todo On Windows: $(shell dir /B $(1)); make sure to handle when no file exists.
core_ls = $(filter-out $(1),$(shell echo $(1)))

# Automated update.

ERLANG_MK_BUILD_CONFIG ?= build.config
ERLANG_MK_BUILD_DIR ?= .erlang.mk.build

erlang-mk:
	git clone https://github.com/ninenines/erlang.mk $(ERLANG_MK_BUILD_DIR)
	if [ -f $(ERLANG_MK_BUILD_CONFIG) ]; then cp $(ERLANG_MK_BUILD_CONFIG) $(ERLANG_MK_BUILD_DIR); fi
	cd $(ERLANG_MK_BUILD_DIR) && $(MAKE)
	cp $(ERLANG_MK_BUILD_DIR)/erlang.mk ./erlang.mk
	rm -rf $(ERLANG_MK_BUILD_DIR)

# The erlang.mk package index is bundled in the default erlang.mk build.
# Search for the string "copyright" to skip to the rest of the code.

PACKAGES += aberth
pkg_aberth_name = aberth
pkg_aberth_description = Generic BERT-RPC server in Erlang
pkg_aberth_homepage = https://github.com/a13x/aberth
pkg_aberth_fetch = git
pkg_aberth_repo = https://github.com/a13x/aberth
pkg_aberth_commit = master

PACKAGES += active
pkg_active_name = active
pkg_active_description = Active development for Erlang: rebuild and reload source/binary files while the VM is running
pkg_active_homepage = https://github.com/proger/active
pkg_active_fetch = git
pkg_active_repo = https://github.com/proger/active
pkg_active_commit = master

PACKAGES += actordb_core
pkg_actordb_core_name = actordb_core
pkg_actordb_core_description = ActorDB main source
pkg_actordb_core_homepage = http://www.actordb.com/
pkg_actordb_core_fetch = git
pkg_actordb_core_repo = https://github.com/biokoda/actordb_core
pkg_actordb_core_commit = master

PACKAGES += actordb_thrift
pkg_actordb_thrift_name = actordb_thrift
pkg_actordb_thrift_description = Thrift API for ActorDB
pkg_actordb_thrift_homepage = http://www.actordb.com/
pkg_actordb_thrift_fetch = git
pkg_actordb_thrift_repo = https://github.com/biokoda/actordb_thrift
pkg_actordb_thrift_commit = master

PACKAGES += aleppo
pkg_aleppo_name = aleppo
pkg_aleppo_description = Alternative Erlang Pre-Processor
pkg_aleppo_homepage = https://github.com/ErlyORM/aleppo
pkg_aleppo_fetch = git
pkg_aleppo_repo = https://github.com/ErlyORM/aleppo
pkg_aleppo_commit = master

PACKAGES += alog
pkg_alog_name = alog
pkg_alog_description = Simply the best logging framework for Erlang
pkg_alog_homepage = https://github.com/siberian-fast-food/alogger
pkg_alog_fetch = git
pkg_alog_repo = https://github.com/siberian-fast-food/alogger
pkg_alog_commit = master

PACKAGES += amqp_client
pkg_amqp_client_name = amqp_client
pkg_amqp_client_description = RabbitMQ Erlang AMQP client
pkg_amqp_client_homepage = https://www.rabbitmq.com/erlang-client-user-guide.html
pkg_amqp_client_fetch = git
pkg_amqp_client_repo = https://github.com/rabbitmq/rabbitmq-erlang-client.git
pkg_amqp_client_commit = master

PACKAGES += annotations
pkg_annotations_name = annotations
pkg_annotations_description = Simple code instrumentation utilities
pkg_annotations_homepage = https://github.com/hyperthunk/annotations
pkg_annotations_fetch = git
pkg_annotations_repo = https://github.com/hyperthunk/annotations
pkg_annotations_commit = master

PACKAGES += antidote
pkg_antidote_name = antidote
pkg_antidote_description = Large-scale computation without synchronisation
pkg_antidote_homepage = https://syncfree.lip6.fr/
pkg_antidote_fetch = git
pkg_antidote_repo = https://github.com/SyncFree/antidote
pkg_antidote_commit = master

PACKAGES += apns
pkg_apns_name = apns
pkg_apns_description = Apple Push Notification Server for Erlang
pkg_apns_homepage = http://inaka.github.com/apns4erl
pkg_apns_fetch = git
pkg_apns_repo = https://github.com/inaka/apns4erl
pkg_apns_commit = 1.0.4

PACKAGES += azdht
pkg_azdht_name = azdht
pkg_azdht_description = Azureus Distributed Hash Table (DHT) in Erlang
pkg_azdht_homepage = https://github.com/arcusfelis/azdht
pkg_azdht_fetch = git
pkg_azdht_repo = https://github.com/arcusfelis/azdht
pkg_azdht_commit = master

PACKAGES += backoff
pkg_backoff_name = backoff
pkg_backoff_description = Simple exponential backoffs in Erlang
pkg_backoff_homepage = https://github.com/ferd/backoff
pkg_backoff_fetch = git
pkg_backoff_repo = https://github.com/ferd/backoff
pkg_backoff_commit = master

PACKAGES += barrel_tcp
pkg_barrel_tcp_name = barrel_tcp
pkg_barrel_tcp_description = barrel is a generic TCP acceptor pool with low latency in Erlang.
pkg_barrel_tcp_homepage = https://github.com/benoitc-attic/barrel_tcp
pkg_barrel_tcp_fetch = git
pkg_barrel_tcp_repo = https://github.com/benoitc-attic/barrel_tcp
pkg_barrel_tcp_commit = master

PACKAGES += basho_bench
pkg_basho_bench_name = basho_bench
pkg_basho_bench_description = A load-generation and testing tool for basically whatever you can write a returning Erlang function for.
pkg_basho_bench_homepage = https://github.com/basho/basho_bench
pkg_basho_bench_fetch = git
pkg_basho_bench_repo = https://github.com/basho/basho_bench
pkg_basho_bench_commit = master

PACKAGES += bcrypt
pkg_bcrypt_name = bcrypt
pkg_bcrypt_description = Bcrypt Erlang / C library
pkg_bcrypt_homepage = https://github.com/riverrun/branglecrypt
pkg_bcrypt_fetch = git
pkg_bcrypt_repo = https://github.com/riverrun/branglecrypt
pkg_bcrypt_commit = master

PACKAGES += beam
pkg_beam_name = beam
pkg_beam_description = BEAM emulator written in Erlang
pkg_beam_homepage = https://github.com/tonyrog/beam
pkg_beam_fetch = git
pkg_beam_repo = https://github.com/tonyrog/beam
pkg_beam_commit = master

PACKAGES += beanstalk
pkg_beanstalk_name = beanstalk
pkg_beanstalk_description = An Erlang client for beanstalkd
pkg_beanstalk_homepage = https://github.com/tim/erlang-beanstalk
pkg_beanstalk_fetch = git
pkg_beanstalk_repo = https://github.com/tim/erlang-beanstalk
pkg_beanstalk_commit = master

PACKAGES += bear
pkg_bear_name = bear
pkg_bear_description = a set of statistics functions for erlang
pkg_bear_homepage = https://github.com/boundary/bear
pkg_bear_fetch = git
pkg_bear_repo = https://github.com/boundary/bear
pkg_bear_commit = master

PACKAGES += bertconf
pkg_bertconf_name = bertconf
pkg_bertconf_description = Make ETS tables out of statc BERT files that are auto-reloaded
pkg_bertconf_homepage = https://github.com/ferd/bertconf
pkg_bertconf_fetch = git
pkg_bertconf_repo = https://github.com/ferd/bertconf
pkg_bertconf_commit = master

PACKAGES += bifrost
pkg_bifrost_name = bifrost
pkg_bifrost_description = Erlang FTP Server Framework
pkg_bifrost_homepage = https://github.com/thorstadt/bifrost
pkg_bifrost_fetch = git
pkg_bifrost_repo = https://github.com/thorstadt/bifrost
pkg_bifrost_commit = master

PACKAGES += binpp
pkg_binpp_name = binpp
pkg_binpp_description = Erlang Binary Pretty Printer
pkg_binpp_homepage = https://github.com/jtendo/binpp
pkg_binpp_fetch = git
pkg_binpp_repo = https://github.com/jtendo/binpp
pkg_binpp_commit = master

PACKAGES += bisect
pkg_bisect_name = bisect
pkg_bisect_description = Ordered fixed-size binary dictionary in Erlang
pkg_bisect_homepage = https://github.com/knutin/bisect
pkg_bisect_fetch = git
pkg_bisect_repo = https://github.com/knutin/bisect
pkg_bisect_commit = master

PACKAGES += bitcask
pkg_bitcask_name = bitcask
pkg_bitcask_description = because you need another a key/value storage engine
pkg_bitcask_homepage = https://github.com/basho/bitcask
pkg_bitcask_fetch = git
pkg_bitcask_repo = https://github.com/basho/bitcask
pkg_bitcask_commit = master

PACKAGES += bitstore
pkg_bitstore_name = bitstore
pkg_bitstore_description = A document based ontology development environment
pkg_bitstore_homepage = https://github.com/bdionne/bitstore
pkg_bitstore_fetch = git
pkg_bitstore_repo = https://github.com/bdionne/bitstore
pkg_bitstore_commit = master

PACKAGES += bootstrap
pkg_bootstrap_name = bootstrap
pkg_bootstrap_description = A simple, yet powerful Erlang cluster bootstrapping application.
pkg_bootstrap_homepage = https://github.com/schlagert/bootstrap
pkg_bootstrap_fetch = git
pkg_bootstrap_repo = https://github.com/schlagert/bootstrap
pkg_bootstrap_commit = master

PACKAGES += boss
pkg_boss_name = boss
pkg_boss_description = Erlang web MVC, now featuring Comet
pkg_boss_homepage = https://github.com/ChicagoBoss/ChicagoBoss
pkg_boss_fetch = git
pkg_boss_repo = https://github.com/ChicagoBoss/ChicagoBoss
pkg_boss_commit = master

PACKAGES += boss_db
pkg_boss_db_name = boss_db
pkg_boss_db_description = BossDB: a sharded, caching, pooling, evented ORM for Erlang
pkg_boss_db_homepage = https://github.com/ErlyORM/boss_db
pkg_boss_db_fetch = git
pkg_boss_db_repo = https://github.com/ErlyORM/boss_db
pkg_boss_db_commit = master

PACKAGES += bson
pkg_bson_name = bson
pkg_bson_description = BSON documents in Erlang, see bsonspec.org
pkg_bson_homepage = https://github.com/comtihon/bson-erlang
pkg_bson_fetch = git
pkg_bson_repo = https://github.com/comtihon/bson-erlang
pkg_bson_commit = master

PACKAGES += bullet
pkg_bullet_name = bullet
pkg_bullet_description = Simple, reliable, efficient streaming for Cowboy.
pkg_bullet_homepage = http://ninenines.eu
pkg_bullet_fetch = git
pkg_bullet_repo = https://github.com/extend/bullet
pkg_bullet_commit = master

PACKAGES += cache
pkg_cache_name = cache
pkg_cache_description = Erlang in-memory cache
pkg_cache_homepage = https://github.com/fogfish/cache
pkg_cache_fetch = git
pkg_cache_repo = https://github.com/fogfish/cache
pkg_cache_commit = master

PACKAGES += cake
pkg_cake_name = cake
pkg_cake_description = Really simple terminal colorization
pkg_cake_homepage = https://github.com/darach/cake-erl
pkg_cake_fetch = git
pkg_cake_repo = https://github.com/darach/cake-erl
pkg_cake_commit = v0.1.2

PACKAGES += carotene
pkg_carotene_name = carotene
pkg_carotene_description = Real-time server
pkg_carotene_homepage = https://github.com/carotene/carotene
pkg_carotene_fetch = git
pkg_carotene_repo = https://github.com/carotene/carotene
pkg_carotene_commit = master

PACKAGES += cberl
pkg_cberl_name = cberl
pkg_cberl_description = NIF based Erlang bindings for Couchbase
pkg_cberl_homepage = https://github.com/chitika/cberl
pkg_cberl_fetch = git
pkg_cberl_repo = https://github.com/chitika/cberl
pkg_cberl_commit = master

PACKAGES += cecho
pkg_cecho_name = cecho
pkg_cecho_description = An ncurses library for Erlang
pkg_cecho_homepage = https://github.com/mazenharake/cecho
pkg_cecho_fetch = git
pkg_cecho_repo = https://github.com/mazenharake/cecho
pkg_cecho_commit = master

PACKAGES += cferl
pkg_cferl_name = cferl
pkg_cferl_description = Rackspace / Open Stack Cloud Files Erlang Client
pkg_cferl_homepage = https://github.com/ddossot/cferl
pkg_cferl_fetch = git
pkg_cferl_repo = https://github.com/ddossot/cferl
pkg_cferl_commit = master

PACKAGES += chaos_monkey
pkg_chaos_monkey_name = chaos_monkey
pkg_chaos_monkey_description = This is The CHAOS MONKEY.  It will kill your processes.
pkg_chaos_monkey_homepage = https://github.com/dLuna/chaos_monkey
pkg_chaos_monkey_fetch = git
pkg_chaos_monkey_repo = https://github.com/dLuna/chaos_monkey
pkg_chaos_monkey_commit = master

PACKAGES += check_node
pkg_check_node_name = check_node
pkg_check_node_description = Nagios Scripts for monitoring Riak
pkg_check_node_homepage = https://github.com/basho-labs/riak_nagios
pkg_check_node_fetch = git
pkg_check_node_repo = https://github.com/basho-labs/riak_nagios
pkg_check_node_commit = master

PACKAGES += chronos
pkg_chronos_name = chronos
pkg_chronos_description = Timer module for Erlang that makes it easy to abstact time out of the tests.
pkg_chronos_homepage = https://github.com/lehoff/chronos
pkg_chronos_fetch = git
pkg_chronos_repo = https://github.com/lehoff/chronos
pkg_chronos_commit = master

PACKAGES += cl
pkg_cl_name = cl
pkg_cl_description = OpenCL binding for Erlang
pkg_cl_homepage = https://github.com/tonyrog/cl
pkg_cl_fetch = git
pkg_cl_repo = https://github.com/tonyrog/cl
pkg_cl_commit = master

PACKAGES += classifier
pkg_classifier_name = classifier
pkg_classifier_description = An Erlang Bayesian Filter and Text Classifier
pkg_classifier_homepage = https://github.com/inaka/classifier
pkg_classifier_fetch = git
pkg_classifier_repo = https://github.com/inaka/classifier
pkg_classifier_commit = master

PACKAGES += clique
pkg_clique_name = clique
pkg_clique_description = CLI Framework for Erlang
pkg_clique_homepage = https://github.com/basho/clique
pkg_clique_fetch = git
pkg_clique_repo = https://github.com/basho/clique
pkg_clique_commit = develop

PACKAGES += cloudi_core
pkg_cloudi_core_name = cloudi_core
pkg_cloudi_core_description = CloudI internal service runtime
pkg_cloudi_core_homepage = http://cloudi.org/
pkg_cloudi_core_fetch = git
pkg_cloudi_core_repo = https://github.com/CloudI/cloudi_core
pkg_cloudi_core_commit = master

PACKAGES += cloudi_service_api_requests
pkg_cloudi_service_api_requests_name = cloudi_service_api_requests
pkg_cloudi_service_api_requests_description = CloudI Service API requests (JSON-RPC/Erlang-term support)
pkg_cloudi_service_api_requests_homepage = http://cloudi.org/
pkg_cloudi_service_api_requests_fetch = git
pkg_cloudi_service_api_requests_repo = https://github.com/CloudI/cloudi_service_api_requests
pkg_cloudi_service_api_requests_commit = master

PACKAGES += cloudi_service_db
pkg_cloudi_service_db_name = cloudi_service_db
pkg_cloudi_service_db_description = CloudI Database (in-memory/testing/generic)
pkg_cloudi_service_db_homepage = http://cloudi.org/
pkg_cloudi_service_db_fetch = git
pkg_cloudi_service_db_repo = https://github.com/CloudI/cloudi_service_db
pkg_cloudi_service_db_commit = master

PACKAGES += cloudi_service_db_cassandra
pkg_cloudi_service_db_cassandra_name = cloudi_service_db_cassandra
pkg_cloudi_service_db_cassandra_description = Cassandra CloudI Service
pkg_cloudi_service_db_cassandra_homepage = http://cloudi.org/
pkg_cloudi_service_db_cassandra_fetch = git
pkg_cloudi_service_db_cassandra_repo = https://github.com/CloudI/cloudi_service_db_cassandra
pkg_cloudi_service_db_cassandra_commit = master

PACKAGES += cloudi_service_db_cassandra_cql
pkg_cloudi_service_db_cassandra_cql_name = cloudi_service_db_cassandra_cql
pkg_cloudi_service_db_cassandra_cql_description = Cassandra CQL CloudI Service
pkg_cloudi_service_db_cassandra_cql_homepage = http://cloudi.org/
pkg_cloudi_service_db_cassandra_cql_fetch = git
pkg_cloudi_service_db_cassandra_cql_repo = https://github.com/CloudI/cloudi_service_db_cassandra_cql
pkg_cloudi_service_db_cassandra_cql_commit = master

PACKAGES += cloudi_service_db_couchdb
pkg_cloudi_service_db_couchdb_name = cloudi_service_db_couchdb
pkg_cloudi_service_db_couchdb_description = CouchDB CloudI Service
pkg_cloudi_service_db_couchdb_homepage = http://cloudi.org/
pkg_cloudi_service_db_couchdb_fetch = git
pkg_cloudi_service_db_couchdb_repo = https://github.com/CloudI/cloudi_service_db_couchdb
pkg_cloudi_service_db_couchdb_commit = master

PACKAGES += cloudi_service_db_elasticsearch
pkg_cloudi_service_db_elasticsearch_name = cloudi_service_db_elasticsearch
pkg_cloudi_service_db_elasticsearch_description = elasticsearch CloudI Service
pkg_cloudi_service_db_elasticsearch_homepage = http://cloudi.org/
pkg_cloudi_service_db_elasticsearch_fetch = git
pkg_cloudi_service_db_elasticsearch_repo = https://github.com/CloudI/cloudi_service_db_elasticsearch
pkg_cloudi_service_db_elasticsearch_commit = master

PACKAGES += cloudi_service_db_memcached
pkg_cloudi_service_db_memcached_name = cloudi_service_db_memcached
pkg_cloudi_service_db_memcached_description = memcached CloudI Service
pkg_cloudi_service_db_memcached_homepage = http://cloudi.org/
pkg_cloudi_service_db_memcached_fetch = git
pkg_cloudi_service_db_memcached_repo = https://github.com/CloudI/cloudi_service_db_memcached
pkg_cloudi_service_db_memcached_commit = master

PACKAGES += cloudi_service_db_mysql
pkg_cloudi_service_db_mysql_name = cloudi_service_db_mysql
pkg_cloudi_service_db_mysql_description = MySQL CloudI Service
pkg_cloudi_service_db_mysql_homepage = http://cloudi.org/
pkg_cloudi_service_db_mysql_fetch = git
pkg_cloudi_service_db_mysql_repo = https://github.com/CloudI/cloudi_service_db_mysql
pkg_cloudi_service_db_mysql_commit = master

PACKAGES += cloudi_service_db_pgsql
pkg_cloudi_service_db_pgsql_name = cloudi_service_db_pgsql
pkg_cloudi_service_db_pgsql_description = PostgreSQL CloudI Service
pkg_cloudi_service_db_pgsql_homepage = http://cloudi.org/
pkg_cloudi_service_db_pgsql_fetch = git
pkg_cloudi_service_db_pgsql_repo = https://github.com/CloudI/cloudi_service_db_pgsql
pkg_cloudi_service_db_pgsql_commit = master

PACKAGES += cloudi_service_db_riak
pkg_cloudi_service_db_riak_name = cloudi_service_db_riak
pkg_cloudi_service_db_riak_description = Riak CloudI Service
pkg_cloudi_service_db_riak_homepage = http://cloudi.org/
pkg_cloudi_service_db_riak_fetch = git
pkg_cloudi_service_db_riak_repo = https://github.com/CloudI/cloudi_service_db_riak
pkg_cloudi_service_db_riak_commit = master

PACKAGES += cloudi_service_db_tokyotyrant
pkg_cloudi_service_db_tokyotyrant_name = cloudi_service_db_tokyotyrant
pkg_cloudi_service_db_tokyotyrant_description = Tokyo Tyrant CloudI Service
pkg_cloudi_service_db_tokyotyrant_homepage = http://cloudi.org/
pkg_cloudi_service_db_tokyotyrant_fetch = git
pkg_cloudi_service_db_tokyotyrant_repo = https://github.com/CloudI/cloudi_service_db_tokyotyrant
pkg_cloudi_service_db_tokyotyrant_commit = master

PACKAGES += cloudi_service_filesystem
pkg_cloudi_service_filesystem_name = cloudi_service_filesystem
pkg_cloudi_service_filesystem_description = Filesystem CloudI Service
pkg_cloudi_service_filesystem_homepage = http://cloudi.org/
pkg_cloudi_service_filesystem_fetch = git
pkg_cloudi_service_filesystem_repo = https://github.com/CloudI/cloudi_service_filesystem
pkg_cloudi_service_filesystem_commit = master

PACKAGES += cloudi_service_http_client
pkg_cloudi_service_http_client_name = cloudi_service_http_client
pkg_cloudi_service_http_client_description = HTTP client CloudI Service
pkg_cloudi_service_http_client_homepage = http://cloudi.org/
pkg_cloudi_service_http_client_fetch = git
pkg_cloudi_service_http_client_repo = https://github.com/CloudI/cloudi_service_http_client
pkg_cloudi_service_http_client_commit = master

PACKAGES += cloudi_service_http_cowboy
pkg_cloudi_service_http_cowboy_name = cloudi_service_http_cowboy
pkg_cloudi_service_http_cowboy_description = cowboy HTTP/HTTPS CloudI Service
pkg_cloudi_service_http_cowboy_homepage = http://cloudi.org/
pkg_cloudi_service_http_cowboy_fetch = git
pkg_cloudi_service_http_cowboy_repo = https://github.com/CloudI/cloudi_service_http_cowboy
pkg_cloudi_service_http_cowboy_commit = master

PACKAGES += cloudi_service_http_elli
pkg_cloudi_service_http_elli_name = cloudi_service_http_elli
pkg_cloudi_service_http_elli_description = elli HTTP CloudI Service
pkg_cloudi_service_http_elli_homepage = http://cloudi.org/
pkg_cloudi_service_http_elli_fetch = git
pkg_cloudi_service_http_elli_repo = https://github.com/CloudI/cloudi_service_http_elli
pkg_cloudi_service_http_elli_commit = master

PACKAGES += cloudi_service_map_reduce
pkg_cloudi_service_map_reduce_name = cloudi_service_map_reduce
pkg_cloudi_service_map_reduce_description = Map/Reduce CloudI Service
pkg_cloudi_service_map_reduce_homepage = http://cloudi.org/
pkg_cloudi_service_map_reduce_fetch = git
pkg_cloudi_service_map_reduce_repo = https://github.com/CloudI/cloudi_service_map_reduce
pkg_cloudi_service_map_reduce_commit = master

PACKAGES += cloudi_service_oauth1
pkg_cloudi_service_oauth1_name = cloudi_service_oauth1
pkg_cloudi_service_oauth1_description = OAuth v1.0 CloudI Service
pkg_cloudi_service_oauth1_homepage = http://cloudi.org/
pkg_cloudi_service_oauth1_fetch = git
pkg_cloudi_service_oauth1_repo = https://github.com/CloudI/cloudi_service_oauth1
pkg_cloudi_service_oauth1_commit = master

PACKAGES += cloudi_service_queue
pkg_cloudi_service_queue_name = cloudi_service_queue
pkg_cloudi_service_queue_description = Persistent Queue Service
pkg_cloudi_service_queue_homepage = http://cloudi.org/
pkg_cloudi_service_queue_fetch = git
pkg_cloudi_service_queue_repo = https://github.com/CloudI/cloudi_service_queue
pkg_cloudi_service_queue_commit = master

PACKAGES += cloudi_service_quorum
pkg_cloudi_service_quorum_name = cloudi_service_quorum
pkg_cloudi_service_quorum_description = CloudI Quorum Service
pkg_cloudi_service_quorum_homepage = http://cloudi.org/
pkg_cloudi_service_quorum_fetch = git
pkg_cloudi_service_quorum_repo = https://github.com/CloudI/cloudi_service_quorum
pkg_cloudi_service_quorum_commit = master

PACKAGES += cloudi_service_router
pkg_cloudi_service_router_name = cloudi_service_router
pkg_cloudi_service_router_description = CloudI Router Service
pkg_cloudi_service_router_homepage = http://cloudi.org/
pkg_cloudi_service_router_fetch = git
pkg_cloudi_service_router_repo = https://github.com/CloudI/cloudi_service_router
pkg_cloudi_service_router_commit = master

PACKAGES += cloudi_service_tcp
pkg_cloudi_service_tcp_name = cloudi_service_tcp
pkg_cloudi_service_tcp_description = TCP CloudI Service
pkg_cloudi_service_tcp_homepage = http://cloudi.org/
pkg_cloudi_service_tcp_fetch = git
pkg_cloudi_service_tcp_repo = https://github.com/CloudI/cloudi_service_tcp
pkg_cloudi_service_tcp_commit = master

PACKAGES += cloudi_service_timers
pkg_cloudi_service_timers_name = cloudi_service_timers
pkg_cloudi_service_timers_description = Timers CloudI Service
pkg_cloudi_service_timers_homepage = http://cloudi.org/
pkg_cloudi_service_timers_fetch = git
pkg_cloudi_service_timers_repo = https://github.com/CloudI/cloudi_service_timers
pkg_cloudi_service_timers_commit = master

PACKAGES += cloudi_service_udp
pkg_cloudi_service_udp_name = cloudi_service_udp
pkg_cloudi_service_udp_description = UDP CloudI Service
pkg_cloudi_service_udp_homepage = http://cloudi.org/
pkg_cloudi_service_udp_fetch = git
pkg_cloudi_service_udp_repo = https://github.com/CloudI/cloudi_service_udp
pkg_cloudi_service_udp_commit = master

PACKAGES += cloudi_service_validate
pkg_cloudi_service_validate_name = cloudi_service_validate
pkg_cloudi_service_validate_description = CloudI Validate Service
pkg_cloudi_service_validate_homepage = http://cloudi.org/
pkg_cloudi_service_validate_fetch = git
pkg_cloudi_service_validate_repo = https://github.com/CloudI/cloudi_service_validate
pkg_cloudi_service_validate_commit = master

PACKAGES += cloudi_service_zeromq
pkg_cloudi_service_zeromq_name = cloudi_service_zeromq
pkg_cloudi_service_zeromq_description = ZeroMQ CloudI Service
pkg_cloudi_service_zeromq_homepage = http://cloudi.org/
pkg_cloudi_service_zeromq_fetch = git
pkg_cloudi_service_zeromq_repo = https://github.com/CloudI/cloudi_service_zeromq
pkg_cloudi_service_zeromq_commit = master

PACKAGES += cluster_info
pkg_cluster_info_name = cluster_info
pkg_cluster_info_description = Fork of Hibari's nifty cluster_info OTP app
pkg_cluster_info_homepage = https://github.com/basho/cluster_info
pkg_cluster_info_fetch = git
pkg_cluster_info_repo = https://github.com/basho/cluster_info
pkg_cluster_info_commit = master

PACKAGES += color
pkg_color_name = color
pkg_color_description = ANSI colors for your Erlang
pkg_color_homepage = https://github.com/julianduque/erlang-color
pkg_color_fetch = git
pkg_color_repo = https://github.com/julianduque/erlang-color
pkg_color_commit = master

PACKAGES += confetti
pkg_confetti_name = confetti
pkg_confetti_description = Erlang configuration provider / application:get_env/2 on steroids
pkg_confetti_homepage = https://github.com/jtendo/confetti
pkg_confetti_fetch = git
pkg_confetti_repo = https://github.com/jtendo/confetti
pkg_confetti_commit = master

PACKAGES += couch
pkg_couch_name = couch
pkg_couch_description = A embeddable document oriented database compatible with Apache CouchDB
pkg_couch_homepage = https://github.com/benoitc/opencouch
pkg_couch_fetch = git
pkg_couch_repo = https://github.com/benoitc/opencouch
pkg_couch_commit = master

PACKAGES += couchbeam
pkg_couchbeam_name = couchbeam
pkg_couchbeam_description = Apache CouchDB client in Erlang
pkg_couchbeam_homepage = https://github.com/benoitc/couchbeam
pkg_couchbeam_fetch = git
pkg_couchbeam_repo = https://github.com/benoitc/couchbeam
pkg_couchbeam_commit = master

PACKAGES += covertool
pkg_covertool_name = covertool
pkg_covertool_description = Tool to convert Erlang cover data files into Cobertura XML reports
pkg_covertool_homepage = https://github.com/idubrov/covertool
pkg_covertool_fetch = git
pkg_covertool_repo = https://github.com/idubrov/covertool
pkg_covertool_commit = master

PACKAGES += cowboy
pkg_cowboy_name = cowboy
pkg_cowboy_description = Small, fast and modular HTTP server.
pkg_cowboy_homepage = http://ninenines.eu
pkg_cowboy_fetch = git
pkg_cowboy_repo = https://github.com/ninenines/cowboy
pkg_cowboy_commit = 1.0.1

PACKAGES += cowdb
pkg_cowdb_name = cowdb
pkg_cowdb_description = Pure Key/Value database library for Erlang Applications
pkg_cowdb_homepage = https://github.com/refuge/cowdb
pkg_cowdb_fetch = git
pkg_cowdb_repo = https://github.com/refuge/cowdb
pkg_cowdb_commit = master

PACKAGES += cowlib
pkg_cowlib_name = cowlib
pkg_cowlib_description = Support library for manipulating Web protocols.
pkg_cowlib_homepage = http://ninenines.eu
pkg_cowlib_fetch = git
pkg_cowlib_repo = https://github.com/ninenines/cowlib
pkg_cowlib_commit = 1.0.1

PACKAGES += cpg
pkg_cpg_name = cpg
pkg_cpg_description = CloudI Process Groups
pkg_cpg_homepage = https://github.com/okeuday/cpg
pkg_cpg_fetch = git
pkg_cpg_repo = https://github.com/okeuday/cpg
pkg_cpg_commit = master

PACKAGES += cqerl
pkg_cqerl_name = cqerl
pkg_cqerl_description = Native Erlang CQL client for Cassandra
pkg_cqerl_homepage = https://matehat.github.io/cqerl/
pkg_cqerl_fetch = git
pkg_cqerl_repo = https://github.com/matehat/cqerl
pkg_cqerl_commit = master

PACKAGES += cr
pkg_cr_name = cr
pkg_cr_description = Chain Replication
pkg_cr_homepage = https://synrc.com/apps/cr/doc/cr.htm
pkg_cr_fetch = git
pkg_cr_repo = https://github.com/spawnproc/cr
pkg_cr_commit = master

PACKAGES += cuttlefish
pkg_cuttlefish_name = cuttlefish
pkg_cuttlefish_description = never lose your childlike sense of wonder baby cuttlefish, promise me?
pkg_cuttlefish_homepage = https://github.com/basho/cuttlefish
pkg_cuttlefish_fetch = git
pkg_cuttlefish_repo = https://github.com/basho/cuttlefish
pkg_cuttlefish_commit = master

PACKAGES += damocles
pkg_damocles_name = damocles
pkg_damocles_description = Erlang library for generating adversarial network conditions for QAing distributed applications/systems on a single Linux box.
pkg_damocles_homepage = https://github.com/lostcolony/damocles
pkg_damocles_fetch = git
pkg_damocles_repo = https://github.com/lostcolony/damocles
pkg_damocles_commit = master

PACKAGES += debbie
pkg_debbie_name = debbie
pkg_debbie_description = .DEB Built In Erlang
pkg_debbie_homepage = https://github.com/crownedgrouse/debbie
pkg_debbie_fetch = git
pkg_debbie_repo = https://github.com/crownedgrouse/debbie
pkg_debbie_commit = master

PACKAGES += decimal
pkg_decimal_name = decimal
pkg_decimal_description = An Erlang decimal arithmetic library
pkg_decimal_homepage = https://github.com/tim/erlang-decimal
pkg_decimal_fetch = git
pkg_decimal_repo = https://github.com/tim/erlang-decimal
pkg_decimal_commit = master

PACKAGES += detergent
pkg_detergent_name = detergent
pkg_detergent_description = An emulsifying Erlang SOAP library
pkg_detergent_homepage = https://github.com/devinus/detergent
pkg_detergent_fetch = git
pkg_detergent_repo = https://github.com/devinus/detergent
pkg_detergent_commit = master

PACKAGES += detest
pkg_detest_name = detest
pkg_detest_description = Tool for running tests on a cluster of erlang nodes
pkg_detest_homepage = https://github.com/biokoda/detest
pkg_detest_fetch = git
pkg_detest_repo = https://github.com/biokoda/detest
pkg_detest_commit = master

PACKAGES += dh_date
pkg_dh_date_name = dh_date
pkg_dh_date_description = Date formatting / parsing library for erlang
pkg_dh_date_homepage = https://github.com/daleharvey/dh_date
pkg_dh_date_fetch = git
pkg_dh_date_repo = https://github.com/daleharvey/dh_date
pkg_dh_date_commit = master

PACKAGES += dhtcrawler
pkg_dhtcrawler_name = dhtcrawler
pkg_dhtcrawler_description = dhtcrawler is a DHT crawler written in erlang. It can join a DHT network and crawl many P2P torrents.
pkg_dhtcrawler_homepage = https://github.com/kevinlynx/dhtcrawler
pkg_dhtcrawler_fetch = git
pkg_dhtcrawler_repo = https://github.com/kevinlynx/dhtcrawler
pkg_dhtcrawler_commit = master

PACKAGES += dirbusterl
pkg_dirbusterl_name = dirbusterl
pkg_dirbusterl_description = DirBuster successor in Erlang
pkg_dirbusterl_homepage = https://github.com/silentsignal/DirBustErl
pkg_dirbusterl_fetch = git
pkg_dirbusterl_repo = https://github.com/silentsignal/DirBustErl
pkg_dirbusterl_commit = master

PACKAGES += dispcount
pkg_dispcount_name = dispcount
pkg_dispcount_description = Erlang task dispatcher based on ETS counters.
pkg_dispcount_homepage = https://github.com/ferd/dispcount
pkg_dispcount_fetch = git
pkg_dispcount_repo = https://github.com/ferd/dispcount
pkg_dispcount_commit = master

PACKAGES += dlhttpc
pkg_dlhttpc_name = dlhttpc
pkg_dlhttpc_description = dispcount-based lhttpc fork for massive amounts of requests to limited endpoints
pkg_dlhttpc_homepage = https://github.com/ferd/dlhttpc
pkg_dlhttpc_fetch = git
pkg_dlhttpc_repo = https://github.com/ferd/dlhttpc
pkg_dlhttpc_commit = master

PACKAGES += dns
pkg_dns_name = dns
pkg_dns_description = Erlang DNS library
pkg_dns_homepage = https://github.com/aetrion/dns_erlang
pkg_dns_fetch = git
pkg_dns_repo = https://github.com/aetrion/dns_erlang
pkg_dns_commit = master

PACKAGES += dnssd
pkg_dnssd_name = dnssd
pkg_dnssd_description = Erlang interface to Apple's Bonjour D    NS Service Discovery implementation
pkg_dnssd_homepage = https://github.com/benoitc/dnssd_erlang
pkg_dnssd_fetch = git
pkg_dnssd_repo = https://github.com/benoitc/dnssd_erlang
pkg_dnssd_commit = master

PACKAGES += dtl
pkg_dtl_name = dtl
pkg_dtl_description = Django Template Language: A full-featured port of the Django template engine to Erlang.
pkg_dtl_homepage = https://github.com/oinksoft/dtl
pkg_dtl_fetch = git
pkg_dtl_repo = https://github.com/oinksoft/dtl
pkg_dtl_commit = master

PACKAGES += dynamic_compile
pkg_dynamic_compile_name = dynamic_compile
pkg_dynamic_compile_description = compile and load erlang modules from string input
pkg_dynamic_compile_homepage = https://github.com/jkvor/dynamic_compile
pkg_dynamic_compile_fetch = git
pkg_dynamic_compile_repo = https://github.com/jkvor/dynamic_compile
pkg_dynamic_compile_commit = master

PACKAGES += e2
pkg_e2_name = e2
pkg_e2_description = Library to simply writing correct OTP applications.
pkg_e2_homepage = http://e2project.org
pkg_e2_fetch = git
pkg_e2_repo = https://github.com/gar1t/e2
pkg_e2_commit = master

PACKAGES += eamf
pkg_eamf_name = eamf
pkg_eamf_description = eAMF provides Action Message Format (AMF) support for Erlang
pkg_eamf_homepage = https://github.com/mrinalwadhwa/eamf
pkg_eamf_fetch = git
pkg_eamf_repo = https://github.com/mrinalwadhwa/eamf
pkg_eamf_commit = master

PACKAGES += eavro
pkg_eavro_name = eavro
pkg_eavro_description = Apache Avro encoder/decoder
pkg_eavro_homepage = https://github.com/SIfoxDevTeam/eavro
pkg_eavro_fetch = git
pkg_eavro_repo = https://github.com/SIfoxDevTeam/eavro
pkg_eavro_commit = master

PACKAGES += ecapnp
pkg_ecapnp_name = ecapnp
pkg_ecapnp_description = Cap'n Proto library for Erlang
pkg_ecapnp_homepage = https://github.com/kaos/ecapnp
pkg_ecapnp_fetch = git
pkg_ecapnp_repo = https://github.com/kaos/ecapnp
pkg_ecapnp_commit = master

PACKAGES += econfig
pkg_econfig_name = econfig
pkg_econfig_description = simple Erlang config handler using INI files
pkg_econfig_homepage = https://github.com/benoitc/econfig
pkg_econfig_fetch = git
pkg_econfig_repo = https://github.com/benoitc/econfig
pkg_econfig_commit = master

PACKAGES += edate
pkg_edate_name = edate
pkg_edate_description = date manipulation library for erlang
pkg_edate_homepage = https://github.com/dweldon/edate
pkg_edate_fetch = git
pkg_edate_repo = https://github.com/dweldon/edate
pkg_edate_commit = master

PACKAGES += edgar
pkg_edgar_name = edgar
pkg_edgar_description = Erlang Does GNU AR
pkg_edgar_homepage = https://github.com/crownedgrouse/edgar
pkg_edgar_fetch = git
pkg_edgar_repo = https://github.com/crownedgrouse/edgar
pkg_edgar_commit = master

PACKAGES += edis
pkg_edis_name = edis
pkg_edis_description = An Erlang implementation of Redis KV Store
pkg_edis_homepage = http://inaka.github.com/edis/
pkg_edis_fetch = git
pkg_edis_repo = https://github.com/inaka/edis
pkg_edis_commit = master

PACKAGES += edns
pkg_edns_name = edns
pkg_edns_description = Erlang/OTP DNS server
pkg_edns_homepage = https://github.com/hcvst/erlang-dns
pkg_edns_fetch = git
pkg_edns_repo = https://github.com/hcvst/erlang-dns
pkg_edns_commit = master

PACKAGES += edown
pkg_edown_name = edown
pkg_edown_description = EDoc extension for generating Github-flavored Markdown
pkg_edown_homepage = https://github.com/uwiger/edown
pkg_edown_fetch = git
pkg_edown_repo = https://github.com/uwiger/edown
pkg_edown_commit = master

PACKAGES += eep
pkg_eep_name = eep
pkg_eep_description = Erlang Easy Profiling (eep) application provides a way to analyze application performance and call hierarchy
pkg_eep_homepage = https://github.com/virtan/eep
pkg_eep_fetch = git
pkg_eep_repo = https://github.com/virtan/eep
pkg_eep_commit = master

PACKAGES += eep_app
pkg_eep_app_name = eep_app
pkg_eep_app_description = Embedded Event Processing
pkg_eep_app_homepage = https://github.com/darach/eep-erl
pkg_eep_app_fetch = git
pkg_eep_app_repo = https://github.com/darach/eep-erl
pkg_eep_app_commit = master

PACKAGES += efene
pkg_efene_name = efene
pkg_efene_description = Alternative syntax for the Erlang Programming Language focusing on simplicity, ease of use and programmer UX
pkg_efene_homepage = https://github.com/efene/efene
pkg_efene_fetch = git
pkg_efene_repo = https://github.com/efene/efene
pkg_efene_commit = master

PACKAGES += eganglia
pkg_eganglia_name = eganglia
pkg_eganglia_description = Erlang library to interact with Ganglia
pkg_eganglia_homepage = https://github.com/inaka/eganglia
pkg_eganglia_fetch = git
pkg_eganglia_repo = https://github.com/inaka/eganglia
pkg_eganglia_commit = v0.9.1

PACKAGES += egeoip
pkg_egeoip_name = egeoip
pkg_egeoip_description = Erlang IP Geolocation module, currently supporting the MaxMind GeoLite City Database.
pkg_egeoip_homepage = https://github.com/mochi/egeoip
pkg_egeoip_fetch = git
pkg_egeoip_repo = https://github.com/mochi/egeoip
pkg_egeoip_commit = master

PACKAGES += ehsa
pkg_ehsa_name = ehsa
pkg_ehsa_description = Erlang HTTP server basic and digest authentication modules
pkg_ehsa_homepage = https://bitbucket.org/a12n/ehsa
pkg_ehsa_fetch = hg
pkg_ehsa_repo = https://bitbucket.org/a12n/ehsa
pkg_ehsa_commit = 2.0.4

PACKAGES += ej
pkg_ej_name = ej
pkg_ej_description = Helper module for working with Erlang terms representing JSON
pkg_ej_homepage = https://github.com/seth/ej
pkg_ej_fetch = git
pkg_ej_repo = https://github.com/seth/ej
pkg_ej_commit = master

PACKAGES += ejabberd
pkg_ejabberd_name = ejabberd
pkg_ejabberd_description = Robust, ubiquitous and massively scalable Jabber / XMPP Instant Messaging platform
pkg_ejabberd_homepage = https://github.com/processone/ejabberd
pkg_ejabberd_fetch = git
pkg_ejabberd_repo = https://github.com/processone/ejabberd
pkg_ejabberd_commit = master

PACKAGES += ejwt
pkg_ejwt_name = ejwt
pkg_ejwt_description = erlang library for JSON Web Token
pkg_ejwt_homepage = https://github.com/artefactop/ejwt
pkg_ejwt_fetch = git
pkg_ejwt_repo = https://github.com/artefactop/ejwt
pkg_ejwt_commit = master

PACKAGES += ekaf
pkg_ekaf_name = ekaf
pkg_ekaf_description = A minimal, high-performance Kafka client in Erlang.
pkg_ekaf_homepage = https://github.com/helpshift/ekaf
pkg_ekaf_fetch = git
pkg_ekaf_repo = https://github.com/helpshift/ekaf
pkg_ekaf_commit = master

PACKAGES += elarm
pkg_elarm_name = elarm
pkg_elarm_description = Alarm Manager for Erlang.
pkg_elarm_homepage = https://github.com/esl/elarm
pkg_elarm_fetch = git
pkg_elarm_repo = https://github.com/esl/elarm
pkg_elarm_commit = master

PACKAGES += eleveldb
pkg_eleveldb_name = eleveldb
pkg_eleveldb_description = Erlang LevelDB API
pkg_eleveldb_homepage = https://github.com/basho/eleveldb
pkg_eleveldb_fetch = git
pkg_eleveldb_repo = https://github.com/basho/eleveldb
pkg_eleveldb_commit = master

PACKAGES += elli
pkg_elli_name = elli
pkg_elli_description = Simple, robust and performant Erlang web server
pkg_elli_homepage = https://github.com/knutin/elli
pkg_elli_fetch = git
pkg_elli_repo = https://github.com/knutin/elli
pkg_elli_commit = master

PACKAGES += elvis
pkg_elvis_name = elvis
pkg_elvis_description = Erlang Style Reviewer
pkg_elvis_homepage = https://github.com/inaka/elvis
pkg_elvis_fetch = git
pkg_elvis_repo = https://github.com/inaka/elvis
pkg_elvis_commit = 0.2.4

PACKAGES += emagick
pkg_emagick_name = emagick
pkg_emagick_description = Wrapper for Graphics/ImageMagick command line tool.
pkg_emagick_homepage = https://github.com/kivra/emagick
pkg_emagick_fetch = git
pkg_emagick_repo = https://github.com/kivra/emagick
pkg_emagick_commit = master

PACKAGES += emysql
pkg_emysql_name = emysql
pkg_emysql_description = Stable, pure Erlang MySQL driver.
pkg_emysql_homepage = https://github.com/Eonblast/Emysql
pkg_emysql_fetch = git
pkg_emysql_repo = https://github.com/Eonblast/Emysql
pkg_emysql_commit = master

PACKAGES += enm
pkg_enm_name = enm
pkg_enm_description = Erlang driver for nanomsg
pkg_enm_homepage = https://github.com/basho/enm
pkg_enm_fetch = git
pkg_enm_repo = https://github.com/basho/enm
pkg_enm_commit = master

PACKAGES += entop
pkg_entop_name = entop
pkg_entop_description = A top-like tool for monitoring an Erlang node
pkg_entop_homepage = https://github.com/mazenharake/entop
pkg_entop_fetch = git
pkg_entop_repo = https://github.com/mazenharake/entop
pkg_entop_commit = master

PACKAGES += epcap
pkg_epcap_name = epcap
pkg_epcap_description = Erlang packet capture interface using pcap
pkg_epcap_homepage = https://github.com/msantos/epcap
pkg_epcap_fetch = git
pkg_epcap_repo = https://github.com/msantos/epcap
pkg_epcap_commit = master

PACKAGES += eper
pkg_eper_name = eper
pkg_eper_description = Erlang performance and debugging tools.
pkg_eper_homepage = https://github.com/massemanet/eper
pkg_eper_fetch = git
pkg_eper_repo = https://github.com/massemanet/eper
pkg_eper_commit = master

PACKAGES += epgsql
pkg_epgsql_name = epgsql
pkg_epgsql_description = Erlang PostgreSQL client library.
pkg_epgsql_homepage = https://github.com/epgsql/epgsql
pkg_epgsql_fetch = git
pkg_epgsql_repo = https://github.com/epgsql/epgsql
pkg_epgsql_commit = master

PACKAGES += episcina
pkg_episcina_name = episcina
pkg_episcina_description = A simple non intrusive resource pool for connections
pkg_episcina_homepage = https://github.com/erlware/episcina
pkg_episcina_fetch = git
pkg_episcina_repo = https://github.com/erlware/episcina
pkg_episcina_commit = master

PACKAGES += eplot
pkg_eplot_name = eplot
pkg_eplot_description = A plot engine written in erlang.
pkg_eplot_homepage = https://github.com/psyeugenic/eplot
pkg_eplot_fetch = git
pkg_eplot_repo = https://github.com/psyeugenic/eplot
pkg_eplot_commit = master

PACKAGES += epocxy
pkg_epocxy_name = epocxy
pkg_epocxy_description = Erlang Patterns of Concurrency
pkg_epocxy_homepage = https://github.com/duomark/epocxy
pkg_epocxy_fetch = git
pkg_epocxy_repo = https://github.com/duomark/epocxy
pkg_epocxy_commit = master

PACKAGES += epubnub
pkg_epubnub_name = epubnub
pkg_epubnub_description = Erlang PubNub API
pkg_epubnub_homepage = https://github.com/tsloughter/epubnub
pkg_epubnub_fetch = git
pkg_epubnub_repo = https://github.com/tsloughter/epubnub
pkg_epubnub_commit = master

PACKAGES += eqm
pkg_eqm_name = eqm
pkg_eqm_description = Erlang pub sub with supply-demand channels
pkg_eqm_homepage = https://github.com/loucash/eqm
pkg_eqm_fetch = git
pkg_eqm_repo = https://github.com/loucash/eqm
pkg_eqm_commit = master

PACKAGES += eredis
pkg_eredis_name = eredis
pkg_eredis_description = Erlang Redis client
pkg_eredis_homepage = https://github.com/wooga/eredis
pkg_eredis_fetch = git
pkg_eredis_repo = https://github.com/wooga/eredis
pkg_eredis_commit = master

PACKAGES += eredis_pool
pkg_eredis_pool_name = eredis_pool
pkg_eredis_pool_description = eredis_pool is Pool of Redis clients, using eredis and poolboy.
pkg_eredis_pool_homepage = https://github.com/hiroeorz/eredis_pool
pkg_eredis_pool_fetch = git
pkg_eredis_pool_repo = https://github.com/hiroeorz/eredis_pool
pkg_eredis_pool_commit = master

PACKAGES += erl_streams
pkg_erl_streams_name = erl_streams
pkg_erl_streams_description = Streams in Erlang
pkg_erl_streams_homepage = https://github.com/epappas/erl_streams
pkg_erl_streams_fetch = git
pkg_erl_streams_repo = https://github.com/epappas/erl_streams
pkg_erl_streams_commit = master

PACKAGES += erlang_cep
pkg_erlang_cep_name = erlang_cep
pkg_erlang_cep_description = A basic CEP package written in erlang
pkg_erlang_cep_homepage = https://github.com/danmacklin/erlang_cep
pkg_erlang_cep_fetch = git
pkg_erlang_cep_repo = https://github.com/danmacklin/erlang_cep
pkg_erlang_cep_commit = master

PACKAGES += erlang_js
pkg_erlang_js_name = erlang_js
pkg_erlang_js_description = A linked-in driver for Erlang to Mozilla's Spidermonkey Javascript runtime.
pkg_erlang_js_homepage = https://github.com/basho/erlang_js
pkg_erlang_js_fetch = git
pkg_erlang_js_repo = https://github.com/basho/erlang_js
pkg_erlang_js_commit = master

PACKAGES += erlang_localtime
pkg_erlang_localtime_name = erlang_localtime
pkg_erlang_localtime_description = Erlang library for conversion from one local time to another
pkg_erlang_localtime_homepage = https://github.com/dmitryme/erlang_localtime
pkg_erlang_localtime_fetch = git
pkg_erlang_localtime_repo = https://github.com/dmitryme/erlang_localtime
pkg_erlang_localtime_commit = master

PACKAGES += erlang_smtp
pkg_erlang_smtp_name = erlang_smtp
pkg_erlang_smtp_description = Erlang SMTP and POP3 server code.
pkg_erlang_smtp_homepage = https://github.com/tonyg/erlang-smtp
pkg_erlang_smtp_fetch = git
pkg_erlang_smtp_repo = https://github.com/tonyg/erlang-smtp
pkg_erlang_smtp_commit = master

PACKAGES += erlang_term
pkg_erlang_term_name = erlang_term
pkg_erlang_term_description = Erlang Term Info
pkg_erlang_term_homepage = https://github.com/okeuday/erlang_term
pkg_erlang_term_fetch = git
pkg_erlang_term_repo = https://github.com/okeuday/erlang_term
pkg_erlang_term_commit = master

PACKAGES += erlastic_search
pkg_erlastic_search_name = erlastic_search
pkg_erlastic_search_description = An Erlang app for communicating with Elastic Search's rest interface.
pkg_erlastic_search_homepage = https://github.com/tsloughter/erlastic_search
pkg_erlastic_search_fetch = git
pkg_erlastic_search_repo = https://github.com/tsloughter/erlastic_search
pkg_erlastic_search_commit = master

PACKAGES += erlasticsearch
pkg_erlasticsearch_name = erlasticsearch
pkg_erlasticsearch_description = Erlang thrift interface to elastic_search
pkg_erlasticsearch_homepage = https://github.com/dieswaytoofast/erlasticsearch
pkg_erlasticsearch_fetch = git
pkg_erlasticsearch_repo = https://github.com/dieswaytoofast/erlasticsearch
pkg_erlasticsearch_commit = master

PACKAGES += erlbrake
pkg_erlbrake_name = erlbrake
pkg_erlbrake_description = Erlang Airbrake notification client
pkg_erlbrake_homepage = https://github.com/kenpratt/erlbrake
pkg_erlbrake_fetch = git
pkg_erlbrake_repo = https://github.com/kenpratt/erlbrake
pkg_erlbrake_commit = master

PACKAGES += erlcloud
pkg_erlcloud_name = erlcloud
pkg_erlcloud_description = Cloud Computing library for erlang (Amazon EC2, S3, SQS, SimpleDB, Mechanical Turk, ELB)
pkg_erlcloud_homepage = https://github.com/gleber/erlcloud
pkg_erlcloud_fetch = git
pkg_erlcloud_repo = https://github.com/gleber/erlcloud
pkg_erlcloud_commit = master

PACKAGES += erlcron
pkg_erlcron_name = erlcron
pkg_erlcron_description = Erlang cronish system
pkg_erlcron_homepage = https://github.com/erlware/erlcron
pkg_erlcron_fetch = git
pkg_erlcron_repo = https://github.com/erlware/erlcron
pkg_erlcron_commit = master

PACKAGES += erldb
pkg_erldb_name = erldb
pkg_erldb_description = ORM (Object-relational mapping) application implemented in Erlang
pkg_erldb_homepage = http://erldb.org
pkg_erldb_fetch = git
pkg_erldb_repo = https://github.com/erldb/erldb
pkg_erldb_commit = master

PACKAGES += erldis
pkg_erldis_name = erldis
pkg_erldis_description = redis erlang client library
pkg_erldis_homepage = https://github.com/cstar/erldis
pkg_erldis_fetch = git
pkg_erldis_repo = https://github.com/cstar/erldis
pkg_erldis_commit = master

PACKAGES += erldns
pkg_erldns_name = erldns
pkg_erldns_description = DNS server, in erlang.
pkg_erldns_homepage = https://github.com/aetrion/erl-dns
pkg_erldns_fetch = git
pkg_erldns_repo = https://github.com/aetrion/erl-dns
pkg_erldns_commit = master

PACKAGES += erldocker
pkg_erldocker_name = erldocker
pkg_erldocker_description = Docker Remote API client for Erlang
pkg_erldocker_homepage = https://github.com/proger/erldocker
pkg_erldocker_fetch = git
pkg_erldocker_repo = https://github.com/proger/erldocker
pkg_erldocker_commit = master

PACKAGES += erlfsmon
pkg_erlfsmon_name = erlfsmon
pkg_erlfsmon_description = Erlang filesystem event watcher for Linux and OSX
pkg_erlfsmon_homepage = https://github.com/proger/erlfsmon
pkg_erlfsmon_fetch = git
pkg_erlfsmon_repo = https://github.com/proger/erlfsmon
pkg_erlfsmon_commit = master

PACKAGES += erlgit
pkg_erlgit_name = erlgit
pkg_erlgit_description = Erlang convenience wrapper around git executable
pkg_erlgit_homepage = https://github.com/gleber/erlgit
pkg_erlgit_fetch = git
pkg_erlgit_repo = https://github.com/gleber/erlgit
pkg_erlgit_commit = master

PACKAGES += erlguten
pkg_erlguten_name = erlguten
pkg_erlguten_description = ErlGuten is a system for high-quality typesetting, written purely in Erlang.
pkg_erlguten_homepage = https://github.com/richcarl/erlguten
pkg_erlguten_fetch = git
pkg_erlguten_repo = https://github.com/richcarl/erlguten
pkg_erlguten_commit = master

PACKAGES += erlmc
pkg_erlmc_name = erlmc
pkg_erlmc_description = Erlang memcached binary protocol client
pkg_erlmc_homepage = https://github.com/jkvor/erlmc
pkg_erlmc_fetch = git
pkg_erlmc_repo = https://github.com/jkvor/erlmc
pkg_erlmc_commit = master

PACKAGES += erlmongo
pkg_erlmongo_name = erlmongo
pkg_erlmongo_description = Record based Erlang driver for MongoDB with gridfs support
pkg_erlmongo_homepage = https://github.com/SergejJurecko/erlmongo
pkg_erlmongo_fetch = git
pkg_erlmongo_repo = https://github.com/SergejJurecko/erlmongo
pkg_erlmongo_commit = master

PACKAGES += erlog
pkg_erlog_name = erlog
pkg_erlog_description = Prolog interpreter in and for Erlang
pkg_erlog_homepage = https://github.com/rvirding/erlog
pkg_erlog_fetch = git
pkg_erlog_repo = https://github.com/rvirding/erlog
pkg_erlog_commit = master

PACKAGES += erlpass
pkg_erlpass_name = erlpass
pkg_erlpass_description = A library to handle password hashing and changing in a safe manner, independent from any kind of storage whatsoever.
pkg_erlpass_homepage = https://github.com/ferd/erlpass
pkg_erlpass_fetch = git
pkg_erlpass_repo = https://github.com/ferd/erlpass
pkg_erlpass_commit = master

PACKAGES += erlport
pkg_erlport_name = erlport
pkg_erlport_description = ErlPort - connect Erlang to other languages
pkg_erlport_homepage = https://github.com/hdima/erlport
pkg_erlport_fetch = git
pkg_erlport_repo = https://github.com/hdima/erlport
pkg_erlport_commit = master

PACKAGES += erlsh
pkg_erlsh_name = erlsh
pkg_erlsh_description = Erlang shell tools
pkg_erlsh_homepage = https://github.com/proger/erlsh
pkg_erlsh_fetch = git
pkg_erlsh_repo = https://github.com/proger/erlsh
pkg_erlsh_commit = master

PACKAGES += erlsha2
pkg_erlsha2_name = erlsha2
pkg_erlsha2_description = SHA-224, SHA-256, SHA-384, SHA-512 implemented in Erlang NIFs.
pkg_erlsha2_homepage = https://github.com/vinoski/erlsha2
pkg_erlsha2_fetch = git
pkg_erlsha2_repo = https://github.com/vinoski/erlsha2
pkg_erlsha2_commit = master

PACKAGES += erlsom
pkg_erlsom_name = erlsom
pkg_erlsom_description = XML parser for Erlang
pkg_erlsom_homepage = https://github.com/willemdj/erlsom
pkg_erlsom_fetch = git
pkg_erlsom_repo = https://github.com/willemdj/erlsom
pkg_erlsom_commit = master

PACKAGES += erlubi
pkg_erlubi_name = erlubi
pkg_erlubi_description = Ubigraph Erlang Client (and Process Visualizer)
pkg_erlubi_homepage = https://github.com/krestenkrab/erlubi
pkg_erlubi_fetch = git
pkg_erlubi_repo = https://github.com/krestenkrab/erlubi
pkg_erlubi_commit = master

PACKAGES += erlvolt
pkg_erlvolt_name = erlvolt
pkg_erlvolt_description = VoltDB Erlang Client Driver
pkg_erlvolt_homepage = https://github.com/VoltDB/voltdb-client-erlang
pkg_erlvolt_fetch = git
pkg_erlvolt_repo = https://github.com/VoltDB/voltdb-client-erlang
pkg_erlvolt_commit = master

PACKAGES += erlware_commons
pkg_erlware_commons_name = erlware_commons
pkg_erlware_commons_description = Erlware Commons is an Erlware project focused on all aspects of reusable Erlang components.
pkg_erlware_commons_homepage = https://github.com/erlware/erlware_commons
pkg_erlware_commons_fetch = git
pkg_erlware_commons_repo = https://github.com/erlware/erlware_commons
pkg_erlware_commons_commit = master

PACKAGES += erlydtl
pkg_erlydtl_name = erlydtl
pkg_erlydtl_description = Django Template Language for Erlang.
pkg_erlydtl_homepage = https://github.com/erlydtl/erlydtl
pkg_erlydtl_fetch = git
pkg_erlydtl_repo = https://github.com/erlydtl/erlydtl
pkg_erlydtl_commit = master

PACKAGES += errd
pkg_errd_name = errd
pkg_errd_description = Erlang RRDTool library
pkg_errd_homepage = https://github.com/archaelus/errd
pkg_errd_fetch = git
pkg_errd_repo = https://github.com/archaelus/errd
pkg_errd_commit = master

PACKAGES += erserve
pkg_erserve_name = erserve
pkg_erserve_description = Erlang/Rserve communication interface
pkg_erserve_homepage = https://github.com/del/erserve
pkg_erserve_fetch = git
pkg_erserve_repo = https://github.com/del/erserve
pkg_erserve_commit = master

PACKAGES += erwa
pkg_erwa_name = erwa
pkg_erwa_description = A WAMP router and client written in Erlang.
pkg_erwa_homepage = https://github.com/bwegh/erwa
pkg_erwa_fetch = git
pkg_erwa_repo = https://github.com/bwegh/erwa
pkg_erwa_commit = 0.1.1

PACKAGES += espec
pkg_espec_name = espec
pkg_espec_description = ESpec: Behaviour driven development framework for Erlang
pkg_espec_homepage = https://github.com/lucaspiller/espec
pkg_espec_fetch = git
pkg_espec_repo = https://github.com/lucaspiller/espec
pkg_espec_commit = master

PACKAGES += estatsd
pkg_estatsd_name = estatsd
pkg_estatsd_description = Erlang stats aggregation app that periodically flushes data to graphite
pkg_estatsd_homepage = https://github.com/RJ/estatsd
pkg_estatsd_fetch = git
pkg_estatsd_repo = https://github.com/RJ/estatsd
pkg_estatsd_commit = master

PACKAGES += etap
pkg_etap_name = etap
pkg_etap_description = etap is a simple erlang testing library that provides TAP compliant output.
pkg_etap_homepage = https://github.com/ngerakines/etap
pkg_etap_fetch = git
pkg_etap_repo = https://github.com/ngerakines/etap
pkg_etap_commit = master

PACKAGES += etest
pkg_etest_name = etest
pkg_etest_description = A lightweight, convention over configuration test framework for Erlang
pkg_etest_homepage = https://github.com/wooga/etest
pkg_etest_fetch = git
pkg_etest_repo = https://github.com/wooga/etest
pkg_etest_commit = master

PACKAGES += etest_http
pkg_etest_http_name = etest_http
pkg_etest_http_description = etest Assertions around HTTP (client-side)
pkg_etest_http_homepage = https://github.com/wooga/etest_http
pkg_etest_http_fetch = git
pkg_etest_http_repo = https://github.com/wooga/etest_http
pkg_etest_http_commit = master

PACKAGES += etoml
pkg_etoml_name = etoml
pkg_etoml_description = TOML language erlang parser
pkg_etoml_homepage = https://github.com/kalta/etoml
pkg_etoml_fetch = git
pkg_etoml_repo = https://github.com/kalta/etoml
pkg_etoml_commit = master

PACKAGES += eunit
pkg_eunit_name = eunit
pkg_eunit_description = The EUnit lightweight unit testing framework for Erlang - this is the canonical development repository.
pkg_eunit_homepage = https://github.com/richcarl/eunit
pkg_eunit_fetch = git
pkg_eunit_repo = https://github.com/richcarl/eunit
pkg_eunit_commit = master

PACKAGES += eunit_formatters
pkg_eunit_formatters_name = eunit_formatters
pkg_eunit_formatters_description = Because eunit's output sucks. Let's make it better.
pkg_eunit_formatters_homepage = https://github.com/seancribbs/eunit_formatters
pkg_eunit_formatters_fetch = git
pkg_eunit_formatters_repo = https://github.com/seancribbs/eunit_formatters
pkg_eunit_formatters_commit = master

PACKAGES += euthanasia
pkg_euthanasia_name = euthanasia
pkg_euthanasia_description = Merciful killer for your Erlang processes
pkg_euthanasia_homepage = https://github.com/doubleyou/euthanasia
pkg_euthanasia_fetch = git
pkg_euthanasia_repo = https://github.com/doubleyou/euthanasia
pkg_euthanasia_commit = master

PACKAGES += evum
pkg_evum_name = evum
pkg_evum_description = Spawn Linux VMs as Erlang processes in the Erlang VM
pkg_evum_homepage = https://github.com/msantos/evum
pkg_evum_fetch = git
pkg_evum_repo = https://github.com/msantos/evum
pkg_evum_commit = master

PACKAGES += exec
pkg_exec_name = exec
pkg_exec_description = Execute and control OS processes from Erlang/OTP.
pkg_exec_homepage = http://saleyn.github.com/erlexec
pkg_exec_fetch = git
pkg_exec_repo = https://github.com/saleyn/erlexec
pkg_exec_commit = master

PACKAGES += exml
pkg_exml_name = exml
pkg_exml_description = XML parsing library in Erlang
pkg_exml_homepage = https://github.com/paulgray/exml
pkg_exml_fetch = git
pkg_exml_repo = https://github.com/paulgray/exml
pkg_exml_commit = master

PACKAGES += exometer
pkg_exometer_name = exometer
pkg_exometer_description = Basic measurement objects and probe behavior
pkg_exometer_homepage = https://github.com/Feuerlabs/exometer
pkg_exometer_fetch = git
pkg_exometer_repo = https://github.com/Feuerlabs/exometer
pkg_exometer_commit = 1.2

PACKAGES += exs1024
pkg_exs1024_name = exs1024
pkg_exs1024_description = Xorshift1024star pseudo random number generator for Erlang.
pkg_exs1024_homepage = https://github.com/jj1bdx/exs1024
pkg_exs1024_fetch = git
pkg_exs1024_repo = https://github.com/jj1bdx/exs1024
pkg_exs1024_commit = master

PACKAGES += exs64
pkg_exs64_name = exs64
pkg_exs64_description = Xorshift64star pseudo random number generator for Erlang.
pkg_exs64_homepage = https://github.com/jj1bdx/exs64
pkg_exs64_fetch = git
pkg_exs64_repo = https://github.com/jj1bdx/exs64
pkg_exs64_commit = master

PACKAGES += exsplus116
pkg_exsplus116_name = exsplus116
pkg_exsplus116_description = Xorshift116plus for Erlang
pkg_exsplus116_homepage = https://github.com/jj1bdx/exsplus116
pkg_exsplus116_fetch = git
pkg_exsplus116_repo = https://github.com/jj1bdx/exsplus116
pkg_exsplus116_commit = master

PACKAGES += exsplus128
pkg_exsplus128_name = exsplus128
pkg_exsplus128_description = Xorshift128plus pseudo random number generator for Erlang.
pkg_exsplus128_homepage = https://github.com/jj1bdx/exsplus128
pkg_exsplus128_fetch = git
pkg_exsplus128_repo = https://github.com/jj1bdx/exsplus128
pkg_exsplus128_commit = master

PACKAGES += ezmq
pkg_ezmq_name = ezmq
pkg_ezmq_description = zMQ implemented in Erlang
pkg_ezmq_homepage = https://github.com/RoadRunnr/ezmq
pkg_ezmq_fetch = git
pkg_ezmq_repo = https://github.com/RoadRunnr/ezmq
pkg_ezmq_commit = master

PACKAGES += ezmtp
pkg_ezmtp_name = ezmtp
pkg_ezmtp_description = ZMTP protocol in pure Erlang.
pkg_ezmtp_homepage = https://github.com/a13x/ezmtp
pkg_ezmtp_fetch = git
pkg_ezmtp_repo = https://github.com/a13x/ezmtp
pkg_ezmtp_commit = master

PACKAGES += fast_disk_log
pkg_fast_disk_log_name = fast_disk_log
pkg_fast_disk_log_description = Pool-based asynchronous Erlang disk logger
pkg_fast_disk_log_homepage = https://github.com/lpgauth/fast_disk_log
pkg_fast_disk_log_fetch = git
pkg_fast_disk_log_repo = https://github.com/lpgauth/fast_disk_log
pkg_fast_disk_log_commit = master

PACKAGES += feeder
pkg_feeder_name = feeder
pkg_feeder_description = Stream parse RSS and Atom formatted XML feeds.
pkg_feeder_homepage = https://github.com/michaelnisi/feeder
pkg_feeder_fetch = git
pkg_feeder_repo = https://github.com/michaelnisi/feeder
pkg_feeder_commit = v1.4.6

PACKAGES += fix
pkg_fix_name = fix
pkg_fix_description = http://fixprotocol.org/ implementation.
pkg_fix_homepage = https://github.com/maxlapshin/fix
pkg_fix_fetch = git
pkg_fix_repo = https://github.com/maxlapshin/fix
pkg_fix_commit = master

PACKAGES += flower
pkg_flower_name = flower
pkg_flower_description = FlowER - a Erlang OpenFlow development platform
pkg_flower_homepage = https://github.com/travelping/flower
pkg_flower_fetch = git
pkg_flower_repo = https://github.com/travelping/flower
pkg_flower_commit = master

PACKAGES += fn
pkg_fn_name = fn
pkg_fn_description = Function utilities for Erlang
pkg_fn_homepage = https://github.com/reiddraper/fn
pkg_fn_fetch = git
pkg_fn_repo = https://github.com/reiddraper/fn
pkg_fn_commit = master

PACKAGES += folsom
pkg_folsom_name = folsom
pkg_folsom_description = Expose Erlang Events and Metrics
pkg_folsom_homepage = https://github.com/boundary/folsom
pkg_folsom_fetch = git
pkg_folsom_repo = https://github.com/boundary/folsom
pkg_folsom_commit = master

PACKAGES += folsom_cowboy
pkg_folsom_cowboy_name = folsom_cowboy
pkg_folsom_cowboy_description = A Cowboy based Folsom HTTP Wrapper.
pkg_folsom_cowboy_homepage = https://github.com/boundary/folsom_cowboy
pkg_folsom_cowboy_fetch = git
pkg_folsom_cowboy_repo = https://github.com/boundary/folsom_cowboy
pkg_folsom_cowboy_commit = master

PACKAGES += folsomite
pkg_folsomite_name = folsomite
pkg_folsomite_description = blow up your graphite / riemann server with folsom metrics
pkg_folsomite_homepage = https://github.com/campanja/folsomite
pkg_folsomite_fetch = git
pkg_folsomite_repo = https://github.com/campanja/folsomite
pkg_folsomite_commit = master

PACKAGES += fs
pkg_fs_name = fs
pkg_fs_description = Erlang FileSystem Listener
pkg_fs_homepage = https://github.com/synrc/fs
pkg_fs_fetch = git
pkg_fs_repo = https://github.com/synrc/fs
pkg_fs_commit = master

PACKAGES += fuse
pkg_fuse_name = fuse
pkg_fuse_description = A Circuit Breaker for Erlang
pkg_fuse_homepage = https://github.com/jlouis/fuse
pkg_fuse_fetch = git
pkg_fuse_repo = https://github.com/jlouis/fuse
pkg_fuse_commit = master

PACKAGES += gcm
pkg_gcm_name = gcm
pkg_gcm_description = An Erlang application for Google Cloud Messaging
pkg_gcm_homepage = https://github.com/pdincau/gcm-erlang
pkg_gcm_fetch = git
pkg_gcm_repo = https://github.com/pdincau/gcm-erlang
pkg_gcm_commit = master

PACKAGES += gcprof
pkg_gcprof_name = gcprof
pkg_gcprof_description = Garbage Collection profiler for Erlang
pkg_gcprof_homepage = https://github.com/knutin/gcprof
pkg_gcprof_fetch = git
pkg_gcprof_repo = https://github.com/knutin/gcprof
pkg_gcprof_commit = master

PACKAGES += geas
pkg_geas_name = geas
pkg_geas_description = Guess Erlang Application Scattering
pkg_geas_homepage = https://github.com/crownedgrouse/geas
pkg_geas_fetch = git
pkg_geas_repo = https://github.com/crownedgrouse/geas
pkg_geas_commit = master

PACKAGES += geef
pkg_geef_name = geef
pkg_geef_description = Git NEEEEF (Erlang NIF)
pkg_geef_homepage = https://github.com/carlosmn/geef
pkg_geef_fetch = git
pkg_geef_repo = https://github.com/carlosmn/geef
pkg_geef_commit = master

PACKAGES += gen_cycle
pkg_gen_cycle_name = gen_cycle
pkg_gen_cycle_description = Simple, generic OTP behaviour for recurring tasks
pkg_gen_cycle_homepage = https://github.com/aerosol/gen_cycle
pkg_gen_cycle_fetch = git
pkg_gen_cycle_repo = https://github.com/aerosol/gen_cycle
pkg_gen_cycle_commit = develop

PACKAGES += gen_icmp
pkg_gen_icmp_name = gen_icmp
pkg_gen_icmp_description = Erlang interface to ICMP sockets
pkg_gen_icmp_homepage = https://github.com/msantos/gen_icmp
pkg_gen_icmp_fetch = git
pkg_gen_icmp_repo = https://github.com/msantos/gen_icmp
pkg_gen_icmp_commit = master

PACKAGES += gen_nb_server
pkg_gen_nb_server_name = gen_nb_server
pkg_gen_nb_server_description = OTP behavior for writing non-blocking servers
pkg_gen_nb_server_homepage = https://github.com/kevsmith/gen_nb_server
pkg_gen_nb_server_fetch = git
pkg_gen_nb_server_repo = https://github.com/kevsmith/gen_nb_server
pkg_gen_nb_server_commit = master

PACKAGES += gen_paxos
pkg_gen_paxos_name = gen_paxos
pkg_gen_paxos_description = An Erlang/OTP-style implementation of the PAXOS distributed consensus protocol
pkg_gen_paxos_homepage = https://github.com/gburd/gen_paxos
pkg_gen_paxos_fetch = git
pkg_gen_paxos_repo = https://github.com/gburd/gen_paxos
pkg_gen_paxos_commit = master

PACKAGES += gen_smtp
pkg_gen_smtp_name = gen_smtp
pkg_gen_smtp_description = A generic Erlang SMTP server and client that can be extended via callback modules
pkg_gen_smtp_homepage = https://github.com/Vagabond/gen_smtp
pkg_gen_smtp_fetch = git
pkg_gen_smtp_repo = https://github.com/Vagabond/gen_smtp
pkg_gen_smtp_commit = master

PACKAGES += gen_tracker
pkg_gen_tracker_name = gen_tracker
pkg_gen_tracker_description = supervisor with ets handling of children and their metadata
pkg_gen_tracker_homepage = https://github.com/erlyvideo/gen_tracker
pkg_gen_tracker_fetch = git
pkg_gen_tracker_repo = https://github.com/erlyvideo/gen_tracker
pkg_gen_tracker_commit = master

PACKAGES += gen_unix
pkg_gen_unix_name = gen_unix
pkg_gen_unix_description = Erlang Unix socket interface
pkg_gen_unix_homepage = https://github.com/msantos/gen_unix
pkg_gen_unix_fetch = git
pkg_gen_unix_repo = https://github.com/msantos/gen_unix
pkg_gen_unix_commit = master

PACKAGES += getopt
pkg_getopt_name = getopt
pkg_getopt_description = Module to parse command line arguments using the GNU getopt syntax
pkg_getopt_homepage = https://github.com/jcomellas/getopt
pkg_getopt_fetch = git
pkg_getopt_repo = https://github.com/jcomellas/getopt
pkg_getopt_commit = master

PACKAGES += gettext
pkg_gettext_name = gettext
pkg_gettext_description = Erlang internationalization library.
pkg_gettext_homepage = https://github.com/etnt/gettext
pkg_gettext_fetch = git
pkg_gettext_repo = https://github.com/etnt/gettext
pkg_gettext_commit = master

PACKAGES += giallo
pkg_giallo_name = giallo
pkg_giallo_description = Small and flexible web framework on top of Cowboy
pkg_giallo_homepage = https://github.com/kivra/giallo
pkg_giallo_fetch = git
pkg_giallo_repo = https://github.com/kivra/giallo
pkg_giallo_commit = master

PACKAGES += gin
pkg_gin_name = gin
pkg_gin_description = The guards  and  for Erlang parse_transform
pkg_gin_homepage = https://github.com/mad-cocktail/gin
pkg_gin_fetch = git
pkg_gin_repo = https://github.com/mad-cocktail/gin
pkg_gin_commit = master

PACKAGES += gitty
pkg_gitty_name = gitty
pkg_gitty_description = Git access in erlang
pkg_gitty_homepage = https://github.com/maxlapshin/gitty
pkg_gitty_fetch = git
pkg_gitty_repo = https://github.com/maxlapshin/gitty
pkg_gitty_commit = master

PACKAGES += gold_fever
pkg_gold_fever_name = gold_fever
pkg_gold_fever_description = A Treasure Hunt for Erlangers
pkg_gold_fever_homepage = https://github.com/inaka/gold_fever
pkg_gold_fever_fetch = git
pkg_gold_fever_repo = https://github.com/inaka/gold_fever
pkg_gold_fever_commit = master

PACKAGES += gossiperl
pkg_gossiperl_name = gossiperl
pkg_gossiperl_description = Gossip middleware in Erlang
pkg_gossiperl_homepage = http://gossiperl.com/
pkg_gossiperl_fetch = git
pkg_gossiperl_repo = https://github.com/gossiperl/gossiperl
pkg_gossiperl_commit = master

PACKAGES += gpb
pkg_gpb_name = gpb
pkg_gpb_description = A Google Protobuf implementation for Erlang
pkg_gpb_homepage = https://github.com/tomas-abrahamsson/gpb
pkg_gpb_fetch = git
pkg_gpb_repo = https://github.com/tomas-abrahamsson/gpb
pkg_gpb_commit = master

PACKAGES += gproc
pkg_gproc_name = gproc
pkg_gproc_description = Extended process registry for Erlang
pkg_gproc_homepage = https://github.com/uwiger/gproc
pkg_gproc_fetch = git
pkg_gproc_repo = https://github.com/uwiger/gproc
pkg_gproc_commit = master

PACKAGES += grapherl
pkg_grapherl_name = grapherl
pkg_grapherl_description = Create graphs of Erlang systems and programs
pkg_grapherl_homepage = https://github.com/eproxus/grapherl
pkg_grapherl_fetch = git
pkg_grapherl_repo = https://github.com/eproxus/grapherl
pkg_grapherl_commit = master

PACKAGES += gun
pkg_gun_name = gun
pkg_gun_description = Asynchronous SPDY, HTTP and Websocket client written in Erlang.
pkg_gun_homepage = http//ninenines.eu
pkg_gun_fetch = git
pkg_gun_repo = https://github.com/ninenines/gun
pkg_gun_commit = master

PACKAGES += gut
pkg_gut_name = gut
pkg_gut_description = gut is a template printing, aka scaffolding, tool for Erlang. Like rails generate or yeoman
pkg_gut_homepage = https://github.com/unbalancedparentheses/gut
pkg_gut_fetch = git
pkg_gut_repo = https://github.com/unbalancedparentheses/gut
pkg_gut_commit = master

PACKAGES += hackney
pkg_hackney_name = hackney
pkg_hackney_description = simple HTTP client in Erlang
pkg_hackney_homepage = https://github.com/benoitc/hackney
pkg_hackney_fetch = git
pkg_hackney_repo = https://github.com/benoitc/hackney
pkg_hackney_commit = master

PACKAGES += hamcrest
pkg_hamcrest_name = hamcrest
pkg_hamcrest_description = Erlang port of Hamcrest
pkg_hamcrest_homepage = https://github.com/hyperthunk/hamcrest-erlang
pkg_hamcrest_fetch = git
pkg_hamcrest_repo = https://github.com/hyperthunk/hamcrest-erlang
pkg_hamcrest_commit = master

PACKAGES += hanoidb
pkg_hanoidb_name = hanoidb
pkg_hanoidb_description = Erlang LSM BTree Storage
pkg_hanoidb_homepage = https://github.com/krestenkrab/hanoidb
pkg_hanoidb_fetch = git
pkg_hanoidb_repo = https://github.com/krestenkrab/hanoidb
pkg_hanoidb_commit = master

PACKAGES += hottub
pkg_hottub_name = hottub
pkg_hottub_description = Permanent Erlang Worker Pool
pkg_hottub_homepage = https://github.com/bfrog/hottub
pkg_hottub_fetch = git
pkg_hottub_repo = https://github.com/bfrog/hottub
pkg_hottub_commit = master

PACKAGES += hpack
pkg_hpack_name = hpack
pkg_hpack_description = HPACK Implementation for Erlang
pkg_hpack_homepage = https://github.com/joedevivo/hpack
pkg_hpack_fetch = git
pkg_hpack_repo = https://github.com/joedevivo/hpack
pkg_hpack_commit = master

PACKAGES += hyper
pkg_hyper_name = hyper
pkg_hyper_description = Erlang implementation of HyperLogLog
pkg_hyper_homepage = https://github.com/GameAnalytics/hyper
pkg_hyper_fetch = git
pkg_hyper_repo = https://github.com/GameAnalytics/hyper
pkg_hyper_commit = master

PACKAGES += ibrowse
pkg_ibrowse_name = ibrowse
pkg_ibrowse_description = Erlang HTTP client
pkg_ibrowse_homepage = https://github.com/cmullaparthi/ibrowse
pkg_ibrowse_fetch = git
pkg_ibrowse_repo = https://github.com/cmullaparthi/ibrowse
pkg_ibrowse_commit = v4.1.1

PACKAGES += ierlang
pkg_ierlang_name = ierlang
pkg_ierlang_description = An Erlang language kernel for IPython.
pkg_ierlang_homepage = https://github.com/robbielynch/ierlang
pkg_ierlang_fetch = git
pkg_ierlang_repo = https://github.com/robbielynch/ierlang
pkg_ierlang_commit = master

PACKAGES += iota
pkg_iota_name = iota
pkg_iota_description = iota (Inter-dependency Objective Testing Apparatus) - a tool to enforce clean separation of responsibilities in Erlang code
pkg_iota_homepage = https://github.com/jpgneves/iota
pkg_iota_fetch = git
pkg_iota_repo = https://github.com/jpgneves/iota
pkg_iota_commit = master

PACKAGES += irc_lib
pkg_irc_lib_name = irc_lib
pkg_irc_lib_description = Erlang irc client library
pkg_irc_lib_homepage = https://github.com/OtpChatBot/irc_lib
pkg_irc_lib_fetch = git
pkg_irc_lib_repo = https://github.com/OtpChatBot/irc_lib
pkg_irc_lib_commit = master

PACKAGES += ircd
pkg_ircd_name = ircd
pkg_ircd_description = A pluggable IRC daemon application/library for Erlang.
pkg_ircd_homepage = https://github.com/tonyg/erlang-ircd
pkg_ircd_fetch = git
pkg_ircd_repo = https://github.com/tonyg/erlang-ircd
pkg_ircd_commit = master

PACKAGES += iris
pkg_iris_name = iris
pkg_iris_description = Iris Erlang binding
pkg_iris_homepage = https://github.com/project-iris/iris-erl
pkg_iris_fetch = git
pkg_iris_repo = https://github.com/project-iris/iris-erl
pkg_iris_commit = master

PACKAGES += iso8601
pkg_iso8601_name = iso8601
pkg_iso8601_description = Erlang ISO 8601 date formatter/parser
pkg_iso8601_homepage = https://github.com/seansawyer/erlang_iso8601
pkg_iso8601_fetch = git
pkg_iso8601_repo = https://github.com/seansawyer/erlang_iso8601
pkg_iso8601_commit = master

PACKAGES += itweet
pkg_itweet_name = itweet
pkg_itweet_description = Twitter Stream API on ibrowse
pkg_itweet_homepage = http://inaka.github.com/itweet/
pkg_itweet_fetch = git
pkg_itweet_repo = https://github.com/inaka/itweet
pkg_itweet_commit = v2.0

PACKAGES += jerg
pkg_jerg_name = jerg
pkg_jerg_description = JSON Schema to Erlang Records Generator
pkg_jerg_homepage = https://github.com/ddossot/jerg
pkg_jerg_fetch = git
pkg_jerg_repo = https://github.com/ddossot/jerg
pkg_jerg_commit = master

PACKAGES += jesse
pkg_jesse_name = jesse
pkg_jesse_description = jesse (JSon Schema Erlang) is an implementation of a json schema validator for Erlang.
pkg_jesse_homepage = https://github.com/klarna/jesse
pkg_jesse_fetch = git
pkg_jesse_repo = https://github.com/klarna/jesse
pkg_jesse_commit = master

PACKAGES += jiffy
pkg_jiffy_name = jiffy
pkg_jiffy_description = JSON NIFs for Erlang.
pkg_jiffy_homepage = https://github.com/davisp/jiffy
pkg_jiffy_fetch = git
pkg_jiffy_repo = https://github.com/davisp/jiffy
pkg_jiffy_commit = master

PACKAGES += jiffy_v
pkg_jiffy_v_name = jiffy_v
pkg_jiffy_v_description = JSON validation utility
pkg_jiffy_v_homepage = https://github.com/shizzard/jiffy-v
pkg_jiffy_v_fetch = git
pkg_jiffy_v_repo = https://github.com/shizzard/jiffy-v
pkg_jiffy_v_commit = 0.3.3

PACKAGES += jobs
pkg_jobs_name = jobs
pkg_jobs_description = a Job scheduler for load regulation
pkg_jobs_homepage = https://github.com/esl/jobs
pkg_jobs_fetch = git
pkg_jobs_repo = https://github.com/esl/jobs
pkg_jobs_commit = 0.3

PACKAGES += joxa
pkg_joxa_name = joxa
pkg_joxa_description = A Modern Lisp for the Erlang VM
pkg_joxa_homepage = https://github.com/joxa/joxa
pkg_joxa_fetch = git
pkg_joxa_repo = https://github.com/joxa/joxa
pkg_joxa_commit = master

PACKAGES += json
pkg_json_name = json
pkg_json_description = a high level json library for erlang (17.0+)
pkg_json_homepage = https://github.com/talentdeficit/json
pkg_json_fetch = git
pkg_json_repo = https://github.com/talentdeficit/json
pkg_json_commit = master

PACKAGES += json_rec
pkg_json_rec_name = json_rec
pkg_json_rec_description = JSON to erlang record
pkg_json_rec_homepage = https://github.com/justinkirby/json_rec
pkg_json_rec_fetch = git
pkg_json_rec_repo = https://github.com/justinkirby/json_rec
pkg_json_rec_commit = master

PACKAGES += jsonerl
pkg_jsonerl_name = jsonerl
pkg_jsonerl_description = yet another but slightly different erlang <-> json encoder/decoder
pkg_jsonerl_homepage = https://github.com/lambder/jsonerl
pkg_jsonerl_fetch = git
pkg_jsonerl_repo = https://github.com/lambder/jsonerl
pkg_jsonerl_commit = master

PACKAGES += jsonpath
pkg_jsonpath_name = jsonpath
pkg_jsonpath_description = Fast Erlang JSON data retrieval and updates via javascript-like notation
pkg_jsonpath_homepage = https://github.com/GeneStevens/jsonpath
pkg_jsonpath_fetch = git
pkg_jsonpath_repo = https://github.com/GeneStevens/jsonpath
pkg_jsonpath_commit = master

PACKAGES += jsonx
pkg_jsonx_name = jsonx
pkg_jsonx_description = JSONX is an Erlang library for efficient decode and encode JSON, written in C.
pkg_jsonx_homepage = https://github.com/iskra/jsonx
pkg_jsonx_fetch = git
pkg_jsonx_repo = https://github.com/iskra/jsonx
pkg_jsonx_commit = master

PACKAGES += jsx
pkg_jsx_name = jsx
pkg_jsx_description = An Erlang application for consuming, producing and manipulating JSON.
pkg_jsx_homepage = https://github.com/talentdeficit/jsx
pkg_jsx_fetch = git
pkg_jsx_repo = https://github.com/talentdeficit/jsx
pkg_jsx_commit = master

PACKAGES += kafka
pkg_kafka_name = kafka
pkg_kafka_description = Kafka consumer and producer in Erlang
pkg_kafka_homepage = https://github.com/wooga/kafka-erlang
pkg_kafka_fetch = git
pkg_kafka_repo = https://github.com/wooga/kafka-erlang
pkg_kafka_commit = master

PACKAGES += kai
pkg_kai_name = kai
pkg_kai_description = DHT storage by Takeshi Inoue
pkg_kai_homepage = https://github.com/synrc/kai
pkg_kai_fetch = git
pkg_kai_repo = https://github.com/synrc/kai
pkg_kai_commit = master

PACKAGES += katja
pkg_katja_name = katja
pkg_katja_description = A simple Riemann client written in Erlang.
pkg_katja_homepage = https://github.com/nifoc/katja
pkg_katja_fetch = git
pkg_katja_repo = https://github.com/nifoc/katja
pkg_katja_commit = master

PACKAGES += kdht
pkg_kdht_name = kdht
pkg_kdht_description = kdht is an erlang DHT implementation
pkg_kdht_homepage = https://github.com/kevinlynx/kdht
pkg_kdht_fetch = git
pkg_kdht_repo = https://github.com/kevinlynx/kdht
pkg_kdht_commit = master

PACKAGES += key2value
pkg_key2value_name = key2value
pkg_key2value_description = Erlang 2-way map
pkg_key2value_homepage = https://github.com/okeuday/key2value
pkg_key2value_fetch = git
pkg_key2value_repo = https://github.com/okeuday/key2value
pkg_key2value_commit = master

PACKAGES += keys1value
pkg_keys1value_name = keys1value
pkg_keys1value_description = Erlang set associative map for key lists
pkg_keys1value_homepage = https://github.com/okeuday/keys1value
pkg_keys1value_fetch = git
pkg_keys1value_repo = https://github.com/okeuday/keys1value
pkg_keys1value_commit = master

PACKAGES += kinetic
pkg_kinetic_name = kinetic
pkg_kinetic_description = Erlang Kinesis Client
pkg_kinetic_homepage = https://github.com/AdRoll/kinetic
pkg_kinetic_fetch = git
pkg_kinetic_repo = https://github.com/AdRoll/kinetic
pkg_kinetic_commit = master

PACKAGES += kjell
pkg_kjell_name = kjell
pkg_kjell_description = Erlang Shell
pkg_kjell_homepage = https://github.com/karlll/kjell
pkg_kjell_fetch = git
pkg_kjell_repo = https://github.com/karlll/kjell
pkg_kjell_commit = master

PACKAGES += kraken
pkg_kraken_name = kraken
pkg_kraken_description = Distributed Pubsub Server for Realtime Apps
pkg_kraken_homepage = https://github.com/Asana/kraken
pkg_kraken_fetch = git
pkg_kraken_repo = https://github.com/Asana/kraken
pkg_kraken_commit = master

PACKAGES += kucumberl
pkg_kucumberl_name = kucumberl
pkg_kucumberl_description = A pure-erlang, open-source, implementation of Cucumber
pkg_kucumberl_homepage = https://github.com/openshine/kucumberl
pkg_kucumberl_fetch = git
pkg_kucumberl_repo = https://github.com/openshine/kucumberl
pkg_kucumberl_commit = master

PACKAGES += kvc
pkg_kvc_name = kvc
pkg_kvc_description = KVC - Key Value Coding for Erlang data structures
pkg_kvc_homepage = https://github.com/etrepum/kvc
pkg_kvc_fetch = git
pkg_kvc_repo = https://github.com/etrepum/kvc
pkg_kvc_commit = master

PACKAGES += kvlists
pkg_kvlists_name = kvlists
pkg_kvlists_description = Lists of key-value pairs (decoded JSON) in Erlang
pkg_kvlists_homepage = https://github.com/jcomellas/kvlists
pkg_kvlists_fetch = git
pkg_kvlists_repo = https://github.com/jcomellas/kvlists
pkg_kvlists_commit = master

PACKAGES += kvs
pkg_kvs_name = kvs
pkg_kvs_description = Container and Iterator
pkg_kvs_homepage = https://github.com/synrc/kvs
pkg_kvs_fetch = git
pkg_kvs_repo = https://github.com/synrc/kvs
pkg_kvs_commit = master

PACKAGES += lager
pkg_lager_name = lager
pkg_lager_description = A logging framework for Erlang/OTP.
pkg_lager_homepage = https://github.com/basho/lager
pkg_lager_fetch = git
pkg_lager_repo = https://github.com/basho/lager
pkg_lager_commit = master

PACKAGES += lager_amqp_backend
pkg_lager_amqp_backend_name = lager_amqp_backend
pkg_lager_amqp_backend_description = AMQP RabbitMQ Lager backend
pkg_lager_amqp_backend_homepage = https://github.com/jbrisbin/lager_amqp_backend
pkg_lager_amqp_backend_fetch = git
pkg_lager_amqp_backend_repo = https://github.com/jbrisbin/lager_amqp_backend
pkg_lager_amqp_backend_commit = master

PACKAGES += lager_syslog
pkg_lager_syslog_name = lager_syslog
pkg_lager_syslog_description = Syslog backend for lager
pkg_lager_syslog_homepage = https://github.com/basho/lager_syslog
pkg_lager_syslog_fetch = git
pkg_lager_syslog_repo = https://github.com/basho/lager_syslog
pkg_lager_syslog_commit = master

PACKAGES += lambdapad
pkg_lambdapad_name = lambdapad
pkg_lambdapad_description = Static site generator using Erlang. Yes, Erlang.
pkg_lambdapad_homepage = https://github.com/gar1t/lambdapad
pkg_lambdapad_fetch = git
pkg_lambdapad_repo = https://github.com/gar1t/lambdapad
pkg_lambdapad_commit = master

PACKAGES += lasp
pkg_lasp_name = lasp
pkg_lasp_description = A Language for Distributed, Eventually Consistent Computations
pkg_lasp_homepage = http://lasp-lang.org/
pkg_lasp_fetch = git
pkg_lasp_repo = https://github.com/lasp-lang/lasp
pkg_lasp_commit = master

PACKAGES += lasse
pkg_lasse_name = lasse
pkg_lasse_description = SSE handler for Cowboy
pkg_lasse_homepage = https://github.com/inaka/lasse
pkg_lasse_fetch = git
pkg_lasse_repo = https://github.com/inaka/lasse
pkg_lasse_commit = 0.1.0

PACKAGES += ldap
pkg_ldap_name = ldap
pkg_ldap_description = LDAP server written in Erlang
pkg_ldap_homepage = https://github.com/spawnproc/ldap
pkg_ldap_fetch = git
pkg_ldap_repo = https://github.com/spawnproc/ldap
pkg_ldap_commit = master

PACKAGES += lethink
pkg_lethink_name = lethink
pkg_lethink_description = erlang driver for rethinkdb
pkg_lethink_homepage = https://github.com/taybin/lethink
pkg_lethink_fetch = git
pkg_lethink_repo = https://github.com/taybin/lethink
pkg_lethink_commit = master

PACKAGES += lfe
pkg_lfe_name = lfe
pkg_lfe_description = Lisp Flavoured Erlang (LFE)
pkg_lfe_homepage = https://github.com/rvirding/lfe
pkg_lfe_fetch = git
pkg_lfe_repo = https://github.com/rvirding/lfe
pkg_lfe_commit = master

PACKAGES += ling
pkg_ling_name = ling
pkg_ling_description = Erlang on Xen
pkg_ling_homepage = https://github.com/cloudozer/ling
pkg_ling_fetch = git
pkg_ling_repo = https://github.com/cloudozer/ling
pkg_ling_commit = master

PACKAGES += live
pkg_live_name = live
pkg_live_description = Automated module and configuration reloader.
pkg_live_homepage = http://ninenines.eu
pkg_live_fetch = git
pkg_live_repo = https://github.com/ninenines/live
pkg_live_commit = master

PACKAGES += lmq
pkg_lmq_name = lmq
pkg_lmq_description = Lightweight Message Queue
pkg_lmq_homepage = https://github.com/iij/lmq
pkg_lmq_fetch = git
pkg_lmq_repo = https://github.com/iij/lmq
pkg_lmq_commit = master

PACKAGES += locker
pkg_locker_name = locker
pkg_locker_description = Atomic distributed 'check and set' for short-lived keys
pkg_locker_homepage = https://github.com/wooga/locker
pkg_locker_fetch = git
pkg_locker_repo = https://github.com/wooga/locker
pkg_locker_commit = master

PACKAGES += locks
pkg_locks_name = locks
pkg_locks_description = A scalable, deadlock-resolving resource locker
pkg_locks_homepage = https://github.com/uwiger/locks
pkg_locks_fetch = git
pkg_locks_repo = https://github.com/uwiger/locks
pkg_locks_commit = master

PACKAGES += log4erl
pkg_log4erl_name = log4erl
pkg_log4erl_description = A logger for erlang in the spirit of Log4J.
pkg_log4erl_homepage = https://github.com/ahmednawras/log4erl
pkg_log4erl_fetch = git
pkg_log4erl_repo = https://github.com/ahmednawras/log4erl
pkg_log4erl_commit = master

PACKAGES += lol
pkg_lol_name = lol
pkg_lol_description = Lisp on erLang, and programming is fun again
pkg_lol_homepage = https://github.com/b0oh/lol
pkg_lol_fetch = git
pkg_lol_repo = https://github.com/b0oh/lol
pkg_lol_commit = master

PACKAGES += lucid
pkg_lucid_name = lucid
pkg_lucid_description = HTTP/2 server written in Erlang
pkg_lucid_homepage = https://github.com/tatsuhiro-t/lucid
pkg_lucid_fetch = git
pkg_lucid_repo = https://github.com/tatsuhiro-t/lucid
pkg_lucid_commit = master

PACKAGES += luerl
pkg_luerl_name = luerl
pkg_luerl_description = Lua in Erlang
pkg_luerl_homepage = https://github.com/rvirding/luerl
pkg_luerl_fetch = git
pkg_luerl_repo = https://github.com/rvirding/luerl
pkg_luerl_commit = develop

PACKAGES += luwak
pkg_luwak_name = luwak
pkg_luwak_description = Large-object storage interface for Riak
pkg_luwak_homepage = https://github.com/basho/luwak
pkg_luwak_fetch = git
pkg_luwak_repo = https://github.com/basho/luwak
pkg_luwak_commit = master

PACKAGES += lux
pkg_lux_name = lux
pkg_lux_description = Lux (LUcid eXpect scripting) simplifies test automation and provides an Expect-style execution of commands
pkg_lux_homepage = https://github.com/hawk/lux
pkg_lux_fetch = git
pkg_lux_repo = https://github.com/hawk/lux
pkg_lux_commit = master

PACKAGES += machi
pkg_machi_name = machi
pkg_machi_description = Machi file store
pkg_machi_homepage = https://github.com/basho/machi
pkg_machi_fetch = git
pkg_machi_repo = https://github.com/basho/machi
pkg_machi_commit = master

PACKAGES += mad
pkg_mad_name = mad
pkg_mad_description = Small and Fast Rebar Replacement
pkg_mad_homepage = https://github.com/synrc/mad
pkg_mad_fetch = git
pkg_mad_repo = https://github.com/synrc/mad
pkg_mad_commit = master

PACKAGES += marina
pkg_marina_name = marina
pkg_marina_description = Non-blocking Erlang Cassandra CQL3 client
pkg_marina_homepage = https://github.com/lpgauth/marina
pkg_marina_fetch = git
pkg_marina_repo = https://github.com/lpgauth/marina
pkg_marina_commit = master

PACKAGES += mavg
pkg_mavg_name = mavg
pkg_mavg_description = Erlang :: Exponential moving average library
pkg_mavg_homepage = https://github.com/EchoTeam/mavg
pkg_mavg_fetch = git
pkg_mavg_repo = https://github.com/EchoTeam/mavg
pkg_mavg_commit = master

PACKAGES += mc_erl
pkg_mc_erl_name = mc_erl
pkg_mc_erl_description = mc-erl is a server for Minecraft 1.4.7 written in Erlang.
pkg_mc_erl_homepage = https://github.com/clonejo/mc-erl
pkg_mc_erl_fetch = git
pkg_mc_erl_repo = https://github.com/clonejo/mc-erl
pkg_mc_erl_commit = master

PACKAGES += mcd
pkg_mcd_name = mcd
pkg_mcd_description = Fast memcached protocol client in pure Erlang
pkg_mcd_homepage = https://github.com/EchoTeam/mcd
pkg_mcd_fetch = git
pkg_mcd_repo = https://github.com/EchoTeam/mcd
pkg_mcd_commit = master

PACKAGES += mcerlang
pkg_mcerlang_name = mcerlang
pkg_mcerlang_description = The McErlang model checker for Erlang
pkg_mcerlang_homepage = https://github.com/fredlund/McErlang
pkg_mcerlang_fetch = git
pkg_mcerlang_repo = https://github.com/fredlund/McErlang
pkg_mcerlang_commit = master

PACKAGES += meck
pkg_meck_name = meck
pkg_meck_description = A mocking library for Erlang
pkg_meck_homepage = https://github.com/eproxus/meck
pkg_meck_fetch = git
pkg_meck_repo = https://github.com/eproxus/meck
pkg_meck_commit = master

PACKAGES += mekao
pkg_mekao_name = mekao
pkg_mekao_description = SQL constructor
pkg_mekao_homepage = https://github.com/ddosia/mekao
pkg_mekao_fetch = git
pkg_mekao_repo = https://github.com/ddosia/mekao
pkg_mekao_commit = master

PACKAGES += memo
pkg_memo_name = memo
pkg_memo_description = Erlang memoization server
pkg_memo_homepage = https://github.com/tuncer/memo
pkg_memo_fetch = git
pkg_memo_repo = https://github.com/tuncer/memo
pkg_memo_commit = master

PACKAGES += merge_index
pkg_merge_index_name = merge_index
pkg_merge_index_description = MergeIndex is an Erlang library for storing ordered sets on disk. It is very similar to an SSTable (in Google's Bigtable) or an HFile (in Hadoop).
pkg_merge_index_homepage = https://github.com/basho/merge_index
pkg_merge_index_fetch = git
pkg_merge_index_repo = https://github.com/basho/merge_index
pkg_merge_index_commit = master

PACKAGES += merl
pkg_merl_name = merl
pkg_merl_description = Metaprogramming in Erlang
pkg_merl_homepage = https://github.com/richcarl/merl
pkg_merl_fetch = git
pkg_merl_repo = https://github.com/richcarl/merl
pkg_merl_commit = master

PACKAGES += mimetypes
pkg_mimetypes_name = mimetypes
pkg_mimetypes_description = Erlang MIME types library
pkg_mimetypes_homepage = https://github.com/spawngrid/mimetypes
pkg_mimetypes_fetch = git
pkg_mimetypes_repo = https://github.com/spawngrid/mimetypes
pkg_mimetypes_commit = master

PACKAGES += mixer
pkg_mixer_name = mixer
pkg_mixer_description = Mix in functions from other modules
pkg_mixer_homepage = https://github.com/chef/mixer
pkg_mixer_fetch = git
pkg_mixer_repo = https://github.com/chef/mixer
pkg_mixer_commit = master

PACKAGES += mochiweb
pkg_mochiweb_name = mochiweb
pkg_mochiweb_description = MochiWeb is an Erlang library for building lightweight HTTP servers.
pkg_mochiweb_homepage = https://github.com/mochi/mochiweb
pkg_mochiweb_fetch = git
pkg_mochiweb_repo = https://github.com/mochi/mochiweb
pkg_mochiweb_commit = master

PACKAGES += mochiweb_xpath
pkg_mochiweb_xpath_name = mochiweb_xpath
pkg_mochiweb_xpath_description = XPath support for mochiweb's html parser
pkg_mochiweb_xpath_homepage = https://github.com/retnuh/mochiweb_xpath
pkg_mochiweb_xpath_fetch = git
pkg_mochiweb_xpath_repo = https://github.com/retnuh/mochiweb_xpath
pkg_mochiweb_xpath_commit = master

PACKAGES += mockgyver
pkg_mockgyver_name = mockgyver
pkg_mockgyver_description = A mocking library for Erlang
pkg_mockgyver_homepage = https://github.com/klajo/mockgyver
pkg_mockgyver_fetch = git
pkg_mockgyver_repo = https://github.com/klajo/mockgyver
pkg_mockgyver_commit = master

PACKAGES += modlib
pkg_modlib_name = modlib
pkg_modlib_description = Web framework based on Erlang's inets httpd
pkg_modlib_homepage = https://github.com/gar1t/modlib
pkg_modlib_fetch = git
pkg_modlib_repo = https://github.com/gar1t/modlib
pkg_modlib_commit = master

PACKAGES += mongodb
pkg_mongodb_name = mongodb
pkg_mongodb_description = MongoDB driver for Erlang
pkg_mongodb_homepage = https://github.com/comtihon/mongodb-erlang
pkg_mongodb_fetch = git
pkg_mongodb_repo = https://github.com/comtihon/mongodb-erlang
pkg_mongodb_commit = master

PACKAGES += mongooseim
pkg_mongooseim_name = mongooseim
pkg_mongooseim_description = Jabber / XMPP server with focus on performance and scalability, by Erlang Solutions
pkg_mongooseim_homepage = https://www.erlang-solutions.com/products/mongooseim-massively-scalable-ejabberd-platform
pkg_mongooseim_fetch = git
pkg_mongooseim_repo = https://github.com/esl/MongooseIM
pkg_mongooseim_commit = master

PACKAGES += moyo
pkg_moyo_name = moyo
pkg_moyo_description = Erlang utility functions library
pkg_moyo_homepage = https://github.com/dwango/moyo
pkg_moyo_fetch = git
pkg_moyo_repo = https://github.com/dwango/moyo
pkg_moyo_commit = master

PACKAGES += msgpack
pkg_msgpack_name = msgpack
pkg_msgpack_description = MessagePack (de)serializer implementation for Erlang
pkg_msgpack_homepage = https://github.com/msgpack/msgpack-erlang
pkg_msgpack_fetch = git
pkg_msgpack_repo = https://github.com/msgpack/msgpack-erlang
pkg_msgpack_commit = master

PACKAGES += mu2
pkg_mu2_name = mu2
pkg_mu2_description = Erlang mutation testing tool
pkg_mu2_homepage = https://github.com/ramsay-t/mu2
pkg_mu2_fetch = git
pkg_mu2_repo = https://github.com/ramsay-t/mu2
pkg_mu2_commit = master

PACKAGES += mustache
pkg_mustache_name = mustache
pkg_mustache_description = Mustache template engine for Erlang.
pkg_mustache_homepage = https://github.com/mojombo/mustache.erl
pkg_mustache_fetch = git
pkg_mustache_repo = https://github.com/mojombo/mustache.erl
pkg_mustache_commit = master

PACKAGES += myproto
pkg_myproto_name = myproto
pkg_myproto_description = MySQL Server Protocol in Erlang
pkg_myproto_homepage = https://github.com/altenwald/myproto
pkg_myproto_fetch = git
pkg_myproto_repo = https://github.com/altenwald/myproto
pkg_myproto_commit = master

PACKAGES += mysql
pkg_mysql_name = mysql
pkg_mysql_description = Erlang MySQL Driver (from code.google.com)
pkg_mysql_homepage = https://github.com/dizzyd/erlang-mysql-driver
pkg_mysql_fetch = git
pkg_mysql_repo = https://github.com/dizzyd/erlang-mysql-driver
pkg_mysql_commit = master

PACKAGES += n2o
pkg_n2o_name = n2o
pkg_n2o_description = WebSocket Application Server
pkg_n2o_homepage = https://github.com/5HT/n2o
pkg_n2o_fetch = git
pkg_n2o_repo = https://github.com/5HT/n2o
pkg_n2o_commit = master

PACKAGES += nat_upnp
pkg_nat_upnp_name = nat_upnp
pkg_nat_upnp_description = Erlang library to map your internal port to an external using UNP IGD
pkg_nat_upnp_homepage = https://github.com/benoitc/nat_upnp
pkg_nat_upnp_fetch = git
pkg_nat_upnp_repo = https://github.com/benoitc/nat_upnp
pkg_nat_upnp_commit = master

PACKAGES += neo4j
pkg_neo4j_name = neo4j
pkg_neo4j_description = Erlang client library for Neo4J.
pkg_neo4j_homepage = https://github.com/dmitriid/neo4j-erlang
pkg_neo4j_fetch = git
pkg_neo4j_repo = https://github.com/dmitriid/neo4j-erlang
pkg_neo4j_commit = master

PACKAGES += neotoma
pkg_neotoma_name = neotoma
pkg_neotoma_description = Erlang library and packrat parser-generator for parsing expression grammars.
pkg_neotoma_homepage = https://github.com/seancribbs/neotoma
pkg_neotoma_fetch = git
pkg_neotoma_repo = https://github.com/seancribbs/neotoma
pkg_neotoma_commit = master

PACKAGES += newrelic
pkg_newrelic_name = newrelic
pkg_newrelic_description = Erlang library for sending metrics to New Relic
pkg_newrelic_homepage = https://github.com/wooga/newrelic-erlang
pkg_newrelic_fetch = git
pkg_newrelic_repo = https://github.com/wooga/newrelic-erlang
pkg_newrelic_commit = master

PACKAGES += nifty
pkg_nifty_name = nifty
pkg_nifty_description = Erlang NIF wrapper generator
pkg_nifty_homepage = https://github.com/parapluu/nifty
pkg_nifty_fetch = git
pkg_nifty_repo = https://github.com/parapluu/nifty
pkg_nifty_commit = master

PACKAGES += nitrogen_core
pkg_nitrogen_core_name = nitrogen_core
pkg_nitrogen_core_description = The core Nitrogen library.
pkg_nitrogen_core_homepage = http://nitrogenproject.com/
pkg_nitrogen_core_fetch = git
pkg_nitrogen_core_repo = https://github.com/nitrogen/nitrogen_core
pkg_nitrogen_core_commit = master

PACKAGES += nkbase
pkg_nkbase_name = nkbase
pkg_nkbase_description = NkBASE distributed database
pkg_nkbase_homepage = https://github.com/Nekso/nkbase
pkg_nkbase_fetch = git
pkg_nkbase_repo = https://github.com/Nekso/nkbase
pkg_nkbase_commit = develop

PACKAGES += nkdocker
pkg_nkdocker_name = nkdocker
pkg_nkdocker_description = Erlang Docker client
pkg_nkdocker_homepage = https://github.com/Nekso/nkdocker
pkg_nkdocker_fetch = git
pkg_nkdocker_repo = https://github.com/Nekso/nkdocker
pkg_nkdocker_commit = master

PACKAGES += nkpacket
pkg_nkpacket_name = nkpacket
pkg_nkpacket_description = Generic Erlang transport layer
pkg_nkpacket_homepage = https://github.com/Nekso/nkpacket
pkg_nkpacket_fetch = git
pkg_nkpacket_repo = https://github.com/Nekso/nkpacket
pkg_nkpacket_commit = master

PACKAGES += nodefinder
pkg_nodefinder_name = nodefinder
pkg_nodefinder_description = automatic node discovery via UDP multicast
pkg_nodefinder_homepage = https://github.com/erlanger/nodefinder
pkg_nodefinder_fetch = git
pkg_nodefinder_repo = https://github.com/okeuday/nodefinder
pkg_nodefinder_commit = master

PACKAGES += nprocreg
pkg_nprocreg_name = nprocreg
pkg_nprocreg_description = Minimal Distributed Erlang Process Registry
pkg_nprocreg_homepage = http://nitrogenproject.com/
pkg_nprocreg_fetch = git
pkg_nprocreg_repo = https://github.com/nitrogen/nprocreg
pkg_nprocreg_commit = master

PACKAGES += oauth
pkg_oauth_name = oauth
pkg_oauth_description = An Erlang OAuth 1.0 implementation
pkg_oauth_homepage = https://github.com/tim/erlang-oauth
pkg_oauth_fetch = git
pkg_oauth_repo = https://github.com/tim/erlang-oauth
pkg_oauth_commit = master

PACKAGES += oauth2
pkg_oauth2_name = oauth2
pkg_oauth2_description = Erlang Oauth2 implementation
pkg_oauth2_homepage = https://github.com/kivra/oauth2
pkg_oauth2_fetch = git
pkg_oauth2_repo = https://github.com/kivra/oauth2
pkg_oauth2_commit = master

PACKAGES += oauth2c
pkg_oauth2c_name = oauth2c
pkg_oauth2c_description = Erlang OAuth2 Client
pkg_oauth2c_homepage = https://github.com/kivra/oauth2_client
pkg_oauth2c_fetch = git
pkg_oauth2c_repo = https://github.com/kivra/oauth2_client
pkg_oauth2c_commit = master

PACKAGES += of_protocol
pkg_of_protocol_name = of_protocol
pkg_of_protocol_description = OpenFlow Protocol Library for Erlang
pkg_of_protocol_homepage = https://github.com/FlowForwarding/of_protocol
pkg_of_protocol_fetch = git
pkg_of_protocol_repo = https://github.com/FlowForwarding/of_protocol
pkg_of_protocol_commit = master

PACKAGES += openflow
pkg_openflow_name = openflow
pkg_openflow_description = An OpenFlow controller written in pure erlang
pkg_openflow_homepage = https://github.com/renatoaguiar/erlang-openflow
pkg_openflow_fetch = git
pkg_openflow_repo = https://github.com/renatoaguiar/erlang-openflow
pkg_openflow_commit = master

PACKAGES += openid
pkg_openid_name = openid
pkg_openid_description = Erlang OpenID
pkg_openid_homepage = https://github.com/brendonh/erl_openid
pkg_openid_fetch = git
pkg_openid_repo = https://github.com/brendonh/erl_openid
pkg_openid_commit = master

PACKAGES += openpoker
pkg_openpoker_name = openpoker
pkg_openpoker_description = Genesis Texas hold'em Game Server
pkg_openpoker_homepage = https://github.com/hpyhacking/openpoker
pkg_openpoker_fetch = git
pkg_openpoker_repo = https://github.com/hpyhacking/openpoker
pkg_openpoker_commit = master

PACKAGES += pal
pkg_pal_name = pal
pkg_pal_description = Pragmatic Authentication Library
pkg_pal_homepage = https://github.com/manifest/pal
pkg_pal_fetch = git
pkg_pal_repo = https://github.com/manifest/pal
pkg_pal_commit = master

PACKAGES += parse_trans
pkg_parse_trans_name = parse_trans
pkg_parse_trans_description = Parse transform utilities for Erlang
pkg_parse_trans_homepage = https://github.com/uwiger/parse_trans
pkg_parse_trans_fetch = git
pkg_parse_trans_repo = https://github.com/uwiger/parse_trans
pkg_parse_trans_commit = master

PACKAGES += parsexml
pkg_parsexml_name = parsexml
pkg_parsexml_description = Simple DOM XML parser with convenient and very simple API
pkg_parsexml_homepage = https://github.com/maxlapshin/parsexml
pkg_parsexml_fetch = git
pkg_parsexml_repo = https://github.com/maxlapshin/parsexml
pkg_parsexml_commit = master

PACKAGES += pegjs
pkg_pegjs_name = pegjs
pkg_pegjs_description = An implementation of PEG.js grammar for Erlang.
pkg_pegjs_homepage = https://github.com/dmitriid/pegjs
pkg_pegjs_fetch = git
pkg_pegjs_repo = https://github.com/dmitriid/pegjs
pkg_pegjs_commit = 0.3

PACKAGES += percept2
pkg_percept2_name = percept2
pkg_percept2_description = Concurrent profiling tool for Erlang
pkg_percept2_homepage = https://github.com/huiqing/percept2
pkg_percept2_fetch = git
pkg_percept2_repo = https://github.com/huiqing/percept2
pkg_percept2_commit = master

PACKAGES += pgsql
pkg_pgsql_name = pgsql
pkg_pgsql_description = Erlang PostgreSQL driver
pkg_pgsql_homepage = https://github.com/semiocast/pgsql
pkg_pgsql_fetch = git
pkg_pgsql_repo = https://github.com/semiocast/pgsql
pkg_pgsql_commit = master

PACKAGES += pkgx
pkg_pkgx_name = pkgx
pkg_pkgx_description = Build .deb packages from Erlang releases
pkg_pkgx_homepage = https://github.com/arjan/pkgx
pkg_pkgx_fetch = git
pkg_pkgx_repo = https://github.com/arjan/pkgx
pkg_pkgx_commit = master

PACKAGES += pkt
pkg_pkt_name = pkt
pkg_pkt_description = Erlang network protocol library
pkg_pkt_homepage = https://github.com/msantos/pkt
pkg_pkt_fetch = git
pkg_pkt_repo = https://github.com/msantos/pkt
pkg_pkt_commit = master

PACKAGES += plain_fsm
pkg_plain_fsm_name = plain_fsm
pkg_plain_fsm_description = A behaviour/support library for writing plain Erlang FSMs.
pkg_plain_fsm_homepage = https://github.com/uwiger/plain_fsm
pkg_plain_fsm_fetch = git
pkg_plain_fsm_repo = https://github.com/uwiger/plain_fsm
pkg_plain_fsm_commit = master

PACKAGES += plumtree
pkg_plumtree_name = plumtree
pkg_plumtree_description = Epidemic Broadcast Trees
pkg_plumtree_homepage = https://github.com/helium/plumtree
pkg_plumtree_fetch = git
pkg_plumtree_repo = https://github.com/helium/plumtree
pkg_plumtree_commit = master

PACKAGES += pmod_transform
pkg_pmod_transform_name = pmod_transform
pkg_pmod_transform_description = Parse transform for parameterized modules
pkg_pmod_transform_homepage = https://github.com/erlang/pmod_transform
pkg_pmod_transform_fetch = git
pkg_pmod_transform_repo = https://github.com/erlang/pmod_transform
pkg_pmod_transform_commit = master

PACKAGES += pobox
pkg_pobox_name = pobox
pkg_pobox_description = External buffer processes to protect against mailbox overflow in Erlang
pkg_pobox_homepage = https://github.com/ferd/pobox
pkg_pobox_fetch = git
pkg_pobox_repo = https://github.com/ferd/pobox
pkg_pobox_commit = master

PACKAGES += ponos
pkg_ponos_name = ponos
pkg_ponos_description = ponos is a simple yet powerful load generator written in erlang
pkg_ponos_homepage = https://github.com/klarna/ponos
pkg_ponos_fetch = git
pkg_ponos_repo = https://github.com/klarna/ponos
pkg_ponos_commit = master

PACKAGES += poolboy
pkg_poolboy_name = poolboy
pkg_poolboy_description = A hunky Erlang worker pool factory
pkg_poolboy_homepage = https://github.com/devinus/poolboy
pkg_poolboy_fetch = git
pkg_poolboy_repo = https://github.com/devinus/poolboy
pkg_poolboy_commit = master

PACKAGES += pooler
pkg_pooler_name = pooler
pkg_pooler_description = An OTP Process Pool Application
pkg_pooler_homepage = https://github.com/seth/pooler
pkg_pooler_fetch = git
pkg_pooler_repo = https://github.com/seth/pooler
pkg_pooler_commit = master

PACKAGES += pqueue
pkg_pqueue_name = pqueue
pkg_pqueue_description = Erlang Priority Queues
pkg_pqueue_homepage = https://github.com/okeuday/pqueue
pkg_pqueue_fetch = git
pkg_pqueue_repo = https://github.com/okeuday/pqueue
pkg_pqueue_commit = master

PACKAGES += procket
pkg_procket_name = procket
pkg_procket_description = Erlang interface to low level socket operations
pkg_procket_homepage = http://blog.listincomprehension.com/search/label/procket
pkg_procket_fetch = git
pkg_procket_repo = https://github.com/msantos/procket
pkg_procket_commit = master

PACKAGES += prop
pkg_prop_name = prop
pkg_prop_description = An Erlang code scaffolding and generator system.
pkg_prop_homepage = https://github.com/nuex/prop
pkg_prop_fetch = git
pkg_prop_repo = https://github.com/nuex/prop
pkg_prop_commit = master

PACKAGES += proper
pkg_proper_name = proper
pkg_proper_description = PropEr: a QuickCheck-inspired property-based testing tool for Erlang.
pkg_proper_homepage = http://proper.softlab.ntua.gr
pkg_proper_fetch = git
pkg_proper_repo = https://github.com/manopapad/proper
pkg_proper_commit = master

PACKAGES += props
pkg_props_name = props
pkg_props_description = Property structure library
pkg_props_homepage = https://github.com/greyarea/props
pkg_props_fetch = git
pkg_props_repo = https://github.com/greyarea/props
pkg_props_commit = master

PACKAGES += protobuffs
pkg_protobuffs_name = protobuffs
pkg_protobuffs_description = An implementation of Google's Protocol Buffers for Erlang, based on ngerakines/erlang_protobuffs.
pkg_protobuffs_homepage = https://github.com/basho/erlang_protobuffs
pkg_protobuffs_fetch = git
pkg_protobuffs_repo = https://github.com/basho/erlang_protobuffs
pkg_protobuffs_commit = master

PACKAGES += psycho
pkg_psycho_name = psycho
pkg_psycho_description = HTTP server that provides a WSGI-like interface for applications and middleware.
pkg_psycho_homepage = https://github.com/gar1t/psycho
pkg_psycho_fetch = git
pkg_psycho_repo = https://github.com/gar1t/psycho
pkg_psycho_commit = master

PACKAGES += ptrackerl
pkg_ptrackerl_name = ptrackerl
pkg_ptrackerl_description = Pivotal Tracker API Client written in Erlang
pkg_ptrackerl_homepage = https://github.com/inaka/ptrackerl
pkg_ptrackerl_fetch = git
pkg_ptrackerl_repo = https://github.com/inaka/ptrackerl
pkg_ptrackerl_commit = master

PACKAGES += purity
pkg_purity_name = purity
pkg_purity_description = A side-effect analyzer for Erlang
pkg_purity_homepage = https://github.com/mpitid/purity
pkg_purity_fetch = git
pkg_purity_repo = https://github.com/mpitid/purity
pkg_purity_commit = master

PACKAGES += push_service
pkg_push_service_name = push_service
pkg_push_service_description = Push service
pkg_push_service_homepage = https://github.com/hairyhum/push_service
pkg_push_service_fetch = git
pkg_push_service_repo = https://github.com/hairyhum/push_service
pkg_push_service_commit = master

PACKAGES += qdate
pkg_qdate_name = qdate
pkg_qdate_description = Date, time, and timezone parsing, formatting, and conversion for Erlang.
pkg_qdate_homepage = https://github.com/choptastic/qdate
pkg_qdate_fetch = git
pkg_qdate_repo = https://github.com/choptastic/qdate
pkg_qdate_commit = 0.4.0

PACKAGES += qrcode
pkg_qrcode_name = qrcode
pkg_qrcode_description = QR Code encoder in Erlang
pkg_qrcode_homepage = https://github.com/komone/qrcode
pkg_qrcode_fetch = git
pkg_qrcode_repo = https://github.com/komone/qrcode
pkg_qrcode_commit = master

PACKAGES += quest
pkg_quest_name = quest
pkg_quest_description = Learn Erlang through this set of challenges. An interactive system for getting to know Erlang.
pkg_quest_homepage = https://github.com/eriksoe/ErlangQuest
pkg_quest_fetch = git
pkg_quest_repo = https://github.com/eriksoe/ErlangQuest
pkg_quest_commit = master

PACKAGES += quickrand
pkg_quickrand_name = quickrand
pkg_quickrand_description = Quick Erlang Random Number Generation
pkg_quickrand_homepage = https://github.com/okeuday/quickrand
pkg_quickrand_fetch = git
pkg_quickrand_repo = https://github.com/okeuday/quickrand
pkg_quickrand_commit = master

PACKAGES += rabbit
pkg_rabbit_name = rabbit
pkg_rabbit_description = RabbitMQ Server
pkg_rabbit_homepage = https://www.rabbitmq.com/
pkg_rabbit_fetch = git
pkg_rabbit_repo = https://github.com/rabbitmq/rabbitmq-server.git
pkg_rabbit_commit = master

PACKAGES += rabbit_exchange_type_riak
pkg_rabbit_exchange_type_riak_name = rabbit_exchange_type_riak
pkg_rabbit_exchange_type_riak_description = Custom RabbitMQ exchange type for sticking messages in Riak
pkg_rabbit_exchange_type_riak_homepage = https://github.com/jbrisbin/riak-exchange
pkg_rabbit_exchange_type_riak_fetch = git
pkg_rabbit_exchange_type_riak_repo = https://github.com/jbrisbin/riak-exchange
pkg_rabbit_exchange_type_riak_commit = master

PACKAGES += rack
pkg_rack_name = rack
pkg_rack_description = Rack handler for erlang
pkg_rack_homepage = https://github.com/erlyvideo/rack
pkg_rack_fetch = git
pkg_rack_repo = https://github.com/erlyvideo/rack
pkg_rack_commit = master

PACKAGES += radierl
pkg_radierl_name = radierl
pkg_radierl_description = RADIUS protocol stack implemented in Erlang.
pkg_radierl_homepage = https://github.com/vances/radierl
pkg_radierl_fetch = git
pkg_radierl_repo = https://github.com/vances/radierl
pkg_radierl_commit = master

PACKAGES += rafter
pkg_rafter_name = rafter
pkg_rafter_description = An Erlang library application which implements the Raft consensus protocol
pkg_rafter_homepage = https://github.com/andrewjstone/rafter
pkg_rafter_fetch = git
pkg_rafter_repo = https://github.com/andrewjstone/rafter
pkg_rafter_commit = master

PACKAGES += ranch
pkg_ranch_name = ranch
pkg_ranch_description = Socket acceptor pool for TCP protocols.
pkg_ranch_homepage = http://ninenines.eu
pkg_ranch_fetch = git
pkg_ranch_repo = https://github.com/ninenines/ranch
pkg_ranch_commit = 1.1.0

PACKAGES += rbeacon
pkg_rbeacon_name = rbeacon
pkg_rbeacon_description = LAN discovery and presence in Erlang.
pkg_rbeacon_homepage = https://github.com/refuge/rbeacon
pkg_rbeacon_fetch = git
pkg_rbeacon_repo = https://github.com/refuge/rbeacon
pkg_rbeacon_commit = master

PACKAGES += rebar
pkg_rebar_name = rebar
pkg_rebar_description = Erlang build tool that makes it easy to compile and test Erlang applications, port drivers and releases.
pkg_rebar_homepage = http://www.rebar3.org
pkg_rebar_fetch = git
pkg_rebar_repo = https://github.com/rebar/rebar3
pkg_rebar_commit = master

PACKAGES += rebus
pkg_rebus_name = rebus
pkg_rebus_description = A stupid simple, internal, pub/sub event bus written in- and for Erlang.
pkg_rebus_homepage = https://github.com/olle/rebus
pkg_rebus_fetch = git
pkg_rebus_repo = https://github.com/olle/rebus
pkg_rebus_commit = master

PACKAGES += rec2json
pkg_rec2json_name = rec2json
pkg_rec2json_description = Compile erlang record definitions into modules to convert them to/from json easily.
pkg_rec2json_homepage = https://github.com/lordnull/rec2json
pkg_rec2json_fetch = git
pkg_rec2json_repo = https://github.com/lordnull/rec2json
pkg_rec2json_commit = master

PACKAGES += recon
pkg_recon_name = recon
pkg_recon_description = Collection of functions and scripts to debug Erlang in production.
pkg_recon_homepage = https://github.com/ferd/recon
pkg_recon_fetch = git
pkg_recon_repo = https://github.com/ferd/recon
pkg_recon_commit = 2.2.1

PACKAGES += record_info
pkg_record_info_name = record_info
pkg_record_info_description = Convert between record and proplist
pkg_record_info_homepage = https://github.com/bipthelin/erlang-record_info
pkg_record_info_fetch = git
pkg_record_info_repo = https://github.com/bipthelin/erlang-record_info
pkg_record_info_commit = master

PACKAGES += redgrid
pkg_redgrid_name = redgrid
pkg_redgrid_description = automatic Erlang node discovery via redis
pkg_redgrid_homepage = https://github.com/jkvor/redgrid
pkg_redgrid_fetch = git
pkg_redgrid_repo = https://github.com/jkvor/redgrid
pkg_redgrid_commit = master

PACKAGES += redo
pkg_redo_name = redo
pkg_redo_description = pipelined erlang redis client
pkg_redo_homepage = https://github.com/jkvor/redo
pkg_redo_fetch = git
pkg_redo_repo = https://github.com/jkvor/redo
pkg_redo_commit = master

PACKAGES += reltool_util
pkg_reltool_util_name = reltool_util
pkg_reltool_util_description = Erlang reltool utility functionality application
pkg_reltool_util_homepage = https://github.com/okeuday/reltool_util
pkg_reltool_util_fetch = git
pkg_reltool_util_repo = https://github.com/okeuday/reltool_util
pkg_reltool_util_commit = master

PACKAGES += relx
pkg_relx_name = relx
pkg_relx_description = Sane, simple release creation for Erlang
pkg_relx_homepage = https://github.com/erlware/relx
pkg_relx_fetch = git
pkg_relx_repo = https://github.com/erlware/relx
pkg_relx_commit = master

PACKAGES += resource_discovery
pkg_resource_discovery_name = resource_discovery
pkg_resource_discovery_description = An application used to dynamically discover resources present in an Erlang node cluster.
pkg_resource_discovery_homepage = http://erlware.org/
pkg_resource_discovery_fetch = git
pkg_resource_discovery_repo = https://github.com/erlware/resource_discovery
pkg_resource_discovery_commit = master

PACKAGES += restc
pkg_restc_name = restc
pkg_restc_description = Erlang Rest Client
pkg_restc_homepage = https://github.com/kivra/restclient
pkg_restc_fetch = git
pkg_restc_repo = https://github.com/kivra/restclient
pkg_restc_commit = master

PACKAGES += rfc4627_jsonrpc
pkg_rfc4627_jsonrpc_name = rfc4627_jsonrpc
pkg_rfc4627_jsonrpc_description = Erlang RFC4627 (JSON) codec and JSON-RPC server implementation.
pkg_rfc4627_jsonrpc_homepage = https://github.com/tonyg/erlang-rfc4627
pkg_rfc4627_jsonrpc_fetch = git
pkg_rfc4627_jsonrpc_repo = https://github.com/tonyg/erlang-rfc4627
pkg_rfc4627_jsonrpc_commit = master

PACKAGES += riak_control
pkg_riak_control_name = riak_control
pkg_riak_control_description = Webmachine-based administration interface for Riak.
pkg_riak_control_homepage = https://github.com/basho/riak_control
pkg_riak_control_fetch = git
pkg_riak_control_repo = https://github.com/basho/riak_control
pkg_riak_control_commit = master

PACKAGES += riak_core
pkg_riak_core_name = riak_core
pkg_riak_core_description = Distributed systems infrastructure used by Riak.
pkg_riak_core_homepage = https://github.com/basho/riak_core
pkg_riak_core_fetch = git
pkg_riak_core_repo = https://github.com/basho/riak_core
pkg_riak_core_commit = master

PACKAGES += riak_dt
pkg_riak_dt_name = riak_dt
pkg_riak_dt_description = Convergent replicated datatypes in Erlang
pkg_riak_dt_homepage = https://github.com/basho/riak_dt
pkg_riak_dt_fetch = git
pkg_riak_dt_repo = https://github.com/basho/riak_dt
pkg_riak_dt_commit = master

PACKAGES += riak_ensemble
pkg_riak_ensemble_name = riak_ensemble
pkg_riak_ensemble_description = Multi-Paxos framework in Erlang
pkg_riak_ensemble_homepage = https://github.com/basho/riak_ensemble
pkg_riak_ensemble_fetch = git
pkg_riak_ensemble_repo = https://github.com/basho/riak_ensemble
pkg_riak_ensemble_commit = master

PACKAGES += riak_kv
pkg_riak_kv_name = riak_kv
pkg_riak_kv_description = Riak Key/Value Store
pkg_riak_kv_homepage = https://github.com/basho/riak_kv
pkg_riak_kv_fetch = git
pkg_riak_kv_repo = https://github.com/basho/riak_kv
pkg_riak_kv_commit = master

PACKAGES += riak_pg
pkg_riak_pg_name = riak_pg
pkg_riak_pg_description = Distributed process groups with riak_core.
pkg_riak_pg_homepage = https://github.com/cmeiklejohn/riak_pg
pkg_riak_pg_fetch = git
pkg_riak_pg_repo = https://github.com/cmeiklejohn/riak_pg
pkg_riak_pg_commit = master

PACKAGES += riak_pipe
pkg_riak_pipe_name = riak_pipe
pkg_riak_pipe_description = Riak Pipelines
pkg_riak_pipe_homepage = https://github.com/basho/riak_pipe
pkg_riak_pipe_fetch = git
pkg_riak_pipe_repo = https://github.com/basho/riak_pipe
pkg_riak_pipe_commit = master

PACKAGES += riak_sysmon
pkg_riak_sysmon_name = riak_sysmon
pkg_riak_sysmon_description = Simple OTP app for managing Erlang VM system_monitor event messages
pkg_riak_sysmon_homepage = https://github.com/basho/riak_sysmon
pkg_riak_sysmon_fetch = git
pkg_riak_sysmon_repo = https://github.com/basho/riak_sysmon
pkg_riak_sysmon_commit = master

PACKAGES += riak_test
pkg_riak_test_name = riak_test
pkg_riak_test_description = I'm in your cluster, testing your riaks
pkg_riak_test_homepage = https://github.com/basho/riak_test
pkg_riak_test_fetch = git
pkg_riak_test_repo = https://github.com/basho/riak_test
pkg_riak_test_commit = master

PACKAGES += riakc
pkg_riakc_name = riakc
pkg_riakc_description = Erlang clients for Riak.
pkg_riakc_homepage = https://github.com/basho/riak-erlang-client
pkg_riakc_fetch = git
pkg_riakc_repo = https://github.com/basho/riak-erlang-client
pkg_riakc_commit = master

PACKAGES += riakhttpc
pkg_riakhttpc_name = riakhttpc
pkg_riakhttpc_description = Riak Erlang client using the HTTP interface
pkg_riakhttpc_homepage = https://github.com/basho/riak-erlang-http-client
pkg_riakhttpc_fetch = git
pkg_riakhttpc_repo = https://github.com/basho/riak-erlang-http-client
pkg_riakhttpc_commit = master

PACKAGES += riaknostic
pkg_riaknostic_name = riaknostic
pkg_riaknostic_description = A diagnostic tool for Riak installations, to find common errors asap
pkg_riaknostic_homepage = https://github.com/basho/riaknostic
pkg_riaknostic_fetch = git
pkg_riaknostic_repo = https://github.com/basho/riaknostic
pkg_riaknostic_commit = master

PACKAGES += riakpool
pkg_riakpool_name = riakpool
pkg_riakpool_description = erlang riak client pool
pkg_riakpool_homepage = https://github.com/dweldon/riakpool
pkg_riakpool_fetch = git
pkg_riakpool_repo = https://github.com/dweldon/riakpool
pkg_riakpool_commit = master

PACKAGES += rivus_cep
pkg_rivus_cep_name = rivus_cep
pkg_rivus_cep_description = Complex event processing in Erlang
pkg_rivus_cep_homepage = https://github.com/vascokk/rivus_cep
pkg_rivus_cep_fetch = git
pkg_rivus_cep_repo = https://github.com/vascokk/rivus_cep
pkg_rivus_cep_commit = master

PACKAGES += rlimit
pkg_rlimit_name = rlimit
pkg_rlimit_description = Magnus Klaar's rate limiter code from etorrent
pkg_rlimit_homepage = https://github.com/jlouis/rlimit
pkg_rlimit_fetch = git
pkg_rlimit_repo = https://github.com/jlouis/rlimit
pkg_rlimit_commit = master

PACKAGES += safetyvalve
pkg_safetyvalve_name = safetyvalve
pkg_safetyvalve_description = A safety valve for your erlang node
pkg_safetyvalve_homepage = https://github.com/jlouis/safetyvalve
pkg_safetyvalve_fetch = git
pkg_safetyvalve_repo = https://github.com/jlouis/safetyvalve
pkg_safetyvalve_commit = master

PACKAGES += seestar
pkg_seestar_name = seestar
pkg_seestar_description = The Erlang client for Cassandra 1.2+ binary protocol
pkg_seestar_homepage = https://github.com/iamaleksey/seestar
pkg_seestar_fetch = git
pkg_seestar_repo = https://github.com/iamaleksey/seestar
pkg_seestar_commit = master

PACKAGES += service
pkg_service_name = service
pkg_service_description = A minimal Erlang behavior for creating CloudI internal services
pkg_service_homepage = http://cloudi.org/
pkg_service_fetch = git
pkg_service_repo = https://github.com/CloudI/service
pkg_service_commit = master

PACKAGES += setup
pkg_setup_name = setup
pkg_setup_description = Generic setup utility for Erlang-based systems
pkg_setup_homepage = https://github.com/uwiger/setup
pkg_setup_fetch = git
pkg_setup_repo = https://github.com/uwiger/setup
pkg_setup_commit = master

PACKAGES += sext
pkg_sext_name = sext
pkg_sext_description = Sortable Erlang Term Serialization
pkg_sext_homepage = https://github.com/uwiger/sext
pkg_sext_fetch = git
pkg_sext_repo = https://github.com/uwiger/sext
pkg_sext_commit = master

PACKAGES += sfmt
pkg_sfmt_name = sfmt
pkg_sfmt_description = SFMT pseudo random number generator for Erlang.
pkg_sfmt_homepage = https://github.com/jj1bdx/sfmt-erlang
pkg_sfmt_fetch = git
pkg_sfmt_repo = https://github.com/jj1bdx/sfmt-erlang
pkg_sfmt_commit = master

PACKAGES += sgte
pkg_sgte_name = sgte
pkg_sgte_description = A simple Erlang Template Engine
pkg_sgte_homepage = https://github.com/filippo/sgte
pkg_sgte_fetch = git
pkg_sgte_repo = https://github.com/filippo/sgte
pkg_sgte_commit = master

PACKAGES += sheriff
pkg_sheriff_name = sheriff
pkg_sheriff_description = Parse transform for type based validation.
pkg_sheriff_homepage = http://ninenines.eu
pkg_sheriff_fetch = git
pkg_sheriff_repo = https://github.com/extend/sheriff
pkg_sheriff_commit = master

PACKAGES += shotgun
pkg_shotgun_name = shotgun
pkg_shotgun_description = better than just a gun
pkg_shotgun_homepage = https://github.com/inaka/shotgun
pkg_shotgun_fetch = git
pkg_shotgun_repo = https://github.com/inaka/shotgun
pkg_shotgun_commit = 0.1.0

PACKAGES += sidejob
pkg_sidejob_name = sidejob
pkg_sidejob_description = Parallel worker and capacity limiting library for Erlang
pkg_sidejob_homepage = https://github.com/basho/sidejob
pkg_sidejob_fetch = git
pkg_sidejob_repo = https://github.com/basho/sidejob
pkg_sidejob_commit = master

PACKAGES += sieve
pkg_sieve_name = sieve
pkg_sieve_description = sieve is a simple TCP routing proxy (layer 7) in erlang
pkg_sieve_homepage = https://github.com/benoitc/sieve
pkg_sieve_fetch = git
pkg_sieve_repo = https://github.com/benoitc/sieve
pkg_sieve_commit = master

PACKAGES += sighandler
pkg_sighandler_name = sighandler
pkg_sighandler_description = Handle UNIX signals in Er    lang
pkg_sighandler_homepage = https://github.com/jkingsbery/sighandler
pkg_sighandler_fetch = git
pkg_sighandler_repo = https://github.com/jkingsbery/sighandler
pkg_sighandler_commit = master

PACKAGES += simhash
pkg_simhash_name = simhash
pkg_simhash_description = Simhashing for Erlang -- hashing algorithm to find near-duplicates in binary data.
pkg_simhash_homepage = https://github.com/ferd/simhash
pkg_simhash_fetch = git
pkg_simhash_repo = https://github.com/ferd/simhash
pkg_simhash_commit = master

PACKAGES += simple_bridge
pkg_simple_bridge_name = simple_bridge
pkg_simple_bridge_description = A simple, standardized interface library to Erlang HTTP Servers.
pkg_simple_bridge_homepage = https://github.com/nitrogen/simple_bridge
pkg_simple_bridge_fetch = git
pkg_simple_bridge_repo = https://github.com/nitrogen/simple_bridge
pkg_simple_bridge_commit = master

PACKAGES += simple_oauth2
pkg_simple_oauth2_name = simple_oauth2
pkg_simple_oauth2_description = Simple erlang OAuth2 client module for any http server framework (Google, Facebook, Yandex, Vkontakte are preconfigured)
pkg_simple_oauth2_homepage = https://github.com/virtan/simple_oauth2
pkg_simple_oauth2_fetch = git
pkg_simple_oauth2_repo = https://github.com/virtan/simple_oauth2
pkg_simple_oauth2_commit = master

PACKAGES += skel
pkg_skel_name = skel
pkg_skel_description = A Streaming Process-based Skeleton Library for Erlang
pkg_skel_homepage = https://github.com/ParaPhrase/skel
pkg_skel_fetch = git
pkg_skel_repo = https://github.com/ParaPhrase/skel
pkg_skel_commit = master

PACKAGES += smother
pkg_smother_name = smother
pkg_smother_description = Extended code coverage metrics for Erlang.
pkg_smother_homepage = https://ramsay-t.github.io/Smother/
pkg_smother_fetch = git
pkg_smother_repo = https://github.com/ramsay-t/Smother
pkg_smother_commit = master

PACKAGES += social
pkg_social_name = social
pkg_social_description = Cowboy handler for social login via OAuth2 providers
pkg_social_homepage = https://github.com/dvv/social
pkg_social_fetch = git
pkg_social_repo = https://github.com/dvv/social
pkg_social_commit = master

PACKAGES += spapi_router
pkg_spapi_router_name = spapi_router
pkg_spapi_router_description = Partially-connected Erlang clustering
pkg_spapi_router_homepage = https://github.com/spilgames/spapi-router
pkg_spapi_router_fetch = git
pkg_spapi_router_repo = https://github.com/spilgames/spapi-router
pkg_spapi_router_commit = master

PACKAGES += sqerl
pkg_sqerl_name = sqerl
pkg_sqerl_description = An Erlang-flavoured SQL DSL
pkg_sqerl_homepage = https://github.com/hairyhum/sqerl
pkg_sqerl_fetch = git
pkg_sqerl_repo = https://github.com/hairyhum/sqerl
pkg_sqerl_commit = master

PACKAGES += srly
pkg_srly_name = srly
pkg_srly_description = Native Erlang Unix serial interface
pkg_srly_homepage = https://github.com/msantos/srly
pkg_srly_fetch = git
pkg_srly_repo = https://github.com/msantos/srly
pkg_srly_commit = master

PACKAGES += sshrpc
pkg_sshrpc_name = sshrpc
pkg_sshrpc_description = Erlang SSH RPC module (experimental)
pkg_sshrpc_homepage = https://github.com/jj1bdx/sshrpc
pkg_sshrpc_fetch = git
pkg_sshrpc_repo = https://github.com/jj1bdx/sshrpc
pkg_sshrpc_commit = master

PACKAGES += stable
pkg_stable_name = stable
pkg_stable_description = Library of assorted helpers for Cowboy web server.
pkg_stable_homepage = https://github.com/dvv/stable
pkg_stable_fetch = git
pkg_stable_repo = https://github.com/dvv/stable
pkg_stable_commit = master

PACKAGES += statebox
pkg_statebox_name = statebox
pkg_statebox_description = Erlang state monad with merge/conflict-resolution capabilities. Useful for Riak.
pkg_statebox_homepage = https://github.com/mochi/statebox
pkg_statebox_fetch = git
pkg_statebox_repo = https://github.com/mochi/statebox
pkg_statebox_commit = master

PACKAGES += statebox_riak
pkg_statebox_riak_name = statebox_riak
pkg_statebox_riak_description = Convenience library that makes it easier to use statebox with riak, extracted from best practices in our production code at Mochi Media.
pkg_statebox_riak_homepage = https://github.com/mochi/statebox_riak
pkg_statebox_riak_fetch = git
pkg_statebox_riak_repo = https://github.com/mochi/statebox_riak
pkg_statebox_riak_commit = master

PACKAGES += statman
pkg_statman_name = statman
pkg_statman_description = Efficiently collect massive volumes of metrics inside the Erlang VM
pkg_statman_homepage = https://github.com/knutin/statman
pkg_statman_fetch = git
pkg_statman_repo = https://github.com/knutin/statman
pkg_statman_commit = master

PACKAGES += statsderl
pkg_statsderl_name = statsderl
pkg_statsderl_description = StatsD client (erlang)
pkg_statsderl_homepage = https://github.com/lpgauth/statsderl
pkg_statsderl_fetch = git
pkg_statsderl_repo = https://github.com/lpgauth/statsderl
pkg_statsderl_commit = master

PACKAGES += stdinout_pool
pkg_stdinout_pool_name = stdinout_pool
pkg_stdinout_pool_description = stdinout_pool    : stuff goes in, stuff goes out. there's never any miscommunication.
pkg_stdinout_pool_homepage = https://github.com/mattsta/erlang-stdinout-pool
pkg_stdinout_pool_fetch = git
pkg_stdinout_pool_repo = https://github.com/mattsta/erlang-stdinout-pool
pkg_stdinout_pool_commit = master

PACKAGES += stockdb
pkg_stockdb_name = stockdb
pkg_stockdb_description = Database for storing Stock Exchange quotes in erlang
pkg_stockdb_homepage = https://github.com/maxlapshin/stockdb
pkg_stockdb_fetch = git
pkg_stockdb_repo = https://github.com/maxlapshin/stockdb
pkg_stockdb_commit = master

PACKAGES += stripe
pkg_stripe_name = stripe
pkg_stripe_description = Erlang interface to the stripe.com API
pkg_stripe_homepage = https://github.com/mattsta/stripe-erlang
pkg_stripe_fetch = git
pkg_stripe_repo = https://github.com/mattsta/stripe-erlang
pkg_stripe_commit = v1

PACKAGES += surrogate
pkg_surrogate_name = surrogate
pkg_surrogate_description = Proxy server written in erlang. Supports reverse proxy load balancing and forward proxy with http (including CONNECT), socks4, socks5, and transparent proxy modes.
pkg_surrogate_homepage = https://github.com/skruger/Surrogate
pkg_surrogate_fetch = git
pkg_surrogate_repo = https://github.com/skruger/Surrogate
pkg_surrogate_commit = master

PACKAGES += swab
pkg_swab_name = swab
pkg_swab_description = General purpose buffer handling module
pkg_swab_homepage = https://github.com/crownedgrouse/swab
pkg_swab_fetch = git
pkg_swab_repo = https://github.com/crownedgrouse/swab
pkg_swab_commit = master

PACKAGES += swarm
pkg_swarm_name = swarm
pkg_swarm_description = Fast and simple acceptor pool for Erlang
pkg_swarm_homepage = https://github.com/jeremey/swarm
pkg_swarm_fetch = git
pkg_swarm_repo = https://github.com/jeremey/swarm
pkg_swarm_commit = master

PACKAGES += switchboard
pkg_switchboard_name = switchboard
pkg_switchboard_description = A framework for processing email using worker plugins.
pkg_switchboard_homepage = https://github.com/thusfresh/switchboard
pkg_switchboard_fetch = git
pkg_switchboard_repo = https://github.com/thusfresh/switchboard
pkg_switchboard_commit = master

PACKAGES += syn
pkg_syn_name = syn
pkg_syn_description = A global process registry for Erlang.
pkg_syn_homepage = https://github.com/ostinelli/syn
pkg_syn_fetch = git
pkg_syn_repo = https://github.com/ostinelli/syn
pkg_syn_commit = master

PACKAGES += sync
pkg_sync_name = sync
pkg_sync_description = On-the-fly recompiling and reloading in Erlang.
pkg_sync_homepage = https://github.com/rustyio/sync
pkg_sync_fetch = git
pkg_sync_repo = https://github.com/rustyio/sync
pkg_sync_commit = master

PACKAGES += syntaxerl
pkg_syntaxerl_name = syntaxerl
pkg_syntaxerl_description = Syntax checker for Erlang
pkg_syntaxerl_homepage = https://github.com/ten0s/syntaxerl
pkg_syntaxerl_fetch = git
pkg_syntaxerl_repo = https://github.com/ten0s/syntaxerl
pkg_syntaxerl_commit = master

PACKAGES += syslog
pkg_syslog_name = syslog
pkg_syslog_description = Erlang port driver for interacting with syslog via syslog(3)
pkg_syslog_homepage = https://github.com/Vagabond/erlang-syslog
pkg_syslog_fetch = git
pkg_syslog_repo = https://github.com/Vagabond/erlang-syslog
pkg_syslog_commit = master

PACKAGES += taskforce
pkg_taskforce_name = taskforce
pkg_taskforce_description = Erlang worker pools for controlled parallelisation of arbitrary tasks.
pkg_taskforce_homepage = https://github.com/g-andrade/taskforce
pkg_taskforce_fetch = git
pkg_taskforce_repo = https://github.com/g-andrade/taskforce
pkg_taskforce_commit = master

PACKAGES += tddreloader
pkg_tddreloader_name = tddreloader
pkg_tddreloader_description = Shell utility for recompiling, reloading, and testing code as it changes
pkg_tddreloader_homepage = https://github.com/version2beta/tddreloader
pkg_tddreloader_fetch = git
pkg_tddreloader_repo = https://github.com/version2beta/tddreloader
pkg_tddreloader_commit = master

PACKAGES += tempo
pkg_tempo_name = tempo
pkg_tempo_description = NIF-based date and time parsing and formatting for Erlang.
pkg_tempo_homepage = https://github.com/selectel/tempo
pkg_tempo_fetch = git
pkg_tempo_repo = https://github.com/selectel/tempo
pkg_tempo_commit = master

PACKAGES += ticktick
pkg_ticktick_name = ticktick
pkg_ticktick_description = Ticktick is an id generator for message service.
pkg_ticktick_homepage = https://github.com/ericliang/ticktick
pkg_ticktick_fetch = git
pkg_ticktick_repo = https://github.com/ericliang/ticktick
pkg_ticktick_commit = master

PACKAGES += tinymq
pkg_tinymq_name = tinymq
pkg_tinymq_description = TinyMQ - a diminutive, in-memory message queue
pkg_tinymq_homepage = https://github.com/ChicagoBoss/tinymq
pkg_tinymq_fetch = git
pkg_tinymq_repo = https://github.com/ChicagoBoss/tinymq
pkg_tinymq_commit = master

PACKAGES += tinymt
pkg_tinymt_name = tinymt
pkg_tinymt_description = TinyMT pseudo random number generator for Erlang.
pkg_tinymt_homepage = https://github.com/jj1bdx/tinymt-erlang
pkg_tinymt_fetch = git
pkg_tinymt_repo = https://github.com/jj1bdx/tinymt-erlang
pkg_tinymt_commit = master

PACKAGES += tirerl
pkg_tirerl_name = tirerl
pkg_tirerl_description = Erlang interface to Elastic Search
pkg_tirerl_homepage = https://github.com/inaka/tirerl
pkg_tirerl_fetch = git
pkg_tirerl_repo = https://github.com/inaka/tirerl
pkg_tirerl_commit = master

PACKAGES += traffic_tools
pkg_traffic_tools_name = traffic_tools
pkg_traffic_tools_description = Simple traffic limiting library
pkg_traffic_tools_homepage = https://github.com/systra/traffic_tools
pkg_traffic_tools_fetch = git
pkg_traffic_tools_repo = https://github.com/systra/traffic_tools
pkg_traffic_tools_commit = master

PACKAGES += trails
pkg_trails_name = trails
pkg_trails_description = A couple of improvements over Cowboy Routes
pkg_trails_homepage = http://inaka.github.io/cowboy-trails/
pkg_trails_fetch = git
pkg_trails_repo = https://github.com/inaka/cowboy-trails
pkg_trails_commit = master

PACKAGES += trane
pkg_trane_name = trane
pkg_trane_description = SAX style broken HTML parser in Erlang
pkg_trane_homepage = https://github.com/massemanet/trane
pkg_trane_fetch = git
pkg_trane_repo = https://github.com/massemanet/trane
pkg_trane_commit = master

PACKAGES += transit
pkg_transit_name = transit
pkg_transit_description = transit format for erlang
pkg_transit_homepage = https://github.com/isaiah/transit-erlang
pkg_transit_fetch = git
pkg_transit_repo = https://github.com/isaiah/transit-erlang
pkg_transit_commit = master

PACKAGES += trie
pkg_trie_name = trie
pkg_trie_description = Erlang Trie Implementation
pkg_trie_homepage = https://github.com/okeuday/trie
pkg_trie_fetch = git
pkg_trie_repo = https://github.com/okeuday/trie
pkg_trie_commit = master

PACKAGES += triq
pkg_triq_name = triq
pkg_triq_description = Trifork QuickCheck
pkg_triq_homepage = https://github.com/krestenkrab/triq
pkg_triq_fetch = git
pkg_triq_repo = https://github.com/krestenkrab/triq
pkg_triq_commit = master

PACKAGES += tunctl
pkg_tunctl_name = tunctl
pkg_tunctl_description = Erlang TUN/TAP interface
pkg_tunctl_homepage = https://github.com/msantos/tunctl
pkg_tunctl_fetch = git
pkg_tunctl_repo = https://github.com/msantos/tunctl
pkg_tunctl_commit = master

PACKAGES += twerl
pkg_twerl_name = twerl
pkg_twerl_description = Erlang client for the Twitter Streaming API
pkg_twerl_homepage = https://github.com/lucaspiller/twerl
pkg_twerl_fetch = git
pkg_twerl_repo = https://github.com/lucaspiller/twerl
pkg_twerl_commit = oauth

PACKAGES += twitter_erlang
pkg_twitter_erlang_name = twitter_erlang
pkg_twitter_erlang_description = An Erlang twitter client
pkg_twitter_erlang_homepage = https://github.com/ngerakines/erlang_twitter
pkg_twitter_erlang_fetch = git
pkg_twitter_erlang_repo = https://github.com/ngerakines/erlang_twitter
pkg_twitter_erlang_commit = master

PACKAGES += ucol_nif
pkg_ucol_nif_name = ucol_nif
pkg_ucol_nif_description = ICU based collation Erlang module
pkg_ucol_nif_homepage = https://github.com/refuge/ucol_nif
pkg_ucol_nif_fetch = git
pkg_ucol_nif_repo = https://github.com/refuge/ucol_nif
pkg_ucol_nif_commit = master

PACKAGES += unicorn
pkg_unicorn_name = unicorn
pkg_unicorn_description = Generic configuration server
pkg_unicorn_homepage = https://github.com/shizzard/unicorn
pkg_unicorn_fetch = git
pkg_unicorn_repo = https://github.com/shizzard/unicorn
pkg_unicorn_commit = 0.3.0

PACKAGES += unsplit
pkg_unsplit_name = unsplit
pkg_unsplit_description = Resolves conflicts in Mnesia after network splits
pkg_unsplit_homepage = https://github.com/uwiger/unsplit
pkg_unsplit_fetch = git
pkg_unsplit_repo = https://github.com/uwiger/unsplit
pkg_unsplit_commit = master

PACKAGES += uuid
pkg_uuid_name = uuid
pkg_uuid_description = Erlang UUID Implementation
pkg_uuid_homepage = https://github.com/okeuday/uuid
pkg_uuid_fetch = git
pkg_uuid_repo = https://github.com/okeuday/uuid
pkg_uuid_commit = v1.4.0

PACKAGES += ux
pkg_ux_name = ux
pkg_ux_description = Unicode eXtention for Erlang (Strings, Collation)
pkg_ux_homepage = https://github.com/erlang-unicode/ux
pkg_ux_fetch = git
pkg_ux_repo = https://github.com/erlang-unicode/ux
pkg_ux_commit = master

PACKAGES += vert
pkg_vert_name = vert
pkg_vert_description = Erlang binding to libvirt virtualization API
pkg_vert_homepage = https://github.com/msantos/erlang-libvirt
pkg_vert_fetch = git
pkg_vert_repo = https://github.com/msantos/erlang-libvirt
pkg_vert_commit = master

PACKAGES += verx
pkg_verx_name = verx
pkg_verx_description = Erlang implementation of the libvirtd remote protocol
pkg_verx_homepage = https://github.com/msantos/verx
pkg_verx_fetch = git
pkg_verx_repo = https://github.com/msantos/verx
pkg_verx_commit = master

PACKAGES += vmq_acl
pkg_vmq_acl_name = vmq_acl
pkg_vmq_acl_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_acl_homepage = https://verne.mq/
pkg_vmq_acl_fetch = git
pkg_vmq_acl_repo = https://github.com/erlio/vmq_acl
pkg_vmq_acl_commit = master

PACKAGES += vmq_bridge
pkg_vmq_bridge_name = vmq_bridge
pkg_vmq_bridge_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_bridge_homepage = https://verne.mq/
pkg_vmq_bridge_fetch = git
pkg_vmq_bridge_repo = https://github.com/erlio/vmq_bridge
pkg_vmq_bridge_commit = master

PACKAGES += vmq_graphite
pkg_vmq_graphite_name = vmq_graphite
pkg_vmq_graphite_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_graphite_homepage = https://verne.mq/
pkg_vmq_graphite_fetch = git
pkg_vmq_graphite_repo = https://github.com/erlio/vmq_graphite
pkg_vmq_graphite_commit = master

PACKAGES += vmq_passwd
pkg_vmq_passwd_name = vmq_passwd
pkg_vmq_passwd_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_passwd_homepage = https://verne.mq/
pkg_vmq_passwd_fetch = git
pkg_vmq_passwd_repo = https://github.com/erlio/vmq_passwd
pkg_vmq_passwd_commit = master

PACKAGES += vmq_server
pkg_vmq_server_name = vmq_server
pkg_vmq_server_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_server_homepage = https://verne.mq/
pkg_vmq_server_fetch = git
pkg_vmq_server_repo = https://github.com/erlio/vmq_server
pkg_vmq_server_commit = master

PACKAGES += vmq_snmp
pkg_vmq_snmp_name = vmq_snmp
pkg_vmq_snmp_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_snmp_homepage = https://verne.mq/
pkg_vmq_snmp_fetch = git
pkg_vmq_snmp_repo = https://github.com/erlio/vmq_snmp
pkg_vmq_snmp_commit = master

PACKAGES += vmq_systree
pkg_vmq_systree_name = vmq_systree
pkg_vmq_systree_description = Component of VerneMQ: A distributed MQTT message broker
pkg_vmq_systree_homepage = https://verne.mq/
pkg_vmq_systree_fetch = git
pkg_vmq_systree_repo = https://github.com/erlio/vmq_systree
pkg_vmq_systree_commit = master

PACKAGES += vmstats
pkg_vmstats_name = vmstats
pkg_vmstats_description = tiny Erlang app that works in conjunction with statsderl in order to generate information on the Erlang VM for graphite logs.
pkg_vmstats_homepage = https://github.com/ferd/vmstats
pkg_vmstats_fetch = git
pkg_vmstats_repo = https://github.com/ferd/vmstats
pkg_vmstats_commit = master

PACKAGES += walrus
pkg_walrus_name = walrus
pkg_walrus_description = Walrus - Mustache-like Templating
pkg_walrus_homepage = https://github.com/devinus/walrus
pkg_walrus_fetch = git
pkg_walrus_repo = https://github.com/devinus/walrus
pkg_walrus_commit = master

PACKAGES += webmachine
pkg_webmachine_name = webmachine
pkg_webmachine_description = A REST-based system for building web applications.
pkg_webmachine_homepage = https://github.com/basho/webmachine
pkg_webmachine_fetch = git
pkg_webmachine_repo = https://github.com/basho/webmachine
pkg_webmachine_commit = master

PACKAGES += websocket_client
pkg_websocket_client_name = websocket_client
pkg_websocket_client_description = Erlang websocket client (ws and wss supported)
pkg_websocket_client_homepage = https://github.com/jeremyong/websocket_client
pkg_websocket_client_fetch = git
pkg_websocket_client_repo = https://github.com/jeremyong/websocket_client
pkg_websocket_client_commit = master

PACKAGES += worker_pool
pkg_worker_pool_name = worker_pool
pkg_worker_pool_description = a simple erlang worker pool
pkg_worker_pool_homepage = https://github.com/inaka/worker_pool
pkg_worker_pool_fetch = git
pkg_worker_pool_repo = https://github.com/inaka/worker_pool
pkg_worker_pool_commit = 1.0.2

PACKAGES += wrangler
pkg_wrangler_name = wrangler
pkg_wrangler_description = Import of the Wrangler svn repository.
pkg_wrangler_homepage = http://www.cs.kent.ac.uk/projects/wrangler/Home.html
pkg_wrangler_fetch = git
pkg_wrangler_repo = https://github.com/RefactoringTools/wrangler
pkg_wrangler_commit = master

PACKAGES += wsock
pkg_wsock_name = wsock
pkg_wsock_description = Erlang library to build WebSocket clients and servers
pkg_wsock_homepage = https://github.com/madtrick/wsock
pkg_wsock_fetch = git
pkg_wsock_repo = https://github.com/madtrick/wsock
pkg_wsock_commit = master

PACKAGES += xhttpc
pkg_xhttpc_name = xhttpc
pkg_xhttpc_description = Extensible HTTP Client for Erlang
pkg_xhttpc_homepage = https://github.com/seriyps/xhttpc
pkg_xhttpc_fetch = git
pkg_xhttpc_repo = https://github.com/seriyps/xhttpc
pkg_xhttpc_commit = master

PACKAGES += xref_runner
pkg_xref_runner_name = xref_runner
pkg_xref_runner_description = Erlang Xref Runner (inspired in rebar xref)
pkg_xref_runner_homepage = https://github.com/inaka/xref_runner
pkg_xref_runner_fetch = git
pkg_xref_runner_repo = https://github.com/inaka/xref_runner
pkg_xref_runner_commit = 0.2.0

PACKAGES += yamerl
pkg_yamerl_name = yamerl
pkg_yamerl_description = YAML 1.2 parser in pure Erlang
pkg_yamerl_homepage = https://github.com/yakaz/yamerl
pkg_yamerl_fetch = git
pkg_yamerl_repo = https://github.com/yakaz/yamerl
pkg_yamerl_commit = master

PACKAGES += yamler
pkg_yamler_name = yamler
pkg_yamler_description = libyaml-based yaml loader for Erlang
pkg_yamler_homepage = https://github.com/goertzenator/yamler
pkg_yamler_fetch = git
pkg_yamler_repo = https://github.com/goertzenator/yamler
pkg_yamler_commit = master

PACKAGES += yaws
pkg_yaws_name = yaws
pkg_yaws_description = Yaws webserver
pkg_yaws_homepage = http://yaws.hyber.org
pkg_yaws_fetch = git
pkg_yaws_repo = https://github.com/klacke/yaws
pkg_yaws_commit = master

PACKAGES += zab_engine
pkg_zab_engine_name = zab_engine
pkg_zab_engine_description = zab propotocol implement by erlang
pkg_zab_engine_homepage = https://github.com/xinmingyao/zab_engine
pkg_zab_engine_fetch = git
pkg_zab_engine_repo = https://github.com/xinmingyao/zab_engine
pkg_zab_engine_commit = master

PACKAGES += zeta
pkg_zeta_name = zeta
pkg_zeta_description = HTTP access log parser in Erlang
pkg_zeta_homepage = https://github.com/s1n4/zeta
pkg_zeta_fetch = git
pkg_zeta_repo = https://github.com/s1n4/zeta
pkg_zeta_commit =  

PACKAGES += zippers
pkg_zippers_name = zippers
pkg_zippers_description = A library for functional zipper data structures in Erlang. Read more on zippers
pkg_zippers_homepage = https://github.com/ferd/zippers
pkg_zippers_fetch = git
pkg_zippers_repo = https://github.com/ferd/zippers
pkg_zippers_commit = master

PACKAGES += zlists
pkg_zlists_name = zlists
pkg_zlists_description = Erlang lazy lists library.
pkg_zlists_homepage = https://github.com/vjache/erlang-zlists
pkg_zlists_fetch = git
pkg_zlists_repo = https://github.com/vjache/erlang-zlists
pkg_zlists_commit = master

PACKAGES += zraft_lib
pkg_zraft_lib_name = zraft_lib
pkg_zraft_lib_description = Erlang raft consensus protocol implementation
pkg_zraft_lib_homepage = https://github.com/dreyk/zraft_lib
pkg_zraft_lib_fetch = git
pkg_zraft_lib_repo = https://github.com/dreyk/zraft_lib
pkg_zraft_lib_commit = master

PACKAGES += zucchini
pkg_zucchini_name = zucchini
pkg_zucchini_description = An Erlang INI parser
pkg_zucchini_homepage = https://github.com/devinus/zucchini
pkg_zucchini_fetch = git
pkg_zucchini_repo = https://github.com/devinus/zucchini
pkg_zucchini_commit = master

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: search

define pkg_print
	$(verbose) printf "%s\n" \
		$(if $(call core_eq,$(1),$(pkg_$(1)_name)),,"Pkg name:    $(1)") \
		"App name:    $(pkg_$(1)_name)" \
		"Description: $(pkg_$(1)_description)" \
		"Home page:   $(pkg_$(1)_homepage)" \
		"Fetch with:  $(pkg_$(1)_fetch)" \
		"Repository:  $(pkg_$(1)_repo)" \
		"Commit:      $(pkg_$(1)_commit)" \
		""

endef

search:
ifdef q
	$(foreach p,$(PACKAGES), \
		$(if $(findstring $(call core_lc,$(q)),$(call core_lc,$(pkg_$(p)_name) $(pkg_$(p)_description))), \
			$(call pkg_print,$(p))))
else
	$(foreach p,$(PACKAGES),$(call pkg_print,$(p)))
endif

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-deps distclean-pkg

# Configuration.

IGNORE_DEPS ?=

DEPS_DIR ?= $(CURDIR)/deps
export DEPS_DIR

REBAR_DEPS_DIR = $(DEPS_DIR)
export REBAR_DEPS_DIR

ALL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(filter-out $(IGNORE_DEPS),$(DEPS)))

ifeq ($(filter $(DEPS_DIR),$(subst :, ,$(ERL_LIBS))),)
ifeq ($(ERL_LIBS),)
	ERL_LIBS = $(DEPS_DIR)
else
	ERL_LIBS := $(ERL_LIBS):$(DEPS_DIR)
endif
endif
export ERL_LIBS

# Verbosity.

dep_verbose_0 = @echo " DEP   " $(1);
dep_verbose = $(dep_verbose_$(V))

# Core targets.

ifneq ($(SKIP_DEPS),)
deps::
else
deps:: $(ALL_DEPS_DIRS)
ifneq ($(IS_DEP),1)
	$(verbose) rm -f $(ERLANG_MK_TMP)/deps.log
endif
	$(verbose) mkdir -p $(ERLANG_MK_TMP)
	$(verbose) for dep in $(ALL_DEPS_DIRS) ; do \
		if grep -qs ^$$dep$$ $(ERLANG_MK_TMP)/deps.log; then \
			echo -n; \
		else \
			echo $$dep >> $(ERLANG_MK_TMP)/deps.log; \
			if [ -f $$dep/GNUmakefile ] || [ -f $$dep/makefile ] || [ -f $$dep/Makefile ]; then \
				$(MAKE) -C $$dep IS_DEP=1 || exit $$?; \
			else \
				echo "ERROR: No Makefile to build dependency $$dep."; \
				exit 1; \
			fi \
		fi \
	done
endif

distclean:: distclean-deps distclean-pkg

# Deps related targets.

# @todo rename GNUmakefile and makefile into Makefile first, if they exist
# While Makefile file could be GNUmakefile or makefile,
# in practice only Makefile is needed so far.
define dep_autopatch
	if [ -f $(DEPS_DIR)/$(1)/Makefile ]; then \
		if [ 0 != `grep -c "include ../\w*\.mk" $(DEPS_DIR)/$(1)/Makefile` ]; then \
			$(call dep_autopatch2,$(1)); \
		elif [ 0 != `grep -ci rebar $(DEPS_DIR)/$(1)/Makefile` ]; then \
			$(call dep_autopatch2,$(1)); \
		elif [ -n "`find $(DEPS_DIR)/$(1)/ -type f -name \*.mk -not -name erlang.mk -exec grep -i rebar '{}' \;`" ]; then \
			$(call dep_autopatch2,$(1)); \
		else \
			if [ -f $(DEPS_DIR)/$(1)/erlang.mk ]; then \
				$(call erlang,$(call dep_autopatch_appsrc.erl,$(1))); \
				$(call dep_autopatch_erlang_mk,$(1)); \
			else \
				$(call erlang,$(call dep_autopatch_app.erl,$(1))); \
			fi \
		fi \
	else \
		if [ ! -d $(DEPS_DIR)/$(1)/src/ ]; then \
			$(call dep_autopatch_noop,$(1)); \
		else \
			$(call dep_autopatch2,$(1)); \
		fi \
	fi
endef

define dep_autopatch2
	$(call erlang,$(call dep_autopatch_appsrc.erl,$(1))); \
	if [ -f $(DEPS_DIR)/$(1)/rebar.config -o -f $(DEPS_DIR)/$(1)/rebar.config.script ]; then \
		$(call dep_autopatch_fetch_rebar); \
		$(call dep_autopatch_rebar,$(1)); \
	else \
		$(call dep_autopatch_gen,$(1)); \
	fi
endef

define dep_autopatch_noop
	printf "noop:\n" > $(DEPS_DIR)/$(1)/Makefile
endef

# Overwrite erlang.mk with the current file by default.
ifeq ($(NO_AUTOPATCH_ERLANG_MK),)
define dep_autopatch_erlang_mk
	echo "include $(ERLANG_MK_FILENAME)" > $(DEPS_DIR)/$(1)/erlang.mk
endef
else
define dep_autopatch_erlang_mk
	echo -n
endef
endif

define dep_autopatch_gen
	printf "%s\n" \
		"ERLC_OPTS = +debug_info" \
		"include ../../erlang.mk" > $(DEPS_DIR)/$(1)/Makefile
endef

define dep_autopatch_fetch_rebar
	mkdir -p $(ERLANG_MK_TMP); \
	if [ ! -d $(ERLANG_MK_TMP)/rebar ]; then \
		git clone -q -n -- https://github.com/rebar/rebar $(ERLANG_MK_TMP)/rebar; \
		cd $(ERLANG_MK_TMP)/rebar; \
		git checkout -q 791db716b5a3a7671e0b351f95ddf24b848ee173; \
		$(MAKE); \
		cd -; \
	fi
endef

define dep_autopatch_rebar
	if [ -f $(DEPS_DIR)/$(1)/Makefile ]; then \
		mv $(DEPS_DIR)/$(1)/Makefile $(DEPS_DIR)/$(1)/Makefile.orig.mk; \
	fi; \
	$(call erlang,$(call dep_autopatch_rebar.erl,$(1))); \
	rm -f $(DEPS_DIR)/$(1)/ebin/$(1).app
endef

define dep_autopatch_rebar.erl
	application:set_env(rebar, log_level, debug),
	Conf1 = case file:consult("$(DEPS_DIR)/$(1)/rebar.config") of
		{ok, Conf0} -> Conf0;
		_ -> []
	end,
	{Conf, OsEnv} = fun() ->
		case filelib:is_file("$(DEPS_DIR)/$(1)/rebar.config.script") of
			false -> {Conf1, []};
			true ->
				Bindings0 = erl_eval:new_bindings(),
				Bindings1 = erl_eval:add_binding('CONFIG', Conf1, Bindings0),
				Bindings = erl_eval:add_binding('SCRIPT', "$(DEPS_DIR)/$(1)/rebar.config.script", Bindings1),
				Before = os:getenv(),
				{ok, Conf2} = file:script("$(DEPS_DIR)/$(1)/rebar.config.script", Bindings),
				{Conf2, lists:foldl(fun(E, Acc) -> lists:delete(E, Acc) end, os:getenv(), Before)}
		end
	end(),
	Write = fun (Text) ->
		file:write_file("$(DEPS_DIR)/$(1)/Makefile", Text, [append])
	end,
	Escape = fun (Text) ->
		re:replace(Text, "\\\\$$$$", "\$$$$$$$$", [global, {return, list}])
	end,
	Write("IGNORE_DEPS = edown eper eunit_formatters meck node_package "
		"rebar_lock_deps_plugin rebar_vsn_plugin reltool_util\n"),
	Write("C_SRC_DIR = /path/do/not/exist\n"),
	Write("DRV_CFLAGS = -fPIC\nexport DRV_CFLAGS\n"),
	Write(["ERLANG_ARCH = ", rebar_utils:wordsize(), "\nexport ERLANG_ARCH\n"]),
	fun() ->
		Write("ERLC_OPTS = +debug_info\nexport ERLC_OPTS\n"),
		case lists:keyfind(erl_opts, 1, Conf) of
			false -> ok;
			{_, ErlOpts} ->
				lists:foreach(fun
					({d, D}) ->
						Write("ERLC_OPTS += -D" ++ atom_to_list(D) ++ "=1\n");
					({i, I}) ->
						Write(["ERLC_OPTS += -I ", I, "\n"]);
					({platform_define, Regex, D}) ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("ERLC_OPTS += -D" ++ atom_to_list(D) ++ "=1\n");
							false -> ok
						end;
					({parse_transform, PT}) ->
						Write("ERLC_OPTS += +'{parse_transform, " ++ atom_to_list(PT) ++ "}'\n");
					(_) -> ok
				end, ErlOpts)
		end,
		Write("\n")
	end(),
	fun() ->
		File = case lists:keyfind(deps, 1, Conf) of
			false -> [];
			{_, Deps} ->
				[begin case case Dep of
							{N, S} when is_atom(N), is_list(S) -> {N, {hex, S}};
							{N, S} when is_tuple(S) -> {N, S};
							{N, _, S} -> {N, S};
							{N, _, S, _} -> {N, S};
							_ -> false
						end of
					false -> ok;
					{Name, Source} ->
						{Method, Repo, Commit} = case Source of
							{hex, V} -> {hex, V, undefined};
							{git, R} -> {git, R, master};
							{M, R, {branch, C}} -> {M, R, C};
							{M, R, {ref, C}} -> {M, R, C};
							{M, R, {tag, C}} -> {M, R, C};
							{M, R, C} -> {M, R, C}
						end,
						Write(io_lib:format("DEPS += ~s\ndep_~s = ~s ~s ~s~n", [Name, Name, Method, Repo, Commit]))
				end end || Dep <- Deps]
		end
	end(),
	fun() ->
		case lists:keyfind(erl_first_files, 1, Conf) of
			false -> ok;
			{_, Files} ->
				Names = [[" ", case lists:reverse(F) of
					"lre." ++ Elif -> lists:reverse(Elif);
					Elif -> lists:reverse(Elif)
				end] || "src/" ++ F <- Files],
				Write(io_lib:format("COMPILE_FIRST +=~s\n", [Names]))
		end
	end(),
	FindFirst = fun(F, Fd) ->
		case io:parse_erl_form(Fd, undefined) of
			{ok, {attribute, _, compile, {parse_transform, PT}}, _} ->
				[PT, F(F, Fd)];
			{ok, {attribute, _, compile, CompileOpts}, _} when is_list(CompileOpts) ->
				case proplists:get_value(parse_transform, CompileOpts) of
					undefined -> [F(F, Fd)];
					PT -> [PT, F(F, Fd)]
				end;
			{ok, {attribute, _, include, Hrl}, _} ->
				case file:open("$(DEPS_DIR)/$(1)/include/" ++ Hrl, [read]) of
					{ok, HrlFd} -> [F(F, HrlFd), F(F, Fd)];
					_ ->
						case file:open("$(DEPS_DIR)/$(1)/src/" ++ Hrl, [read]) of
							{ok, HrlFd} -> [F(F, HrlFd), F(F, Fd)];
							_ -> [F(F, Fd)]
						end
				end;
			{ok, {attribute, _, include_lib, "$(1)/include/" ++ Hrl}, _} ->
				{ok, HrlFd} = file:open("$(DEPS_DIR)/$(1)/include/" ++ Hrl, [read]),
				[F(F, HrlFd), F(F, Fd)];
			{ok, {attribute, _, include_lib, Hrl}, _} ->
				case file:open("$(DEPS_DIR)/$(1)/include/" ++ Hrl, [read]) of
					{ok, HrlFd} -> [F(F, HrlFd), F(F, Fd)];
					_ -> [F(F, Fd)]
				end;
			{ok, {attribute, _, import, {Imp, _}}, _} ->
				case file:open("$(DEPS_DIR)/$(1)/src/" ++ atom_to_list(Imp) ++ ".erl", [read]) of
					{ok, ImpFd} -> [Imp, F(F, ImpFd), F(F, Fd)];
					_ -> [F(F, Fd)]
				end;
			{eof, _} ->
				file:close(Fd),
				[];
			_ ->
				F(F, Fd)
		end
	end,
	fun() ->
		ErlFiles = filelib:wildcard("$(DEPS_DIR)/$(1)/src/*.erl"),
		First0 = lists:usort(lists:flatten([begin
			{ok, Fd} = file:open(F, [read]),
			FindFirst(FindFirst, Fd)
		end || F <- ErlFiles])),
		First = lists:flatten([begin
			{ok, Fd} = file:open("$(DEPS_DIR)/$(1)/src/" ++ atom_to_list(M) ++ ".erl", [read]),
			FindFirst(FindFirst, Fd)
		end || M <- First0, lists:member("$(DEPS_DIR)/$(1)/src/" ++ atom_to_list(M) ++ ".erl", ErlFiles)]) ++ First0,
		Write(["COMPILE_FIRST +=", [[" ", atom_to_list(M)] || M <- First,
			lists:member("$(DEPS_DIR)/$(1)/src/" ++ atom_to_list(M) ++ ".erl", ErlFiles)], "\n"])
	end(),
	Write("\n\nrebar_dep: preprocess pre-deps deps pre-app app\n"),
	Write("\npreprocess::\n"),
	Write("\npre-deps::\n"),
	Write("\npre-app::\n"),
	PatchHook = fun(Cmd) ->
		case Cmd of
			"make -C" ++ Cmd1 -> "$$$$\(MAKE) -C" ++ Escape(Cmd1);
			"gmake -C" ++ Cmd1 -> "$$$$\(MAKE) -C" ++ Escape(Cmd1);
			"make " ++ Cmd1 -> "$$$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
			"gmake " ++ Cmd1 -> "$$$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
			_ -> Escape(Cmd)
		end
	end,
	fun() ->
		case lists:keyfind(pre_hooks, 1, Conf) of
			false -> ok;
			{_, Hooks} ->
				[case H of
					{'get-deps', Cmd} ->
						Write("\npre-deps::\n\t" ++ PatchHook(Cmd) ++ "\n");
					{compile, Cmd} ->
						Write("\npre-app::\n\tCC=$$$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
					{Regex, compile, Cmd} ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("\npre-app::\n\tCC=$$$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
							false -> ok
						end;
					_ -> ok
				end || H <- Hooks]
		end
	end(),
	ShellToMk = fun(V) ->
		re:replace(re:replace(V, "(\\\\$$$$)(\\\\w*)", "\\\\1(\\\\2)", [global]),
			"-Werror\\\\b", "", [{return, list}, global])
	end,
	PortSpecs = fun() ->
		case lists:keyfind(port_specs, 1, Conf) of
			false ->
				case filelib:is_dir("$(DEPS_DIR)/$(1)/c_src") of
					false -> [];
					true ->
						[{"priv/" ++ proplists:get_value(so_name, Conf, "$(1)_drv.so"),
							proplists:get_value(port_sources, Conf, ["c_src/*.c"]), []}]
				end;
			{_, Specs} ->
				lists:flatten([case S of
					{Output, Input} -> {ShellToMk(Output), Input, []};
					{Regex, Output, Input} ->
						case rebar_utils:is_arch(Regex) of
							true -> {ShellToMk(Output), Input, []};
							false -> []
						end;
					{Regex, Output, Input, [{env, Env}]} ->
						case rebar_utils:is_arch(Regex) of
							true -> {ShellToMk(Output), Input, Env};
							false -> []
						end
				end || S <- Specs])
		end
	end(),
	PortSpecWrite = fun (Text) ->
		file:write_file("$(DEPS_DIR)/$(1)/c_src/Makefile.erlang.mk", Text, [append])
	end,
	case PortSpecs of
		[] -> ok;
		_ ->
			Write("\npre-app::\n\t$$$$\(MAKE) -f c_src/Makefile.erlang.mk\n"),
			PortSpecWrite(io_lib:format("ERL_CFLAGS = -finline-functions -Wall -fPIC -I ~s/erts-~s/include -I ~s\n",
				[code:root_dir(), erlang:system_info(version), code:lib_dir(erl_interface, include)])),
			PortSpecWrite(io_lib:format("ERL_LDFLAGS = -L ~s -lerl_interface -lei\n",
				[code:lib_dir(erl_interface, lib)])),
			[PortSpecWrite(["\n", E, "\n"]) || E <- OsEnv],
			FilterEnv = fun(Env) ->
				lists:flatten([case E of
					{_, _} -> E;
					{Regex, K, V} ->
						case rebar_utils:is_arch(Regex) of
							true -> {K, V};
							false -> []
						end
				end || E <- Env])
			end,
			MergeEnv = fun(Env) ->
				lists:foldl(fun ({K, V}, Acc) ->
					case lists:keyfind(K, 1, Acc) of
						false -> [{K, rebar_utils:expand_env_variable(V, K, "")}|Acc];
						{_, V0} -> [{K, rebar_utils:expand_env_variable(V, K, V0)}|Acc]
					end
				end, [], Env)
			end,
			PortEnv = case lists:keyfind(port_env, 1, Conf) of
				false -> [];
				{_, PortEnv0} -> FilterEnv(PortEnv0)
			end,
			PortSpec = fun ({Output, Input0, Env}) ->
				filelib:ensure_dir("$(DEPS_DIR)/$(1)/" ++ Output),
				Input = [[" ", I] || I <- Input0],
				PortSpecWrite([
					[["\n", K, " = ", ShellToMk(V)] || {K, V} <- lists:reverse(MergeEnv(PortEnv))],
					case $(PLATFORM) of
						darwin -> "\n\nLDFLAGS += -flat_namespace -undefined suppress";
						_ -> ""
					end,
					"\n\nall:: ", Output, "\n\n",
					"%.o: %.c\n\t$$$$\(CC) -c -o $$$$\@ $$$$\< $$$$\(CFLAGS) $$$$\(ERL_CFLAGS) $$$$\(DRV_CFLAGS) $$$$\(EXE_CFLAGS)\n\n",
					"%.o: %.C\n\t$$$$\(CXX) -c -o $$$$\@ $$$$\< $$$$\(CXXFLAGS) $$$$\(ERL_CFLAGS) $$$$\(DRV_CFLAGS) $$$$\(EXE_CFLAGS)\n\n",
					"%.o: %.cc\n\t$$$$\(CXX) -c -o $$$$\@ $$$$\< $$$$\(CXXFLAGS) $$$$\(ERL_CFLAGS) $$$$\(DRV_CFLAGS) $$$$\(EXE_CFLAGS)\n\n",
					"%.o: %.cpp\n\t$$$$\(CXX) -c -o $$$$\@ $$$$\< $$$$\(CXXFLAGS) $$$$\(ERL_CFLAGS) $$$$\(DRV_CFLAGS) $$$$\(EXE_CFLAGS)\n\n",
					[[Output, ": ", K, " = ", ShellToMk(V), "\n"] || {K, V} <- lists:reverse(MergeEnv(FilterEnv(Env)))],
					Output, ": $$$$\(foreach ext,.c .C .cc .cpp,",
						"$$$$\(patsubst %$$$$\(ext),%.o,$$$$\(filter %$$$$\(ext),$$$$\(wildcard", Input, "))))\n",
					"\t$$$$\(CC) -o $$$$\@ $$$$\? $$$$\(LDFLAGS) $$$$\(ERL_LDFLAGS) $$$$\(DRV_LDFLAGS) $$$$\(EXE_LDFLAGS)",
					case filename:extension(Output) of
						[] -> "\n";
						_ -> " -shared\n"
					end])
			end,
			[PortSpec(S) || S <- PortSpecs]
	end,
	Write("\ninclude $(ERLANG_MK_FILENAME)"),
	RunPlugin = fun(Plugin, Step) ->
		case erlang:function_exported(Plugin, Step, 2) of
			false -> ok;
			true ->
				c:cd("$(DEPS_DIR)/$(1)/"),
				Ret = Plugin:Step({config, "", Conf, dict:new(), dict:new(), dict:new(),
					dict:store(base_dir, "", dict:new())}, undefined),
				io:format("rebar plugin ~p step ~p ret ~p~n", [Plugin, Step, Ret])
		end
	end,
	fun() ->
		case lists:keyfind(plugins, 1, Conf) of
			false -> ok;
			{_, Plugins} ->
				[begin
					case lists:keyfind(deps, 1, Conf) of
						false -> ok;
						{_, Deps} ->
							case lists:keyfind(P, 1, Deps) of
								false -> ok;
								_ ->
									Path = "$(DEPS_DIR)/" ++ atom_to_list(P),
									io:format("~s", [os:cmd("$(MAKE) -C $(DEPS_DIR)/$(1) " ++ Path)]),
									io:format("~s", [os:cmd("$(MAKE) -C " ++ Path ++ " IS_DEP=1")]),
									code:add_patha(Path ++ "/ebin")
							end
					end
				end || P <- Plugins],
				[case code:load_file(P) of
					{module, P} -> ok;
					_ ->
						case lists:keyfind(plugin_dir, 1, Conf) of
							false -> ok;
							{_, PluginsDir} ->
								ErlFile = "$(DEPS_DIR)/$(1)/" ++ PluginsDir ++ "/" ++ atom_to_list(P) ++ ".erl",
								{ok, P, Bin} = compile:file(ErlFile, [binary]),
								{module, P} = code:load_binary(P, ErlFile, Bin)
						end
				end || P <- Plugins],
				[RunPlugin(P, preprocess) || P <- Plugins],
				[RunPlugin(P, pre_compile) || P <- Plugins],
				[RunPlugin(P, compile) || P <- Plugins]
		end
	end(),
	halt()
endef

define dep_autopatch_app.erl
	UpdateModules = fun(App) ->
		case filelib:is_regular(App) of
			false -> ok;
			true ->
				{ok, [{application, $(1), L0}]} = file:consult(App),
				Mods = filelib:fold_files("$(DEPS_DIR)/$(1)/src", "\\\\.erl$$$$", true,
					fun (F, Acc) -> [list_to_atom(filename:rootname(filename:basename(F)))|Acc] end, []),
				L = lists:keystore(modules, 1, L0, {modules, Mods}),
				ok = file:write_file(App, io_lib:format("~p.~n", [{application, $(1), L}]))
		end
	end,
	UpdateModules("$(DEPS_DIR)/$(1)/ebin/$(1).app"),
	halt()
endef

define dep_autopatch_appsrc.erl
	AppSrcOut = "$(DEPS_DIR)/$(1)/src/$(1).app.src",
	AppSrcIn = case filelib:is_regular(AppSrcOut) of false -> "$(DEPS_DIR)/$(1)/ebin/$(1).app"; true -> AppSrcOut end,
	case filelib:is_regular(AppSrcIn) of
		false -> ok;
		true ->
			{ok, [{application, $(1), L0}]} = file:consult(AppSrcIn),
			L1 = lists:keystore(modules, 1, L0, {modules, []}),
			L2 = case lists:keyfind(vsn, 1, L1) of {_, git} -> lists:keyreplace(vsn, 1, L1, {vsn, "git"}); _ -> L1 end,
			L3 = case lists:keyfind(registered, 1, L2) of false -> [{registered, []}|L2]; _ -> L2 end,
			ok = file:write_file(AppSrcOut, io_lib:format("~p.~n", [{application, $(1), L3}])),
			case AppSrcOut of AppSrcIn -> ok; _ -> ok = file:delete(AppSrcIn) end
	end,
	halt()
endef

define dep_fetch_git
	git clone -q -n -- $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)); \
	cd $(DEPS_DIR)/$(call dep_name,$(1)) && git checkout -q $(call dep_commit,$(1));
endef

define dep_fetch_hg
	hg clone -q -U $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)); \
	cd $(DEPS_DIR)/$(call dep_name,$(1)) && hg update -q $(call dep_commit,$(1));
endef

define dep_fetch_svn
	svn checkout -q $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1));
endef

define dep_fetch_cp
	cp -R $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1));
endef

define dep_fetch_hex.erl
	ssl:start(),
	inets:start(),
	{ok, {{_, 200, _}, _, Body}} = httpc:request(get,
		{"https://s3.amazonaws.com/s3.hex.pm/tarballs/$(1)-$(2).tar", []},
		[], [{body_format, binary}]),
	{ok, Files} = erl_tar:extract({binary, Body}, [memory]),
	{_, Source} = lists:keyfind("contents.tar.gz", 1, Files),
	ok = erl_tar:extract({binary, Source}, [{cwd, "$(DEPS_DIR)/$(1)"}, compressed]),
	halt()
endef

# Hex only has a package version. No need to look in the Erlang.mk packages.
define dep_fetch_hex
	$(call erlang,$(call dep_fetch_hex.erl,$(1),$(strip $(word 2,$(dep_$(1))))));
endef

define dep_fetch_fail
	echo "Unknown or invalid dependency: $(1). Please consult the erlang.mk README for instructions." >&2; \
	exit 78;
endef

# Kept for compatibility purposes with older Erlang.mk configuration.
define dep_fetch_legacy
	$(warning WARNING: '$(1)' dependency configuration uses deprecated format.) \
	git clone -q -n -- $(word 1,$(dep_$(1))) $(DEPS_DIR)/$(1); \
	cd $(DEPS_DIR)/$(1) && git checkout -q $(if $(word 2,$(dep_$(1))),$(word 2,$(dep_$(1))),master);
endef

define dep_fetch
	$(if $(dep_$(1)), \
		$(if $(dep_fetch_$(word 1,$(dep_$(1)))), \
			$(word 1,$(dep_$(1))), \
			legacy), \
		$(if $(filter $(1),$(PACKAGES)), \
			$(pkg_$(1)_fetch), \
			fail))
endef

dep_name = $(if $(dep_$(1)),$(1),$(pkg_$(1)_name))
dep_repo = $(patsubst git://github.com/%,https://github.com/%, \
	$(if $(dep_$(1)),$(word 2,$(dep_$(1))),$(pkg_$(1)_repo)))
dep_commit = $(if $(dep_$(1)),$(word 3,$(dep_$(1))),$(pkg_$(1)_commit))

define dep_target
$(DEPS_DIR)/$(1):
	$(verbose) mkdir -p $(DEPS_DIR)
	$(dep_verbose) $(call dep_fetch_$(strip $(call dep_fetch,$(1))),$(1))
	$(verbose) if [ -f $(DEPS_DIR)/$(1)/configure.ac -o -f $(DEPS_DIR)/$(1)/configure.in ]; then \
		echo " AUTO  " $(1); \
		cd $(DEPS_DIR)/$(1) && autoreconf -Wall -vif -I m4; \
	fi
	- $(verbose) if [ -f $(DEPS_DIR)/$(1)/configure ]; then \
		echo " CONF  " $(1); \
		cd $(DEPS_DIR)/$(1) && ./configure; \
	fi
ifeq ($(filter $(1),$(NO_AUTOPATCH)),)
	$(verbose) if [ "$(1)" = "amqp_client" -a "$(RABBITMQ_CLIENT_PATCH)" ]; then \
		if [ ! -d $(DEPS_DIR)/rabbitmq-codegen ]; then \
			echo " PATCH  Downloading rabbitmq-codegen"; \
			git clone https://github.com/rabbitmq/rabbitmq-codegen.git $(DEPS_DIR)/rabbitmq-codegen; \
		fi; \
		if [ ! -d $(DEPS_DIR)/rabbitmq-server ]; then \
			echo " PATCH  Downloading rabbitmq-server"; \
			git clone https://github.com/rabbitmq/rabbitmq-server.git $(DEPS_DIR)/rabbitmq-server; \
		fi; \
		ln -s $(DEPS_DIR)/amqp_client/deps/rabbit_common-0.0.0 $(DEPS_DIR)/rabbit_common; \
	elif [ "$(1)" = "rabbit" -a "$(RABBITMQ_SERVER_PATCH)" ]; then \
		if [ ! -d $(DEPS_DIR)/rabbitmq-codegen ]; then \
			echo " PATCH  Downloading rabbitmq-codegen"; \
			git clone https://github.com/rabbitmq/rabbitmq-codegen.git $(DEPS_DIR)/rabbitmq-codegen; \
		fi \
	else \
		$(call dep_autopatch,$(1)) \
	fi
endif
endef

$(foreach dep,$(DEPS),$(eval $(call dep_target,$(dep))))

distclean-deps:
	$(gen_verbose) rm -rf $(DEPS_DIR)

# External plugins.

DEP_PLUGINS ?=

define core_dep_plugin
-include $(DEPS_DIR)/$(1)

$(DEPS_DIR)/$(1): $(DEPS_DIR)/$(2) ;
endef

$(foreach p,$(DEP_PLUGINS),\
	$(eval $(if $(findstring /,$p),\
		$(call core_dep_plugin,$p,$(firstword $(subst /, ,$p))),\
		$(call core_dep_plugin,$p/plugins.mk,$p))))

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring protobuffs,$(ERLANG_MK_DISABLE_PLUGINS)),)

# Verbosity.

proto_verbose_0 = @echo " PROTO " $(filter %.proto,$(?F));
proto_verbose = $(proto_verbose_$(V))

# Core targets.

define compile_proto
	$(verbose) mkdir -p ebin/ include/
	$(proto_verbose) $(call erlang,$(call compile_proto.erl,$(1)))
	$(proto_verbose) erlc +debug_info -o ebin/ ebin/*.erl
	$(verbose) rm ebin/*.erl
endef

define compile_proto.erl
	[begin
		Dir = filename:dirname(filename:dirname(F)),
		protobuffs_compile:generate_source(F,
			[{output_include_dir, Dir ++ "/include"},
				{output_src_dir, Dir ++ "/ebin"}])
	end || F <- string:tokens("$(1)", " ")],
	halt().
endef

ifneq ($(wildcard src/),)
ebin/$(PROJECT).app:: $(sort $(call core_find,src/,*.proto))
	$(if $(strip $?),$(call compile_proto,$?))
endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: clean-app

# Configuration.

ERLC_OPTS ?= -Werror +debug_info +warn_export_vars +warn_shadow_vars \
	+warn_obsolete_guard # +bin_opt_info +warn_export_all +warn_missing_spec
COMPILE_FIRST ?=
COMPILE_FIRST_PATHS = $(addprefix src/,$(addsuffix .erl,$(COMPILE_FIRST)))
ERLC_EXCLUDE ?=
ERLC_EXCLUDE_PATHS = $(addprefix src/,$(addsuffix .erl,$(ERLC_EXCLUDE)))

ERLC_MIB_OPTS ?=
COMPILE_MIB_FIRST ?=
COMPILE_MIB_FIRST_PATHS = $(addprefix mibs/,$(addsuffix .mib,$(COMPILE_MIB_FIRST)))

# Verbosity.

app_verbose_0 = @echo " APP   " $(PROJECT);
app_verbose = $(app_verbose_$(V))

appsrc_verbose_0 = @echo " APP   " $(PROJECT).app.src;
appsrc_verbose = $(appsrc_verbose_$(V))

erlc_verbose_0 = @echo " ERLC  " $(filter-out $(patsubst %,%.erl,$(ERLC_EXCLUDE)),\
	$(filter %.erl %.core,$(?F)));
erlc_verbose = $(erlc_verbose_$(V))

xyrl_verbose_0 = @echo " XYRL  " $(filter %.xrl %.yrl,$(?F));
xyrl_verbose = $(xyrl_verbose_$(V))

asn1_verbose_0 = @echo " ASN1  " $(filter %.asn1,$(?F));
asn1_verbose = $(asn1_verbose_$(V))

mib_verbose_0 = @echo " MIB   " $(filter %.bin %.mib,$(?F));
mib_verbose = $(mib_verbose_$(V))

# Targets.

ifeq ($(wildcard ebin/test),)
app:: app-build
else
app:: clean app-build
endif

ifeq ($(wildcard src/$(PROJECT)_app.erl),)
define app_file
{application, $(PROJECT), [
	{description, "$(PROJECT_DESCRIPTION)"},
	{vsn, "$(PROJECT_VERSION)"},
	{id, "$(1)"},
	{modules, [$(call comma_list,$(2))]},
	{registered, []},
	{applications, [$(call comma_list,kernel stdlib $(OTP_DEPS) $(DEPS))]}
]}.
endef
else
define app_file
{application, $(PROJECT), [
	{description, "$(PROJECT_DESCRIPTION)"},
	{vsn, "$(PROJECT_VERSION)"},
	{id, "$(1)"},
	{modules, [$(call comma_list,$(2))]},
	{registered, [$(call comma_list,$(PROJECT)_sup $(PROJECT_REGISTERED))]},
	{applications, [$(call comma_list,kernel stdlib $(OTP_DEPS) $(DEPS))]},
	{mod, {$(PROJECT)_app, []}}
]}.
endef
endif

app-build: $(EXTRA_SOURCES) erlc-include ebin/$(PROJECT).app
	$(eval GITDESCRIBE := $(shell git describe --dirty --abbrev=7 --tags --always --first-parent 2>/dev/null || true))
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename $(shell find ebin -type f -name *.beam))))))
ifeq ($(wildcard src/$(PROJECT).app.src),)
	$(app_verbose) echo $(subst $(newline),,$(subst ",\",$(call app_file,$(GITDESCRIBE),$(MODULES)))) \
		> ebin/$(PROJECT).app
else
	$(verbose) if [ -z "$$(grep -E '^[^%]*{\s*modules\s*,' src/$(PROJECT).app.src)" ]; then \
		echo "Empty modules entry not found in $(PROJECT).app.src. Please consult the erlang.mk README for instructions." >&2; \
		exit 1; \
	fi
	$(appsrc_verbose) cat src/$(PROJECT).app.src \
		| sed "s/{[[:space:]]*modules[[:space:]]*,[[:space:]]*\[\]}/{modules, \[$(call comma_list,$(MODULES))\]}/" \
		| sed "s/{id,[[:space:]]*\"git\"}/{id, \"$(GITDESCRIBE)\"}/" \
		> ebin/$(PROJECT).app
endif

erlc-include: $(filter %.erl %.hrl,$(EXTRA_SOURCES))
	- $(verbose) if [ -d ebin/ ]; then \
		find include/ src/ -type f -name \*.hrl -newer ebin -exec touch $(shell find src/ -type f -name "*.erl") \; 2>/dev/null || printf ''; \
	fi

define compile_erl
	$(erlc_verbose) erlc -v $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(filter-out $(ERLC_EXCLUDE_PATHS),\
		$(COMPILE_FIRST_PATHS) $(1))
endef

define compile_xyrl
	$(xyrl_verbose) erlc -v -o ebin/ $(1)
	$(xyrl_verbose) erlc $(ERLC_OPTS) -o ebin/ ebin/*.erl
	$(verbose) rm ebin/*.erl
endef

define compile_asn1
	$(asn1_verbose) erlc -v -I include/ -o ebin/ $(1)
	$(verbose) mv ebin/*.hrl include/
	$(verbose) mv ebin/*.asn1db include/
	$(verbose) rm ebin/*.erl
endef

define compile_mib
	$(mib_verbose) erlc -v $(ERLC_MIB_OPTS) -o priv/mibs/ \
		-I priv/mibs/ $(COMPILE_MIB_FIRST_PATHS) $(1)
	$(mib_verbose) erlc -o include/ -- priv/mibs/*.bin
endef

ifneq ($(wildcard src/),)
ebin/$(PROJECT).app:: erlc-include
	$(verbose) mkdir -p ebin/

ifneq ($(wildcard asn1/),)
ebin/$(PROJECT).app:: $(sort $(call core_find,asn1/,*.asn1) $(filter %.asn1,$(EXTRA_SOURCES)))
	$(verbose) mkdir -p include
	$(if $(strip $?),$(call compile_asn1,$?))
endif

ifneq ($(wildcard mibs/),)
ebin/$(PROJECT).app:: $(sort $(call core_find,mibs/,*.mib) $(filter %.mib,$(EXTRA_SOURCES)))
	$(verbose) mkdir -p priv/mibs/ include
	$(if $(strip $?),$(call compile_mib,$?))
endif

ebin/$(PROJECT).app:: $(sort $(call core_find,src/,*.erl *.core) $(filter %.erl %.core,$(EXTRA_SOURCES)))
	$(if $(strip $?),$(call compile_erl,$?))

ebin/$(PROJECT).app:: $(sort $(call core_find,src/,*.xrl *.yrl) $(filter %.xrl %.yrl,$(EXTRA_SOURCES)))
	$(if $(strip $?),$(call compile_xyrl,$?))
endif

clean:: clean-app

clean-app:
	$(gen_verbose) rm -rf ebin/ priv/mibs/ \
		$(addprefix include/,$(addsuffix .hrl,$(notdir $(basename $(call core_find,mibs/,*.mib)))))
	$(gen_verbose) rm -f $(EXTRA_SOURCES)

# Copyright (c) 2015, Viktor Söderqvist <viktor@zuiderkwast.se>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: docs-deps

# Configuration.

ALL_DOC_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(DOC_DEPS))

# Targets.

$(foreach dep,$(DOC_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
doc-deps:
else
doc-deps: $(ALL_DOC_DEPS_DIRS)
	$(verbose) for dep in $(ALL_DOC_DEPS_DIRS) ; do $(MAKE) -C $$dep; done
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: test-deps test-dir test-build clean-test-dir

# Configuration.

TEST_DIR ?= $(CURDIR)/test

ALL_TEST_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(TEST_DEPS))

TEST_ERLC_OPTS ?= +debug_info +warn_export_vars +warn_shadow_vars +warn_obsolete_guard
TEST_ERLC_OPTS += -DTEST=1

# Targets.

$(foreach dep,$(TEST_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
test-deps:
else
test-deps: $(ALL_TEST_DEPS_DIRS)
	$(verbose) for dep in $(ALL_TEST_DEPS_DIRS) ; do $(MAKE) -C $$dep IS_DEP=1; done
endif

ifneq ($(wildcard $(TEST_DIR)),)
test-dir:
	$(gen_verbose) erlc -v $(TEST_ERLC_OPTS) -I include/ -o $(TEST_DIR) \
		$(call core_find,$(TEST_DIR)/,*.erl) -pa ebin/
endif

ifeq ($(wildcard ebin/test),)
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: clean deps test-deps
	$(verbose) $(MAKE) --no-print-directory app-build test-dir ERLC_OPTS="$(TEST_ERLC_OPTS)"
	$(gen_verbose) touch ebin/test
else
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: deps test-deps
	$(verbose) $(MAKE) --no-print-directory app-build test-dir ERLC_OPTS="$(TEST_ERLC_OPTS)"
endif

clean:: clean-test-dir

clean-test-dir:
ifneq ($(wildcard $(TEST_DIR)/*.beam),)
	$(gen_verbose) rm -f $(TEST_DIR)/*.beam
endif

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring asciidoc,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: asciidoc asciidoc-guide asciidoc-manual install-asciidoc distclean-asciidoc

MAN_INSTALL_PATH ?= /usr/local/share/man
MAN_SECTIONS ?= 3 7

docs:: asciidoc

asciidoc: distclean-asciidoc doc-deps asciidoc-guide asciidoc-manual

ifeq ($(wildcard doc/src/guide/book.asciidoc),)
asciidoc-guide:
else
asciidoc-guide:
	a2x -v -f pdf doc/src/guide/book.asciidoc && mv doc/src/guide/book.pdf doc/guide.pdf
	a2x -v -f chunked doc/src/guide/book.asciidoc && mv doc/src/guide/book.chunked/ doc/html/
endif

ifeq ($(wildcard doc/src/manual/*.asciidoc),)
asciidoc-manual:
else
asciidoc-manual:
	for f in doc/src/manual/*.asciidoc ; do \
		a2x -v -f manpage $$f ; \
	done
	for s in $(MAN_SECTIONS); do \
		mkdir -p doc/man$$s/ ; \
		mv doc/src/manual/*.$$s doc/man$$s/ ; \
		gzip doc/man$$s/*.$$s ; \
	done

install-docs:: install-asciidoc

install-asciidoc: asciidoc-manual
	for s in $(MAN_SECTIONS); do \
		mkdir -p $(MAN_INSTALL_PATH)/man$$s/ ; \
		install -g 0 -o 0 -m 0644 doc/man$$s/*.gz $(MAN_INSTALL_PATH)/man$$s/ ; \
	done
endif

distclean:: distclean-asciidoc

distclean-asciidoc:
	$(gen_verbose) rm -rf doc/html/ doc/guide.pdf doc/man3/ doc/man7/

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2014-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring bootstrap,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: bootstrap bootstrap-lib bootstrap-rel new list-templates

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Bootstrap targets:" \
		"  bootstrap          Generate a skeleton of an OTP application" \
		"  bootstrap-lib      Generate a skeleton of an OTP library" \
		"  bootstrap-rel      Generate the files needed to build a release" \
		"  new t=TPL n=NAME   Generate a module NAME based on the template TPL" \
		"  list-templates     List available templates"

# Bootstrap templates.

define bs_appsrc
{application, $(PROJECT), [
	{description, ""},
	{vsn, "0.1.0"},
	{id, "git"},
	{modules, []},
	{registered, []},
	{applications, [
		kernel,
		stdlib
	]},
	{mod, {$(PROJECT)_app, []}},
	{env, []}
]}.
endef

define bs_appsrc_lib
{application, $(PROJECT), [
	{description, ""},
	{vsn, "0.1.0"},
	{id, "git"},
	{modules, []},
	{registered, []},
	{applications, [
		kernel,
		stdlib
	]}
]}.
endef

ifdef SP
define bs_Makefile
PROJECT = $(PROJECT)

# Whitespace to be used when creating files from templates.
SP = $(SP)

include erlang.mk
endef
else
define bs_Makefile
PROJECT = $(PROJECT)
include erlang.mk
endef
endif

define bs_app
-module($(PROJECT)_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	$(PROJECT)_sup:start_link().

stop(_State) ->
	ok.
endef

define bs_relx_config
{release, {$(PROJECT)_release, "1"}, [$(PROJECT)]}.
{extended_start_script, true}.
{sys_config, "rel/sys.config"}.
{vm_args, "rel/vm.args"}.
endef

define bs_sys_config
[
].
endef

define bs_vm_args
-name $(PROJECT)@127.0.0.1
-setcookie $(PROJECT)
-heart
endef

# Normal templates.

define tpl_supervisor
-module($(n)).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Procs = [],
	{ok, {{one_for_one, 1, 5}, Procs}}.
endef

define tpl_gen_server
-module($(n)).
-behaviour(gen_server).

%% API.
-export([start_link/0]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-record(state, {
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_server:start_link(?MODULE, [], []).

%% gen_server.

init([]) ->
	{ok, #state{}}.

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
endef

define tpl_cowboy_http
-module($(n)).
-behaviour(cowboy_http_handler).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{ok, Req, #state{}}.

handle(Req, State=#state{}) ->
	{ok, Req2} = cowboy_req:reply(200, Req),
	{ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_gen_fsm
-module($(n)).
-behaviour(gen_fsm).

%% API.
-export([start_link/0]).

%% gen_fsm.
-export([init/1]).
-export([state_name/2]).
-export([handle_event/3]).
-export([state_name/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([terminate/3]).
-export([code_change/4]).

-record(state, {
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_fsm:start_link(?MODULE, [], []).

%% gen_fsm.

init([]) ->
	{ok, state_name, #state{}}.

state_name(_Event, StateData) ->
	{next_state, state_name, StateData}.

handle_event(_Event, StateName, StateData) ->
	{next_state, StateName, StateData}.

state_name(_Event, _From, StateData) ->
	{reply, ignored, state_name, StateData}.

handle_sync_event(_Event, _From, StateName, StateData) ->
	{reply, ignored, StateName, StateData}.

handle_info(_Info, StateName, StateData) ->
	{next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData) ->
	ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.
endef

define tpl_cowboy_loop
-module($(n)).
-behaviour(cowboy_loop_handler).

-export([init/3]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{loop, Req, #state{}, 5000, hibernate}.

info(_Info, Req, State) ->
	{loop, Req, State, hibernate}.

terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_cowboy_rest
-module($(n)).

-export([init/3]).
-export([content_types_provided/2]).
-export([get_html/2]).

init(_, _Req, _Opts) ->
	{upgrade, protocol, cowboy_rest}.

content_types_provided(Req, State) ->
	{[{{<<"text">>, <<"html">>, '*'}, get_html}], Req, State}.

get_html(Req, State) ->
	{<<"<html><body>This is REST!</body></html>">>, Req, State}.
endef

define tpl_cowboy_ws
-module($(n)).
-behaviour(cowboy_websocket_handler).

-export([init/3]).
-export([websocket_init/3]).
-export([websocket_handle/3]).
-export([websocket_info/3]).
-export([websocket_terminate/3]).

-record(state, {
}).

init(_, _, _) ->
	{upgrade, protocol, cowboy_websocket}.

websocket_init(_, Req, _Opts) ->
	Req2 = cowboy_req:compact(Req),
	{ok, Req2, #state{}}.

websocket_handle({text, Data}, Req, State) ->
	{reply, {text, Data}, Req, State};
websocket_handle({binary, Data}, Req, State) ->
	{reply, {binary, Data}, Req, State};
websocket_handle(_Frame, Req, State) ->
	{ok, Req, State}.

websocket_info(_Info, Req, State) ->
	{ok, Req, State}.

websocket_terminate(_Reason, _Req, _State) ->
	ok.
endef

define tpl_ranch_protocol
-module($(n)).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-type opts() :: [].
-export_type([opts/0]).

-record(state, {
	socket :: inet:socket(),
	transport :: module()
}).

start_link(Ref, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
	{ok, Pid}.

-spec init(ranch:ref(), inet:socket(), module(), opts()) -> ok.
init(Ref, Socket, Transport, _Opts) ->
	ok = ranch:accept_ack(Ref),
	loop(#state{socket=Socket, transport=Transport}).

loop(State) ->
	loop(State).
endef

# Plugin-specific targets.

define render_template
	$(verbose) echo "$${_$(1)}" > $(2)
endef

ifndef WS
ifdef SP
WS = $(subst a,,a $(wordlist 1,$(SP),a a a a a a a a a a a a a a a a a a a a))
else
WS = $(tab)
endif
endif

$(foreach template,$(filter bs_% tpl_%,$(.VARIABLES)), \
	$(eval _$(template) = $$(subst $$(tab),$$(WS),$$($(template)))) \
	$(eval export _$(template)))

bootstrap:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(call render_template,bs_Makefile,Makefile)
	$(verbose) mkdir src/
	$(call render_template,bs_appsrc,src/$(PROJECT).app.src)
	$(call render_template,bs_app,src/$(PROJECT)_app.erl)
	$(eval n := $(PROJECT)_sup)
	$(call render_template,tpl_supervisor,src/$(PROJECT)_sup.erl)

bootstrap-lib:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(call render_template,bs_Makefile,Makefile)
	$(verbose) mkdir src/
	$(call render_template,bs_appsrc_lib,src/$(PROJECT).app.src)

bootstrap-rel:
ifneq ($(wildcard relx.config),)
	$(error Error: relx.config already exists)
endif
ifneq ($(wildcard rel/),)
	$(error Error: rel/ directory already exists)
endif
	$(call render_template,bs_relx_config,relx.config)
	$(verbose) mkdir rel/
	$(call render_template,bs_sys_config,rel/sys.config)
	$(call render_template,bs_vm_args,rel/vm.args)

new:
ifeq ($(wildcard src/),)
	$(error Error: src/ directory does not exist)
endif
ifndef t
	$(error Usage: $(MAKE) new t=TEMPLATE n=NAME)
endif
ifndef tpl_$(t)
	$(error Unknown template)
endif
ifndef n
	$(error Usage: $(MAKE) new t=TEMPLATE n=NAME)
endif
	$(call render_template,tpl_$(t),src/$(n).erl)

list-templates:
	$(verbose) echo Available templates: $(sort $(patsubst tpl_%,%,$(filter tpl_%,$(.VARIABLES))))

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2014-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring c_src,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: clean-c_src distclean-c_src-env

# Configuration.

C_SRC_DIR ?= $(CURDIR)/c_src
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk
C_SRC_OUTPUT ?= $(CURDIR)/priv/$(PROJECT).so
C_SRC_TYPE ?= shared

# System type and C compiler/flags.

ifeq ($(PLATFORM),darwin)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -arch x86_64 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -arch x86_64 -finline-functions -Wall
	LDFLAGS ?= -arch x86_64 -flat_namespace -undefined suppress
else ifeq ($(PLATFORM),freebsd)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
else ifeq ($(PLATFORM),linux)
	CC ?= gcc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
endif

CFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)
CXXFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)

LDLIBS += -L $(ERL_INTERFACE_LIB_DIR) -lerl_interface -lei

ifeq ($(C_SRC_TYPE),shared)
LDFLAGS += -shared
endif

# Verbosity.

c_verbose_0 = @echo " C     " $(?F);
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(?F);
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

# Targets.

ifeq ($(wildcard $(C_SRC_DIR)),)
else ifneq ($(wildcard $(C_SRC_DIR)/Makefile),)
app:: app-c_src

test-build:: app-c_src

app-c_src:
	$(MAKE) -C $(C_SRC_DIR)

clean::
	$(MAKE) -C $(C_SRC_DIR) clean

else

ifeq ($(SOURCES),)
SOURCES := $(sort $(call core_find,$(C_SRC_DIR)/,*.c *.C *.cc *.cpp))
endif
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

app:: $(C_SRC_ENV) $(C_SRC_OUTPUT)

test-build:: $(C_SRC_ENV) $(C_SRC_OUTPUT)

$(C_SRC_OUTPUT): $(OBJECTS)
	$(verbose) mkdir -p priv/
	$(link_verbose) $(CC) $(OBJECTS) $(LDFLAGS) $(LDLIBS) -o $(C_SRC_OUTPUT)

%.o: %.c
	$(COMPILE_C) $(OUTPUT_OPTION) $<

%.o: %.cc
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.C
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.cpp
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

clean:: clean-c_src

clean-c_src:
	$(gen_verbose) rm -f $(C_SRC_OUTPUT) $(OBJECTS)

endif

ifneq ($(wildcard $(C_SRC_DIR)),)
$(C_SRC_ENV):
	$(verbose) $(ERL) -eval "file:write_file(\"$(C_SRC_ENV)\", \
		io_lib:format( \
			\"ERTS_INCLUDE_DIR ?= ~s/erts-~s/include/~n\" \
			\"ERL_INTERFACE_INCLUDE_DIR ?= ~s~n\" \
			\"ERL_INTERFACE_LIB_DIR ?= ~s~n\", \
			[code:root_dir(), erlang:system_info(version), \
			code:lib_dir(erl_interface, include), \
			code:lib_dir(erl_interface, lib)])), \
		halt()."

distclean:: distclean-c_src-env

distclean-c_src-env:
	$(gen_verbose) rm -f $(C_SRC_ENV)

-include $(C_SRC_ENV)
endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring ci,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: ci ci-setup distclean-kerl

KERL ?= $(CURDIR)/kerl
export KERL

KERL_URL ?= https://raw.githubusercontent.com/yrashk/kerl/master/kerl

OTP_GIT ?= https://github.com/erlang/otp

CI_INSTALL_DIR ?= $(HOME)/erlang
CI_OTP ?=

ifeq ($(strip $(CI_OTP)),)
ci::
else
ci:: $(addprefix ci-,$(CI_OTP))

ci-prepare: $(addprefix $(CI_INSTALL_DIR)/,$(CI_OTP))

ci-setup::

ci_verbose_0 = @echo " CI    " $(1);
ci_verbose = $(ci_verbose_$(V))

define ci_target
ci-$(1): $(CI_INSTALL_DIR)/$(1)
	$(ci_verbose) \
		PATH="$(CI_INSTALL_DIR)/$(1)/bin:$(PATH)" \
		CI_OTP_RELEASE="$(1)" \
		CT_OPTS="-label $(1)" \
		$(MAKE) clean ci-setup tests
endef

$(foreach otp,$(CI_OTP),$(eval $(call ci_target,$(otp))))

define ci_otp_target
ifeq ($(wildcard $(CI_INSTALL_DIR)/$(1)),)
$(CI_INSTALL_DIR)/$(1): $(KERL)
	$(KERL) build git $(OTP_GIT) $(1) $(1)
	$(KERL) install $(1) $(CI_INSTALL_DIR)/$(1)
endif
endef

$(foreach otp,$(CI_OTP),$(eval $(call ci_otp_target,$(otp))))

$(KERL):
	$(gen_verbose) $(call core_http_get,$(KERL),$(KERL_URL))
	$(verbose) chmod +x $(KERL)

help::
	$(verbose) printf "%s\n" "" \
		"Continuous Integration targets:" \
		"  ci          Run '$(MAKE) tests' on all configured Erlang versions." \
		"" \
		"The CI_OTP variable must be defined with the Erlang versions" \
		"that must be tested. For example: CI_OTP = OTP-17.3.4 OTP-17.5.3"

distclean:: distclean-kerl

distclean-kerl:
	$(gen_verbose) rm -rf $(KERL)
endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring ct,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: ct distclean-ct

# Configuration.

CT_OPTS ?=
ifneq ($(wildcard $(TEST_DIR)),)
	CT_SUITES ?= $(sort $(subst _SUITE.erl,,$(notdir $(call core_find,$(TEST_DIR)/,*_SUITE.erl))))
else
	CT_SUITES ?=
endif

# Core targets.

tests:: ct

distclean:: distclean-ct

help::
	$(verbose) printf "%s\n" "" \
		"Common_test targets:" \
		"  ct          Run all the common_test suites for this project" \
		"" \
		"All your common_test suites have their associated targets." \
		"A suite named http_SUITE can be ran using the ct-http target."

# Plugin-specific targets.

CT_RUN = ct_run \
	-no_auto_compile \
	-noinput \
	-pa $(CURDIR)/ebin $(DEPS_DIR)/*/ebin $(TEST_DIR) \
	-dir $(TEST_DIR) \
	-logdir $(CURDIR)/logs

ifeq ($(CT_SUITES),)
ct:
else
ct: test-build
	$(verbose) mkdir -p $(CURDIR)/logs/
	$(gen_verbose) $(CT_RUN) -suite $(addsuffix _SUITE,$(CT_SUITES)) $(CT_OPTS)
endif

define ct_suite_target
ct-$(1): test-build
	$(verbose) mkdir -p $(CURDIR)/logs/
	$(gen_verbose) $(CT_RUN) -suite $(addsuffix _SUITE,$(1)) $(CT_OPTS)
endef

$(foreach test,$(CT_SUITES),$(eval $(call ct_suite_target,$(test))))

distclean-ct:
	$(gen_verbose) rm -rf $(CURDIR)/logs/

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring dialyzer,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: plt distclean-plt dialyze

# Configuration.

DIALYZER_PLT ?= $(CURDIR)/.$(PROJECT).plt
export DIALYZER_PLT

PLT_APPS ?=
DIALYZER_DIRS ?= --src -r src
DIALYZER_OPTS ?= -Werror_handling -Wrace_conditions \
	-Wunmatched_returns # -Wunderspecs

# Core targets.

check:: dialyze

distclean:: distclean-plt

help::
	$(verbose) printf "%s\n" "" \
		"Dialyzer targets:" \
		"  plt         Build a PLT file for this project" \
		"  dialyze     Analyze the project using Dialyzer"

# Plugin-specific targets.

$(DIALYZER_PLT): deps app
	$(verbose) dialyzer --build_plt --apps erts kernel stdlib $(PLT_APPS) $(OTP_DEPS) $(ALL_DEPS_DIRS)

plt: $(DIALYZER_PLT)

distclean-plt:
	$(gen_verbose) rm -f $(DIALYZER_PLT)

ifneq ($(wildcard $(DIALYZER_PLT)),)
dialyze:
else
dialyze: $(DIALYZER_PLT)
endif
	$(verbose) dialyzer --no_native $(DIALYZER_DIRS) $(DIALYZER_OPTS)

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring edoc,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: distclean-edoc edoc

# Configuration.

EDOC_OPTS ?=

# Core targets.

docs:: distclean-edoc edoc

distclean:: distclean-edoc

# Plugin-specific targets.

edoc: doc-deps
	$(gen_verbose) $(ERL) -eval 'edoc:application($(PROJECT), ".", [$(EDOC_OPTS)]), halt().'

distclean-edoc:
	$(gen_verbose) rm -f doc/*.css doc/*.html doc/*.png doc/edoc-info

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2015, Erlang Solutions Ltd.
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring elvis,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: elvis distclean-elvis

# Configuration.

ELVIS_CONFIG ?= $(CURDIR)/elvis.config

ELVIS ?= $(CURDIR)/elvis
export ELVIS

ELVIS_URL ?= https://github.com/inaka/elvis/releases/download/0.2.5-beta2/elvis
ELVIS_CONFIG_URL ?= https://github.com/inaka/elvis/releases/download/0.2.5-beta2/elvis.config
ELVIS_OPTS ?=

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Elvis targets:" \
		"  elvis       Run Elvis using the local elvis.config or download the default otherwise"

distclean:: distclean-elvis

# Plugin-specific targets.

$(ELVIS):
	$(gen_verbose) $(call core_http_get,$(ELVIS),$(ELVIS_URL))
	$(verbose) chmod +x $(ELVIS)

$(ELVIS_CONFIG):
	$(verbose) $(call core_http_get,$(ELVIS_CONFIG),$(ELVIS_CONFIG_URL))

elvis: $(ELVIS) $(ELVIS_CONFIG)
	$(verbose) $(ELVIS) rock -c $(ELVIS_CONFIG) $(ELVIS_OPTS)

distclean-elvis:
	$(gen_verbose) rm -rf $(ELVIS)

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring erlydtl,$(ERLANG_MK_DISABLE_PLUGINS)),)

# Configuration.

DTL_FULL_PATH ?= 0
DTL_PATH ?= templates/
DTL_SUFFIX ?= _dtl

# Verbosity.

dtl_verbose_0 = @echo " DTL   " $(filter %.dtl,$(?F));
dtl_verbose = $(dtl_verbose_$(V))

# Core targets.

define erlydtl_compile.erl
	[begin
		Module0 = case $(DTL_FULL_PATH) of
			0 ->
				filename:basename(F, ".dtl");
			1 ->
				"$(DTL_PATH)" ++ F2 = filename:rootname(F, ".dtl"),
				re:replace(F2, "/",  "_",  [{return, list}, global])
		end,
		Module = list_to_atom(string:to_lower(Module0) ++ "$(DTL_SUFFIX)"),
		case erlydtl:compile(F, Module, [{out_dir, "ebin/"}, return_errors, {doc_root, "templates"}]) of
			ok -> ok;
			{ok, _} -> ok
		end
	end || F <- string:tokens("$(1)", " ")],
	halt().
endef

ifneq ($(wildcard src/),)
ebin/$(PROJECT).app:: $(sort $(call core_find,$(DTL_PATH),*.dtl))
	$(if $(strip $?),\
		$(dtl_verbose) $(call erlang,$(call erlydtl_compile.erl,$?,-pa ebin/ $(DEPS_DIR)/erlydtl/ebin/)))
endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2014 Dave Cottlehuber <dch@skunkwerks.at>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring escript,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: distclean-escript escript

# Configuration.

ESCRIPT_NAME ?= $(PROJECT)
ESCRIPT_COMMENT ?= This is an -*- erlang -*- file

ESCRIPT_BEAMS ?= "ebin/*", "deps/*/ebin/*"
ESCRIPT_SYS_CONFIG ?= "rel/sys.config"
ESCRIPT_EMU_ARGS ?= -pa . \
	-sasl errlog_type error \
	-escript main $(ESCRIPT_NAME)
ESCRIPT_SHEBANG ?= /usr/bin/env escript
ESCRIPT_STATIC ?= "deps/*/priv/**", "priv/**"

# Core targets.

distclean:: distclean-escript

help::
	$(verbose) printf "%s\n" "" \
		"Escript targets:" \
		"  escript     Build an executable escript archive" \

# Plugin-specific targets.

# Based on https://github.com/synrc/mad/blob/master/src/mad_bundle.erl
# Copyright (c) 2013 Maxim Sokhatsky, Synrc Research Center
# Modified MIT License, https://github.com/synrc/mad/blob/master/LICENSE :
# Software may only be used for the great good and the true happiness of all
# sentient beings.

define ESCRIPT_RAW
'Read = fun(F) -> {ok, B} = file:read_file(filename:absname(F)), B end,'\
'Files = fun(L) -> A = lists:concat([filelib:wildcard(X)||X<- L ]),'\
'  [F || F <- A, not filelib:is_dir(F) ] end,'\
'Squash = fun(L) -> [{filename:basename(F), Read(F) } || F <- L ] end,'\
'Zip = fun(A, L) -> {ok,{_,Z}} = zip:create(A, L, [{compress,all},memory]), Z end,'\
'Ez = fun(Escript) ->'\
'  Static = Files([$(ESCRIPT_STATIC)]),'\
'  Beams = Squash(Files([$(ESCRIPT_BEAMS), $(ESCRIPT_SYS_CONFIG)])),'\
'  Archive = Beams ++ [{ "static.gz", Zip("static.gz", Static)}],'\
'  escript:create(Escript, [ $(ESCRIPT_OPTIONS)'\
'    {archive, Archive, [memory]},'\
'    {shebang, "$(ESCRIPT_SHEBANG)"},'\
'    {comment, "$(ESCRIPT_COMMENT)"},'\
'    {emu_args, " $(ESCRIPT_EMU_ARGS)"}'\
'  ]),'\
'  file:change_mode(Escript, 8#755)'\
'end,'\
'Ez("$(ESCRIPT_NAME)"),'\
'halt().'
endef

ESCRIPT_COMMAND = $(subst ' ',,$(ESCRIPT_RAW))

escript:: distclean-escript deps app
	$(gen_verbose) $(ERL) -eval $(ESCRIPT_COMMAND)

distclean-escript:
	$(gen_verbose) rm -f $(ESCRIPT_NAME)

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2014, Enrique Fernandez <enrique.fernandez@erlang-solutions.com>
# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring eunit,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: eunit

# Configuration

EUNIT_OPTS ?=

# Core targets.

tests:: eunit

help::
	$(verbose) printf "%s\n" "" \
		"EUnit targets:" \
		"  eunit       Run all the EUnit tests for this project"

# Plugin-specific targets.

define eunit.erl
	case "$(COVER)" of
		"" -> ok;
		_ ->
			case cover:compile_beam_directory("ebin") of
				{error, _} -> halt(1);
				_ -> ok
			end
	end,
	case eunit:test([$(call comma_list,$(1))], [$(EUNIT_OPTS)]) of
		ok -> ok;
		error -> halt(2)
	end,
	case "$(COVER)" of
		"" -> ok;
		_ ->
			cover:export("eunit.coverdata")
	end,
	halt()
endef

EUNIT_EBIN_MODS = $(notdir $(basename $(call core_find,ebin/,*.beam)))
EUNIT_TEST_MODS = $(notdir $(basename $(call core_find,$(TEST_DIR)/,*.beam)))
EUNIT_MODS = $(foreach mod,$(EUNIT_EBIN_MODS) $(filter-out \
	$(patsubst %,%_tests,$(EUNIT_EBIN_MODS)),$(EUNIT_TEST_MODS)),{module,'$(mod)'})

eunit: test-build
	$(gen_verbose) $(ERL) -pa $(TEST_DIR) $(DEPS_DIR)/*/ebin ebin \
		-eval "$(subst $(newline),,$(subst ",\",$(call eunit.erl,$(EUNIT_MODS))))"

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring relx,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: relx-rel distclean-relx-rel distclean-relx run

# Configuration.

RELX_CONFIG ?= $(CURDIR)/relx.config

RELX ?= $(CURDIR)/relx
export RELX

RELX_URL ?= https://github.com/erlware/relx/releases/download/v2.0.0/relx
RELX_OPTS ?=
RELX_OUTPUT_DIR ?= _rel

ifeq ($(firstword $(RELX_OPTS)),-o)
	RELX_OUTPUT_DIR = $(word 2,$(RELX_OPTS))
else
	RELX_OPTS += -o $(RELX_OUTPUT_DIR)
endif

# Core targets.

ifeq ($(IS_DEP),)
ifneq ($(wildcard $(RELX_CONFIG)),)
rel:: distclean-relx-rel relx-rel
endif
endif

distclean:: distclean-relx-rel distclean-relx

# Plugin-specific targets.

$(RELX):
	$(gen_verbose) $(call core_http_get,$(RELX),$(RELX_URL))
	$(verbose) chmod +x $(RELX)

relx-rel: $(RELX)
	$(verbose) $(RELX) -c $(RELX_CONFIG) $(RELX_OPTS)

distclean-relx-rel:
	$(gen_verbose) rm -rf $(RELX_OUTPUT_DIR)

distclean-relx:
	$(gen_verbose) rm -rf $(RELX)

# Run target.

ifeq ($(wildcard $(RELX_CONFIG)),)
run:
else

define get_relx_release.erl
	{ok, Config} = file:consult("$(RELX_CONFIG)"),
	{release, {Name, _}, _} = lists:keyfind(release, 1, Config),
	io:format("~s", [Name]),
	halt(0).
endef

RELX_RELEASE = `$(call erlang,$(get_relx_release.erl))`

run: all
	$(verbose) $(RELX_OUTPUT_DIR)/$(RELX_RELEASE)/bin/$(RELX_RELEASE) console

help::
	$(verbose) printf "%s\n" "" \
		"Relx targets:" \
		"  run         Compile the project, build the release and run it"

endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2014, M Robert Martin <rob@version2beta.com>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring shell,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: shell

# Configuration.

SHELL_PATH ?= -pa $(CURDIR)/ebin $(DEPS_DIR)/*/ebin
SHELL_OPTS ?=

ALL_SHELL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(SHELL_DEPS))

# Core targets

help::
	$(verbose) printf "%s\n" "" \
		"Shell targets:" \
		"  shell       Run an erlang shell with SHELL_OPTS or reasonable default"

# Plugin-specific targets.

$(foreach dep,$(SHELL_DEPS),$(eval $(call dep_target,$(dep))))

build-shell-deps: $(ALL_SHELL_DEPS_DIRS)
	$(verbose) for dep in $(ALL_SHELL_DEPS_DIRS) ; do $(MAKE) -C $$dep ; done

shell: build-shell-deps
	$(gen_verbose) erl $(SHELL_PATH) $(SHELL_OPTS)

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2015, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring triq,$(ERLANG_MK_DISABLE_PLUGINS)),)

ifeq ($(filter triq,$(DEPS) $(TEST_DEPS)),triq)
.PHONY: triq

# Targets.

tests:: triq

define triq_check.erl
	code:add_pathsa(["$(CURDIR)/ebin", "$(DEPS_DIR)/*/ebin"]),
	try
		case $(1) of
			all -> [true] =:= lists:usort([triq:check(M) || M <- [$(call comma_list,$(3))]]);
			module -> triq:check($(2));
			function -> triq:check($(2))
		end
	of
		true -> halt(0);
		_ -> halt(1)
	catch error:undef ->
		io:format("Undefined property or module~n"),
		halt(0)
	end.
endef

ifdef t
ifeq (,$(findstring :,$(t)))
triq: test-build
	$(verbose) $(call erlang,$(call triq_check.erl,module,$(t)))
else
triq: test-build
	$(verbose) echo Testing $(t)/0
	$(verbose) $(call erlang,$(call triq_check.erl,function,$(t)()))
endif
else
triq: test-build
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename $(wildcard ebin/*.beam))))))
	$(gen_verbose) $(call erlang,$(call triq_check.erl,all,undefined,$(MODULES)))
endif
endif

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright (c) 2015, Erlang Solutions Ltd.
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring xref,$(ERLANG_MK_DISABLE_PLUGINS)),)

.PHONY: xref distclean-xref

# Configuration.

ifeq ($(XREF_CONFIG),)
	XREF_ARGS :=
else
	XREF_ARGS := -c $(XREF_CONFIG)
endif

XREFR ?= $(CURDIR)/xrefr
export XREFR

XREFR_URL ?= https://github.com/inaka/xref_runner/releases/download/0.2.2/xrefr

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Xref targets:" \
		"  xref        Run Xrefr using $XREF_CONFIG as config file if defined"

distclean:: distclean-xref

# Plugin-specific targets.

$(XREFR):
	$(gen_verbose) $(call core_http_get,$(XREFR),$(XREFR_URL))
	$(verbose) chmod +x $(XREFR)

xref: deps app $(XREFR)
	$(gen_verbose) $(XREFR) $(XREFR_ARGS)

distclean-xref:
	$(gen_verbose) rm -rf $(XREFR)

endif # ERLANG_MK_DISABLE_PLUGINS

# Copyright 2015, Viktor Söderqvist <viktor@zuiderkwast.se>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(findstring cover,$(ERLANG_MK_DISABLE_PLUGINS)),)

COVER_REPORT_DIR = cover

# Hook in coverage to ct

ifdef COVER
ifdef CT_RUN
# All modules in 'ebin'
COVER_MODS = $(notdir $(basename $(call core_ls,ebin/*.beam)))

test-build:: $(TEST_DIR)/ct.cover.spec

$(TEST_DIR)/ct.cover.spec:
	$(verbose) echo Cover mods: $(COVER_MODS)
	$(gen_verbose) printf "%s\n" \
		'{incl_mods,[$(subst $(space),$(comma),$(COVER_MODS))]}.' \
		'{export,"$(CURDIR)/ct.coverdata"}.' > $@

CT_RUN += -cover $(TEST_DIR)/ct.cover.spec
endif
endif

# Core targets

ifdef COVER
ifneq ($(COVER_REPORT_DIR),)
tests::
	$(verbose) $(MAKE) --no-print-directory cover-report
endif
endif

clean:: coverdata-clean

ifneq ($(COVER_REPORT_DIR),)
distclean:: cover-report-clean
endif

help::
	$(verbose) printf "%s\n" "" \
		"Cover targets:" \
		"  cover-report  Generate a HTML coverage report from previously collected" \
		"                cover data." \
		"  all.coverdata Merge {eunit,ct}.coverdata into one coverdata file." \
		"" \
		"If COVER=1 is set, coverage data is generated by the targets eunit and ct. The" \
		"target tests additionally generates a HTML coverage report from the combined" \
		"coverdata files from each of these testing tools. HTML reports can be disabled" \
		"by setting COVER_REPORT_DIR to empty."

# Plugin specific targets

COVERDATA = $(filter-out all.coverdata,$(wildcard *.coverdata))

.PHONY: coverdata-clean
coverdata-clean:
	$(gen_verbose) rm -f *.coverdata ct.cover.spec

# Merge all coverdata files into one.
all.coverdata: $(COVERDATA)
	$(gen_verbose) $(ERL) -eval ' \
		$(foreach f,$(COVERDATA),cover:import("$(f)") == ok orelse halt(1),) \
		cover:export("$@"), halt(0).'

# These are only defined if COVER_REPORT_DIR is non-empty. Set COVER_REPORT_DIR to
# empty if you want the coverdata files but not the HTML report.
ifneq ($(COVER_REPORT_DIR),)

.PHONY: cover-report-clean cover-report

cover-report-clean:
	$(gen_verbose) rm -rf $(COVER_REPORT_DIR)

ifeq ($(COVERDATA),)
cover-report:
else

# Modules which include eunit.hrl always contain one line without coverage
# because eunit defines test/0 which is never called. We compensate for this.
EUNIT_HRL_MODS = $(subst $(space),$(comma),$(shell \
	grep -e '^\s*-include.*include/eunit\.hrl"' src/*.erl \
	| sed "s/^src\/\(.*\)\.erl:.*/'\1'/" | uniq))

define cover_report.erl
	$(foreach f,$(COVERDATA),cover:import("$(f)") == ok orelse halt(1),)
	Ms = cover:imported_modules(),
	[cover:analyse_to_file(M, "$(COVER_REPORT_DIR)/" ++ atom_to_list(M)
		++ ".COVER.html", [html])  || M <- Ms],
	Report = [begin {ok, R} = cover:analyse(M, module), R end || M <- Ms],
	EunitHrlMods = [$(EUNIT_HRL_MODS)],
	Report1 = [{M, {Y, case lists:member(M, EunitHrlMods) of
		true -> N - 1; false -> N end}} || {M, {Y, N}} <- Report],
	TotalY = lists:sum([Y || {_, {Y, _}} <- Report1]),
	TotalN = lists:sum([N || {_, {_, N}} <- Report1]),
	TotalPerc = round(100 * TotalY / (TotalY + TotalN)),
	{ok, F} = file:open("$(COVER_REPORT_DIR)/index.html", [write]),
	io:format(F, "<!DOCTYPE html><html>~n"
		"<head><meta charset=\"UTF-8\">~n"
		"<title>Coverage report</title></head>~n"
		"<body>~n", []),
	io:format(F, "<h1>Coverage</h1>~n<p>Total: ~p%</p>~n", [TotalPerc]),
	io:format(F, "<table><tr><th>Module</th><th>Coverage</th></tr>~n", []),
	[io:format(F, "<tr><td><a href=\"~p.COVER.html\">~p</a></td>"
		"<td>~p%</td></tr>~n",
		[M, M, round(100 * Y / (Y + N))]) || {M, {Y, N}} <- Report1],
	How = "$(subst $(space),$(comma)$(space),$(basename $(COVERDATA)))",
	Date = "$(shell date -u "+%Y-%m-%dT%H:%M:%SZ")",
	io:format(F, "</table>~n"
		"<p>Generated using ~s and erlang.mk on ~s.</p>~n"
		"</body></html>", [How, Date]),
	halt().
endef

cover-report:
	$(gen_verbose) mkdir -p $(COVER_REPORT_DIR)
	$(gen_verbose) $(call erlang,$(cover_report.erl))

endif
endif # ifneq ($(COVER_REPORT_DIR),)

endif # ERLANG_MK_DISABLE_PLUGINS
