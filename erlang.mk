# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
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

.PHONY: all app apps deps search rel relup docs install-docs check tests clean distclean help erlang-mk

ERLANG_MK_FILENAME := $(realpath $(lastword $(MAKEFILE_LIST)))
export ERLANG_MK_FILENAME

ERLANG_MK_VERSION = e13b4c7
ERLANG_MK_WITHOUT = 

# Make 3.81 and 3.82 are deprecated.

ifeq ($(MAKELEVEL)$(MAKE_VERSION),03.81)
$(warning Please upgrade to GNU Make 4 or later: https://erlang.mk/guide/installation.html)
endif

ifeq ($(MAKELEVEL)$(MAKE_VERSION),03.82)
$(warning Please upgrade to GNU Make 4 or later: https://erlang.mk/guide/installation.html)
endif

# Core configuration.

PROJECT ?= $(notdir $(CURDIR))
PROJECT := $(strip $(PROJECT))

PROJECT_VERSION ?= rolling
PROJECT_MOD ?=
PROJECT_ENV ?= []

# Verbosity.

V ?= 0

verbose_0 = @
verbose_2 = set -x;
verbose = $(verbose_$(V))

ifeq ($V,3)
SHELL := $(SHELL) -x
endif

gen_verbose_0 = @echo " GEN   " $@;
gen_verbose_2 = set -x;
gen_verbose = $(gen_verbose_$(V))

gen_verbose_esc_0 = @echo " GEN   " $$@;
gen_verbose_esc_2 = set -x;
gen_verbose_esc = $(gen_verbose_esc_$(V))

# Temporary files directory.

ERLANG_MK_TMP ?= $(CURDIR)/.erlang.mk
export ERLANG_MK_TMP

# "erl" command.

ERL = erl -noinput -boot no_dot_erlang -kernel start_distribution false +P 1024 +Q 1024

# Platform detection.

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
else ifeq ($(UNAME_S),DragonFly)
PLATFORM = dragonfly
else ifeq ($(shell uname -o),Msys)
PLATFORM = msys2
else
$(error Unable to detect platform. Please open a ticket with the output of uname -a.)
endif

export PLATFORM
endif

# Core targets.

all:: deps app rel

# Noop to avoid a Make warning when there's nothing to do.
rel::
	$(verbose) :

relup:: deps app

check:: tests

clean:: clean-crashdump

clean-crashdump:
ifneq ($(wildcard erl_crash.dump),)
	$(gen_verbose) rm -f erl_crash.dump
endif

distclean:: clean distclean-tmp

$(ERLANG_MK_TMP):
	$(verbose) mkdir -p $(ERLANG_MK_TMP)

distclean-tmp:
	$(gen_verbose) rm -rf $(ERLANG_MK_TMP)

help::
	$(verbose) printf "%s\n" \
		"erlang.mk (version $(ERLANG_MK_VERSION)) is distributed under the terms of the ISC License." \
		"Copyright (c) 2013-2016 Loïc Hoguin <essen@ninenines.eu>" \
		"" \
		"Usage: [V=1] $(MAKE) [target]..." \
		"" \
		"Core targets:" \
		"  all           Run deps, app and rel targets in that order" \
		"  app           Compile the project" \
		"  deps          Fetch dependencies (if needed) and compile them" \
		"  fetch-deps    Fetch dependencies recursively (if needed) without compiling them" \
		"  list-deps     List dependencies recursively on stdout" \
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
$(subst $(space),$(comma),$(strip $1))
endef

define escape_dquotes
$(subst ",\",$1)
endef

# Adding erlang.mk to make Erlang scripts who call init:get_plain_arguments() happy.
define erlang
$(ERL) $2 -pz $(ERLANG_MK_TMP)/rebar3/_build/prod/lib/*/ebin/ -eval "$(subst $(newline),,$(call escape_dquotes,$1))" -- erlang.mk
endef

ifeq ($(PLATFORM),msys2)
core_native_path = $(shell cygpath -m $1)
else
core_native_path = $1
endif

core_http_get = curl -Lf$(if $(filter-out 0,$V),,s)o $(call core_native_path,$1) $2

core_eq = $(and $(findstring $1,$2),$(findstring $2,$1))

# We skip files that contain spaces because they end up causing issues.
# Files that begin with a dot are already ignored by the wildcard function.
core_find = $(foreach f,$(wildcard $(1:%/=%)/*),$(if $(wildcard $f/.),$(call core_find,$f,$2),$(if $(filter $(subst *,%,$2),$f),$(if $(wildcard $f),$f))))

core_lc = $(subst A,a,$(subst B,b,$(subst C,c,$(subst D,d,$(subst E,e,$(subst F,f,$(subst G,g,$(subst H,h,$(subst I,i,$(subst J,j,$(subst K,k,$(subst L,l,$(subst M,m,$(subst N,n,$(subst O,o,$(subst P,p,$(subst Q,q,$(subst R,r,$(subst S,s,$(subst T,t,$(subst U,u,$(subst V,v,$(subst W,w,$(subst X,x,$(subst Y,y,$(subst Z,z,$1))))))))))))))))))))))))))

core_ls = $(filter-out $1,$(shell echo $1))

# @todo Use a solution that does not require using perl.
core_relpath = $(shell perl -e 'use File::Spec; print File::Spec->abs2rel(@ARGV) . "\n"' $1 $2)

define core_render
	printf -- '$(subst $(newline),\n,$(subst %,%%,$(subst ','\'',$(subst $(tab),$(WS),$(call $1)))))\n' > $2
endef

# Automated update.

ERLANG_MK_REPO ?= https://github.com/ninenines/erlang.mk
ERLANG_MK_COMMIT ?=
ERLANG_MK_BUILD_CONFIG ?= build.config
ERLANG_MK_BUILD_DIR ?= .erlang.mk.build

erlang-mk: WITHOUT ?= $(ERLANG_MK_WITHOUT)
erlang-mk:
ifdef ERLANG_MK_COMMIT
	$(verbose) git clone $(ERLANG_MK_REPO) $(ERLANG_MK_BUILD_DIR)
	$(verbose) cd $(ERLANG_MK_BUILD_DIR) && git checkout $(ERLANG_MK_COMMIT)
else
	$(verbose) git clone --depth 1 $(ERLANG_MK_REPO) $(ERLANG_MK_BUILD_DIR)
endif
	$(verbose) if [ -f $(ERLANG_MK_BUILD_CONFIG) ]; then cp $(ERLANG_MK_BUILD_CONFIG) $(ERLANG_MK_BUILD_DIR)/build.config; fi
	$(gen_verbose) $(MAKE) --no-print-directory -C $(ERLANG_MK_BUILD_DIR) WITHOUT='$(strip $(WITHOUT))' UPGRADE=1
	$(verbose) cp $(ERLANG_MK_BUILD_DIR)/erlang.mk ./erlang.mk
	$(verbose) rm -rf $(ERLANG_MK_BUILD_DIR)
	$(verbose) rm -rf $(ERLANG_MK_TMP)

# The erlang.mk package index is bundled in the default erlang.mk build.
# Search for the string "copyright" to skip to the rest of the code.

# Copyright (c) 2015-2017, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-kerl

KERL_INSTALL_DIR ?= $(HOME)/erlang

ifeq ($(strip $(KERL)),)
KERL := $(ERLANG_MK_TMP)/kerl/kerl
endif

KERL_DIR = $(ERLANG_MK_TMP)/kerl

export KERL

KERL_GIT ?= https://github.com/kerl/kerl
KERL_COMMIT ?= master

KERL_MAKEFLAGS ?=

OTP_GIT ?= https://github.com/erlang/otp

define kerl_otp_target
$(KERL_INSTALL_DIR)/$1: $(KERL)
	$(verbose) if [ ! -d $$@ ]; then \
		MAKEFLAGS="$(KERL_MAKEFLAGS)" $(KERL) build git $(OTP_GIT) $1 $1; \
		$(KERL) install $1 $(KERL_INSTALL_DIR)/$1; \
	fi
endef

$(KERL): $(KERL_DIR)

$(KERL_DIR): | $(ERLANG_MK_TMP)
	$(gen_verbose) git clone --depth 1 $(KERL_GIT) $(ERLANG_MK_TMP)/kerl
	$(verbose) cd $(ERLANG_MK_TMP)/kerl && git checkout $(KERL_COMMIT)
	$(verbose) chmod +x $(KERL)

distclean:: distclean-kerl

distclean-kerl:
	$(gen_verbose) rm -rf $(KERL_DIR)

# Allow users to select which version of Erlang/OTP to use for a project.

ifneq ($(strip $(LATEST_ERLANG_OTP)),)
# In some environments it is necessary to filter out master.
ERLANG_OTP := $(notdir $(lastword $(sort\
	$(filter-out $(KERL_INSTALL_DIR)/master $(KERL_INSTALL_DIR)/OTP_R%,\
	$(filter-out %-rc1 %-rc2 %-rc3,$(wildcard $(KERL_INSTALL_DIR)/*[^-native]))))))
endif

ERLANG_OTP ?=

# Use kerl to enforce a specific Erlang/OTP version for a project.
ifneq ($(strip $(ERLANG_OTP)),)

export PATH := $(KERL_INSTALL_DIR)/$(ERLANG_OTP)/bin:$(PATH)
SHELL := env PATH=$(PATH) $(SHELL)
$(eval $(call kerl_otp_target,$(ERLANG_OTP)))

# Build Erlang/OTP only if it doesn't already exist.
ifeq ($(wildcard $(KERL_INSTALL_DIR)/$(ERLANG_OTP))$(BUILD_ERLANG_OTP),)
$(info Building Erlang/OTP $(ERLANG_OTP)... Please wait...)
$(shell $(MAKE) $(KERL_INSTALL_DIR)/$(ERLANG_OTP) ERLANG_OTP=$(ERLANG_OTP) BUILD_ERLANG_OTP=1 >&2)
endif

endif

PACKAGES += asciideck
pkg_asciideck_name = asciideck
pkg_asciideck_description = Asciidoc for Erlang.
pkg_asciideck_homepage = https://ninenines.eu
pkg_asciideck_fetch = git
pkg_asciideck_repo = https://github.com/ninenines/asciideck
pkg_asciideck_commit = master

PACKAGES += cowboy
pkg_cowboy_name = cowboy
pkg_cowboy_description = Small, fast and modular HTTP server.
pkg_cowboy_homepage = http://ninenines.eu
pkg_cowboy_fetch = git
pkg_cowboy_repo = https://github.com/ninenines/cowboy
pkg_cowboy_commit = master

PACKAGES += cowlib
pkg_cowlib_name = cowlib
pkg_cowlib_description = Support library for manipulating Web protocols.
pkg_cowlib_homepage = http://ninenines.eu
pkg_cowlib_fetch = git
pkg_cowlib_repo = https://github.com/ninenines/cowlib
pkg_cowlib_commit = master

PACKAGES += elixir
pkg_elixir_name = elixir
pkg_elixir_description = Elixir is a dynamic, functional language for building scalable and maintainable applications.
pkg_elixir_homepage = https://elixir-lang.org
pkg_elixir_fetch = git
pkg_elixir_repo = https://github.com/elixir-lang/elixir
pkg_elixir_commit = main

PACKAGES += erlydtl
pkg_erlydtl_name = erlydtl
pkg_erlydtl_description = Django Template Language for Erlang.
pkg_erlydtl_homepage = https://github.com/erlydtl/erlydtl
pkg_erlydtl_fetch = git
pkg_erlydtl_repo = https://github.com/erlydtl/erlydtl
pkg_erlydtl_commit = master

PACKAGES += gpb
pkg_gpb_name = gpb
pkg_gpb_description = A Google Protobuf implementation for Erlang
pkg_gpb_homepage = https://github.com/tomas-abrahamsson/gpb
pkg_gpb_fetch = git
pkg_gpb_repo = https://github.com/tomas-abrahamsson/gpb
pkg_gpb_commit = master

PACKAGES += gun
pkg_gun_name = gun
pkg_gun_description = Asynchronous SPDY, HTTP and Websocket client written in Erlang.
pkg_gun_homepage = http//ninenines.eu
pkg_gun_fetch = git
pkg_gun_repo = https://github.com/ninenines/gun
pkg_gun_commit = master

PACKAGES += hex_core
pkg_hex_core_name = hex_core
pkg_hex_core_description = Reference implementation of Hex specifications
pkg_hex_core_homepage = https://github.com/hexpm/hex_core
pkg_hex_core_fetch = git
HEX_CORE_GIT ?= https://github.com/hexpm/hex_core
pkg_hex_core_repo = $(HEX_CORE_GIT)
pkg_hex_core_commit = e57b4fb15cde710b3ae09b1d18f148f6999a63cc

PACKAGES += proper
pkg_proper_name = proper
pkg_proper_description = PropEr: a QuickCheck-inspired property-based testing tool for Erlang.
pkg_proper_homepage = http://proper.softlab.ntua.gr
pkg_proper_fetch = git
pkg_proper_repo = https://github.com/manopapad/proper
pkg_proper_commit = master

PACKAGES += ranch
pkg_ranch_name = ranch
pkg_ranch_description = Socket acceptor pool for TCP protocols.
pkg_ranch_homepage = http://ninenines.eu
pkg_ranch_fetch = git
pkg_ranch_repo = https://github.com/ninenines/ranch
pkg_ranch_commit = master

PACKAGES += relx
pkg_relx_name = relx
pkg_relx_description = Sane, simple release creation for Erlang
pkg_relx_homepage = https://github.com/erlware/relx
pkg_relx_fetch = git
pkg_relx_repo = https://github.com/erlware/relx
pkg_relx_commit = main

PACKAGES += triq
pkg_triq_name = triq
pkg_triq_description = Trifork QuickCheck
pkg_triq_homepage = https://triq.gitlab.io
pkg_triq_fetch = git
pkg_triq_repo = https://gitlab.com/triq/triq.git
pkg_triq_commit = master

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: search

define pkg_print
	$(verbose) printf "%s\n" \
		$(if $(call core_eq,$1,$(pkg_$(1)_name)),,"Pkg name:    $1") \
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
		$(if $(findstring $(call core_lc,$q),$(call core_lc,$(pkg_$(p)_name) $(pkg_$(p)_description))), \
			$(call pkg_print,$p)))
else
	$(foreach p,$(PACKAGES),$(call pkg_print,$p))
endif

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-deps clean-tmp-deps.log

# Configuration.

ifdef OTP_DEPS
$(warning The variable OTP_DEPS is deprecated in favor of LOCAL_DEPS.)
endif

IGNORE_DEPS ?=
export IGNORE_DEPS

APPS_DIR ?= $(CURDIR)/apps
export APPS_DIR

DEPS_DIR ?= $(CURDIR)/deps
export DEPS_DIR

REBAR_DEPS_DIR = $(DEPS_DIR)
export REBAR_DEPS_DIR

# When testing Erlang.mk and updating these, make sure
# to delete test/test_rebar_git before running tests again.
REBAR3_GIT ?= https://github.com/erlang/rebar3
REBAR3_COMMIT ?= bde4b54248d16280b2c70a244aca3bb7566e2033 # 3.23.0

CACHE_DEPS ?= 0

CACHE_DIR ?= $(if $(XDG_CACHE_HOME),$(XDG_CACHE_HOME),$(HOME)/.cache)/erlang.mk
export CACHE_DIR

HEX_CONFIG ?=

define hex_config.erl
	begin
		Config0 = hex_core:default_config(),
		Config0$(HEX_CONFIG)
	end
endef

# External "early" plugins (see core/plugins.mk for regular plugins).
# They both use the core_dep_plugin macro.

define core_dep_plugin
ifeq ($2,$(PROJECT))
-include $$(patsubst $(PROJECT)/%,%,$1)
else
-include $(DEPS_DIR)/$1

$(DEPS_DIR)/$1: $(DEPS_DIR)/$2 ;
endif
endef

DEP_EARLY_PLUGINS ?=

$(foreach p,$(DEP_EARLY_PLUGINS),\
	$(eval $(if $(findstring /,$p),\
		$(call core_dep_plugin,$p,$(firstword $(subst /, ,$p))),\
		$(call core_dep_plugin,$p/early-plugins.mk,$p))))

# Query functions.

query_fetch_method = $(if $(dep_$(1)),$(call _qfm_dep,$(word 1,$(dep_$(1)))),$(call _qfm_pkg,$1))
_qfm_dep = $(if $(dep_fetch_$(1)),$1,fail)
_qfm_pkg = $(if $(pkg_$(1)_fetch),$(pkg_$(1)_fetch),fail)

query_name = $(if $(dep_$(1)),$1,$(if $(pkg_$(1)_name),$(pkg_$(1)_name),$1))

query_repo = $(call _qr,$1,$(call query_fetch_method,$1))
_qr = $(if $(query_repo_$(2)),$(call query_repo_$(2),$1),$(call query_repo_git,$1))

query_repo_default = $(if $(dep_$(1)),$(word 2,$(dep_$(1))),$(pkg_$(1)_repo))
query_repo_git = $(patsubst git://github.com/%,https://github.com/%,$(call query_repo_default,$1))
query_repo_git-subfolder = $(call query_repo_git,$1)
query_repo_git-submodule = -
query_repo_hg = $(call query_repo_default,$1)
query_repo_svn = $(call query_repo_default,$1)
query_repo_cp = $(call query_repo_default,$1)
query_repo_ln = $(call query_repo_default,$1)
query_repo_hex = https://hex.pm/packages/$(if $(word 3,$(dep_$(1))),$(word 3,$(dep_$(1))),$1)
query_repo_fail = -

query_version = $(call _qv,$1,$(call query_fetch_method,$1))
_qv = $(if $(query_version_$(2)),$(call query_version_$(2),$1),$(call query_version_default,$1))

query_version_default = $(if $(dep_$(1)_commit),$(dep_$(1)_commit),$(if $(dep_$(1)),$(word 3,$(dep_$(1))),$(pkg_$(1)_commit)))
query_version_git = $(call query_version_default,$1)
query_version_git-subfolder = $(call query_version_default,$1)
query_version_git-submodule = -
query_version_hg = $(call query_version_default,$1)
query_version_svn = -
query_version_cp = -
query_version_ln = -
query_version_hex = $(if $(dep_$(1)_commit),$(dep_$(1)_commit),$(if $(dep_$(1)),$(word 2,$(dep_$(1))),$(pkg_$(1)_commit)))
query_version_fail = -

query_extra = $(call _qe,$1,$(call query_fetch_method,$1))
_qe = $(if $(query_extra_$(2)),$(call query_extra_$(2),$1),-)

query_extra_git = -
query_extra_git-subfolder = $(if $(dep_$(1)),subfolder=$(word 4,$(dep_$(1))),-)
query_extra_git-submodule = -
query_extra_hg = -
query_extra_svn = -
query_extra_cp = -
query_extra_ln = -
query_extra_hex = $(if $(dep_$(1)),package-name=$(word 3,$(dep_$(1))),-)
query_extra_fail = -

query_absolute_path = $(addprefix $(DEPS_DIR)/,$(call query_name,$1))

# Deprecated legacy query function. Used by RabbitMQ and its third party plugins.
# Can be removed once RabbitMQ has been updated and enough time has passed.
dep_name = $(call query_name,$(1))

# Application directories.

LOCAL_DEPS_DIRS = $(foreach a,$(LOCAL_DEPS),$(if $(wildcard $(APPS_DIR)/$a),$(APPS_DIR)/$a))
# Elixir is handled specially as it must be built before all other deps
# when Mix autopatching is necessary.
ALL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(foreach dep,$(filter-out $(IGNORE_DEPS),$(BUILD_DEPS) $(DEPS)),$(call query_name,$(dep))))

# When we are calling an app directly we don't want to include it here
# otherwise it'll be treated both as an apps and a top-level project.
ALL_APPS_DIRS = $(if $(wildcard $(APPS_DIR)/),$(filter-out $(APPS_DIR),$(shell find $(APPS_DIR) -maxdepth 1 -type d)))
ifdef ROOT_DIR
ifndef IS_APP
ALL_APPS_DIRS := $(filter-out $(APPS_DIR)/$(notdir $(CURDIR)),$(ALL_APPS_DIRS))
endif
endif

ifeq ($(filter $(APPS_DIR) $(DEPS_DIR),$(subst :, ,$(ERL_LIBS))),)
ifeq ($(ERL_LIBS),)
	ERL_LIBS = $(APPS_DIR):$(DEPS_DIR)
else
	ERL_LIBS := $(ERL_LIBS):$(APPS_DIR):$(DEPS_DIR)
endif
endif
export ERL_LIBS

export NO_AUTOPATCH

# Verbosity.

dep_verbose_0 = @echo " DEP    $1 ($(call query_version,$1))";
dep_verbose_2 = set -x;
dep_verbose = $(dep_verbose_$(V))

# Optimization: don't recompile deps unless truly necessary.

ifndef IS_DEP
ifneq ($(MAKELEVEL),0)
$(shell rm -f ebin/dep_built)
endif
endif

# Core targets.

ALL_APPS_DIRS_TO_BUILD = $(if $(LOCAL_DEPS_DIRS)$(IS_APP),$(LOCAL_DEPS_DIRS),$(ALL_APPS_DIRS))

apps:: $(ALL_APPS_DIRS) clean-tmp-deps.log | $(ERLANG_MK_TMP)
# Create ebin directory for all apps to make sure Erlang recognizes them
# as proper OTP applications when using -include_lib. This is a temporary
# fix, a proper fix would be to compile apps/* in the right order.
ifndef IS_APP
ifneq ($(ALL_APPS_DIRS),)
	$(verbose) set -e; for dep in $(ALL_APPS_DIRS) ; do \
		mkdir -p $$dep/ebin; \
	done
endif
endif
# At the toplevel: if LOCAL_DEPS is defined with at least one local app, only
# compile that list of apps. Otherwise, compile everything.
# Within an app: compile all LOCAL_DEPS that are (uncompiled) local apps.
ifneq ($(ALL_APPS_DIRS_TO_BUILD),)
	$(verbose) set -e; for dep in $(ALL_APPS_DIRS_TO_BUILD); do \
		if grep -qs ^$$dep$$ $(ERLANG_MK_TMP)/apps.log; then \
			:; \
		else \
			echo $$dep >> $(ERLANG_MK_TMP)/apps.log; \
			$(MAKE) -C $$dep $(if $(IS_TEST),test-build-app) IS_APP=1; \
		fi \
	done
endif

clean-tmp-deps.log:
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) rm -f $(ERLANG_MK_TMP)/apps.log $(ERLANG_MK_TMP)/deps.log
endif

# Erlang.mk does not rebuild dependencies after they were compiled
# once. If a developer is working on the top-level project and some
# dependencies at the same time, he may want to change this behavior.
# There are two solutions:
#     1. Set `FULL=1` so that all dependencies are visited and
#        recursively recompiled if necessary.
#     2. Set `FORCE_REBUILD=` to the specific list of dependencies that
#        should be recompiled (instead of the whole set).

FORCE_REBUILD ?=

ifeq ($(origin FULL),undefined)
ifneq ($(strip $(force_rebuild_dep)$(FORCE_REBUILD)),)
define force_rebuild_dep
echo "$(FORCE_REBUILD)" | grep -qw "$$(basename "$1")"
endef
endif
endif

ifneq ($(SKIP_DEPS),)
deps::
else
ALL_DEPS_DIRS_TO_BUILD = $(if $(filter-out $(DEPS_DIR)/elixir,$(ALL_DEPS_DIRS)),$(filter-out $(DEPS_DIR)/elixir,$(ALL_DEPS_DIRS)),$(ALL_DEPS_DIRS))

deps:: $(ALL_DEPS_DIRS_TO_BUILD) apps clean-tmp-deps.log | $(ERLANG_MK_TMP)
ifneq ($(ALL_DEPS_DIRS_TO_BUILD),)
	$(verbose) set -e; for dep in $(ALL_DEPS_DIRS_TO_BUILD); do \
		if grep -qs ^$$dep$$ $(ERLANG_MK_TMP)/deps.log; then \
			:; \
		else \
			echo $$dep >> $(ERLANG_MK_TMP)/deps.log; \
			if [ -z "$(strip $(FULL))" ] $(if $(force_rebuild_dep),&& ! ($(call force_rebuild_dep,$$dep)),) && [ ! -L $$dep ] && [ -f $$dep/ebin/dep_built ]; then \
				:; \
			elif [ "$$dep" = "$(DEPS_DIR)/hut" -a "$(HUT_PATCH)" ]; then \
				$(MAKE) -C $$dep app IS_DEP=1; \
				if [ ! -L $$dep ] && [ -d $$dep/ebin ]; then touch $$dep/ebin/dep_built; fi; \
			elif [ -f $$dep/GNUmakefile ] || [ -f $$dep/makefile ] || [ -f $$dep/Makefile ]; then \
				$(MAKE) -C $$dep IS_DEP=1; \
				if [ ! -L $$dep ] && [ -d $$dep/ebin ]; then touch $$dep/ebin/dep_built; fi; \
			else \
				echo "Error: No Makefile to build dependency $$dep." >&2; \
				exit 2; \
			fi \
		fi \
	done
endif
endif

# Deps related targets.

autopatch_verbose_0 = @echo " PATCH " $(subst autopatch-,,$@) "(method: $(AUTOPATCH_METHOD))";
autopatch_verbose_2 = set -x;
autopatch_verbose = $(autopatch_verbose_$(V))

define dep_autopatch_detect
	if [ -f $(DEPS_DIR)/$1/erlang.mk ]; then \
		echo erlang.mk; \
	elif [ -f $(DEPS_DIR)/$1/mix.exs -a -d $(DEPS_DIR)/$1/lib ]; then \
		if [ "$(ELIXIR)" != "disable" ]; then \
			echo mix; \
		elif [ -f $(DEPS_DIR)/$1/rebar.lock -o -f $(DEPS_DIR)/$1/rebar.config ]; then \
			echo rebar3; \
		elif [ -f $(DEPS_DIR)/$1/Makefile ]; then \
			echo noop; \
		else \
			exit 99; \
		fi \
	elif [ -f $(DEPS_DIR)/$1/Makefile ]; then \
		if [ -f $(DEPS_DIR)/$1/rebar.lock ]; then \
			echo rebar3; \
		elif [ 0 != \`grep -c "include ../\w*\.mk" $(DEPS_DIR)/$1/Makefile\` ]; then \
			echo rebar3; \
		elif [ 0 != \`grep -ci "^[^#].*rebar" $(DEPS_DIR)/$1/Makefile\` ]; then \
			echo rebar3; \
		elif [ -n "\`find $(DEPS_DIR)/$1/ -type f -name \*.mk -not -name erlang.mk -exec grep -i "^[^#].*rebar" '{}' \;\`" ]; then \
			echo rebar3; \
		else \
			echo noop; \
		fi \
	elif [ ! -d $(DEPS_DIR)/$1/src/ ]; then \
		echo noop; \
	else \
		echo rebar3; \
	fi
endef

define dep_autopatch_for_erlang.mk
	rm -rf $(DEPS_DIR)/$1/ebin/; \
	$(call erlang,$(call dep_autopatch_appsrc.erl,$1)); \
	$(call dep_autopatch_erlang_mk,$1)
endef

define dep_autopatch_for_rebar3
	! test -f $(DEPS_DIR)/$1/ebin/$1.app || \
	mv -n $(DEPS_DIR)/$1/ebin/$1.app $(DEPS_DIR)/$1/src/$1.app.src; \
	rm -f $(DEPS_DIR)/$1/ebin/$1.app; \
	if [ -f $(DEPS_DIR)/$1/src/$1.app.src.script ]; then \
		$(call erlang,$(call dep_autopatch_appsrc_script.erl,$1)); \
	fi; \
	$(call erlang,$(call dep_autopatch_appsrc.erl,$1)); \
	if [ -f $(DEPS_DIR)/$1/rebar -o -f $(DEPS_DIR)/$1/rebar.config -o -f $(DEPS_DIR)/$1/rebar.config.script -o -f $(DEPS_DIR)/$1/rebar.lock ]; then \
		$(call dep_autopatch_fetch_rebar); \
		$(call dep_autopatch_rebar,$1); \
	else \
		$(call dep_autopatch_gen,$1); \
	fi
endef

define dep_autopatch_for_mix
	$(call dep_autopatch_mix,$1)
endef

define dep_autopatch_for_noop
	test -f $(DEPS_DIR)/$1/Makefile || printf "noop:\n" > $(DEPS_DIR)/$1/Makefile
endef

define maybe_flock
	if command -v flock >/dev/null; then \
		flock $1 sh -c "$2"; \
	elif command -v lockf >/dev/null; then \
		lockf $1 sh -c "$2"; \
	else \
		$2; \
	fi
endef

# Replace "include erlang.mk" with a line that will load the parent Erlang.mk
# if given. Do it for all 3 possible Makefile file names.
ifeq ($(NO_AUTOPATCH_ERLANG_MK),)
define dep_autopatch_erlang_mk
	for f in Makefile makefile GNUmakefile; do \
		if [ -f $(DEPS_DIR)/$1/$$f ]; then \
			sed -i.bak s/'include *erlang.mk'/'include $$(if $$(ERLANG_MK_FILENAME),$$(ERLANG_MK_FILENAME),erlang.mk)'/ $(DEPS_DIR)/$1/$$f; \
		fi \
	done
endef
else
define dep_autopatch_erlang_mk
	:
endef
endif

define dep_autopatch_gen
	printf "%s\n" \
		"ERLC_OPTS = +debug_info" \
		"include ../../erlang.mk" > $(DEPS_DIR)/$1/Makefile
endef

# We use flock/lockf when available to avoid concurrency issues.
define dep_autopatch_fetch_rebar
	$(call maybe_flock,$(ERLANG_MK_TMP)/rebar.lock,$(call dep_autopatch_fetch_rebar2))
endef

define dep_autopatch_fetch_rebar2
	if [ ! -d $(ERLANG_MK_TMP)/rebar3 ]; then \
		git clone -q -n -- $(REBAR3_GIT) $(ERLANG_MK_TMP)/rebar3; \
		cd $(ERLANG_MK_TMP)/rebar3; \
		git checkout -q $(REBAR3_COMMIT); \
		./bootstrap; \
		cd -; \
	fi
endef

define dep_autopatch_rebar
	if [ -f $(DEPS_DIR)/$1/Makefile ]; then \
		mv $(DEPS_DIR)/$1/Makefile $(DEPS_DIR)/$1/Makefile.orig.mk; \
	fi; \
	$(call erlang,$(call dep_autopatch_rebar.erl,$1)); \
	rm -f $(DEPS_DIR)/$1/ebin/$1.app
endef

define dep_autopatch_rebar.erl
	application:load(rebar),
	application:set_env(rebar, log_level, debug),
	{module, rebar3} = c:l(rebar3),
	Conf1 = case file:consult("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config)") of
		{ok, Conf0} -> Conf0;
		_ -> []
	end,
	{Conf, OsEnv} = fun() ->
		case filelib:is_file("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)") of
			false -> {Conf1, []};
			true ->
				Bindings0 = erl_eval:new_bindings(),
				Bindings1 = erl_eval:add_binding('CONFIG', Conf1, Bindings0),
				Bindings = erl_eval:add_binding('SCRIPT', "$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)", Bindings1),
				Before = os:getenv(),
				{ok, Conf2} = file:script("$(call core_native_path,$(DEPS_DIR)/$1/rebar.config.script)", Bindings),
				{Conf2, lists:foldl(fun(E, Acc) -> lists:delete(E, Acc) end, os:getenv(), Before)}
		end
	end(),
	Write = fun (Text) ->
		file:write_file("$(call core_native_path,$(DEPS_DIR)/$1/Makefile)", Text, [append])
	end,
	Escape = fun (Text) ->
		re:replace(Text, "\\\\$$", "\$$$$", [global, {return, list}])
	end,
	Write("IGNORE_DEPS += edown eper eunit_formatters meck node_package "
		"rebar_lock_deps_plugin rebar_vsn_plugin reltool_util\n"),
	Write("C_SRC_DIR = /path/do/not/exist\n"),
	Write("C_SRC_TYPE = rebar\n"),
	Write("DRV_CFLAGS = -fPIC\nexport DRV_CFLAGS\n"),
	Write(["ERLANG_ARCH = ", rebar_utils:wordsize(), "\nexport ERLANG_ARCH\n"]),
	ToList = fun
		(V) when is_atom(V) -> atom_to_list(V);
		(V) when is_list(V) -> "'\\"" ++ V ++ "\\"'"
	end,
	fun() ->
		Write("ERLC_OPTS = +debug_info\n"),
		case lists:keyfind(erl_opts, 1, Conf) of
			false -> ok;
			{_, ErlOpts} ->
				lists:foreach(fun
					({d, D}) ->
						Write("ERLC_OPTS += -D" ++ ToList(D) ++ "=1\n");
					({d, DKey, DVal}) ->
						Write("ERLC_OPTS += -D" ++ ToList(DKey) ++ "=" ++ ToList(DVal) ++ "\n");
					({i, I}) ->
						Write(["ERLC_OPTS += -I ", I, "\n"]);
					({platform_define, Regex, D}) ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("ERLC_OPTS += -D" ++ ToList(D) ++ "=1\n");
							false -> ok
						end;
					({parse_transform, PT}) ->
						Write("ERLC_OPTS += +'{parse_transform, " ++ ToList(PT) ++ "}'\n");
					(_) -> ok
				end, ErlOpts)
		end,
		Write("\n")
	end(),
	GetHexVsn2 = fun(N, NP) ->
		case file:consult("$(call core_native_path,$(DEPS_DIR)/$1/rebar.lock)") of
			{ok, Lock} ->
				LockPkgs = case lists:keyfind("1.2.0", 1, Lock) of
					{_, LP} ->
						LP;
					_ ->
						case lists:keyfind("1.1.0", 1, Lock) of
							{_, LP} ->
								LP;
							_ ->
								false
						end
				end,
				if
					is_list(LockPkgs) ->
						case lists:keyfind(atom_to_binary(N, latin1), 1, LockPkgs) of
							{_, {pkg, _, Vsn}, _} ->
								{N, {hex, NP, binary_to_list(Vsn)}};
							_ ->
								false
						end;
					true ->
						false
				end;
			_ ->
				false
		end
	end,
	GetHexVsn3Common = fun(N, NP, S0) ->
		case GetHexVsn2(N, NP) of
			false ->
				S2 = case S0 of
					" " ++ S1 -> S1;
					_ -> S0
				end,
				S = case length([ok || $$. <- S2]) of
					0 -> S2 ++ ".0.0";
					1 -> S2 ++ ".0";
					_ -> S2
				end,
				{N, {hex, NP, S}};
			NameSource ->
				NameSource
		end
	end,
	GetHexVsn3 = fun
		(N, NP, "~>" ++ S0) ->
			GetHexVsn3Common(N, NP, S0);
		(N, NP, ">=" ++ S0) ->
			GetHexVsn3Common(N, NP, S0);
		(N, NP, S) -> {N, {hex, NP, S}}
	end,
	ConvertCommit = fun
		({branch, C}) -> C;
		({ref, C}) -> C;
		({tag, C}) -> C;
		(C) -> C
	end,
	fun() ->
		File = case lists:keyfind(deps, 1, Conf) of
			false -> [];
			{_, Deps} ->
				[begin case case Dep of
							N when is_atom(N) -> GetHexVsn2(N, N);
							{N, S} when is_atom(N), is_list(S) -> GetHexVsn3(N, N, S);
							{N, {pkg, NP}} when is_atom(N) -> GetHexVsn2(N, NP);
							{N, S, {pkg, NP}} -> GetHexVsn3(N, NP, S);
							{N, S} when is_tuple(S) -> {N, S};
							{N, _, S} -> {N, S};
							{N, _, S, _} -> {N, S};
							_ -> false
						end of
					false -> ok;
					{Name, {git_subdir, Repo, Commit, SubDir}} ->
						Write(io_lib:format("DEPS += ~s\ndep_~s = git-subfolder ~s ~s ~s~n", [Name, Name, Repo, ConvertCommit(Commit), SubDir]));
					{Name, Source} ->
						{Method, Repo, Commit} = case Source of
							{hex, NPV, V} -> {hex, V, NPV};
							{git, R} -> {git, R, master};
							{M, R, C} -> {M, R, C}
						end,
						Write(io_lib:format("DEPS += ~s\ndep_~s = ~s ~s ~s~n", [Name, Name, Method, Repo, ConvertCommit(Commit)]))
				end end || Dep <- Deps]
		end
	end(),
	fun() ->
		case lists:keyfind(erl_first_files, 1, Conf) of
			false -> ok;
			{_, Files0} ->
				Files = [begin
					hd(filelib:wildcard("$(call core_native_path,$(DEPS_DIR)/$1/src/)**/" ++ filename:rootname(F) ++ ".*rl"))
				end || "src/" ++ F <- Files0],
				Names = [[" ", case lists:reverse(F) of
					"lre." ++ Elif -> lists:reverse(Elif);
					"lrx." ++ Elif -> lists:reverse(Elif);
					"lry." ++ Elif -> lists:reverse(Elif);
					Elif -> lists:reverse(Elif)
				end] || "$(call core_native_path,$(DEPS_DIR)/$1/src/)" ++ F <- Files],
				Write(io_lib:format("COMPILE_FIRST +=~s\n", [Names]))
		end
	end(),
	Write("\n\nrebar_dep: preprocess pre-deps deps pre-app app\n"),
	Write("\npreprocess::\n"),
	Write("\npre-deps::\n"),
	Write("\npre-app::\n"),
	PatchHook = fun(Cmd) ->
		Cmd2 = re:replace(Cmd, "^([g]?make)(.*)( -C.*)", "\\\\1\\\\3\\\\2", [{return, list}]),
		case Cmd2 of
			"make -C" ++ Cmd1 -> "$$\(MAKE) -C" ++ Escape(Cmd1);
			"gmake -C" ++ Cmd1 -> "$$\(MAKE) -C" ++ Escape(Cmd1);
			"make " ++ Cmd1 -> "$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
			"gmake " ++ Cmd1 -> "$$\(MAKE) -f Makefile.orig.mk " ++ Escape(Cmd1);
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
						Write("\npre-app::\n\tCC=$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
					{{pc, compile}, Cmd} ->
						Write("\npre-app::\n\tCC=$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
					{Regex, compile, Cmd} ->
						case rebar_utils:is_arch(Regex) of
							true -> Write("\npre-app::\n\tCC=$$\(CC) " ++ PatchHook(Cmd) ++ "\n");
							false -> ok
						end;
					_ -> ok
				end || H <- Hooks]
		end
	end(),
	ShellToMk = fun(V0) ->
		V1 = re:replace(V0, "[$$][(]", "$$\(shell ", [global]),
		V = re:replace(V1, "([$$])(?![(])(\\\\w*)", "\\\\1(\\\\2)", [global]),
		re:replace(V, "-Werror\\\\b", "", [{return, list}, global])
	end,
	PortSpecs = fun() ->
		case lists:keyfind(port_specs, 1, Conf) of
			false ->
				case filelib:is_dir("$(call core_native_path,$(DEPS_DIR)/$1/c_src)") of
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
		file:write_file("$(call core_native_path,$(DEPS_DIR)/$1/c_src/Makefile.erlang.mk)", Text, [append])
	end,
	case PortSpecs of
		[] -> ok;
		_ ->
			Write("\npre-app::\n\t@$$\(MAKE) --no-print-directory -f c_src/Makefile.erlang.mk\n"),
			PortSpecWrite(io_lib:format("ERL_CFLAGS ?= -finline-functions -Wall -fPIC -I \\"~s/erts-~s/include\\" -I \\"~s\\"\n",
				[code:root_dir(), erlang:system_info(version), code:lib_dir(erl_interface, include)])),
			PortSpecWrite(io_lib:format("ERL_LDFLAGS ?= -L \\"~s\\" -lei\n",
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
				filelib:ensure_dir("$(call core_native_path,$(DEPS_DIR)/$1/)" ++ Output),
				Input = [[" ", I] || I <- Input0],
				PortSpecWrite([
					[["\n", K, " = ", ShellToMk(V)] || {K, V} <- lists:reverse(MergeEnv(PortEnv))],
					case $(PLATFORM) of
						darwin -> "\n\nLDFLAGS += -flat_namespace -undefined suppress";
						_ -> ""
					end,
					"\n\nall:: ", Output, "\n\t@:\n\n",
					"%.o: %.c\n\t$$\(CC) -c -o $$\@ $$\< $$\(CFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.C\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.cc\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					"%.o: %.cpp\n\t$$\(CXX) -c -o $$\@ $$\< $$\(CXXFLAGS) $$\(ERL_CFLAGS) $$\(DRV_CFLAGS) $$\(EXE_CFLAGS)\n\n",
					[[Output, ": ", K, " += ", ShellToMk(V), "\n"] || {K, V} <- lists:reverse(MergeEnv(FilterEnv(Env)))],
					Output, ": $$\(foreach ext,.c .C .cc .cpp,",
						"$$\(patsubst %$$\(ext),%.o,$$\(filter %$$\(ext),$$\(wildcard", Input, "))))\n",
					"\t$$\(CC) -o $$\@ $$\? $$\(LDFLAGS) $$\(ERL_LDFLAGS) $$\(DRV_LDFLAGS) $$\(LDLIBS) $$\(EXE_LDFLAGS)",
					case {filename:extension(Output), $(PLATFORM)} of
					    {[], _} -> "\n";
					    {".so", darwin} -> " -shared\n";
					    {".dylib", darwin} -> " -shared\n";
					    {_, darwin} -> "\n";
					    _ -> " -shared\n"
					end])
			end,
			[PortSpec(S) || S <- PortSpecs]
	end,
	fun() ->
		case lists:keyfind(plugins, 1, Conf) of
			false -> ok;
			{_, Plugins0} ->
				Plugins = [P || P <- Plugins0, is_tuple(P)],
				case lists:keyfind('lfe-compile', 1, Plugins) of
					false -> ok;
					_ -> Write("\nBUILD_DEPS = lfe lfe.mk\ndep_lfe.mk = git https://github.com/ninenines/lfe.mk master\nDEP_PLUGINS = lfe.mk\n")
				end
		end
	end(),
	Write("\ninclude $$\(if $$\(ERLANG_MK_FILENAME),$$\(ERLANG_MK_FILENAME),erlang.mk)"),
	RunPlugin = fun(Plugin, Step) ->
		case erlang:function_exported(Plugin, Step, 2) of
			false -> ok;
			true ->
				c:cd("$(call core_native_path,$(DEPS_DIR)/$1/)"),
				Ret = Plugin:Step({config, "", Conf, dict:new(), dict:new(), dict:new(),
					dict:store(base_dir, "", dict:new())}, undefined),
				io:format("rebar plugin ~p step ~p ret ~p~n", [Plugin, Step, Ret])
		end
	end,
	fun() ->
		case lists:keyfind(plugins, 1, Conf) of
			false -> ok;
			{_, Plugins0} ->
				Plugins = [P || P <- Plugins0, is_atom(P)],
				[begin
					case lists:keyfind(deps, 1, Conf) of
						false -> ok;
						{_, Deps} ->
							case lists:keyfind(P, 1, Deps) of
								false -> ok;
								_ ->
									Path = "$(call core_native_path,$(DEPS_DIR)/)" ++ atom_to_list(P),
									io:format("~s", [os:cmd("$(MAKE) -C $(call core_native_path,$(DEPS_DIR)/$1) " ++ Path)]),
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
								ErlFile = "$(call core_native_path,$(DEPS_DIR)/$1/)" ++ PluginsDir ++ "/" ++ atom_to_list(P) ++ ".erl",
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

define dep_autopatch_appsrc_script.erl
	AppSrc = "$(call core_native_path,$(DEPS_DIR)/$1/src/$1.app.src)",
	AppSrcScript = AppSrc ++ ".script",
	Conf1 = case file:consult(AppSrc) of
		{ok, Conf0} -> Conf0;
		{error, enoent} -> []
	end,
	Bindings0 = erl_eval:new_bindings(),
	Bindings1 = erl_eval:add_binding('CONFIG', Conf1, Bindings0),
	Bindings = erl_eval:add_binding('SCRIPT', AppSrcScript, Bindings1),
	Conf = case file:script(AppSrcScript, Bindings) of
		{ok, [C]} -> C;
		{ok, C} -> C
	end,
	ok = file:write_file(AppSrc, io_lib:format("~p.~n", [Conf])),
	halt()
endef

define dep_autopatch_appsrc.erl
	AppSrcOut = "$(call core_native_path,$(DEPS_DIR)/$1/src/$1.app.src)",
	AppSrcIn = case filelib:is_regular(AppSrcOut) of false -> "$(call core_native_path,$(DEPS_DIR)/$1/ebin/$1.app)"; true -> AppSrcOut end,
	case filelib:is_regular(AppSrcIn) of
		false -> ok;
		true ->
			{ok, [{application, $1, L0}]} = file:consult(AppSrcIn),
			L1 = lists:keystore(modules, 1, L0, {modules, []}),
			L2 = case lists:keyfind(vsn, 1, L1) of
				{_, git} -> lists:keyreplace(vsn, 1, L1, {vsn, lists:droplast(os:cmd("git -C $(DEPS_DIR)/$1 describe --dirty --tags --always"))});
				{_, {cmd, _}} -> lists:keyreplace(vsn, 1, L1, {vsn, "cmd"});
				_ -> L1
			end,
			L3 = case lists:keyfind(registered, 1, L2) of false -> [{registered, []}|L2]; _ -> L2 end,
			ok = file:write_file(AppSrcOut, io_lib:format("~p.~n", [{application, $1, L3}])),
			case AppSrcOut of AppSrcIn -> ok; _ -> ok = file:delete(AppSrcIn) end
	end,
	halt()
endef

ifeq ($(CACHE_DEPS),1)

define dep_cache_fetch_git
	mkdir -p $(CACHE_DIR)/git; \
	if test -d "$(join $(CACHE_DIR)/git/,$(call query_name,$1))"; then \
		cd $(join $(CACHE_DIR)/git/,$(call query_name,$1)); \
		if ! git checkout -q $(call query_version,$1); then \
			git remote set-url origin $(call query_repo_git,$1) && \
			git pull --all && \
			git cat-file -e $(call query_version_git,$1) 2>/dev/null; \
		fi; \
	else \
		git clone -q -n -- $(call query_repo_git,$1) $(join $(CACHE_DIR)/git/,$(call query_name,$1)); \
	fi; \
	git clone -q --single-branch -- $(join $(CACHE_DIR)/git/,$(call query_name,$1)) $2; \
	cd $2 && git checkout -q $(call query_version_git,$1)
endef

define dep_fetch_git
	$(call dep_cache_fetch_git,$1,$(DEPS_DIR)/$(call query_name,$1));
endef

define dep_fetch_git-subfolder
	mkdir -p $(ERLANG_MK_TMP)/git-subfolder; \
	$(call dep_cache_fetch_git,$1,$(ERLANG_MK_TMP)/git-subfolder/$(call query_name,$1)); \
	ln -s $(ERLANG_MK_TMP)/git-subfolder/$(call query_name,$1)/$(word 4,$(dep_$1)) \
		$(DEPS_DIR)/$(call query_name,$1);
endef

else

define dep_fetch_git
	git clone -q -n -- $(call query_repo_git,$1) $(DEPS_DIR)/$(call query_name,$1); \
	cd $(DEPS_DIR)/$(call query_name,$1) && git checkout -q $(call query_version_git,$1);
endef

define dep_fetch_git-subfolder
	mkdir -p $(ERLANG_MK_TMP)/git-subfolder; \
	git clone -q -n -- $(call query_repo_git-subfolder,$1) \
		$(ERLANG_MK_TMP)/git-subfolder/$(call query_name,$1); \
	cd $(ERLANG_MK_TMP)/git-subfolder/$(call query_name,$1) \
		&& git checkout -q $(call query_version_git-subfolder,$1); \
	ln -s $(ERLANG_MK_TMP)/git-subfolder/$(call query_name,$1)/$(word 4,$(dep_$1)) \
		$(DEPS_DIR)/$(call query_name,$1);
endef

endif

define dep_fetch_git-submodule
	git submodule update --init -- $(DEPS_DIR)/$1;
endef

define dep_fetch_hg
	hg clone -q -U $(call query_repo_hg,$1) $(DEPS_DIR)/$(call query_name,$1); \
	cd $(DEPS_DIR)/$(call query_name,$1) && hg update -q $(call query_version_hg,$1);
endef

define dep_fetch_svn
	svn checkout -q $(call query_repo_svn,$1) $(DEPS_DIR)/$(call query_name,$1);
endef

define dep_fetch_cp
	cp -R $(call query_repo_cp,$1) $(DEPS_DIR)/$(call query_name,$1);
endef

define dep_fetch_ln
	ln -s $(call query_repo_ln,$1) $(DEPS_DIR)/$(call query_name,$1);
endef

define hex_get_tarball.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	case hex_repo:get_tarball(Config, <<"$1">>, <<"$(strip $2)">>) of
		{ok, {200, _, Tarball}} ->
			ok = file:write_file("$(call core_native_path,$3)", Tarball),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(79)
	end
endef

ifeq ($(CACHE_DEPS),1)

# Hex only has a package version. No need to look in the Erlang.mk packages.
define dep_fetch_hex
	mkdir -p $(CACHE_DIR)/hex $(DEPS_DIR)/$1; \
	$(eval hex_pkg_name := $(if $(word 3,$(dep_$1)),$(word 3,$(dep_$1)),$1)) \
	$(eval hex_tar_name := $(hex_pkg_name)-$(strip $(word 2,$(dep_$1))).tar) \
	$(if $(wildcard $(CACHE_DIR)/hex/$(hex_tar_name)),,\
		$(call erlang,$(call hex_get_tarball.erl,$(hex_pkg_name),$(word 2,$(dep_$1)),$(CACHE_DIR)/hex/$(hex_tar_name)));) \
	tar -xOf $(CACHE_DIR)/hex/$(hex_tar_name) contents.tar.gz | tar -C $(DEPS_DIR)/$1 -xzf -;
endef

else

# Hex only has a package version. No need to look in the Erlang.mk packages.
define dep_fetch_hex
	mkdir -p $(ERLANG_MK_TMP)/hex $(DEPS_DIR)/$1; \
	$(call erlang,$(call hex_get_tarball.erl,$(if $(word 3,$(dep_$1)),$(word 3,$(dep_$1)),$1),$(word 2,$(dep_$1)),$(ERLANG_MK_TMP)/hex/$1.tar)); \
	tar -xOf $(ERLANG_MK_TMP)/hex/$1.tar contents.tar.gz | tar -C $(DEPS_DIR)/$1 -xzf -;
endef

endif

define dep_fetch_fail
	echo "Error: Unknown or invalid dependency: $1." >&2; \
	exit 78;
endef

define dep_target
$(DEPS_DIR)/$(call query_name,$1): $(if $(filter elixir,$(BUILD_DEPS) $(DEPS)),$(if $(filter-out elixir,$1),$(DEPS_DIR)/elixir/ebin/dep_built)) $(if $(filter hex,$(call query_fetch_method,$1)),$(if $(wildcard $(DEPS_DIR)/$(call query_name,$1)),,$(DEPS_DIR)/hex_core/ebin/dep_built)) | $(ERLANG_MK_TMP)
	$(eval DEP_NAME := $(call query_name,$1))
	$(eval DEP_STR := $(if $(filter $1,$(DEP_NAME)),$1,"$1 ($(DEP_NAME))"))
	$(verbose) if test -d $(APPS_DIR)/$(DEP_NAME); then \
		echo "Error: Dependency" $(DEP_STR) "conflicts with application found in $(APPS_DIR)/$(DEP_NAME)." >&2; \
		exit 17; \
	fi
	$(verbose) mkdir -p $(DEPS_DIR)
	$(dep_verbose) $(call dep_fetch_$(strip $(call query_fetch_method,$1)),$1)
	$(verbose) if [ -f $(DEPS_DIR)/$1/configure.ac -o -f $(DEPS_DIR)/$1/configure.in ] \
			&& [ ! -f $(DEPS_DIR)/$1/configure ]; then \
		echo " AUTO  " $(DEP_STR); \
		cd $(DEPS_DIR)/$1 && autoreconf -Wall -vif -I m4; \
	fi
	- $(verbose) if [ -f $(DEPS_DIR)/$(DEP_NAME)/configure ]; then \
		echo " CONF  " $(DEP_STR); \
		cd $(DEPS_DIR)/$(DEP_NAME) && ./configure; \
	fi
ifeq ($(filter $1,$(NO_AUTOPATCH)),)
	$(verbose) AUTOPATCH_METHOD=`$(call dep_autopatch_detect,$1)`; \
	if [ $$$$? -eq 99 ]; then \
		echo "Elixir is currently disabled. Please set 'ELIXIR = system' in the Makefile to enable"; \
		exit 99; \
	fi; \
	$$(MAKE) --no-print-directory autopatch-$(DEP_NAME) AUTOPATCH_METHOD=$$$$AUTOPATCH_METHOD
endif

.PHONY: autopatch-$(call query_name,$1)

ifeq ($1,elixir)
autopatch-elixir::
	$$(verbose) ln -s lib/elixir/ebin $(DEPS_DIR)/elixir/
else
autopatch-$(call query_name,$1)::
	$$(autopatch_verbose) $$(call dep_autopatch_for_$(AUTOPATCH_METHOD),$(call query_name,$1))
endif
endef

# We automatically depend on hex_core when the project isn't already.
$(if $(filter hex_core,$(DEPS) $(BUILD_DEPS) $(DOC_DEPS) $(REL_DEPS) $(TEST_DEPS)),,\
	$(eval $(call dep_target,hex_core)))

$(DEPS_DIR)/hex_core/ebin/dep_built: | $(ERLANG_MK_TMP)
	$(verbose) $(call maybe_flock,$(ERLANG_MK_TMP)/hex_core.lock,\
		if [ ! -e $(DEPS_DIR)/hex_core/ebin/dep_built ]; then \
			$(MAKE) $(DEPS_DIR)/hex_core; \
			$(MAKE) -C $(DEPS_DIR)/hex_core IS_DEP=1; \
			touch $(DEPS_DIR)/hex_core/ebin/dep_built; \
		fi)

$(DEPS_DIR)/elixir/ebin/dep_built: | $(ERLANG_MK_TMP)
	$(verbose) $(call maybe_flock,$(ERLANG_MK_TMP)/elixir.lock,\
		if [ ! -e $(DEPS_DIR)/elixir/ebin/dep_built ]; then \
			$(MAKE) $(DEPS_DIR)/elixir; \
			$(MAKE) -C $(DEPS_DIR)/elixir; \
			touch $(DEPS_DIR)/elixir/ebin/dep_built; \
		fi)

$(foreach dep,$(BUILD_DEPS) $(DEPS),$(eval $(call dep_target,$(dep))))

ifndef IS_APP
clean:: clean-apps

clean-apps:
	$(verbose) set -e; for dep in $(ALL_APPS_DIRS) ; do \
		$(MAKE) -C $$dep clean IS_APP=1; \
	done

distclean:: distclean-apps

distclean-apps:
	$(verbose) set -e; for dep in $(ALL_APPS_DIRS) ; do \
		$(MAKE) -C $$dep distclean IS_APP=1; \
	done
endif

ifndef SKIP_DEPS
distclean:: distclean-deps

distclean-deps:
	$(gen_verbose) rm -rf $(DEPS_DIR)
endif

ifeq ($(CACHE_DEPS),1)
cacheclean:: cacheclean-git cacheclean-hex

cacheclean-git:
	$(gen_verbose) rm -rf $(CACHE_DIR)/git

cacheclean-hex:
	$(gen_verbose) rm -rf $(CACHE_DIR)/hex
endif

# Forward-declare variables used in core/deps-tools.mk. This is required
# in case plugins use them.

ERLANG_MK_RECURSIVE_DEPS_LIST = $(ERLANG_MK_TMP)/recursive-deps-list.log
ERLANG_MK_RECURSIVE_DOC_DEPS_LIST = $(ERLANG_MK_TMP)/recursive-doc-deps-list.log
ERLANG_MK_RECURSIVE_REL_DEPS_LIST = $(ERLANG_MK_TMP)/recursive-rel-deps-list.log
ERLANG_MK_RECURSIVE_TEST_DEPS_LIST = $(ERLANG_MK_TMP)/recursive-test-deps-list.log
ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST = $(ERLANG_MK_TMP)/recursive-shell-deps-list.log

ERLANG_MK_QUERY_DEPS_FILE = $(ERLANG_MK_TMP)/query-deps.log
ERLANG_MK_QUERY_DOC_DEPS_FILE = $(ERLANG_MK_TMP)/query-doc-deps.log
ERLANG_MK_QUERY_REL_DEPS_FILE = $(ERLANG_MK_TMP)/query-rel-deps.log
ERLANG_MK_QUERY_TEST_DEPS_FILE = $(ERLANG_MK_TMP)/query-test-deps.log
ERLANG_MK_QUERY_SHELL_DEPS_FILE = $(ERLANG_MK_TMP)/query-shell-deps.log

# Copyright (c) 2024, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: beam-cache-restore-app beam-cache-restore-test clean-beam-cache distclean-beam-cache

BEAM_CACHE_DIR ?= $(ERLANG_MK_TMP)/beam-cache
PROJECT_BEAM_CACHE_DIR = $(BEAM_CACHE_DIR)/$(PROJECT)

clean:: clean-beam-cache

clean-beam-cache:
	$(verbose) rm -rf $(PROJECT_BEAM_CACHE_DIR)

distclean:: distclean-beam-cache

$(PROJECT_BEAM_CACHE_DIR):
	$(verbose) mkdir -p $(PROJECT_BEAM_CACHE_DIR)

distclean-beam-cache:
	$(gen_verbose) rm -rf $(BEAM_CACHE_DIR)

beam-cache-restore-app: | $(PROJECT_BEAM_CACHE_DIR)
	$(verbose) rm -rf $(PROJECT_BEAM_CACHE_DIR)/ebin-test
ifneq ($(wildcard ebin/),)
	$(verbose) mv ebin/ $(PROJECT_BEAM_CACHE_DIR)/ebin-test
endif
ifneq ($(wildcard $(PROJECT_BEAM_CACHE_DIR)/ebin-app),)
	$(gen_verbose) mv $(PROJECT_BEAM_CACHE_DIR)/ebin-app ebin/
else
	$(verbose) $(MAKE) --no-print-directory clean-app
endif

beam-cache-restore-test: | $(PROJECT_BEAM_CACHE_DIR)
	$(verbose) rm -rf $(PROJECT_BEAM_CACHE_DIR)/ebin-app
ifneq ($(wildcard ebin/),)
	$(verbose) mv ebin/ $(PROJECT_BEAM_CACHE_DIR)/ebin-app
endif
ifneq ($(wildcard $(PROJECT_BEAM_CACHE_DIR)/ebin-test),)
	$(gen_verbose) mv $(PROJECT_BEAM_CACHE_DIR)/ebin-test ebin/
else
	$(verbose) $(MAKE) --no-print-directory clean-app
endif

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: clean-app

# Configuration.

ERLC_OPTS ?= -Werror +debug_info +warn_export_vars +warn_shadow_vars \
	+warn_obsolete_guard # +bin_opt_info +warn_export_all +warn_missing_spec
COMPILE_FIRST ?=
COMPILE_FIRST_PATHS = $(addprefix src/,$(addsuffix .erl,$(COMPILE_FIRST)))
ERLC_EXCLUDE ?=
ERLC_EXCLUDE_PATHS = $(addprefix src/,$(addsuffix .erl,$(ERLC_EXCLUDE)))

ERLC_ASN1_OPTS ?=

ERLC_MIB_OPTS ?=
COMPILE_MIB_FIRST ?=
COMPILE_MIB_FIRST_PATHS = $(addprefix mibs/,$(addsuffix .mib,$(COMPILE_MIB_FIRST)))

# Verbosity.

app_verbose_0 = @echo " APP   " $(PROJECT);
app_verbose_2 = set -x;
app_verbose = $(app_verbose_$(V))

appsrc_verbose_0 = @echo " APP   " $(PROJECT).app.src;
appsrc_verbose_2 = set -x;
appsrc_verbose = $(appsrc_verbose_$(V))

makedep_verbose_0 = @echo " DEPEND" $(PROJECT).d;
makedep_verbose_2 = set -x;
makedep_verbose = $(makedep_verbose_$(V))

erlc_verbose_0 = @echo " ERLC  " $(filter-out $(patsubst %,%.erl,$(ERLC_EXCLUDE)),\
	$(filter %.erl %.core,$(?F)));
erlc_verbose_2 = set -x;
erlc_verbose = $(erlc_verbose_$(V))

xyrl_verbose_0 = @echo " XYRL  " $(filter %.xrl %.yrl,$(?F));
xyrl_verbose_2 = set -x;
xyrl_verbose = $(xyrl_verbose_$(V))

asn1_verbose_0 = @echo " ASN1  " $(filter %.asn1,$(?F));
asn1_verbose_2 = set -x;
asn1_verbose = $(asn1_verbose_$(V))

mib_verbose_0 = @echo " MIB   " $(filter %.bin %.mib,$(?F));
mib_verbose_2 = set -x;
mib_verbose = $(mib_verbose_$(V))

ifneq ($(wildcard src/)$(wildcard lib/),)

# Targets.

app:: $(if $(wildcard ebin/test),beam-cache-restore-app) deps
	$(verbose) $(MAKE) --no-print-directory $(PROJECT).d
	$(verbose) $(MAKE) --no-print-directory app-build

PROJECT_MOD := $(if $(PROJECT_MOD),$(PROJECT_MOD),$(if $(wildcard src/$(PROJECT)_app.erl),$(PROJECT)_app))

define app_file
{application, '$(PROJECT)', [
	{description, "$(PROJECT_DESCRIPTION)"},
	{vsn, "$(PROJECT_VERSION)"},$(if $(IS_DEP),
	{id$(comma)$(space)"$1"}$(comma))
	{modules, [$(call comma_list,$2)]},
	{registered, [$(if $(PROJECT_MOD),$(call comma_list,$(if $(filter $(PROJECT_MOD),$(PROJECT)_app),$(PROJECT)_sup) $(PROJECT_REGISTERED)))]},
	{applications, [$(call comma_list,kernel stdlib $(OTP_DEPS) $(LOCAL_DEPS) $(OPTIONAL_DEPS) $(foreach dep,$(DEPS),$(call query_name,$(dep))))]},
	{optional_applications, [$(call comma_list,$(OPTIONAL_DEPS))]},$(if $(PROJECT_MOD),
	{mod$(comma)$(space){$(patsubst %,'%',$(PROJECT_MOD))$(comma)$(space)[]}}$(comma))
	{env, $(subst \,\\,$(PROJECT_ENV))}$(if $(findstring {,$(PROJECT_APP_EXTRA_KEYS)),$(comma)$(newline)$(tab)$(subst \,\\,$(PROJECT_APP_EXTRA_KEYS)),)
]}.
endef

app-build: ebin/$(PROJECT).app
	$(verbose) :

# Source files.

ALL_SRC_FILES := $(sort $(call core_find,src/,*))

ERL_FILES := $(filter %.erl,$(ALL_SRC_FILES))
CORE_FILES := $(filter %.core,$(ALL_SRC_FILES))

ALL_LIB_FILES := $(sort $(call core_find,lib/,*))
EX_FILES := $(filter-out lib/mix/%,$(filter %.ex,$(ALL_SRC_FILES) $(ALL_LIB_FILES)))

# ASN.1 files.

ifneq ($(wildcard asn1/),)
ASN1_FILES = $(sort $(call core_find,asn1/,*.asn1))
ERL_FILES += $(addprefix src/,$(patsubst %.asn1,%.erl,$(notdir $(ASN1_FILES))))

define compile_asn1
	$(verbose) mkdir -p include/
	$(asn1_verbose) erlc -v -I include/ -o asn1/ +noobj $(ERLC_ASN1_OPTS) $1
	$(verbose) mv asn1/*.erl src/
	-$(verbose) mv asn1/*.hrl include/
	$(verbose) mv asn1/*.asn1db include/
endef

$(PROJECT).d:: $(ASN1_FILES)
	$(if $(strip $?),$(call compile_asn1,$?))
endif

# SNMP MIB files.

ifneq ($(wildcard mibs/),)
MIB_FILES = $(sort $(call core_find,mibs/,*.mib))

$(PROJECT).d:: $(COMPILE_MIB_FIRST_PATHS) $(MIB_FILES)
	$(verbose) mkdir -p include/ priv/mibs/
	$(mib_verbose) erlc -v $(ERLC_MIB_OPTS) -o priv/mibs/ -I priv/mibs/ $?
	$(mib_verbose) erlc -o include/ -- $(addprefix priv/mibs/,$(patsubst %.mib,%.bin,$(notdir $?)))
endif

# Leex and Yecc files.

XRL_FILES := $(filter %.xrl,$(ALL_SRC_FILES))
XRL_ERL_FILES = $(addprefix src/,$(patsubst %.xrl,%.erl,$(notdir $(XRL_FILES))))
ERL_FILES += $(XRL_ERL_FILES)

YRL_FILES := $(filter %.yrl,$(ALL_SRC_FILES))
YRL_ERL_FILES = $(addprefix src/,$(patsubst %.yrl,%.erl,$(notdir $(YRL_FILES))))
ERL_FILES += $(YRL_ERL_FILES)

$(PROJECT).d:: $(XRL_FILES) $(YRL_FILES)
	$(if $(strip $?),$(xyrl_verbose) erlc -v -o src/ $(YRL_ERLC_OPTS) $?)

# Erlang and Core Erlang files.

define makedep.erl
	E = ets:new(makedep, [bag]),
	G = digraph:new([acyclic]),
	ErlFiles = lists:usort(string:tokens("$(ERL_FILES)", " ")),
	DepsDir = "$(call core_native_path,$(DEPS_DIR))",
	AppsDir = "$(call core_native_path,$(APPS_DIR))",
	DepsDirsSrc = "$(if $(wildcard $(DEPS_DIR)/*/src), $(call core_native_path,$(wildcard $(DEPS_DIR)/*/src)))",
	DepsDirsInc = "$(if $(wildcard $(DEPS_DIR)/*/include), $(call core_native_path,$(wildcard $(DEPS_DIR)/*/include)))",
	AppsDirsSrc = "$(if $(wildcard $(APPS_DIR)/*/src), $(call core_native_path,$(wildcard $(APPS_DIR)/*/src)))",
	AppsDirsInc = "$(if $(wildcard $(APPS_DIR)/*/include), $(call core_native_path,$(wildcard $(APPS_DIR)/*/include)))",
	DepsDirs = lists:usort(string:tokens(DepsDirsSrc++DepsDirsInc, " ")),
	AppsDirs = lists:usort(string:tokens(AppsDirsSrc++AppsDirsInc, " ")),
	Modules = [{list_to_atom(filename:basename(F, ".erl")), F} || F <- ErlFiles],
	Add = fun (Mod, Dep) ->
		case lists:keyfind(Dep, 1, Modules) of
			false -> ok;
			{_, DepFile} ->
				{_, ModFile} = lists:keyfind(Mod, 1, Modules),
				ets:insert(E, {ModFile, DepFile}),
				digraph:add_vertex(G, Mod),
				digraph:add_vertex(G, Dep),
				digraph:add_edge(G, Mod, Dep)
		end
	end,
	AddHd = fun (F, Mod, DepFile) ->
		case file:open(DepFile, [read]) of
			{error, enoent} ->
				ok;
			{ok, Fd} ->
				{_, ModFile} = lists:keyfind(Mod, 1, Modules),
				case ets:match(E, {ModFile, DepFile}) of
					[] ->
						ets:insert(E, {ModFile, DepFile}),
						F(F, Fd, Mod,0);
					_ -> ok
				end
		end
	end,
	SearchHrl = fun
		F(_Hrl, []) -> {error,enoent};
		F(Hrl, [Dir|Dirs]) ->
			HrlF = filename:join([Dir,Hrl]),
			case filelib:is_file(HrlF) of
				true  ->
				{ok, HrlF};
				false -> F(Hrl,Dirs)
			end
	end,
	Attr = fun
		(_F, Mod, behavior, Dep) ->
			Add(Mod, Dep);
		(_F, Mod, behaviour, Dep) ->
			Add(Mod, Dep);
		(_F, Mod, compile, {parse_transform, Dep}) ->
			Add(Mod, Dep);
		(_F, Mod, compile, Opts) when is_list(Opts) ->
			case proplists:get_value(parse_transform, Opts) of
				undefined -> ok;
				Dep -> Add(Mod, Dep)
			end;
		(F, Mod, include, Hrl) ->
			case SearchHrl(Hrl, ["src", "include",AppsDir,DepsDir]++AppsDirs++DepsDirs) of
				{ok, FoundHrl} -> AddHd(F, Mod, FoundHrl);
				{error, _} -> false
			end;
		(F, Mod, include_lib, Hrl) ->
			case SearchHrl(Hrl, ["src", "include",AppsDir,DepsDir]++AppsDirs++DepsDirs) of
				{ok, FoundHrl} -> AddHd(F, Mod, FoundHrl);
				{error, _} -> false
			end;
		(F, Mod, import, {Imp, _}) ->
			IsFile =
				case lists:keyfind(Imp, 1, Modules) of
					false -> false;
					{_, FilePath} -> filelib:is_file(FilePath)
				end,
			case IsFile of
				false -> ok;
				true -> Add(Mod, Imp)
			end;
		(_, _, _, _) -> ok
	end,
	MakeDepend = fun
		(F, Fd, Mod, StartLocation) ->
			case io:parse_erl_form(Fd, undefined, StartLocation) of
				{ok, AbsData, EndLocation} ->
					case AbsData of
						{attribute, _, Key, Value} ->
							Attr(F, Mod, Key, Value),
							F(F, Fd, Mod, EndLocation);
						_ -> F(F, Fd, Mod, EndLocation)
					end;
				{eof, _ } -> file:close(Fd);
				{error, ErrorDescription } ->
					file:close(Fd);
				{error, ErrorInfo, ErrorLocation} ->
					F(F, Fd, Mod, ErrorLocation)
			end,
			ok
	end,
	[begin
		Mod = list_to_atom(filename:basename(F, ".erl")),
		case file:open(F, [read]) of
			{ok, Fd} -> MakeDepend(MakeDepend, Fd, Mod,0);
			{error, enoent} -> ok
		end
	end || F <- ErlFiles],
	Depend = sofs:to_external(sofs:relation_to_family(sofs:relation(ets:tab2list(E)))),
	CompileFirst = [X || X <- lists:reverse(digraph_utils:topsort(G)), [] =/= digraph:in_neighbours(G, X)],
	TargetPath = fun(Target) ->
		case lists:keyfind(Target, 1, Modules) of
			false -> "";
			{_, DepFile} ->
				DirSubname = tl(string:tokens(filename:dirname(DepFile), "/")),
				string:join(DirSubname ++ [atom_to_list(Target)], "/")
		end
	end,
	Output0 = [
		"# Generated by Erlang.mk. Edit at your own risk!\n\n",
		[[F, "::", [[" ", D] || D <- Deps], "; @touch \$$@\n"] || {F, Deps} <- Depend],
		"\nCOMPILE_FIRST +=", [[" ", TargetPath(CF)] || CF <- CompileFirst], "\n"
	],
	Output = case "é" of
		[233] -> unicode:characters_to_binary(Output0);
		_ -> Output0
	end,
	ok = file:write_file("$1", Output),
	halt()
endef

ifeq ($(if $(NO_MAKEDEP),$(wildcard $(PROJECT).d),),)
$(PROJECT).d:: $(ERL_FILES) $(EX_FILES) $(call core_find,include/,*.hrl) $(MAKEFILE_LIST)
	$(makedep_verbose) $(call erlang,$(call makedep.erl,$@))
endif

ifeq ($(IS_APP)$(IS_DEP),)
ifneq ($(words $(ERL_FILES) $(EX_FILES) $(CORE_FILES) $(ASN1_FILES) $(MIB_FILES) $(XRL_FILES) $(YRL_FILES) $(EX_FILES)),0)
# Rebuild everything when the Makefile changes.
$(ERLANG_MK_TMP)/last-makefile-change: $(MAKEFILE_LIST) | $(ERLANG_MK_TMP)
	$(verbose) if test -f $@; then \
		touch $(ERL_FILES) $(EX_FILES) $(CORE_FILES) $(ASN1_FILES) $(MIB_FILES) $(XRL_FILES) $(YRL_FILES) $(EX_FILES); \
		touch -c $(PROJECT).d; \
	fi
	$(verbose) touch $@

$(ERL_FILES) $(EX_FILES) $(CORE_FILES) $(ASN1_FILES) $(MIB_FILES) $(XRL_FILES) $(YRL_FILES):: $(ERLANG_MK_TMP)/last-makefile-change
ebin/$(PROJECT).app:: $(ERLANG_MK_TMP)/last-makefile-change
endif
endif

$(PROJECT).d::
	$(verbose) :

include $(wildcard $(PROJECT).d)

ebin/$(PROJECT).app:: ebin/

ebin/:
	$(verbose) mkdir -p ebin/

define compile_erl
	$(erlc_verbose) erlc -v $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(filter-out $(ERLC_EXCLUDE_PATHS),$(COMPILE_FIRST_PATHS) $1)
endef

define validate_app_file
	case file:consult("ebin/$(PROJECT).app") of
		{ok, _} -> halt();
		_ -> halt(1)
	end
endef

ebin/$(PROJECT).app:: $(ERL_FILES) $(CORE_FILES) $(wildcard src/$(PROJECT).app.src) $(EX_FILES)
	$(eval FILES_TO_COMPILE := $(filter-out $(EX_FILES) src/$(PROJECT).app.src,$?))
	$(if $(strip $(FILES_TO_COMPILE)),$(call compile_erl,$(FILES_TO_COMPILE)))
	$(if $(filter $(ELIXIR),disable),,$(if $(filter $?,$(EX_FILES)),$(elixirc_verbose) $(eval MODULES := $(shell $(call erlang,$(call compile_ex.erl,$(EX_FILES)))))))
	$(eval ELIXIR_COMP_FAILED := $(if $(filter _ERROR_,$(firstword $(MODULES))),true,false))
# Older git versions do not have the --first-parent flag. Do without in that case.
	$(verbose) if $(ELIXIR_COMP_FAILED); then exit 1; fi
	$(eval GITDESCRIBE := $(shell git describe --dirty --abbrev=7 --tags --always --first-parent 2>/dev/null \
		|| git describe --dirty --abbrev=7 --tags --always 2>/dev/null || true))
	$(eval MODULES := $(MODULES) $(patsubst %,'%',$(sort $(notdir $(basename \
		$(filter-out $(ERLC_EXCLUDE_PATHS),$(ERL_FILES) $(CORE_FILES) $(BEAM_FILES)))))))
ifeq ($(wildcard src/$(PROJECT).app.src),)
	$(app_verbose) printf '$(subst %,%%,$(subst $(newline),\n,$(subst ','\'',$(call app_file,$(GITDESCRIBE),$(MODULES)))))' \
		> ebin/$(PROJECT).app
	$(verbose) if ! $(call erlang,$(call validate_app_file)); then \
		echo "The .app file produced is invalid. Please verify the value of PROJECT_ENV." >&2; \
		exit 1; \
	fi
else
	$(verbose) if [ -z "$$(grep -e '^[^%]*{\s*modules\s*,' src/$(PROJECT).app.src)" ]; then \
		echo "Empty modules entry not found in $(PROJECT).app.src. Please consult the erlang.mk documentation for instructions." >&2; \
		exit 1; \
	fi
	$(appsrc_verbose) cat src/$(PROJECT).app.src \
		| sed "s/{[[:space:]]*modules[[:space:]]*,[[:space:]]*\[\]}/{modules, \[$(call comma_list,$(MODULES))\]}/" \
		| sed "s/{id,[[:space:]]*\"git\"}/{id, \"$(subst /,\/,$(GITDESCRIBE))\"}/" \
		> ebin/$(PROJECT).app
endif
ifneq ($(wildcard src/$(PROJECT).appup),)
	$(verbose) cp src/$(PROJECT).appup ebin/
endif

clean:: clean-app

clean-app:
	$(gen_verbose) rm -rf $(PROJECT).d ebin/ priv/mibs/ $(XRL_ERL_FILES) $(YRL_ERL_FILES) \
		$(addprefix include/,$(patsubst %.mib,%.hrl,$(notdir $(MIB_FILES)))) \
		$(addprefix include/,$(patsubst %.asn1,%.hrl,$(notdir $(ASN1_FILES)))) \
		$(addprefix include/,$(patsubst %.asn1,%.asn1db,$(notdir $(ASN1_FILES)))) \
		$(addprefix src/,$(patsubst %.asn1,%.erl,$(notdir $(ASN1_FILES))))

endif

# Copyright (c) 2024, Tyler Hughes <tyler@tylerhughes.dev>
# Copyright (c) 2024, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Elixir is automatically enabled in all cases except when
# an Erlang project uses an Elixir dependency. In that case
# $(ELIXIR) must be set explicitly.
ELIXIR ?= $(if $(filter elixir,$(BUILD_DEPS) $(DEPS)),dep,$(if $(EX_FILES),system,disable))
export ELIXIR

ifeq ($(ELIXIR),system)
# We expect 'elixir' to be on the path.
ELIXIR_BIN ?= $(shell readlink -f `which elixir`)
ELIXIR_LIBS ?= $(abspath $(dir $(ELIXIR_BIN))/../lib)
# Fallback in case 'elixir' is a shim.
ifeq ($(wildcard $(ELIXIR_LIBS)/elixir/),)
ELIXIR_LIBS = $(abspath $(shell elixir -e 'IO.puts(:code.lib_dir(:elixir))')/../)
endif
ELIXIR_LIBS := $(ELIXIR_LIBS)
export ELIXIR_LIBS
ERL_LIBS := $(ERL_LIBS):$(ELIXIR_LIBS)
else
ifeq ($(ELIXIR),dep)
ERL_LIBS := $(ERL_LIBS):$(DEPS_DIR)/elixir/lib/
endif
endif

elixirc_verbose_0 = @echo " EXC    $(words $(EX_FILES)) files";
elixirc_verbose_2 = set -x;
elixirc_verbose = $(elixirc_verbose_$(V))

# Unfortunately this currently requires Elixir.
# https://github.com/jelly-beam/verl is a good choice
# for an Erlang implementation, but we already have to
# pull hex_core and Rebar3 so adding yet another pull
# is annoying, especially one that would be necessary
# every time we autopatch Rebar projects. Wait and see.
define hex_version_resolver.erl
	HexVersionResolve = fun(Name, Req) ->
		application:ensure_all_started(ssl),
		application:ensure_all_started(inets),
		Config = $(hex_config.erl),
		case hex_repo:get_package(Config, atom_to_binary(Name)) of
			{ok, {200, _RespHeaders, Package}} ->
				#{releases := List} = Package,
				{value, #{version := Version}} = lists:search(fun(#{version := Vsn}) ->
					M = list_to_atom("Elixir.Version"),
					F = list_to_atom("match?"),
					M:F(Vsn, Req)
				end, List),
				{ok, Version};
			{ok, {Status, _, Errors}} ->
				{error, Status, Errors}
		end
	end,
	HexVersionResolveAndPrint = fun(Name, Req) ->
		case HexVersionResolve(Name, Req) of
			{ok, Version} ->
				io:format("~s", [Version]),
				halt(0);
			{error, Status, Errors} ->
				io:format("Error ~b: ~0p~n", [Status, Errors]),
				halt(77)
		end
	end
endef

define dep_autopatch_mix.erl
	$(call hex_version_resolver.erl),
	{ok, _} = application:ensure_all_started(elixir),
	{ok, _} = application:ensure_all_started(mix),
	MixFile = <<"$(call core_native_path,$(DEPS_DIR)/$1/mix.exs)">>,
	{Mod, Bin} =
		case elixir_compiler:file(MixFile, fun(_File, _LexerPid) -> ok end) of
			[{T = {_, _}, _CheckerPid}] -> T;
			[T = {_, _}] -> T
		end,
	{module, Mod} = code:load_binary(Mod, binary_to_list(MixFile), Bin),
	Project = Mod:project(),
	Application = try Mod:application() catch error:undef -> [] end,
	StartMod = case lists:keyfind(mod, 1, Application) of
		{mod, {StartMod0, _StartArgs}} ->
			atom_to_list(StartMod0);
		_ ->
			""
	end,
	Write = fun (Text) ->
		file:write_file("$(call core_native_path,$(DEPS_DIR)/$1/Makefile)", Text, [append])
	end,
	Write([
		"PROJECT = ", atom_to_list(proplists:get_value(app, Project)), "\n"
		"PROJECT_DESCRIPTION = ", proplists:get_value(description, Project, ""), "\n"
		"PROJECT_VERSION = ", proplists:get_value(version, Project, ""), "\n"
		"PROJECT_MOD = ", StartMod, "\n"
		"define PROJECT_ENV\n",
		io_lib:format("~p", [proplists:get_value(env, Application, [])]), "\n"
		"endef\n\n"]),
	ExtraApps = lists:usort([eex, elixir, logger, mix] ++ proplists:get_value(extra_applications, Application, [])),
	Write(["LOCAL_DEPS += ", lists:join(" ", [atom_to_list(App) || App <- ExtraApps]), "\n\n"]),
	Deps = proplists:get_value(deps, Project, []) -- [elixir_make],
	IsRequiredProdDep = fun(Opts) ->
		(proplists:get_value(optional, Opts) =/= true)
		andalso
		case proplists:get_value(only, Opts, prod) of
			prod -> true;
			L when is_list(L) -> lists:member(prod, L);
			_ -> false
		end
	end,
	lists:foreach(fun
		({Name, Req}) when is_binary(Req) ->
			{ok, Vsn} = HexVersionResolve(Name, Req),
			Write(["DEPS += ", atom_to_list(Name), "\n"]),
			Write(["dep_", atom_to_list(Name), " = hex ", Vsn, " ", atom_to_list(Name), "\n"]);
		({Name, Opts}) when is_list(Opts) ->
			Path = proplists:get_value(path, Opts),
			case IsRequiredProdDep(Opts) of
				true when Path =/= undefined ->
					Write(["DEPS += ", atom_to_list(Name), "\n"]),
					Write(["dep_", atom_to_list(Name), " = ln ", Path, "\n"]);
				true when Path =:= undefined ->
					Write(["DEPS += ", atom_to_list(Name), "\n"]),
					io:format(standard_error, "Warning: No version given for ~p.", [Name]);
				false ->
					ok
			end;
		({Name, Req, Opts}) ->
			case IsRequiredProdDep(Opts) of
				true ->
					{ok, Vsn} = HexVersionResolve(Name, Req),
					Write(["DEPS += ", atom_to_list(Name), "\n"]),
					Write(["dep_", atom_to_list(Name), " = hex ", Vsn, " ", atom_to_list(Name), "\n"]);
				false ->
					ok
			end;
		(_) ->
			ok
	end, Deps),
	case lists:member(elixir_make, proplists:get_value(compilers, Project, [])) of
		false -> 
			ok;
		true ->
			Write("# https://hexdocs.pm/elixir_make/Mix.Tasks.Compile.ElixirMake.html\n"),
			MakeVal = fun(Key, Proplist, DefaultVal, DefaultReplacement) ->
				case proplists:get_value(Key, Proplist, DefaultVal) of
					DefaultVal -> DefaultReplacement;
					Value -> Value
				end
			end,
			MakeMakefile = binary_to_list(MakeVal(make_makefile, Project, default, <<"Makefile">>)),
			MakeExe = MakeVal(make_executable, Project, default, "$$\(MAKE)"),
			MakeCwd = MakeVal(make_cwd, Project, undefined, <<".">>),
			MakeTargets = MakeVal(make_targets, Project, [], []),
			MakeArgs = MakeVal(make_args, Project, undefined, []),
			case file:rename("$(DEPS_DIR)/$1/" ++ MakeMakefile, "$(DEPS_DIR)/$1/elixir_make.mk") of
				ok -> ok;
				Err = {error, _} ->
					io:format(standard_error, "Failed to copy Makefile with error ~p~n", [Err]),
					halt(90)
			end,
			Write(["app::\n"
				"\t", MakeExe, " -C ", MakeCwd, " -f $(DEPS_DIR)/$1/elixir_make.mk",
				lists:join(" ", MakeTargets),
				lists:join(" ", MakeArgs),
				"\n\n"]),
			case MakeVal(make_clean, Project, nil, undefined) of
				undefined ->
					ok;
				Clean ->
					Write(["clean::\n\t", Clean, "\n\n"])
			end
	end,
	Write("ERLC_OPTS = +debug_info\n\n"),
	Write("include $$\(if $$\(ERLANG_MK_FILENAME),$$\(ERLANG_MK_FILENAME),erlang.mk)"),
	halt()
endef

define dep_autopatch_mix
	sed 's|\(defmodule.*do\)|\1\n  try do\n    Code.compiler_options(on_undefined_variable: :warn)\n    rescue _ -> :ok\n  end\n|g' -i $(DEPS_DIR)/$(1)/mix.exs; \
	$(MAKE) $(DEPS_DIR)/hex_core/ebin/dep_built; \
	MIX_ENV="$(if $(MIX_ENV),$(strip $(MIX_ENV)),prod)" \
		$(call erlang,$(call dep_autopatch_mix.erl,$1))
endef

# We change the group leader so the Elixir io:format output
# isn't captured as we need to either print the modules on
# success, or print _ERROR_ on failure.
define compile_ex.erl
	{ok, _} = application:ensure_all_started(elixir),
	{ok, _} = application:ensure_all_started(mix),
	ModCode = list_to_atom("Elixir.Code"),
	ModCode:put_compiler_option(ignore_module_conflict, true),
	ModComp = list_to_atom("Elixir.Kernel.ParallelCompiler"),
	ModMixProject = list_to_atom("Elixir.Mix.Project"),
	erlang:group_leader(whereis(standard_error), self()),
	ModMixProject:in_project($(PROJECT), ".", [], fun(_MixFile) ->
		case ModComp:compile_to_path([$(call comma_list,$(patsubst %,<<"%">>,$1))], <<"ebin/">>) of
			{ok, Modules, _} ->
				lists:foreach(fun(E) -> io:format(user, "~p ", [E]) end, Modules),
				halt(0);
			{error, _ErroredModules, _WarnedModules} ->
				io:format(user, "_ERROR_", []),
				halt(1)
		end
	end)
endef

# Copyright (c) 2016, Loïc Hoguin <essen@ninenines.eu>
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
	$(verbose) set -e; for dep in $(ALL_DOC_DEPS_DIRS) ; do $(MAKE) -C $$dep IS_DEP=1; done
endif

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: rel-deps

# Configuration.

ALL_REL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(REL_DEPS))

# Targets.

$(foreach dep,$(REL_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
rel-deps:
else
rel-deps: $(ALL_REL_DEPS_DIRS)
	$(verbose) set -e; for dep in $(ALL_REL_DEPS_DIRS) ; do $(MAKE) -C $$dep; done
endif

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
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
	$(verbose) set -e; for dep in $(ALL_TEST_DEPS_DIRS) ; do \
		if [ -z "$(strip $(FULL))" ] && [ ! -L $$dep ] && [ -f $$dep/ebin/dep_built ]; then \
			:; \
		else \
			$(MAKE) -C $$dep IS_DEP=1; \
			if [ ! -L $$dep ] && [ -d $$dep/ebin ]; then touch $$dep/ebin/dep_built; fi; \
		fi \
	done
endif

ifneq ($(wildcard $(TEST_DIR)),)
test-dir: $(ERLANG_MK_TMP)/$(PROJECT).last-testdir-build
	@:

test_erlc_verbose_0 = @echo " ERLC  " $(filter-out $(patsubst %,%.erl,$(ERLC_EXCLUDE)),\
	$(filter %.erl %.core,$(notdir $(FILES_TO_COMPILE))));
test_erlc_verbose_2 = set -x;
test_erlc_verbose = $(test_erlc_verbose_$(V))

define compile_test_erl
	$(test_erlc_verbose) erlc -v $(TEST_ERLC_OPTS) -o $(TEST_DIR) \
		-pa ebin/ -I include/ $1
endef

ERL_TEST_FILES = $(call core_find,$(TEST_DIR)/,*.erl)

$(ERLANG_MK_TMP)/$(PROJECT).last-testdir-build: $(ERL_TEST_FILES) $(MAKEFILE_LIST)
# When we have to recompile files in src/ the .d file always gets rebuilt.
# Therefore we want to ignore it when rebuilding test files.
	$(eval FILES_TO_COMPILE := $(if $(filter $(filter-out $(PROJECT).d,$(MAKEFILE_LIST)),$?),$(filter $(ERL_TEST_FILES),$^),$(filter $(ERL_TEST_FILES),$?)))
	$(if $(strip $(FILES_TO_COMPILE)),$(call compile_test_erl,$(FILES_TO_COMPILE)) && touch $@)
endif

test-build:: IS_TEST=1
test-build:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build:: $(if $(wildcard src),$(if $(wildcard ebin/test),,beam-cache-restore-test)) $(if $(IS_APP),,deps test-deps)
# We already compiled everything when IS_APP=1.
ifndef IS_APP
ifneq ($(wildcard src),)
	$(verbose) $(MAKE) --no-print-directory $(PROJECT).d ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
	$(verbose) $(MAKE) --no-print-directory app-build ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
	$(gen_verbose) touch ebin/test
endif
ifneq ($(wildcard $(TEST_DIR)),)
	$(verbose) $(MAKE) --no-print-directory test-dir ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
endif
endif

# Roughly the same as test-build, but when IS_APP=1.
# We only care about compiling the current application.
ifdef IS_APP
test-build-app:: ERLC_OPTS=$(TEST_ERLC_OPTS)
test-build-app:: deps test-deps
ifneq ($(wildcard src),)
	$(verbose) $(MAKE) --no-print-directory $(PROJECT).d ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
	$(verbose) $(MAKE) --no-print-directory app-build ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
	$(gen_verbose) touch ebin/test
endif
ifneq ($(wildcard $(TEST_DIR)),)
	$(verbose) $(MAKE) --no-print-directory test-dir ERLC_OPTS="$(call escape_dquotes,$(TEST_ERLC_OPTS))"
endif
endif

clean:: clean-test-dir

clean-test-dir:
ifneq ($(wildcard $(TEST_DIR)/*.beam),)
	$(gen_verbose) rm -f $(TEST_DIR)/*.beam $(ERLANG_MK_TMP)/$(PROJECT).last-testdir-build
endif

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: rebar.config

compat_ref = {$(shell (git -C $(DEPS_DIR)/$1 show-ref -q --verify "refs/heads/$2" && echo branch) || (git -C $(DEPS_DIR)/$1 show-ref -q --verify "refs/tags/$2" && echo tag) || echo ref),"$2"}

# We strip out -Werror because we don't want to fail due to
# warnings when used as a dependency.

compat_prepare_erlc_opts = $(shell echo "$1" | sed 's/, */,/g')

define compat_convert_erlc_opts
$(if $(filter-out -Werror,$1),\
	$(if $(findstring +,$1),\
		$(shell echo $1 | cut -b 2-)))
endef

define compat_erlc_opts_to_list
[$(call comma_list,$(foreach o,$(call compat_prepare_erlc_opts,$1),$(call compat_convert_erlc_opts,$o)))]
endef

define compat_rebar_config
{deps, [
$(call comma_list,$(foreach d,$(DEPS),\
	$(if $(filter hex,$(call query_fetch_method,$d)),\
		{$(call query_name,$d)$(comma)"$(call query_version_hex,$d)"},\
		{$(call query_name,$d)$(comma)".*"$(comma){git,"$(call query_repo,$d)"$(comma)$(call compat_ref,$(call query_name,$d),$(call query_version,$d))}})))
]}.
{erl_opts, $(call compat_erlc_opts_to_list,$(ERLC_OPTS))}.
endef

rebar.config: deps
	$(gen_verbose) $(call core_render,compat_rebar_config,rebar.config)

define tpl_application.app.src
{application, project_name, [
	{description, ""},
	{vsn, "0.1.0"},
	{id, "git"},
	{modules, []},
	{registered, []},
	{applications, [
		kernel,
		stdlib
	]},
	{mod, {project_name_app, []}},
	{env, []}
]}.
endef

define tpl_application
-module(project_name_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	project_name_sup:start_link().

stop(_State) ->
	ok.
endef

define tpl_apps_Makefile
PROJECT = project_name
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0
template_sp
# Make sure we know where the applications are located.
ROOT_DIR ?= rel_root_dir
APPS_DIR ?= ..
DEPS_DIR ?= rel_deps_dir

include rel_root_dir/erlang.mk
endef

define tpl_cowboy_http_h
-module(template_name).
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

define tpl_cowboy_loop_h
-module(template_name).
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

define tpl_cowboy_rest_h
-module(template_name).

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

define tpl_cowboy_websocket_h
-module(template_name).
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

define tpl_gen_fsm
-module(template_name).
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

define tpl_gen_server
-module(template_name).
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

define tpl_gen_statem
-module(template_name).
-behaviour(gen_statem).

%% API.
-export([start_link/0]).

%% gen_statem.
-export([callback_mode/0]).
-export([init/1]).
-export([state_name/3]).
-export([handle_event/4]).
-export([terminate/3]).
-export([code_change/4]).

-record(state, {
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
	gen_statem:start_link(?MODULE, [], []).

%% gen_statem.

callback_mode() ->
	state_functions.

init([]) ->
	{ok, state_name, #state{}}.

state_name(_EventType, _EventData, StateData) ->
	{next_state, state_name, StateData}.

handle_event(_EventType, _EventData, StateName, StateData) ->
	{next_state, StateName, StateData}.

terminate(_Reason, _StateName, _StateData) ->
	ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
	{ok, StateName, StateData}.
endef

define tpl_library.app.src
{application, project_name, [
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

define tpl_module
-module(template_name).
-export([]).
endef

define tpl_ranch_protocol
-module(template_name).
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

define tpl_relx.config
{release, {project_name_release, "1"}, [project_name, sasl, runtime_tools]}.
{dev_mode, false}.
{include_erts, true}.
{extended_start_script, true}.
{sys_config, "config/sys.config"}.
{vm_args, "config/vm.args"}.
endef

define tpl_supervisor
-module(template_name).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
	Procs = [],
	{ok, {{one_for_one, 1, 5}, Procs}}.
endef

define tpl_sys.config
[
].
endef

define tpl_top_Makefile
PROJECT = project_name
PROJECT_DESCRIPTION = New project
PROJECT_VERSION = 0.1.0
template_sp
include erlang.mk
endef

define tpl_vm.args
-name project_name@127.0.0.1
-setcookie project_name
-heart
endef


# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(filter asciideck,$(DEPS) $(DOC_DEPS)),asciideck)

.PHONY: asciidoc asciidoc-guide asciidoc-manual install-asciidoc distclean-asciidoc-guide distclean-asciidoc-manual

# Core targets.

docs:: asciidoc

distclean:: distclean-asciidoc-guide distclean-asciidoc-manual

# Plugin-specific targets.

asciidoc: asciidoc-guide asciidoc-manual

# User guide.

ifeq ($(wildcard doc/src/guide/book.asciidoc),)
asciidoc-guide:
else
asciidoc-guide: distclean-asciidoc-guide doc-deps
	a2x -v -f pdf doc/src/guide/book.asciidoc && mv doc/src/guide/book.pdf doc/guide.pdf
	a2x -v -f chunked doc/src/guide/book.asciidoc && mv doc/src/guide/book.chunked/ doc/html/

distclean-asciidoc-guide:
	$(gen_verbose) rm -rf doc/html/ doc/guide.pdf
endif

# Man pages.

ASCIIDOC_MANUAL_FILES := $(wildcard doc/src/manual/*.asciidoc)

ifeq ($(ASCIIDOC_MANUAL_FILES),)
asciidoc-manual:
else

# Configuration.

MAN_INSTALL_PATH ?= /usr/local/share/man
MAN_SECTIONS ?= 3 7
MAN_PROJECT ?= $(shell echo $(PROJECT) | sed 's/^./\U&\E/')
MAN_VERSION ?= $(PROJECT_VERSION)

# Plugin-specific targets.

define asciidoc2man.erl
try
	[begin
		io:format(" ADOC   ~s~n", [F]),
		ok = asciideck:to_manpage(asciideck:parse_file(F), #{
			compress => gzip,
			outdir => filename:dirname(F),
			extra2 => "$(MAN_PROJECT) $(MAN_VERSION)",
			extra3 => "$(MAN_PROJECT) Function Reference"
		})
	end || F <- [$(shell echo $(addprefix $(comma)\",$(addsuffix \",$1)) | sed 's/^.//')]],
	halt(0)
catch C:E$(if $V,:S) ->
	io:format("Exception: ~p:~p~n$(if $V,Stacktrace: ~p~n)", [C, E$(if $V,$(comma) S)]),
	halt(1)
end.
endef

asciidoc-manual:: doc-deps

asciidoc-manual:: $(ASCIIDOC_MANUAL_FILES)
	$(gen_verbose) $(call erlang,$(call asciidoc2man.erl,$?))
	$(verbose) $(foreach s,$(MAN_SECTIONS),mkdir -p doc/man$s/ && mv doc/src/manual/*.$s.gz doc/man$s/;)

install-docs:: install-asciidoc

install-asciidoc: asciidoc-manual
	$(foreach s,$(MAN_SECTIONS),\
		mkdir -p $(MAN_INSTALL_PATH)/man$s/ && \
		install -g `id -g` -o `id -u` -m 0644 doc/man$s/*.gz $(MAN_INSTALL_PATH)/man$s/;)

distclean-asciidoc-manual:
	$(gen_verbose) rm -rf $(addprefix doc/man,$(MAN_SECTIONS))
endif
endif

# Copyright (c) 2014-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: bootstrap bootstrap-lib bootstrap-rel new list-templates

# Core targets.

help::
	$(verbose) printf "%s\n" "" \
		"Bootstrap targets:" \
		"  bootstrap          Generate a skeleton of an OTP application" \
		"  bootstrap-lib      Generate a skeleton of an OTP library" \
		"  bootstrap-rel      Generate the files needed to build a release" \
		"  new-app in=NAME    Create a new local OTP application NAME" \
		"  new-lib in=NAME    Create a new local OTP library NAME" \
		"  new t=TPL n=NAME   Generate a module NAME based on the template TPL" \
		"  new t=T n=N in=APP Generate a module NAME based on the template TPL in APP" \
		"  list-templates     List available templates"

# Plugin-specific targets.

ifndef WS
ifdef SP
WS = $(subst a,,a $(wordlist 1,$(SP),a a a a a a a a a a a a a a a a a a a a))
else
WS = $(tab)
endif
endif

ifdef SP
define template_sp

# By default templates indent with a single tab per indentation
# level. Set this variable to the number of spaces you prefer:
SP = $(SP)

endef
else
template_sp =
endif

# @todo Additional template placeholders could be added.
subst_template = $(subst rel_root_dir,$(call core_relpath,$(dir $(ERLANG_MK_FILENAME)),$(APPS_DIR)/app),$(subst rel_deps_dir,$(call core_relpath,$(DEPS_DIR),$(APPS_DIR)/app),$(subst template_sp,$(template_sp),$(subst project_name,$p,$(subst template_name,$n,$1)))))

define core_render_template
	$(eval define _tpl_$(1)$(newline)$(call subst_template,$(tpl_$(1)))$(newline)endef)
	$(verbose) $(call core_render,_tpl_$(1),$2)
endef

bootstrap:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(if $(shell echo $p | LC_ALL=C grep -x "[a-z0-9_]*"),,\
		$(error Error: Invalid characters in the application name))
	$(eval n := $(PROJECT)_sup)
	$(verbose) $(call core_render_template,top_Makefile,Makefile)
	$(verbose) mkdir src/
ifdef LEGACY
	$(verbose) $(call core_render_template,application.app.src,src/$(PROJECT).app.src)
endif
	$(verbose) $(call core_render_template,application,src/$(PROJECT)_app.erl)
	$(verbose) $(call core_render_template,supervisor,src/$(PROJECT)_sup.erl)

bootstrap-lib:
ifneq ($(wildcard src/),)
	$(error Error: src/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(if $(shell echo $p | LC_ALL=C grep -x "[a-z0-9_]*"),,\
		$(error Error: Invalid characters in the application name))
	$(verbose) $(call core_render_template,top_Makefile,Makefile)
	$(verbose) mkdir src/
ifdef LEGACY
	$(verbose) $(call core_render_template,library.app.src,src/$(PROJECT).app.src)
endif

bootstrap-rel:
ifneq ($(wildcard relx.config),)
	$(error Error: relx.config already exists)
endif
ifneq ($(wildcard config/),)
	$(error Error: config/ directory already exists)
endif
	$(eval p := $(PROJECT))
	$(verbose) $(call core_render_template,relx.config,relx.config)
	$(verbose) mkdir config/
	$(verbose) $(call core_render_template,sys.config,config/sys.config)
	$(verbose) $(call core_render_template,vm.args,config/vm.args)
	$(verbose) awk '/^include erlang.mk/ && !ins {print "REL_DEPS += relx";ins=1};{print}' Makefile > Makefile.bak
	$(verbose) mv Makefile.bak Makefile

new-app:
ifndef in
	$(error Usage: $(MAKE) new-app in=APP)
endif
ifneq ($(wildcard $(APPS_DIR)/$in),)
	$(error Error: Application $in already exists)
endif
	$(eval p := $(in))
	$(if $(shell echo $p | LC_ALL=C grep -x "[a-z0-9_]*"),,\
		$(error Error: Invalid characters in the application name))
	$(eval n := $(in)_sup)
	$(verbose) mkdir -p $(APPS_DIR)/$p/src/
	$(verbose) $(call core_render_template,apps_Makefile,$(APPS_DIR)/$p/Makefile)
ifdef LEGACY
	$(verbose) $(call core_render_template,application.app.src,$(APPS_DIR)/$p/src/$p.app.src)
endif
	$(verbose) $(call core_render_template,application,$(APPS_DIR)/$p/src/$p_app.erl)
	$(verbose) $(call core_render_template,supervisor,$(APPS_DIR)/$p/src/$p_sup.erl)

new-lib:
ifndef in
	$(error Usage: $(MAKE) new-lib in=APP)
endif
ifneq ($(wildcard $(APPS_DIR)/$in),)
	$(error Error: Application $in already exists)
endif
	$(eval p := $(in))
	$(if $(shell echo $p | LC_ALL=C grep -x "[a-z0-9_]*"),,\
		$(error Error: Invalid characters in the application name))
	$(verbose) mkdir -p $(APPS_DIR)/$p/src/
	$(verbose) $(call core_render_template,apps_Makefile,$(APPS_DIR)/$p/Makefile)
ifdef LEGACY
	$(verbose) $(call core_render_template,library.app.src,$(APPS_DIR)/$p/src/$p.app.src)
endif

# These are not necessary because we don't expose those as "normal" templates.
BOOTSTRAP_TEMPLATES = apps_Makefile top_Makefile \
	application.app.src library.app.src application \
	relx.config sys.config vm.args

# Templates may override the path they will be written to when using 'new'.
# Only special template paths must be listed. Default is src/template_name.erl
# Substitution is also applied to the paths. Examples:
#
#tplp_top_Makefile = Makefile
#tplp_application.app.src = src/project_name.app.src
#tplp_application = src/project_name_app.erl
#tplp_relx.config = relx.config

# Erlang.mk bundles its own templates at build time into the erlang.mk file.

new:
	$(if $(t),,$(error Usage: $(MAKE) new t=TEMPLATE n=NAME [in=APP]))
	$(if $(n),,$(error Usage: $(MAKE) new t=TEMPLATE n=NAME [in=APP]))
	$(if $(tpl_$(t)),,$(error Error: $t template does not exist; try $(Make) list-templates))
	$(eval dest := $(if $(in),$(APPS_DIR)/$(in)/)$(call subst_template,$(if $(tplp_$(t)),$(tplp_$(t)),src/template_name.erl)))
	$(if $(wildcard $(dir $(dest))),,$(error Error: $(dir $(dest)) directory does not exist))
	$(if $(wildcard $(dest)),$(error Error: The file $(dest) already exists))
	$(eval p := $(PROJECT))
	$(call core_render_template,$(t),$(dest))

list-templates:
	$(verbose) @echo Available templates:
	$(verbose) printf "    %s\n" $(sort $(filter-out $(BOOTSTRAP_TEMPLATES),$(patsubst tpl_%,%,$(filter tpl_%,$(.VARIABLES)))))

# Copyright (c) 2014-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: clean-c_src distclean-c_src-env

# Configuration.

C_SRC_DIR ?= $(CURDIR)/c_src
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk
C_SRC_OUTPUT ?= $(CURDIR)/priv/$(PROJECT)
C_SRC_TYPE ?= shared

# System type and C compiler/flags.

ifeq ($(PLATFORM),msys2)
	C_SRC_OUTPUT_EXECUTABLE_EXTENSION ?= .exe
	C_SRC_OUTPUT_SHARED_EXTENSION ?= .dll
	C_SRC_OUTPUT_STATIC_EXTENSION ?= .lib
else
	C_SRC_OUTPUT_EXECUTABLE_EXTENSION ?=
	C_SRC_OUTPUT_SHARED_EXTENSION ?= .so
	C_SRC_OUTPUT_STATIC_EXTENSION ?= .a
endif

ifeq ($(C_SRC_TYPE),shared)
	C_SRC_OUTPUT_FILE = $(C_SRC_OUTPUT)$(C_SRC_OUTPUT_SHARED_EXTENSION)
else ifeq ($(C_SRC_TYPE),static)
	C_SRC_OUTPUT_FILE = $(C_SRC_OUTPUT)$(C_SRC_OUTPUT_STATIC_EXTENSION)
else
	C_SRC_OUTPUT_FILE = $(C_SRC_OUTPUT)$(C_SRC_OUTPUT_EXECUTABLE_EXTENSION)
endif

RANLIB ?= ranlib
ARFLAGS ?= cr

ifeq ($(PLATFORM),msys2)
# We hardcode the compiler used on MSYS2. The default CC=cc does
# not produce working code. The "gcc" MSYS2 package also doesn't.
	CC = /mingw64/bin/gcc
	export CC
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
else ifeq ($(PLATFORM),darwin)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -Wall
	LDFLAGS ?= -flat_namespace -undefined suppress
else ifeq ($(PLATFORM),freebsd)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
else ifeq ($(PLATFORM),linux)
	CC ?= gcc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -Wall
endif

ifneq ($(PLATFORM),msys2)
	CFLAGS += -fPIC
	CXXFLAGS += -fPIC
endif

ifeq ($(C_SRC_TYPE),static)
	CFLAGS += -DSTATIC_ERLANG_NIF=1
	CXXFLAGS += -DSTATIC_ERLANG_NIF=1
endif

CFLAGS += -I"$(ERTS_INCLUDE_DIR)" -I"$(ERL_INTERFACE_INCLUDE_DIR)"
CXXFLAGS += -I"$(ERTS_INCLUDE_DIR)" -I"$(ERL_INTERFACE_INCLUDE_DIR)"

LDLIBS += -L"$(ERL_INTERFACE_LIB_DIR)" -lei

# Verbosity.

c_verbose_0 = @echo " C     " $(filter-out $(notdir $(MAKEFILE_LIST) $(C_SRC_ENV)),$(^F));
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(filter-out $(notdir $(MAKEFILE_LIST) $(C_SRC_ENV)),$(^F));
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

ar_verbose_0 = @echo " AR    " $(@F);
ar_verbose = $(ar_verbose_$(V))

ranlib_verbose_0 = @echo " RANLIB" $(@F);
ranlib_verbose = $(ranlib_verbose_$(V))

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
SOURCES := $(sort $(foreach pat,*.c *.C *.cc *.cpp,$(call core_find,$(C_SRC_DIR)/,$(pat))))
endif
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

app:: $(C_SRC_ENV) $(C_SRC_OUTPUT_FILE)

test-build:: $(C_SRC_ENV) $(C_SRC_OUTPUT_FILE)

ifneq ($(C_SRC_TYPE),static)
$(C_SRC_OUTPUT_FILE): $(OBJECTS)
	$(verbose) mkdir -p $(dir $@)
	$(link_verbose) $(CC) $(OBJECTS) \
		$(LDFLAGS) $(if $(filter $(C_SRC_TYPE),shared),-shared) $(LDLIBS) \
		-o $(C_SRC_OUTPUT_FILE)
else
$(C_SRC_OUTPUT_FILE): $(OBJECTS)
	$(verbose) mkdir -p $(dir $@)
	$(ar_verbose) $(AR) $(ARFLAGS) $(C_SRC_OUTPUT_FILE) $(OBJECTS)
	$(ranlib_verbose) $(RANLIB) $(C_SRC_OUTPUT_FILE)
endif


$(OBJECTS): $(MAKEFILE_LIST) $(C_SRC_ENV)

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
	$(gen_verbose) rm -f $(C_SRC_OUTPUT_FILE) $(OBJECTS)

endif

ifneq ($(wildcard $(C_SRC_DIR)),)
ERL_ERTS_DIR = $(shell $(ERL) -eval 'io:format("~s~n", [code:lib_dir(erts)]), halt().')

$(C_SRC_ENV):
	$(verbose) $(ERL) -eval "file:write_file(\"$(call core_native_path,$(C_SRC_ENV))\", \
		io_lib:format( \
			\"# Generated by Erlang.mk. Edit at your own risk!~n~n\" \
			\"ERTS_INCLUDE_DIR ?= ~s/erts-~s/include/~n\" \
			\"ERL_INTERFACE_INCLUDE_DIR ?= ~s~n\" \
			\"ERL_INTERFACE_LIB_DIR ?= ~s~n\" \
			\"ERTS_DIR ?= $(ERL_ERTS_DIR)~n\", \
			[code:root_dir(), erlang:system_info(version), \
			code:lib_dir(erl_interface, include), \
			code:lib_dir(erl_interface, lib)])), \
		halt()."

distclean:: distclean-c_src-env

distclean-c_src-env:
	$(gen_verbose) rm -f $(C_SRC_ENV)

-include $(C_SRC_ENV)

ifneq ($(ERL_ERTS_DIR),$(ERTS_DIR))
$(shell rm -f $(C_SRC_ENV))
endif
endif

# Templates.

define bs_c_nif
#include "erl_nif.h"

static int loads = 0;

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
	/* Initialize private data. */
	*priv_data = NULL;

	loads++;

	return 0;
}

static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
	/* Convert the private data to the new version. */
	*priv_data = *old_priv_data;

	loads++;

	return 0;
}

static void unload(ErlNifEnv* env, void* priv_data)
{
	if (loads == 1) {
		/* Destroy the private data. */
	}

	loads--;
}

static ERL_NIF_TERM hello(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	if (enif_is_atom(env, argv[0])) {
		return enif_make_tuple2(env,
			enif_make_atom(env, "hello"),
			argv[0]);
	}

	return enif_make_tuple2(env,
		enif_make_atom(env, "error"),
		enif_make_atom(env, "badarg"));
}

static ErlNifFunc nif_funcs[] = {
	{"hello", 1, hello}
};

ERL_NIF_INIT($n, nif_funcs, load, NULL, upgrade, unload)
endef

define bs_erl_nif
-module($n).

-export([hello/1]).

-on_load(on_load/0).
on_load() ->
	PrivDir = case code:priv_dir(?MODULE) of
		{error, _} ->
			AppPath = filename:dirname(filename:dirname(code:which(?MODULE))),
			filename:join(AppPath, "priv");
		Path ->
			Path
	end,
	erlang:load_nif(filename:join(PrivDir, atom_to_list(?MODULE)), 0).

hello(_) ->
	erlang:nif_error({not_loaded, ?MODULE}).
endef

new-nif:
ifneq ($(wildcard $(C_SRC_DIR)/$n.c),)
	$(error Error: $(C_SRC_DIR)/$n.c already exists)
endif
ifneq ($(wildcard src/$n.erl),)
	$(error Error: src/$n.erl already exists)
endif
ifndef n
	$(error Usage: $(MAKE) new-nif n=NAME [in=APP])
endif
ifdef in
	$(verbose) $(MAKE) -C $(APPS_DIR)/$(in)/ new-nif n=$n in=
else
	$(verbose) mkdir -p $(C_SRC_DIR) src/
	$(verbose) $(call core_render,bs_c_nif,$(C_SRC_DIR)/$n.c)
	$(verbose) $(call core_render,bs_erl_nif,src/$n.erl)
endif

# Copyright (c) 2015-2017, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: ci ci-prepare ci-setup

CI_OTP ?=

ifeq ($(strip $(CI_OTP)),)
ci::
else

ci:: $(addprefix ci-,$(CI_OTP))

ci-prepare: $(addprefix ci-prepare-,$(CI_OTP))

ci-setup::
	$(verbose) :

ci-extra::
	$(verbose) :

ci_verbose_0 = @echo " CI    " $1;
ci_verbose = $(ci_verbose_$(V))

define ci_target
ci-prepare-$1: $(KERL_INSTALL_DIR)/$2
	$(verbose) :

ci-$1: ci-prepare-$1
	$(verbose) $(MAKE) --no-print-directory clean
	$(ci_verbose) \
		PATH="$(KERL_INSTALL_DIR)/$2/bin:$(PATH)" \
		CI_OTP_RELEASE="$1" \
		CT_OPTS="-label $1" \
		CI_VM="$3" \
		$(MAKE) ci-setup tests
	$(verbose) $(MAKE) --no-print-directory ci-extra
endef

$(foreach otp,$(CI_OTP),$(eval $(call ci_target,$(otp),$(otp),otp)))

$(foreach otp,$(filter-out $(ERLANG_OTP),$(CI_OTP)),$(eval $(call kerl_otp_target,$(otp))))

help::
	$(verbose) printf "%s\n" "" \
		"Continuous Integration targets:" \
		"  ci          Run '$(MAKE) tests' on all configured Erlang versions." \
		"" \
		"The CI_OTP variable must be defined with the Erlang versions" \
		"that must be tested. For example: CI_OTP = OTP-17.3.4 OTP-17.5.3"

endif

# Copyright (c) 2020, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifdef CONCUERROR_TESTS

.PHONY: concuerror distclean-concuerror

# Configuration

CONCUERROR_LOGS_DIR ?= $(CURDIR)/logs
CONCUERROR_OPTS ?=

# Core targets.

check:: concuerror

ifndef KEEP_LOGS
distclean:: distclean-concuerror
endif

# Plugin-specific targets.

$(ERLANG_MK_TMP)/Concuerror/bin/concuerror: | $(ERLANG_MK_TMP)
	$(verbose) git clone https://github.com/parapluu/Concuerror $(ERLANG_MK_TMP)/Concuerror
	$(verbose) $(MAKE) -C $(ERLANG_MK_TMP)/Concuerror

$(CONCUERROR_LOGS_DIR):
	$(verbose) mkdir -p $(CONCUERROR_LOGS_DIR)

define concuerror_html_report
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Concuerror HTML report</title>
</head>
<body>
<h1>Concuerror HTML report</h1>
<p>Generated on $(concuerror_date)</p>
<ul>
$(foreach t,$(concuerror_targets),<li><a href="$(t).txt">$(t)</a></li>)
</ul>
</body>
</html>
endef

concuerror: $(addprefix concuerror-,$(subst :,-,$(CONCUERROR_TESTS)))
	$(eval concuerror_date := $(shell date))
	$(eval concuerror_targets := $^)
	$(verbose) $(call core_render,concuerror_html_report,$(CONCUERROR_LOGS_DIR)/concuerror.html)

define concuerror_target
.PHONY: concuerror-$1-$2

concuerror-$1-$2: test-build | $(ERLANG_MK_TMP)/Concuerror/bin/concuerror $(CONCUERROR_LOGS_DIR)
	$(ERLANG_MK_TMP)/Concuerror/bin/concuerror \
		--pa $(CURDIR)/ebin --pa $(TEST_DIR) \
		-o $(CONCUERROR_LOGS_DIR)/concuerror-$1-$2.txt \
		$$(CONCUERROR_OPTS) -m $1 -t $2
endef

$(foreach test,$(CONCUERROR_TESTS),$(eval $(call concuerror_target,$(firstword $(subst :, ,$(test))),$(lastword $(subst :, ,$(test))))))

distclean-concuerror:
	$(gen_verbose) rm -rf $(CONCUERROR_LOGS_DIR)

endif

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: ct apps-ct distclean-ct

# Configuration.

CT_OPTS ?=

ifneq ($(wildcard $(TEST_DIR)),)
ifndef CT_SUITES
CT_SUITES := $(sort $(subst _SUITE.erl,,$(notdir $(call core_find,$(TEST_DIR)/,*_SUITE.erl))))
endif
endif
CT_SUITES ?=
CT_LOGS_DIR ?= $(CURDIR)/logs

# Core targets.

tests:: ct

ifndef KEEP_LOGS
distclean:: distclean-ct
endif

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
	-pa $(CURDIR)/ebin $(TEST_DIR) \
	-dir $(TEST_DIR) \
	-logdir $(CT_LOGS_DIR)

ifeq ($(CT_SUITES),)
ct: $(if $(IS_APP)$(ROOT_DIR),,apps-ct)
else
# We do not run tests if we are in an apps/* with no test directory.
ifneq ($(IS_APP)$(wildcard $(TEST_DIR)),1)
ct: test-build $(if $(IS_APP)$(ROOT_DIR),,apps-ct)
	$(verbose) mkdir -p $(CT_LOGS_DIR)
	$(gen_verbose) $(CT_RUN) -sname ct_$(PROJECT) -suite $(addsuffix _SUITE,$(CT_SUITES)) $(CT_OPTS)
endif
endif

ifneq ($(ALL_APPS_DIRS),)
define ct_app_target
apps-ct-$1: test-build
	$$(MAKE) -C $1 ct IS_APP=1
endef

$(foreach app,$(ALL_APPS_DIRS),$(eval $(call ct_app_target,$(app))))

apps-ct: $(addprefix apps-ct-,$(ALL_APPS_DIRS))
endif

ifdef t
ifeq (,$(findstring :,$t))
CT_EXTRA = -group $t
else
t_words = $(subst :, ,$t)
CT_EXTRA = -group $(firstword $(t_words)) -case $(lastword $(t_words))
endif
else
ifdef c
CT_EXTRA = -case $c
else
CT_EXTRA =
endif
endif

define ct_suite_target
ct-$1: test-build
	$$(verbose) mkdir -p $$(CT_LOGS_DIR)
	$$(gen_verbose_esc) $$(CT_RUN) -sname ct_$$(PROJECT) -suite $$(addsuffix _SUITE,$1) $$(CT_EXTRA) $$(CT_OPTS)
endef

$(foreach test,$(CT_SUITES),$(eval $(call ct_suite_target,$(test))))

distclean-ct:
	$(gen_verbose) rm -rf $(CT_LOGS_DIR)

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: plt distclean-plt dialyze

# Configuration.

DIALYZER_PLT ?= $(CURDIR)/.$(PROJECT).plt
export DIALYZER_PLT

PLT_APPS ?=
DIALYZER_DIRS ?= --src -r $(wildcard src) $(ALL_APPS_DIRS)
DIALYZER_OPTS ?= -Werror_handling -Wunmatched_returns # -Wunderspecs
DIALYZER_PLT_OPTS ?=

# Core targets.

check:: dialyze

distclean:: distclean-plt

help::
	$(verbose) printf "%s\n" "" \
		"Dialyzer targets:" \
		"  plt         Build a PLT file for this project" \
		"  dialyze     Analyze the project using Dialyzer"

# Plugin-specific targets.

define filter_opts.erl
	Opts = init:get_plain_arguments(),
	{Filtered, _} = lists:foldl(fun
		(O,                         {Os, true}) -> {[O|Os], false};
		(O = "-D",                  {Os, _})    -> {[O|Os], true};
		(O = [\\$$-, \\$$D, _ | _], {Os, _})    -> {[O|Os], false};
		(O = "-I",                  {Os, _})    -> {[O|Os], true};
		(O = [\\$$-, \\$$I, _ | _], {Os, _})    -> {[O|Os], false};
		(O = "-pa",                 {Os, _})    -> {[O|Os], true};
		(_,                         Acc)        -> Acc
	end, {[], false}, Opts),
	io:format("~s~n", [string:join(lists:reverse(Filtered), " ")]),
	halt().
endef

# DIALYZER_PLT is a variable understood directly by Dialyzer.
#
# We append the path to erts at the end of the PLT. This works
# because the PLT file is in the external term format and the
# function binary_to_term/1 ignores any trailing data.
$(DIALYZER_PLT): deps app
	$(eval DEPS_LOG := $(shell test -f $(ERLANG_MK_TMP)/deps.log && \
		while read p; do test -d $$p/ebin && echo $$p/ebin; done <$(ERLANG_MK_TMP)/deps.log))
	$(verbose) dialyzer --build_plt $(DIALYZER_PLT_OPTS) --apps \
		erts kernel stdlib $(PLT_APPS) $(OTP_DEPS) $(LOCAL_DEPS) $(DEPS_LOG) || test $$? -eq 2
	$(verbose) $(ERL) -eval 'io:format("~n~s~n", [code:lib_dir(erts)]), halt().' >> $@

plt: $(DIALYZER_PLT)

distclean-plt:
	$(gen_verbose) rm -f $(DIALYZER_PLT)

ifneq ($(wildcard $(DIALYZER_PLT)),)
dialyze: $(if $(filter --src,$(DIALYZER_DIRS)),,deps app)
	$(verbose) if ! tail -n1 $(DIALYZER_PLT) | \
		grep -q "^`$(ERL) -eval 'io:format("~s", [code:lib_dir(erts)]), halt().'`$$"; then \
		rm $(DIALYZER_PLT); \
		$(MAKE) plt; \
	fi
else
dialyze: $(DIALYZER_PLT)
endif
	$(verbose) dialyzer `$(ERL) \
		-eval "$(subst $(newline),,$(call escape_dquotes,$(call filter_opts.erl)))" \
		-extra $(ERLC_OPTS)` $(DIALYZER_DIRS) $(DIALYZER_OPTS) $(if $(wildcard ebin/),-pa ebin/)

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-edoc edoc

# Configuration.

EDOC_OPTS ?=
EDOC_SRC_DIRS ?=
EDOC_OUTPUT ?= doc

define edoc.erl
	SrcPaths = lists:foldl(fun(P, Acc) ->
		filelib:wildcard(atom_to_list(P) ++ "/{src,c_src}")
		++ lists:filter(fun(D) ->
			filelib:is_dir(D)
		end, filelib:wildcard(atom_to_list(P) ++ "/{src,c_src}/**"))
		++ Acc
	end, [], [$(call comma_list,$(patsubst %,'%',$(call core_native_path,$(EDOC_SRC_DIRS))))]),
	DefaultOpts = [{dir, "$(EDOC_OUTPUT)"}, {source_path, SrcPaths}, {subpackages, false}],
	edoc:application($(1), ".", [$(2)] ++ DefaultOpts),
	halt(0).
endef

# Core targets.

ifneq ($(strip $(EDOC_SRC_DIRS)$(wildcard doc/overview.edoc)),)
docs:: edoc
endif

distclean:: distclean-edoc

# Plugin-specific targets.

edoc: distclean-edoc doc-deps
	$(gen_verbose) $(call erlang,$(call edoc.erl,$(PROJECT),$(EDOC_OPTS)))

distclean-edoc:
	$(gen_verbose) rm -f $(EDOC_OUTPUT)/*.css $(EDOC_OUTPUT)/*.html $(EDOC_OUTPUT)/*.png $(EDOC_OUTPUT)/edoc-info

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Configuration.

DTL_FULL_PATH ?=
DTL_PATH ?= templates/
DTL_PREFIX ?=
DTL_SUFFIX ?= _dtl
DTL_OPTS ?=

# Verbosity.

dtl_verbose_0 = @echo " DTL   " $(filter %.dtl,$(?F));
dtl_verbose = $(dtl_verbose_$(V))

# Core targets.

DTL_PATH := $(abspath $(DTL_PATH))
DTL_FILES := $(sort $(call core_find,$(DTL_PATH),*.dtl))

ifneq ($(DTL_FILES),)

DTL_NAMES   = $(addprefix $(DTL_PREFIX),$(addsuffix $(DTL_SUFFIX),$(DTL_FILES:$(DTL_PATH)/%.dtl=%)))
DTL_MODULES = $(if $(DTL_FULL_PATH),$(subst /,_,$(DTL_NAMES)),$(notdir $(DTL_NAMES)))
BEAM_FILES += $(addsuffix .beam,$(addprefix ebin/,$(DTL_MODULES)))

ifneq ($(words $(DTL_FILES)),0)
# Rebuild templates when the Makefile changes.
$(ERLANG_MK_TMP)/last-makefile-change-erlydtl: $(MAKEFILE_LIST) | $(ERLANG_MK_TMP)
	$(verbose) if test -f $@; then \
		touch $(DTL_FILES); \
	fi
	$(verbose) touch $@

ebin/$(PROJECT).app:: $(ERLANG_MK_TMP)/last-makefile-change-erlydtl
endif

define erlydtl_compile.erl
	[begin
		Module0 = case "$(strip $(DTL_FULL_PATH))" of
			"" ->
				filename:basename(F, ".dtl");
			_ ->
				"$(call core_native_path,$(DTL_PATH))/" ++ F2 = filename:rootname(F, ".dtl"),
				re:replace(F2, "/",  "_",  [{return, list}, global])
		end,
		Module = list_to_atom("$(DTL_PREFIX)" ++ string:to_lower(Module0) ++ "$(DTL_SUFFIX)"),
		case erlydtl:compile(F, Module, [$(DTL_OPTS)] ++ [{out_dir, "ebin/"}, return_errors]) of
			ok -> ok;
			{ok, _} -> ok
		end
	end || F <- string:tokens("$(1)", " ")],
	halt().
endef

ebin/$(PROJECT).app:: $(DTL_FILES) | ebin/
	$(if $(strip $?),\
		$(dtl_verbose) $(call erlang,$(call erlydtl_compile.erl,$(call core_native_path,$?)),\
			-pa ebin/))

endif

# Copyright (c) 2016, Loïc Hoguin <essen@ninenines.eu>
# Copyright (c) 2014, Dave Cottlehuber <dch@skunkwerks.at>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-escript escript escript-zip

# Configuration.

ESCRIPT_NAME ?= $(PROJECT)
ESCRIPT_FILE ?= $(ESCRIPT_NAME)

ESCRIPT_SHEBANG ?= /usr/bin/env escript
ESCRIPT_COMMENT ?= This is an -*- erlang -*- file
ESCRIPT_EMU_ARGS ?= -escript main $(ESCRIPT_NAME)

ESCRIPT_ZIP ?= 7z a -tzip -mx=9 -mtc=off $(if $(filter-out 0,$(V)),,> /dev/null)
ESCRIPT_ZIP_FILE ?= $(ERLANG_MK_TMP)/escript.zip

# Core targets.

distclean:: distclean-escript

help::
	$(verbose) printf "%s\n" "" \
		"Escript targets:" \
		"  escript     Build an executable escript archive" \

# Plugin-specific targets.

ALL_ESCRIPT_DEPS_DIRS = $(LOCAL_DEPS_DIRS) $(addprefix $(DEPS_DIR)/,$(foreach dep,$(filter-out $(IGNORE_DEPS),$(DEPS)),$(call query_name,$(dep))))

ESCRIPT_RUNTIME_DEPS_FILE ?= $(ERLANG_MK_TMP)/escript-deps.log

escript-list-runtime-deps:
ifeq ($(IS_DEP),)
	$(verbose) rm -f $(ESCRIPT_RUNTIME_DEPS_FILE)
endif
	$(verbose) touch $(ESCRIPT_RUNTIME_DEPS_FILE)
	$(verbose) set -e; for dep in $(ALL_ESCRIPT_DEPS_DIRS) ; do \
		if ! grep -qs ^$$dep$$ $(ESCRIPT_RUNTIME_DEPS_FILE); then \
			echo $$dep >> $(ESCRIPT_RUNTIME_DEPS_FILE); \
			if grep -qs -E "^[[:blank:]]*include[[:blank:]]+(erlang\.mk|.*/erlang\.mk|.*ERLANG_MK_FILENAME.*)$$" \
			 $$dep/GNUmakefile $$dep/makefile $$dep/Makefile; then \
				$(MAKE) -C $$dep escript-list-runtime-deps \
				 IS_DEP=1 \
				 ESCRIPT_RUNTIME_DEPS_FILE=$(ESCRIPT_RUNTIME_DEPS_FILE); \
			fi \
		fi \
	done
ifeq ($(IS_DEP),)
	$(verbose) sort < $(ESCRIPT_RUNTIME_DEPS_FILE) | uniq > $(ESCRIPT_RUNTIME_DEPS_FILE).sorted
	$(verbose) mv $(ESCRIPT_RUNTIME_DEPS_FILE).sorted $(ESCRIPT_RUNTIME_DEPS_FILE)
endif

escript-prepare: deps app
	$(MAKE) escript-list-runtime-deps

escript-zip:: escript-prepare
	$(verbose) mkdir -p $(dir $(abspath $(ESCRIPT_ZIP_FILE)))
	$(verbose) rm -f $(abspath $(ESCRIPT_ZIP_FILE))
	$(gen_verbose) cd .. && $(ESCRIPT_ZIP) $(abspath $(ESCRIPT_ZIP_FILE)) $(notdir $(CURDIR))/ebin/*
ifneq ($(DEPS),)
	$(verbose) cd $(DEPS_DIR) && $(ESCRIPT_ZIP) $(abspath $(ESCRIPT_ZIP_FILE)) \
		$(subst $(DEPS_DIR)/,,$(addsuffix /*,$(wildcard \
		$(addsuffix /ebin,$(shell cat $(ESCRIPT_RUNTIME_DEPS_FILE))))))
endif

# @todo Only generate the zip file if there were changes.
escript:: escript-zip
	$(gen_verbose) printf "%s\n" \
		"#!$(ESCRIPT_SHEBANG)" \
		"%% $(ESCRIPT_COMMENT)" \
		"%%! $(ESCRIPT_EMU_ARGS)" > $(ESCRIPT_FILE)
	$(verbose) cat $(abspath $(ESCRIPT_ZIP_FILE)) >> $(ESCRIPT_FILE)
	$(verbose) chmod +x $(ESCRIPT_FILE)

distclean-escript:
	$(gen_verbose) rm -f $(ESCRIPT_FILE) $(abspath $(ESCRIPT_ZIP_FILE))

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# Copyright (c) 2014, Enrique Fernandez <enrique.fernandez@erlang-solutions.com>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

.PHONY: eunit apps-eunit

# Eunit can be disabled by setting this to any other value.
EUNIT ?= system

ifeq ($(EUNIT),system)

# Configuration

EUNIT_OPTS ?=
EUNIT_ERL_OPTS ?=
EUNIT_TEST_SPEC ?= $1

# Core targets.

tests:: eunit

help::
	$(verbose) printf "%s\n" "" \
		"EUnit targets:" \
		"  eunit       Run all the EUnit tests for this project"

# Plugin-specific targets.

define eunit.erl
	$(call cover.erl)
	CoverSetup(),
	case eunit:test($(call EUNIT_TEST_SPEC,$1), [$(EUNIT_OPTS)]) of
		ok -> ok;
		error -> halt(2)
	end,
	CoverExport("$(call core_native_path,$(COVER_DATA_DIR))/eunit.coverdata"),
	halt()
endef

EUNIT_ERL_OPTS += -pa $(TEST_DIR) $(CURDIR)/ebin

ifdef t
ifeq (,$(findstring :,$(t)))
eunit: test-build cover-data-dir
	$(gen_verbose) $(call erlang,$(call eunit.erl,['$(t)']),$(EUNIT_ERL_OPTS))
else
eunit: test-build cover-data-dir
	$(gen_verbose) $(call erlang,$(call eunit.erl,fun $(t)/0),$(EUNIT_ERL_OPTS))
endif
else
EUNIT_EBIN_MODS = $(notdir $(basename $(ERL_FILES) $(BEAM_FILES)))
EUNIT_TEST_MODS = $(notdir $(basename $(call core_find,$(TEST_DIR)/,*.erl)))

EUNIT_MODS = $(foreach mod,$(EUNIT_EBIN_MODS) $(filter-out \
	$(patsubst %,%_tests,$(EUNIT_EBIN_MODS)),$(EUNIT_TEST_MODS)),'$(mod)')

eunit: test-build $(if $(IS_APP)$(ROOT_DIR),,apps-eunit) cover-data-dir
ifneq ($(wildcard src/ $(TEST_DIR)),)
	$(gen_verbose) $(call erlang,$(call eunit.erl,[$(call comma_list,$(EUNIT_MODS))]),$(EUNIT_ERL_OPTS))
endif

ifneq ($(ALL_APPS_DIRS),)
apps-eunit: test-build
	$(verbose) eunit_retcode=0 ; for app in $(ALL_APPS_DIRS); do $(MAKE) -C $$app eunit IS_APP=1; \
		[ $$? -ne 0 ] && eunit_retcode=1 ; done ; \
		exit $$eunit_retcode
endif
endif

endif

# Copyright (c) 2020, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

define hex_user_create.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	case hex_api_user:create(Config, <<"$(strip $1)">>, <<"$(strip $2)">>, <<"$(strip $3)">>) of
		{ok, {201, _, #{<<"email">> := Email, <<"url">> := URL, <<"username">> := Username}}} ->
			io:format("User ~s (~s) created at ~s~n"
				"Please check your inbox for a confirmation email.~n"
				"You must confirm before you are allowed to publish packages.~n",
				[Username, Email, URL]),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(80)
	end
endef

# The $(info ) call inserts a new line after the password prompt.
hex-user-create: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_USERNAME),,$(eval HEX_USERNAME := $(shell read -p "Username: " username; echo $$username)))
	$(if $(HEX_PASSWORD),,$(eval HEX_PASSWORD := $(shell stty -echo; read -p "Password: " password; stty echo; echo $$password) $(info )))
	$(if $(HEX_EMAIL),,$(eval HEX_EMAIL := $(shell read -p "Email: " email; echo $$email)))
	$(gen_verbose) $(call erlang,$(call hex_user_create.erl,$(HEX_USERNAME),$(HEX_PASSWORD),$(HEX_EMAIL)))

define hex_key_add.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => iolist_to_binary([<<"Basic ">>, base64:encode(<<"$(strip $1):$(strip $2)">>)])},
	Permissions = [
		case string:split(P, <<":">>) of
			[D] -> #{domain => D};
			[D, R] -> #{domain => D, resource => R}
		end
	|| P <- string:split(<<"$(strip $4)">>, <<",">>, all)],
	case hex_api_key:add(ConfigF, <<"$(strip $3)">>, Permissions) of
		{ok, {201, _, #{<<"secret">> := Secret}}} ->
			io:format("Key ~s created for user ~s~nSecret: ~s~n"
				"Please store the secret in a secure location, such as a password store.~n"
				"The secret will be requested for most Hex-related operations.~n",
				[<<"$(strip $3)">>, <<"$(strip $1)">>, Secret]),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(81)
	end
endef

hex-key-add: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_USERNAME),,$(eval HEX_USERNAME := $(shell read -p "Username: " username; echo $$username)))
	$(if $(HEX_PASSWORD),,$(eval HEX_PASSWORD := $(shell stty -echo; read -p "Password: " password; stty echo; echo $$password) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_key_add.erl,$(HEX_USERNAME),$(HEX_PASSWORD),\
		$(if $(name),$(name),$(shell hostname)-erlang-mk),\
		$(if $(perm),$(perm),api)))

HEX_TARBALL_EXTRA_METADATA ?=

# @todo Check that we can += files
HEX_TARBALL_FILES ?= \
	$(wildcard early-plugins.mk) \
	$(wildcard ebin/$(PROJECT).app) \
	$(wildcard ebin/$(PROJECT).appup) \
	$(wildcard $(notdir $(ERLANG_MK_FILENAME))) \
	$(sort $(call core_find,include/,*.hrl)) \
	$(wildcard LICENSE*) \
	$(wildcard Makefile) \
	$(wildcard plugins.mk) \
	$(sort $(call core_find,priv/,*)) \
	$(wildcard README*) \
	$(wildcard rebar.config) \
	$(sort $(if $(LEGACY),$(filter-out src/$(PROJECT).app.src,$(call core_find,src/,*)),$(call core_find,src/,*)))

HEX_TARBALL_OUTPUT_FILE ?= $(ERLANG_MK_TMP)/$(PROJECT).tar

# @todo Need to check for rebar.config and/or the absence of DEPS to know
# whether a project will work with Rebar.
#
# @todo contributors licenses links in HEX_TARBALL_EXTRA_METADATA

# In order to build the requirements metadata we look into DEPS.
# We do not require that the project use Hex dependencies, however
# Hex.pm does require that the package name and version numbers
# correspond to a real Hex package.
define hex_tarball_create.erl
	Files0 = [$(call comma_list,$(patsubst %,"%",$(HEX_TARBALL_FILES)))],
	Requirements0 = #{
		$(foreach d,$(DEPS),
			<<"$(if $(subst hex,,$(call query_fetch_method,$d)),$d,$(if $(word 3,$(dep_$d)),$(word 3,$(dep_$d)),$d))">> => #{
				<<"app">> => <<"$d">>,
				<<"optional">> => false,
				<<"requirement">> => <<"$(if $(hex_req_$d),$(strip $(hex_req_$d)),$(call query_version,$d))">>
			},)
		$(if $(DEPS),dummy => dummy)
	},
	Requirements = maps:remove(dummy, Requirements0),
	Metadata0 = #{
		app => <<"$(strip $(PROJECT))">>,
		build_tools => [<<"make">>, <<"rebar3">>],
		description => <<"$(strip $(PROJECT_DESCRIPTION))">>,
		files => [unicode:characters_to_binary(F) || F <- Files0],
		name => <<"$(strip $(PROJECT))">>,
		requirements => Requirements,
		version => <<"$(strip $(PROJECT_VERSION))">>
	},
	Metadata = Metadata0$(HEX_TARBALL_EXTRA_METADATA),
	Files = [case file:read_file(F) of
		{ok, Bin} ->
			{F, Bin};
		{error, Reason} ->
			io:format("Error trying to open file ~0p: ~0p~n", [F, Reason]),
			halt(82)
	end || F <- Files0],
	case hex_tarball:create(Metadata, Files) of
		{ok, #{tarball := Tarball}} ->
			ok = file:write_file("$(strip $(HEX_TARBALL_OUTPUT_FILE))", Tarball),
			halt(0);
		{error, Reason} ->
			io:format("Error ~0p~n", [Reason]),
			halt(83)
	end
endef

hex_tar_verbose_0 = @echo " TAR    $(notdir $(ERLANG_MK_TMP))/$(@F)";
hex_tar_verbose_2 = set -x;
hex_tar_verbose = $(hex_tar_verbose_$(V))

$(HEX_TARBALL_OUTPUT_FILE): $(DEPS_DIR)/hex_core/ebin/dep_built app
	$(hex_tar_verbose) $(call erlang,$(call hex_tarball_create.erl))

hex-tarball-create: $(HEX_TARBALL_OUTPUT_FILE)

define hex_release_publish_summary.erl
	{ok, Tarball} = erl_tar:open("$(strip $(HEX_TARBALL_OUTPUT_FILE))", [read]),
	ok = erl_tar:extract(Tarball, [{cwd, "$(ERLANG_MK_TMP)"}, {files, ["metadata.config"]}]),
	{ok, Metadata} = file:consult("$(ERLANG_MK_TMP)/metadata.config"),
	#{
		<<"name">> := Name,
		<<"version">> := Version,
		<<"files">> := Files,
		<<"requirements">> := Deps
	} = maps:from_list(Metadata),
	io:format("Publishing ~s ~s~n  Dependencies:~n", [Name, Version]),
	case Deps of
		[] ->
			io:format("    (none)~n");
		_ ->
			[begin
				#{<<"app">> := DA, <<"requirement">> := DR} = maps:from_list(D),
				io:format("    ~s ~s~n", [DA, DR])
			end || {_, D} <- Deps]
	end,
	io:format("  Included files:~n"),
	[io:format("    ~s~n", [F]) || F <- Files],
	io:format("You may also review the contents of the tarball file.~n"
		"Please enter your secret key to proceed.~n"),
	halt(0)
endef

define hex_release_publish.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	{ok, Tarball} = file:read_file("$(strip $(HEX_TARBALL_OUTPUT_FILE))"),
	case hex_api_release:publish(ConfigF, Tarball, [{replace, $2}]) of
		{ok, {200, _, #{}}} ->
			io:format("Release replaced~n"),
			halt(0);
		{ok, {201, _, #{}}} ->
			io:format("Release published~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(84)
	end
endef

hex-release-tarball: $(DEPS_DIR)/hex_core/ebin/dep_built $(HEX_TARBALL_OUTPUT_FILE)
	$(verbose) $(call erlang,$(call hex_release_publish_summary.erl))

hex-release-publish: $(DEPS_DIR)/hex_core/ebin/dep_built hex-release-tarball
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_release_publish.erl,$(HEX_SECRET),false))

hex-release-replace: $(DEPS_DIR)/hex_core/ebin/dep_built hex-release-tarball
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_release_publish.erl,$(HEX_SECRET),true))

define hex_release_delete.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	case hex_api_release:delete(ConfigF, <<"$(strip $(PROJECT))">>, <<"$(strip $(PROJECT_VERSION))">>) of
		{ok, {204, _, _}} ->
			io:format("Release $(strip $(PROJECT_VERSION)) deleted~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(85)
	end
endef

hex-release-delete: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_release_delete.erl,$(HEX_SECRET)))

define hex_release_retire.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	Params = #{<<"reason">> => <<"$(strip $3)">>, <<"message">> => <<"$(strip $4)">>},
	case hex_api_release:retire(ConfigF, <<"$(strip $(PROJECT))">>, <<"$(strip $2)">>, Params) of
		{ok, {204, _, _}} ->
			io:format("Release $(strip $2) has been retired~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(86)
	end
endef

hex-release-retire: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_release_retire.erl,$(HEX_SECRET),\
		$(if $(HEX_VERSION),$(HEX_VERSION),$(PROJECT_VERSION)),\
		$(if $(HEX_REASON),$(HEX_REASON),invalid),\
		$(HEX_MESSAGE)))

define hex_release_unretire.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	case hex_api_release:unretire(ConfigF, <<"$(strip $(PROJECT))">>, <<"$(strip $2)">>) of
		{ok, {204, _, _}} ->
			io:format("Release $(strip $2) is not retired anymore~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(87)
	end
endef

hex-release-unretire: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_release_unretire.erl,$(HEX_SECRET),\
		$(if $(HEX_VERSION),$(HEX_VERSION),$(PROJECT_VERSION))))

HEX_DOCS_DOC_DIR ?= doc/
HEX_DOCS_TARBALL_FILES ?= $(sort $(call core_find,$(HEX_DOCS_DOC_DIR),*))
HEX_DOCS_TARBALL_OUTPUT_FILE ?= $(ERLANG_MK_TMP)/$(PROJECT)-docs.tar.gz

$(HEX_DOCS_TARBALL_OUTPUT_FILE): $(DEPS_DIR)/hex_core/ebin/dep_built app docs
	$(hex_tar_verbose) tar czf $(HEX_DOCS_TARBALL_OUTPUT_FILE) -C $(HEX_DOCS_DOC_DIR) \
		$(HEX_DOCS_TARBALL_FILES:$(HEX_DOCS_DOC_DIR)%=%)

hex-docs-tarball-create: $(HEX_DOCS_TARBALL_OUTPUT_FILE)

define hex_docs_publish.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	{ok, Tarball} = file:read_file("$(strip $(HEX_DOCS_TARBALL_OUTPUT_FILE))"),
	case hex_api:post(ConfigF,
			["packages", "$(strip $(PROJECT))", "releases", "$(strip $(PROJECT_VERSION))", "docs"],
			{"application/octet-stream", Tarball}) of
		{ok, {Status, _, _}} when Status >= 200, Status < 300 ->
			io:format("Docs published~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(88)
	end
endef

hex-docs-publish: $(DEPS_DIR)/hex_core/ebin/dep_built hex-docs-tarball-create
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_docs_publish.erl,$(HEX_SECRET)))

define hex_docs_delete.erl
	{ok, _} = application:ensure_all_started(ssl),
	{ok, _} = application:ensure_all_started(inets),
	Config = $(hex_config.erl),
	ConfigF = Config#{api_key => <<"$(strip $1)">>},
	case hex_api:delete(ConfigF,
			["packages", "$(strip $(PROJECT))", "releases", "$(strip $2)", "docs"]) of
		{ok, {Status, _, _}} when Status >= 200, Status < 300 ->
			io:format("Docs removed~n"),
			halt(0);
		{ok, {Status, _, Errors}} ->
			io:format("Error ~b: ~0p~n", [Status, Errors]),
			halt(89)
	end
endef

hex-docs-delete: $(DEPS_DIR)/hex_core/ebin/dep_built
	$(if $(HEX_SECRET),,$(eval HEX_SECRET := $(shell stty -echo; read -p "Secret: " secret; stty echo; echo $$secret) $(info )))
	$(gen_verbose) $(call erlang,$(call hex_docs_delete.erl,$(HEX_SECRET),\
		$(if $(HEX_VERSION),$(HEX_VERSION),$(PROJECT_VERSION))))

# Copyright (c) 2015-2017, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(filter proper,$(DEPS) $(TEST_DEPS)),proper)
.PHONY: proper

# Targets.

tests:: proper

define proper_check.erl
	$(call cover.erl)
	code:add_pathsa([
		"$(call core_native_path,$(CURDIR)/ebin)",
		"$(call core_native_path,$(DEPS_DIR)/*/ebin)",
		"$(call core_native_path,$(TEST_DIR))"]),
	Module = fun(M) ->
		[true] =:= lists:usort([
			case atom_to_list(F) of
				"prop_" ++ _ ->
					io:format("Testing ~p:~p/0~n", [M, F]),
					proper:quickcheck(M:F(), nocolors);
				_ ->
					true
			end
		|| {F, 0} <- M:module_info(exports)])
	end,
	try begin
		CoverSetup(),
		Res = case $(1) of
			all -> [true] =:= lists:usort([Module(M) || M <- [$(call comma_list,$(3))]]);
			module -> Module($(2));
			function -> proper:quickcheck($(2), nocolors)
		end,
		CoverExport("$(COVER_DATA_DIR)/proper.coverdata"),
		Res
	end of
		true -> halt(0);
		_ -> halt(1)
	catch error:undef$(if $V,:Stacktrace) ->
		io:format("Undefined property or module?~n$(if $V,~p~n)", [$(if $V,Stacktrace)]),
		halt(0)
	end.
endef

ifdef t
ifeq (,$(findstring :,$(t)))
proper: test-build cover-data-dir
	$(verbose) $(call erlang,$(call proper_check.erl,module,$(t)))
else
proper: test-build cover-data-dir
	$(verbose) echo Testing $(t)/0
	$(verbose) $(call erlang,$(call proper_check.erl,function,$(t)()))
endif
else
proper: test-build cover-data-dir
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename \
		$(wildcard ebin/*.beam) $(call core_find,$(TEST_DIR)/,*.beam))))))
	$(gen_verbose) $(call erlang,$(call proper_check.erl,all,undefined,$(MODULES)))
endif
endif

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Verbosity.

proto_verbose_0 = @echo " PROTO " $(filter %.proto,$(?F));
proto_verbose = $(proto_verbose_$(V))

# Core targets.

ifneq ($(wildcard src/),)
ifneq ($(filter gpb protobuffs,$(BUILD_DEPS) $(DEPS)),)
PROTO_FILES := $(filter %.proto,$(ALL_SRC_FILES))
ERL_FILES += $(addprefix src/,$(patsubst %.proto,%_pb.erl,$(notdir $(PROTO_FILES))))

ifeq ($(PROTO_FILES),)
$(ERLANG_MK_TMP)/last-makefile-change-protobuffs:
	$(verbose) :
else
# Rebuild proto files when the Makefile changes.
# We exclude $(PROJECT).d to avoid a circular dependency.
$(ERLANG_MK_TMP)/last-makefile-change-protobuffs: $(filter-out $(PROJECT).d,$(MAKEFILE_LIST)) | $(ERLANG_MK_TMP)
	$(verbose) if test -f $@; then \
		touch $(PROTO_FILES); \
	fi
	$(verbose) touch $@

$(PROJECT).d:: $(ERLANG_MK_TMP)/last-makefile-change-protobuffs
endif

ifeq ($(filter gpb,$(BUILD_DEPS) $(DEPS)),)
define compile_proto.erl
	[begin
		protobuffs_compile:generate_source(F, [
			{output_include_dir, "./include"},
			{output_src_dir, "./src"}])
	end || F <- string:tokens("$1", " ")],
	halt().
endef
else
define compile_proto.erl
	[begin
		gpb_compile:file(F, [
			$(foreach i,$(sort $(dir $(PROTO_FILES))),{i$(comma) "$i"}$(comma))
			{include_as_lib, true},
			{module_name_suffix, "_pb"},
			{o_hrl, "./include"},
			{o_erl, "./src"},
			{use_packages, true}
		])
	end || F <- string:tokens("$1", " ")],
	halt().
endef
endif

ifneq ($(PROTO_FILES),)
$(PROJECT).d:: $(PROTO_FILES)
	$(verbose) mkdir -p ebin/ include/
	$(if $(strip $?),$(proto_verbose) $(call erlang,$(call compile_proto.erl,$?)))
endif
endif
endif

# Copyright (c) 2013-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(filter relx,$(BUILD_DEPS) $(DEPS) $(REL_DEPS)),relx)
.PHONY: relx-rel relx-relup distclean-relx-rel run

# Configuration.

RELX_CONFIG ?= $(CURDIR)/relx.config
RELX_CONFIG_SCRIPT ?= $(CURDIR)/relx.config.script

RELX_OUTPUT_DIR ?= _rel
RELX_REL_EXT ?=
RELX_TAR ?= 1

ifdef SFX
	RELX_TAR = 1
endif

# Core targets.

ifeq ($(IS_DEP),)
ifneq ($(wildcard $(RELX_CONFIG))$(wildcard $(RELX_CONFIG_SCRIPT)),)
rel:: relx-rel

relup:: relx-relup
endif
endif

distclean:: distclean-relx-rel

# Plugin-specific targets.

define relx_get_config.erl
	(fun() ->
		Config0 =
			case file:consult("$(call core_native_path,$(RELX_CONFIG))") of
				{ok, Terms} ->
					Terms;
				{error, _} ->
					[]
			end,
		case filelib:is_file("$(call core_native_path,$(RELX_CONFIG_SCRIPT))") of
			true ->
				Bindings = erl_eval:add_binding('CONFIG', Config0, erl_eval:new_bindings()),
				{ok, Config1} = file:script("$(call core_native_path,$(RELX_CONFIG_SCRIPT))", Bindings),
				Config1;
			false ->
				Config0
		end
	end)()
endef

define relx_release.erl
	Config = $(call relx_get_config.erl),
	{release, {Name, Vsn0}, _} = lists:keyfind(release, 1, Config),
	Vsn = case Vsn0 of
		{cmd, Cmd} -> os:cmd(Cmd);
		semver -> "";
		{semver, _} -> "";
		{git, short} -> string:trim(os:cmd("git rev-parse --short HEAD"), both, "\n");
		{git, long} -> string:trim(os:cmd("git rev-parse HEAD"), both, "\n");
		VsnStr -> Vsn0
	end,
	{ok, _} = relx:build_release(#{name => Name, vsn => Vsn}, Config ++ [{output_dir, "$(RELX_OUTPUT_DIR)"}]),
	halt(0).
endef

define relx_tar.erl
	Config = $(call relx_get_config.erl),
	{release, {Name, Vsn0}, _} = lists:keyfind(release, 1, Config),
	Vsn = case Vsn0 of
		{cmd, Cmd} -> os:cmd(Cmd);
		semver -> "";
		{semver, _} -> "";
		{git, short} -> string:trim(os:cmd("git rev-parse --short HEAD"), both, "\n");
		{git, long} -> string:trim(os:cmd("git rev-parse HEAD"), both, "\n");
		VsnStr -> Vsn0
	end,
	{ok, _} = relx:build_tar(#{name => Name, vsn => Vsn}, Config ++ [{output_dir, "$(RELX_OUTPUT_DIR)"}]),
	halt(0).
endef

define relx_relup.erl
	Config = $(call relx_get_config.erl),
	{release, {Name, Vsn0}, _} = lists:keyfind(release, 1, Config),
	Vsn = case Vsn0 of
		{cmd, Cmd} -> os:cmd(Cmd);
		semver -> "";
		{semver, _} -> "";
		{git, short} -> string:trim(os:cmd("git rev-parse --short HEAD"), both, "\n");
		{git, long} -> string:trim(os:cmd("git rev-parse HEAD"), both, "\n");
		VsnStr -> Vsn0
	end,
	{ok, _} = relx:build_relup(Name, Vsn, undefined, Config ++ [{output_dir, "$(RELX_OUTPUT_DIR)"}]),
	halt(0).
endef

relx-rel: rel-deps app
	$(call erlang,$(call relx_release.erl),-pa ebin/)
	$(verbose) $(MAKE) relx-post-rel
	$(if $(filter-out 0,$(RELX_TAR)),$(call erlang,$(call relx_tar.erl),-pa ebin/))

relx-relup: rel-deps app
	$(call erlang,$(call relx_release.erl),-pa ebin/)
	$(MAKE) relx-post-rel
	$(call erlang,$(call relx_relup.erl),-pa ebin/)
	$(if $(filter-out 0,$(RELX_TAR)),$(call erlang,$(call relx_tar.erl),-pa ebin/))

distclean-relx-rel:
	$(gen_verbose) rm -rf $(RELX_OUTPUT_DIR)

# Default hooks.
relx-post-rel::
	$(verbose) :

# Run target.

ifeq ($(wildcard $(RELX_CONFIG))$(wildcard $(RELX_CONFIG_SCRIPT)),)
run::
else

define get_relx_release.erl
	Config = $(call relx_get_config.erl),
	{release, {Name, Vsn0}, _} = lists:keyfind(release, 1, Config),
	Vsn = case Vsn0 of
		{cmd, Cmd} -> os:cmd(Cmd);
		semver -> "";
		{semver, _} -> "";
		{git, short} -> string:trim(os:cmd("git rev-parse --short HEAD"), both, "\n");
		{git, long} -> string:trim(os:cmd("git rev-parse HEAD"), both, "\n");
		VsnStr -> Vsn0
	end,
	Extended = case lists:keyfind(extended_start_script, 1, Config) of
		{_, true} -> "1";
		_ -> ""
	end,
	io:format("~s ~s ~s", [Name, Vsn, Extended]),
	halt(0).
endef

RELX_REL := $(shell $(call erlang,$(get_relx_release.erl)))
RELX_REL_NAME := $(word 1,$(RELX_REL))
RELX_REL_VSN := $(word 2,$(RELX_REL))
RELX_REL_CMD := $(if $(word 3,$(RELX_REL)),console)

ifeq ($(PLATFORM),msys2)
RELX_REL_EXT := .cmd
endif

run:: RELX_TAR := 0
run:: all
	$(verbose) $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) $(RELX_REL_CMD)

ifdef RELOAD
rel::
	$(verbose) $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) ping
	$(verbose) $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/bin/$(RELX_REL_NAME)$(RELX_REL_EXT) \
		eval "io:format(\"~p~n\", [c:lm()])."
endif

help::
	$(verbose) printf "%s\n" "" \
		"Relx targets:" \
		"  run         Compile the project, build the release and run it"

endif
endif

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# Copyright (c) 2014, M Robert Martin <rob@version2beta.com>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

.PHONY: shell

# Configuration.

SHELL_ERL ?= erl
SHELL_PATHS ?= $(CURDIR)/ebin $(TEST_DIR)
SHELL_OPTS ?=

ALL_SHELL_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(SHELL_DEPS))

# Core targets

help::
	$(verbose) printf "%s\n" "" \
		"Shell targets:" \
		"  shell       Run an erlang shell with SHELL_OPTS or reasonable default"

# Plugin-specific targets.

$(foreach dep,$(SHELL_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
build-shell-deps:
else
build-shell-deps: $(ALL_SHELL_DEPS_DIRS)
	$(verbose) set -e; for dep in $(ALL_SHELL_DEPS_DIRS) ; do \
		if [ -z "$(strip $(FULL))" ] && [ ! -L $$dep ] && [ -f $$dep/ebin/dep_built ]; then \
			:; \
		else \
			$(MAKE) -C $$dep IS_DEP=1; \
			if [ ! -L $$dep ] && [ -d $$dep/ebin ]; then touch $$dep/ebin/dep_built; fi; \
		fi \
	done
endif

shell:: build-shell-deps
	$(gen_verbose) $(SHELL_ERL) -pa $(SHELL_PATHS) $(SHELL_OPTS)

# Copyright 2017, Stanislaw Klekot <dozzie@jarowit.net>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: distclean-sphinx sphinx

# Configuration.

SPHINX_BUILD ?= sphinx-build
SPHINX_SOURCE ?= doc
SPHINX_CONFDIR ?=
SPHINX_FORMATS ?= html
SPHINX_DOCTREES ?= $(ERLANG_MK_TMP)/sphinx.doctrees
SPHINX_OPTS ?=

#sphinx_html_opts =
#sphinx_html_output = html
#sphinx_man_opts =
#sphinx_man_output = man
#sphinx_latex_opts =
#sphinx_latex_output = latex

# Helpers.

sphinx_build_0 = @echo " SPHINX" $1; $(SPHINX_BUILD) -N -q
sphinx_build_1 = $(SPHINX_BUILD) -N
sphinx_build_2 = set -x; $(SPHINX_BUILD)
sphinx_build = $(sphinx_build_$(V))

define sphinx.build
$(call sphinx_build,$1) -b $1 -d $(SPHINX_DOCTREES) $(if $(SPHINX_CONFDIR),-c $(SPHINX_CONFDIR)) $(SPHINX_OPTS) $(sphinx_$1_opts) -- $(SPHINX_SOURCE) $(call sphinx.output,$1)

endef

define sphinx.output
$(if $(sphinx_$1_output),$(sphinx_$1_output),$1)
endef

# Targets.

ifneq ($(wildcard $(if $(SPHINX_CONFDIR),$(SPHINX_CONFDIR),$(SPHINX_SOURCE))/conf.py),)
docs:: sphinx
distclean:: distclean-sphinx
endif

help::
	$(verbose) printf "%s\n" "" \
		"Sphinx targets:" \
		"  sphinx      Generate Sphinx documentation." \
		"" \
		"ReST sources and 'conf.py' file are expected in directory pointed by" \
		"SPHINX_SOURCE ('doc' by default). SPHINX_FORMATS lists formats to build (only" \
		"'html' format is generated by default); target directory can be specified by" \
		'setting sphinx_$${format}_output, for example: sphinx_html_output = output/html' \
		"Additional Sphinx options can be set in SPHINX_OPTS."

# Plugin-specific targets.

sphinx:
	$(foreach F,$(SPHINX_FORMATS),$(call sphinx.build,$F))

distclean-sphinx:
	$(gen_verbose) rm -rf $(filter-out $(SPHINX_SOURCE),$(foreach F,$(SPHINX_FORMATS),$(call sphinx.output,$F)))

# Copyright (c) 2017, Jean-Sébastien Pédron <jean-sebastien@rabbitmq.com>
# This file is contributed to erlang.mk and subject to the terms of the ISC License.

.PHONY: show-ERL_LIBS show-ERLC_OPTS show-TEST_ERLC_OPTS

show-ERL_LIBS:
	@echo $(ERL_LIBS)

show-ERLC_OPTS:
	@$(foreach opt,$(ERLC_OPTS) -pa ebin -I include,echo "$(opt)";)

show-TEST_ERLC_OPTS:
	@$(foreach opt,$(TEST_ERLC_OPTS) -pa ebin -I include,echo "$(opt)";)

# Copyright (c) 2015-2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

ifeq ($(filter triq,$(DEPS) $(TEST_DEPS)),triq)
.PHONY: triq

# Targets.

tests:: triq

define triq_check.erl
	$(call cover.erl)
	code:add_pathsa([
		"$(call core_native_path,$(CURDIR)/ebin)",
		"$(call core_native_path,$(DEPS_DIR)/*/ebin)",
		"$(call core_native_path,$(TEST_DIR))"]),
	try begin
		CoverSetup(),
		Res = case $(1) of
			all -> [true] =:= lists:usort([triq:check(M) || M <- [$(call comma_list,$(3))]]);
			module -> triq:check($(2));
			function -> triq:check($(2))
		end,
		CoverExport("$(COVER_DATA_DIR)/triq.coverdata"),
		Res
	end of
		true -> halt(0);
		_ -> halt(1)
	catch error:undef$(if $V,:Stacktrace) ->
		io:format("Undefined property or module?~n$(if $V,~p~n)", [$(if $V,Stacktrace)]),
		halt(0)
	end.
endef

ifdef t
ifeq (,$(findstring :,$(t)))
triq: test-build cover-data-dir
	$(verbose) $(call erlang,$(call triq_check.erl,module,$(t)))
else
triq: test-build cover-data-dir
	$(verbose) echo Testing $(t)/0
	$(verbose) $(call erlang,$(call triq_check.erl,function,$(t)()))
endif
else
triq: test-build cover-data-dir
	$(eval MODULES := $(patsubst %,'%',$(sort $(notdir $(basename \
		$(wildcard ebin/*.beam) $(call core_find,$(TEST_DIR)/,*.beam))))))
	$(gen_verbose) $(call erlang,$(call triq_check.erl,all,undefined,$(MODULES)))
endif
endif

# Copyright (c) 2022, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: xref

# Configuration.

# We do not use locals_not_used or deprecated_function_calls
# because the compiler will error out by default in those
# cases with Erlang.mk. Deprecated functions may make sense
# in some cases but few libraries define them. We do not
# use exports_not_used by default because it hinders more
# than it helps library projects such as Cowboy. Finally,
# undefined_functions provides little that undefined_function_calls
# doesn't already provide, so it's not enabled by default.
XREF_CHECKS ?= [undefined_function_calls]

# Instead of predefined checks a query can be evaluated
# using the Xref DSL. The $q variable is used in that case.

# The scope is a list of keywords that correspond to
# application directories, being essentially an easy way
# to configure which applications to analyze. With:
#
# - app:  .
# - apps: $(ALL_APPS_DIRS)
# - deps: $(ALL_DEPS_DIRS)
# - otp:  Built-in Erlang/OTP applications.
#
# The default is conservative (app) and will not be
# appropriate for all types of queries (for example
# application_call requires adding all applications
# that might be called or they will not be found).
XREF_SCOPE ?= app # apps deps otp

# If the above is not enough, additional application
# directories can be configured.
XREF_EXTRA_APP_DIRS ?=

# As well as additional non-application directories.
XREF_EXTRA_DIRS ?=

# Erlang.mk supports -ignore_xref([...]) with forms
# {M, F, A} | {F, A} | M, the latter ignoring whole
# modules. Ignores can also be provided project-wide.
XREF_IGNORE ?= []

# All callbacks may be ignored. Erlang.mk will ignore
# them automatically for exports_not_used (unless it
# is explicitly disabled by the user).
XREF_IGNORE_CALLBACKS ?=

# Core targets.

help::
	$(verbose) printf '%s\n' '' \
		'Xref targets:' \
		'  xref         Analyze the project using Xref' \
		'  xref q=QUERY Evaluate an Xref query'

# Plugin-specific targets.

define xref.erl
	{ok, Xref} = xref:start([]),
	Scope = [$(call comma_list,$(XREF_SCOPE))],
	AppDirs0 = [$(call comma_list,$(foreach d,$(XREF_EXTRA_APP_DIRS),"$d"))],
	AppDirs1 = case lists:member(otp, Scope) of
		false -> AppDirs0;
		true ->
			RootDir = code:root_dir(),
			AppDirs0 ++ [filename:dirname(P) || P <- code:get_path(), lists:prefix(RootDir, P)]
	end,
	AppDirs2 = case lists:member(deps, Scope) of
		false -> AppDirs1;
		true -> [$(call comma_list,$(foreach d,$(ALL_DEPS_DIRS),"$d"))] ++ AppDirs1
	end,
	AppDirs3 = case lists:member(apps, Scope) of
		false -> AppDirs2;
		true -> [$(call comma_list,$(foreach d,$(ALL_APPS_DIRS),"$d"))] ++ AppDirs2
	end,
	AppDirs = case lists:member(app, Scope) of
		false -> AppDirs3;
		true -> ["../$(notdir $(CURDIR))"|AppDirs3]
	end,
	[{ok, _} = xref:add_application(Xref, AppDir, [{builtins, true}]) || AppDir <- AppDirs],
	ExtraDirs = [$(call comma_list,$(foreach d,$(XREF_EXTRA_DIRS),"$d"))],
	[{ok, _} = xref:add_directory(Xref, ExtraDir, [{builtins, true}]) || ExtraDir <- ExtraDirs],
	ok = xref:set_library_path(Xref, code:get_path() -- (["ebin", "."] ++ AppDirs ++ ExtraDirs)),
	Checks = case {$1, is_list($2)} of
		{check, true} -> $2;
		{check, false} -> [$2];
		{query, _} -> [$2]
	end,
	FinalRes = [begin
		IsInformational = case $1 of
			query -> true;
			check ->
				is_tuple(Check) andalso
					lists:member(element(1, Check),
						[call, use, module_call, module_use, application_call, application_use])
		end,
		{ok, Res0} = case $1 of
			check -> xref:analyze(Xref, Check);
			query -> xref:q(Xref, Check)
		end,
		Res = case IsInformational of
			true -> Res0;
			false ->
				lists:filter(fun(R) ->
					{Mod, InMFA, MFA} = case R of
						{InMFA0 = {M, _, _}, MFA0} -> {M, InMFA0, MFA0};
						{M, _, _} -> {M, R, R}
					end,
					Attrs = try
						Mod:module_info(attributes)
					catch error:undef ->
						[]
					end,
					InlineIgnores = lists:flatten([
						[case V of
							M when is_atom(M) -> {M, '_', '_'};
							{F, A} -> {Mod, F, A};
							_ -> V
						end || V <- Values]
					|| {ignore_xref, Values} <- Attrs]),
					BuiltinIgnores = [
						{eunit_test, wrapper_test_exported_, 0}
					],
					DoCallbackIgnores = case {Check, "$(strip $(XREF_IGNORE_CALLBACKS))"} of
						{exports_not_used, ""} -> true;
						{_, "0"} -> false;
						_ -> true
					end,
					CallbackIgnores = case DoCallbackIgnores of
						false -> [];
						true ->
							Behaviors = lists:flatten([
								[BL || {behavior, BL} <- Attrs],
								[BL || {behaviour, BL} <- Attrs]
							]),
							[{Mod, CF, CA} || B <- Behaviors, {CF, CA} <- B:behaviour_info(callbacks)]
					end,
					WideIgnores = if
						is_list($(XREF_IGNORE)) ->
							[if is_atom(I) -> {I, '_', '_'}; true -> I end
								|| I <- $(XREF_IGNORE)];
						true -> [$(XREF_IGNORE)]
					end,
					Ignores = InlineIgnores ++ BuiltinIgnores ++ CallbackIgnores ++ WideIgnores,
					not (lists:member(InMFA, Ignores)
					orelse lists:member(MFA, Ignores)
					orelse lists:member({Mod, '_', '_'}, Ignores))
				end, Res0)
		end,
		case Res of
			[] -> ok;
			_ when IsInformational ->
				case Check of
					{call, {CM, CF, CA}} ->
						io:format("Functions that ~s:~s/~b calls:~n", [CM, CF, CA]);
					{use, {CM, CF, CA}} ->
						io:format("Function ~s:~s/~b is called by:~n", [CM, CF, CA]);
					{module_call, CMod} ->
						io:format("Modules that ~s calls:~n", [CMod]);
					{module_use, CMod} ->
						io:format("Module ~s is used by:~n", [CMod]);
					{application_call, CApp} ->
						io:format("Applications that ~s calls:~n", [CApp]);
					{application_use, CApp} ->
						io:format("Application ~s is used by:~n", [CApp]);
					_ when $1 =:= query ->
						io:format("Query ~s returned:~n", [Check])
				end,
				[case R of
					{{InM, InF, InA}, {M, F, A}} ->
						io:format("- ~s:~s/~b called by ~s:~s/~b~n",
							[M, F, A, InM, InF, InA]);
					{M, F, A} ->
						io:format("- ~s:~s/~b~n", [M, F, A]);
					ModOrApp ->
						io:format("- ~s~n", [ModOrApp])
				end || R <- Res],
				ok;
			_ ->
				[case {Check, R} of
					{undefined_function_calls, {{InM, InF, InA}, {M, F, A}}} ->
						io:format("Undefined function ~s:~s/~b called by ~s:~s/~b~n",
							[M, F, A, InM, InF, InA]);
					{undefined_functions, {M, F, A}} ->
						io:format("Undefined function ~s:~s/~b~n", [M, F, A]);
					{locals_not_used, {M, F, A}} ->
						io:format("Unused local function ~s:~s/~b~n", [M, F, A]);
					{exports_not_used, {M, F, A}} ->
						io:format("Unused exported function ~s:~s/~b~n", [M, F, A]);
					{deprecated_function_calls, {{InM, InF, InA}, {M, F, A}}} ->
						io:format("Deprecated function ~s:~s/~b called by ~s:~s/~b~n",
							[M, F, A, InM, InF, InA]);
					{deprecated_functions, {M, F, A}} ->
						io:format("Deprecated function ~s:~s/~b~n", [M, F, A]);
					_ ->
						io:format("~p: ~p~n", [Check, R])
				end || R <- Res],
				error
		end
	end || Check <- Checks],
	stopped = xref:stop(Xref),
	case lists:usort(FinalRes) of
		[ok] -> halt(0);
		_ -> halt(1)
	end
endef

xref: deps app
ifdef q
	$(verbose) $(call erlang,$(call xref.erl,query,"$q"),-pa ebin/)
else
	$(verbose) $(call erlang,$(call xref.erl,check,$(XREF_CHECKS)),-pa ebin/)
endif

# Copyright (c) 2016, Loïc Hoguin <essen@ninenines.eu>
# Copyright (c) 2015, Viktor Söderqvist <viktor@zuiderkwast.se>
# This file is part of erlang.mk and subject to the terms of the ISC License.

COVER_REPORT_DIR ?= cover
COVER_DATA_DIR ?= $(COVER_REPORT_DIR)

ifdef COVER
COVER_APPS ?= $(notdir $(ALL_APPS_DIRS))
COVER_DEPS ?=
COVER_EXCLUDE_MODS ?=
endif

# Code coverage for Common Test.

ifdef COVER
ifdef CT_RUN
ifneq ($(wildcard $(TEST_DIR)),)
test-build:: $(TEST_DIR)/ct.cover.spec

$(TEST_DIR)/ct.cover.spec: cover-data-dir
	$(gen_verbose) printf "%s\n" \
		"{incl_app, '$(PROJECT)', details}." \
		"{incl_dirs, '$(PROJECT)', [\"$(call core_native_path,$(CURDIR)/ebin)\" \
			$(foreach a,$(COVER_APPS),$(comma) \"$(call core_native_path,$(APPS_DIR)/$a/ebin)\") \
			$(foreach d,$(COVER_DEPS),$(comma) \"$(call core_native_path,$(DEPS_DIR)/$d/ebin)\")]}." \
		'{export,"$(call core_native_path,$(abspath $(COVER_DATA_DIR))/ct.coverdata)"}.' \
		"{excl_mods, '$(PROJECT)', [$(call comma_list,$(COVER_EXCLUDE_MODS))]}." > $@

CT_RUN += -cover $(TEST_DIR)/ct.cover.spec
endif
endif
endif

# Code coverage for other tools.

ifdef COVER
define cover.erl
	CoverSetup = fun() ->
		Dirs = ["$(call core_native_path,$(CURDIR)/ebin)"
			$(foreach a,$(COVER_APPS),$(comma) "$(call core_native_path,$(APPS_DIR)/$a/ebin)")
			$(foreach d,$(COVER_DEPS),$(comma) "$(call core_native_path,$(DEPS_DIR)/$d/ebin)")],
		Excludes = [$(call comma_list,$(foreach e,$(COVER_EXCLUDE_MODS),"$e"))],
		[case file:list_dir(Dir) of
			{error, enotdir} -> false;
			{error, _} ->	halt(2);
			{ok, Files} ->
			BeamFiles =  [filename:join(Dir, File) ||
				File <- Files,
				not lists:member(filename:basename(File, ".beam"), Excludes),
				filename:extension(File) =:= ".beam"],
			case cover:compile_beam(BeamFiles) of
				{error, _} -> halt(1);
				_ -> true
			end
		end || Dir <- Dirs]
	end,
	CoverExport = fun(Filename) -> cover:export(Filename) end,
endef
else
define cover.erl
	CoverSetup = fun() -> ok end,
	CoverExport = fun(_) -> ok end,
endef
endif

# Core targets

ifdef COVER
ifneq ($(COVER_REPORT_DIR),)
tests::
	$(verbose) $(MAKE) --no-print-directory cover-report
endif

cover-data-dir: | $(COVER_DATA_DIR)

$(COVER_DATA_DIR):
	$(verbose) mkdir -p $(COVER_DATA_DIR)
else
cover-data-dir:
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
		"  all.coverdata Merge all coverdata files into all.coverdata." \
		"" \
		"If COVER=1 is set, coverage data is generated by the targets eunit and ct. The" \
		"target tests additionally generates a HTML coverage report from the combined" \
		"coverdata files from each of these testing tools. HTML reports can be disabled" \
		"by setting COVER_REPORT_DIR to empty."

# Plugin specific targets

COVERDATA = $(filter-out $(COVER_DATA_DIR)/all.coverdata,$(wildcard $(COVER_DATA_DIR)/*.coverdata))

.PHONY: coverdata-clean
coverdata-clean:
	$(gen_verbose) rm -f $(COVER_DATA_DIR)/*.coverdata $(TEST_DIR)/ct.cover.spec

# Merge all coverdata files into one.
define cover_export.erl
	$(foreach f,$(COVERDATA),cover:import("$(f)") == ok orelse halt(1),)
	cover:export("$(COVER_DATA_DIR)/$@"), halt(0).
endef

all.coverdata: $(COVERDATA) cover-data-dir
	$(gen_verbose) $(call erlang,$(cover_export.erl))

# These are only defined if COVER_REPORT_DIR is non-empty. Set COVER_REPORT_DIR to
# empty if you want the coverdata files but not the HTML report.
ifneq ($(COVER_REPORT_DIR),)

.PHONY: cover-report-clean cover-report

cover-report-clean:
	$(gen_verbose) rm -rf $(COVER_REPORT_DIR)
ifneq ($(COVER_REPORT_DIR),$(COVER_DATA_DIR))
	$(if $(shell ls -A $(COVER_DATA_DIR)/),,$(verbose) rmdir $(COVER_DATA_DIR))
endif

ifeq ($(COVERDATA),)
cover-report:
else

# Modules which include eunit.hrl always contain one line without coverage
# because eunit defines test/0 which is never called. We compensate for this.
EUNIT_HRL_MODS = $(subst $(space),$(comma),$(shell \
	grep -H -e '^\s*-include.*include/eunit\.hrl"' src/*.erl \
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
	Perc = fun(Y, N) -> case Y + N of 0 -> 100; S -> round(100 * Y / S) end end,
	TotalPerc = Perc(TotalY, TotalN),
	{ok, F} = file:open("$(COVER_REPORT_DIR)/index.html", [write]),
	io:format(F, "<!DOCTYPE html><html>~n"
		"<head><meta charset=\"UTF-8\">~n"
		"<title>Coverage report</title></head>~n"
		"<body>~n", []),
	io:format(F, "<h1>Coverage</h1>~n<p>Total: ~p%</p>~n", [TotalPerc]),
	io:format(F, "<table><tr><th>Module</th><th>Coverage</th></tr>~n", []),
	[io:format(F, "<tr><td><a href=\"~p.COVER.html\">~p</a></td>"
		"<td>~p%</td></tr>~n",
		[M, M, Perc(Y, N)]) || {M, {Y, N}} <- Report1],
	How = "$(subst $(space),$(comma)$(space),$(basename $(COVERDATA)))",
	Date = "$(shell date -u "+%Y-%m-%dT%H:%M:%SZ")",
	io:format(F, "</table>~n"
		"<p>Generated using ~s and erlang.mk on ~s.</p>~n"
		"</body></html>", [How, Date]),
	halt().
endef

cover-report:
	$(verbose) mkdir -p $(COVER_REPORT_DIR)
	$(gen_verbose) $(call erlang,$(cover_report.erl))

endif
endif # ifneq ($(COVER_REPORT_DIR),)

# Copyright (c) 2016, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

.PHONY: sfx

ifdef RELX_REL
ifdef SFX

# Configuration.

SFX_ARCHIVE ?= $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME)/$(RELX_REL_NAME)-$(RELX_REL_VSN).tar.gz
SFX_OUTPUT_FILE ?= $(RELX_OUTPUT_DIR)/$(RELX_REL_NAME).run

# Core targets.

rel:: sfx

# Plugin-specific targets.

define sfx_stub
#!/bin/sh

TMPDIR=`mktemp -d`
ARCHIVE=`awk '/^__ARCHIVE_BELOW__$$/ {print NR + 1; exit 0;}' $$0`
FILENAME=$$(basename $$0)
REL=$${FILENAME%.*}

tail -n+$$ARCHIVE $$0 | tar -xzf - -C $$TMPDIR

$$TMPDIR/bin/$$REL console
RET=$$?

rm -rf $$TMPDIR

exit $$RET

__ARCHIVE_BELOW__
endef

sfx:
	$(verbose) $(call core_render,sfx_stub,$(SFX_OUTPUT_FILE))
	$(gen_verbose) cat $(SFX_ARCHIVE) >> $(SFX_OUTPUT_FILE)
	$(verbose) chmod +x $(SFX_OUTPUT_FILE)

endif
endif

# Copyright (c) 2013-2017, Loïc Hoguin <essen@ninenines.eu>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# External plugins.

DEP_PLUGINS ?=

$(foreach p,$(DEP_PLUGINS),\
	$(eval $(if $(findstring /,$p),\
		$(call core_dep_plugin,$p,$(firstword $(subst /, ,$p))),\
		$(call core_dep_plugin,$p/plugins.mk,$p))))

help:: help-plugins

help-plugins::
	$(verbose) :

# Copyright (c) 2013-2015, Loïc Hoguin <essen@ninenines.eu>
# Copyright (c) 2015-2016, Jean-Sébastien Pédron <jean-sebastien@rabbitmq.com>
# This file is part of erlang.mk and subject to the terms of the ISC License.

# Fetch dependencies recursively (without building them).

.PHONY: fetch-deps fetch-doc-deps fetch-rel-deps fetch-test-deps \
	fetch-shell-deps

.PHONY: $(ERLANG_MK_RECURSIVE_DEPS_LIST) \
	$(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST) \
	$(ERLANG_MK_RECURSIVE_REL_DEPS_LIST) \
	$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
	$(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST)

fetch-deps: $(ERLANG_MK_RECURSIVE_DEPS_LIST)
fetch-doc-deps: $(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST)
fetch-rel-deps: $(ERLANG_MK_RECURSIVE_REL_DEPS_LIST)
fetch-test-deps: $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST)
fetch-shell-deps: $(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST)

ifneq ($(SKIP_DEPS),)
$(ERLANG_MK_RECURSIVE_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_REL_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST):
	$(verbose) :> $@
else
# By default, we fetch "normal" dependencies. They are also included no
# matter the type of requested dependencies.
#
# $(ALL_DEPS_DIRS) includes $(BUILD_DEPS).

$(ERLANG_MK_RECURSIVE_DEPS_LIST): $(LOCAL_DEPS_DIRS) $(ALL_DEPS_DIRS)
$(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST): $(LOCAL_DEPS_DIRS) $(ALL_DEPS_DIRS) $(ALL_DOC_DEPS_DIRS)
$(ERLANG_MK_RECURSIVE_REL_DEPS_LIST): $(LOCAL_DEPS_DIRS) $(ALL_DEPS_DIRS) $(ALL_REL_DEPS_DIRS)
$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST): $(LOCAL_DEPS_DIRS) $(ALL_DEPS_DIRS) $(ALL_TEST_DEPS_DIRS)
$(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST): $(LOCAL_DEPS_DIRS) $(ALL_DEPS_DIRS) $(ALL_SHELL_DEPS_DIRS)

# Allow to use fetch-deps and $(DEP_TYPES) to fetch multiple types of
# dependencies with a single target.
ifneq ($(filter doc,$(DEP_TYPES)),)
$(ERLANG_MK_RECURSIVE_DEPS_LIST): $(ALL_DOC_DEPS_DIRS)
endif
ifneq ($(filter rel,$(DEP_TYPES)),)
$(ERLANG_MK_RECURSIVE_DEPS_LIST): $(ALL_REL_DEPS_DIRS)
endif
ifneq ($(filter test,$(DEP_TYPES)),)
$(ERLANG_MK_RECURSIVE_DEPS_LIST): $(ALL_TEST_DEPS_DIRS)
endif
ifneq ($(filter shell,$(DEP_TYPES)),)
$(ERLANG_MK_RECURSIVE_DEPS_LIST): $(ALL_SHELL_DEPS_DIRS)
endif

ERLANG_MK_RECURSIVE_TMP_LIST := $(abspath $(ERLANG_MK_TMP)/recursive-tmp-deps-$(shell echo $$PPID).log)

$(ERLANG_MK_RECURSIVE_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_REL_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST) \
$(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST): | $(ERLANG_MK_TMP)
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) rm -f $(ERLANG_MK_RECURSIVE_TMP_LIST)
endif
	$(verbose) touch $(ERLANG_MK_RECURSIVE_TMP_LIST)
	$(verbose) set -e; for dep in $^ ; do \
		if ! grep -qs ^$$dep$$ $(ERLANG_MK_RECURSIVE_TMP_LIST); then \
			echo $$dep >> $(ERLANG_MK_RECURSIVE_TMP_LIST); \
			if grep -qs -E "^[[:blank:]]*include[[:blank:]]+(erlang\.mk|.*/erlang\.mk|.*ERLANG_MK_FILENAME.*)$$" \
			 $$dep/GNUmakefile $$dep/makefile $$dep/Makefile; then \
				$(MAKE) -C $$dep fetch-deps \
				 IS_DEP=1 \
				 ERLANG_MK_RECURSIVE_TMP_LIST=$(ERLANG_MK_RECURSIVE_TMP_LIST); \
			fi \
		fi \
	done
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) sort < $(ERLANG_MK_RECURSIVE_TMP_LIST) | \
		uniq > $(ERLANG_MK_RECURSIVE_TMP_LIST).sorted
	$(verbose) mv $(ERLANG_MK_RECURSIVE_TMP_LIST).sorted $@
	$(verbose) rm $(ERLANG_MK_RECURSIVE_TMP_LIST)
endif
endif # ifneq ($(SKIP_DEPS),)

# List dependencies recursively.

.PHONY: list-deps list-doc-deps list-rel-deps list-test-deps \
	list-shell-deps

list-deps: $(ERLANG_MK_RECURSIVE_DEPS_LIST)
list-doc-deps: $(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST)
list-rel-deps: $(ERLANG_MK_RECURSIVE_REL_DEPS_LIST)
list-test-deps: $(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST)
list-shell-deps: $(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST)

list-deps list-doc-deps list-rel-deps list-test-deps list-shell-deps:
	$(verbose) cat $^

# Query dependencies recursively.

.PHONY: query-deps query-doc-deps query-rel-deps query-test-deps \
	query-shell-deps

QUERY ?= name fetch_method repo version

define query_target
$1: $2 clean-tmp-query.log
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) rm -f $4
endif
	$(verbose) $(foreach dep,$3,\
		echo $(PROJECT): $(foreach q,$(QUERY),$(call query_$(q),$(dep))) >> $4 ;)
	$(if $(filter-out query-deps,$1),,\
		$(verbose) set -e; for dep in $3 ; do \
			if grep -qs ^$$$$dep$$$$ $(ERLANG_MK_TMP)/query.log; then \
				:; \
			else \
				echo $$$$dep >> $(ERLANG_MK_TMP)/query.log; \
				$(MAKE) -C $(DEPS_DIR)/$$$$dep $$@ QUERY="$(QUERY)" IS_DEP=1 || true; \
			fi \
		done)
ifeq ($(IS_APP)$(IS_DEP),)
	$(verbose) touch $4
	$(verbose) cat $4
endif
endef

clean-tmp-query.log:
ifeq ($(IS_DEP),)
	$(verbose) rm -f $(ERLANG_MK_TMP)/query.log
endif

$(eval $(call query_target,query-deps,$(ERLANG_MK_RECURSIVE_DEPS_LIST),$(BUILD_DEPS) $(DEPS),$(ERLANG_MK_QUERY_DEPS_FILE)))
$(eval $(call query_target,query-doc-deps,$(ERLANG_MK_RECURSIVE_DOC_DEPS_LIST),$(DOC_DEPS),$(ERLANG_MK_QUERY_DOC_DEPS_FILE)))
$(eval $(call query_target,query-rel-deps,$(ERLANG_MK_RECURSIVE_REL_DEPS_LIST),$(REL_DEPS),$(ERLANG_MK_QUERY_REL_DEPS_FILE)))
$(eval $(call query_target,query-test-deps,$(ERLANG_MK_RECURSIVE_TEST_DEPS_LIST),$(TEST_DEPS),$(ERLANG_MK_QUERY_TEST_DEPS_FILE)))
$(eval $(call query_target,query-shell-deps,$(ERLANG_MK_RECURSIVE_SHELL_DEPS_LIST),$(SHELL_DEPS),$(ERLANG_MK_QUERY_SHELL_DEPS_FILE)))
