asm_verbose_0 = @echo " ASM   " $(filter-out $(patsubst %,%.erl,$(ERLC_EXCLUDE)),\
	$(filter %.erl %.core,$(?F)));
asm_verbose_2 = set -x;
asm_verbose = $(asm_verbose_$(V))

OTP_MINIMUM_FROM := $(DEPS_DIR)/rabbitmq_prelaunch/src/rabbit_prelaunch_erlang_compat.erl
OTP_MINIMUM := $(shell grep -E 'define.*OTP_MINIMUM,' $(OTP_MINIMUM_FROM) | cut -d '"' -f 2)

ifneq ($(strip $(OTP_MINIMUM)),)
OLD_COMPILER_PATH := $(DEPS_DIR)/rabbit/priv/compiler
OLD_COMPILER_ARG := -pa $(OLD_COMPILER_PATH)/ebin

deps:: old-compiler

old-compiler: $(OLD_COMPILER_PATH)

distclean:: distclean-old-compiler

distclean-old-compiler:
	rm -rf $(OLD_COMPILER_PATH)

HEX_PM_ERLANG_BUILDS_INDEX := https://builds.hex.pm/builds/otp/amd64/ubuntu-24.04/builds.txt

$(OLD_COMPILER_PATH): $(OTP_MINIMUM_FROM)
	$(gen_verbose) \
	echo "Minimum Erlang/OTP version = $(OTP_MINIMUM)"; \
	otp_builds_index=$@.builds.txt; \
	$(call core_http_get,$$otp_builds_index,$(HEX_PM_ERLANG_BUILDS_INDEX)); \
	otp_version=$$(grep -E '^OTP-$(OTP_MINIMUM)' $$otp_builds_index | tail -1 | cut -d ' ' -f 1); \
	echo "Selected Erlang/OTP version = $$otp_version"; \
	echo "Downloading compiler from Erlang/OTP $(OTP_MINIMUM)"; \
	otp_archive=$@.otp.tar.gz; \
	$(call core_http_get,$$otp_archive,$(dir $(HEX_PM_ERLANG_BUILDS_INDEX))$$otp_version.tar.gz); \
	tar xf "$$otp_archive" -C '$(dir $@)' --strip-components 2 "$$otp_version/lib/compiler-*"; \
	mv '$(dir $@)/compiler-'* $@; \
	touch -r $(OTP_MINIMUM_FROM) $@; \
	rm -f "$$otp_archive" "$$otp_builds_index"

ASM_COMPILER := erlc $(OLD_COMPILER_ARG)
BEAM_COMPILER := erlc

# We compile sources files in two steps:
# 1. From the `.erl` source file to the `.S` assembly file with the old
#    compiler ($(ASM_COMPILER)).
# 2. From the `.S` assembly file to the final `.beam` module with the default
#    compiler.
#
# The assembly files are used by Horus to generate standalone function that
# should be compatible with all versions of Erlang we support, regardless of
# the version of Erlang the RabbitMQ node emitting a transaction uses.
define compile_erl
	$(eval ERL_FIRST_FILES := $(filter-out $(ERLC_EXCLUDE_PATHS),$(COMPILE_FIRST_PATHS)))
	$(eval ASM_FIRST_FILES := $(patsubst %,ebin/%.S,$(notdir $(basename $(ERL_FIRST_FILES)))))
	$(eval ERL_REMAINING_FILES := $(filter-out $(ERLC_EXCLUDE_PATHS) $(COMPILE_FIRST_PATHS),$1))
	$(eval ASM_REMAINING_FILES:= $(patsubst %,ebin/%.S,$(notdir $(basename $(ERL_REMAINING_FILES)))))
	$(asm_verbose) $(ASM_COMPILER) -v -S $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(ERL_FIRST_FILES)
	$(erlc_verbose) $(BEAM_COMPILER) -v $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(ASM_FIRST_FILES)
	$(verbose) $(ASM_COMPILER) -v -S $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(ERL_REMAINING_FILES)
	$(verbose) $(BEAM_COMPILER) -v $(if $(IS_DEP),$(filter-out -Werror,$(ERLC_OPTS)),$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(ASM_REMAINING_FILES)
endef
endif
