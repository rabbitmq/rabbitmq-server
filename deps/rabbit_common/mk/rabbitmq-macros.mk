# Macro to compare two x.y.z versions.
#
# Usage:
#  ifeq ($(call compare_version,$(ERTS_VER),$(MAX_ERTS_VER),<),true)
#    # Only evaluated if $(ERTS_VER) < $(MAX_ERTS_VER)
#  endif

define compare_version
$(shell awk 'BEGIN {
	split("$(1)", v1, ".");
	version1 = v1[1] * 1000000 + v1[2] * 10000 + v1[3] * 100 + v1[4];

	split("$(2)", v2, ".");
	version2 = v2[1] * 1000000 + v2[2] * 10000 + v2[3] * 100 + v2[4];

	if (version1 $(3) version2) {
		print "true";
	} else {
		print "false";
	}
}')
endef
