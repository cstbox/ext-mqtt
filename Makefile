# CSTBox framework
#
# Makefile for building the Debian distribution package containing the
# MQTT gateway extension.
#
# author = Eric PASCUAL - CSTB (eric.pascual@cstb.fr)

# name of the CSTBox module
MODULE_NAME=ext-mqtt

include $(CSTBOX_DEVEL_HOME)/lib/makefile-dist.mk

copy_files: \
	copy_bin_files \
	copy_python_files \
	copy_init_scripts

