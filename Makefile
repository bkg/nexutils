distro ?= $(shell lsb_release --id --short)

all:

deps:
ifneq (,$(findstring $(distro),Debian Ubuntu))
	sudo apt-get install gdal-bin nco
else
	@echo "Please install NCO and GDAL command line utilities for $(distro)"
endif
