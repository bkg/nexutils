NEXutils
========
Retrieve [NASA NEX](https://aws.amazon.com/nasa/nex/) public data sets from S3.
This script will pull down NetCDF files for climate variables and generate
annual aggregations stored as compressed GeoTIFF.

Usage
-----
Provide a S3 prefix as the only mandatory command line argument such as:

    ./nexutil.rkt NEX-DCP30/NEX-quartile/rcp85/mon/atmos/tasmax

All of the available S3 objects corresponding to that prefix will be
fetched. Depending on the prefix, this will potentially write quite a lot of data to
`/tmp` so be sure you are not starved for disk space. Use the `--datadir` flag
to specify an alternate directory for download.

See `./nexutil.rkt --help` for all available command line flags.

Installation
------------
In addition to Racket, you will need a few packages installed, namely NCO and
GDAL. If you are on Debian or Ubuntu, running `make deps` will ensure you are
ready to go. Otherwise, please use your package manager to install them.

The chunked bottom-up NetCDF files are problematic for GDAL<2.0 which is the
minimum required for translation to GeoTIFF.
