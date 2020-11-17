# DIS: Discs over Immutable Storage

DIS is a block devices which uses another block device as a cache and is backed by one of several backends. E.g. fast local NVMe as a cache and object backend with Amazon S3 API results to a block device with speeds close to the local NVMe and advantages of remote object store with unlimited size.
