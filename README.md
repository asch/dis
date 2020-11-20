# DIS: Discs over Immutable Storage

## Description

`DIS` is a block devices which uses another block device as a cache and is backed by one of several backends. E.g. fast local NVMe as a cache and object backend with Amazon S3 API results to a block device with speeds close to the local NVMe and advantages of remote object store with unlimited size. Compared to `bcache`, `DIS` is crash-consistent, supports various backends and is easily extensible.

## Available caching devices

- Any block device (local drive, loop device, ...)

## Available backends

- File
- Object with Amazon S3 API
- Object with Ceph Rados API
