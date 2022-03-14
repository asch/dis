# DIS: blockDevice over Immutable Storage

## Description

`DIS` is a block devices which uses another block device as a cache and is backed by one of several backends. E.g. fast local NVMe as a cache and object backend with Amazon S3 API results to a block device with speeds close to the local NVMe and advantages of remote object store with unlimited size. Compared to `bcache`, `DIS` is crash-consistent, supports various backends and is easily extensible.

## Available caching devices

- Any block device (local drive, loop device, ...)

## Available backends

- File
- Object with Amazon S3 API
- Object with Ceph Rados API

## Requirements

- Linux Kernel 5.0.0 + Headers (Newer kernels not supported.)
- Go 1.16 or newer

## EuroSys 2022

This work was accepted to EuroSys 2022 conference under the title **Beating the I/O bottleneck: A case for log-structured virtual disks**.

**NOTE:** Throughout the paper we use `LSVD` abbreviation instead of `DIS`. However the code uses `DIS` abbreviation everywhere.

## Usage

`DIS` consists from a kernel module and a userspace daemon. `run.sh` contains script which builds the kernel module, load it, setup new device mapper device and run the userspace daemon. Please take your time and edit it before running.

More detailed instructions:

1. Build and load the kernel module.

```bash
$ cd kernel
$ make
$ insmod dm-disbd.ko
```

2. Run the device mapper device.

The device mapper accepts following parameters:

```
0 <N> disbd <device> disa <base> <limit> <backlog>
```

| Parameter | Description |
| --- | --- |
| 0, <N\> | `dmsetup` parameters, N is virtual volume size in 512-byte sectors |
| "disbd" | `dmsetup` parameter, use "disbd" module (must be loaded already) |
| <device\> | the local cache device or partition |
| "disa" | name of the character device for user-space communication (e.g. `/dev/disbd/disa`) |
| <base\> | start offset of write cache in <device> (in sectors) |
| <limit\> | end (+1) of write cache in <device>, again in sectors |
| <backlog\> | max writes to queue to backend before blocking (in sectors) |

The read cache is managed by the userspace, and the write cache by the kernel device mapper; data fetched for reads will be stored in the range from <limit> to the end of the device.

The following example will create the block device `/dev/mapper/disa` (last argument to `dmsetup`) with the control device `/dev/disbd/disa` (5th value in device mapper table):

```bash
$ virt_size=$((80*1024*1024/2)) # 80GB
$ dev=/dev/nvme0n1p1 
$ devsz=$(blockdev --getsz /dev/nvme0n1p1)
$ limit=$((devsz/2))
$ backlog=$((64*1024)) # 128 MB

$ echo 0 $virt_size disbd $dev disa 0 $limit $backlog | dmsetup --noudevsync create disa
```
3. Build, configure and run the userspace daemon.

The userspace component must be started after the device mapper is configured. Note that this can cause a deadlock with `udev`, which normally tries to read the volume partition before `dmsetup` returns; however the map is not available until the userspace is running.

To **build** the userspace daemon:

```bash
$ cd userspace
$ go build
```

**Configuration** is via the [spf13/viper](https://github.com/spf13/viper) system, reading configuration from (a) `config.toml` in the current directory, and (b) environment variables of the form `DIS_`, in that order.

Key configuration parameters for the configuration file are following:

```toml
[cache]
base = "<read cache base (sectors)>"
bound = "<read cache bound (sectors)>"
file = "nvme device / partition"

[backend]
enabled = "file | null | object --  only use object"

[backend.object]
api = "s3 | rados"
gcMode = "on | off | statsOnly | silent"
gcVersion = 2
objectSizeM = "object size (MB)"

[backend.object.s3]
bucket = "<bucket>"
region = "<region>"
remote = "<endpoint> (e.g. http://1.2.3.4:5678)"

[backend.object.rados]
pool = "<rados pool>"

[ioctl] 
ctl = "/dev/disbd/disa" # character device interface to device mapper
extents = 128 # internal parameter
```

Environment variables take precedence, and are of the form DIS_..., with all names upper-cased, e.g. DIS_BACKEND_OBJECT_S3_BUCKET=testbucket.

To **run** the userspace daemon:

```bash
$ cd userspace
$ go run .
```

## Benchmarks

1. Configuration

The configuration for the complete benchmark set is taken from `benchmarks/config.toml`. It has following parameters:

| Parameter | Description |
| --- | --- |
| `iterations` | Array with iteration suffixes. E.g. `[1,2,9]` will run 3 iterations and create output files with suffixes 1, 2 and 9. |
| `enabled` | Array with named configurations to be run. See below. |
| `benchmarks` | Array with benchmarks to be run. Can contain `fio` and `fb` (Filebench). |

Named configurations present different `DIS` setups to be benchmarked. Every named configuration can contain following parameters:

| Parameter | Description |
| --- | --- |
| `dev` | Blockdevice path (e.g. `/dev/mapper/disa`). | 
| `cache_size_M` | Array with cache sizes in MB to be tested (e.g. `[10240, 716800]`). |
| `env` | Environment variables to be set. |

Note that the current configuration includes RGW S3 endpoints and RADOS pools which are specific to our test configuration. Tests were performed for RBD in both replicated and erasure-coded (not reported) configurations, using separate RADOS pool names for each, and for DIS over S3 with replicated (not reported) and erasure-coded pools, using a separate RGW instance configured for each pool.

The `fio.toml` file specifies the fio tests, running all combinations of the following fio parameters:

| Parameter | Description |
| --- | --- | 
| `rw` | Array containing a subset of following values: write, randwrite, read, randread. |
| `bs` | Array with benchmarked block sizes. |
| `iodepth` | Array with benchmarked iodepths. |

The `common` section specifies parameters common to all fio runs like `runtime` or `ioengine` to use. 

The `fb.toml` file specifies the Filebench tests, and contains full Filebench configuration files for each of the tested configurations: "fileserver", fileserver-fsync", "oltp", and "varmail".

2. Running benchmarks

```bash
run_benchmarks.sh
```

or

```bash
$ cd benchmarks
$ ./run.py
```
