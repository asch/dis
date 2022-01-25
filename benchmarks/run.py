#!/usr/bin/env python3

import os
import re
import shlex
import subprocess
import time
import toml
import uuid

def discard():
    print("Running discard and parted")
    subprocess.run(shlex.split('sudo blkdiscard /dev/nvme0n1'), check=True)
    subprocess.run(shlex.split(f'sudo parted --script /dev/nvme0n1 mklabel gpt mkpart primary 4MiB {cache_size_M + 4}MiB'), check=True)
    time.sleep(2)

def rbd_on():
    discard()
    subprocess.run(shlex.split('./helpers/rbd_on.sh'), check=True)

def rbd_off():
    subprocess.run(shlex.split('./helpers/rbd_off.sh'), check=True)

def bcache_on():
    discard()
    subprocess.run(shlex.split('./helpers/bcache_on.sh'), check=True)

def bcache_off():
    time.sleep(5)
    subprocess.run(shlex.split('./helpers/bcache_off.sh'), check=True)

def dis_on():
    discard()
    subprocess.run(shlex.split('sudo rm -f /tmp/dis_ready'), check=True)
    subprocess.Popen(shlex.split('./helpers/dis_on.sh'))
    while not(os.path.isfile('/tmp/dis_ready')):
        time.sleep(1)
    time.sleep(1)

def dis_off():
    time.sleep(10)
    subprocess.run(shlex.split('./helpers/dis_off.sh'), check=True)

def fio(dev, pre, post, fd, backend):
    t = toml.load('fio.toml')
    args = [f"--filename={dev}"]
    for o in t['common']:
        args.append(f"--{o}={t['common'][o]}")
    for o in t['nooptions']:
        args.append(f"--{o}")
    for rw in t['rw']:
        for bs in t['bs']:
            for iodepth in t['iodepth']:
                pre()

                size = '100%'
                if 'read' in rw:
                    size = '20g'
                    plain_rbd = 'rbd' in backend and 'bcache' not in backend
                    small_bcache = 'bcache' in backend and '10GBcache' in backend
                    if not (plain_rbd or small_bcache):
                        subprocess.run(shlex.split(f'sudo fio --filename={dev} --name=experiment --ioengine=libaio --direct=1 --refill_buffers=1 --minimal --group_reporting --rw=write --bs=64k --iodepth=32 --size=30g --offset=0'), check=True)
                        #subprocess.run(shlex.split(f'sudo dd if=/dev/zero of={dev} bs=8M oflag=direct count={(30*1024**3)//(8*1024**2)}'), check=True)

                all_args = ['sudo', 'fio'] + args + [f"--rw={rw}", f"--bs={bs}", f"--iodepth={iodepth}", f"--size={size}"]
                print(all_args)

                subprocess.run(shlex.split('sleep 2'), check=True)
                f = subprocess.run(all_args, capture_output=True, universal_newlines=True)

                bw = float(f.stdout.split(';')[47]) / 1024
                iops = f.stdout.split(';')[48]
                if 'read' in rw:
                    bw = float(f.stdout.split(';')[6]) / 1024
                    iops = f.stdout.split(';')[7]
                fd.write(f'{rw},{bs},{iodepth},{backend},{iteration},{bw},{iops}\n')
                print(f.stdout)
                post()

def fb(dev, pre, post, fd, backend):
    os.system("sudo sh -c 'echo 0 > /proc/sys/kernel/randomize_va_space'")
    t = toml.load('fb.toml')
    for w in t['enabled']:
        pre()
        subprocess.run(shlex.split(f'sudo mkfs.ext4 -F -F {dev}'), check=True)
        subprocess.run(shlex.split(f'sudo mount {dev} /mnt'), check=True)

        fname = str(uuid.uuid1())
        with open(fname, "w+") as tmp_file:
            tmp_file.write(t[w])

        args = ['sudo', 'filebench', '-f'] + [fname]
        print(args)
        subprocess.run(shlex.split('sleep 2'), check=True)
        f = subprocess.run(args, capture_output=True, universal_newlines=True)
        subprocess.run(shlex.split('sudo umount /mnt'), check=True)
        os.remove(fname)

        try:
            bw = re.search('Summary:.*\ +(\d+\.\d+)mb/s', f.stdout).group(1)
            ops = re.search('Summary:.*\ +(\d+\.\d+) ops/s', f.stdout).group(1)
            fd.write(f'{w},{backend},{iteration},{bw},{ops}\n')
        except:
            fd.write(f'{w},{backend},{iteration},-1,-1\n')

        print(f.stdout)
        post()

if __name__ == '__main__':
    t = toml.load('config.toml')

    for benchmark in t['benchmarks']:
        tt = toml.load(benchmark + ".toml")
        with open(benchmark + ".csv", "w") as fd:
            fd.write(tt['header'] + "\n")

    for iteration in t['iterations']:
        for benchmark in t['benchmarks']:
            with open(benchmark + ".csv", "a") as fd:
                b_f = eval(benchmark)
                for backend_ in t['enabled']:
                    backend = backend_.split(sep='_')[0]
                    d = t[backend_]['dev']
                    on_f = eval(backend + "_on")
                    off_f = eval(backend + "_off")
                    for cache_size_M in t[backend_]['cache_size_M']:
                        os.environ["cache_size_M"] = str(cache_size_M)
                        for env in t[backend_]["env"]:
                            os.environ[env] = str(t[backend_]["env"][env])

                        cache_size_G = cache_size_M // 1024
                        b_f(dev=d, pre=on_f, post=off_f, fd=fd, backend=backend_ + f"_{cache_size_G}GBcache")
