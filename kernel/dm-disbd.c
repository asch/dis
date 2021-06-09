/*
 * Copyright (C) 2016 Peter Desnoyers. All rights reserved.
 * Copyright (C) 2020 Vojtech Aschenbrenner. All rights reserved.
 *
 * This file is released under the GPL.
 */

#include <linux/slab.h>
#include <linux/blkdev.h>
#include <linux/mempool.h>
#include <linux/slab.h>
#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/bio.h>
#include <linux/random.h>
#include <linux/crc32.h>
#include <linux/seq_file.h>
#include <linux/proc_fs.h>
#include <linux/completion.h>
#include <linux/jiffies.h>
#include <linux/sort.h>
#include <linux/miscdevice.h>
#include <linux/workqueue.h>

#include <linux/vmstat.h>

#include "dm-disbd.h"

#define DM_MSG_PREFIX "dis"

struct write_record {
	struct list_head list;
	struct disbd *dis;
	sector_t lba;
	sector_t pba;
	int len;
};

struct disbd {
	spinlock_t lock;
	spinlock_t rb_r_lock;
	spinlock_t rb_w_lock;
	struct rb_root rb_r;
	struct rb_root rb_w;

	int trim_me; /* 0..7 */

	struct bio_list pending_reads;
	wait_queue_head_t read_wait;
	struct bio_list faulted_reads;

	struct bio_list undone_writes;
	struct list_head done_writes;
	wait_queue_head_t write_wait;
	int done_count;
	sector_t done_sectors;
	sector_t prev_done_sectors;
	sector_t max_sectors;
	atomic_t n_undone;
	atomic_t n_done;
	atomic_t seq;

	mempool_t *extent_pool;
	mempool_t *page_pool;
	struct bio_set bs;

	struct dm_dev *dev;

	char nodename[32];
	struct miscdevice misc;

	sector_t base;
	sector_t bound;
	sector_t frontier;

	struct dm_target *ti;
};

/* total size = 48 bytes (64b). fits in 1 cache line
 * for 32b is 32 bytes, fits in ARM cache line
 */
struct extent {
	struct rb_node rb; /* 24 bytes */
	sector_t lba; /* 512B LBA (8) */
	sector_t pba; /* (8) */
	int32_t len; /* (4) */
};

static struct kmem_cache *_extent_cache;

static void extent_init(struct extent *e, sector_t lba, sector_t pba, sector_t len)
{
	memset(e, 0, sizeof(*e));
	e->lba = lba;
	e->pba = pba;
	e->len = len;
}

#define MIN_EXTENTS 16
#define MIN_POOL_PAGES 16
#define MIN_POOL_IOS 16

/************** Extent map management *****************/

enum map_type { MAP_READ = 1, MAP_WRITE = 2 };

/* find a map entry containing 'lba' or the next higher entry.
 * see Documentation/rbtree.txt
 */
static struct extent *_dis_rb_geq(struct rb_root *root, off_t lba)
{
	struct rb_node *node = root->rb_node; /* top of the tree */
	struct extent *higher = NULL;

	while (node) {
		struct extent *e = container_of(node, struct extent, rb);
		if (e->lba >= lba && (!higher || e->lba < higher->lba)) {
			higher = e;
		}
		if (lba < e->lba) {
			node = node->rb_left;
		} else if (lba >= e->lba + e->len) {
			node = node->rb_right;
		} else {
			return e;
		}
	}
	return higher;
}

static struct extent *dis_rb_geq(struct disbd *dis, off_t lba, enum map_type map)
{
	struct extent *e = NULL;
	unsigned long flags;

	struct rb_root *root = (map == MAP_WRITE) ? &dis->rb_w : &dis->rb_r;
	spinlock_t *rb_lock = (map == MAP_WRITE) ? &dis->rb_w_lock : &dis->rb_r_lock;

	spin_lock_irqsave(rb_lock, flags);
	e = _dis_rb_geq(root, lba);
	spin_unlock_irqrestore(rb_lock, flags);

	return e;
}

int _dis_verbose;

/* must hold rb_lock */
static void dis_rb_insert(struct disbd *dis, struct extent *new, enum map_type map)
{
	struct rb_root *root = (map == MAP_WRITE) ? &dis->rb_w : &dis->rb_r;

	struct rb_node **link = &root->rb_node, *parent = NULL;
	struct extent *e = NULL;

	RB_CLEAR_NODE(&new->rb);

	/* Go to the bottom of the tree */
	while (*link) {
		parent = *link;
		e = container_of(parent, struct extent, rb);
		if (new->lba < e->lba) {
			link = &(*link)->rb_left;
		} else {
			link = &(*link)->rb_right;
		}
	}
	/* Put the new node there */
	rb_link_node(&new->rb, parent, link);
	rb_insert_color(&new->rb, root);
}

/* must hold rb_lock */
static void dis_rb_remove(struct disbd *dis, struct extent *e, enum map_type map)
{
	struct rb_root *root = (map == MAP_WRITE) ? &dis->rb_w : &dis->rb_r;
	rb_erase(&e->rb, root);
}

/* must hold rb_lock */
static struct extent *_dis_rb_next(struct extent *e)
{
	struct rb_node *node = rb_next(&e->rb);
	return (node == NULL) ? NULL : container_of(node, struct extent, rb);
}

/*
static struct extent *dis_rb_next(struct disbd *dis, struct extent *e)
{
	unsigned long flags;
	spin_lock_irqsave(&dis->rb_lock, flags);
	e = _dis_rb_next(e);
	spin_unlock_irqrestore(&dis->rb_lock, flags);
	return e;
}
*/

/* Update mapping. Removes any total overlaps, edits any partial
 * overlaps, adds new extent to map.
 */
static struct extent *dis_update_range(struct disbd *dis, sector_t lba, sector_t pba, sector_t len,
				       enum map_type map)
{
	struct extent *e = NULL, *_new = NULL, *_new2 = NULL;
	unsigned long flags;
	struct rb_root *root = (map == MAP_WRITE) ? &dis->rb_w : &dis->rb_r;

	//DMINFO("update range %llu %llu %llu", (u64)lba, (u64)pba, (u64)len);
	BUG_ON(len == 0);

	if (unlikely(!(_new = mempool_alloc(dis->extent_pool, GFP_NOWAIT))))
		return NULL;
	_new2 = mempool_alloc(dis->extent_pool, GFP_NOWAIT);

	spinlock_t *rb_lock = (map == MAP_WRITE) ? &dis->rb_w_lock : &dis->rb_r_lock;

	spin_lock_irqsave(rb_lock, flags);
	e = _dis_rb_geq(root, lba);

	if (e != NULL) {
		/* [----------------------]        e     new     new2
		 *        [++++++]           -> [-----][+++++][--------]
		 */
		if (e->lba < lba && e->lba + e->len > lba + len) {
			sector_t new_lba = lba + len;
			sector_t new_len = e->lba + e->len - new_lba;
			sector_t new_pba = e->pba + (e->len - new_len);

			if (_new2 == NULL)
				goto fail;
			extent_init(_new2, lba + len, new_pba, new_len);
			e->len = lba - e->lba; /* do this *before* inserting below */
			dis_rb_insert(dis, _new2, map);
			e = _new2;
			_new2 = NULL;
		}
		/* [------------]
		 *        [+++++++++]        -> [------][+++++++++]
		 */
		else if (e->lba < lba) {
			e->len = lba - e->lba;
			if (e->len == 0) {
				DMERR("zero-length extent");
				goto fail;
			}
			e = _dis_rb_next(e);
		}
		/*          [------]
		 *   [+++++++++++++++]        -> [+++++++++++++++]
		 */
		while (e != NULL && e->lba + e->len <= lba + len) {
			struct extent *tmp = _dis_rb_next(e);
			dis_rb_remove(dis, e, map);
			mempool_free(e, dis->extent_pool);
			e = tmp;
		}
		/*          [------]
		 *   [+++++++++]        -> [++++++++++][---]
		 */
		if (e != NULL && lba + len > e->lba) {
			sector_t n = (lba + len) - e->lba;
			e->lba += n;
			e->pba += n;
			e->len -= n;
		}
	}

	/* TRIM indicated by pba = -1 */
	if (pba != -1) {
		extent_init(_new, lba, pba, len);
		dis_rb_insert(dis, _new, map);
	}
	spin_unlock_irqrestore(rb_lock, flags);
	if (_new2 != NULL) {
		mempool_free(_new2, dis->extent_pool);
	}
	return NULL;

fail:
	spin_unlock_irqrestore(rb_lock, flags);
	DMERR("could not allocate extent");
	if (_new)
		mempool_free(_new, dis->extent_pool);
	if (_new2)
		mempool_free(_new2, dis->extent_pool);
	return NULL;
}

/************** Received I/O handling *****************/

static void queue_read(struct disbd *dis, struct bio *bio)
{
	unsigned long flags;

	//DMINFO("%llx (%llu %d) -> pending_reads", (u64)bio, (u64)bio->bi_iter.bi_sector, bio_sectors(bio));

	spin_lock_irqsave(&dis->lock, flags);
	bio_list_add(&dis->pending_reads, bio);
	spin_unlock_irqrestore(&dis->lock, flags);
	wake_up(&dis->read_wait);
}

static int split_read_io(struct disbd *dis, struct bio *bio)
{
	struct bio *split = NULL;
	//DMINFO("%llx read %llu %d", (u64)bio, (u64)bio->bi_iter.bi_sector, bio_sectors(bio));

	do {
		sector_t sector = bio->bi_iter.bi_sector;
		unsigned sectors = bio_sectors(bio);
		struct extent *ew = dis_rb_geq(dis, sector, MAP_WRITE);
		struct extent *er = dis_rb_geq(dis, sector, MAP_READ);

		struct extent *e = NULL;

		// Selection between Write and Read extent map
		// 1. If the extent is present only in one map => choose it
		if (er == NULL || ew == NULL) {
			e = (ew != NULL) ? ew : er;
			goto map_selected;
		}

		// 2. If the extent map is present in both Read and Write Maps => prefer Write Map
		if (ew->lba <= sector) {
			e = ew;
		} else if (er->lba <= sector) {
			e = er;
		} else if (ew->lba < sector + sectors) {
			e = ew;
		} else if (er->lba < sector + sectors) {
			e = er;
		}

		struct extent tmp;
		if (e == er && er->lba + er->len >= ew->lba) {
			tmp = *er;
			tmp.len = ew->lba - er->lba;
			e = &tmp;
		}
	map_selected:

		/* [----bio-----] [eeeeeee] - no map at all, fault it */
		if (e == NULL || e->lba >= sector + sectors) {
			queue_read(dis, bio);
			return DM_MAPIO_SUBMITTED;
		}

		/* .      [eeeeeeeeee...
		   . [---------bio------] - bio prefix not mapped */
		else if (sector < e->lba) {
			sector_t overlap = e->lba - sector;
			split = bio_split(bio, overlap, GFP_NOIO, &fs_bio_set);
			//DMINFO("%llx split %llu", (u64)bio, (u64)overlap);
			bio_chain(split, bio);
			split_read_io(dis, split);
		}

		/* .   [eeeeeeeeeeee]
		   .      [---------bio------] - bio prefix mapped - submit
		   .      ^ overlap ^ */
		else {
			sector_t overlap = e->lba + e->len - sector;
			if (overlap < sectors) {
				sectors = overlap;
				split = bio_split(bio, sectors, GFP_NOIO, &fs_bio_set);
				//DMINFO("%llx split2 %llu", (u64)bio, (u64)overlap);
				bio_chain(split, bio);
			} else {
				split = bio;
			}

			sector = e->pba + sector - e->lba; /* nonsense if zerofill */
			split->bi_iter.bi_sector = sector;
			//DMINFO("%llu rmap %llu", (u64)split, sector);
			bio_set_dev(split, dis->dev->bdev);
			generic_make_request(split);
		}
	} while (split != bio);

	return DM_MAPIO_SUBMITTED;
}

/* TODO: not sure how to get the map update properly synchronized.
 * We have to avoid the case where following reads get the new PBA, but
 * pass the write to that PBA and get bogus data. (it's OK to get the
 * old data for that *LBA* until we complete a write, but not the arbitrary
 * contents of the new PBA)
 * Unfortunately we can't just use bio_chain to link the bios together,
 * because the device mapper framework uses endio.
 *
 * possible solution:
 *   - clone the bio and submit
 *   - on cloned bio endio:
 *      - update map
 *      - complete parent bio successfully
 *
 * with this solution the header endio just has to free the bio and page
 */

/* endio function for write header. This is a total hack at the moment -
 * bi_private points to a struct write_record, which has the extent info
 * to put in the write map; doesn't handled batched writes
 */
static void dis_hdr_endio(struct bio *bio)
{
	struct bio_vec *bv;
	struct write_record *rec = bio->bi_private;
	struct disbd *dis = rec->dis;
	//struct bvec_iter_all iter;  -- different kernel version
	int i;

	//DMINFO("%llu hdr endio %llu %d : %llu %llu %d",
	//(u64)bio, (u64)bio->bi_iter.bi_sector, bio_sectors(bio),
	//(u64)rec->lba, (u64)rec->pba, (int)rec->len);
	//bio_for_each_segment_all(bv, bio, iter) {

	if (rec->lba != -1)
		dis_update_range(dis, rec->lba, rec->pba, rec->len, MAP_WRITE);

	bio_for_each_segment_all (bv, bio, i) {
		mempool_free(bv->bv_page, dis->page_pool);
		bv->bv_page = NULL;
	}

	bio_put(bio);
	if (rec->lba != -1) {
		unsigned long flags;
		spin_lock_irqsave(&dis->lock, flags);
		list_add_tail(&rec->list, &dis->done_writes);
		atomic_inc(&dis->n_done);
		spin_unlock_irqrestore(&dis->lock, flags);
		wake_up(&dis->write_wait);
	} else
		kfree(rec);
}

/* note that ppage returns a pointer to the last page in the bio -
 * i.e. the only page if it's a 1-page bio for header/trailer
 * TODO overly complicated - we only need 1 page
 */
static struct bio *dis_alloc_bio(struct disbd *dis, unsigned sectors, struct page **ppage)
{
	int i, val, remainder, npages;
	struct bio_vec *bv = NULL;
	struct bio *bio;
	struct page *page;
	//struct bvec_iter_all iter;

	npages = sectors / 8;
	remainder = (sectors * 512) - npages * PAGE_SIZE;

	if (!(bio = bio_alloc_bioset(GFP_NOIO, npages + (remainder > 0), &dis->bs)))
		goto fail;
	for (i = 0; i < npages; i++) {
		if (!(page = mempool_alloc(dis->page_pool, GFP_NOIO)))
			goto fail;
		val = bio_add_page(bio, page, PAGE_SIZE, 0);
		if (ppage != NULL)
			*ppage = page;
	}
	if (remainder > 0) {
		if (!(page = mempool_alloc(dis->page_pool, GFP_NOIO)))
			goto fail;
		if (ppage != NULL)
			*ppage = page;
		val = bio_add_page(bio, page, remainder, 0);
	}
	return bio;

fail:
	printk(KERN_INFO "dis_alloc_bio: FAIL (%d pages + %d)\n", npages, sectors % 8);
	WARN_ON(1);
	if (bio != NULL) {
		//bio_for_each_segment_all(bv, bio, iter) {
		bio_for_each_segment_all (bv, bio, i) {
			mempool_free(bv->bv_page, dis->page_pool);
		}
		bio_put(bio);
	}
	return NULL;
}

/* Hmm. We could aggregate writes by having a window of W outstanding
 * writes to the device, then queueing any writes that arrive before one
 * of them completes and submitting them all together when the completion
 * comes in.
 */

/* create a bio for a journal header
*/
static struct bio *alloc_header(struct disbd *dis, struct dis_header **hdr)
{
	struct page *page;
	struct bio *bio = dis_alloc_bio(dis, 8, &page);
	if (bio == NULL)
		return NULL;
	bio_add_page(bio, page, DIS_HDR_SIZE, 0);
	bio->bi_end_io = dis_hdr_endio;
	bio_set_dev(bio, dis->dev->bdev);
	bio->bi_opf = WRITE;

	*hdr = page_address(page);
	memset(*hdr, 0, DIS_HDR_SIZE);
	return bio;
}

static void sign_header(struct dis_header *h)
{
	h->h.crc32 = crc32_le(0, (u8 *)h, DIS_HDR_SIZE);
}

#define NO_LBA 0xFFFFFFFFFFUL

static sector_t maybe_wrap(struct disbd *dis, int sectors)
{
	sector_t lba, wrap_lba = NO_LBA;
	unsigned long flags;

	sectors = round_up(sectors, 8);

	spin_lock_irqsave(&dis->lock, flags);
	if (dis->frontier + sectors + 8 >= dis->bound) {
		wrap_lba = dis->frontier;
		dis->frontier = dis->base;
		//DMINFO("wrapping at %d (%d)", (int)wrap_lba, sectors);
	}
	lba = dis->frontier;
	dis->frontier += (sectors + 8);
	spin_unlock_irqrestore(&dis->lock, flags);

	if (wrap_lba != NO_LBA) {
		struct dis_header *h;
		struct bio *bio = alloc_header(dis, &h);
		if (bio == NULL)
			goto bail;
		h->h = (struct _disheader){ .magic = DIS_HDR_MAGIC,
					    .seq = atomic_inc_return(&dis->seq),
					    .n_extents = 0,
					    .n_sectors = dis->bound - wrap_lba };
		sign_header(h);
		bio->bi_iter.bi_sector = wrap_lba;

		/* god this is a hack */
		struct write_record *rec = kmalloc(sizeof(*rec), GFP_NOIO);
		rec->lba = -1;
		rec->dis = dis;
		bio->bi_private = rec;

		generic_make_request(bio);
	}

	/* Divide the log in 8ths (octants).
	 * - dis->trim_me is the number of the oldest non-empty octant.
	 * - when the write frontier enters the octant before that, we trim
	 *    the next octant.
	 * thus trim_me starts at 0, frontier starts at 0. After writing 0..6,
	 * the write frontier crosses over into octant 7 and we trim octant 0.
	 */
	int span = dis->bound - dis->base;
	int octant = (dis->frontier - dis->base) * 8 / span;

	if (((octant + 1) % 8) == dis->trim_me) {
		struct extent *e, *tmp;
		sector_t low = dis->base + dis->trim_me * span / 8;
		sector_t high = low + span / 8;

		spin_lock_irqsave(&dis->rb_w_lock, flags);
		spin_lock(&dis->rb_r_lock);
		for (e = _dis_rb_geq(&dis->rb_w, 0); e != NULL; e = tmp) {
			tmp = _dis_rb_next(e);
			if (e->pba + e->len <= low || e->pba >= high)
				continue;

			/*
			 * Remove also extents from read map which
			 * overlaps remove write map extents
                         */
			//
			// TODO: For now remove the whole extent
			// Later improve with update range
			// Don't forget to remove data from read cache
			while (true) {
				struct extent *er = _dis_rb_geq(&dis->rb_r, e->lba);
				if (er == NULL || er->lba >= e->lba + e->len)
					break;
				dis_rb_remove(dis, er, MAP_READ);
				mempool_free(er, dis->extent_pool);
			}

			dis_rb_remove(dis, e, MAP_WRITE);
			mempool_free(e, dis->extent_pool);
		}
		dis->trim_me = (dis->trim_me + 1) % 8;
		spin_unlock(&dis->rb_r_lock);
		spin_unlock_irqrestore(&dis->rb_w_lock, flags);
	}

bail:
	return lba;
}

/* this prepends a header to each write, with no batching. Maybe not the
 * most efficient way, but it works.
 * indirectly locks dis->lock (via maybe_wrap)
 */
static int do_map_write_io(struct disbd *dis, struct bio *bio)
{
	sector_t lba = bio->bi_iter.bi_sector;
	unsigned sectors = bio_sectors(bio);
	unsigned n_pages = DIV_ROUND_UP(sectors, 8);

	struct write_record *rec = kmalloc(sizeof(*rec), GFP_NOIO);
	if (!rec)
		return DM_MAPIO_DELAY_REQUEUE; /* ??? */

	/* first prep the header and extent list
	 */
	sector_t hdr_pba = maybe_wrap(dis, n_pages * 8);
	struct dis_header *h;
	struct bio *hdr_bio = alloc_header(dis, &h);
	hdr_bio->bi_iter.bi_sector = hdr_pba;
	h->h = (struct _disheader){ .magic = DIS_HDR_MAGIC,
				    .seq = atomic_inc_return(&dis->seq),
				    .n_extents = 1,
				    .n_sectors = sectors };
	sector_t pba = hdr_pba + 8;

	//DMINFO("%llu do map write %llu %d -> %llu", (u64)bio,
	//bio->bi_iter.bi_sector, bio_sectors(bio), (u64)pba);

	h->extents[0].lba = lba;
	h->extents[0].len = sectors;
	sign_header(h);

	/* (chain the bios so hdr_bio completes before bio, updating map)
	 * TODO - bio_chain doesn't work here, because device mapper has
	 * already set the endio function. (works in read, because we only
	 * chain clones)
	 */
	bio_set_dev(bio, dis->dev->bdev);
	bio->bi_iter.bi_sector = pba;
	//bio_chain(bio, hdr_bio); doesn't work

	rec->lba = lba; /* map info for endio */
	rec->pba = pba;
	rec->len = sectors;
	rec->dis = dis;
	hdr_bio->bi_private = rec;

	generic_make_request(hdr_bio);
	generic_make_request(bio);

	return DM_MAPIO_SUBMITTED;
}

static int map_write_io(struct disbd *dis, struct bio *bio)
{
	unsigned long flags;
	int n;

	//DMINFO("%llu map write %llu %d", (u64)bio, bio->bi_iter.bi_sector, bio_sectors(bio));

	spin_lock_irqsave(&dis->lock, flags);
	if ((n = dis->done_sectors) >= dis->max_sectors) {
		atomic_inc(&dis->n_undone);
		bio_list_add(&dis->undone_writes, bio);
	} else {
		dis->done_count++;
		dis->done_sectors += bio_sectors(bio);
	}
	spin_unlock_irqrestore(&dis->lock, flags);
	if (n >= dis->max_sectors)
		return DM_MAPIO_SUBMITTED;
	else
		return do_map_write_io(dis, bio);
}

static int dis_map(struct dm_target *ti, struct bio *bio)
{
	struct disbd *dis = ti->private;

	switch (bio_op(bio)) {
		//	case REQ_OP_DISCARD:
		//		return dis_discard(dis, bio);

	case REQ_OP_FLUSH:
		bio_set_dev(bio, dis->dev->bdev);
		generic_make_request(bio);
		return DM_MAPIO_SUBMITTED;

	case REQ_OP_READ:
		return split_read_io(dis, bio);

	case REQ_OP_WRITE:
		return map_write_io(dis, bio);

	default:
		printk(KERN_INFO "unknown bio op: %d \n", bio_op(bio));
		return DM_MAPIO_KILL;
	}
}

static void purge_map(struct disbd *dis, enum map_type map)
{
	struct rb_root *root = (map == MAP_WRITE) ? &dis->rb_w : &dis->rb_r;
	struct rb_node *node = rb_first(root);
	while (node) {
		struct rb_node *tmp = rb_next(node);
		struct extent *e = container_of(node, struct extent, rb);
		rb_erase(node, root);
		mempool_free(e, dis->extent_pool);
		node = tmp;
	}
}

static void dis_dtr(struct dm_target *ti)
{
	struct disbd *dis = ti->private;

	ti->private = NULL;

	misc_deregister(&dis->misc);
	purge_map(dis, MAP_READ);
	purge_map(dis, MAP_WRITE);

	mempool_destroy(dis->extent_pool);
	mempool_destroy(dis->page_pool);
	bioset_exit(&dis->bs);
	dm_put_device(ti, dis->dev);

	kfree(dis);
}

static struct disbd *_dis;
static const struct file_operations dis_misc_fops;

/*
 * argv[0] = blockdev name
 * argv[1] = misc device name
 */
static int dis_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	int r = -ENOMEM;
	struct disbd *dis;
	unsigned long base, bound;
	sector_t max_sectors;
	char d;

	//DMINFO("ctr %s %s %s %s", argv[0], argv[1], argv[2], argv[3]);

	if (argc != 5) {
		ti->error = "dm-disbd: Invalid argument count";
		if (argc > 5)
			DMINFO("6th %s", argv[5]);
		return -EINVAL;
	}

	if (sscanf(argv[2], "%lu%c", &base, &d) != 1) {
		ti->error = "dm-disbd: Invalid cache base";
		return -EINVAL;
	}
	if (sscanf(argv[3], "%lu%c", &bound, &d) != 1) {
		ti->error = "dm-disbd: Invalid cache bound";
		return -EINVAL;
	}
	if (sscanf(argv[4], "%lu%c", &max_sectors, &d) != 1) {
		ti->error = "dm-disbd: Invalid max sector size";
		return -EINVAL;
	}

	if (!(_dis = dis = kzalloc(sizeof(*dis), GFP_KERNEL)))
		return -ENOMEM;
	ti->private = dis;

#if 0
	dm_table_set_type(ti->table, DM_TYPE_REQUEST_BASED);
#endif
	if ((r = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &dis->dev))) {
		ti->error = "dm-disbd: Device lookup failed.";
		return r;
	}
	if (bound > dis->dev->bdev->bd_inode->i_size / 512) {
		ti->error = "dm-disbd: Invalid cache bound";
		dm_put_device(ti, dis->dev);
		return -EINVAL;
	}
	dis->base = base;
	dis->bound = bound;
	dis->max_sectors = max_sectors;

	sprintf(dis->nodename, "disbd/%s", argv[1]);

	spin_lock_init(&dis->lock);
	spin_lock_init(&dis->rb_r_lock);
	spin_lock_init(&dis->rb_w_lock);
	bio_list_init(&dis->pending_reads);
	bio_list_init(&dis->faulted_reads);
	INIT_LIST_HEAD(&dis->done_writes);
	bio_list_init(&dis->undone_writes);

	init_waitqueue_head(&dis->read_wait);
	init_waitqueue_head(&dis->write_wait);

	r = -ENOMEM;
	ti->error = "dm-disbd: No memory";

	dis->extent_pool = mempool_create_slab_pool(MIN_EXTENTS, _extent_cache);
	if (!dis->extent_pool)
		goto fail;
	dis->page_pool = mempool_create_page_pool(MIN_POOL_PAGES, 0);
	if (!dis->page_pool)
		goto fail;

	if (bioset_init(&dis->bs, 32, 0, BIOSET_NEED_BVECS))
		goto fail;

	dis->rb_r = RB_ROOT;
	dis->rb_w = RB_ROOT;

	ti->error = "dm-disbd: misc_register failed";
	dis->misc.minor = MISC_DYNAMIC_MINOR;
	dis->misc.name = "dm-disbd";
	dis->misc.nodename = dis->nodename;
	dis->misc.fops = &dis_misc_fops;

	dis->ti = ti;

	if (misc_register(&dis->misc))
		goto fail;

	return 0;

fail:
	bioset_exit(&dis->bs);
	if (dis->page_pool)
		mempool_destroy(dis->page_pool);
	if (dis->extent_pool)
		mempool_destroy(dis->extent_pool);
	if (dis->dev)
		dm_put_device(ti, dis->dev);
	kfree(dis);

	return r;
}

static void dis_io_hints(struct dm_target *ti, struct queue_limits *limits)
{
	limits->physical_block_size = 4096;
	limits->io_min = 4096;
	limits->max_hw_sectors = 512; /* want max I/O to be 64 pages */
}

static int dis_iterate_devices(struct dm_target *ti, iterate_devices_callout_fn fn, void *data)
{
	struct disbd *dis = ti->private;
	return fn(ti, dis->dev, dis->base, dis->bound - dis->base, data);
}

#if 0
/* some day I'll try to get request-based mapping to work
 */
static int dis_clone_and_map(struct dm_target *ti, struct request *rq,
			    union map_info *map_context,
			    struct request **__clone)
{
	DMERR("clone_and_map");
	return DM_MAPIO_SUBMITTED;
}

static void dis_release_clone(struct request *clone)
{
	DMERR("release clone");
}

static int dis_end_io(struct dm_target *ti, struct request *clone,
		 blk_status_t error, union map_info *map_context)
{
	DMERR("end_io");
	return DM_ENDIO_DONE;
}

static int dis_busy(struct dm_target *ti)
{
	return false;
}
#endif

static struct target_type dis_target = {
	.name = "disbd",
	.features = DM_TARGET_IMMUTABLE,
	.version = { 1, 0, 0 },
	.module = THIS_MODULE,
	.ctr = dis_ctr,
	.dtr = dis_dtr,
	.map = dis_map,
#if 0
	.clone_and_map_rq = dis_clone_and_map,
	.release_clone_rq = dis_release_clone,
	.rq_end_io       = dis_end_io,
#endif
	.status = 0 /*dis_status*/,
	.prepare_ioctl = 0 /*dis_prepare_ioctl*/,
	.message = 0 /*dis_message*/,
	.iterate_devices = dis_iterate_devices,
	.io_hints = dis_io_hints,
};

struct {
	char *name;
	int value;
} ioctl_map[] = {
	{ "IOCTL_DIS_GET_MAP", IOCTL_DIS_GET_MAP },
	{ "IOCTL_DIS_WRITES", IOCTL_DIS_WRITES },
	{ "IOCTL_DIS_READS", IOCTL_DIS_READS },
	{ "IOCTL_DIS_RESOLVE", IOCTL_DIS_RESOLVE },
	{ 0, 0 },
};
static char *ioctl_name(int code)
{
	int i;
	for (i = 0; ioctl_map[i].name != NULL; i++)
		if (ioctl_map[i].value == code)
			return ioctl_map[i].name;
	return "IOCTL UNKNOWN";
}

static int ioctl_get_map(struct disbd *dis, void *arg)
{
	return 0; // copy over from old version
}

static int ioctl_write_wait(struct disbd *dis, void *arg)
{
	struct ioctl_writes iw;
	struct dis_extent *extents;
	unsigned long flags;
	struct bio_list tmp = BIO_EMPTY_LIST;
	struct list_head tmp_writes;
	int i = 0;

	spin_lock_irqsave(&dis->lock, flags);
	dis->done_sectors -= dis->prev_done_sectors;
	dis->prev_done_sectors = 0;
	//spin_unlock_irqrestore(&dis->lock, flags);

	/* if we free up any stalled bios, grab them here under the lock
	 */
	//spin_lock_irqsave(&dis->lock, flags);
	sector_t n = dis->done_sectors;
	while (!bio_list_empty(&dis->undone_writes) && n < dis->max_sectors) {
		struct bio *bio = bio_list_pop(&dis->undone_writes);
		atomic_dec(&dis->n_undone);
		bio_list_add(&tmp, bio);
		n += bio_sectors(bio);
	}
	spin_unlock_irqrestore(&dis->lock, flags);

	/* now run them through map_write_io again. hysteresis is to
	 * minimize the chance that it goes back on the list again.
	 * (map_write_io locks dis->lock in maybe_wrap)
	 */
	while (!bio_list_empty(&tmp)) {
		struct bio *bio = bio_list_pop(&tmp);
		//DMINFO("%llu write wait recycle", (u64)bio);
		map_write_io(dis, bio);
	}

	if (copy_from_user(&iw, arg, sizeof(iw)))
		return -EFAULT;
	extents = iw.extents;

	wait_event_interruptible(dis->write_wait, !list_empty(&dis->done_writes));

	INIT_LIST_HEAD(&tmp_writes);

	spin_lock_irqsave(&dis->lock, flags);
	for (i = 0; !list_empty(&dis->done_writes) && i < iw.n_extents; i++) {
		struct write_record *rec =
			list_first_entry(&dis->done_writes, struct write_record, list);
		list_del(&rec->list);
		atomic_dec(&dis->n_done);
		list_add_tail(&rec->list, &tmp_writes);
		dis->done_count--;
		dis->prev_done_sectors += rec->len;
		//dis->done_sectors -= rec->len;
	}
	spin_unlock_irqrestore(&dis->lock, flags);

	for (i = 0; !list_empty(&tmp_writes) && i < iw.n_extents; i++) {
		struct write_record *rec = list_first_entry(&tmp_writes, struct write_record, list);
		list_del(&rec->list);
		struct dis_extent e = { .lba = rec->lba, .pba = rec->pba, .len = rec->len };
		kfree(rec);
		if (copy_to_user(extents, &e, sizeof(e))) {
			return -EFAULT;
		}
		extents++;
	}

	//DMINFO("write_wait: %llu = %d", (u64)arg, i);
	iw.n_extents = i;
	if (copy_to_user(arg, &iw, sizeof(iw)))
		return -EFAULT;
	return 0;
}

/* TODO - I don't like this idea of only resolving the faulted reads, since that may
 * make readahead less effective. we'll see. It makes locking easier, though...
 */
static int ioctl_resolve(struct disbd *dis, void *arg)
{
	struct dm_target *ti = dis->ti;
	struct ioctl_resolve ir;
	unsigned long flags;
	int i;

	if (copy_from_user(&ir, arg, sizeof(ir)))
		return -EFAULT;
	struct dis_extent *extents = ir.extents;
	sector_t max_pba = dis->dev->bdev->bd_inode->i_size / 512;

	for (i = 0; i < ir.n_extents; i++) {
		struct dis_extent e;
		if (copy_from_user(&e, &extents[i], sizeof(e)))
			return -EFAULT;
		if (e.lba < ti->begin || e.lba + e.len > ti->begin + ti->len || e.len == 0 ||
		    e.pba + e.len > max_pba) {
			// || e.len % 8*)
			DMERR("put: invalid range: %lu %lu (%llu-%llu)", (long)e.lba, (long)e.len,
			      (u64)ti->begin, (u64)(ti->begin + ti->len));
			return -EINVAL;
		}
		//DMINFO("resolve: %llu -> %llu +%d", (u64)e.lba, (u64)e.pba, e.len);
		dis_update_range(dis, e.lba, e.pba, e.len, MAP_READ);
	}

	/* take all the read bios off the list, run them back through again
	 */
	struct bio *bio;
	while ((bio = bio_list_pop(&dis->faulted_reads)) != NULL) {
		//DMINFO("%llx (%llu %d) -> recycle", (u64)bio, (u64)bio->bi_iter.bi_sector, bio_sectors(bio));
		split_read_io(dis, bio);
	}

	/* trim map entries if requested
	 */
	if (ir.clear_lo != ir.clear_hi) {
		struct extent *e, *tmp;
		sector_t low = ir.clear_lo;
		sector_t high = ir.clear_hi;

		spin_lock_irqsave(&dis->rb_r_lock, flags);
		for (e = _dis_rb_geq(&dis->rb_r, 0); e != NULL; e = tmp) {
			tmp = _dis_rb_next(e);
			if (e->pba + e->len <= low || e->pba >= high)
				continue;
			dis_rb_remove(dis, e, MAP_READ);
			mempool_free(e, dis->extent_pool);
		}
		spin_unlock_irqrestore(&dis->rb_r_lock, flags);
	}

	return 0;
}

static int ioctl_read_wait(struct disbd *dis, void *arg)
{
	unsigned long flags = 0;
	struct ioctl_reads ir;
	struct dis_extent *extents;

	if (copy_from_user(&ir, arg, sizeof(ir)))
		return -EFAULT;
	extents = ir.extents;

	//DMINFO("read_wait pending %d nfaulted %d", !bio_list_empty(&dis->pending_reads), dis->n_faulted);
	//DMINFO("read_wait pending %d", !bio_list_empty(&dis->pending_reads));
	wait_event_interruptible(dis->read_wait, !bio_list_empty(&dis->pending_reads));
	//DMINFO("read_wait pending %d", !bio_list_empty(&dis->pending_reads));

	struct bio_list tmp = BIO_EMPTY_LIST;
	int i;

	spin_lock_irqsave(&dis->lock, flags);
	for (i = 0; !bio_list_empty(&dis->pending_reads) && i < ir.n_extents; i++) {
		struct bio *bio = bio_list_pop(&dis->pending_reads);
		bio_list_add(&tmp, bio);
	}
	spin_unlock_irqrestore(&dis->lock, flags);

	for (i = 0; !bio_list_empty(&tmp) && i < ir.n_extents; i++) {
		struct bio *bio = bio_list_pop(&tmp);
		struct dis_extent e = { .lba = bio->bi_iter.bi_sector,
					.pba = 0,
					.len = bio_sectors(bio) };
		if (copy_to_user(extents, &e, sizeof(e)))
			return -EFAULT;
		extents++;
		bio_list_add(&dis->faulted_reads, bio);
	}

	ir.n_extents = i;
	if (copy_to_user(arg, &ir, sizeof(ir)))
		return -EFAULT;
	return 0;
}

static long dis_dev_ioctl(struct file *fp, unsigned int num, unsigned long arg)
{
	// https://elixir.bootlin.com/linux/latest/source/drivers/char/misc.c#L173
	struct miscdevice *m = fp->private_data;
	struct disbd *dis = container_of(m, struct disbd, misc);

	//printk(KERN_INFO "ioctl %d (%s) dis %p\n", num, ioctl_name(num), dis);

	switch (num) {
	case IOCTL_DIS_NO_OP:
		printk(KERN_INFO "n_done %d n_undone %d\n", atomic_read(&dis->n_done),
		       atomic_read(&dis->n_undone));
		return 0;

	case IOCTL_DIS_GET_MAP:
		return ioctl_get_map(dis, (void *)arg);

	case IOCTL_DIS_WRITES:
		return ioctl_write_wait(dis, (void *)arg);

	case IOCTL_DIS_READS:
		return ioctl_read_wait(dis, (void *)arg);

	case IOCTL_DIS_RESOLVE:
		return ioctl_resolve(dis, (void *)arg);

	default:
		return -EINVAL;
	}
}

static int dis_dev_release(struct inode *in, struct file *fp)
{
	struct miscdevice *m = fp->private_data;
	struct disbd *dis = container_of(m, struct disbd, misc);

	while (!bio_list_empty(&dis->faulted_reads)) {
		struct bio *bio = bio_list_pop(&dis->faulted_reads);
		bio->bi_status = BLK_STS_IOERR;
		bio_endio(bio);
	}

	while (!bio_list_empty(&dis->undone_writes)) {
		struct bio *bio = bio_list_pop(&dis->undone_writes);
		bio->bi_status = BLK_STS_IOERR;
		bio_endio(bio);
	}

	return 0;
}

static const struct file_operations dis_misc_fops = {
	.owner = THIS_MODULE,
	.unlocked_ioctl = dis_dev_ioctl,
	.release = dis_dev_release,
};

static int __init dm_dis_init(void)
{
	int r = -ENOMEM;

	if (!(_extent_cache = KMEM_CACHE(extent, 0)))
		goto fail;
	if ((r = dm_register_target(&dis_target)) < 0)
		goto fail;
	return 0;

fail:
	if (_extent_cache)
		kmem_cache_destroy(_extent_cache);
	return r;
}

static void __exit dm_dis_exit(void)
{
	dm_unregister_target(&dis_target);
	kmem_cache_destroy(_extent_cache);
}

module_init(dm_dis_init);
module_exit(dm_dis_exit);

MODULE_DESCRIPTION(DM_NAME " generic user-space-controlled Object Translation Layer");
MODULE_LICENSE("GPL");
