/*-------------------------------------------------------------------------
 *
 * blockset.h
 *		Data structure for operations on set of block numbers
 *
 * IDENTIFICATION
 *    src/include/lib/blockset.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BLOCKSET_H
#define BLOCKSET_H

#include "storage/block.h"

typedef struct BlockSetData *BlockSet;

extern BlockSet blockset_set(BlockSet bs, BlockNumber blkno);
extern bool blockset_get(BlockNumber blkno, BlockSet bs);
extern BlockNumber blockset_next(BlockSet bs, BlockNumber blkno);
extern void blockset_free(BlockSet bs);

#endif							/* BLOCKSET_H */
