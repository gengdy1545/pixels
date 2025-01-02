/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "visibility/visibility.h"
#include <cstring>
#include <new>
#include <algorithm>
#include <stdexcept>
#include <cassert>
#include <mutex>

static const std::size_t BLOCK_CAPACITY = 1024; // how many epochs per block
static const std::size_t PATCH_CHUNK_SIZE = 4096; // how many bytes per patch-chunk

/**
 * Holds metadata about a single epoch
 */
struct EpochInfo {
    // Timestamp of the epoch
    std::uint64_t epochTs;
    // [patchStart, patchEnd) offsets in the patch
    std::size_t patchStart;
    std::size_t patchEnd;
};

/**
 * An array of EpochInfo with 'count' indicating how many are used.
 * minTs, maxTs: to accelerate the search of the epoch.
 */
struct EpochBlock {
    EpochInfo epochs[BLOCK_CAPACITY];
    std::size_t count;
    std::uint64_t minTs;
    std::uint64_t maxTs;
};

/**
 * A single-linked chunk holding up to PATCH_CHUNK_SIZE bytes.
 * used is the number of bytes used, baseOffset is global offset start.
 */
struct PatchChunk {
    PatchChunk *next;
    std::size_t used;
    std::size_t baseOffset;
    unsigned char data[PATCH_CHUNK_SIZE]; // raw bytes
};

struct Visibility::Impl {
    std::mutex mutex;
    /**
     * The 256-row bitmaps
     */
    std::int8_t allValue;                // 0: all not deleted, 1: all deleted, -1: mixed
    std::uint64_t intendDeleteBitmap[4]; // 1-bit means intend to delete
    std::uint64_t actualDeleteBitmap[4]; // 1-bit means actually deleted

    /**
     * Array-of-blocks to store epochs
     */
    EpochBlock** blockArr;               // pointer to an array of block pointers
    std::size_t blockCount;              // number of blocks in use
    std::size_t blockCap;                // capacity of the blockArr pointer array

    /**
     * Patch storage (chunked)
     */
    PatchChunk *patchHead;               // singly-linked list of chunks
    PatchChunk *patchTail;
    std::size_t globalPatchWritePos;     // next write position in the patch

    /**
     * Constructor / Destructor
     */
    Impl() {
        allValue = 0;
        std::memset(intendDeleteBitmap, 0, sizeof(intendDeleteBitmap));
        std::memset(actualDeleteBitmap, 0, sizeof(actualDeleteBitmap));

        // allocate pointer array for blocks
        blockCap = 8;
        blockCount = 0;
        blockArr = new(std::nothrow) EpochBlock *[blockCap];
        if (blockArr) {
            std::memset(blockArr, 0, blockCap * sizeof(EpochBlock*));
        }

        // initialize patch chunk list
        patchHead = nullptr;
        patchTail = nullptr;
        globalPatchWritePos = 0;

        // creat the first block so we always have something
        addBlock();
    }

    ~Impl() {
        // free all blocks
        for (std::size_t i = 0; i < blockCount; i++) {
            delete blockArr[i];
        }
        delete[] blockArr;

        // free patch chunks
        PatchChunk *chunk = patchHead;
        while (chunk) {
            PatchChunk *next = chunk->next;
            delete chunk;
            chunk = next;
        }
    }

    /**
     * Add a new block to the block array
     */
    EpochBlock* addBlock() {
        if (blockCount == blockCap) {
            std::size_t newCap = blockCap * 2;
            auto** newArr = new(std::nothrow) EpochBlock *[newCap];
            if (!newArr) {
                // handle OOM
                return nullptr;
            }
            std::memcpy(newArr, blockArr, blockCount * sizeof(EpochBlock*));
            std::memset(newArr + blockCount, 0, (newCap - blockCount) * sizeof(EpochBlock*));
            delete[] blockArr;
            blockArr = newArr;
            blockCap = newCap;
        }

        EpochBlock* b = new(std::nothrow) EpochBlock;
        if (!b) {
            // handle OOM
            return nullptr;
        }
        b->count = 0;
        b->minTs = 0;
        b->maxTs = 0;
        blockArr[blockCount++] = b;
        return b;
    }

    /**
     * Insert a new epoch in ascending order into the last block
     * Returns pointer to the new EpochInfo or nullptr on error
     */
    EpochInfo* insertEpoch(std::uint64_t epochTs, std::size_t patchStart, std::size_t patchEnd) {
        if (blockCount == 0) {
            if (!addBlock())
                return nullptr; // OOM
        }
        EpochBlock* block = blockArr[blockCount - 1];
        if (block->count == BLOCK_CAPACITY) {
            // need a new block
            block = addBlock();
            if (!block)
                return nullptr; // OOM
        }
        EpochInfo* info = &block->epochs[block->count];
        info->epochTs = epochTs;
        info->patchStart = patchStart;
        info->patchEnd = patchEnd;

        block->count++;
        if (block->count == 1) {
            block->minTs = epochTs;
            block->maxTs = epochTs;
        } else {
            if (epochTs < block->minTs) {
                block->minTs = epochTs;
            } else if (epochTs > block->maxTs) {
                block->maxTs = epochTs;
            }
        }
        return info;
    }

    /**
     * Find epoch by exact match.
     * Return pointer or nullptr.
     */
    EpochInfo* findEpochExact(std::uint64_t epochTs) {
        if (blockCount == 0)
            return nullptr;

        // binary search in block array
        std::size_t l = 0, r = blockCount - 1;
        while(l <= r) {
            std::size_t mid = (l + r) / 2;
            EpochBlock* block = blockArr[mid];
            if (block->maxTs < epochTs) {
                l = mid + 1;
            } else if (block->minTs > epochTs) {
                r = mid - 1;
            } else {
                // candidate block
                l = mid;
                break;
            }
        }
        if (l >= blockCount)
            return nullptr;
        EpochBlock* block = blockArr[l];
        if (block->count == 0)
            return nullptr;
        if (epochTs < block->minTs || epochTs > block->maxTs)
            return nullptr;

        // local binary search in block
        std::size_t lb = 0, rb = block->count - 1;
        while (lb <= rb) {
            std::size_t mid = (lb + rb) / 2;
            std::uint64_t midTs = block->epochs[mid].epochTs;
            if (mid < epochTs) {
                lb = mid + 1;
            } else if (mid > epochTs) {
                rb = mid - 1;
            } else {
                return &block->epochs[mid];
            }
        }
        return nullptr;
    }

    /**
     * Remove blocks whose maxTs < cutoff
     */
    void cleanupOldEpochs(std::uint64_t cutoff) {
        std::size_t removeCount = 0;
        for (; removeCount < blockCount; removeCount++) {
            EpochBlock* block = blockArr[removeCount];
            if (block->maxTs >= cutoff)
                break;
        }
        if (removeCount == 0)
            return;
        if (removeCount == blockCount) {
            throw std::runtime_error("cleanupOldEpochs: all blocks are removed");
        }
        for (std::size_t i = 0; i < removeCount; ++i) {
            delete blockArr[i];
            blockArr[i] = nullptr;
        }

        std::size_t remain = blockCount - removeCount;
        for (std::size_t i = 0; i < remain; ++i) {
            blockArr[i] = blockArr[i + removeCount];
        }
        for (std::size_t i = remain; i < blockCount; ++i) {
            blockArr[i] = nullptr;
        }
        blockCount = remain;
    }

    /**
     *
     */
    void appendPatchBytes(const unsigned char* src, std::size_t len) {
        std::size_t offset = 0;
        while (offset < len) {
            if (!patchTail || patchTail->used == PATCH_CHUNK_SIZE) {
                addPatchChunk();
            }
            PatchChunk* tail = patchTail;
            std::size_t capacity = PATCH_CHUNK_SIZE - tail->used;
            std::size_t toWrite = std::min(len - offset, capacity);
            std::memcpy(tail->data + tail->used, src + offset, toWrite);
            tail->used += toWrite;
            offset += toWrite;
        }
        globalPatchWritePos += len;
    }

    /**
     * Append single byte
     */
    void appendPatchByte(unsigned char c) {
        appendPatchBytes(&c, 1);
    }

    /**
     * Create a new patch chunk
     */
    void addPatchChunk() {
        PatchChunk* c = new(std::nothrow) PatchChunk;
        if (!c) {
            // handle OOM
            return;
        }
        c->next = nullptr;
        c->used = 0;
        if (!patchTail) {
            c->baseOffset = 0;
            patchHead = c;
            patchTail = c;
        } else {
            c->baseOffset = patchTail->baseOffset + patchTail->used;
            patchTail->next = c;
            patchTail = c;
        }
    }

    /**
     * Read patch data [globalOffset..globalOffset+len) into 'desc'
     */
    void readPatchBytes(std::size_t globalOffset, std::size_t len, unsigned char* dest) {
        std::size_t bytesLeft = len;
        std::size_t destOff = 0;
        PatchChunk* c = patchHead;
        while (c && bytesLeft > 0) {
            std::size_t chunkStart = c->baseOffset;
            std::size_t chunkEnd = chunkStart + c->used;
            if (globalOffset >= chunkEnd) {
                c = c->next;
                continue;
            }
            std::size_t offInChunk = (globalOffset > chunkStart) ? (globalOffset - chunkStart) : 0;
            std::size_t available = c->used - offInChunk;
            std::size_t toRead = std::min(available, bytesLeft);
            std::memcpy(dest + destOff, c->data + offInChunk, toRead);
            bytesLeft -= toRead;
            destOff += toRead;
            globalOffset += toRead;
            c = (bytesLeft > 0) ? c->next : c;
        }
        assert(bytesLeft == 0); // If we reach here, bytesLeft should be zero
    }
};

/**
 * Visibility constructor
 */
Visibility::Visibility() {
    impl = new Impl();
}

/**
 * Visibility destructor
 */
Visibility::~Visibility() {
    delete impl;
    impl = nullptr;
}

/**
 * Create a new epoch with the given timestamp
 * @param epochTs
 */
void Visibility::createNewEpoch(uint64_t epochTs) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    std::size_t patchStart = impl->globalPatchWritePos;
    unsigned char checkpointBuf[32];
    std::memcpy(checkpointBuf, impl->intendDeleteBitmap, 32);
    impl->appendPatchBytes(checkpointBuf, 32);
    std::size_t patchEnd = impl->globalPatchWritePos;
    impl->insertEpoch(epochTs, patchStart, patchEnd);
}

/**
 * Mark the record as "intend to delete" in the local bitmap,
 * and append the record index to the patch array in the current epoch.
 * @param rowId
 * @param epochTs
 */
void Visibility::deleteRecord(int rowId, uint64_t epochTs) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (GET_BITMAP_BIT(impl->intendDeleteBitmap, rowId) == 1ULL) {
        // the same record will not be marked twice
        throw std::runtime_error("deleteRecord: already marked as intend to delete");
    }
    SET_BITMAP_BIT(impl->intendDeleteBitmap, rowId);
    EpochInfo* info = impl->findEpochExact(epochTs);
    if (!info) {
        return;
    }
    unsigned char rowByte = static_cast<unsigned char>(rowId);
    std::size_t oldEnd = info->patchEnd;
    impl->appendPatchByte(rowByte);
    std::size_t newEnd = impl->globalPatchWritePos;
    info->patchEnd = newEnd;
}

/**
 * Retrieve a 256-bit bitmap indicating which rows are deleted for the given epoch.
 * @param epochTs
 * @param visibilityBitmap
 */
void Visibility::getVisibilityBitmap(uint64_t epochTs, uint64_t *visibilityBitmap) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    std::memset(visibilityBitmap, 0, 4 * sizeof(uint64_t));
    EpochInfo* info = impl->findEpochExact(epochTs);
    if (!info) {
        return;
    }
    const std::size_t ckpSize = 32;
    unsigned char ckpBuf[ckpSize];
    if ((info->patchEnd - info->patchStart) < ckpSize) {
        throw std::runtime_error("getVisibilityBitmap: invalid patch size");
        return;
    }
    impl->readPatchBytes(info->patchStart, ckpSize, ckpBuf);
    std::memcpy(visibilityBitmap, ckpBuf, ckpSize);
    std::size_t pos = info->patchStart + ckpSize;
    while (pos < info->patchEnd) {
        unsigned char rowByte = 0;
        impl->readPatchBytes(pos, 1, &rowByte);
        SET_BITMAP_BIT(visibilityBitmap, rowByte);
        pos++;
    }
}

/**
 * Removes old epochs whose epochTs is less than the given timestamp,
 * and free patch data for the removed epochs.
 * @param cleanUpToEpochTs
 */
void Visibility::cleanEpochArrAndPatchArr(uint64_t cleanUpToEpochTs) {
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->cleanupOldEpochs(cleanUpToEpochTs);
}