/*
 * Copyright 2025 PixelsDB.
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
#ifndef PIXELS_RETINA_TILE_VISIBILITY_H
#define PIXELS_RETINA_TILE_VISIBILITY_H

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId) \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif
#ifndef BITMAP_WORDS
#define BITMAP_WORDS(cap) (((cap) + 63) / 64)
#endif
#include "RetinaBase.h"
#include <atomic>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <vector>

#ifdef RETINA_SIMD
#include <immintrin.h>

/**
 * Count the number of set bits in an array of uint64_t words using AVX2.
 *
 * Uses the VPSHUFB-based 4-bit lookup table algorithm (Muła et al.):
 *   - Split each byte into two 4-bit nibbles.
 *   - Use VPSHUFB to look up the popcount of each nibble in a 16-entry table.
 *   - Accumulate byte-level counts with VPSADBW against zero.
 *
 * This processes 32 bytes (4 × uint64_t) per AVX2 iteration, making it
 * efficient when NUM_WORDS >= 4. For the tail (< 4 words), falls back to
 * scalar __builtin_popcountll.
 *
 * @param words  Pointer to the uint64_t array.
 * @param n      Number of uint64_t elements.
 * @return       Total number of set bits.
 */
inline uint64_t popcountBitmapAVX2(const uint64_t* words, size_t n) {
    // 4-bit popcount lookup table: popcount_table[i] = popcount(i) for i in [0,15]
    const __m256i lookup = _mm256_set_epi8(
        4, 3, 3, 2, 3, 2, 2, 1,
        3, 2, 2, 1, 2, 1, 1, 0,
        4, 3, 3, 2, 3, 2, 2, 1,
        3, 2, 2, 1, 2, 1, 1, 0
    );
    const __m256i low_mask = _mm256_set1_epi8(0x0F);
    __m256i acc = _mm256_setzero_si256();

    // Process 4 words (32 bytes) per iteration
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(words + i));
        __m256i lo = _mm256_and_si256(v, low_mask);
        __m256i hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), low_mask);
        __m256i cnt = _mm256_add_epi8(
            _mm256_shuffle_epi8(lookup, lo),
            _mm256_shuffle_epi8(lookup, hi)
        );
        // Accumulate byte counts into 64-bit lanes via SAD against zero
        acc = _mm256_add_epi64(acc, _mm256_sad_epu8(cnt, _mm256_setzero_si256()));
    }

    // Horizontal sum of the four 64-bit lanes in acc
    __m128i lo128 = _mm256_castsi256_si128(acc);
    __m128i hi128 = _mm256_extracti128_si256(acc, 1);
    __m128i sum128 = _mm_add_epi64(lo128, hi128);
    uint64_t result = static_cast<uint64_t>(_mm_cvtsi128_si64(sum128))
                    + static_cast<uint64_t>(_mm_cvtsi128_si64(_mm_unpackhi_epi64(sum128, sum128)));

    // Scalar tail for remaining words
    for (; i < n; ++i) {
        result += static_cast<uint64_t>(__builtin_popcountll(words[i]));
    }
    return result;
}
#endif // RETINA_SIMD

// rowId supports up to 65535, timestamp uses 48 bits
inline uint64_t makeDeleteIndex(uint16_t rowId, uint64_t ts) {
    return (static_cast<uint64_t>(rowId) << 48 | (ts & 0x0000FFFFFFFFFFFFULL));
}

inline uint16_t extractRowId(uint64_t raw) {
    return static_cast<uint16_t>(raw >> 48);
}

inline uint64_t extractTimestamp(uint64_t raw) {
    return (raw & 0x0000FFFFFFFFFFFFULL);
}

struct DeleteIndexBlock : public pixels::RetinaBase<DeleteIndexBlock> {
    static constexpr size_t BLOCK_CAPACITY = 8;
    uint64_t items[BLOCK_CAPACITY] = {0};
    std::atomic<DeleteIndexBlock *> next{nullptr};
};

// C++11 compatibility: ODR definition for constexpr static member
#if __cplusplus < 201703L
inline constexpr size_t DeleteIndexBlock::BLOCK_CAPACITY;
#endif

/**
 * VersionedData - A versioned snapshot of the base state
 * Used for Copy-on-Write during garbage collection
 * IMPORTANT: head is part of the version to ensure atomic visibility
 */
template<size_t CAPACITY>
struct VersionedData : public pixels::RetinaBase<VersionedData<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
    uint64_t baseBitmap[NUM_WORDS];
    uint64_t baseTimestamp;
    uint64_t baseInvalidCount;
    DeleteIndexBlock* head; // Delete chain head, part of the version

    VersionedData() : baseTimestamp(0), baseInvalidCount(0), head(nullptr) {
        std::memset(baseBitmap, 0, sizeof(baseBitmap));
    }

    VersionedData(uint64_t ts, const uint64_t* bitmap, DeleteIndexBlock* h)
        : baseTimestamp(ts), head(h) {
        std::memcpy(baseBitmap, bitmap, NUM_WORDS * sizeof(uint64_t));
#ifdef RETINA_SIMD
        baseInvalidCount = popcountBitmapAVX2(baseBitmap, NUM_WORDS);
#else
        baseInvalidCount = 0;
        for (size_t i = 0; i < NUM_WORDS; ++i) {
            baseInvalidCount += __builtin_popcountll(baseBitmap[i]);
        }
#endif
    }
};

/**
 * RetiredVersion - Tracks a retired version for epoch-based reclamation
 */
template<size_t CAPACITY>
struct RetiredVersion : public pixels::RetinaBase<RetiredVersion<CAPACITY>> {
    VersionedData<CAPACITY>* data; // Fixed: added <CAPACITY>
    DeleteIndexBlock* blocksToDelete;
    uint64_t retireEpoch;

    RetiredVersion(VersionedData<CAPACITY>* d, DeleteIndexBlock* b, uint64_t e)
        : data(d), blocksToDelete(b), retireEpoch(e) {}
};

template<size_t CAPACITY>
class TileVisibility : public pixels::RetinaBase<TileVisibility<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
 public:
    TileVisibility();
    TileVisibility(uint64_t ts, const uint64_t* bitmap);
    ~TileVisibility() override;
    void deleteTileRecord(uint16_t rowId, uint64_t ts);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t* outBitmap) const;
    void collectTileGarbage(uint64_t ts);

    uint64_t getInvalidCount() const;

    // Storage GC methods
    std::vector<uint64_t> exportDeletionBlocks() const;
    void prependDeletionBlocks(const uint64_t* items, size_t count);
    const uint64_t* getBaseBitmap() const;

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    void reclaimRetiredVersions();

    std::atomic<VersionedData<CAPACITY>*> currentVersion;
    std::atomic<DeleteIndexBlock *> tail;
    std::atomic<size_t> tailUsed;
    std::vector<RetiredVersion<CAPACITY>> retired;  // Protected by GC (single writer)
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
