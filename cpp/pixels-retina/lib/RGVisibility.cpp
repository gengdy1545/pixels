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
#include "RGVisibility.h"
#include <stdexcept>
#include <cstring>
#include <thread>

template<size_t CAPACITY>
RGVisibility<CAPACITY>::RGVisibility(uint64_t rgRecordNum)
    : tileCount((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY) {
    size_t allocSize = tileCount * sizeof(TileVisibility<CAPACITY>);
    void* rawMemory = operator new[](allocSize);
    tileVisibilities = static_cast<TileVisibility<CAPACITY>*>(rawMemory);
    for (uint64_t i = 0; i < tileCount; ++i) {
        new (&tileVisibilities[i]) TileVisibility<CAPACITY>();
    }
}

template<size_t CAPACITY>
RGVisibility<CAPACITY>::RGVisibility(uint64_t rgRecordNum, uint64_t timestamp, const std::vector<uint64_t>& initialBitmap)
    : tileCount((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY) {
    size_t allocSize = tileCount * sizeof(TileVisibility<CAPACITY>);
    void* rawMemory = operator new[](allocSize);

    if (initialBitmap.size() < tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY) {
        operator delete[](rawMemory);
        throw std::runtime_error("Initial bitmap size is too small for the given record number.");
    }

    tileVisibilities = static_cast<TileVisibility<CAPACITY>*>(rawMemory);
    for (uint64_t i = 0; i < tileCount; ++i) {
        // Each tile takes 4 uint64_t
        const uint64_t* tileBitmap = &initialBitmap[i * BITMAP_SIZE_PER_TILE_VISIBILITY];
        // We use timestamp 0 for restored checkpoints to serve as the base state
        new (&tileVisibilities[i]) TileVisibility<CAPACITY>(timestamp, tileBitmap);
    }
}

template<size_t CAPACITY>
RGVisibility<CAPACITY>::~RGVisibility() {
    for (uint64_t i = 0; i < tileCount; ++i) {
        tileVisibilities[i].~TileVisibility();
    }
    operator delete[](tileVisibilities);
}

template<size_t CAPACITY>
void RGVisibility<CAPACITY>::collectRGGarbage(uint64_t timestamp) {
// TileVisibility::collectTileGarbage uses COW + Epoch, so it's safe to call concurrently
    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].collectTileGarbage(timestamp);
    }
}

template<size_t CAPACITY>
TileVisibility<CAPACITY>* RGVisibility<CAPACITY>::getTileVisibility(uint32_t rowId) const {
    uint32_t tileIndex = rowId / VISIBILITY_RECORD_CAPACITY;
    if (tileIndex >= tileCount) {
        throw std::runtime_error("Row id is out of range.");
    }
    return &tileVisibilities[tileIndex];
}

template<size_t CAPACITY>
void RGVisibility<CAPACITY>::deleteRGRecord(uint32_t rowId, uint64_t timestamp) {
    TileVisibility<CAPACITY>* tileVisibility = getTileVisibility(rowId);
    tileVisibility->deleteTileRecord(rowId % VISIBILITY_RECORD_CAPACITY, timestamp);
}

template<size_t CAPACITY>
uint64_t* RGVisibility<CAPACITY>::getRGVisibilityBitmap(uint64_t timestamp) {
    // TileVisibility::getTileVisibilityBitmap uses Epoch protection internally
    size_t len = tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
    size_t byteSize = len * sizeof(uint64_t);
    auto* bitmap = new uint64_t[len];
    pixels::g_retina_tracked_memory.fetch_add(byteSize, std::memory_order_relaxed);
    memset(bitmap, 0, byteSize);

    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].getTileVisibilityBitmap(timestamp, bitmap + i * BITMAP_SIZE_PER_TILE_VISIBILITY);
    }
    return bitmap;
}

template<size_t CAPACITY>
uint64_t RGVisibility<CAPACITY>::getBitmapSize() const {
    return tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
}

template<size_t CAPACITY>
double RGVisibility<CAPACITY>::getInvalidRatio() const {
    uint64_t totalInvalid = 0;
    for (uint64_t i = 0; i < tileCount; i++) {
        totalInvalid += tileVisibilities[i].getInvalidCount();
    }
    return static_cast<double>(totalInvalid) / (tileCount * CAPACITY);
}

template<size_t CAPACITY>
std::vector<uint64_t> RGVisibility<CAPACITY>::exportDeletionBlocks() const {
    std::vector<uint64_t> result;
    
    for (uint64_t tileIndex = 0; tileIndex < tileCount; tileIndex++) {
        std::vector<uint64_t> tileItems = tileVisibilities[tileIndex].exportDeletionBlocks();
        
        // Convert tile-local rowId to RG-global rowId
        for (uint64_t item : tileItems) {
            uint16_t localRowId = extractRowId(item);
            uint64_t timestamp = extractTimestamp(item);
            uint32_t globalRowId = tileIndex * CAPACITY + localRowId;
            
            // Pack global rowId (32-bit) and timestamp (48-bit) into uint64_t
            // Use high 16 bits for rowId, low 48 bits for timestamp
            uint64_t globalItem = (static_cast<uint64_t>(globalRowId) << 48) | (timestamp & 0x0000FFFFFFFFFFFFULL);
            result.push_back(globalItem);
        }
    }
    
    return result;
}

template<size_t CAPACITY>
void RGVisibility<CAPACITY>::prependDeletionBlocks(const uint64_t* items, size_t count) {
    // Group items by tile
    std::vector<std::vector<uint64_t>> tileItems(tileCount);
    
    for (size_t i = 0; i < count; i++) {
        uint64_t item = items[i];
        uint32_t globalRowId = static_cast<uint32_t>(item >> 48);
        uint64_t timestamp = item & 0x0000FFFFFFFFFFFFULL;
        
        uint32_t tileIndex = globalRowId / CAPACITY;
        uint16_t localRowId = globalRowId % CAPACITY;
        
        if (tileIndex >= tileCount) {
            throw std::runtime_error("Row id is out of range during prepend.");
        }
        
        // Pack local rowId and timestamp
        uint64_t localItem = makeDeleteIndex(localRowId, timestamp);
        tileItems[tileIndex].push_back(localItem);
    }
    
    // Prepend to each tile
    for (uint64_t tileIndex = 0; tileIndex < tileCount; tileIndex++) {
        if (!tileItems[tileIndex].empty()) {
            tileVisibilities[tileIndex].prependDeletionBlocks(
                tileItems[tileIndex].data(), 
                tileItems[tileIndex].size());
        }
    }
}

template<size_t CAPACITY>
std::vector<uint64_t> RGVisibility<CAPACITY>::getBaseBitmap() const {
    std::vector<uint64_t> result;
    result.reserve(tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY);
    
    for (uint64_t i = 0; i < tileCount; i++) {
        const uint64_t* tileBitmap = tileVisibilities[i].getBaseBitmap();
        for (size_t j = 0; j < BITMAP_SIZE_PER_TILE_VISIBILITY; j++) {
            result.push_back(tileBitmap[j]);
        }
    }
    
    return result;
}

// Explicit Instantiations for JNI use
template class RGVisibility<RETINA_CAPACITY>;
