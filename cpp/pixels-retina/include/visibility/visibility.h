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
#ifndef PIXELS_RETINA_VISIBILITY_H
#define PIXELS_RETINA_VISIBILITY_H

#include <cstdint>
#include <cstddef>
#include <memory>

/**
 * Bitmap Macros
 * - GET_BITMAP_BIT(bitmap, rowId): Get the bit at the given rowId in the bitmap.
 * - SET_BITMAP_BIT(bitmap, rowId): Set the bit at the given rowId in the bitmap.
 * - CLEAR_BITMAP_BIT(bitmap, rowId): Clear the bit at the given rowId in the bitmap.
 */
#ifndef GET_BITMAP_BIT
#define GET_BITMAP_BIT(bitmap, rowId) (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#endif

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

#ifndef CLEAR_BITMAP_BIT
#define CLEAR_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] &= ~(1ULL << ((rowId) % 64)))
#endif

/**
 * Manage 256-row visibility state.
 */
class Visibility {
public:
    Visibility();
    ~Visibility();

    /**
     * Create a new epoch with the given timestamp
     * @param epochTs
     */
    void createNewEpoch(uint64_t epochTs);

    /**
     * Mark the record as "intend to delete" in the local bitmap,
     * and append the record index to the patch array in the current epoch.
     * @param rowId
     * @param epochTs
     */
    void deleteRecord(int rowId, uint64_t epochTs);

    /**
     * Retrieve a 256-bit bitmap indicating which rows are deleted for the given epoch.
     * @param epochTs
     * @param visibilityBitmap
     */
    void getVisibilityBitmap(uint64_t epochTs, uint64_t *visibilityBitmap);

    /**
     * Removes old epochs whose epochTs is less than the given timestamp,
     * and free patch data for the removed epochs.
     * @param cleanUpToEpochTs
     */
    void cleanEpochArrAndPatchArr(uint64_t cleanUpToEpochTs);

private:
    // Disallow copying for simplicity.
    Visibility(const Visibility&) = delete;
    Visibility& operator=(const Visibility&) = delete;

    // Implementation details are hidden in the .cpp file.
    struct Impl;
    Impl* impl;
};

#endif //PIXELS_RETINA_VISIBILITY_H
