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
#define ROW_COUNT 25600
#define VISIBILITIES_NUM (ROW_COUNT + 256 - 1) / 256
#define BITMAP_SIZE (VISIBILITIES_NUM * 4)
#define INVALID_BITS_COUNT (-ROW_COUNT & 255)

#include "gtest/gtest.h"
#include "RGVisibility.h"

#include <bitset>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstring>
#include <sstream>

bool RETINA_TEST_DEBUG = true;

class RGVisibilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    }
    
    void TearDown() override {
        delete rgVisibility;
    }

    RGVisibilityInstance* rgVisibility;
};

TEST_F(RGVisibilityTest, BasicDeleteAndVisibility) {
    uint64_t timestamp1 = 100;
    uint64_t timestamp2 = 200;

    rgVisibility->deleteRGRecord(5, timestamp1);
    rgVisibility->deleteRGRecord(10, timestamp1);
    rgVisibility->deleteRGRecord(15, timestamp2);
    rgVisibility->collectRGGarbage(timestamp1);

    uint64_t* bitmap1 = rgVisibility->getRGVisibilityBitmap(timestamp1);
    EXPECT_EQ(bitmap1[0], 0b0000010000100000);
    delete[] bitmap1;

    uint64_t* bitmap2 = rgVisibility->getRGVisibilityBitmap(timestamp2);
    EXPECT_EQ(bitmap2[0], 0b1000010000100000);
    delete[] bitmap2;
}

TEST_F(RGVisibilityTest, InvalidCount) {
    // Fill one block (8 items) in Tile 0 with timestamp 100
    for(int i = 0; i < 8; ++i) {
        rgVisibility->deleteRGRecord(i, 100);
    }
    // Fill second block (8 items) with timestamp 200
    for(int i = 0; i < 8; ++i) {
        rgVisibility->deleteRGRecord(8 + i, 200);
    }

    // GC(150) -> Should reclaim first block (8 items) but not second
    rgVisibility->collectRGGarbage(150);
    
    // Test getInvalidCount instead of getInvalidCount
    uint64_t invalidCount = rgVisibility->getInvalidCount();
    EXPECT_EQ(invalidCount, 8UL);

    // GC(250) -> Should reclaim second block (8 items) -> Total 16
    rgVisibility->collectRGGarbage(250);
    invalidCount = rgVisibility->getInvalidCount();
    EXPECT_EQ(invalidCount, 16UL);
}

TEST_F(RGVisibilityTest, MultiThread) {
    struct DeleteRecord {
        uint64_t timestamp;
        uint32_t rowId;
        DeleteRecord(uint64_t timestamp, uint32_t rowId) : timestamp(timestamp), rowId(rowId) {}
    };

    std::vector<DeleteRecord> deleteHistory;
    std::mutex historyMutex;
    std::mutex printMutex;
    std::atomic<bool> running{true};
    std::atomic<uint64_t> MaxTimestamp{0};
    std::atomic<uint64_t> MinTimestamp{0};
    std::atomic<int> verificationCount{0};
    
    auto printError = [&](const std::string& msg) {
        std::lock_guard<std::mutex> lock(printMutex);
        ADD_FAILURE() << msg;
    };

    auto verifyBitmap = [&](uint64_t timestamp, const uint64_t* bitmap) {
        uint64_t expectedBitmap[BITMAP_SIZE] = {0};
        std::vector<DeleteRecord> historySnapshot;
        
        {
            std::lock_guard<std::mutex> lock(historyMutex);
            historySnapshot = deleteHistory;
        }
        
        for (const auto& record : historySnapshot) {
            if (record.timestamp <= timestamp) {
                uint64_t bitmapIndex = record.rowId / 64;
                uint64_t bitOffset = record.rowId % 64;
                expectedBitmap[bitmapIndex] |= (1ULL << bitOffset);
            }
        }

        for (size_t i = 0; i < BITMAP_SIZE; i++) {
            if (bitmap[i] != expectedBitmap[i]) {
                if (RETINA_TEST_DEBUG) {
                    std::stringstream ss;
                    ss << "Bitmap verification failed at timestamp " << timestamp << "\n";
                    ss << "Bitmap segment " << i << " (rows " << (i*64) << "-" << (i*64+63) << "):\n";
                    ss << "Actual:   " << std::bitset<64>(bitmap[i]) << "\n";
                    ss << "Expected: " << std::bitset<64>(expectedBitmap[i]) << "\n\n";
                    ss << "Delete history up to timestamp " << timestamp << ":\n";
                    for (const auto& record : historySnapshot) {
                        if (record.timestamp <= timestamp) {
                            ss << "- Timestamp " << record.timestamp << ": deleted row " << record.rowId << "\n";
                        }
                    }
                    printError(ss.str());
                }
                return false;
            }
        }
        
        verificationCount++;
        return true;
    };

    auto deleteThread = std::thread([&]() {
        uint64_t timestamp = 1;
        std::random_device rd;
        std::mt19937 gen(rd());
        
        std::vector<uint32_t> remainingRows;
        for (uint32_t i = 0; i < ROW_COUNT; i++) {
            remainingRows.push_back(i);
        }

        while (!remainingRows.empty() && running) {
            std::uniform_int_distribution<size_t> indexDist(0, remainingRows.size() - 1);
            size_t index = indexDist(gen);
            uint32_t rowId = remainingRows[index];

            remainingRows[index] = remainingRows.back();
            remainingRows.pop_back();

            {
                std::lock_guard<std::mutex> lock(historyMutex);
                rgVisibility->deleteRGRecord(rowId, timestamp);
                deleteHistory.emplace_back(timestamp, rowId);
            }

            MaxTimestamp.store(timestamp);
            timestamp++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (RETINA_TEST_DEBUG) {
            std::lock_guard<std::mutex> lock(printMutex);
            std::cout << "Delete thread completed: deleted " << deleteHistory.size() 
                      << " rows with max timestamp " << (timestamp-1) << std::endl;
        }

        running.store(false);
    });

    auto gcThread = std::thread([&]() {
        uint64_t gcTs = 0;
        while (running) {
            gcTs += 10;
            if (gcTs <= MinTimestamp.load()) {
                rgVisibility->collectRGGarbage(gcTs);
                if (RETINA_TEST_DEBUG) {
                    std::lock_guard<std::mutex> lock(printMutex);
                    std::cout << "GC thread completed: GCed up to timestamp " << gcTs << std::endl;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    std::vector<std::thread> getThreads;
    for (int i = 0; i < 100; i++) {
        getThreads.emplace_back([&, i]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            int localVerificationCount = 0;
            
            while (running) {
                uint64_t maxTs = MaxTimestamp.load();
                uint64_t minTs = MinTimestamp.load();
                if (maxTs == 0 || minTs > maxTs) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                
                std::uniform_int_distribution<uint64_t> tsDist(minTs, maxTs);
                uint64_t queryTs = tsDist(gen);
                uint64_t* bitmap = rgVisibility->getRGVisibilityBitmap(queryTs);

                EXPECT_TRUE(verifyBitmap(queryTs, bitmap));

                delete[] bitmap;
                localVerificationCount++;
                MinTimestamp.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }

            if (RETINA_TEST_DEBUG) {
                std::lock_guard<std::mutex> lock(printMutex);
                std::cout << "Get thread " << i << " completed: performed " 
                          << localVerificationCount << " verifications" << std::endl;
            }
        });
    }

    deleteThread.join();
    gcThread.join();
    for (auto& t : getThreads) {
        t.join();
    }

    uint64_t* finalBitmap = rgVisibility->getRGVisibilityBitmap(MaxTimestamp.load());
    uint64_t* expectedFinalBitmap = new uint64_t[BITMAP_SIZE]();
    std::memset(expectedFinalBitmap, 0xFF, sizeof(uint64_t) * BITMAP_SIZE);
    if (INVALID_BITS_COUNT != 0) {
        for (size_t i = ROW_COUNT; i < ROW_COUNT + INVALID_BITS_COUNT; i++) {
            expectedFinalBitmap[i / 64] &= ~(1ULL << (i % 64));
        }
    }
    
    EXPECT_TRUE(verifyBitmap(MaxTimestamp.load(), finalBitmap));
    
    delete[] finalBitmap;
    delete[] expectedFinalBitmap;
}

// Test: verify that getInvalidCount() returns the correct value after one GC,
// even while delete and read threads are still running concurrently.
TEST_F(RGVisibilityTest, InvalidCountAfterGC) {
    // Number of rows to delete in the first batch (must fill at least one full
    // DeleteIndexBlock so that collectRGGarbage has something to compact).
    // We delete 64 rows so that 8 full blocks (8 items each) are produced.
    static constexpr uint32_t FIRST_BATCH_SIZE = 64;

    std::atomic<bool> stopThreads{false};
    // Signals that the first batch of deletes has been committed
    std::atomic<bool> firstBatchDone{false};

    // Timestamp counter shared across threads; each delete gets a unique ts
    std::atomic<uint64_t> tsCounter{1};

    // -----------------------------------------------------------------------
    // Delete thread
    // Phase 1: delete FIRST_BATCH_SIZE distinct rows with increasing timestamps,
    //          then signal firstBatchDone.
    // Phase 2: keep deleting remaining rows until stopThreads is set.
    // -----------------------------------------------------------------------
    std::thread deleteThread([&]() {
        // Phase 1 – first batch (rows 0 .. FIRST_BATCH_SIZE-1)
        for (uint32_t rowId = 0; rowId < FIRST_BATCH_SIZE; ++rowId) {
            uint64_t ts = tsCounter.fetch_add(1, std::memory_order_relaxed);
            rgVisibility->deleteRGRecord(rowId, ts);
        }
        firstBatchDone.store(true, std::memory_order_release);

        // Phase 2 – continue deleting remaining rows until told to stop
        uint32_t nextRow = FIRST_BATCH_SIZE;
        while (!stopThreads.load(std::memory_order_acquire)) {
            if (nextRow < ROW_COUNT) {
                uint64_t ts = tsCounter.fetch_add(1, std::memory_order_relaxed);
                rgVisibility->deleteRGRecord(nextRow, ts);
                ++nextRow;
            } else {
                // All rows deleted; just spin-wait
                std::this_thread::yield();
            }
        }
    });

    // -----------------------------------------------------------------------
    // Read threads – continuously call getRGVisibilityBitmap() until stopped
    // -----------------------------------------------------------------------
    static constexpr int NUM_READ_THREADS = 4;
    std::vector<std::thread> readThreads;
    readThreads.reserve(NUM_READ_THREADS);
    for (int i = 0; i < NUM_READ_THREADS; ++i) {
        readThreads.emplace_back([&]() {
            while (!stopThreads.load(std::memory_order_acquire)) {
                // Use UINT64_MAX so the query timestamp is always >= baseTimestamp,
                // even after GC advances baseTimestamp. We only care that the call
                // doesn't crash; bitmap correctness is covered by MultiThread test.
                uint64_t* bmp = rgVisibility->getRGVisibilityBitmap(UINT64_MAX);
                delete[] bmp;
                std::this_thread::yield();
            }
        });
    }

    // -----------------------------------------------------------------------
    // Main thread: wait for first batch, run GC, snapshot getInvalidCount()
    // -----------------------------------------------------------------------

    // Wait until the first batch of deletes is done
    while (!firstBatchDone.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // GC with the timestamp that covers all first-batch deletes.
    // tsCounter has been incremented FIRST_BATCH_SIZE times (values 1..64),
    // so the max first-batch timestamp is FIRST_BATCH_SIZE.
    uint64_t gcTimestamp = FIRST_BATCH_SIZE;
    rgVisibility->collectRGGarbage(gcTimestamp);

    // Snapshot getInvalidCount() while delete/read threads are still running
    uint64_t snapshot = rgVisibility->getInvalidCount();

    // Stop all threads and wait for them to finish
    stopThreads.store(true, std::memory_order_release);
    deleteThread.join();
    for (auto& t : readThreads) {
        t.join();
    }

    // -----------------------------------------------------------------------
    // Ground-truth: count set bits in baseBitmap across all tiles.
    // Because no second GC was triggered, the baseBitmap is frozen at the
    // state produced by the single collectRGGarbage() call above.
    // -----------------------------------------------------------------------
    std::vector<uint64_t> baseBitmap = rgVisibility->getBaseBitmap();
    uint64_t baseBitmapCount = 0;
    for (uint64_t word : baseBitmap) {
        baseBitmapCount += static_cast<uint64_t>(__builtin_popcountll(word));
    }

    std::cout << "InvalidCountAfterGC: snapshot=" << snapshot
              << " baseBitmapCount=" << baseBitmapCount << std::endl;

    // The two values must be identical: both reflect exactly the rows that
    // were merged into baseBitmap by the single GC call.
    EXPECT_EQ(snapshot, baseBitmapCount)
        << "snapshot=" << snapshot
        << " baseBitmapCount=" << baseBitmapCount;
}
