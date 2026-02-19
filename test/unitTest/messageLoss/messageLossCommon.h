/*
 *  Copyright (C) 2004-2026 Savoir-faire Linux Inc.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

/**
 * @file messageLossCommon.h
 * @brief Common utilities for message loss testing
 *
 * This header provides utilities for testing message delivery reliability,
 * network failure simulation, and sync verification.
 */

#include <chrono>
#include <functional>
#include <string>
#include <vector>
#include <map>
#include <set>

namespace jami {
namespace test {

/**
 * Message delivery status codes (from account_const.h)
 */
enum class MessageDeliveryStatus {
    UNKNOWN = 0,
    IDLE = 1,
    SENDING = 2,
    SENT = 3,
    FAILURE = 4,
    DISPLAYED = 5
};

/**
 * Network simulation modes for testing
 */
enum class NetworkSimulation {
    NORMAL,           // Normal network operation
    HIGH_LATENCY,     // Delayed packet delivery
    PACKET_LOSS,      // Random packet drops
    INTERMITTENT,     // Flapping connection
    OFFLINE           // No connectivity
};

/**
 * Configuration for message loss tests
 */
struct MessageLossTestConfig {
    // Retry behavior (mirrors message_engine.h constants)
    static constexpr unsigned MAX_RETRIES = 20;
    static constexpr auto SAVE_DELAY = std::chrono::seconds(5);

    // Sync buffer (mirrors sync_module.cpp constant)
    static constexpr size_t SYNC_BUFFER_SIZE = 65535; // UINT16_MAX

    // Test timeouts
    static constexpr auto DEFAULT_TIMEOUT = std::chrono::seconds(30);
    static constexpr auto SYNC_TIMEOUT = std::chrono::seconds(120);
    static constexpr auto RETRY_EXHAUSTION_TIMEOUT = std::chrono::minutes(10);
};

/**
 * Helper to track message delivery across accounts
 */
class MessageTracker {
public:
    void onMessageSent(const std::string& accountId, const std::string& msgId) {
        std::lock_guard<std::mutex> lk(mtx_);
        sentMessages_[accountId].insert(msgId);
    }

    void onMessageReceived(const std::string& accountId, const std::string& msgId) {
        std::lock_guard<std::mutex> lk(mtx_);
        receivedMessages_[accountId].insert(msgId);
    }

    void onMessageFailed(const std::string& accountId, const std::string& msgId) {
        std::lock_guard<std::mutex> lk(mtx_);
        failedMessages_[accountId].insert(msgId);
    }

    bool wasDelivered(const std::string& senderAccount, const std::string& receiverAccount,
                      const std::string& msgId) const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto sent = sentMessages_.find(senderAccount);
        auto recv = receivedMessages_.find(receiverAccount);
        if (sent == sentMessages_.end() || recv == receivedMessages_.end())
            return false;
        return sent->second.count(msgId) && recv->second.count(msgId);
    }

    size_t getSentCount(const std::string& accountId) const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = sentMessages_.find(accountId);
        return it != sentMessages_.end() ? it->second.size() : 0;
    }

    size_t getReceivedCount(const std::string& accountId) const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = receivedMessages_.find(accountId);
        return it != receivedMessages_.end() ? it->second.size() : 0;
    }

    size_t getFailedCount(const std::string& accountId) const {
        std::lock_guard<std::mutex> lk(mtx_);
        auto it = failedMessages_.find(accountId);
        return it != failedMessages_.end() ? it->second.size() : 0;
    }

    void reset() {
        std::lock_guard<std::mutex> lk(mtx_);
        sentMessages_.clear();
        receivedMessages_.clear();
        failedMessages_.clear();
    }

private:
    mutable std::mutex mtx_;
    std::map<std::string, std::set<std::string>> sentMessages_;
    std::map<std::string, std::set<std::string>> receivedMessages_;
    std::map<std::string, std::set<std::string>> failedMessages_;
};

/**
 * Utility to generate large payloads for buffer overflow testing
 */
inline std::string generateLargePayload(size_t targetSize) {
    std::string payload;
    payload.reserve(targetSize);
    const std::string chunk = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    while (payload.size() < targetSize) {
        payload += chunk;
    }
    payload.resize(targetSize);
    return payload;
}

/**
 * Helper to wait for specific message delivery with detailed logging
 */
template<typename Predicate>
bool waitForDeliveryWithLog(
    std::condition_variable& cv,
    std::unique_lock<std::mutex>& lk,
    std::chrono::seconds timeout,
    Predicate pred,
    const std::string& description)
{
    auto start = std::chrono::steady_clock::now();
    bool result = cv.wait_for(lk, timeout, pred);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);

    if (result) {
        JAMI_LOG("[MessageLoss] {} - completed in {}ms", description, elapsed.count());
    } else {
        JAMI_WARNING("[MessageLoss] {} - TIMEOUT after {}ms", description, elapsed.count());
    }
    return result;
}

} // namespace test
} // namespace jami
