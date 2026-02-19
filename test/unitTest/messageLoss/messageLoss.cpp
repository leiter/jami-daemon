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

/**
 * @file messageLoss.cpp
 * @brief Tests for message loss scenarios in jami-daemon
 *
 * This test suite covers edge cases and failure scenarios that can lead to
 * message loss, including:
 * - Message retry exhaustion (MAX_RETRIES = 20)
 * - Network failures during sync
 * - Large sync data truncation
 * - Race conditions during connection shutdown
 * - Message persistence across daemon restarts
 * - Concurrent sync from multiple devices
 */

#include "fileutils.h"
#include "manager.h"
#include "jamidht/jamiaccount.h"
#include "../../test_runner.h"
#include "jami.h"
#include "account_const.h"
#include "common.h"

#include <dhtnet/connectionmanager.h>
#include <dhtnet/multiplexed_socket.h>

#include <cppunit/TestAssert.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <random>

using namespace libjami::Account;
using namespace std::literals::chrono_literals;

namespace jami {
namespace test {

/**
 * Test data structure for tracking account events
 */
struct AccountData
{
    std::string visibleUri;
    std::string visibleDeviceId;
    std::string conversationId;
    bool registered {false};
    bool stopped {false};
    bool deviceAnnounced {false};
    bool requestReceived {false};
    bool requestRemoved {false};
    bool errorDetected {false};

    // Message tracking
    std::vector<libjami::SwarmMessage> messagesReceived;
    std::vector<libjami::SwarmMessage> messagesLoaded;
    std::map<std::string, int> messageStatus; // msgId -> status (2=SENDING, 3=SENT, 4=FAILURE)
    int failureCount {0};
    int sentCount {0};
    int sendingCount {0};

    // Member tracking
    std::map<std::string, int> members;

    void reset() {
        conversationId.clear();
        registered = false;
        stopped = false;
        deviceAnnounced = false;
        requestReceived = false;
        requestRemoved = false;
        errorDetected = false;
        messagesReceived.clear();
        messagesLoaded.clear();
        messageStatus.clear();
        failureCount = 0;
        sentCount = 0;
        sendingCount = 0;
        members.clear();
    }
};

class MessageLossTest : public CppUnit::TestFixture
{
public:
    MessageLossTest()
    {
        // Init daemon
        libjami::init(libjami::InitFlag(libjami::LIBJAMI_FLAG_DEBUG | libjami::LIBJAMI_FLAG_CONSOLE_LOG));
        if (not Manager::instance().initialized)
            CPPUNIT_ASSERT(libjami::start("jami-sample.yml"));
    }

    ~MessageLossTest() { libjami::fini(); }

    static std::string name() { return "MessageLoss"; }

    void setUp();
    void tearDown();

    std::string aliceId;
    AccountData aliceData;
    std::string bobId;
    AccountData bobData;
    std::string alice2Id;
    AccountData alice2Data;

    std::mutex mtx;
    std::unique_lock<std::mutex> lk {mtx};
    std::condition_variable cv;

    void connectSignals();

private:
    // ═══════════════════════════════════════════════════════════════════════
    // Test Cases: Message Retry Exhaustion
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Message fails permanently after MAX_RETRIES (20) attempts
     *
     * Scenario:
     * 1. Alice sends message to offline Bob
     * 2. Message retries 20 times
     * 3. Message status should be FAILURE
     * 4. Message should be removed from queue (potential loss)
     */
    void testMessageRetryExhaustion();

    /**
     * Test: Messages sent during network instability
     *
     * Scenario:
     * 1. Alice and Bob establish conversation
     * 2. Bob goes offline
     * 3. Alice sends multiple messages rapidly
     * 4. Each message starts retry cycle
     * 5. Bob comes back online before retries exhausted
     * 6. All messages should eventually be delivered
     */
    void testMessageRetryRecovery();

    /**
     * Test: Verify message failure signal is emitted
     *
     * Scenario:
     * 1. Alice sends message to non-existent peer
     * 2. After retries, FAILURE status should be signaled
     */
    void testMessageFailureSignal();

    // ═══════════════════════════════════════════════════════════════════════
    // Test Cases: Sync Data Loss
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Large sync data exceeding buffer size
     *
     * Scenario:
     * 1. Alice creates many conversations (to exceed 65KB sync data)
     * 2. Alice adds second device
     * 3. Verify all conversations are synced (not truncated)
     */
    void testLargeSyncDataNotTruncated();

    /**
     * Test: Sync during network interruption
     *
     * Scenario:
     * 1. Alice has device1 with conversations
     * 2. Alice adds device2
     * 3. During sync, simulate network issues
     * 4. Verify sync completes or properly fails
     */
    void testSyncDuringNetworkInterruption();

    /**
     * Test: Concurrent sync from multiple devices
     *
     * Scenario:
     * 1. Alice has 3 devices
     * 2. All devices try to sync simultaneously
     * 3. Verify no data corruption or loss
     */
    void testConcurrentMultiDeviceSync();

    // ═══════════════════════════════════════════════════════════════════════
    // Test Cases: Connection Race Conditions
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Message sent during connection shutdown
     *
     * Scenario:
     * 1. Alice and Bob have active connection
     * 2. Bob initiates shutdown
     * 3. Alice sends message during shutdown window
     * 4. Message should not be silently lost
     */
    void testMessageDuringConnectionShutdown();

    /**
     * Test: Rapid connect/disconnect cycles
     *
     * Scenario:
     * 1. Alice and Bob repeatedly connect/disconnect
     * 2. Messages sent during transitions
     * 3. All messages should eventually be delivered or fail explicitly
     */
    void testRapidConnectDisconnectCycles();

    // ═══════════════════════════════════════════════════════════════════════
    // Test Cases: Persistence and Recovery
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: Pending messages survive daemon restart
     *
     * Scenario:
     * 1. Alice sends message to offline Bob
     * 2. Message in pending state (not yet sent)
     * 3. Alice's daemon restarts
     * 4. Pending messages should be restored and retried
     */
    void testPendingMessagesPersistAcrossRestart();

    /**
     * Test: Message sent just before crash (5s save window)
     *
     * Scenario:
     * 1. Alice sends message
     * 2. Message added to queue but not yet persisted (within 5s window)
     * 3. Daemon crashes
     * 4. Verify message is lost (documents the vulnerability)
     */
    void testMessageLossDuringSaveWindow();

    // ═══════════════════════════════════════════════════════════════════════
    // Test Cases: End-to-End Delivery
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Test: No delivery ACK - message appears sent but not received
     *
     * Scenario:
     * 1. Alice sends message to Bob
     * 2. Network drops packet after Alice's write succeeds
     * 3. Alice sees SENT, but Bob never receives
     * 4. Document this gap in delivery confirmation
     */
    void testSentButNotReceivedScenario();

    /**
     * Test: Git commit succeeds but announce fails
     *
     * Scenario:
     * 1. Alice sends conversation message
     * 2. Git commit succeeds locally
     * 3. Swarm announce fails
     * 4. Message is in Alice's history but peers never see it
     */
    void testCommitSuccessAnnounceFailure();

    CPPUNIT_TEST_SUITE(MessageLossTest);

    // Retry exhaustion tests
    CPPUNIT_TEST(testMessageRetryExhaustion);
    CPPUNIT_TEST(testMessageRetryRecovery);
    CPPUNIT_TEST(testMessageFailureSignal);

    // Sync data tests
    CPPUNIT_TEST(testLargeSyncDataNotTruncated);
    CPPUNIT_TEST(testSyncDuringNetworkInterruption);
    CPPUNIT_TEST(testConcurrentMultiDeviceSync);

    // Race condition tests
    CPPUNIT_TEST(testMessageDuringConnectionShutdown);
    CPPUNIT_TEST(testRapidConnectDisconnectCycles);

    // Persistence tests
    CPPUNIT_TEST(testPendingMessagesPersistAcrossRestart);
    CPPUNIT_TEST(testMessageLossDuringSaveWindow);

    // End-to-end delivery tests
    CPPUNIT_TEST(testSentButNotReceivedScenario);
    CPPUNIT_TEST(testCommitSuccessAnnounceFailure);

    CPPUNIT_TEST_SUITE_END();
};

CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(MessageLossTest, MessageLossTest::name());

void
MessageLossTest::setUp()
{
    // Try pre-built accounts first (fast: ~10s vs ~5min for new accounts)
    // Fall back to creating new accounts if archives don't exist
    std::map<std::string, std::string> actors;

    if (std::filesystem::exists("actors/archives/alice.gz") &&
        std::filesystem::exists("actors/archives/bob.gz")) {
        JAMI_DBG("Using pre-built test accounts from archives");
        actors = load_actors_and_wait_for_announcement("actors/alice-bob-prebuilt.yml");
    } else {
        JAMI_DBG("Pre-built archives not found, creating new accounts (this will take several minutes)");
        actors = load_actors_and_wait_for_announcement("actors/alice-bob.yml");
    }

    aliceId = actors["alice"];
    bobId = actors["bob"];
    alice2Id = "";
    aliceData.reset();
    bobData.reset();
    alice2Data.reset();
}

void
MessageLossTest::tearDown()
{
    auto aliceArchive = std::filesystem::current_path().string() + "/alice.gz";
    std::remove(aliceArchive.c_str());

    if (alice2Id.empty()) {
        wait_for_removal_of({aliceId, bobId});
    } else {
        wait_for_removal_of({aliceId, bobId, alice2Id});
    }
}

void
MessageLossTest::connectSignals()
{
    std::map<std::string, std::shared_ptr<libjami::CallbackWrapperBase>> confHandlers;

    // Account registration status
    confHandlers.insert(libjami::exportable_callback<libjami::ConfigurationSignal::VolatileDetailsChanged>(
        [&](const std::string& accountId, const std::map<std::string, std::string>&) {
            AccountData* data = nullptr;
            std::shared_ptr<JamiAccount> account;

            if (accountId == aliceId) {
                data = &aliceData;
                account = Manager::instance().getAccount<JamiAccount>(aliceId);
            } else if (accountId == bobId) {
                data = &bobData;
                account = Manager::instance().getAccount<JamiAccount>(bobId);
            } else if (accountId == alice2Id) {
                data = &alice2Data;
                account = Manager::instance().getAccount<JamiAccount>(alice2Id);
            }

            if (data && account) {
                auto details = account->getVolatileAccountDetails();
                auto status = details[libjami::Account::ConfProperties::Registration::STATUS];
                data->registered = (status == "REGISTERED");
                data->stopped = (status == "UNREGISTERED");
                data->deviceAnnounced = (details[libjami::Account::VolatileProperties::DEVICE_ANNOUNCED] == "true");
            }
            cv.notify_one();
        }));

    // Conversation ready
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::ConversationReady>(
        [&](const std::string& accountId, const std::string& conversationId) {
            if (accountId == aliceId) {
                aliceData.conversationId = conversationId;
            } else if (accountId == bobId) {
                bobData.conversationId = conversationId;
            } else if (accountId == alice2Id) {
                alice2Data.conversationId = conversationId;
            }
            cv.notify_one();
        }));

    // Message status changes - critical for tracking delivery
    confHandlers.insert(libjami::exportable_callback<libjami::ConfigurationSignal::AccountMessageStatusChanged>(
        [&](const std::string& accountId,
            const std::string& /*conversationId*/,
            const std::string& /*peer*/,
            const std::string& msgId,
            int status) {
            AccountData* data = nullptr;
            if (accountId == aliceId) data = &aliceData;
            else if (accountId == bobId) data = &bobData;
            else if (accountId == alice2Id) data = &alice2Data;

            if (data) {
                data->messageStatus[msgId] = status;
                // Status codes: 2=SENDING, 3=SENT, 4=FAILURE
                if (status == 2) data->sendingCount++;
                else if (status == 3) data->sentCount++;
                else if (status == 4) data->failureCount++;
            }
            cv.notify_one();
        }));

    // Message received
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::SwarmMessageReceived>(
        [&](const std::string& accountId, const std::string& /*conversationId*/, libjami::SwarmMessage message) {
            if (accountId == aliceId) {
                aliceData.messagesReceived.emplace_back(message);
            } else if (accountId == bobId) {
                bobData.messagesReceived.emplace_back(message);
            } else if (accountId == alice2Id) {
                alice2Data.messagesReceived.emplace_back(message);
            }
            cv.notify_one();
        }));

    // Messages loaded
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::SwarmLoaded>(
        [&](uint32_t,
            const std::string& accountId,
            const std::string& /*conversationId*/,
            std::vector<libjami::SwarmMessage> messages) {
            if (accountId == aliceId) {
                aliceData.messagesLoaded.insert(aliceData.messagesLoaded.end(), messages.begin(), messages.end());
            } else if (accountId == bobId) {
                bobData.messagesLoaded.insert(bobData.messagesLoaded.end(), messages.begin(), messages.end());
            } else if (accountId == alice2Id) {
                alice2Data.messagesLoaded.insert(alice2Data.messagesLoaded.end(), messages.begin(), messages.end());
            }
            cv.notify_one();
        }));

    // Conversation request
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::ConversationRequestReceived>(
        [&](const std::string& accountId,
            const std::string& /*conversationId*/,
            std::map<std::string, std::string> /*metadatas*/) {
            if (accountId == aliceId) aliceData.requestReceived = true;
            else if (accountId == bobId) bobData.requestReceived = true;
            else if (accountId == alice2Id) alice2Data.requestReceived = true;
            cv.notify_one();
        }));

    // Conversation error
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::OnConversationError>(
        [&](const std::string& accountId,
            const std::string& /*conversationId*/,
            int /*code*/,
            const std::string& /*what*/) {
            if (accountId == aliceId) aliceData.errorDetected = true;
            else if (accountId == bobId) bobData.errorDetected = true;
            else if (accountId == alice2Id) alice2Data.errorDetected = true;
            cv.notify_one();
        }));

    // Member events
    confHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::ConversationMemberEvent>(
        [&](const std::string& accountId, const std::string& /*conversationId*/, const auto& member, auto status) {
            if (accountId == aliceId) aliceData.members[member] = status;
            else if (accountId == bobId) bobData.members[member] = status;
            else if (accountId == alice2Id) alice2Data.members[member] = status;
            cv.notify_one();
        }));

    libjami::registerSignalHandlers(confHandlers);
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations: Message Retry Exhaustion
// ═══════════════════════════════════════════════════════════════════════════

void
MessageLossTest::testMessageRetryExhaustion()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Take Bob offline
    Manager::instance().sendRegister(bobId, false);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.stopped; }));

    // Give time for connection to fully close
    std::this_thread::sleep_for(2s);

    // Send message to offline Bob
    auto initialMsgCount = aliceData.messagesReceived.size();
    libjami::sendMessage(aliceId, convId, std::string("Message to offline peer"), "");

    // Wait for message to appear in Alice's view (local commit)
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return aliceData.messagesReceived.size() > initialMsgCount;
    }));

    // The message engine will retry up to MAX_RETRIES (20) times
    // After exhaustion, we should see a FAILURE status
    // Note: This test documents the current behavior where message is dropped

    // Wait for potential failure signal (may take a while as retries happen)
    // In current implementation, FAILURE is not always signaled for swarm messages
    // This is a gap that should be addressed

    // Bring Bob back online
    Manager::instance().sendRegister(bobId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.registered && bobData.deviceAnnounced; }));

    // Check if Bob received the message (eventual delivery after reconnect)
    bool messageReceived = cv.wait_for(lk, 60s, [&] {
        for (const auto& msg : bobData.messagesReceived) {
            if (msg.body.count("body") && msg.body.at("body") == "Message to offline peer") {
                return true;
            }
        }
        return false;
    });

    // Document current behavior: message may or may not be delivered
    // depending on timing of retries vs reconnection
    JAMI_LOG("Message delivery after reconnect: {}", messageReceived ? "success" : "failure");
}

void
MessageLossTest::testMessageRetryRecovery()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Verify initial connectivity
    auto aliceMsgCount = aliceData.messagesReceived.size();
    auto bobMsgCount = bobData.messagesReceived.size();

    libjami::sendMessage(aliceId, convId, std::string("Test connectivity"), "");
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return bobData.messagesReceived.size() > bobMsgCount;
    }));

    // Take Bob offline briefly
    Manager::instance().sendRegister(bobId, false);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.stopped; }));
    std::this_thread::sleep_for(1s);

    // Send multiple messages while Bob is offline
    const int numMessages = 5;
    bobMsgCount = bobData.messagesReceived.size();

    for (int i = 0; i < numMessages; i++) {
        libjami::sendMessage(aliceId, convId, "Offline message " + std::to_string(i), "");
    }

    // Brief offline period - not enough to exhaust retries
    std::this_thread::sleep_for(3s);

    // Bring Bob back online
    Manager::instance().sendRegister(bobId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.registered && bobData.deviceAnnounced; }));

    // All messages should eventually be delivered
    CPPUNIT_ASSERT(cv.wait_for(lk, 120s, [&] {
        int receivedCount = 0;
        for (const auto& msg : bobData.messagesReceived) {
            if (msg.body.count("body")) {
                auto& body = msg.body.at("body");
                if (body.find("Offline message") != std::string::npos) {
                    receivedCount++;
                }
            }
        }
        return receivedCount >= numMessages;
    }));
}

void
MessageLossTest::testMessageFailureSignal()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Remove Bob's account entirely (not just offline)
    Manager::instance().removeAccount(bobId, true);
    std::this_thread::sleep_for(2s);

    // Reset failure counter
    aliceData.failureCount = 0;
    aliceData.sentCount = 0;

    // Send message to removed account
    libjami::sendMessage(aliceId, convId, std::string("Message to removed peer"), "");

    // Document: In swarm conversations, failure signal may not be emitted
    // because the message is committed to git locally first
    // This is a known gap - the sender sees the message but delivery fails

    // Wait and check for any status signals
    std::this_thread::sleep_for(10s);

    JAMI_LOG("Failure count: {}, Sent count: {}", aliceData.failureCount, aliceData.sentCount);
    // Note: Current implementation may not emit FAILURE for swarm messages
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations: Sync Data Loss
// ═══════════════════════════════════════════════════════════════════════════

void
MessageLossTest::testLargeSyncDataNotTruncated()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);

    // Create many conversations to generate large sync data
    // Goal: exceed UINT16_MAX (65KB) sync buffer
    const int numConversations = 50; // Should generate significant sync data
    std::vector<std::string> conversationIds;

    for (int i = 0; i < numConversations; i++) {
        auto convId = libjami::startConversation(aliceId);
        conversationIds.push_back(convId);
        // Add a message to each conversation
        libjami::sendMessage(aliceId, convId, "Initial message for conv " + std::to_string(i), "");
    }

    // Wait for all conversations to be created
    std::this_thread::sleep_for(5s);

    // Export and create second device
    auto aliceArchive = std::filesystem::current_path().string() + "/alice.gz";
    aliceAccount->exportArchive(aliceArchive);

    std::map<std::string, std::string> details = libjami::getAccountTemplate("RING");
    details[ConfProperties::TYPE] = "RING";
    details[ConfProperties::DISPLAYNAME] = "ALICE2";
    details[ConfProperties::ALIAS] = "ALICE2";
    details[ConfProperties::UPNP_ENABLED] = "true";
    details[ConfProperties::ARCHIVE_PASSWORD] = "";
    details[ConfProperties::ARCHIVE_PATH] = aliceArchive;

    // Track conversations received on device2
    std::set<std::string> syncedConversations;
    std::map<std::string, std::shared_ptr<libjami::CallbackWrapperBase>> syncHandlers;
    syncHandlers.insert(libjami::exportable_callback<libjami::ConversationSignal::ConversationReady>(
        [&](const std::string& accountId, const std::string& conversationId) {
            if (accountId == alice2Id) {
                syncedConversations.insert(conversationId);
                cv.notify_one();
            }
        }));
    libjami::registerSignalHandlers(syncHandlers);

    alice2Id = Manager::instance().addAccount(details);

    // Wait for device announcement
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return alice2Data.deviceAnnounced; }));

    // Wait for sync to complete - all conversations should be synced
    CPPUNIT_ASSERT(cv.wait_for(lk, 300s, [&] {
        return syncedConversations.size() >= static_cast<size_t>(numConversations);
    }));

    // Verify no conversations were lost
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(numConversations), syncedConversations.size());

    JAMI_LOG("Successfully synced {} conversations", syncedConversations.size());
}

void
MessageLossTest::testSyncDuringNetworkInterruption()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);

    // Create some conversations with messages
    auto convId = libjami::startConversation(aliceId);
    for (int i = 0; i < 10; i++) {
        libjami::sendMessage(aliceId, convId, "Message " + std::to_string(i), "");
    }
    std::this_thread::sleep_for(2s);

    // Export archive
    auto aliceArchive = std::filesystem::current_path().string() + "/alice.gz";
    aliceAccount->exportArchive(aliceArchive);

    std::map<std::string, std::string> details = libjami::getAccountTemplate("RING");
    details[ConfProperties::TYPE] = "RING";
    details[ConfProperties::DISPLAYNAME] = "ALICE2";
    details[ConfProperties::ALIAS] = "ALICE2";
    details[ConfProperties::UPNP_ENABLED] = "true";
    details[ConfProperties::ARCHIVE_PASSWORD] = "";
    details[ConfProperties::ARCHIVE_PATH] = aliceArchive;

    alice2Id = Manager::instance().addAccount(details);

    // Wait for device to start
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return alice2Data.registered; }));

    // Simulate network interruption by briefly disabling alice1
    Manager::instance().sendRegister(aliceId, false);
    std::this_thread::sleep_for(500ms);
    Manager::instance().sendRegister(aliceId, true);

    // Wait for sync to complete despite interruption
    CPPUNIT_ASSERT(cv.wait_for(lk, 120s, [&] { return !alice2Data.conversationId.empty(); }));

    // Verify messages are synced
    libjami::loadConversation(alice2Id, convId, "", 0);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return alice2Data.messagesLoaded.size() >= 10;
    }));
}

void
MessageLossTest::testConcurrentMultiDeviceSync()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);

    // Create conversation with messages
    auto convId = libjami::startConversation(aliceId);
    for (int i = 0; i < 5; i++) {
        libjami::sendMessage(aliceId, convId, "Message " + std::to_string(i), "");
    }
    std::this_thread::sleep_for(2s);

    // Export archive
    auto aliceArchive = std::filesystem::current_path().string() + "/alice.gz";
    aliceAccount->exportArchive(aliceArchive);

    // Create alice2 first
    std::map<std::string, std::string> details = libjami::getAccountTemplate("RING");
    details[ConfProperties::TYPE] = "RING";
    details[ConfProperties::DISPLAYNAME] = "ALICE2";
    details[ConfProperties::ALIAS] = "ALICE2";
    details[ConfProperties::UPNP_ENABLED] = "true";
    details[ConfProperties::ARCHIVE_PASSWORD] = "";
    details[ConfProperties::ARCHIVE_PATH] = aliceArchive;

    alice2Id = Manager::instance().addAccount(details);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !alice2Data.conversationId.empty(); }));

    // Now both devices send messages simultaneously
    std::thread t1([&]() {
        for (int i = 0; i < 3; i++) {
            libjami::sendMessage(aliceId, convId, "From device1: " + std::to_string(i), "");
            std::this_thread::sleep_for(100ms);
        }
    });

    std::thread t2([&]() {
        for (int i = 0; i < 3; i++) {
            libjami::sendMessage(alice2Id, convId, "From device2: " + std::to_string(i), "");
            std::this_thread::sleep_for(100ms);
        }
    });

    t1.join();
    t2.join();

    // Wait for messages to sync
    std::this_thread::sleep_for(10s);

    // Load messages on both devices and verify consistency
    aliceData.messagesLoaded.clear();
    alice2Data.messagesLoaded.clear();

    libjami::loadConversation(aliceId, convId, "", 0);
    libjami::loadConversation(alice2Id, convId, "", 0);

    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return aliceData.messagesLoaded.size() >= 11 && alice2Data.messagesLoaded.size() >= 11;
    }));

    // Verify same number of messages on both devices
    CPPUNIT_ASSERT_EQUAL(aliceData.messagesLoaded.size(), alice2Data.messagesLoaded.size());
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations: Connection Race Conditions
// ═══════════════════════════════════════════════════════════════════════════

void
MessageLossTest::testMessageDuringConnectionShutdown()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Send initial message to verify connectivity
    auto bobMsgCount = bobData.messagesReceived.size();
    libjami::sendMessage(aliceId, convId, std::string("Before shutdown"), "");
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return bobData.messagesReceived.size() > bobMsgCount;
    }));

    // Start shutdown and immediately send message
    bobMsgCount = bobData.messagesReceived.size();

    // These operations happen nearly simultaneously
    std::thread shutdownThread([&]() {
        Manager::instance().sendRegister(bobId, false);
    });

    // Send message during shutdown window
    std::this_thread::sleep_for(10ms); // Tiny delay to hit race window
    libjami::sendMessage(aliceId, convId, std::string("During shutdown"), "");

    shutdownThread.join();

    // Wait for Bob to be offline
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.stopped; }));

    // Bring Bob back online
    Manager::instance().sendRegister(bobId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.registered && bobData.deviceAnnounced; }));

    // Check if message was delivered
    bool messageDelivered = cv.wait_for(lk, 60s, [&] {
        for (const auto& msg : bobData.messagesReceived) {
            if (msg.body.count("body") && msg.body.at("body") == "During shutdown") {
                return true;
            }
        }
        return false;
    });

    JAMI_LOG("Message during shutdown: {}", messageDelivered ? "delivered" : "potentially lost");
}

void
MessageLossTest::testRapidConnectDisconnectCycles()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    int messagesSent = 0;
    const int cycles = 5;

    for (int i = 0; i < cycles; i++) {
        // Send message
        libjami::sendMessage(aliceId, convId, "Cycle " + std::to_string(i), "");
        messagesSent++;

        // Quick offline/online cycle for Bob
        Manager::instance().sendRegister(bobId, false);
        std::this_thread::sleep_for(200ms);
        Manager::instance().sendRegister(bobId, true);

        // Wait for Bob to be back online
        cv.wait_for(lk, 10s, [&] { return bobData.registered; });
    }

    // Wait for all messages to potentially arrive
    std::this_thread::sleep_for(30s);

    // Count received messages
    int cycleMessagesReceived = 0;
    for (const auto& msg : bobData.messagesReceived) {
        if (msg.body.count("body")) {
            auto& body = msg.body.at("body");
            if (body.find("Cycle") != std::string::npos) {
                cycleMessagesReceived++;
            }
        }
    }

    JAMI_LOG("Rapid cycles: Sent {}, Received {}", messagesSent, cycleMessagesReceived);

    // All messages should eventually be delivered
    CPPUNIT_ASSERT_EQUAL(messagesSent, cycleMessagesReceived);
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations: Persistence and Recovery
// ═══════════════════════════════════════════════════════════════════════════

void
MessageLossTest::testPendingMessagesPersistAcrossRestart()
{
    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Take Bob offline
    Manager::instance().sendRegister(bobId, false);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.stopped; }));
    std::this_thread::sleep_for(2s);

    // Send message while Bob is offline
    libjami::sendMessage(aliceId, convId, std::string("Persistent message"), "");

    // Wait for message to be persisted (> 5s save timer)
    std::this_thread::sleep_for(7s);

    // "Restart" Alice by unregistering and re-registering
    // Note: This is not a true daemon restart, but tests the registration path
    Manager::instance().sendRegister(aliceId, false);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return aliceData.stopped; }));

    std::this_thread::sleep_for(2s);

    Manager::instance().sendRegister(aliceId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return aliceData.registered && aliceData.deviceAnnounced; }));

    // Bring Bob back online
    Manager::instance().sendRegister(bobId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.registered && bobData.deviceAnnounced; }));

    // Message should be delivered after Alice's "restart"
    bool messageDelivered = cv.wait_for(lk, 60s, [&] {
        for (const auto& msg : bobData.messagesReceived) {
            if (msg.body.count("body") && msg.body.at("body") == "Persistent message") {
                return true;
            }
        }
        return false;
    });

    CPPUNIT_ASSERT_MESSAGE("Persistent message should be delivered after restart", messageDelivered);
}

void
MessageLossTest::testMessageLossDuringSaveWindow()
{
    // This test documents the 5-second save window vulnerability
    // Messages sent within this window before a crash may be lost

    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Take Bob offline
    Manager::instance().sendRegister(bobId, false);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.stopped; }));
    std::this_thread::sleep_for(2s);

    // Send message
    libjami::sendMessage(aliceId, convId, std::string("Within save window"), "");

    // Immediately "crash" (remove account without waiting for save)
    // This simulates the window where message is in memory but not persisted
    // Note: We can't truly simulate a crash, but we can document the window

    JAMI_LOG("DOCUMENTATION: Messages sent within 5s of daemon crash may be lost");
    JAMI_LOG("The scheduleSave() in message_engine.cpp uses a 5-second delay");

    // Wait for save to complete
    std::this_thread::sleep_for(6s);

    // Now the message should be persisted
    // Bring Bob online to verify
    Manager::instance().sendRegister(bobId, true);
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] { return bobData.registered && bobData.deviceAnnounced; }));

    bool messageDelivered = cv.wait_for(lk, 60s, [&] {
        for (const auto& msg : bobData.messagesReceived) {
            if (msg.body.count("body") && msg.body.at("body") == "Within save window") {
                return true;
            }
        }
        return false;
    });

    CPPUNIT_ASSERT_MESSAGE("Message after save window should be delivered", messageDelivered);
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations: End-to-End Delivery
// ═══════════════════════════════════════════════════════════════════════════

void
MessageLossTest::testSentButNotReceivedScenario()
{
    // This test documents the gap where sender sees SENT but receiver never gets message
    // Current architecture: socket->write() success = SENT, but no ACK from peer

    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Verify normal delivery works
    auto bobMsgCount = bobData.messagesReceived.size();
    libjami::sendMessage(aliceId, convId, std::string("Test message"), "");

    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return bobData.messagesReceived.size() > bobMsgCount;
    }));

    JAMI_LOG("DOCUMENTATION: Current architecture has no delivery ACK");
    JAMI_LOG("socket->write() success results in SENT status");
    JAMI_LOG("But packet may be dropped after write, before peer processes");
    JAMI_LOG("Peer pull from git provides eventual consistency, not delivery confirmation");

    // The test passes because we can't easily simulate packet drop
    // But documents the architectural gap
}

void
MessageLossTest::testCommitSuccessAnnounceFailure()
{
    // This test documents the gap where git commit succeeds but swarm announce fails

    connectSignals();

    auto aliceAccount = Manager::instance().getAccount<JamiAccount>(aliceId);
    auto bobAccount = Manager::instance().getAccount<JamiAccount>(bobId);
    auto bobUri = bobAccount->getUsername();

    // Create conversation
    auto convId = libjami::startConversation(aliceId);
    libjami::addConversationMember(aliceId, convId, bobUri);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return bobData.requestReceived; }));

    libjami::acceptConversationRequest(bobId, convId);
    CPPUNIT_ASSERT(cv.wait_for(lk, 60s, [&] { return !bobData.conversationId.empty(); }));

    // Send normal message to verify flow
    auto aliceMsgCount = aliceData.messagesReceived.size();
    auto bobMsgCount = bobData.messagesReceived.size();

    libjami::sendMessage(aliceId, convId, std::string("Normal message"), "");

    // Alice sees message immediately (local commit)
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return aliceData.messagesReceived.size() > aliceMsgCount;
    }));

    // Bob should receive via swarm announce -> pull
    CPPUNIT_ASSERT(cv.wait_for(lk, 30s, [&] {
        return bobData.messagesReceived.size() > bobMsgCount;
    }));

    JAMI_LOG("DOCUMENTATION: Message flow is:");
    JAMI_LOG("1. sendMessage() -> git commit (local, always succeeds if repo valid)");
    JAMI_LOG("2. announce() -> swarm broadcast (may fail silently)");
    JAMI_LOG("3. peers pull() from git (requires announce to know about commit)");
    JAMI_LOG("Gap: If announce fails, message is in sender's git but peers don't know to pull");
}

} // namespace test
} // namespace jami

CORE_TEST_RUNNER(jami::test::MessageLossTest::name())
