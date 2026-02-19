/*
 *  Copyright (C) 2024-2026 Savoir-faire Linux Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

/**
 * @file create_test_accounts.cpp
 * @brief Tool to pre-generate test accounts with DHT announcements
 *
 * This tool creates Jami accounts, waits for DHT announcements,
 * then exports the archives. The archives can be used by unit tests
 * to avoid the slow DHT announcement process during test setup.
 *
 * Usage: create_test_accounts [output_directory]
 *   Default output: test/unitTest/actors/archives/
 */

#include "jami.h"
#include "manager.h"
#include "jamidht/jamiaccount.h"
#include "account_const.h"
#include "fileutils.h"

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include <map>
#include <filesystem>

using namespace std::chrono_literals;

std::mutex mtx;
std::condition_variable cv;
std::map<std::string, std::atomic_bool> accountsAnnounced;

void waitForAnnouncement(const std::vector<std::string>& accountIds, std::chrono::seconds timeout) {
    std::map<std::string, std::shared_ptr<libjami::CallbackWrapperBase>> handlers;

    handlers.insert(libjami::exportable_callback<libjami::ConfigurationSignal::VolatileDetailsChanged>(
        [&](const std::string& accountId, const std::map<std::string, std::string>& details) {
            try {
                if (details.at(libjami::Account::VolatileProperties::DEVICE_ANNOUNCED) == "true") {
                    accountsAnnounced[accountId] = true;
                    cv.notify_all();
                }
            } catch (...) {}
        }));

    libjami::registerSignalHandlers(handlers);

    std::unique_lock<std::mutex> lk(mtx);
    auto deadline = std::chrono::steady_clock::now() + timeout;

    cv.wait_until(lk, deadline, [&]() {
        for (const auto& id : accountIds) {
            if (!accountsAnnounced[id].load()) return false;
        }
        return true;
    });

    libjami::unregisterSignalHandlers();
}

int main(int argc, char* argv[]) {
    std::cout << "=== Jami Test Account Generator ===" << std::endl;

    // Output directory
    std::string outputDir = "test/unitTest/actors/archives";
    if (argc > 1) {
        outputDir = argv[1];
    }

    std::filesystem::create_directories(outputDir);

    // Initialize daemon
    libjami::init(libjami::InitFlag(libjami::LIBJAMI_FLAG_CONSOLE_LOG));
    if (!libjami::start("jami-sample.yml")) {
        std::cerr << "Failed to start daemon" << std::endl;
        return 1;
    }

    // Account names
    std::vector<std::string> names = {"alice", "bob", "carol", "dave"};
    std::vector<std::string> accountIds;

    // Create accounts
    for (const auto& name : names) {
        std::cout << "Creating account: " << name << std::endl;

        std::map<std::string, std::string> details = libjami::getAccountTemplate("RING");
        details[libjami::Account::ConfProperties::TYPE] = "RING";
        details[libjami::Account::ConfProperties::ALIAS] = name;
        details[libjami::Account::ConfProperties::DISPLAYNAME] = name;
        details[libjami::Account::ConfProperties::UPNP_ENABLED] = "true";
        details[libjami::Account::ConfProperties::ARCHIVE_PASSWORD] = "";

        auto accountId = jami::Manager::instance().addAccount(details);
        accountIds.push_back(accountId);
        accountsAnnounced[accountId] = false;

        std::cout << "  -> Account ID: " << accountId << std::endl;
    }

    // Wait for DHT announcement
    std::cout << "\nWaiting for DHT announcement (up to 10 minutes)..." << std::endl;
    waitForAnnouncement(accountIds, 600s);

    // Check which accounts were announced
    int announced = 0;
    for (const auto& id : accountIds) {
        if (accountsAnnounced[id].load()) announced++;
    }
    std::cout << "Announced: " << announced << "/" << accountIds.size() << std::endl;

    // Export archives
    std::cout << "\nExporting archives to: " << outputDir << std::endl;
    for (size_t i = 0; i < names.size(); i++) {
        auto account = jami::Manager::instance().getAccount<jami::JamiAccount>(accountIds[i]);
        if (account) {
            std::string archivePath = outputDir + "/" + names[i] + ".gz";
            if (account->exportArchive(archivePath)) {
                std::cout << "  Exported: " << archivePath << std::endl;
            } else {
                std::cerr << "  FAILED: " << archivePath << std::endl;
            }
        }
    }

    // Cleanup
    std::cout << "\nCleaning up..." << std::endl;
    for (const auto& id : accountIds) {
        jami::Manager::instance().removeAccount(id, true);
    }

    std::this_thread::sleep_for(2s);
    libjami::fini();

    std::cout << "\nDone! Archives saved to: " << outputDir << std::endl;
    return 0;
}
