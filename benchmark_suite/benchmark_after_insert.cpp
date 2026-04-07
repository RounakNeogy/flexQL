#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "flexql.h"

using namespace std;
using namespace std::chrono;

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool open_db(FlexQL **db) {
    return flexql_open("127.0.0.1", 9000, db) == FLEXQL_OK;
}

static bool run_concurrent_query_benchmark(const string &label, int client_count,
                                           const vector<string> &sqls) {
    atomic<long long> total_rows{0};
    atomic<bool> failed{false};
    mutex err_mu;
    string first_error;
    vector<thread> workers;
    auto bench_start = high_resolution_clock::now();

    for (int i = 0; i < client_count; ++i) {
        workers.emplace_back([&, i]() {
            FlexQL *worker_db = nullptr;
            if (!open_db(&worker_db)) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) {
                    first_error = "client open failed";
                }
                return;
            }

            QueryStats stats;
            char *errMsg = nullptr;
            if (flexql_exec(worker_db, sqls[i].c_str(), count_rows_callback, &stats, &errMsg) != FLEXQL_OK) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) {
                    first_error = errMsg ? errMsg : "unknown error";
                }
                if (errMsg) {
                    flexql_free(errMsg);
                }
                flexql_close(worker_db);
                return;
            }

            total_rows += stats.rows;
            flexql_close(worker_db);
        });
    }

    for (auto &worker : workers) {
        worker.join();
    }

    auto bench_end = high_resolution_clock::now();
    if (failed.load()) {
        cout << "[FAIL] " << label << " -> " << first_error << "\n";
        return false;
    }

    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    cout << "[PASS] " << label << " | clients=" << client_count
         << " | total_rows=" << total_rows.load()
         << " | elapsed=" << elapsed << " ms\n";
    return true;
}

int main(int argc, char **argv) {
    int client_count = 1;
    long long target_rows = 100000; // default assumption matching the insert defaults

    if (argc > 1) {
        target_rows = atoll(argv[1]);
        if (target_rows <= 0) {
            cout << "Invalid row count. Use a positive integer.\n";
            return 1;
        }
    }

    if (argc > 2) {
        client_count = atoi(argv[2]);
        if (client_count <= 0) {
            cout << "Invalid client count. Use a positive integer.\n";
            return 1;
        }
    }

    cout << "Running Post-Insert Query Benchmarks...\n";
    cout << "Assuming table 'BIG_USERS' contains " << target_rows << " rows.\n";
    cout << "Client count: " << client_count << "\n\n";

    vector<vector<string>> test_queries;
    vector<string> test_labels;

    auto add_test = [&](const string& label, const string& sql_pattern, bool use_probe) {
        vector<string> sqls;
        for (int i = 0; i < client_count; ++i) {
            long long probe_id = ((long long)i * target_rows / client_count) + 1;
            if (probe_id > target_rows) probe_id = target_rows;
            
            string sql = sql_pattern;
            if (use_probe) {
                size_t pos = sql.find("<PROBE>");
                if (pos != string::npos) {
                    sql.replace(pos, 7, to_string(probe_id));
                }
            }
            sqls.push_back(sql);
        }
        test_queries.push_back(sqls);
        test_labels.push_back(label);
    };

    // 1. Full Table Scan
    add_test("Full Table Select", "SELECT * FROM BIG_USERS;", false);

    // 2. Exact Match (ID = X) -> uses primary index
    add_test("Primary Index Lookup (ID = X)", "SELECT ID, NAME, BALANCE FROM BIG_USERS WHERE ID = <PROBE>;", true);

    // 3. Inequality (ID != 1)
    add_test("Inequality Check (ID != 1)", "SELECT ID, NAME FROM BIG_USERS WHERE ID != 1;", false);

    // 4. Greater Than (ID > X)
    add_test("Greater Than (ID > X)", "SELECT ID FROM BIG_USERS WHERE ID > <PROBE>;", true);

    // 5. Less Than (ID < X)
    add_test("Less Than (ID < X)", "SELECT ID FROM BIG_USERS WHERE ID < <PROBE>;", true);

    // 6. Greater Than or Equal (ID >= X)
    add_test("Greater Than Eq (ID >= X)", "SELECT ID FROM BIG_USERS WHERE ID >= <PROBE>;", true);

    // 7. Less Than or Equal (ID <= X)
    add_test("Less Than Eq (ID <= X)", "SELECT ID FROM BIG_USERS WHERE ID <= <PROBE>;", true);

    // 8. Balance Match (BALANCE = X)
    add_test("Non-Index Column Match (BALANCE = 1500)", "SELECT ID FROM BIG_USERS WHERE BALANCE = 1500;", false);

    // 9. Balance Greater Than (BALANCE > X)
    add_test("Non-Index Col Greater (BALANCE > 9000)", "SELECT ID FROM BIG_USERS WHERE BALANCE > 9000;", false);

    // 10. String Match (EMAIL = X)
    add_test("String Exact Match (EMAIL = 'user1@mail.com')", "SELECT ID FROM BIG_USERS WHERE EMAIL = 'user1@mail.com';", false);

    // 11. String Inequality (NAME != X)
    add_test("String Inequality (NAME != 'user1')", "SELECT ID FROM BIG_USERS WHERE NAME != 'user1';", false);

    for (size_t i = 0; i < test_queries.size(); ++i) {
        if (!run_concurrent_query_benchmark(test_labels[i], client_count, test_queries[i])) {
            cout << "Benchmark suite failed at: " << test_labels[i] << "\n";
            return 1;
        }
    }

    cout << "\nAll benchmarks completed successfully!\n";
    return 0;
}
