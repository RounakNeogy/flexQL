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

static const long long DEFAULT_INSERT_ROWS = 10000000LL;
static const int DEFAULT_CLIENTS = 4;
static const int INSERT_BATCH_SIZE = 100000;

struct QueryStats {
    long long rows = 0;
};

static void maybe_print_insert_progress(long long current_rows, long long target_rows,
                                        int &next_progress_mark) {
    while (target_rows > 0 && next_progress_mark <= 10 &&
           current_rows >= (target_rows * next_progress_mark) / 10) {
        cout << "Insert progress: " << (next_progress_mark * 10) << "% ("
             << current_rows << "/" << target_rows << " rows)\n";
        next_progress_mark++;
    }
}

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

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool run_query(FlexQL *db, const string &sql, const string &label) {
    QueryStats stats;
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " | rows=" << stats.rows << " | " << elapsed << " ms\n";
    return true;
}

static bool query_rows(FlexQL *db, const string &sql, vector<string> &out_rows) {
    struct Collector {
        vector<string> rows;
    } collector;

    auto cb = [](void *data, int argc, char **argv, char **azColName) -> int {
        (void)azColName;
        Collector *c = static_cast<Collector*>(data);
        string row;
        for (int i = 0; i < argc; ++i) {
            if (i > 0) {
                row += "|";
            }
            row += (argv[i] ? argv[i] : "NULL");
        }
        c->rows.push_back(row);
        return 0;
    };

    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    out_rows = collector.rows;
    return true;
}

static bool assert_rows_equal(const string &label, const vector<string> &actual,
                              const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "):\n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "):\n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool expect_query_failure(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc == FLEXQL_OK) {
        cout << "[FAIL] " << label << " (expected failure, got success)\n";
        return false;
    }
    if (errMsg) {
        flexql_free(errMsg);
    }
    cout << "[PASS] " << label << "\n";
    return true;
}

static bool assert_row_count(const string &label, const vector<string> &rows,
                             size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size()
         << ")\n";
    return false;
}

static bool run_data_level_unit_tests(FlexQL *db) {
    cout << "\n[[...Running Unit Tests...]]\n\n";

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_USERS(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_USERS"));
    record(run_exec(db, "DELETE FROM TEST_USERS;", "DELETE TEST_USERS"));

    auto insert_test_user = [&](long long id, const string &name, long long balance,
                                long long expires_at) -> bool {
        stringstream ss;
        ss << "INSERT INTO TEST_USERS VALUES (" << id << ", '" << name << "', " << balance << ", "
           << expires_at << ");";
        return run_exec(db, ss.str(), "INSERT TEST_USERS ID=" + to_string(id));
    };

    record(insert_test_user(1, "Alice", 1200, 1893456000));
    record(insert_test_user(2, "Bob", 450, 1893456000));
    record(insert_test_user(3, "Carol", 2200, 1893456000));
    record(insert_test_user(4, "Dave", 800, 1893456000));

    vector<string> rows;

    bool q0 = query_rows(db, "SELECT * FROM TEST_USERS;", rows);
    record(q0);
    if (q0) {
        record(assert_rows_equal("Basic SELECT * validation", rows,
                                 {"1|Alice|1200|1893456000", "2|Bob|450|1893456000",
                                  "3|Carol|2200|1893456000", "4|Dave|800|1893456000"}));
    }

    bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", rows);
    record(q1);
    if (q1) {
        record(assert_rows_equal("Single-row value validation", rows, {"Bob|450"}));
    }

    bool q2 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000;", rows);
    record(q2);
    if (q2) {
        record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
    }

    bool q4 = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", rows);
    record(q4);
    if (q4) {
        record(assert_row_count("Empty result-set validation", rows, 0));
    }

    record(run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_ORDERS(ORDER_ID DECIMAL, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_ORDERS"));
    record(run_exec(db, "DELETE FROM TEST_ORDERS;", "DELETE TEST_ORDERS"));

    record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (101, 1, 50, 1893456000);",
                    "INSERT TEST_ORDERS ORDER_ID=101"));
    record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (102, 1, 150, 1893456000);",
                    "INSERT TEST_ORDERS ORDER_ID=102"));
    record(run_exec(db, "INSERT INTO TEST_ORDERS VALUES (103, 3, 500, 1893456000);",
                    "INSERT TEST_ORDERS ORDER_ID=103"));

    bool q7 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT > 900;",
            rows);
    record(q7);
    if (q7) {
        record(assert_row_count("Join with no matches validation", rows, 0));
    }

    record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;",
                                "Invalid SQL should fail"));
    record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    int passed_tests = total_tests - failed_tests;
    cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_multi_client_insert_benchmark(FlexQL *db, long long target_rows, int client_count) {
    if (!run_exec(
            db,
            "CREATE TABLE IF NOT EXISTS BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE BIG_USERS")) {
        return false;
    }
    if (!run_exec(db, "DELETE FROM BIG_USERS;", "DELETE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting multi-client insertion benchmark for " << target_rows
         << " rows across " << client_count << " clients...\n";

    atomic<long long> inserted_rows{0};
    atomic<bool> failed{false};
    mutex err_mu;
    mutex progress_mu;
    string first_error;
    vector<thread> workers;
    int next_progress_mark = 1;

    auto bench_start = high_resolution_clock::now();

    for (int client_id = 0; client_id < client_count; ++client_id) {
        workers.emplace_back([&, client_id]() {
            FlexQL *worker_db = nullptr;
            if (!open_db(&worker_db)) {
                lock_guard<mutex> lock(err_mu);
                failed = true;
                if (first_error.empty()) {
                    first_error = "client open failed";
                }
                return;
            }

            for (long long base = client_id; base < target_rows && !failed.load();
                 base += (long long)INSERT_BATCH_SIZE * client_count) {
                stringstream ss;
                ss << "INSERT INTO BIG_USERS VALUES ";

                int in_batch = 0;
                for (long long offset = 0; offset < INSERT_BATCH_SIZE; ++offset) {
                    long long global_row = base + offset * client_count;
                    if (global_row >= target_rows) {
                        break;
                    }
                    long long id = global_row + 1;
                    if (in_batch > 0) {
                        ss << ",";
                    }
                    ss << "(" << id
                       << ", 'user" << id << "'"
                       << ", 'user" << id << "@mail.com'"
                       << ", " << (1000.0 + (id % 10000))
                       << ", 1893456000)";
                    in_batch++;
                }
                ss << ";";

                if (in_batch == 0) {
                    continue;
                }

                char *errMsg = nullptr;
                if (flexql_exec(worker_db, ss.str().c_str(), nullptr, nullptr, &errMsg) != FLEXQL_OK) {
                    lock_guard<mutex> lock(err_mu);
                    failed = true;
                    if (first_error.empty()) {
                        first_error = errMsg ? errMsg : "unknown error";
                    }
                    if (errMsg) {
                        flexql_free(errMsg);
                    }
                    break;
                }
                {
                    long long current_rows = inserted_rows.fetch_add(in_batch) + in_batch;
                    lock_guard<mutex> lock(progress_mu);
                    maybe_print_insert_progress(current_rows, target_rows, next_progress_mark);
                }
            }

            flexql_close(worker_db);
        });
    }

    for (auto &worker : workers) {
        worker.join();
    }

    auto bench_end = high_resolution_clock::now();
    if (failed.load()) {
        cout << "[FAIL] Multi-client INSERT benchmark -> " << first_error << "\n";
        return false;
    }

    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (inserted_rows.load() * 1000LL / elapsed)
                                         : inserted_rows.load();

    cout << "[PASS] Multi-client INSERT benchmark complete\n";
    cout << "Rows inserted: " << inserted_rows.load() << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    return inserted_rows.load() == target_rows;
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

static bool run_select_benchmarks(long long target_rows, int client_count) {
    cout << "\nStarting multi-client SELECT benchmarks...\n";

    vector<string> select_sqls;
    vector<string> where_sqls;
    for (int i = 0; i < client_count; ++i) {
        long long probe_id = ((long long)i * target_rows / client_count) + 1;
        if (probe_id > target_rows) {
            probe_id = target_rows;
        }
        select_sqls.push_back("SELECT ID, NAME, BALANCE FROM BIG_USERS;");

        stringstream ss;
        ss << "SELECT ID, NAME, BALANCE FROM BIG_USERS WHERE ID = " << probe_id << ";";
        where_sqls.push_back(ss.str());
    }

    if (!run_concurrent_query_benchmark("Multi-client SELECT benchmark (full table scan)",
                                        client_count, select_sqls)) {
        return false;
    }

    if (!run_concurrent_query_benchmark("Multi-client WHERE benchmark (primary key equality)",
                                        client_count, where_sqls)) {
        return false;
    }

    return true;
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    long long insert_rows = DEFAULT_INSERT_ROWS;
    int client_count = DEFAULT_CLIENTS;
    bool run_unit_tests_only = false;

    if (argc > 1) {
        string arg1 = argv[1];
        if (arg1 == "--unit-test") {
            run_unit_tests_only = true;
        } else {
            insert_rows = atoll(argv[1]);
            if (insert_rows <= 0) {
                cout << "Invalid row count. Use a positive integer or --unit-test.\n";
                return 1;
            }
        }
    }

    if (argc > 2) {
        client_count = atoi(argv[2]);
        if (client_count <= 0) {
            cout << "Invalid client count. Use a positive integer.\n";
            return 1;
        }
    }

    if (!open_db(&db)) {
        cout << "Cannot open FlexQL. Start ./bin/flexql_server 9000 first.\n";
        return 1;
    }

    cout << "Connected to FlexQL server\n";

    if (run_unit_tests_only) {
        bool ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    cout << "Running SQL subset checks plus multi-client insert/select/where benchmarks...\n";
    cout << "Target insert rows: " << insert_rows << "\n";
    cout << "Client count: " << client_count << "\n\n";

    if (!run_multi_client_insert_benchmark(db, insert_rows, client_count)) {
        flexql_close(db);
        return 1;
    }

    if (!run_select_benchmarks(insert_rows, client_count)) {
        flexql_close(db);
        return 1;
    }

    if (!run_data_level_unit_tests(db)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}
