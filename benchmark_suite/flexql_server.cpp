#include <iostream>
#include <string>
#include <sqlite3.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>

#define PORT 9000
#define BUFFER_SIZE 4096

sqlite3 *db;

static int callback(void *client_socket, int argc, char **argv, char **azColName) {
    int sock = *(int*)client_socket;

    std::string row = "ROW ";
    for (int i = 0; i < argc; i++) {
        row += (argv[i] ? argv[i] : "NULL");
        if (i != argc - 1) row += " ";
    }
    row += "\n";

    send(sock, row.c_str(), row.size(), 0);
    return 0;
}

static void send_result(int client_socket, const std::string &message) {
    send(client_socket, message.c_str(), message.size(), 0);
}

int main() {
    int server_fd, client_socket;
    struct sockaddr_in address;
    int addrlen = sizeof(address);

    // Start with a fresh DB file for each server run since DROP is unsupported.
    std::remove("flexql.db");
    sqlite3_open("flexql.db", &db);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, 3);

    std::cout << "FlexQL Server running on port " << PORT << std::endl;

    while (1) {
        client_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        std::cout << "Client connected\n";

        std::string pending;
        char buffer[BUFFER_SIZE];
        ssize_t bytes_read;

        while ((bytes_read = read(client_socket, buffer, BUFFER_SIZE)) > 0) {
            pending.append(buffer, bytes_read);

            size_t pos = std::string::npos;
            while ((pos = pending.find(';')) != std::string::npos) {
                std::string sql = pending.substr(0, pos + 1);
                pending.erase(0, pos + 1);

                char *errMsg = 0;
                if (sqlite3_exec(db, sql.c_str(), callback, &client_socket, &errMsg) != SQLITE_OK) {
                    std::string err = "ERROR: ";
                    err += (errMsg ? errMsg : "unknown sqlite error");
                    err += "\nEND\n";
                    send_result(client_socket, err);
                    sqlite3_free(errMsg);
                } else {
                    send_result(client_socket, "OK\nEND\n");
                }
            }
        }

        close(client_socket);
    }

    sqlite3_close(db);
}