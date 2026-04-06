#include "flexql.h"
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <string>

struct FlexQL {
    int sock;
};

int flexql_open(const char *host, int port, FlexQL **outDb) {
    FlexQL *db = (FlexQL*)malloc(sizeof(FlexQL));

    db->sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    inet_pton(AF_INET, host, &serv_addr.sin_addr);

    if (connect(db->sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        free(db);
        return FLEXQL_ERROR;
    }

    *outDb = db;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    close(db->sock);
    free(db);
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
) {
    if (send(db->sock, sql, strlen(sql), MSG_NOSIGNAL) < 0) {
        if (errmsg) {
            const char *msg = "send failed (socket closed by server)";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    std::string pending;
    char buffer[4096 + 1];
    int valread;
    bool done = false;
    bool hasError = false;
    std::string errorText;

    while (!done && (valread = read(db->sock, buffer, 4096)) > 0) {
        buffer[valread] = '\0';
        pending.append(buffer, valread);

        size_t newlinePos;
        while ((newlinePos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, newlinePos);
            pending.erase(0, newlinePos + 1);

            if (line == "END") {
                done = true;
                break;
            }

            if (line.rfind("ERROR:", 0) == 0) {
                hasError = true;
                errorText = line;
                continue;
            }

            if (callback && line.rfind("ROW ", 0) == 0) {
                std::string rowValue = line.substr(4);
                char *argv[1];
                char *col[1];
                argv[0] = (char*)rowValue.c_str();
                col[0] = (char*)"row";
                callback(arg, 1, argv, col);
            }
        }
    }

    if (!done && valread < 0) {
        if (errmsg) {
            const char *msg = "read failed";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    if (!done) {
        if (errmsg) {
            const char *msg = "connection closed before END";
            *errmsg = (char*)malloc(strlen(msg) + 1);
            if (*errmsg) {
                strcpy(*errmsg, msg);
            }
        }
        return FLEXQL_ERROR;
    }

    if (hasError) {
        if (errmsg) {
            *errmsg = (char*)malloc(errorText.size() + 1);
            if (*errmsg) {
                strcpy(*errmsg, errorText.c_str());
            }
        }
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    free(ptr);
}