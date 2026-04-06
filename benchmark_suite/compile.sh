g++ flexql_server.cpp -o server -lsqlite3 && \
g++ flexql.cpp benchmark_flexql.cpp -o benchmark -lsqlite3