include ../build_config.mk

OBJS += ../src/net/link.o ../src/net/fde.o ../src/util/log.o ../src/util/bytes.o
CFLAGS += -I../src
EXES = ssdb-bench ssdb-dump ssdb-repair rocksdb-import

all: ssdb-bench.o ssdb-dump.o ssdb-repair.o rocksdb-import.o ssdb-migrate.o
	${CXX} -o ssdb-bench ssdb-bench.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o ssdb-dump ssdb-dump.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o ssdb-repair ssdb-repair.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o rocksdb-import rocksdb-import.o ${OBJS} ${UTIL_OBJS} ${CLIBS}
	${CXX} -o ssdb-migrate ssdb-migrate.o ../api/cpp/libssdb-client.a ../src/util/libutil.a

ssdb-migrate.o: ssdb-migrate.cpp
	${CXX} ${CFLAGS} -I../api/cpp -c ssdb-migrate.cpp
ssdb-bench.o: ssdb-bench.cpp
	${CXX} ${CFLAGS} -c ssdb-bench.cpp
ssdb-dump.o: ssdb-dump.cpp
	${CXX} ${CFLAGS} -c ssdb-dump.cpp
ssdb-repair.o: ssdb-repair.cpp
	${CXX} ${CFLAGS} -c ssdb-repair.cpp
rocksdb-import.o: rocksdb-import.cpp
	${CXX} ${CFLAGS} -c rocksdb-import.cpp

clean:
	rm -f *.exe *.exe.stackdump *.o ${EXES}
	rm -rf _cpy_

