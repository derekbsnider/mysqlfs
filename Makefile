all:
    make mysqlfs

mysqlfs: mysqlfs.cpp
    g++ -D_FILE_OFFSET_BITS=64 mysqlfs.cpp -o mysqlfs -lfuse -lmysqlclient

test:   mysqlfs
    ./mysqlfs -H localhost -u root -p MyR0Ot\! -o direct_io mnt

stop:
    fusermount -u mnt
