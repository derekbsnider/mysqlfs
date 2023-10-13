# mysqlfs
Uses FUSE to provide a virtual filesystem interface to a MySQL database.

Requires libfuse-dev and libmysqlclient-dev

The root directory contains all available databases as directories, and
within each database directory will be subdirectories of all the tables for
that corresponding database.

Within each table subdirectory there are some files containing the basic
information for the table:

  count -- contains an integer of the number of rows in the table
  schema -- contains the MySQL schema reported by DESCRIBE <table>

To perform a standard MySQL query, a symlink is created with the name of
the query and a target of the query itself:

  userlist -> SELECT user FROM mysql.user


File extensions

Common file extensions are supported to export the data into the desired
format:

  txt -- plain text, space separated values
  json -- JSON encoded data
  csv -- comma separated values


File open modes:
  O_RDONLY -- read only, perform (SELECT/DESC/SHOW) query when opening file
  O_WRONLY -- write only, delete all rows from table when opening file, should use INSERT when writing
  O_RDWR -- read/write, do nothing upon opening, should use REPLACE or UPDATE when writing, not sure how to mix read and write queries
  O_APPEND -- do nothing upon opening, should only allow INSERT when writing
  O_TRUNC - if open for write, delete all rows from table when opening file
