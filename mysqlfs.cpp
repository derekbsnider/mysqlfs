// MySQL represented as a FUSE filesystem
// (c)2015 Derek Snider <derekbsnider@gmail.com>

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <getopt.h>
#include <mysql/mysql.h>
#include <syslog.h>
#include <iostream>
#include <string>
#include <map>

using namespace std;

static MYSQL realsql;
static MYSQL *mysql = &realsql;
static MYSQL_RES *mysql_res;
static MYSQL_ROW mysql_row;

typedef enum { feUNKNOWN, feTXT, feCSV, feJSON, feHTML, feXML } fext_t;

class Extension
{
public:
	fext_t type;
	Extension() { type = feTXT; }
	Extension(string &s) { init(s); }
	void init(string &);
	string header();
	string footer();
	string rowStart();
	string rowEnd();
	string datum(const char *);
	string separator();
};

class fsQuery;

class fsNode
{
public:
	struct stat stbuf;
	fsQuery *qn;
	Extension ext;
	fsNode() { qn = NULL; memset(&stbuf, 0, sizeof(struct stat)); stbuf.st_mode = S_IFREG | 0444;  stbuf.st_nlink = 1; }
};

class fsDatabase: public fsNode
{
public:
	fsDatabase() {  memset(&stbuf, 0, sizeof(struct stat)); stbuf.st_mode = S_IFDIR | 0755;  stbuf.st_nlink = 2; }
};

class fsTable: public fsNode
{
public:
	fsTable() {  memset(&stbuf, 0, sizeof(struct stat)); stbuf.st_mode = S_IFDIR | 0755;  stbuf.st_nlink = 2; }
};

class fsQuery: public fsNode
{
public:
	bool _open;
	string query;
	string result;
	fsQuery() { _open = false; memset(&stbuf, 0, sizeof(struct stat)); stbuf.st_mode = S_IFLNK | 0755;  stbuf.st_nlink = 1; }
};

map<string, fsDatabase *> fsDatabases;
map<string, fsTable *> fsTables;
map<string, fsQuery *> fsQueries;
map<string, fsNode *> fsNodes;

void Extension::init(string &s)
{
	size_t dot = s.find_last_of(".");

	/**/ if ( !dot )					type = feUNKNOWN;
	else if ( !strcasecmp(s.c_str()+dot+1, "txt") )		type = feTXT;
	else if ( !strcasecmp(s.c_str()+dot+1, "csv") )		type = feCSV;
	else if ( !strcasecmp(s.c_str()+dot+1, "json") )	type = feJSON;
	else if ( !strcasecmp(s.c_str()+dot+1, "html") )	type = feHTML;
	else if ( !strcasecmp(s.c_str()+dot+1, "xml") )		type = feXML;
	else							type = feUNKNOWN;
}

string Extension::header()
{
	if ( type == feJSON ) return "[";
	return "";
}

string Extension::footer()
{
	if ( type == feJSON ) return "]\n";
	return "";
}

string Extension::rowStart()
{
	if ( type == feJSON ) return "[";
	return "";
}

string Extension::rowEnd()
{
	if ( type == feJSON ) return "]";
	return "\n";
}

string Extension::separator()
{
	switch(type)
	{
		default:	break;
		case feJSON:
		case feCSV:	return ",";
	}

	return " ";
}

string Extension::datum(const char *d)
{
	if ( type != feJSON && type != feCSV )
		return d;

	string ret("\"");

	if ( type == feJSON )
	{
		while ( *d )
		{
			if ( *d == '"' )
				ret += '\\';
			ret += *d;
			++d;
		}
	}
	else
	{
		while ( *d )
		{
			if ( *d == '"' )
				ret += '"';
			ret += *d;
			++d;
		}
	}


	ret.append("\"");

	return ret;
}

static int addQuery(const char *path, const char *query)
{
	string tostr(path);
	fsQuery *fq;

	fq = new fsQuery;
	fq->query = query;
	fq->ext.init(tostr);
	fsQueries[tostr] = fq;
	fsNodes[tostr] = fq;

	string rqpath = tostr.substr(0, tostr.find_last_of("/")+1);
	rqpath.append(fq->query);
	fsNode *fn = new fsNode;
	fn->qn = fq;
	fsNodes[rqpath] = fn;

	return 0;
}

static int delQuery(const char *path)
{
	map<string, fsQuery *>::iterator qi = fsQueries.find(path);
	map<string, fsNode *>::iterator ni;

	if ( qi == fsQueries.end() )
		return -1;

	ni = fsNodes.find(qi->second->query);

	delete qi->second;
	fsQueries.erase(qi);

	if ( ni != fsNodes.end() )
	{
		delete ni->second;
		fsNodes.erase(ni);
	}

	if ( (ni=fsNodes.find(path)) != fsNodes.end() )
		fsNodes.erase(ni);

	return 0;
}


static int getTables(string db)
{
	string path;
	string qpath;
	string query;
	map<string, fsTable *>::iterator fi;
	MYSQL_RES *res;
	MYSQL_ROW row;
	fsTable *ft;
	fsQuery *fq;
	fsNode *fn;

	mysql_select_db(mysql, db.substr(1).c_str());

	if ( !(res=mysql_list_tables(mysql, "")) )
	{
		cout << "Error: " << mysql_errno(mysql) << " -- " << mysql_error(mysql) << endl;
		return -1;
	}

	while ( (row=mysql_fetch_row(res)) )
	{
		path = db + "/";
		path.append(row[0]);
		if ( (fi=fsTables.find(path)) == fsTables.end() )
		{
			fsTables[path] = (ft = new fsTable);
			fsNodes[path] = ft;
#ifdef COUNT
			qpath = path + "/count";
			query = "SELECT COUNT(*) FROM " + db.substr(1) + '.';
			query.append(row[0]);
			addQuery(qpath.c_str(), query.c_str());
#endif
		}
	}

		
	return 0;
}

static int getDatabases()
{
	string path;
	map<string, fsDatabase *>::iterator fi;
	fsDatabase *fd;

	fsNodes["/"] = new fsDatabase;

	if ( !(mysql_res=mysql_list_dbs(mysql, "")) )
	{
		cout << "Error: " << mysql_errno(mysql) << " -- " << mysql_error(mysql) << endl;
		return -1;
	}

	while ( (mysql_row=mysql_fetch_row(mysql_res)) )
	{
		path = "/";
		path.append(mysql_row[0]);
		if ( (fi=fsDatabases.find(path)) == fsDatabases.end() )
		{
			fsDatabases[path] = (fd = new fsDatabase);
			fsNodes[path] = fd;
			getTables(path);
		}
	}
		
	return 0;
}

static int mysqlfs_getattr(const char *path, struct stat *stbuf)
{
	map<string, fsNode *>::iterator fi;

	syslog(LOG_INFO, "mysqlfs_getattr(%s)", path);

	if ( (fi=fsNodes.find(path)) == fsNodes.end() )
	{
		syslog(LOG_INFO, "mysqlfs_getattr(%s) returning ENOENT", path);
		memset(stbuf, 0, sizeof(struct stat));
		return -ENOENT;
	}

	memcpy(stbuf, fi->second, sizeof(struct stat));
	stbuf->st_mtime = time(0);
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();

#if 0
	if ( S_ISREG(stbuf->st_mode) )
	{
		fsQuery *fq = fi->second->qn;
		if ( fq )
			stbuf->st_size = fq->result.length();
	}
#endif	

	string type;

	if ( S_ISREG(stbuf->st_mode) )
		type = "REG";
	else
	if ( S_ISDIR(stbuf->st_mode) )
		type = "DIR";
	else
	if ( S_ISLNK(stbuf->st_mode) )
		type = "LNK";
	else
	if ( S_ISFIFO(stbuf->st_mode) )
		type = "FIFO";
	else
	if ( S_ISCHR(stbuf->st_mode) )
		type = "CHR";

	syslog(LOG_INFO, "mysqlfs_getattr(%s) returning type %s", path, type.c_str());
	return 0;
}

static int mysqlfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;

	syslog(LOG_INFO, "mysqlfs_readdir(%s)", path);

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);

	map<string, fsDatabase *>::iterator di;

	if ( !strcmp(path, "/") )
	{

		for ( di = fsDatabases.begin(); di != fsDatabases.end(); ++di )
			filler(buf, di->first.substr(1).c_str(), NULL, 0);
		return 0;
	}

	int plen = strlen(path);
	map<string, fsTable *>::iterator ti;

	if ( (di=fsDatabases.find(path)) != fsDatabases.end() )
	{
		for ( ti = fsTables.begin(); ti != fsTables.end(); ++ti )
			if ( !ti->first.compare(0, plen, path) )
				filler(buf, ti->first.substr(plen+1).c_str(), NULL, 0);
		return 0;
	}

	if ( (ti=fsTables.find(path)) != fsTables.end() )
	{
		map<string, fsQuery *>::iterator qi;

		for ( qi = fsQueries.begin(); qi != fsQueries.end(); ++qi )
			if ( !qi->first.compare(0, plen, path) )
				filler(buf, qi->first.substr(plen+1).c_str(), NULL, 0);
		return 0;
	}

	return -ENOENT;
}

bool isReadQuery(string &q)
{
	if ( !strncasecmp(q.c_str(), "SELECT", 6)
	||   !strncasecmp(q.c_str(), "DESC", 4)
	||   !strncasecmp(q.c_str(), "SHOW", 4) )
		return true;
	syslog(LOG_INFO, "isReadQuery(%s) returning false", q.c_str());
	return false;
}

bool isWriteQuery(string &q)
{
	if ( !strncasecmp(q.c_str(), "INSERT", 6)
	||   !strncasecmp(q.c_str(), "REPLACE", 7)
	||   !strncasecmp(q.c_str(), "UPDATE", 6) )
		return true;
	syslog(LOG_INFO, "isWriteQuery(%s) returning false", q.c_str());
	return false;
}

bool isAppendQuery(string &q)
{
	if ( !strncasecmp(q.c_str(), "INSERT", 6) )
		return true;
	return false;
}

static int mysqlfs_open(const char *path, struct fuse_file_info *fi)
{
	map<string, fsNode *>::iterator ni;
	fsQuery *fq;

	syslog(LOG_INFO, "mysqlfs_open(%s)", path);

	if ( (ni=fsNodes.find(path)) == fsNodes.end() )
		return -ENOENT;

	// must point to a query
	if ( !(fq=ni->second->qn) )
		return -EACCES;

	// read only
	if ( (fi->flags & 3) == O_RDONLY && !isReadQuery(fq->query) )
		return -EACCES;
	// write only
	if ( (fi->flags & 3) == O_WRONLY && !isWriteQuery(fq->query) )
		return -EACCES;
	// read and/or write
	if ( (fi->flags & 3) == O_RDWR && !isReadQuery(fq->query) && !isWriteQuery(fq->query) )
		return -EACCES;
	// append allow insert only
	if ( (fi->flags & O_APPEND) && !isAppendQuery(fq->query) )
		return -EACCES;

	// file type should be regular
	if ( !S_ISREG(ni->second->stbuf.st_mode) )
		return -EACCES;

	syslog(LOG_INFO, "query: %s", fq->query.c_str());
	fq->_open = false;

	if ( (fi->flags & 3) == O_WRONLY || (fi->flags & O_TRUNC) )
	{
		// delete table
	}

	fq->result = string("");

	// perform read query upon opening
	if ( (fi->flags & 3) != O_WRONLY && isReadQuery(fq->query) )
	{
		if ( mysql_query(mysql, fq->query.c_str()) != 0 )
		{
			syslog(LOG_INFO, "query failed: %s", mysql_error(mysql));
			return -EACCES;
		}

		if ( !(mysql_res=mysql_store_result(mysql)) )
		{
			syslog(LOG_INFO, "mysql_store_result failed: %s", mysql_error(mysql));
			return -EACCES;
		}
		syslog(LOG_INFO, "query successful");
	
		int nfields = mysql_num_fields(mysql_res);

		if ( fq->ext.type == feCSV )
		{
			MYSQL_FIELD *fields = mysql_fetch_fields(mysql_res);

			for ( int i = 0; i < nfields; ++i )
			{
				fq->result.append(fields[i].name);
				if ( i+1 < nfields && fields[i+1].name )
					fq->result.append(fq->ext.separator());
			}
			fq->result.append("\n");
		}
		else
			fq->result.append(fq->ext.header());

		while ( (mysql_row=mysql_fetch_row(mysql_res)) )
		{
			fq->result.append(fq->ext.rowStart());
			for ( int i = 0; i < nfields && mysql_row[i]; ++i )
			{
				fq->result.append(fq->ext.datum(mysql_row[i]));
				if ( i+1 < nfields && mysql_row[i+1] )
					fq->result.append(fq->ext.separator());
			}
			fq->result.append(fq->ext.rowEnd());
		}
		fq->result.append(fq->ext.footer());
		syslog(LOG_INFO, "mysqlfs_open: result: %s", fq->result.c_str());
		mysql_free_result(mysql_res);
	}
	fq->_open = true;	

	return 0;
}

static int mysqlfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;

	map<string, fsNode *>::iterator ni;
	fsQuery *fq;

	syslog(LOG_INFO, "mysqlfs_read(%s, %lu, %ld)", path, size, offset);

	if ( (ni=fsNodes.find(path)) == fsNodes.end() )
		return -ENOENT;

	// must point to a query
	if ( !(fq=ni->second->qn) )
		return -EACCES;

	if ( !fq->_open )
		return -EBADF;

	len = fq->result.length();
	syslog(LOG_INFO, "mysqlfs_read: len(%lu) result: %s", len, fq->result.c_str());

	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		syslog(LOG_INFO, "mysqlfs_read: memcpy(buf, %ld, %lu)", offset, size);
		memcpy(buf, fq->result.c_str() + offset, size);
	} else
		size = 0;

	return size;
}

static int mysqlfs_unlink(const char *path)
{
	return delQuery(path);
}


static int mysqlfs_readlink(const char *path, char *buf, size_t size)
{
	map<string, fsQuery *>::iterator qi = fsQueries.find(path);

	if ( qi == fsQueries.end() )
		return -1;

	strcpy(buf, qi->second->query.c_str());

	return 0;
}


static int mysqlfs_symlink(const char *from, const char *to)
{
	syslog(LOG_INFO, "mysqlfs_symlink(%s, %s)", from, to);

	return addQuery(to, from);
}

static int mysqlfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	map<string, fsNode *>::iterator ni;
	fsQuery *fq;

	syslog(LOG_INFO, "mysqlfs_read(%s, %lu, %ld)", path, size, offset);

	if ( (ni=fsNodes.find(path)) == fsNodes.end() )
		return -ENOENT;

	// must point to a query
	if ( !(fq=ni->second->qn) )
		return -EACCES;

	if ( !fq->_open )
		return -EBADF;

	// only allow writes if the query is a write query (should already be checked upon open)
	if ( !isWriteQuery(fq->query) )
		return -EACCES;

	// at this point we need to determine if we have a complete row of data, and then perform a query, formatted using fq->query
	// for example, if fq->query is "INSERT INTO table (test1, test2) VALUES (%s, %s)", then we need to receive two fields first
	// parsed based on file extension type, generate a query filling in the values, and then perform the query

	return size;
}


static struct fuse_operations mysqlfs_oper;
static struct option long_options[] = {
        {"host", required_argument, 0, 'H'},
        {"port", required_argument, 0, 'P'},
        {"user", required_argument, 0, 'u'},
        {"password", required_argument, 0, 'p'},
        {0, 0, 0, 0}
};

static int myopt_long(int argc, char **argv, struct option *lopt, int *start)
{
	int i, j;

	i = start ? *start : 1;

	for ( ; i < argc; ++i )
	{
		if ( argv[i][0] != '-' )
			continue;
		for ( j = 0; lopt[j].name; ++j )
		{
			if ( (argv[i][1] == '-' && !strcmp(argv[i]+2, lopt[j].name))
			||    argv[i][1] == lopt[j].val )
			{
				if ( start )
					*start = i;
				return lopt[j].val;
                        }
		}
	}
	return -1;
}

int main(int argc, char *argv[])
{
	int opt, opt_pos = 1;
	char *p, *user, *port, *host, *pass;

	mysqlfs_oper.getattr	= mysqlfs_getattr;
	mysqlfs_oper.readdir	= mysqlfs_readdir;
	mysqlfs_oper.open	= mysqlfs_open;
	mysqlfs_oper.read	= mysqlfs_read;
	mysqlfs_oper.readlink	= mysqlfs_readlink;
	mysqlfs_oper.symlink	= mysqlfs_symlink;
	mysqlfs_oper.unlink	= mysqlfs_unlink;


	while ( argv[1] )
	{
		if ( (opt = myopt_long(argc, argv, long_options, &opt_pos)) != -1 )
		{
			p = argv[opt_pos+1];
			switch(opt)
			{
				case 'H':
					host = p;
					break;
				case 'P':
					port = p;
					break;
				case 'u':
					user = p;
					break;
				case 'p':
					pass = p;
					break;
			}
			if ( argc > opt_pos )
			{
				memmove(argv+opt_pos, argv+opt_pos+2, sizeof(char *)*(argc-opt_pos+2));
				argv[argc-1] = NULL;
				argv[argc] = NULL;
				argc -= 2;
			}
		}
		else
			break;
	}

	if ( user && host && pass )
	{
		mysql_init(mysql);
		if ( !mysql_real_connect(mysql, host, user, pass, "mysql", 0, NULL, 0) )
		{
			printf("Failed to connect: %d %s\n", mysql_errno(mysql), mysql_error(mysql));
			return 0;
		}
	}
	else
	{
		puts("No MYSQL credentials");
		return 0;
	}

	getDatabases();

	openlog("mysqlfs", LOG_PID, LOG_USER);

	return fuse_main(argc, argv, &mysqlfs_oper, NULL);
}
