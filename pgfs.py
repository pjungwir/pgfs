#!/usr/bin/env python3

# pgfs - Builds an on-disk tree of directories and hard links to show what's in your database.

import sys
import psycopg2
import os
import fcntl
import errno
import shutil

# TODO: Support tablespaces
# TODO: Support "dry run"

class Database:
    def __init__(self, oid, name):
        self.oid = oid
        self.name = name
        self.schemas = {}

class Schema:
    def __init__(self, oid, name):
        self.oid = oid
        self.name = name
        self.tables = {}

class Table:
    def __init__(self, name, relfilenode):
        self.name = name
        self.relfilenode = relfilenode

def usage():
    print("USAGE: pgfs.py <destroot>")
    sys.exit(1)

def get_data_dir(conn):
    with conn.cursor() as cur:
        cur.execute("SHOW data_directory")
        return cur.fetchone()[0]

def get_tables(conn, schema):
    with conn.cursor() as cur:
        cur.execute("SELECT relname, relfilenode FROM pg_class WHERE relnamespace = %s AND relkind = 'r'", (schema.oid,))
        return {Table(row[0], row[1]) for row in cur.fetchall()}

def get_schemas(conn):
    schemas = []
    with conn.cursor() as cur:
        cur.execute("SELECT oid, nspname FROM pg_namespace WHERE nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')")
        return {Schema(row[0], row[1]) for row in cur.fetchall()}

def get_databases(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT oid, datname FROM pg_database WHERE NOT datistemplate")
        return [Database(row[0], row[1]) for row in cur.fetchall()]

def get_contents(conn):
    databases = get_databases(conn)
    for db in databases:
        conn2 = psycopg2.connect(database=db.name)
        db.schemas = get_schemas(conn2)
        for sch in db.schemas:
            sch.tables = get_tables(conn2, sch)
    return databases

def print_tree(databases):
    for db in databases:
        print(db.name)
        for sch in db.schemas:
            print("  %s" % sch.name)
            for t in sch.tables:
                print("    %s" % t.name)

def build_tree(destroot):
    conn = psycopg2.connect()       # Just use normal libpq envvars: PGPORT, PGDATABASE, etc.
    pgroot = get_data_dir(conn)
    # print(pgroot)
    databases = get_contents(conn)
    # print_tree(databases)
    write_tree(pgroot, destroot, databases)

def same_inode(f1, f2):
    return os.stat(f1).st_ino == os.stat(f2).st_ino

def _write_tree(pgroot, destroot, databases):
    for db in databases:
        db_dir = os.path.join(destroot, db.name)
        os.makedirs(db_dir, exist_ok=True)

        for sch in db.schemas:
            sch_dir = os.path.join(db_dir, sch.name)
            os.makedirs(sch_dir, exist_ok=True)

            for t in sch.tables:
                src_file = os.path.join(pgroot, "base", "%d" % db.oid, "%d" % t.relfilenode)
                dest_file = os.path.join(sch_dir, t.name)
                # TODO: Support an option to just make empty files instead of hard links. Maybe support symlinks too.
                # touch(t_file, excl=False)
                try:
                    os.link(src_file, dest_file)
                except FileExistsError as e:
                    # Probably okay, but make sure it's a hardlink to the right file.
                    # If not then replace it.
                    if not same_inode(src_file, dest_file):
                        os.unlink(dest_file)
                        os.link(src_file, dest_file)

    # Remove dropped databases:
    dbs_by_name = { db.name : db for db in databases }
    with os.scandir(destroot) as it:
        for d in it:
            if d.name == ".pgfs": continue
            db = dbs_by_name.get(d.name, None)
            if db is None:
                shutil.rmtree(d.path)
            else:
                # Remove dropped schemas:
                schemas_by_name = { sch.name : sch for sch in db.schemas }
                with os.scandir(d.path) as it2:
                    for d2 in it2:
                        sch_dir = os.path.join(d.path, d2)
                        sch = schemas_by_name.get(d2.name, None)
                        if sch is None:
                            shutil.rmtree(d2.path)
                        else:
                            # Remove dropped tables:
                            tables_by_name = { t.name : t for t in sch.tables }
                            with os.scandir(d2.path) as it3:
                                for d3 in it3:
                                    t = tables_by_name.get(d3.name, None)
                                    if t is None:
                                        os.unlink(d3.path)


def touch(filename, excl=True):
    flags = os.O_CREAT
    if excl:
        flags = flags | os.O_EXCL
    os.close(os.open(filename, flags))

def already_locked(ex):
    return ex.errno == errno.EACCES or ex.errno == errno.EAGAIN

def lock_file(filename):
    # TODO: Support Windows too
    f = os.open(filename, os.O_RDWR)
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        if already_locked(e):
            print(".pgfs is already locked. Refusing to continue")
            sys.exit(1)
        else:
            raise e
    return f

def unlock_file(f):
    # TODO: Support Windows too
    fcntl.lockf(f, fcntl.LOCK_UN)

def write_tree(pgroot, destroot, databases):
    # If destroot doesn't exist, create it.
    # If it exists and is empty, populate it.
    # If it exists and is not empty, then only proceed if it has a .pgfs file at the top-level. Otherwise warn & die.
    # If there are databases that no longer exist, remove them.
    # If there are schemas that no longer exist, remove them.
    # If there are tables that no longer exist, remove them.
    dot_pgfs = os.path.join(destroot, ".pgfs")
    f = None
    if os.path.isdir(destroot) and len(os.listdir(destroot)) > 0:
        if not os.path.isfile(dot_pgfs):
            print("WARNING: You asked to build a pgfs tree in %s, but it already exists and doesn't look like a pgfs tree (no .pgfs file found)." % destroot)
            print("Quitting lest we delete something we shouldn't.")
            sys.exit(1)
    else:
        os.makedirs(destroot, exist_ok=True)
        touch(dot_pgfs)

    f = lock_file(dot_pgfs)
    try:
        _write_tree(pgroot, destroot, databases)
    finally:
        unlock_file(f)

def main(argv):
    if len(argv) != 2:
        usage()
    destroot = argv[1]
    build_tree(destroot)

if __name__ == '__main__':
    main(sys.argv)
