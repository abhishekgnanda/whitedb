/*
* $Id:  $
* $Version: $
*
* Copyright (c) Andri Rebane 2009, Priit J�rv 2009
*
* This file is part of wgandalf
*
* Wgandalf is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
* 
* Wgandalf is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
* 
* You should have received a copy of the GNU General Public License
* along with Wgandalf.  If not, see <http://www.gnu.org/licenses/>.
*
*/

 /** @file dbdump.c
 *  DB dumping support for wgandalf memory database
 *
 */

/* ====== Includes =============== */

#include <stdio.h>
#include <stdlib.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <malloc.h>
#endif

#ifdef _WIN32
#include "../config-w32.h"
#else
#include "../config.h"
#endif
#include "dballoc.h"
#include "dblock.h"
/*#include "dbmem.h"*/

/* ====== Private headers and defs ======== */

#include "dbdump.h"

#define USE_MAPPING 0
/* Dump/import speed, 100MB db - average of 10 tests:
 * Windows:
 *  stdio w 1595 ms  r 211 ms
 *  mapped file w 1872 ms r 188 ms
 * Linux:
 *  stdio w 1871 ms (min 614, max 3595) r 135 ms
 */
 
/* ======= Private protos ================ */




/* ====== Functions ============== */


/** dump shared memory to the disk.
 *  Returns 0 when successful (no error).
 *  -1 non-fatal error (db may continue)
 *  -2 fatal error (should abort db)
 *  This function is parallel-safe (may run during normal db usage)
 */

gint wg_dump(void * db,char fileName[]) {
#if (defined(_WIN32) && USE_MAPPING)
  void *hviewfile;
  HANDLE hmapfile, hfile;
#else
  FILE *f;
#endif
  db_memsegment_header* dbh = (db_memsegment_header *) db;
  gint dbsize = dbh->free; /* first unused offset - 0 = db size */
  gint err = -1;
  gint lock_id;

  /* Open the dump file */
#if (defined(_WIN32) && USE_MAPPING)
  hfile = CreateFile(fileName,       // lpFileName
              GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
              FILE_SHARE_READ,              // dwShareMode
              NULL,           // lpSecurityAttributes
              CREATE_ALWAYS,  // dwCreationDisposition
              FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
              NULL            // hTemplateFile
            );

  if(hfile==INVALID_HANDLE_VALUE) {
    fprintf(stderr, "Error opening file\n");
    return -1;
  }

  hmapfile = CreateFileMapping(
               hfile,
               NULL,                    /* default security */
               PAGE_READWRITE,          /* read/write access */
               0,                       /* higher DWORD of size */
               (DWORD) dbsize,         /* lower DWORD of size */
               NULL);

  if(!hmapfile) {
    fprintf(stderr, "Error opening file mapping\n");
    CloseHandle(hfile);
    return -1;
  }
#else
  f = fopen(fileName, "wb");
  if(!f) {
    fprintf(stderr, "Error opening file\n");
    return -1;
  }
#endif

  /* Get shared lock on the db */
  lock_id = wg_start_read(db);
  if(!lock_id) {
    fprintf(stderr, "Failed to lock the database for dump\n");
    return -1;
  }

  /* Now, write the memory area to file */
#if (defined(_WIN32) && USE_MAPPING)
  hviewfile = (void*) MapViewOfFile(hmapfile, FILE_MAP_ALL_ACCESS, 0, 0, 0);
  if(hviewfile==NULL) {
    fprintf(stderr, "Error opening file mapping\n");
  }
  else {
    CopyMemory(hviewfile, db, dbsize);

    /* Attempt to flush buffers. If this fails, we
     * may be out of disk space or some other error
     * has occured, in any case the write has effectively failed. */
    if(FlushViewOfFile (hviewfile,0) && FlushFileBuffers(hfile))
      err = 0;
    else
      fprintf(stderr, "Error flushing buffers\n");

    UnmapViewOfFile(hviewfile);
  }
#else
  if(fwrite(db, dbsize, 1, f) == 1)
    err = 0;
  else
    fprintf(stderr, "Error writing file\n");
#endif

  /* We're done writing (either buffers or mmap-ed file) */
  if(!wg_end_read(db, lock_id)) {
    fprintf(stderr, "Failed to unlock the database\n");
    err = -2; /* This error should be handled as fatal */
  }

#if (defined(_WIN32) && USE_MAPPING)
  CloseHandle(hmapfile);
  CloseHandle(hfile);
#else
  fflush(f);
  fclose(f);
#endif

  /* Get exclusive lock to modify the logging ares */
  lock_id = wg_start_write(db);
  if(!lock_id) {
    fprintf(stderr, "Failed to lock the database for log reset\n");
    return -2; /* Logging area inconsistent --> fatal. */
  }

  //flush logging
  while(dbh->logging.logoffset>dbh->logging.firstoffset)
  {
    //write zeros to logging area
    dbstore(db,dbh->logging.logoffset,0);
    dbh->logging.logoffset--;        
  }

  if(!wg_end_write(db, lock_id)) {
    fprintf(stderr, "Failed to unlock the database\n");
    err = -2; /* Write lock failure --> fatal */
  }
  return err;
}


/** Import database dump from disk.
 *  Returns 0 when successful (no error).
 *  -1 non-fatal error (db may continue)
 *  -2 fatal error (should abort db)
 *
 *  this function is NOT parallel-safe. Other processes accessing
 *  db concurrently may cause undefined behaviour (including data loss)
 */
gint wg_import_dump(void * db,char fileName[]) {
#if (defined(_WIN32) && USE_MAPPING)
  void *hviewfile;
  HANDLE hmapfile,hfile;
#else
  db_memsegment_header* dumph;
  FILE *f;
#endif
  db_memsegment_header* dbh = (db_memsegment_header *) db;
  gint dbsize = -1, newsize;
  gint err = -1;

  /* Attempt to open the dump file */
#if (defined(_WIN32) && USE_MAPPING)
  hfile = CreateFile(fileName,       // lpFileName
              GENERIC_READ | GENERIC_WRITE , // dwDesiredAccess
              FILE_SHARE_READ,              // dwShareMode
              NULL,           // lpSecurityAttributes
              OPEN_EXISTING,  // dwCreationDisposition
              FILE_ATTRIBUTE_NORMAL, // dwFlagsAndAttributes
              NULL            // hTemplateFile
            );
    
  if(hfile==INVALID_HANDLE_VALUE) {
    fprintf(stderr, "Error opening file\n");
    return -1;
  }

  hmapfile = CreateFileMapping(
               hfile,
               NULL,                    /* default security */
               PAGE_READWRITE,          /* read/write access */
               0,
               0,                       /* use current file size */
               NULL);

  if(!hmapfile) {
    fprintf(stderr, "Error opening file mapping\n");
    CloseHandle(hfile);
    return -1;
  }
#else
  f = fopen(fileName, "rb");
  if(!f) {
    fprintf(stderr, "Error opening file\n");
    return -1;
  }
#endif

  /* Examine the dump header. */
#if (defined(_WIN32) && USE_MAPPING)
  hviewfile = (void*) MapViewOfFile(hmapfile, FILE_MAP_ALL_ACCESS, 0, 0, 0);
  if(hviewfile==NULL) {
    fprintf(stderr, "Error opening file mapping\n");
  }
  else {
    if(dbcheck(hviewfile) && \
      ((db_memsegment_header *) hviewfile)->version==MEMSEGMENT_VERSION) {
      dbsize = ((db_memsegment_header *) hviewfile)->free;
    } else
      fprintf(stderr, "Incompatible dump file %s\n", fileName);
  }
#else
  /* With the non-mapped file, the most sane way of handling this is to
   * read the entire header into local memory. This way changes in header
   * structure won't break this code (naturally they will still break
   * dump file compatibility) */
  dumph = malloc(sizeof(db_memsegment_header));
  if(!dumph) {
    fprintf(stderr, "malloc error in wg_import_dump\n");
  }
  else if(fread(dumph, sizeof(db_memsegment_header), 1, f) != 1) {
    fprintf(stderr, "Error reading dump header\n");
  }
  else {
    /* XXX: mathes the code for the memory mapped case, but
     * don't merge just yet - one version might stay and the other
     * go, eventually */
    if(dbcheck(dumph) && dumph->version==MEMSEGMENT_VERSION) {
      dbsize = dumph->free;
    } else
      fprintf(stderr, "Incompatible dump file %s\n", fileName);
  }
  if(dumph) free(dumph);
#endif

  /* 0 > dbsize >= dbh->size indicates that we were
   * able to read the dump and it contained a compatible
   * memory image that fits in our current shared memory.
   */
  if(dbh->size < dbsize) {
    fprintf(stderr, "Data does not fit in shared memory area.\n");
  } else if(dbsize > 0) {
    /* We have a compatible dump file. */
    newsize = dbh->size;
#if (defined(_WIN32) && USE_MAPPING)
    CopyMemory(db, hviewfile, dbsize);
    err = 0;
    dbh->size = newsize; /* restore correct size of memory segment */
#else
    fseek(f, 0, SEEK_SET);
    if(fread(db, dbsize, 1, f) != 1) {
      fprintf(stderr, "Error reading dump file\n");
      err = -2; /* database is in undetermined state now */
    } else {
      err = 0;
      dbh->size = newsize;
    }
#endif
  }

#if (defined(_WIN32) && USE_MAPPING)
  if(hviewfile)
    UnmapViewOfFile(hviewfile);
  CloseHandle(hmapfile);
  CloseHandle(hfile);
#else
  fclose(f);
#endif

  /* any errors up to now? */
  if(err) return err;

  /* Initialize db state */
  /* XXX: logging ignored here, for now */
  return wg_init_locks(db);
}