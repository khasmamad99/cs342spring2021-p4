#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h> // for CHAR_BIT
#include <stdint.h> // for uint32_t
#include <string.h>
#include "simplefs.h"

#define DIR_ENTRY_SIZE 128
#define DIR_ENTRY_COUNT (4 * BLOCKSIZE) / sizeof(dir_entry)
#define FCB_COUNT (4 * BLOCKSIZE) / sizeof(fcb)
#define BITMAP_SIZE (4 * BLOCKSIZE) / sizeof(word_t)
#define IDX_BLOCK_SIZE BLOCKSIZE / sizeof(uint32_t)

// Global Variables =======================================
int vdisk_fd; // Global virtual disk file descriptor. Global within the library.
              // Will be assigned with the vsfs_mount call.
              // Any function in this file can use this.
              // Applications will not use  this directly. 

typedef struct {
    int bitmap_idx;
    int rootdir_idx;
} superblock;

typedef struct {
    char filename[110];
    uint32_t inode;
    char padding[12];
} dir_entry;

typedef struct {
    int used;
    int idx_block;
    int size;
    char padding[128 - 3 * sizeof(int)];
} fcb;


typedef struct {
    int mode;
    int pos;
    int inode;
    fcb fcb;
} open_file_info;

open_file_info *open_file_table[DIR_ENTRY_COUNT] = {NULL};

// bitmap stuff
typedef uint32_t word_t;
enum { BITS_PER_WORD = sizeof(word_t) * CHAR_BIT };
#define WORD_OFFSET(b) ((b) / BITS_PER_WORD)
#define BIT_OFFSET(b)  ((b) % BITS_PER_WORD)


/*********** FUNCTIONS **************/
int read_multiple_blocks(void *blocks, int start, int k);
int write_multiple_blocks(void *blocks, int start, int k);
int load_dir_entries(dir_entry **dirs_ptr);
int load_fcb(fcb **fcbs_ptr);
int load_bitmap(word_t **bitmap);
void bitmap_set_bit(word_t *bitmap, int n);
void bitmap_clear_bit(word_t *bitmap, int n);
int bitmap_get_bit(word_t *bitmap, int n);
int read_from_block(void *buf, int start, int n);
int write_to_block(void *buf, int start, int n);
int bitmap_first_empty_bit(word_t *bitmap);
int is_open(int fd);
/*************************************/

// ========================================================


// read block k from disk (virtual disk) into buffer block.
// size of the block is BLOCKSIZE.
// space for block must be allocated outside of this function.
// block numbers start from 0 in the virtual disk. 
int read_block (void *block, int k)
{
    int n;
    int offset;

    offset = k * BLOCKSIZE;
    lseek(vdisk_fd, (off_t) offset, SEEK_SET);
    n = read (vdisk_fd, block, BLOCKSIZE);
    if (n != BLOCKSIZE) {
	printf ("read error\n");
	return -1;
    }
    return (0); 
}

// write block k into the virtual disk. 
int write_block (void *block, int k)
{
    int n;
    int offset;

    offset = k * BLOCKSIZE;
    lseek(vdisk_fd, (off_t) offset, SEEK_SET);
    n = write (vdisk_fd, block, BLOCKSIZE);
    if (n != BLOCKSIZE) {
	printf ("write error\n");
	return (-1);
    }
    return 0; 
}


/**********************************************************************
   The following functions are to be called by applications directly. 
***********************************************************************/

// this function is partially implemented.
int create_format_vdisk (char *vdiskname, unsigned int m)
{
    if (m < 24 || m > 29) {
        printf("m should be in the range [24, 29]\n");
        return -1;
    }

    char command[1000];
    int size;
    int num = 1;
    int count;
    size  = num << m;
    count = size / BLOCKSIZE;
    //    printf ("%d %d", m, size);
    sprintf (command, "dd if=/dev/zero of=%s bs=%d count=%d",
             vdiskname, BLOCKSIZE, count);
    //printf ("executing command = %s\n", command);
    system (command);

    // now write the code to format the disk below.
    // write the superblock
    // !?? add free block count, block size, volume size ??!
    superblock sblock;
    sblock.bitmap_idx = 1;
    sblock.rootdir_idx = 5;
    if (write_block((void *) &sblock, 0) == -1) {
        printf("error writing superblock\n");
        return -1;
    };

    // mount so that the contents can be written
    if (sfs_mount (vdiskname) != 0) {
        printf ("could not mount \n");
        return -1;
    }

    // write the bitmap
    word_t *bitmap;
    bitmap = (word_t *) malloc(4 * BLOCKSIZE);
    // !!! first several blocks should be non-empty !!!
    int i;
    for (i = 0; i < BITMAP_SIZE; i++)
        bitmap[i] = 0;

    // first 13 blocks are occupied
    for (i = 0; i < 13; i++)
        bitmap_set_bit(bitmap, i);


    if (write_multiple_blocks((void *) bitmap, 4, 1) == -1) {
        printf("error writing bitmap\n");
        return -1;
    }
    free(bitmap);

    // write the directory entries
    dir_entry *dirs = (dir_entry *) malloc(4 * BLOCKSIZE);
    // initaize the entry names to empty strings and inodes to -1
    for (i = 0; i < DIR_ENTRY_COUNT; i++) {
        strcpy(dirs[i].filename, "");
        dirs[i].inode = -1;
    }
    // write
    if (write_multiple_blocks((void *) dirs, 4, 5) == -1) {
        printf("error writing directory entries");
        return -1;
    }

    free(dirs);

    // write FCBs
    fcb *fcbs = (fcb *) malloc(4 * BLOCKSIZE);
    for (i = 0; i < FCB_COUNT; i++) {
        fcbs[i].idx_block = -1;
        fcbs[i].used = 0;
        fcbs[i].size = 0;
    }

    if (write_multiple_blocks((void *) fcbs, 4, 9) == -1) {
        printf("error writing fcbs\n");
        return -1;
    }
    free(fcbs);

    sfs_umount();

    return (0); 
}


// already implemented
int sfs_mount (char *vdiskname)
{
    // simply open the Linux file vdiskname and in this
    // way make it ready to be used for other operations.
    // vdisk_fd is global; hence other function can use it. 
    vdisk_fd = open(vdiskname, O_RDWR); 
    return(0);
}


// already implemented
int sfs_umount ()
{
    fsync (vdisk_fd); // copy everything in memory to disk
    close (vdisk_fd);
    return (0); 
}


int sfs_create(char *filename)
{   
    dir_entry *dirs;
    if (load_dir_entries(&dirs) == -1)
        return -1;

    int i, dir_idx;
    dir_idx = -1;
    // check if a file with the same name exists
    for (i = 0; i < DIR_ENTRY_COUNT && strcmp(dirs[i].filename, filename) != 0; i++) {
        if (dir_idx == -1 && dirs[i].inode == -1)
            dir_idx = i;
    }
    
    if (i != DIR_ENTRY_COUNT) {
        printf("error: file %s already exists\n", filename);
        return -1;
    }

    if (dir_idx == -1) {
        printf("error: max number of dir entries is reached\n");
        return -1;
    }

    strcpy(dirs[dir_idx].filename, filename);
    
    fcb *fcbs;
    if (load_fcb(&fcbs) == -1)
        return -1;
    for (i = 0; i < FCB_COUNT && fcbs[i].used; i++)
        ;
    if (i == FCB_COUNT) {
        printf("error: max number of fcbs is reached\n");
        return -1;
    }
    dirs[dir_idx].inode = i;
    // write dirs back
    if (write_multiple_blocks((void *) dirs, 4, 5) == -1)
        return -1;

    // find an empty block for the index block of the file
    word_t *bitmap;
    if (load_bitmap(&bitmap) == -1)
        return -1;


    int empty_bit_idx = bitmap_first_empty_bit(bitmap);
    fcbs[i].idx_block = empty_bit_idx;
    bitmap_set_bit(bitmap, empty_bit_idx);
    // write back the bitmap
    if (write_multiple_blocks((void *) bitmap, 4, 1) == -1)
        return -1;

    fcbs[i].used = 1;
    // write back the fcbs
    if (write_multiple_blocks((void *) fcbs, 4, 9) == -1)
        return -1;

    // set the entires in the index block to -1
    uint32_t *idx_block_ptr = (uint32_t *) malloc(BLOCKSIZE);
    for (i = 0; i < IDX_BLOCK_SIZE; i++)
        idx_block_ptr[i] = -1;

    // write the index block
    if (write_block((void *) idx_block_ptr, empty_bit_idx) == -1)
        return -1;

    free(dirs);
    free(fcbs);
    free(bitmap);
    free(idx_block_ptr);

    return (0);
}


int sfs_open(char *file, int mode)
{
    if (mode != MODE_READ && mode != MODE_APPEND) {
        printf("error: invalid mode\n");
        return -1;
    }

    dir_entry *dirs;
    if (load_dir_entries(&dirs) == -1)
        return -1;

    int i, inode;
    for (i = 0; i < DIR_ENTRY_COUNT && strcmp(dirs[i].filename, file) != 0; i++)
        ;
    if (i == DIR_ENTRY_COUNT) {
        printf("error: file %s nof found\n", file);
        return -1;
    }
    inode = dirs[i].inode;

    fcb *fcbs;
    if (load_fcb(&fcbs) == -1)
        return -1;

    for (i = 0; i < DIR_ENTRY_COUNT && open_file_table[i] != NULL; i++)
        ;
    if (i == DIR_ENTRY_COUNT) {
        printf("error: open file table is full");
        return -1;
    }

    open_file_table[i] = (open_file_info *) malloc(sizeof(open_file_info));
    open_file_table[i]->fcb = fcbs[inode];
    open_file_table[i]->mode = mode;
    open_file_table[i]->pos = 0;
    if (mode == MODE_APPEND)
        open_file_table[i]->pos = fcbs[inode].size;
    open_file_table[i]->inode = inode;

    free(dirs);
    free(fcbs);

    return (i); 
}

int sfs_close(int fd)
{
    if (is_open(fd) == -1)
        return -1;

    // if opened in append mode, write back the file fcb
    if (open_file_table[fd]->mode == MODE_APPEND) {
        fcb *fcbs;
        if (load_fcb(&fcbs) == -1)
            return -1;
        
        fcbs[open_file_table[fd]->inode] = open_file_table[fd]->fcb;
        if (write_multiple_blocks((void *) fcbs, 4, 9) == -1)
            return -1;
    }

    free(open_file_table[fd]);
    open_file_table[fd] = NULL;

    return (0); 
}

int sfs_getsize (int  fd)
{
    if (is_open(fd) == -1)
        return -1;

    return open_file_table[fd]->fcb.size;
}

int sfs_read(int fd, void *buf, int n)
{
    if (is_open(fd) == -1)
        return -1;

    if (open_file_table[fd]->mode != MODE_READ) {
        printf("error file is not opened in read mode\n");
        return -1;
    }

    int idx_block_no = open_file_table[fd]->fcb.idx_block;
    uint32_t *idx_block = (uint32_t *) malloc(BLOCKSIZE);
    if (read_block((void *) idx_block, idx_block_no) == -1) {
        printf("error reading index block of the file\n");
        return -1;
    }

    int pos, start_block, start_offset, abs_start_offset;
    int end_block, end_offset;
    pos = open_file_table[fd]->pos;
    start_block = pos / BLOCKSIZE;
    start_offset = pos % BLOCKSIZE;
    end_block = (pos + n) / BLOCKSIZE;
    end_offset = (pos + n) % BLOCKSIZE;
    abs_start_offset = idx_block[start_block] * BLOCKSIZE + start_offset;

    if (start_block == end_block) {
        // reading from a single block
        if (read_from_block(buf, abs_start_offset, n) == -1)
            return -1;
        pos += n;
    } else {
        // reading from multiple blocks
        int read_size;
        int total_read = 0;

        // 1. read the remaining part of the starting/current block
        read_size = BLOCKSIZE - start_offset;
        if (read_from_block(buf, abs_start_offset, read_size) != read_size)
            return -1;
        total_read += read_size;
        pos += read_size;
        buf += read_size;

        // 2. read the blocks in between
        int i;
        for (i = start_block + 1; i < end_block; i++) {
            if (read_block(buf, idx_block[i]) == -1) {
                printf("read error\n");
                return -1;
            }
            buf += BLOCKSIZE;
            pos += BLOCKSIZE;
            total_read += BLOCKSIZE;
        }

        // 3. read from the last block
        read_size = end_offset;
        int start = idx_block[end_block] * BLOCKSIZE;
        if (read_from_block(buf, start, read_size) != read_size) {
            printf("read error\n");
            return -1;
        }
        total_read += read_size;
        pos += read_size;

        // consider deleting
        if (total_read != n) {
            printf("read error: total_read != n\n");
            return -1;
        }
    }

    open_file_table[fd]->pos = pos;
    free(idx_block);

    return (n); 
}


int sfs_append(int fd, void *buf, int n)
{
    if (is_open(fd) == -1)
        return -1;
    
    if (open_file_table[fd]->mode != MODE_APPEND) {
        printf("error file is not opened in append mode\n");
        return -1;
    }

    int idx_block_no = open_file_table[fd]->fcb.idx_block;
    uint32_t *idx_block = (uint32_t *) malloc(BLOCKSIZE);
    if (read_block((void *) idx_block, idx_block_no) == -1) {
        printf("error reading index block of the file\n");
        return -1;
    }

    int pos, start_block, start_offset, abs_start_offset;
    int end_block, end_offset;
    pos = open_file_table[fd]->pos;
    start_block = pos / BLOCKSIZE;
    start_offset = pos % BLOCKSIZE;
    end_block = (pos + n) / BLOCKSIZE;
    end_offset = (pos + n) % BLOCKSIZE;

    // check if max file size is exceeded
    if (end_block >= IDX_BLOCK_SIZE){
        printf("error: max file size is reached\n");
        return -1;
    }

    // allocate new blocks
    word_t *bitmap;
    if (load_bitmap(&bitmap) == -1)
        return -1;

    int loop_start = (idx_block[start_block] == -1) ? start_block : start_block + 1;
    int first_empty_bit;
    int i;
    for (i = loop_start; i <= end_block; i++) {
        first_empty_bit = bitmap_first_empty_bit(bitmap);
        idx_block[i] = first_empty_bit;
        bitmap_set_bit(bitmap, first_empty_bit);
    }

    // write back the index block and the bitmap
    if (write_block((void *) idx_block, idx_block_no) == -1)
        return -1;
    if (write_multiple_blocks((void *) bitmap, 4, 1) == -1)
        return -1;

    abs_start_offset = idx_block[start_block] * BLOCKSIZE + start_offset;
    if (start_block == end_block) {
        // writing to a single block
        if (write_to_block(buf, abs_start_offset, n) != n)
            return -1;
        pos += n;
    } else {
        // writing to multiple blocks
        int write_size;
        int total_write = 0;

        // 1. write to the remaining part of the starting/current block
        write_size = BLOCKSIZE - start_offset;
        if (write_to_block(buf, abs_start_offset, write_size) != write_size)
            return -1;
        total_write += write_size;
        pos += write_size;
        buf += write_size;

        // 2. write to the blocks in between
        for (i = start_block + 1; i < end_block; i++) {
            if (write_block(buf, idx_block[i]) == -1) {
                printf("write error\n");
                return -1;
            }
            buf += BLOCKSIZE;
            pos += BLOCKSIZE;
            total_write += BLOCKSIZE;
        }

        // 3. write to the last block
        write_size = end_offset;
        int start = idx_block[end_block] * BLOCKSIZE;
        if (write_to_block(buf, start, write_size) != write_size) {
            printf("write error\n");
            return -1;
        }
        total_write += write_size;
        pos += write_size;

        // consider deleting
        if (total_write != n) {
            printf("read error: total_read != n\n");
            return -1;
        }
    }

    // update the file size
    open_file_table[fd]->fcb.size += n;
    open_file_table[fd]->pos = pos;

    free(idx_block);
    free(bitmap);

    return (n); 
}

int sfs_delete(char *filename)
{
    // load directory entries
    dir_entry *dirs;
    if (load_dir_entries(&dirs) == -1)
        return -1;

    // find the file
    int i;
    for (i = 0; i < DIR_ENTRY_COUNT && strcmp(filename, dirs[i].filename) != 0; i++)
        ;
    
    if (i == DIR_ENTRY_COUNT) {
        printf("error: file %s not found\n", filename);
        return -1;
    }

    // set the name to empty string and inode to -1
    strcpy(dirs[i].filename, "");
    int inode = dirs[i].inode;
    dirs[i].inode = -1;

    // write back dirs
    if (write_multiple_blocks((void *) dirs, 4, 5) == -1) {
        printf("error writing dirs\n");
        return -1;
    }

    // load file fcb
    fcb *fcbs;
    if (load_fcb(&fcbs) == -1)
        return -1;

    int idx_block_no = fcbs[inode].idx_block;
    fcbs[inode].used = 0;
    fcbs[inode].size = 0;
    fcbs[inode].idx_block = -1;

    // write back fcbs
    if (write_multiple_blocks((void *) fcbs, 4, 9) == -1) {
        printf("error writing fcbs\n");
        return -1;
    }

    // load index block
    uint32_t *idx_block = (uint32_t *) malloc(BLOCKSIZE);
    if (read_block((void *) idx_block, idx_block_no) == -1) {
        printf("error reading index block of the file\n");
        return -1;
    }

    // load bitmap
    word_t *bitmap;
    if (load_bitmap(&bitmap) == -1)
        return -1;

    // update the bitmap
    for (i = 0; i < IDX_BLOCK_SIZE && idx_block[i] != -1; i++) {
        bitmap_clear_bit(bitmap, idx_block[i]);
        idx_block[i] = -1;
    }

    // free index block
    bitmap_clear_bit(bitmap, idx_block_no);

    // write back the bitmap
    if (write_multiple_blocks((void *) bitmap, 4, 1) == -1) {
        printf("error writing bitmap\n");
        return -1;
    }

    free(dirs);
    free(fcbs);
    free(idx_block);
    free(bitmap);

    return (0); 
}


int is_open(int fd) {
    if (open_file_table[fd] == NULL) {
        printf("error: file not in open file table\n");
        return -1;
    }
    return 0;
}

int load_dir_entries(dir_entry **dirs_ptr) {
    int k = 4;
    void *dirs_block = (void *) malloc(k * BLOCKSIZE);
    if (read_multiple_blocks(dirs_block, 5, k) == -1) {
        printf("error loading directory entries\n");
        return -1;
    }
    *dirs_ptr = (dir_entry *) dirs_block;
    return 0;
}

int load_fcb(fcb **fcbs_ptr) {
    int k = 4;
    void *fcbs_block = (void *) malloc(k * BLOCKSIZE);
    if (read_multiple_blocks(fcbs_block, 9, k) == -1) {
        printf("error loading fcbs\n");
        return -1;
    }
    *fcbs_ptr = (fcb *) fcbs_block;
    return 0;
}

int load_bitmap(word_t **bitmap) {
    int k = 4;
    void *bitmap_block = (void *) malloc(k * BLOCKSIZE);
    if (read_multiple_blocks(bitmap_block, 1, k) == -1) {
        printf("error loading fcbs\n");
        return -1;
    }
    *bitmap = (word_t *) bitmap_block;
    return 0;
}

int read_multiple_blocks(void *blocks, int start, int k) {
    int i;
    for (i = 0; i < k; i++) {
        if (read_block(blocks + i * BLOCKSIZE, start + i) == -1)
            return -1;
    }
    return 0;
}

int write_multiple_blocks(void *blocks, int k, int start) {
    int i;
    for (i = 0; i < k; i++) {
        if (write_block(blocks + i * BLOCKSIZE, start + i) == -1)
            return -1;
    }
    return 0;
}

void bitmap_set_bit(word_t *bitmap, int n) {
    bitmap[WORD_OFFSET(n)] |= ((word_t) 1 << BIT_OFFSET(n));
}

void bitmap_clear_bit(word_t *bitmap, int n) {
    bitmap[WORD_OFFSET(n)] &= ~((word_t) 1 << BIT_OFFSET(n)); 
}

int bitmap_get_bit(word_t *bitmap, int n) {
    word_t bit = bitmap[WORD_OFFSET(n)] & ((word_t) 1 << BIT_OFFSET(n));
    return bit != 0; 
}

int bitmap_first_empty_bit(word_t *bitmap) {
    int i, j;
    for (i = 0; i < BITMAP_SIZE && bitmap[i] == (word_t) -1; i++)
        ;
    if (i == BITMAP_SIZE) {
        printf("error: no more blocks available\n");
        return -1;
    }

    word_t word = bitmap[i];
    for (j = 0; j < BITS_PER_WORD && (word & ((word_t) 1 << j)) != 0; j++)
        ;
    return i * BITS_PER_WORD + j;
}

int read_from_block(void *buf, int start, int n) {
    int read_size;
    lseek(vdisk_fd, (off_t) start, SEEK_SET);
    read_size = read (vdisk_fd, buf, n);
    if (n != read_size) {
	    printf ("read error\n");
        return -1;
    }
    return read_size;
}

int write_to_block(void *buf, int start, int n) {
    int write_size;
    lseek(vdisk_fd, (off_t) start, SEEK_SET);
    write_size = write (vdisk_fd, buf, n);
    if (n != write_size) {
	    printf ("write error\n");
        return -1;
    }
    return write_size;
}

void print_bitmap(word_t *bitmap) {
    for (int i = 0; i < BITMAP_SIZE; i ++)
        printf("%x ", bitmap[i]);

    printf("\n");
}