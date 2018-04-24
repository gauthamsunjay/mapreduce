/*
    Data structure for each partition:
    Hash table with open addressing. This is to make sorting easier. 
    Every entry in the hash table is a knode which stores the reference to the values list.
*/

#include<pthread.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "mapreduce.h"

#define HASH_MAP_SIZE 16384
#define SUBSET_PER_LOCK 32

typedef struct _valnode {
    char *value;
    struct _valnode *next;
} ValNode;

typedef struct _knode {
    char *key;
    struct _valnode *head; // head of the values list
    pthread_mutex_t lock;
    struct _valnode *to_free;
} KeyNode;

typedef struct _reducer_args {
    int partition;
    Reducer reduce;
} rargs;

int total_files;
int cfile;
int num_locks = HASH_MAP_SIZE / SUBSET_PER_LOCK;
char **fileq;
Partitioner p_fn;
int NUM_PARTITIONS;
pthread_mutex_t file_lock;

pthread_mutex_t **subset_locks;
KeyNode ***hash_maps;
int *cur_node_in_partition;

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long hash_fn(char *key) {
    unsigned long hash = 0;
    int c;
    while ((c = *key++) != '\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    // while ((c = *key++) != '\0')
    //     hash = hash * 33 + c;
    
    return hash;
}

void insert_val(KeyNode *knode, char *value) {
    ValNode *vnode = (ValNode *) malloc(sizeof(ValNode));
    vnode->value = value;
    pthread_mutex_lock(&knode->lock);
    vnode->next = knode->head;
    knode->head = vnode;
    pthread_mutex_unlock(&knode->lock);
}

void hm_put(char *key, char *value, int partition) {
    unsigned long hash = hash_fn(key);
    int bucket = hash % HASH_MAP_SIZE;
    int exists = 0;

    int current_subset = (bucket & 0x03FE0) >> 5;
    int new_subset;

    pthread_mutex_lock(&subset_locks[partition][current_subset]);
    while (hash_maps[partition][bucket] != NULL) {
        if (strcmp(hash_maps[partition][bucket]->key, key) == 0) {
            exists = 1;
            pthread_mutex_unlock(&subset_locks[partition][current_subset]);
            break;
        }
        bucket++;

        bucket = bucket % HASH_MAP_SIZE;
        new_subset = (bucket & 0x03FE0) >> 5;

        if (new_subset != current_subset) {
            pthread_mutex_unlock(&subset_locks[partition][current_subset]);
            current_subset = new_subset;
            pthread_mutex_lock(&subset_locks[partition][current_subset]);
        }
    }
    
    if (exists) {
        free(key);
    }

    else {
        KeyNode *knode = (KeyNode *) malloc(sizeof(KeyNode));
        knode->key = key;
        knode->head = NULL;
        knode->to_free = NULL;
        pthread_mutex_init(&knode->lock, NULL);

        hash_maps[partition][bucket] = knode;
        pthread_mutex_unlock(&subset_locks[partition][current_subset]);
    }
    
    insert_val(hash_maps[partition][bucket], value);
}

void MR_Emit(char *k, char *v) {
    char *key = (char *) malloc(strlen(k));
    char *val = (char *) malloc(strlen(v));
    key = strcpy(key, k);
    val = strcpy(val, v);
    int partition = p_fn(key, NUM_PARTITIONS);
    hm_put(key, val, partition);
}

char* fetch_file() {
    int i;
    pthread_mutex_lock(&file_lock);
    i = ++cfile;
    pthread_mutex_unlock(&file_lock);
    if (i >= total_files)
        return NULL;
    return fileq[i];
}

void *map_thread(void *arg) {
    Mapper map = (Mapper) arg;
    while (1) {
        char* file = fetch_file();
        if (file == NULL)
            return NULL;
        map(file);
    }
}

void execute_mappers(Mapper map, int num_mappers) { 
    int i;
    pthread_t map_threads[num_mappers];
    for (i = 0; i < num_mappers; i++) {
        pthread_create(&map_threads[i], NULL, map_thread, map);
    }
   
    for (i = 0; i < num_mappers; i++) {
        pthread_join(map_threads[i], NULL);
    }
}

int compare_keys(const void *p, const void *q) {
    KeyNode *kn1 = *((KeyNode **) p);
    KeyNode *kn2 = *((KeyNode **) q);

    int rv;

    if (!kn1) {
        rv = 1;
    }

    else if (!kn2) {
        rv = -1;
    }

    else {
        rv = strcmp(kn1->key, kn2->key);
    }

    return rv;
}

void *sorter(void *arg) {
    int partition = *((int *) arg);
    qsort(hash_maps[partition], HASH_MAP_SIZE, sizeof(KeyNode *), compare_keys);
    return NULL;
}

void sort_keys() {
    int i;
    pthread_t sorter_threads[NUM_PARTITIONS];
    int partitions[NUM_PARTITIONS];
    for (i = 0; i < NUM_PARTITIONS; i++) {
        partitions[i] = i;
        pthread_create(&sorter_threads[i], NULL, sorter, &partitions[i]);
    }

    for (i = 0; i < NUM_PARTITIONS; i++) {
        pthread_join(sorter_threads[i], NULL);
    }
}

char *get_next(char *key, int partition) {
    KeyNode *knode = hash_maps[partition][cur_node_in_partition[partition]];

    if (knode->head != NULL) {
        if (knode->to_free != NULL) {
            free(knode->to_free->value);
            free(knode->to_free);
        }

        knode->to_free = knode->head;
        knode->head = knode->head->next;

        return knode->to_free->value;
    }
    
    return NULL;
}

void *reducer_thread(void *args) {
    rargs *_rargs = (rargs *) args;
    KeyNode *knode;
    int partition = _rargs->partition;

    int i = 0;
    while ((knode = hash_maps[partition][i]) != NULL && i < HASH_MAP_SIZE) {
        cur_node_in_partition[partition] = i++;
        _rargs->reduce(knode->key, get_next, partition);
        if (knode->to_free != NULL) {
            free(knode->to_free->value);
            free(knode->to_free);
        }
        free(knode->key);
        free(knode);
    }

    free(hash_maps[partition]);

    for (i = 0; i < num_locks; i++) {
        pthread_mutex_destroy(&subset_locks[partition][i]);
    }
    
    free(subset_locks[partition]);
    return NULL;
}

void execute_reducers(Reducer reduce) {
    int i;
    pthread_t reducer_threads[NUM_PARTITIONS];
    rargs args[NUM_PARTITIONS];
    for (i = 0; i < NUM_PARTITIONS; i++) {
        args[i].partition = i;
        args[i].reduce = reduce;
        pthread_create(&reducer_threads[i], NULL, reducer_thread, &args[i]);
    }
    
    for (i = 0; i < NUM_PARTITIONS; i++) {
        pthread_join(reducer_threads[i], NULL);
    }
}

void initialize(int argc, char *argv[], int num_reducers, Partitioner partition) {
    total_files = argc;
    cfile = 0;
    p_fn = partition;
    NUM_PARTITIONS = num_reducers;
    pthread_mutex_init(&file_lock, NULL);
    
    fileq = argv;
    hash_maps = (KeyNode ***) malloc(NUM_PARTITIONS * sizeof(KeyNode **));
    cur_node_in_partition = (int *) malloc(NUM_PARTITIONS * sizeof(int));
    subset_locks = (pthread_mutex_t **) malloc(NUM_PARTITIONS * sizeof(pthread_mutex_t *));
    
    int i, j;

    for (i = 0; i < NUM_PARTITIONS; i++) {
        hash_maps[i] = (KeyNode **) calloc(HASH_MAP_SIZE, sizeof(KeyNode *));
        cur_node_in_partition[i] = -1;
        subset_locks[i] = (pthread_mutex_t *) malloc(num_locks * sizeof(pthread_mutex_t));
        for (j = 0; j < num_locks; j++) {
            pthread_mutex_init(&subset_locks[i][j], NULL);
        }
    } 
}

void print_values() {
    int i, j;
    for (i = 0; i < NUM_PARTITIONS; i++) {
        printf("Partition = %d\n", i);
        for (j = 0; j < HASH_MAP_SIZE; j++) {
            KeyNode *ktemp = hash_maps[i][j];
            if (ktemp) {
                ValNode *temp = ktemp->head;
                printf("Key = %s\n", ktemp->key);
                while (temp != NULL) {
                    printf("Value = %s\n", temp->value);
                    temp = temp->next;
                }
                printf("\n");
            }
        }
    }
}

void cleanup() {
    free(hash_maps);
    free(subset_locks);
    free(cur_node_in_partition);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, Partitioner partition) {
    
    initialize(argc, argv, num_reducers, partition);
    execute_mappers(map, num_mappers);
    sort_keys();
    execute_reducers(reduce);
    cleanup();
}
