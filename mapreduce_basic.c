#include<pthread.h>
#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include "mapreduce.h"

//definitions

typedef struct _kv {
    char *key;
    char *value;
} kv;

typedef struct _reducer_args {
    int partition;
    Reducer reduce;
} rargs;

pthread_mutex_t file_lock;
int total_files;
char **fileq;
Partitioner p_fn;
int NUM_PARTITIONS;

int *partition_size;
kv ***partitions;
int *last_index_per_partition;
pthread_mutex_t *partition_locks;
int *iterator_indices;

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

char *get_next(char *key, int partition_number) {
    int idx = iterator_indices[partition_number];
    if (idx <= last_index_per_partition[partition_number]) {
        kv *kv_pair = partitions[partition_number][idx];
        if (strcmp(kv_pair->key, key) == 0) {
            iterator_indices[partition_number]++;
            return kv_pair->value;
        }
    }
    return NULL;
}

void insert_kv(char *key, char *value, int partition) {
    kv *kv_pair = (kv *) malloc(sizeof(kv));
    kv_pair->key = key;
    kv_pair->value = value;
    pthread_mutex_lock(&partition_locks[partition]);
    int idx = last_index_per_partition[partition];
    if ((idx + 1) > 0 && (idx + 1) % partition_size[partition] == 0) {
        partition_size[partition] *= 2;
        partitions[partition] = realloc(partitions[partition], partition_size[partition] * sizeof(kv *));
    }
    last_index_per_partition[partition]++;
    partitions[partition][last_index_per_partition[partition]] = kv_pair;
    pthread_mutex_unlock(&partition_locks[partition]);
}

void MR_Emit(char *k, char *v) {
    char *key = (char *) malloc(sizeof(char) * strlen(k));
    char *val = (char *) malloc(sizeof(char) * strlen(v));
    key = strcpy(key, k);
    val = strcpy(val, v);
    int partition = p_fn(key, NUM_PARTITIONS);
    insert_kv(key, val, partition);
}

int compare_keys(const void *p, const void *q) {
    kv *kv1 = *((kv **) p);
    kv *kv2 = *((kv **) q);
    return strcmp(kv1->key, kv2->key);
}

void *sorter(void *arg) {
    int partition = *((int *) arg);
    qsort(partitions[partition], last_index_per_partition[partition] + 1, sizeof(kv *), compare_keys);
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

char* fetch_file() {
    static int cfile = 0;
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

void *reducer_thread(void *args) {
    rargs *_rargs = (rargs *) args;
    if (last_index_per_partition[_rargs->partition] >= 0) {
        while (1) {
            if (iterator_indices[_rargs->partition] > last_index_per_partition[_rargs->partition])
                break;

            kv *kvp = partitions[_rargs->partition][iterator_indices[_rargs->partition]];
            _rargs->reduce(kvp->key, get_next, _rargs->partition);
        }
    }
    
    int j;
    int i = _rargs->partition;
    for (j = 0; j <= last_index_per_partition[i]; j++) {
        free(partitions[i][j]->key);
        free(partitions[i][j]->value);
        free(partitions[i][j]);
    }

    free(partitions[i]);
    pthread_mutex_destroy(&partition_locks[i]);

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
    fileq = argv;
    p_fn = partition;
    NUM_PARTITIONS = num_reducers;
    pthread_mutex_init(&file_lock, NULL);

    partitions = (kv ***) malloc(NUM_PARTITIONS * sizeof(kv **));
    last_index_per_partition = (int *) malloc(NUM_PARTITIONS * sizeof(int));
    partition_locks = (pthread_mutex_t *) malloc(NUM_PARTITIONS * sizeof(pthread_mutex_t));
    iterator_indices = (int *) malloc(NUM_PARTITIONS * sizeof(int));
    partition_size = (int *) malloc(NUM_PARTITIONS * sizeof(int));

    int i;
    for (i = 0; i < NUM_PARTITIONS; i++) {
        partition_size[i] = 50;
        partitions[i] = (kv **) malloc(partition_size[i] * sizeof(kv *));
        last_index_per_partition[i] = -1;
        iterator_indices[i] = 0;
        pthread_mutex_init(&partition_locks[i], NULL);
    }
}

void print_values() {
    int i, j;
    for (i = 0; i < NUM_PARTITIONS; i++) {
        printf("Partition = %d\n", i);
        for (j = 0; j <= last_index_per_partition[i]; j++) {
            printf("Key = %s, value = %s\n", partitions[i][j]->key, partitions[i][j]->value);
        }
    }
}

void cleanup() {
    free(partitions);
    free(last_index_per_partition);
    free(iterator_indices);
    free(partition_locks);
    free(partition_size);
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, Partitioner partition) {
    
    initialize(argc, argv, num_reducers, partition);
    execute_mappers(map, num_mappers);
    /*printf("--------------------\n");
    print_values();*/
    sort_keys();
    /*printf("--------------------\n");
    print_values();*/
    execute_reducers(reduce);
    cleanup();
}
