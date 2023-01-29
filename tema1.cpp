#include <vector>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <math.h>
#include <string.h>
#include <iterator>
#include <unordered_map>
using namespace std;

struct MapperData
{
    int id;
    vector<string> *files_available;
    int max_exponent;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    int mappers_counter;
    vector<unordered_map<int, unordered_set<int> > > *allLists;
};

struct ReducerData
{
    int id;
    vector<unordered_map<int, unordered_set<int> > > *allLists;
    pthread_barrier_t *barrier;
    pthread_mutex_t *mutex;
    unordered_set<int> *setToAdd;
};

int baseSearch(int exponent, int nr)
{
    int minBase = 1, maxBase = nr;
    while (minBase <= maxBase) {
        int mid = minBase + (maxBase - minBase) / 2;
        if (pow(mid, exponent) == nr) {
            return mid;
        }
        if (pow(mid, exponent) < nr) {
            minBase = mid + 1;
        }
        else {
            maxBase = mid - 1;
        }
    }
    return -1;
}

void is_perfect_power(int max_exponent, int nr, unordered_map<int, unordered_set<int> > *lists) {
    int exponent = 2;
    while(exponent <= max_exponent) {
        if (baseSearch(exponent, nr) != -1) {
            unordered_map<int, unordered_set<int> >::iterator it;
            it = lists->find(exponent);
            if (it != lists->end()) {
                lists->at(exponent).insert(nr);
            } else {
                unordered_set<int> set;
                set.insert(nr);
                lists->insert({exponent, set});
            }
        }
        exponent++;
    }
}

void proccess_file(string file_name, MapperData *mapper_data) {
    unordered_map<int, unordered_set<int> > partialLists;
    ifstream input_file;
    input_file.open(file_name);
    int integers_counter;
    input_file >> integers_counter;
    int i;
    for (i = 0; i < integers_counter; i++) {
        int nr_to_check;
        input_file >> nr_to_check;
        is_perfect_power(mapper_data->max_exponent, nr_to_check, &partialLists);
    }
    pthread_mutex_lock(mapper_data->mutex);
    mapper_data->allLists->push_back(partialLists);
    pthread_mutex_unlock(mapper_data->mutex);
}

void *reducer_function(void *arg) {
    ReducerData reducer_data = *(ReducerData *)arg;
    pthread_barrier_wait(reducer_data.barrier);
    for (long unsigned int i = 0; i < reducer_data.allLists->size(); i++) {
        unordered_map<int, unordered_set<int> >::iterator it = (*reducer_data.allLists)[i].find(reducer_data.id + 2);
        if (it != (*reducer_data.allLists)[i].end()) {
            reducer_data.setToAdd->insert(it->second.begin(), it->second.end());
        }
    }
    pthread_mutex_lock(reducer_data.mutex);
    string output_file_name;
    string out_string = "out";
    string txt_string = ".txt";
    string id_string = to_string(reducer_data.id + 2);
    output_file_name = out_string + id_string + txt_string;
    ofstream test_file;
    test_file.open(output_file_name);
    test_file << reducer_data.setToAdd->size();
    pthread_mutex_unlock(reducer_data.mutex);
    test_file.close();
    pthread_exit(NULL);
}

void *mapper_function(void *arg) {
    MapperData mapper_data = *(MapperData *)arg;
    while(!(*mapper_data.files_available).empty()) {
        pthread_mutex_lock(mapper_data.mutex);
        string file_to_process = "";
        if (!(*mapper_data.files_available).empty()) {
            file_to_process = (*mapper_data.files_available)[0];
            (*mapper_data.files_available).erase((*mapper_data.files_available).begin());
        }
        pthread_mutex_unlock(mapper_data.mutex);
        if (file_to_process != "") {
            proccess_file(file_to_process, &mapper_data);
        }
    }
    pthread_barrier_wait(mapper_data.barrier);
    pthread_exit(NULL);
}

bool isNumber(const string& s)
{
    std::string::const_iterator it;
    for (it = s.begin(); it != s.end(); ++it) {
        if (!isdigit(*it)) {
            return false;
        }
    }
    return true;
}

int main(int argc, char *argv[]) {
    int mapper_nr;
    int reducer_nr;
    mapper_nr = atoi(argv[1]);
    reducer_nr = atoi(argv[2]);
	pthread_t mid[mapper_nr];
	pthread_t rid[mapper_nr];
    ifstream test_file(argv[3]);
    vector<string> input_files;
    string str;

    // read the names of the input files
    while(getline(test_file, str)) {
        if (!isNumber(str)) {
            input_files.push_back(str);
        }
    }

    vector<unordered_map<int, unordered_set<int> > > allLists; // stores all the perfect powers from all the input files
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, mapper_nr + reducer_nr);
    pthread_mutex_init(&mutex, NULL);
    int maxim;

    if (mapper_nr >= reducer_nr) {
        maxim = mapper_nr;
    } else {
        maxim = reducer_nr;
    }

    int i;
    for (i = 0; i < maxim; i++) {
        if (i < mapper_nr) {
            MapperData *mappers_data = (MapperData *) malloc(sizeof(MapperData));
            mappers_data->max_exponent = reducer_nr + 1;
            mappers_data->files_available = &input_files;
            mappers_data->id = i;
            mappers_data->mutex = &mutex;
            mappers_data->allLists = &allLists;
            mappers_data->mappers_counter = mapper_nr;
            mappers_data->barrier = &barrier;
            pthread_create(&mid[i], NULL, mapper_function, mappers_data);
        }

        if (i < reducer_nr) {
            ReducerData *reducers_data = (ReducerData *) malloc(sizeof(ReducerData));
            reducers_data->allLists = &allLists;
            reducers_data->id = i;
            reducers_data->mutex = &mutex;
            reducers_data->barrier = &barrier;
            reducers_data->setToAdd = new unordered_set<int>();
            pthread_create(&rid[i], NULL, reducer_function, reducers_data);
        }
    }

    void *status;
    for (i = 0; i < mapper_nr; i++) {
		pthread_join(mid[i], &status);
	}
    for (i = 0; i < reducer_nr; i++) {
		pthread_join(rid[i], &status);
	}
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&mutex);
    
    return 0;
}
