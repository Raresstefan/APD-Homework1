# Parallel processing using the paradigm map-reduced

## Mappers

Mappers start first. The input files are assigned to mappers dynamically using a vector which stores the names of the files that

have to be processed. Once a file is assigned to a mapper it is erased from the vector and the next mapper will look for processing

one of the remaining files.

I decided to store all the perfect powers found in a file in a hashmap where the key is the exponent and the value is an unordered_set

that stores all the unique numbers that are perfect powers for that exponent.

## Reducers

Reducers will start just after the mappers are done with processing all the input files. This thing is protected by the barrier emplaced

at the beginning of the reducer_function. The reducers will look in the vector created by the mappers and will count all the unique values found

for the current exponent, which is the reducer_id added by 2(the indexing of the reducers starts from 0.).
