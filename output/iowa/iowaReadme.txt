The binary structure of the test iowa.idx should* be as follows:
(Changes from the diagram are noted at the bottom)

Header: 
    Magic_number    (4 bytes, unsigned int)
    check_sum**     (4 bytes, unsigned int)
    num_nodes       (4 bytes, int)
----------------------------------------------------
Node Records:
    node_record 1:   
    .   num_vertices    (4 bytes, int)
    .   num_neighbors   (4 bytes, int)
    .   nodePos         (4 bytes, int)
    .   
    node_record n_1:
-----------------------------------------------------
Nodes:
    node 1:   
    .   node_ID         (4 bytes, int)
    .   vertex 1:
    .   .   x           (8 bytes, double)
    .   .   y           (8 bytes, double)
    .   .
    .   .
    .   vertex n_3: 
    .   neighbor_ID 1   (4 bytes, int)
    .   .
    .   .
    .   neighbor_ID n_4  
    .   demograhpics:   
    .   .   total_pop   (4 bytes, int)
    .   .   aa_pop      (4 bytes, int)
    .   .   ai_pop      (4 bytes, int)
    .   .   as_pop      (4 bytes, int)
    .   .   ca_pop      (4 bytes, int)
    .   .   other_pop   (4 bytes, int)
    .
    .
    node n_2:

Differences:
    -   vertex 'x' and 'y' values must be stored as doubles(8B) instead of flaots(4B)
    -   Demographic data is missing LA population (limitation of the source data),
            so I omited that value, and added a 'total_pop' value at the top of the 
            demographics structure
    -   check_sum is not yet implemented, for now expect 0xABBAABBA

* "should" means that I would like to run a few basic unit tests to veryify that the output
    script is working how I think it is.
** check_sum is not yet implemented, see differences above ^