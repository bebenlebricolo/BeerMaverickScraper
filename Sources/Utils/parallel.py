import os
from typing import Generic, TypeVar


T = TypeVar("T")

def spread_load_for_parallel(input_list : list[T], num_jobs : int = -1) -> list[list[T]] :
    if num_jobs == 1 :
        return [input_list]
    
    # Default to number of cores, even if it's not really a good metric in Python ecosystem (GIL)
    # It's just there to provide a default when upper layers of code don't know (or don't care) about how much jobs can be done in parallel.
    # Furthermore, as this function is used in the context of I/O bound computation, we won't be leveraging multiple cores efficiently anyway.
    if num_jobs == -1:
        num_jobs = os.cpu_count()

    remainder = len(input_list) % num_jobs
    item_per_list = len(input_list) // num_jobs

    output_matrix : list[list[str]] = []
    start_index = 0
    for _ in range(0,num_jobs) :
        end_index = start_index + item_per_list

        if remainder > 0 :
            end_index += 1
            remainder -= 1

        output_matrix.append(input_list[start_index:end_index])
        
        # Next start is previous end
        start_index = end_index

    return output_matrix