from typing import Generic, TypeVar

T = TypeVar("T")

def spread_load_for_parallel(input_list : list[T], num_jobs : int = -1) -> list[list[T]] :
    if num_jobs == -1 :
        return [input_list]

    remainder = len(input_list) % num_jobs
    item_per_list = len(input_list) // num_jobs

    output_matrix : list[list[str]] = []
    for i in range(0,num_jobs) :
        start_index = i * item_per_list
        end_index = (i + 1) * item_per_list + 1

        if remainder != 0 :
            end_index += 1
            start_index += 1
            remainder -= 1

        output_matrix.append(input_list[start_index:end_index])

    return output_matrix