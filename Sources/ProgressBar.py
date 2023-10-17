import os

def fill_buffer() -> list[str] :
    buffer : list[str] = []
    for _ in range(0, os.get_terminal_size().columns - 2) :
        buffer += " "
    return buffer


def write_on_buffer(input : str, buffer : list[str]) -> list[str] :
    buffer[0:len(input)] = input
    buffer[len(input)] = "\0"
    return buffer

def print_buffer(buffer : list[str]) :
    # https://stackoverflow.com/questions/1228299/changing-one-character-in-a-string
    content = "".join(buffer)
    print(f"\r{content}", end="")

def draw_progress_bar(percentage : int) -> list[str] :
    buffer = fill_buffer()
    buffer[0] = "["
    buffer[-1] = "]"
    perc_str = f"{percentage}%"

    # Add some left padding
    if len(perc_str) == 2:
        perc_str = "  " + perc_str
    elif len(perc_str) == 3:
        perc_str = " " + perc_str

    # Some more padding so that we can breathe
    ### 100% ]
    perc_str = " " + perc_str + " "
    buffer[-5:-1] = perc_str

    # Compute remaining length accounting with the [] characters
    usable_length = len(buffer) - 2 - len(perc_str)
    number_of_hash = round(percentage * usable_length / 100)

    for i in range(1, number_of_hash + 1) :
        buffer[i] = "#"

    return buffer
