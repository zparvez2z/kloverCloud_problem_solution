def find_earlest_time(x, Array):
  
    steps_to_take = set([i for i in range(1, x + 1)])
    steps_taken = set()

    for index, leaf in enumerate(Array):
        steps_taken.add(leaf)
        if steps_taken == steps_to_take:
            return index
    return -1

if __name__ == "__main__":
    earliest_time = find_earlest_time(5,[1,3,1,4,2,3,5,4])
    print("earliest_time: {}".format(earliest_time))