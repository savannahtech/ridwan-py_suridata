import random
from multiprocessing import Manager, Pool
from dataset import employees as original_employees


def clean_data(employees: list) -> list:
    """Clean duplicates from the data source using fields combination."""
    seen = set()  # keep track of seen combinations
    for each in employees:
        key = (each["name"], each["department"], each["age"])
        if key not in seen:
            seen.add(key)
            yield each


def chunk_generator(data, chunk_size):
    """Generate cleaned chunks of data."""
    chunk = []

    # Loop through cleaned data and yield chunks of specified size
    for item in clean_data(data):
        chunk.append(item)

        # Check if the chunk is of the desired size
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []  # empty the chunk container for the next iteration

    # Yield the remaining items if any that do not match the size
    if chunk:
        yield chunk


def generate_pairs(
    chunked_employees: list, paired: list, first_employees, used_first_employees, lock
):
    """Generate random pairs ensuring uniqueness and fairness."""
    random.shuffle(chunked_employees)

    while len(chunked_employees) >= 2:
        employee1 = chunked_employees.pop()["name"]
        employee2 = chunked_employees.pop()["name"]
        new_pair = (employee1, employee2)
        paired.append(new_pair)
        first_employees.append(employee1)
      
        # create another pair where employee2 is first i.e (employee2, x)
        random_employee = None
        with lock:
            while not random_employee:
                try:
                    random_employee = random.choice(first_employees)
                    if random_employee == employee1:  # continue pair search
                        random_employee = None
                except IndexError:
                    random_employee = None
                if random_employee:
                    if random_employee not in used_first_employees:
                        used_first_employees.append(random_employee)
                        paired.append((employee2, random_employee))
                        break
                    else:
                        if len(first_employees) - len(used_first_employees) == 1:
                            set_a = set(first_employees)
                            set_b = set(used_first_employees)
                            set_diff = set_a - set_b
                            remaining_employee = set_diff.pop()
                            paired.append((employee2, remaining_employee))
                            break
                        random_employee = None


def main(employees, chunk_size=30):
    # Aim for an even chunk size(data) to facilitate pairing
    # Note: Preset size of each chunk must be even.
    chunks = list(chunk_generator(employees, chunk_size))

    with Manager() as manager:
        paired = manager.list()
        first_employees = manager.list()
        used_first_employees = manager.list()
        lock = manager.Lock()

        with Pool() as pool:
            pool.starmap(
                generate_pairs,
                [
                    (chunk, paired, first_employees, used_first_employees, lock)
                    for chunk in chunks
                ],
            )

        paired_list = list(paired)

    return paired_list


if __name__ == "__main__":
    unique_pairs = main(original_employees)
    print("Final Unique Pairs:", unique_pairs)
    print("Length of pairs:", len(unique_pairs))
