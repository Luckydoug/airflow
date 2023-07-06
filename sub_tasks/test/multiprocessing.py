import multiprocessing

def worker():
    print("Worker process")

if __name__ == "__main__":
    # Create a new process
    process = multiprocessing.Process(target=worker)

    # Start the process
    process.start()

    # Wait for the process to complete
    process.join()
