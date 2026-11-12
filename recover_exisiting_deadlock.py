
from modules.deadlock_manager import (
    detect_deadlock,
    recover_deadlock,
    finish_process
)

print("\n--- GLOBAL DEADLOCK DETECTION ---")

# Check all active processes, not just newly created ones
cycle = detect_deadlock()

print("Detected cycle:", cycle)

if cycle:
    print("\nDEADLOCK DETECTED SUCCESSFULLY!")

    print("\n--- DEADLOCK RECOVERY ---")

    print("Recovery result:", recover_deadlock(cycle))

    # Clean up all processes involved in the cycle
    involved_processes = set(cycle)

    for pid in involved_processes:
        finish_process(pid)

    print("\nDeadlock recovery and cleanup completed.")

else:
    print("\nNo deadlock detected.")
