
from modules.deadlock_manager import (
    ensure_road_resources,
    create_process,
    get_resource_id,
    request_resource,
    detect_deadlock,
    recover_deadlock,
    finish_process
)

ensure_road_resources()

NW = get_resource_id("ZONE_NW")
NE = get_resource_id("ZONE_NE")
SW = get_resource_id("ZONE_SW")
SE = get_resource_id("ZONE_SE")

p1 = create_process("DEADLOCK_NORTH")
p2 = create_process("DEADLOCK_WEST")
p3 = create_process("DEADLOCK_SOUTH")
p4 = create_process("DEADLOCK_EAST")

processes = [p1, p2, p3, p4]

try:
    print("\n--- INITIAL ALLOCATION ---")
    print("P1 gets NW:", request_resource(p1, NW))
    print("P2 gets SW:", request_resource(p2, SW))
    print("P3 gets SE:", request_resource(p3, SE))
    print("P4 gets NE:", request_resource(p4, NE))

    print("\n--- CIRCULAR WAIT ---")
    print("P1 requests SW:", request_resource(p1, SW))
    print("P2 requests SE:", request_resource(p2, SE))
    print("P3 requests NE:", request_resource(p3, NE))
    print("P4 requests NW:", request_resource(p4, NW))

    print("\n--- DEADLOCK DETECTION ---")
    cycle = detect_deadlock(processes)
    print("Detected cycle:", cycle)

    if cycle:
        print("\nDEADLOCK DETECTED SUCCESSFULLY!")
        print("\n--- RECOVERY ---")
        print("Recovery result:", recover_deadlock(cycle))
    else:
        print("\nNo deadlock detected.")

finally:
    for pid in processes:
        finish_process(pid)

    print("\nTest cleanup completed.")
