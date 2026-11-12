from database.connection import get_connection
from modules.bankers_algorithm import (
    TRAFFIC_RESOURCES,
    check_request_safety
)
from datetime import datetime
def test_database_connection():
    connection = None
    cursor = None

    try:
        connection = get_connection()

        if connection is None:
            print("Database connection failed.")
            return

        cursor = connection.cursor()

        cursor.execute("SELECT DATABASE()")
        result = cursor.fetchone()

        print("Connected database:", result[0])
        print("Deadlock Manager is ready!")

    except Exception as error:
        print("Database error:", error)

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def create_process(process_name):
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO os_processes (process_name, status)
            VALUES (%s, 'RUNNING')
            """,
            (process_name,)
        )

        connection.commit()
        process_id = cursor.lastrowid

        print(f"Process created: {process_name}, ID: {process_id}")
        return process_id

    except Exception as error:
        if connection:
            connection.rollback()
        print("Error creating process:", error)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def create_resource(resource_name, total_instances=1):
    connection = None
    cursor = None

    try:
        if total_instances < 1:
            print("Resource instances must be at least 1.")
            return None

        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO os_resources (resource_name, total_instances)
            VALUES (%s, %s)
            """,
            (resource_name, total_instances)
        )

        connection.commit()
        resource_id = cursor.lastrowid

        print(f"Resource created: {resource_name}, ID: {resource_id}")
        return resource_id

    except Exception as error:
        if connection:
            connection.rollback()
        print("Error creating resource:", error)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()




def request_resource(process_id, resource_id):
    """
    Request a resource.
    True  = granted
    False = waiting (unavailable or unsafe)
    None  = error
    """

    connection = None
    cursor = None
    lock_acquired = False

    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Serialize resource requests during the safety check
        # and allocation operation.
        cursor.execute(
            "SELECT GET_LOCK(%s, 10)",
            ("TRAFFIC_BANKERS_LOCK",)
        )

        lock_result = cursor.fetchone()

        if not lock_result or lock_result[0] != 1:
            print("Could not acquire Banker's allocation lock.")
            return None

        lock_acquired = True

        # 1. Verify that the process exists and is active.
        cursor.execute(
            """
            SELECT status
            FROM os_processes
            WHERE process_id = %s
            """,
            (process_id,)
        )
        process = cursor.fetchone()

        if not process or process[0] in ("COMPLETED", "ABORTED"):
            return None

        # 2. Identify the requested resource.
        cursor.execute(
            """
            SELECT resource_name, total_instances
            FROM os_resources
            WHERE os_resource_id = %s
            """,
            (resource_id,)
        )
        resource = cursor.fetchone()

        if not resource:
            return None

        resource_name, total_instances = resource

        # 3. Check whether this process already has a request.
        cursor.execute(
            """
            SELECT request_id, request_status
            FROM os_resource_requests
            WHERE process_id = %s
              AND os_resource_id = %s
              AND request_status IN ('WAITING', 'GRANTED')
            LIMIT 1
            """,
            (process_id, resource_id)
        )
        existing_request = cursor.fetchone()

        if existing_request:
            if existing_request[1] == "GRANTED":
                return True

        # 4. Run Banker's Algorithm for actual traffic resources.
        banker_status = "SAFE"

        if resource_name in TRAFFIC_RESOURCES:

            decision = check_request_safety(
                process_id,
                resource_name,
                request_amount=1
            )

            banker_status = decision["status"]

            if banker_status not in ("SAFE", "UNSAFE", "UNAVAILABLE"):
                print(
                    f"Banker's Algorithm error for P{process_id}: "
                    f"{banker_status}"
                )
                return None

        # 5. Begin the actual database transaction.
        connection.commit()
        connection.start_transaction()

        cursor.execute(
            """
            SELECT status
            FROM os_processes
            WHERE process_id = %s
            FOR UPDATE
            """,
            (process_id,)
        )
        process = cursor.fetchone()

        if not process or process[0] in ("COMPLETED", "ABORTED"):
            connection.rollback()
            return None

        cursor.execute(
            """
            SELECT total_instances
            FROM os_resources
            WHERE os_resource_id = %s
            FOR UPDATE
            """,
            (resource_id,)
        )
        resource = cursor.fetchone()

        if not resource:
            connection.rollback()
            return None

        # 6. Check actual resource availability again.
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM os_resource_allocations
            WHERE os_resource_id = %s
            """,
            (resource_id,)
        )
        allocated = cursor.fetchone()[0]

        available = allocated < resource[0]

        # 7. Grant only if both conditions are satisfied.
        if banker_status == "SAFE" and available:

            cursor.execute(
                """
                INSERT INTO os_resource_allocations
                    (process_id, os_resource_id)
                VALUES (%s, %s)
                """,
                (process_id, resource_id)
            )

            if existing_request:
                cursor.execute(
                    """
                    UPDATE os_resource_requests
                    SET request_status = 'GRANTED'
                    WHERE request_id = %s
                    """,
                    (existing_request[0],)
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO os_resource_requests
                        (process_id, os_resource_id, request_status)
                    VALUES (%s, %s, 'GRANTED')
                    """,
                    (process_id, resource_id)
                )

            cursor.execute(
                """
                UPDATE os_processes
                SET status = 'RUNNING'
                WHERE process_id = %s
                """,
                (process_id,)
            )

            connection.commit()

            print(
                f"Resource granted to P{process_id}: {resource_name} "
                f"| Banker's status: {banker_status}"
            )
            return True

        # 8. Unsafe or unavailable requests remain waiting.
        if existing_request:
            cursor.execute(
                """
                UPDATE os_resource_requests
                SET request_status = 'WAITING'
                WHERE request_id = %s
                """,
                (existing_request[0],)
            )
        else:
            cursor.execute(
                """
                INSERT INTO os_resource_requests
                    (process_id, os_resource_id, request_status)
                VALUES (%s, %s, 'WAITING')
                """,
                (process_id, resource_id)
            )

        cursor.execute(
            """
            UPDATE os_processes
            SET status = 'WAITING'
            WHERE process_id = %s
            """,
            (process_id,)
        )

        connection.commit()

        print(
            f"Resource not granted to P{process_id}: {resource_name} "
            f"| Banker's status: {banker_status}"
        )
        return False

    except Exception as error:
        if connection:
            connection.rollback()

        print("Resource request error:", error)
        return None

    finally:
        if lock_acquired and cursor and connection:
            try:
                cursor.execute(
                    "SELECT RELEASE_LOCK(%s)",
                    ("TRAFFIC_BANKERS_LOCK",)
                )
                cursor.fetchone()  # Consume the SELECT result.
            except Exception as error:
                print("Could not release Banker's lock:", error)
        if cursor:
            cursor.close()

        if connection:
            connection.close()


def detect_deadlock(process_ids=None):
    """
    Detect circular wait.
    If process_ids is supplied, check only those processes.
    """
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Optional process filter for isolated terminal tests.
        process_ids = (
            sorted(set(process_ids))
            if process_ids is not None else None
        )

        if process_ids is not None and not process_ids:
            print("\nNo processes to check. No deadlock detected.")
            return []

        # 1. Read resource capacities.
        cursor.execute("""
            SELECT os_resource_id, total_instances
            FROM os_resources
        """)
        resources = dict(cursor.fetchall())

        # 2. Read resource allocations.
        if process_ids is None:
            cursor.execute("""
                SELECT process_id, os_resource_id
                FROM os_resource_allocations
            """)
        else:
            placeholders = ",".join(["%s"] * len(process_ids))
            cursor.execute(
                f"""
                SELECT process_id, os_resource_id
                FROM os_resource_allocations
                WHERE process_id IN ({placeholders})
                """,
                tuple(process_ids)
            )

        allocations = cursor.fetchall()

        holders = {}
        for process_id, resource_id in allocations:
            holders.setdefault(resource_id, []).append(process_id)

        # 3. Read waiting requests.
        if process_ids is None:
            cursor.execute("""
                SELECT process_id, os_resource_id
                FROM os_resource_requests
                WHERE request_status = 'WAITING'
            """)
        else:
            placeholders = ",".join(["%s"] * len(process_ids))
            cursor.execute(
                f"""
                SELECT process_id, os_resource_id
                FROM os_resource_requests
                WHERE request_status = 'WAITING'
                  AND process_id IN ({placeholders})
                """,
                tuple(process_ids)
            )

        waiting_requests = cursor.fetchall()

        # 4. Build wait-for graph.
        graph = {}

        for process_id, resource_id in waiting_requests:
            current_holders = holders.get(resource_id, [])
            capacity = resources.get(resource_id, 0)

            if len(current_holders) >= capacity:
                for holder_id in current_holders:
                    if holder_id != process_id:
                        graph.setdefault(process_id, set()).add(holder_id)

        # 5. Detect a cycle using DFS.
        visited = set()
        active = set()
        path = []
        deadlock_cycle = []

        def dfs(process_id):
            nonlocal deadlock_cycle

            visited.add(process_id)
            active.add(process_id)
            path.append(process_id)

            for neighbour in graph.get(process_id, set()):
                if neighbour not in visited:
                    if dfs(neighbour):
                        return True

                elif neighbour in active:
                    cycle_start = path.index(neighbour)
                    deadlock_cycle = path[cycle_start:] + [neighbour]
                    return True

            path.pop()
            active.remove(process_id)
            return False

        for process_id in graph:
            if process_id not in visited:
                if dfs(process_id):
                    break

        # 6. Report result.
        if deadlock_cycle:
            cycle_text = " -> ".join(
                f"P{pid}" for pid in deadlock_cycle
            )
            print("\nDEADLOCK DETECTED!")
            print("Circular wait:", cycle_text)
            return deadlock_cycle

        print("\nNo deadlock detected.")
        return []

    except Exception as error:
        print("Deadlock detection error:", error)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def recover_deadlock(cycle=None):
    # Detect a deadlock if the cycle was not supplied.
    if cycle is None:
        cycle = detect_deadlock()

    if cycle is None or len(cycle) < 2:
        print("Recovery skipped: no deadlock cycle available.")
        return

    # Select the second process in the cycle as the victim.
    victim_id = cycle[-2]

    connection = None
    cursor = None
    waiting_candidates = []
    try:
        connection = get_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        # 1. Check whether the victim can be aborted.
        cursor.execute(
            """
            SELECT status
            FROM os_processes
            WHERE process_id = %s
            FOR UPDATE
            """,
            (victim_id,)
        )
        victim = cursor.fetchone()

        if victim is None or victim[0] in ("COMPLETED", "ABORTED"):
            connection.rollback()
            print("Recovery cancelled: victim is not active.")
            return

        # 2. Find the resources held by the victim.
        cursor.execute(
            """
            SELECT os_resource_id
            FROM os_resource_allocations
            WHERE process_id = %s
            """,
            (victim_id,)
        )
        released_resources = {
            row[0] for row in cursor.fetchall()
        }

        # 3. Release all resources held by the victim.
        cursor.execute(
            """
            DELETE FROM os_resource_allocations
            WHERE process_id = %s
            """,
            (victim_id,)
        )

        # 4. Cancel the victim's outstanding requests.
        cursor.execute(
            """
            UPDATE os_resource_requests
            SET request_status = 'CANCELLED'
            WHERE process_id = %s
              AND request_status IN ('WAITING', 'GRANTED')
            """,
            (victim_id,)
        )

        # 5. Mark the victim as aborted.
        cursor.execute(
            """
            UPDATE os_processes
            SET status = 'ABORTED'
            WHERE process_id = %s
            """,
            (victim_id,)
        )

        
        # 6. Collect waiting processes for the released resources.
        # Actual allocation will be performed after commit through
        # request_resource(), which checks Banker's safety.

        for resource_id in sorted(released_resources):

            cursor.execute(
                """
                SELECT request_id, process_id
                FROM os_resource_requests
                WHERE os_resource_id = %s
                AND request_status = 'WAITING'
                ORDER BY request_id
                """,
                (resource_id,)
            )

            waiting_requests = cursor.fetchall()

            for waiting_request_id, waiting_process_id in waiting_requests:

                cursor.execute(
                    """
                    SELECT status
                    FROM os_processes
                    WHERE process_id = %s
                    """,
                    (waiting_process_id,)
                )

                waiting_process = cursor.fetchone()

                if (
                    waiting_process is None
                    or waiting_process[0] in ("COMPLETED", "ABORTED")
                ):
                    cursor.execute(
                        """
                        UPDATE os_resource_requests
                        SET request_status = 'CANCELLED'
                        WHERE request_id = %s
                        """,
                        (waiting_request_id,)
                    )
                    continue

                waiting_candidates.append(
                    (waiting_process_id, resource_id)
                )


        connection.commit()

        # 7. Retry waiting requests through Banker's Algorithm.
        for waiting_process_id, resource_id in waiting_candidates:

            granted = request_resource(waiting_process_id, resource_id)

            if granted:
                print(
                    f"Waiting process P{waiting_process_id} "
                    f"received resource {resource_id} safely."
                )
            else:
                print(
                    f"Process P{waiting_process_id} remains waiting "
                    f"for resource {resource_id}."
                )
        print(f"\nRecovery successful. Victim: P{victim_id}")
        print("Victim status: ABORTED")
        print("Released resources:", sorted(released_resources))
        print("Waiting processes have been checked for allocation.")

    except Exception as error:
        if connection:
            connection.rollback()
        print("Deadlock recovery error:", error)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()



def ensure_road_resources():
    """Create shared road and intersection resources if they do not already exist."""
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        road_resources = [
            ("ROAD_SEGMENT_1", 1),
            ("ROAD_SEGMENT_2", 1),

            # Four logical intersection zones
            ("ZONE_NW", 1),
            ("ZONE_NE", 1),
            ("ZONE_SW", 1),
            ("ZONE_SE", 1)
        ]

        for resource_name, total_instances in road_resources:
            cursor.execute(
                """
                INSERT INTO os_resources
                    (resource_name, total_instances)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE
                    total_instances = VALUES(total_instances)
                """,
                (resource_name, total_instances)
            )

        connection.commit()
        print("Road and intersection resources are ready.")

    except Exception as error:
        if connection:
            connection.rollback()
        print("Could not initialize road resources:", error)

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# -----------------------------------------
# OS RESOURCE HELPERS FOR TRAFFIC INTEGRATION
# -----------------------------------------

def get_resource_id(resource_name):
    """Return the database ID of a named OS resource."""
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT os_resource_id
            FROM os_resources
            WHERE resource_name = %s
            """,
            (resource_name,)
        )

        row = cursor.fetchone()
        return row[0] if row else None

    except Exception as error:
        print("Could not find OS resource:", error)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_intersection_zone_path(direction):
    """Return the intersection zones occupied by a vehicle."""

    zone_paths = {
        "NORTH": ("ZONE_NW", "ZONE_SW"),
        "SOUTH": ("ZONE_SE", "ZONE_NE"),
        "EAST": ("ZONE_NE", "ZONE_NW"),
        "WEST": ("ZONE_SW", "ZONE_SE")
    }

    return zone_paths.get(str(direction).upper())

def get_process_status(process_id):
    """Return the current status of an OS process."""
    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            "SELECT status FROM os_processes WHERE process_id = %s",
            (process_id,)
        )

        row = cursor.fetchone()
        return row[0] if row else None

    except Exception as error:
        print("Could not read process status:", error)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def finish_process(process_id, final_status="COMPLETED"):
    """
    Finish an OS process and release its allocated resources.
    Waiting processes are considered for released resources.
    """
    if final_status not in ("COMPLETED", "ABORTED"):
        print("Invalid final process status.")
        return False

    connection = None
    cursor = None
    waiting_candidates = []
    try:
        connection = get_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT status
            FROM os_processes
            WHERE process_id = %s
            FOR UPDATE
            """,
            (process_id,)
        )

        process = cursor.fetchone()

        if process is None:
            connection.rollback()
            print("Cannot finish: process does not exist.")
            return False

        if process[0] in ("COMPLETED", "ABORTED"):
            connection.rollback()
            return True

        # Remember which resources this process holds.
        cursor.execute(
            """
            SELECT os_resource_id
            FROM os_resource_allocations
            WHERE process_id = %s
            """,
            (process_id,)
        )
        released_resources = {row[0] for row in cursor.fetchall()}

        # Release all resources held by this process.
        cursor.execute(
            "DELETE FROM os_resource_allocations WHERE process_id = %s",
            (process_id,)
        )

        # Cancel outstanding requests belonging to this process.
        cursor.execute(
            """
            UPDATE os_resource_requests
            SET request_status = 'CANCELLED'
            WHERE process_id = %s
              AND request_status IN ('WAITING', 'GRANTED')
            """,
            (process_id,)
        )

        cursor.execute(
            "UPDATE os_processes SET status = %s WHERE process_id = %s",
            (final_status, process_id)
        )

        # Reconsider waiting processes for each released resource.
        
# Collect waiting processes for released resources.
# Actual allocation will happen after commit through
# request_resource(), which performs Banker's safety checking.

        for resource_id in sorted(released_resources):

            cursor.execute(
                """
                SELECT request_id, process_id
                FROM os_resource_requests
                WHERE os_resource_id = %s
                AND request_status = 'WAITING'
                ORDER BY request_id
                """,
                (resource_id,)
            )

            waiting_requests = cursor.fetchall()

            for waiting_request_id, waiting_process_id in waiting_requests:

                cursor.execute(
                    """
                    SELECT status
                    FROM os_processes
                    WHERE process_id = %s
                    """,
                    (waiting_process_id,)
                )

                waiting_process = cursor.fetchone()

                if (
                    waiting_process is None
                    or waiting_process[0] in ("COMPLETED", "ABORTED")
                ):
                    cursor.execute(
                        """
                        UPDATE os_resource_requests
                        SET request_status = 'CANCELLED'
                        WHERE request_id = %s
                        """,
                        (waiting_request_id,)
                    )
                    continue

                waiting_candidates.append(
                    (waiting_process_id, resource_id)
                )


        connection.commit()

        # Retry waiting requests through the Banker's safety check.
        for waiting_process_id, resource_id in waiting_candidates:

            granted = request_resource(waiting_process_id, resource_id)

            if granted:
                print(
                    f"Waiting process P{waiting_process_id} "
                    f"received resource {resource_id} safely."
                )
            else:
                print(
                    f"Process P{waiting_process_id} remains waiting "
                    f"for resource {resource_id}."
                )
        print(
            f"OS process P{process_id} marked {final_status}; "
            f"released resources: {sorted(released_resources)}"
        )
        return True

    except Exception as error:
        if connection:
            connection.rollback()
        print("Could not finish OS process:", error)
        return False

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def release_process_resources(process_id):
    """
    Release resources temporarily without terminating the process.
    Used when normal traffic is preempted by emergency traffic.
    """
    connection = None
    cursor = None
    waiting_candidates = []
    try:
        connection = get_connection()
        connection.start_transaction()
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT status
            FROM os_processes
            WHERE process_id = %s
            FOR UPDATE
            """,
            (process_id,)
        )

        process = cursor.fetchone()

        if process is None or process[0] in ("COMPLETED", "ABORTED"):
            connection.rollback()
            return False

        cursor.execute(
            """
            SELECT os_resource_id
            FROM os_resource_allocations
            WHERE process_id = %s
            """,
            (process_id,)
        )

        released_resources = {
            row[0] for row in cursor.fetchall()
        }

        cursor.execute(
            """
            DELETE FROM os_resource_allocations
            WHERE process_id = %s
            """,
            (process_id,)
        )

        cursor.execute(
            """
            UPDATE os_resource_requests
            SET request_status = 'CANCELLED'
            WHERE process_id = %s
              AND request_status IN ('WAITING', 'GRANTED')
            """,
            (process_id,)
        )

        cursor.execute(
            """
            UPDATE os_processes
            SET status = 'WAITING'
            WHERE process_id = %s
            """,
            (process_id,)
        )
        
        # Collect waiting processes for the released resources.
        # Actual allocation will happen after commit through
        # request_resource(), which checks Banker's safety.

        for resource_id in sorted(released_resources):

            cursor.execute(
                """
                SELECT request_id, process_id
                FROM os_resource_requests
                WHERE os_resource_id = %s
                  AND request_status = 'WAITING'
                ORDER BY request_id
                """,
                (resource_id,)
            )

            waiting_requests = cursor.fetchall()

            for waiting_request_id, waiting_process_id in waiting_requests:

                cursor.execute(
                    """
                    SELECT status
                    FROM os_processes
                    WHERE process_id = %s
                    """,
                    (waiting_process_id,)
                )

                waiting_process = cursor.fetchone()

                if (
                    waiting_process is None
                    or waiting_process[0] in ("COMPLETED", "ABORTED")
                ):
                    cursor.execute(
                        """
                        UPDATE os_resource_requests
                        SET request_status = 'CANCELLED'
                        WHERE request_id = %s
                        """,
                        (waiting_request_id,)
                    )
                    continue

                waiting_candidates.append(
                    (waiting_process_id, resource_id)
                )


        connection.commit()
                # Retry waiting requests through Banker's Algorithm.
        for waiting_process_id, resource_id in waiting_candidates:

            granted = request_resource(waiting_process_id, resource_id)

            if granted:
                print(
                    f"Waiting process P{waiting_process_id} "
                    f"received resource {resource_id} safely."
                )
            else:
                print(
                    f"Process P{waiting_process_id} remains waiting "
                    f"for resource {resource_id}."
                )
        print(
            f"Process P{process_id} paused; "
            f"released resources: {sorted(released_resources)}"
        )
        return True

    except Exception as error:
        if connection:
            connection.rollback()
        print("Could not release process resources:", error)
        return False

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def run_deadlock_demo():
    """Create an isolated test scenario and demonstrate detection and recovery."""

    suffix = datetime.now().strftime("%Y%m%d%H%M%S")

    print("\n========== DEADLOCK TEST ==========")

    # Create two independent test processes.
    p1 = create_process(f"DL_P1_{suffix}")
    p2 = create_process(f"DL_P2_{suffix}")

    # Create two unique resources, each with one instance.
    r1 = create_resource(f"DL_R1_{suffix}", 1)
    r2 = create_resource(f"DL_R2_{suffix}", 1)

    if None in (p1, p2, r1, r2):
        print("Could not create the test setup. Check the database errors.")
        return

    print(f"\nProcess P1 ID: {p1}")
    print(f"Process P2 ID: {p2}")
    print(f"Resource R1 ID: {r1}")
    print(f"Resource R2 ID: {r2}")

    # Phase 1: Each process acquires a different resource.
    print("\n--- Phase 1: Allocate resources ---")

    if request_resource(p1, r1) is not True:
        print("Could not allocate R1 to P1.")
        return

    if request_resource(p2, r2) is not True:
        print("Could not allocate R2 to P2.")
        return

    print("P1 holds R1.")
    print("P2 holds R2.")

    # Phase 2: Each process requests the resource held by the other.
    print("\n--- Phase 2: Create waiting requests ---")

    result1 = request_resource(p1, r2)
    result2 = request_resource(p2, r1)

    if result1 is not False or result2 is not False:
        print("The expected waiting state was not created.")
        print("Check the resource request results before proceeding.")
        return

    print("P1 is waiting for R2.")
    print("P2 is waiting for R1.")

    # Phase 3: Detect a cycle involving only these test processes.
    print("\n--- Phase 3: Detect deadlock ---")

    cycle = detect_deadlock([p1, p2])

    if cycle is None:
        print("Detection failed. Check the database error above.")
        return

    if not cycle:
        print("No deadlock detected in the test processes.")
        return

    print("Deadlock detected:", " -> ".join(map(str, cycle)))

    # Phase 4: Recover only if a deadlock was detected.
    choice = input("Do you want to recover from this deadlock? (y/n): ")
    if choice.strip().lower() == "y":
        recover_deadlock(cycle)

        print("\n--- Phase 5: Verify recovery ---")
        remaining_cycle = detect_deadlock([p1, p2])

        if remaining_cycle is None:
            print("Could not verify recovery because detection failed.")
        elif remaining_cycle:
            print("A deadlock cycle still exists:", remaining_cycle)
        else:
            print("No deadlock remains among the test processes.")

    else:
        print("Recovery skipped. Test records remain in the database.")

    print("\n========== TEST FINISHED ==========")


if __name__ == "__main__":
    test_database_connection()
    ensure_road_resources()

    while True:
        print("\n====== DEADLOCK MANAGER ======")
        print("1. Run deadlock detection and recovery demo")
        print("2. Check database connection")
        print("3. Exit")

        choice = input("Enter your choice: ").strip()

        if choice == "1":
            run_deadlock_demo()
        elif choice == "2":
            test_database_connection()
        elif choice == "3":
            print("Exiting Deadlock Manager.")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")
