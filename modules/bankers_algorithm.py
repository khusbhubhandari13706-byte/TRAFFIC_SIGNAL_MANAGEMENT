
from database.connection import get_connection


# Resources currently considered by Banker's Algorithm.
TRAFFIC_RESOURCES = [
    "ROAD_SEGMENT_1",
    "ROAD_SEGMENT_2",
    "ZONE_NW",
    "ZONE_NE",
    "ZONE_SW",
    "ZONE_SE"
]


def get_road_resource_for_direction(direction):
    """Return the road resource used by a traffic direction."""

    direction = str(direction).upper()

    if direction in ("NORTH", "SOUTH"):
        return "ROAD_SEGMENT_1"

    if direction in ("EAST", "WEST"):
        return "ROAD_SEGMENT_2"

    raise ValueError(f"Invalid traffic direction: {direction}")



def register_max_claims(process_id, direction):
    """
    Register the maximum resources a traffic process may need.

    Each process may require:
    1. Its direction's road segment.
    2. The two intersection zones on its route.
    """

    connection = None
    cursor = None

    direction = str(direction).upper()

    # Straight-through intersection paths.
    zone_paths = {
        "NORTH": ("ZONE_NW", "ZONE_SW"),
        "SOUTH": ("ZONE_SE", "ZONE_NE"),
        "EAST": ("ZONE_NE", "ZONE_NW"),
        "WEST": ("ZONE_SW", "ZONE_SE")
    }

    if direction not in zone_paths:
        raise ValueError(f"Invalid traffic direction: {direction}")

    try:
        road_resource = get_road_resource_for_direction(direction)

        # Maximum claim = road segment + both intersection zones.
        required_resources = {
            road_resource,
            *zone_paths[direction]
        }

        connection = get_connection()
        connection.start_transaction()
        cursor = connection.cursor(dictionary=True)

        # Verify that the process exists and is active.
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

        if process is None or process["status"] not in (
            "RUNNING", "WAITING"
        ):
            raise ValueError(
                f"Process P{process_id} does not exist or is inactive."
            )

        # Fetch all six traffic resources.
        placeholders = ", ".join(["%s"] * len(TRAFFIC_RESOURCES))

        cursor.execute(
            f"""
            SELECT os_resource_id, resource_name
            FROM os_resources
            WHERE resource_name IN ({placeholders})
            """,
            tuple(TRAFFIC_RESOURCES)
        )

        resources = cursor.fetchall()

        resource_ids = {
            row["resource_name"]: row["os_resource_id"]
            for row in resources
        }

        if len(resource_ids) != len(TRAFFIC_RESOURCES):
            raise ValueError("Some traffic resources are missing.")

        # Register maximum claims for every resource.
        for resource_name in TRAFFIC_RESOURCES:

            maximum = (
                1 if resource_name in required_resources else 0
            )

            cursor.execute(
                """
                INSERT INTO os_resource_max_claims
                    (process_id, os_resource_id, max_instances)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    max_instances = VALUES(max_instances)
                """,
                (
                    process_id,
                    resource_ids[resource_name],
                    maximum
                )
            )

        connection.commit()

        print(
            f"Maximum claims registered for P{process_id}: "
            f"{road_resource}=1, "
            f"{', '.join(zone_paths[direction])}=1"
        )

        return True

    except Exception:
        if connection:
            connection.rollback()
        raise

    finally:
        if cursor:
            cursor.close()

        if connection and connection.is_connected():
            connection.close()

def get_bankers_state(process_ids=None):
    """
    Build Available, Allocation, Max and Need matrices
    using the existing MySQL tables.

    Every active process being checked must have an explicit
    maximum claim for all six traffic resources.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)

        # 1. Fetch traffic resource capacities.
        placeholders = ", ".join(["%s"] * len(TRAFFIC_RESOURCES))

        cursor.execute(
            f"""
            SELECT os_resource_id, resource_name, total_instances
            FROM os_resources
            WHERE resource_name IN ({placeholders})
            """,
            tuple(TRAFFIC_RESOURCES)
        )

        resource_rows = cursor.fetchall()

        resource_ids = {
            row["resource_name"]: row["os_resource_id"]
            for row in resource_rows
        }

        if len(resource_ids) != len(TRAFFIC_RESOURCES):
            raise ValueError("Some traffic resources are missing.")

        # Preserve a consistent resource order.
        resource_names = list(TRAFFIC_RESOURCES)

        total = {
            row["resource_name"]: row["total_instances"]
            for row in resource_rows
        }

        # 2. Fetch active processes.
        # Do not JOIN maximum claims here. Missing claims must
        # be detected instead of silently excluding processes.

        if process_ids is None:

            cursor.execute(
            """
            SELECT process_id, process_name
            FROM os_processes
            WHERE status IN ('RUNNING', 'WAITING')
            AND process_name NOT LIKE 'BROWSER_DL_%'
            ORDER BY process_id
            """
        )

        else:

            process_ids = sorted(set(process_ids))

            if not process_ids:
                process_rows = []
            else:
                process_placeholders = ", ".join(
                    ["%s"] * len(process_ids)
                )

                cursor.execute(
                    f"""
                    SELECT process_id, process_name
                    FROM os_processes
                    WHERE status IN ('RUNNING', 'WAITING')
                    AND process_id IN ({process_placeholders})
                    AND process_name NOT LIKE 'BROWSER_DL_%'
                    ORDER BY process_id
                    """,
                    tuple(process_ids)
                )
                process_rows = cursor.fetchall()

        if process_ids is None:
            process_rows = cursor.fetchall()

        processes = {
            row["process_id"]: row["process_name"]
            for row in process_rows
        }

        active_ids = list(processes.keys())

        allocation = {
            pid: {name: 0 for name in resource_names}
            for pid in active_ids
        }

        maximum = {
            pid: {name: 0 for name in resource_names}
            for pid in active_ids
        }

        # 3. Fetch ALL allocations to calculate Available.
        cursor.execute(
            """
            SELECT process_id, os_resource_id, COUNT(*) AS allocated
            FROM os_resource_allocations
            GROUP BY process_id, os_resource_id
            """
        )

        all_allocations = cursor.fetchall()

        allocated_total = {
            name: 0 for name in resource_names
        }

        id_to_name = {
            resource_ids[name]: name
            for name in resource_names
        }

        for row in all_allocations:

            resource_name = id_to_name.get(row["os_resource_id"])

            if resource_name is None:
                continue

            allocated_total[resource_name] += row["allocated"]

            pid = row["process_id"]

            if pid in allocation:
                allocation[pid][resource_name] = row["allocated"]

            elif pid not in processes:
                raise ValueError(
                    f"Inactive or unselected process P{pid} "
                    f"still holds {resource_name}."
                )

        # 4. Calculate Available.
        available = {}

        for name in resource_names:

            available[name] = total[name] - allocated_total[name]

            if available[name] < 0:
                raise ValueError(
                    f"Resource over-allocation detected: {name}"
                )

        # 5. Fetch maximum claims.
        if active_ids:

            process_placeholders = ", ".join(
                ["%s"] * len(active_ids)
            )

            cursor.execute(
                f"""
                SELECT process_id, os_resource_id, max_instances
                FROM os_resource_max_claims
                WHERE process_id IN ({process_placeholders})
                """,
                tuple(active_ids)
            )

            claims = cursor.fetchall()

            registered_claims = {
                (row["process_id"], row["os_resource_id"])
                for row in claims
            }

            # Validate that every active process has all six claims.
            for pid in active_ids:
                for name in resource_names:

                    resource_id = resource_ids[name]

                    if (pid, resource_id) not in registered_claims:
                        raise ValueError(
                            f"Missing maximum claim for process P{pid}, "
                            f"resource {name}. Register claims first."
                        )

            for row in claims:

                pid = row["process_id"]
                resource_name = id_to_name.get(
                    row["os_resource_id"]
                )

                if resource_name is not None:
                    maximum[pid][resource_name] = row["max_instances"]

        # 6. Calculate Need = Max - Allocation.
        need = {}

        for pid in active_ids:

            need[pid] = {}

            for name in resource_names:

                max_value = maximum[pid][name]
                allocated_value = allocation[pid][name]

                if max_value < allocated_value:
                    raise ValueError(
                        f"Max claim is less than Allocation "
                        f"for process P{pid}, resource {name}"
                    )

                if max_value > total[name]:
                    raise ValueError(
                        f"Max claim exceeds total capacity "
                        f"for resource {name}"
                    )

                need[pid][name] = max_value - allocated_value

        return {
            "processes": processes,
            "resource_names": resource_names,
            "total": total,
            "available": available,
            "allocation": allocation,
            "maximum": maximum,
            "need": need
        }

    finally:
        if cursor:
            cursor.close()

        if connection and connection.is_connected():
            connection.close()


def is_safe_state(state):
    """
    Banker's Algorithm safety check.
    Returns whether the system is in a safe state.
    """

    available = state["available"]
    allocation = state["allocation"]
    need = state["need"]

    process_ids = list(state["processes"].keys())
    resource_names = state["resource_names"]

    work = available.copy()

    finish = {
        pid: False for pid in process_ids
    }

    safe_sequence = []

    while len(safe_sequence) < len(process_ids):

        found = False

        for pid in process_ids:

            if finish[pid]:
                continue

            # Check Need <= Work.
            can_finish = all(
                need[pid][name] <= work[name]
                for name in resource_names
            )

            if can_finish:

                # Pretend this process completes.
                # Release its allocated resources.
                for name in resource_names:
                    work[name] += allocation[pid][name]

                finish[pid] = True
                safe_sequence.append(pid)
                found = True

        if not found:
            break

    unfinished = [
        pid for pid in process_ids
        if not finish[pid]
    ]

    return {
        "safe": len(safe_sequence) == len(process_ids),
        "safe_sequence": safe_sequence,
        "unfinished_processes": unfinished
    }


def check_bankers_safety(process_ids=None):
    """Check whether the current system state is safe."""

    state = get_bankers_state(process_ids)
    result = is_safe_state(state)

    return {
        "state": state,
        "result": result
    }


def check_request_safety(
    process_id,
    resource_name,
    request_amount=1
):
    """
    Check whether granting a resource request keeps
    the system in a safe state.

    This function does not modify the database.
    """

    if resource_name not in TRAFFIC_RESOURCES:
        raise ValueError(f"Unknown traffic resource: {resource_name}")

    if (
        not isinstance(request_amount, int)
        or isinstance(request_amount, bool)
        or request_amount <= 0
    ):
        raise ValueError("Request amount must be a positive integer.")

    # Build the current state of all active processes.
    state = get_bankers_state()

    if process_id not in state["processes"]:
        return {
            "status": "PROCESS_NOT_ACTIVE",
            "safe_sequence": []
        }

    # Check whether the request exceeds the process's remaining need.
    if request_amount > state["need"][process_id][resource_name]:
        return {
            "status": "EXCEEDS_MAX",
            "safe_sequence": []
        }

    # If the resource is not currently available, it must wait.
    if request_amount > state["available"][resource_name]:
        return {
            "status": "UNAVAILABLE",
            "safe_sequence": []
        }

    # Create a hypothetical copy of the current state.
    # Do not change the actual database state.
    hypothetical = {
        "processes": state["processes"].copy(),
        "resource_names": state["resource_names"].copy(),
        "total": state["total"].copy(),
        "available": state["available"].copy(),

        "allocation": {
            pid: values.copy()
            for pid, values in state["allocation"].items()
        },

        "maximum": {
            pid: values.copy()
            for pid, values in state["maximum"].items()
        },

        "need": {
            pid: values.copy()
            for pid, values in state["need"].items()
        }
    }

    # Pretend the requested resource has been granted.
    hypothetical["available"][resource_name] -= request_amount

    hypothetical["allocation"][process_id][resource_name] += (
        request_amount
    )

    hypothetical["need"][process_id][resource_name] -= (
        request_amount
    )

    # Run the safety algorithm on the hypothetical state.
    result = is_safe_state(hypothetical)

    return {
        "status": "SAFE" if result["safe"] else "UNSAFE",
        "safe_sequence": result["safe_sequence"],
        "unfinished_processes": result["unfinished_processes"]
    }
