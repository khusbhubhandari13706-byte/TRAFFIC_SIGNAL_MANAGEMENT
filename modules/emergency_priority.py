import threading
from queue import PriorityQueue
from database.connection import get_connection


# Lower number means higher priority.
# This is our project's configurable simulation policy.
VEHICLE_PRIORITY = {
    "AMBULANCE": 1,
    "FIRE_TRUCK": 2,
    "POLICE": 3
}

VALID_DIRECTIONS = ("NORTH", "SOUTH", "EAST", "WEST")

emergency_lock = threading.Lock()


def register_emergency(queue, sequence, direction, vehicle_type, vehicle_count):
    direction = direction.upper().strip()
    vehicle_type = vehicle_type.upper().strip()

    # Validate direction.
    if direction not in VALID_DIRECTIONS:
        print("Invalid direction!")
        return sequence

    # Validate vehicle type.
    if vehicle_type not in VEHICLE_PRIORITY:
        print("Invalid vehicle type!")
        return sequence

    # Validate vehicle count.
    if not vehicle_count.isdigit() or int(vehicle_count) <= 0:
        print("Vehicle count must be a positive integer!")
        return sequence

    vehicle_count = int(vehicle_count)
    type_priority = VEHICLE_PRIORITY[vehicle_type]

    connection = None
    cursor = None

    try:
        with emergency_lock:
            connection = get_connection()
            cursor = connection.cursor()

            # The current schema requires these incident fields.
            # They are generated internally, not requested from the user.
            location = f"{direction} direction"
            incident_type = "EMERGENCY_VEHICLE"

            cursor.execute(
                """
                INSERT INTO incidents (location, incident_type, status)
                VALUES (%s, %s, 'OPEN')
                """,
                (location, incident_type)
            )

            incident_id = cursor.lastrowid

            # Save the emergency request.
            cursor.execute(
                """
                INSERT INTO emergency_requests
                (incident_id, vehicle_type, direction,
                 priority, vehicle_count, status)
                VALUES (%s, %s, %s, %s, %s, 'PENDING')
                """,
                (
                    incident_id,
                    vehicle_type,
                    direction,
                    type_priority,
                    vehicle_count
                )
            )

            request_id = cursor.lastrowid

            # Save an event in the system log.
            cursor.execute(
                """
                INSERT INTO system_logs (event_type, details)
                VALUES (%s, %s)
                """,
                (
                    "EMERGENCY_REQUEST",
                    f"{direction}: {vehicle_count} {vehicle_type}(s)"
                )
            )

            connection.commit()

            # PriorityQueue sorts tuples in ascending order:
            # 1. Vehicle type priority (smaller = higher priority)
            # 2. Negative vehicle count (larger count = higher priority)
            # 3. Sequence number (smaller = earlier request)
            queue.put(
                (
                    type_priority,
                    -vehicle_count,
                    sequence,
                    request_id,
                    incident_id,
                    direction,
                    vehicle_type,
                    vehicle_count
                )
            )

            print(
                f"\nEmergency request registered!"
                f"\nRequest ID: {request_id}"
                f"\nDirection: {direction}"
                f"\nVehicle type: {vehicle_type}"
                f"\nVehicle count: {vehicle_count}"
            )

            return sequence + 1

    except Exception as error:
        if connection is not None:
            connection.rollback()

        print("Could not register emergency:", error)
        return sequence

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def display_priority_order(queue):
    """
    Display emergency requests in scheduling order.
    Actual signal switching will be handled by the shared
    signal controller during integration.
    """

    requests = []

    # Remove requests in PriorityQueue order.
    while not queue.empty():
        requests.append(queue.get())

    print("\n========== EMERGENCY PRIORITY QUEUE ==========")

    for position, request in enumerate(requests, start=1):
        (
            type_priority,
            negative_count,
            sequence,
            request_id,
            incident_id,
            direction,
            vehicle_type,
            vehicle_count
        ) = request

        print(
            f"{position}. Request ID: {request_id} | "
            f"Direction: {direction} | "
            f"Type: {vehicle_type} | "
            f"Count: {vehicle_count}"
        )

        queue.task_done()

    print("==============================================")

    print(
        "\nNote: Requests remain PENDING until the shared "
        "signal controller starts serving them."
    )


def main():
    emergency_queue = PriorityQueue()
    sequence = 0

    try:
        number_of_requests = int(
            input("How many emergency requests? ")
        )

        if number_of_requests <= 0:
            print("Enter a positive number of requests.")
            return

        for i in range(number_of_requests):
            print(f"\n--- Emergency Request {i + 1} ---")

            direction = input(
                "Direction (NORTH/SOUTH/EAST/WEST): "
            )

            vehicle_type = input(
                "Vehicle type (AMBULANCE/FIRE_TRUCK/POLICE): "
            )

            vehicle_count = input(
                "Number of emergency vehicles: "
            )

            sequence = register_emergency(
                emergency_queue,
                sequence,
                direction,
                vehicle_type,
                vehicle_count
            )

        # Run the priority-queue display in a separate thread.
        worker = threading.Thread(
            target=display_priority_order,
            args=(emergency_queue,),
            name="EmergencyPriorityScheduler"
        )

        worker.start()
        worker.join()

    except ValueError:
        print("Please enter a valid number of requests.")


if __name__ == "__main__":
    main()