import threading
import time
from queue import PriorityQueue

from database.connection import get_connection


# Lower number means higher priority.
PRIORITY = {
    "HIGH": 1,
    "MEDIUM": 2,
    "LOW": 3
}

# Green signal duration based on traffic density.
GREEN_TIME = {
    "HIGH": 8,
    "MEDIUM": 5,
    "LOW": 3
}

YELLOW_TIME = 1
ALL_RED_TIME = 1

DIRECTIONS = ("NORTH", "SOUTH", "EAST", "WEST")

# Protects signal-state transitions.
signal_lock = threading.Lock()


def log_event(event_type, details):
    """Save an event in the MySQL system_logs table."""

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO system_logs (event_type, details)
            VALUES (%s, %s)
            """,
            (event_type, details)
        )

        connection.commit()

    except Exception as error:
        if connection is not None:
            connection.rollback()

        print("Logging error:", error)

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def add_request(queue, sequence, direction, density, vehicle_count):
    """Validate and register a normal traffic request."""

    direction = direction.strip().upper()
    density = density.strip().upper()

    if direction not in DIRECTIONS:
        print("Invalid direction!")
        return sequence

    if density not in PRIORITY:
        print("Invalid density!")
        return sequence

    try:
        vehicle_count = int(vehicle_count)

        if vehicle_count < 0:
            print("Vehicle count cannot be negative!")
            return sequence

    except ValueError:
        print("Vehicle count must be an integer!")
        return sequence

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        # Save the traffic observation.
        cursor.execute(
            """
            INSERT INTO traffic_readings
                (signal_id, density, vehicle_count)
            SELECT signal_id, %s, %s
            FROM signals
            WHERE direction = %s
            """,
            (density, vehicle_count, direction)
        )

        if cursor.rowcount != 1:
            connection.rollback()
            print("Could not find the selected signal!")
            return sequence

        reading_id = cursor.lastrowid
        priority = PRIORITY[density]

        cursor.execute(
            """
            INSERT INTO system_logs (event_type, details)
            VALUES (%s, %s)
            """,
            (
                "TRAFFIC_REQUEST",
                f"Direction={direction}, density={density}, "
                f"vehicles={vehicle_count}, priority={priority}"
            )
        )

        connection.commit()

        # Priority first, arrival sequence second (FCFS).
        queue.put(
            (
                priority,
                sequence,
                reading_id,
                direction,
                density,
                vehicle_count
            )
        )

        print(
            f"Request added: {direction}, "
            f"density={density}, vehicles={vehicle_count}, "
            f"priority={priority}"
        )

        return sequence + 1

    except Exception as error:
        if connection is not None:
            connection.rollback()

        print("Could not save traffic request:", error)
        return sequence

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def update_signal_state(direction, state):
    """
    Update the signal state safely in MySQL.

    GREEN is assigned only after all other signals are set RED.
    Both changes are committed in one transaction.
    """

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        if state == "GREEN":
            # Ensure only one direction can be GREEN.
            cursor.execute(
                "UPDATE signals SET current_state = 'RED'"
            )

            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'GREEN'
                WHERE direction = %s
                """,
                (direction,)
            )

            if cursor.rowcount != 1:
                raise ValueError("Selected signal does not exist.")

        elif state == "RED":
            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'RED'
                WHERE direction = %s
                """,
                (direction,)
            )

        elif state == "YELLOW":
            # Keep every other direction RED.
            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'RED'
                WHERE direction != %s
                """,
                (direction,)
            )

            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'YELLOW'
                WHERE direction = %s
                """,
                (direction,)
            )

            if cursor.rowcount != 1:
                raise ValueError("Selected signal does not exist.")

        else:
            raise ValueError("Invalid signal state.")

        connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def set_all_red():
    """Set all directions to RED during clearance."""

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor()

        cursor.execute(
            "UPDATE signals SET current_state = 'RED'"
        )

        connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def serve_traffic(direction, density, vehicle_count, priority):
    """Run one complete, safely timed signal cycle."""

    with signal_lock:
        print("\n------------------------------")
        print("TRAFFIC DENSITY SCHEDULER")
        print("------------------------------")
        print("Direction:", direction)
        print("Density:", density)
        print("Vehicle count:", vehicle_count)
        print("Priority:", priority)

        try:
            # Begin from a safe all-red state.
            set_all_red()
            print("ALL RED: clearance phase")
            time.sleep(ALL_RED_TIME)

            # Give the selected direction a green signal.
            update_signal_state(direction, "GREEN")

            green_duration = GREEN_TIME[density]

            print(
                f"{direction}: GREEN for "
                f"{green_duration} seconds"
            )

            log_event(
                "PRIORITY_SCHEDULING",
                f"Direction={direction}, density={density}, "
                f"priority={priority}, green_time={green_duration}"
            )

            time.sleep(green_duration)

            # Transition from GREEN to YELLOW.
            update_signal_state(direction, "YELLOW")

            print(f"{direction}: YELLOW for {YELLOW_TIME} second(s)")
            time.sleep(YELLOW_TIME)

            # End the cycle with every direction RED.
            set_all_red()

            print("ALL RED: cycle completed")

            log_event(
                "SIGNAL_CYCLE_COMPLETED",
                f"Completed normal traffic cycle for {direction}"
            )

        except Exception as error:
            # If something fails, try to leave all signals RED.
            print("Signal scheduling error:", error)

            try:
                set_all_red()
            except Exception as safety_error:
                print("Could not restore ALL RED:", safety_error)

            log_event(
                "SIGNAL_ERROR",
                f"Direction={direction}; error={error}"
            )


def schedule_traffic(queue):
    """Process queued traffic requests in priority order."""

    while not queue.empty():
        priority, sequence, reading_id,direction, density, vehicle_count = (
            queue.get()
        )

        try:
            print(
                f"\nServing request #{sequence + 1}"
            )

            serve_traffic(
                direction,
                density,
                vehicle_count,
                priority
            )

        finally:
            queue.task_done()


def main():
    queue = PriorityQueue()
    sequence = 0

    print("\n=== TRAFFIC DENSITY MANAGEMENT ===")

    try:
        count = int(input("How many traffic requests? "))

        if count <= 0:
            print("Enter a positive number.")
            return

        for _ in range(count):
            print("\nEnter traffic request details")

            direction = input(
                "Direction (NORTH/SOUTH/EAST/WEST): "
            )

            density = input(
                "Density (LOW/MEDIUM/HIGH): "
            )

            vehicle_count = input(
                "Number of vehicles: "
            )

            sequence = add_request(
                queue,
                sequence,
                direction,
                density,
                vehicle_count
            )

        if queue.empty():
            print("No valid requests to schedule.")
            return

        print("\nStarting Traffic Scheduler...")

        # Run scheduling in a separate OS thread.
        scheduler_thread = threading.Thread(
            target=schedule_traffic,
            args=(queue,),
            name="TrafficScheduler"
        )

        scheduler_thread.start()
        scheduler_thread.join()

        print("\nAll traffic requests processed.")

    except ValueError:
        print("Please enter a valid number.")

    except KeyboardInterrupt:
        print("\nProgram interrupted by user.")


if __name__ == "__main__":
    main()