import threading
import time
from queue import PriorityQueue, Empty

from database.connection import get_connection


VALID_DIRECTIONS = ("NORTH", "SOUTH", "EAST", "WEST")

NORMAL_GREEN_TIME = {
    "HIGH": 8,
    "MEDIUM": 5,
    "LOW": 3
}

EMERGENCY_GREEN_TIME = 6

YELLOW_TIME = 1
ALL_RED_TIME = 1


class SignalController:

    def __init__(self):
        # Normal traffic: density priority, then FCFS.
        self.normal_queue = PriorityQueue()

        # Emergency traffic: vehicle-type priority,
        # then vehicle count, then FCFS.
        self.emergency_queue = PriorityQueue()

        # Shared synchronization lock.
        self.signal_lock = threading.Lock()

        # Used to interrupt normal green when an
        # emergency request arrives.
        self.emergency_event = threading.Event()

        self.wake_event = threading.Event()
        self.stop_event = threading.Event()

    # -----------------------------------------
    # LOG EVENTS IN DATABASE
    # -----------------------------------------
    def log_event(self, event_type, details):
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

    # -----------------------------------------
    # ADD NORMAL TRAFFIC REQUEST
    # -----------------------------------------
    def submit_normal(
        self, priority, sequence, request_id,
        direction, density, vehicle_count
    ):
        direction = direction.upper()
        density = density.upper()

        if direction not in VALID_DIRECTIONS:
            raise ValueError("Invalid direction")

        if density not in NORMAL_GREEN_TIME:
            raise ValueError("Invalid density")

        self.normal_queue.put(
            (
                priority,
                sequence,
                request_id,
                direction,
                density,
                vehicle_count
            )
        )

        self.wake_event.set()

        print(f"Normal traffic queued: {direction}, {density}")

    # -----------------------------------------
    # ADD EMERGENCY REQUEST
    # -----------------------------------------
    def submit_emergency(
        self, type_priority, vehicle_count, sequence,
        request_id, incident_id, direction, vehicle_type
    ):
        direction = direction.upper()
        vehicle_type = vehicle_type.upper()

        if direction not in VALID_DIRECTIONS:
            raise ValueError("Invalid direction")

        if vehicle_count <= 0:
            raise ValueError("Vehicle count must be positive")

        self.emergency_queue.put(
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

        # Notify the controller that emergency traffic exists.
        self.emergency_event.set()
        self.wake_event.set()

        print(
            f"Emergency queued: {direction}, "
            f"{vehicle_count} {vehicle_type}(s)"
        )

    # -----------------------------------------
    # SAFE SIGNAL TRANSITION
    # -----------------------------------------
    def switch_to_green(self, direction):

        connection = None
        cursor = None

        try:
            connection = get_connection()
            cursor = connection.cursor()

            # Find any currently GREEN signal.
            cursor.execute(
                """
                SELECT direction
                FROM signals
                WHERE current_state = 'GREEN'
                """
            )

            current_green = cursor.fetchone()

            # GREEN -> YELLOW before switching direction.
            if current_green is not None:
                old_direction = current_green[0]

                if old_direction != direction:
                    cursor.execute(
                        """
                        UPDATE signals
                        SET current_state = 'YELLOW'
                        WHERE direction = %s
                        """,
                        (old_direction,)
                    )

                    connection.commit()

                    print(f"{old_direction}: YELLOW")
                    time.sleep(YELLOW_TIME)

            # All directions RED for clearance.
            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'RED'
                """
            )

            connection.commit()

            print("ALL DIRECTIONS: RED")
            time.sleep(ALL_RED_TIME)

            # Give GREEN to the selected direction.
            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'GREEN'
                WHERE direction = %s
                """,
                (direction,)
            )

            connection.commit()

            print(f"{direction}: GREEN")

        except Exception as error:
            if connection is not None:
                connection.rollback()

            print("Signal transition error:", error)
            raise

        finally:
            if cursor is not None:
                cursor.close()

            if connection is not None:
                connection.close()

    # -----------------------------------------
    # SET ALL SIGNALS RED
    # -----------------------------------------
    def set_all_red(self):

        connection = None
        cursor = None

        try:
            connection = get_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                UPDATE signals
                SET current_state = 'RED'
                """
            )

            connection.commit()

            print("ALL DIRECTIONS: RED")

        except Exception as error:
            if connection is not None:
                connection.rollback()

            print("Could not set signals RED:", error)
            raise

        finally:
            if cursor is not None:
                cursor.close()

            if connection is not None:
                connection.close()

    # -----------------------------------------
    # SERVE NORMAL TRAFFIC
    # -----------------------------------------
    def serve_normal(self, request):

        (
            priority,
            sequence,
            request_id,
            direction,
            density,
            vehicle_count
        ) = request

        with self.signal_lock:

            # An emergency might have arrived while
            # this normal request was waiting.
            if self.emergency_event.is_set():
                return

            try:
                self.switch_to_green(direction)

                duration = NORMAL_GREEN_TIME[density]

                print(
                    f"Serving normal traffic: {direction}, "
                    f"density={density}, "
                    f"duration={duration}s"
                )

                # Wait in short intervals so an emergency
                # can interrupt normal green safely.
                interrupted = self.emergency_event.wait(
                    timeout=duration
                )

                if interrupted:
                    print(
                        "Emergency arrived. "
                        "Normal green will end safely."
                    )

                # GREEN -> YELLOW -> ALL-RED.
                connection = get_connection()
                cursor = connection.cursor()

                cursor.execute(
                    """
                    UPDATE signals
                    SET current_state = 'YELLOW'
                    WHERE direction = %s
                    """,
                    (direction,)
                )

                connection.commit()

                cursor.close()
                connection.close()

                print(f"{direction}: YELLOW")
                time.sleep(YELLOW_TIME)

                self.set_all_red()
                time.sleep(ALL_RED_TIME)

                self.log_event(
                    "NORMAL_TRAFFIC_SERVED",
                    f"{direction}, density={density}"
                )

            except Exception as error:
                print("Normal traffic error:", error)

    # -----------------------------------------
    # SERVE EMERGENCY REQUEST
    # -----------------------------------------
    def serve_emergency(self, request):

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

        with self.signal_lock:

            connection = None
            cursor = None

            try:
                connection = get_connection()
                cursor = connection.cursor()

                # Start only a request that is still pending.
                cursor.execute(
                    """
                    UPDATE emergency_requests
                    SET status = 'IN_PROGRESS'
                    WHERE request_id = %s
                      AND status = 'PENDING'
                    """,
                    (request_id,)
                )

                if cursor.rowcount == 0:
                    connection.rollback()
                    return

                cursor.execute(
                    """
                    UPDATE incidents
                    SET status = 'ASSIGNED'
                    WHERE incident_id = %s
                    """,
                    (incident_id,)
                )

                connection.commit()

            except Exception as error:
                if connection is not None:
                    connection.rollback()

                print("Could not start emergency:", error)
                return

            finally:
                if cursor is not None:
                    cursor.close()

                if connection is not None:
                    connection.close()

            try:
                self.switch_to_green(direction)

                duration = EMERGENCY_GREEN_TIME

                print(
                    f"EMERGENCY PRIORITY: {direction} | "
                    f"{vehicle_count} {vehicle_type}(s)"
                )

                print(
                    f"Simulating emergency passage for "
                    f"{duration} seconds..."
                )

                time.sleep(duration)

                # End the emergency green safely.
                connection = get_connection()
                cursor = connection.cursor()

                cursor.execute(
                    """
                    UPDATE signals
                    SET current_state = 'YELLOW'
                    WHERE direction = %s
                    """,
                    (direction,)
                )

                connection.commit()

                cursor.close()
                connection.close()

                print(f"{direction}: YELLOW")
                time.sleep(YELLOW_TIME)

                self.set_all_red()
                time.sleep(ALL_RED_TIME)

                # Mark request completed after simulation.
                connection = get_connection()
                cursor = connection.cursor()

                cursor.execute(
                    """
                    UPDATE emergency_requests
                    SET status = 'COMPLETED'
                    WHERE request_id = %s
                    """,
                    (request_id,)
                )

                cursor.execute(
                    """
                    UPDATE incidents
                    SET status = 'RESOLVED'
                    WHERE incident_id = %s
                    """,
                    (incident_id,)
                )

                connection.commit()

                print(
                    f"Emergency request {request_id} COMPLETED."
                )

                self.log_event(
                    "EMERGENCY_COMPLETED",
                    f"{direction}, {vehicle_type}, "
                    f"count={vehicle_count}"
                )

            except Exception as error:
                print("Emergency servicing error:", error)

            finally:
                if cursor is not None:
                    cursor.close()

                if connection is not None:
                    connection.close()

    # -----------------------------------------
    # CENTRAL SCHEDULER
    # -----------------------------------------
    def run_until_idle(self):

        while (
            not self.normal_queue.empty()
            or not self.emergency_queue.empty()
        ):

            # Emergency queue always gets checked first.
            try:
                emergency = self.emergency_queue.get_nowait()

            except Empty:
                emergency = None

            if emergency is not None:
                try:
                    self.serve_emergency(emergency)

                finally:
                    self.emergency_queue.task_done()

                # Keep emergency mode active while more
                # emergency requests are waiting.
                if self.emergency_queue.empty():
                    self.emergency_event.clear()

                continue

            # If an emergency appeared, loop back to
            # check the emergency queue before normal traffic.
            if self.emergency_event.is_set():
                continue

            try:
                normal = self.normal_queue.get_nowait()

            except Empty:
                continue

            try:
                self.serve_normal(normal)

            finally:
                self.normal_queue.task_done()

        print("\nAll queued requests have been processed.")