import random
import threading
import time
import uuid
from queue import Empty, PriorityQueue, Queue

from database.connection import get_connection
from modules.bankers_algorithm import (
    register_max_claims,
    check_request_safety,
    get_road_resource_for_direction
)
from modules.deadlock_manager import (
    create_process,
    detect_deadlock,
    ensure_road_resources,
    finish_process,
    get_intersection_zone_path,
    get_resource_id,
    recover_deadlock,
    request_resource,
    release_process_resources,
)
VALID_DIRECTIONS = ("NORTH", "SOUTH", "EAST", "WEST")

# Every process must acquire intersection resources in this order.
RESOURCE_ORDER = {
    "ZONE_NW": 1,
    "ZONE_NE": 2,
    "ZONE_SW": 3,
    "ZONE_SE": 4
}
# These values remain the normal-traffic priority order source.
# They no longer decide how many seconds a signal stays GREEN.
NORMAL_GREEN_TIME = {"HIGH": 8, "MEDIUM": 5, "LOW": 3}

YELLOW_TIME = 1
ALL_RED_TIME = 1

# The browser acknowledges a vehicle after its crossing animation ends.
# If the browser closes or stops responding, the controller uses this
# safety timeout so the worker cannot remain blocked forever.
VEHICLE_ACK_TIMEOUT = 12
VEHICLE_HEADWAY = 0.35


class SignalController:
    def __init__(self):
        ensure_road_resources()

        # Normal: density priority, then global arrival order.
        self.normal_queue = PriorityQueue()

        # Emergency tuple order:
        # type priority, arrival timestamp, traffic-ahead count,
        # random tie-break, global sequence, request/incident IDs,
        # direction, vehicle type, remaining vehicle count.
        self.emergency_queue = PriorityQueue()

        self.signal_lock = threading.Lock()
        self.emergency_event = threading.Event()
        self.wake_event = threading.Event()
        self.stop_event = threading.Event()

        self.traffic_state_lock = threading.Lock()
        self.emergency_state_lock = threading.Lock()

        # Active records contain only vehicles still waiting in that batch.
        self.active_normal_requests = {}
        self.normal_process_ids = {}
        self.active_emergency_requests = {}

        # Movement events are consumed by /api/status. The matching browser
        # animation calls /api/vehicle-exited when the vehicle leaves the road.
        self.movement_events = Queue()
        self.pending_movement_acks = {}
        self.movement_ack_lock = threading.Lock()

    # --------------------------------------------------
    # DATABASE LOGGING
    # --------------------------------------------------
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
                (event_type, details),
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

    # --------------------------------------------------
    # REQUEST REGISTRATION
    # --------------------------------------------------
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
        if vehicle_count <= 0:
            raise ValueError("Vehicle count must be positive")

        self.normal_queue.put(
            (priority, sequence, request_id, direction, density, vehicle_count)
        )
        self.wake_event.set()
        print(
            f"Normal traffic queued: {direction}, density={density}, "
            f"vehicles={vehicle_count}, arrival_order={sequence}"
        )

    def _count_normal_vehicles_ahead(self, direction, emergency_sequence):
        total = 0

        with self.normal_queue.mutex:
            pending = list(self.normal_queue.queue)

        for item in pending:
            if item[3] == direction and item[1] < emergency_sequence:
                total += item[5]

        with self.traffic_state_lock:
            active = list(self.active_normal_requests.values())

        for item in active:
            if item["direction"] == direction and item["sequence"] < emergency_sequence:
                total += item["remaining"]

        return total

    def submit_emergency(
        self, type_priority, vehicle_count, sequence,
        request_id, incident_id, direction, vehicle_type,
        arrival_time=None
    ):
        direction = direction.upper()
        vehicle_type = vehicle_type.upper()

        if direction not in VALID_DIRECTIONS:
            raise ValueError("Invalid direction")
        if vehicle_count <= 0:
            raise ValueError("Vehicle count must be positive")

        arrival_time = arrival_time or f"{time.time():.6f}"
        vehicles_ahead = self._count_normal_vehicles_ahead(
            direction, sequence
        )

        # Same emergency type + same arrival time: fewer vehicles ahead wins.
        # If that is also equal, use a random tie-break.
        request = (
            type_priority,
            arrival_time,
            vehicles_ahead,
            random.random(),
            sequence,
            request_id,
            incident_id,
            direction,
            vehicle_type,
            vehicle_count,
        )

        with self.emergency_state_lock:
            self.emergency_queue.put(request)
            self.emergency_event.set()

        self.wake_event.set()
        print(
            f"Emergency queued: {direction}, {vehicle_count} {vehicle_type}(s); "
            f"vehicles ahead={vehicles_ahead}; arrival_order={sequence}"
        )

    # --------------------------------------------------
    # QUEUE / VEHICLE STATE FOR FRONTEND
    # --------------------------------------------------
    def get_normal_queue_counts(self):
        counts = {direction: 0 for direction in VALID_DIRECTIONS}

        with self.normal_queue.mutex:
            pending = list(self.normal_queue.queue)
        for item in pending:
            counts[item[3]] += item[5]

        with self.traffic_state_lock:
            active = list(self.active_normal_requests.values())
        for item in active:
            counts[item["direction"]] += item["remaining"]

        return counts

    def get_emergency_queue_counts(self):
        counts = {direction: 0 for direction in VALID_DIRECTIONS}

        with self.emergency_queue.mutex:
            pending = list(self.emergency_queue.queue)
        for item in pending:
            counts[item[7]] += item[9]

        with self.traffic_state_lock:
            active = list(self.active_emergency_requests.values())
        for item in active:
            counts[item["direction"]] += item["remaining"]

        return counts

    def get_vehicle_queue_snapshot(self):
        """Return the real FIFO order for each approach, not a guessed order."""
        queues = {direction: [] for direction in VALID_DIRECTIONS}
        records = []

        with self.normal_queue.mutex:
            normal_pending = list(self.normal_queue.queue)
        for item in normal_pending:
            priority, sequence, request_id, direction, density, count = item
            records.append((sequence, direction, "NORMAL", "", request_id, count))

        with self.emergency_queue.mutex:
            emergency_pending = list(self.emergency_queue.queue)
        for item in emergency_pending:
            (
                type_priority, arrival_time, ahead, tie_break, sequence,
                request_id, incident_id, direction, vehicle_type, count
            ) = item
            records.append((sequence, direction, "EMERGENCY", vehicle_type, request_id, count))

        with self.traffic_state_lock:
            active_normal = list(self.active_normal_requests.values())
            active_emergency = list(self.active_emergency_requests.values())

        for item in active_normal:
            records.append((
                item["sequence"], item["direction"], "NORMAL", "",
                item["request_id"], item["remaining"]
            ))
        for item in active_emergency:
            records.append((
                item["sequence"], item["direction"], "EMERGENCY",
                item["vehicle_type"], item["request_id"], item["remaining"]
            ))

        for sequence, direction, kind, vehicle_type, request_id, count in sorted(records):
            for vehicle_index in range(count):
                queues[direction].append({
                    "type": kind,
                    "vehicle_type": vehicle_type,
                    "request_id": request_id,
                    "arrival_order": sequence,
                    "vehicle_index": vehicle_index,
                })

        return queues

    def get_movement_events(self):
        events = []
        while True:
            try:
                event = self.movement_events.get_nowait()
                self.movement_events.task_done()
                with self.movement_ack_lock:
                    still_pending = event.get("event_id") in self.pending_movement_acks
                if still_pending:
                    events.append(event)
            except Empty:
                break
        return events

    def acknowledge_vehicle_exit(self, event_id):
        with self.movement_ack_lock:
            acknowledgement = self.pending_movement_acks.get(event_id)
        if acknowledgement is None:
            return False
        acknowledgement.set()
        return True

    # --------------------------------------------------
    # SIGNAL CONTROL
    # --------------------------------------------------
    def switch_to_green(self, direction):
        """Keep the current green if it is already the selected direction."""
        connection = None
        cursor = None

        with self.signal_lock:
            try:
                connection = get_connection()
                cursor = connection.cursor()
                cursor.execute(
                    """
                    SELECT direction
                    FROM signals
                    WHERE current_state = 'GREEN'
                    LIMIT 1
                    """
                )
                current = cursor.fetchone()
                current_direction = current[0] if current else None

                if current_direction == direction:
                    return False

                if current_direction is not None:
                    cursor.execute(
                        """
                        UPDATE signals
                        SET current_state = 'YELLOW'
                        WHERE direction = %s
                        """,
                        (current_direction,),
                    )
                    connection.commit()
                    print(f"{current_direction}: YELLOW")
                    time.sleep(YELLOW_TIME)

                cursor.execute("UPDATE signals SET current_state = 'RED'")
                connection.commit()
                print("ALL DIRECTIONS: RED")
                time.sleep(ALL_RED_TIME)

                cursor.execute(
                    """
                    UPDATE signals
                    SET current_state = 'GREEN'
                    WHERE direction = %s
                    """,
                    (direction,),
                )
                connection.commit()
                print(f"{direction}: GREEN")
                return True

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

    def set_all_red(self):
        connection = None
        cursor = None
        try:
            connection = get_connection()
            cursor = connection.cursor()
            cursor.execute("UPDATE signals SET current_state = 'RED'")
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

    def get_road_resource(self, direction):
        if direction in ("NORTH", "SOUTH"):
            return get_resource_id("ROAD_SEGMENT_1")
        return get_resource_id("ROAD_SEGMENT_2")

    def request_resource_with_bankers(self, process_id, direction):
        """
        Check Banker's Algorithm before actually granting a road resource.

        Returns:
            True  - resource successfully granted
            False - process must wait and retry
        """

        resource_name = get_road_resource_for_direction(direction)
        resource_id = self.get_road_resource(direction)

        if resource_id is None:
            raise RuntimeError(f"Resource not found: {resource_name}")

        decision = check_request_safety(
            process_id,
            resource_name,
            request_amount=1
        )

        status = decision["status"]

        if status == "SAFE":

            # Only now ask the existing resource manager to grant it.
            granted = request_resource(process_id, resource_id)

            if granted is True:
                print(
                    f"Banker's Algorithm: SAFE | "
                    f"P{process_id} acquired {resource_name}"
                )
                return True

            if granted is False:
                # A SAFE decision means the resource was available.
                # A failed grant indicates a concurrent allocation or race.
                raise RuntimeError(
                    f"{resource_name} became unavailable after "
                    "the safety check. Request was not granted."
                )

            raise RuntimeError("Database resource request failed.")

        if status in ("UNSAFE", "UNAVAILABLE"):

            print(
                f"Banker's Algorithm: {status} | "
                f"P{process_id} must wait for {resource_name}"
            )
            return False

        raise RuntimeError(
            f"Banker's Algorithm could not process request: {status}"
        )
    def get_intersection_zone_resources(self, direction):
        """Get the database resource IDs for a vehicle's intersection path."""

        zone_names = get_intersection_zone_path(direction)

        if zone_names is None:
            raise ValueError("Invalid traffic direction.")

        zone_ids = []

        for zone_name in zone_names:
            resource_id = get_resource_id(zone_name)

            if resource_id is None:
                raise RuntimeError(
                    f"Intersection resource not found: {zone_name}"
                )

            zone_ids.append(resource_id)

        return zone_names, zone_ids

    
    
    def request_intersection_zones(self, process_id, direction):
        """
        Acquire intersection zones in globally fixed order.
        Already-held zones are skipped during retries.
        Banker's Algorithm checks every new allocation.
        """

        zone_names, zone_ids = self.get_intersection_zone_resources(direction)

        ordered_resources = sorted(
            zip(zone_names, zone_ids),
            key=lambda item: RESOURCE_ORDER[item[0]]
        )

        # Find zones already allocated to this process.
        connection = None
        cursor = None

        try:
            connection = get_connection()
            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT os_resource_id
                FROM os_resource_allocations
                WHERE process_id = %s
                """,
                (process_id,)
            )

            held_resource_ids = {
                row[0] for row in cursor.fetchall()
            }

        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

        acquired_zones = []

        for zone_name, resource_id in ordered_resources:

            # Do not request or count a resource already held.
            if resource_id in held_resource_ids:
                acquired_zones.append(zone_name)
                continue

            decision = check_request_safety(
                process_id,
                zone_name,
                request_amount=1
            )

            status = decision["status"]

            if status != "SAFE":
                print(
                    f"Banker's Algorithm: {status} | "
                    f"P{process_id} waiting for {zone_name}"
                )

                return {
                    "granted": False,
                    "acquired": acquired_zones,
                    "waiting_for": zone_name
                }

            granted = request_resource(process_id, resource_id)

            if granted is None:
                raise RuntimeError(
                    f"Resource request failed for {zone_name}"
                )

            if granted is False:
                print(
                    f"P{process_id} waiting for {zone_name}. "
                    f"Already holding: {acquired_zones}"
                )

                return {
                    "granted": False,
                    "acquired": acquired_zones,
                    "waiting_for": zone_name
                }

            acquired_zones.append(zone_name)
            held_resource_ids.add(resource_id)

        print(
            f"P{process_id} acquired zones safely: {acquired_zones}"
        )

        return {
            "granted": True,
            "acquired": acquired_zones,
            "waiting_for": None
        }

    # --------------------------------------------------
    # MOVEMENT HANDSHAKE
    # --------------------------------------------------
    def _dispatch_vehicle(
        self, direction, request_id, movement_type,
        vehicle_type="", vehicle_number=1, batch_count=1
    ):
        """Start one vehicle animation and return its exit acknowledgement."""
        event_id = uuid.uuid4().hex
        acknowledgement = threading.Event()
        created_at = time.monotonic()

        with self.movement_ack_lock:
            self.pending_movement_acks[event_id] = acknowledgement

        self.movement_events.put({
            "event_id": event_id,
            "direction": direction,
            "vehicle_count": 1,
            "request_id": request_id,
            "type": movement_type,
            "vehicle_type": vehicle_type,
            "vehicle_number": vehicle_number,
            "batch_count": batch_count,
        })

        return event_id, acknowledgement, created_at

    def _wait_for_vehicle_exits(self, in_flight):
        """Wait until every dispatched vehicle has completely left the road."""
        for event_id, acknowledgement, created_at in in_flight:
            remaining_timeout = max(
                0,
                VEHICLE_ACK_TIMEOUT - (time.monotonic() - created_at)
            )
            completed = acknowledgement.wait(timeout=remaining_timeout)

            with self.movement_ack_lock:
                self.pending_movement_acks.pop(event_id, None)

            if completed:
                print("Vehicle exit acknowledged:", event_id)
            else:
                print(
                    f"Vehicle exit acknowledgement timed out for {event_id}; "
                    "continuing with safety fallback."
                )

    # --------------------------------------------------
    # QUEUE HELPERS
    # --------------------------------------------------
    def _peek_next_emergency(self):
        with self.emergency_queue.mutex:
            if not self.emergency_queue.queue:
                return None
            return self.emergency_queue.queue[0]

    def _pop_next_emergency(self):
        try:
            return self.emergency_queue.get_nowait()
        except Empty:
            return None

    def _take_normal_ahead_of_emergency(self, emergency):
        """Take the oldest same-lane normal batch that arrived before it."""
        emergency_sequence = emergency[4]
        emergency_direction = emergency[7]
        removed = []
        selected = None

        while True:
            try:
                removed.append(self.normal_queue.get_nowait())
            except Empty:
                break

        eligible = [
            item for item in removed
            if item[3] == emergency_direction and item[1] < emergency_sequence
        ]

        if eligible:
            selected = min(eligible, key=lambda item: item[1])

        for item in removed:
            if item is selected:
                continue
            self.normal_queue.put(item)
            self.normal_queue.task_done()

        return selected

    def _pop_next_normal(self):
        """Choose by density priority without overtaking vehicles in one lane."""
        removed = []
        while True:
            try:
                removed.append(self.normal_queue.get_nowait())
            except Empty:
                break

        if not removed:
            return None

        # Only the oldest request from each direction can be the lane head.
        lane_heads = {}
        for item in removed:
            direction = item[3]
            if direction not in lane_heads or item[1] < lane_heads[direction][1]:
                lane_heads[direction] = item

        selected = min(lane_heads.values(), key=lambda item: (item[0], item[1]))

        for item in removed:
            if item is selected:
                continue
            self.normal_queue.put(item)
            self.normal_queue.task_done()

        return selected

    def _requeue_emergency(self, request, remaining):
        if remaining <= 0:
            return
        updated = request[:-1] + (remaining,)
        with self.emergency_state_lock:
            self.emergency_queue.put(updated)
            self.emergency_event.set()

    def _refresh_emergency_event(self):
        with self.emergency_state_lock:
            queue_empty = self.emergency_queue.empty()
            with self.traffic_state_lock:
                active_empty = not self.active_emergency_requests
            if queue_empty and active_empty:
                self.emergency_event.clear()
            else:
                self.emergency_event.set()

    def _higher_priority_emergency_waiting(self, current_request):
        candidate = self._peek_next_emergency()
        if candidate is None:
            return False
        # Compare the complete emergency ordering. This includes vehicle
        # type priority, arrival time, traffic ahead, random tie-break and FCFS.
        return candidate[:5] < current_request[:5]

    # --------------------------------------------------
    # NORMAL TRAFFIC SERVICE
    # --------------------------------------------------
    
    
    def serve_normal(self, request, emergency_context=None):

        (
            priority,
            sequence,
            request_id,
            direction,
            density,
            vehicle_count
        ) = request

        direction = direction.upper()
        density = density.upper()

        remaining = vehicle_count
        in_flight = []

        resource_acquired = False
        paused = False
        aborted = False
        completed = False

        # Reuse the same OS process when a paused request resumes.
        process_id = self.normal_process_ids.get(request_id)

        if process_id is None:
            process_id = create_process(
                f"NORMAL_TRAFFIC_{request_id}"
            )

            if process_id is None:
                print("Could not create normal traffic process.")
                return request

            self.normal_process_ids[request_id] = process_id

        # Register the active normal request for frontend status.
        with self.traffic_state_lock:
            self.active_normal_requests[request_id] = {
                "priority": priority,
                "sequence": sequence,
                "request_id": request_id,
                "direction": direction,
                "density": density,
                "vehicle_count": vehicle_count,
                "remaining": remaining,
            }

        try:
            # Register maximum resource requirements.
            register_max_claims(process_id, direction)

            # Check whether a pending emergency requires preemption.
            def should_pause_for_emergency():

                pending = self._peek_next_emergency()

                if pending is None:
                    return False

                # When serving normal vehicles ahead of an emergency,
                # only a higher-priority emergency can interrupt them.
                if emergency_context is not None:
                    return pending[:5] < emergency_context[:5]

                # Otherwise, allow only normal vehicles that are
                # physically ahead of an emergency in the same lane.
                is_ahead = (
                    pending[7] == direction
                    and sequence < pending[4]
                )

                return not is_ahead

            # Acquire the road resource using Banker's Algorithm.
            while not resource_acquired:

                if self.stop_event.is_set():
                    aborted = True
                    break

                if should_pause_for_emergency():
                    paused = True
                    break

                resource_acquired = self.request_resource_with_bankers(
                    process_id,
                    direction
                )

                if not resource_acquired:
                    time.sleep(0.1)

            # Acquire intersection zones only after acquiring the road.
            if resource_acquired and not aborted and not paused:

                zone_result = {"granted": False}

                while not zone_result["granted"]:

                    if self.stop_event.is_set():
                        aborted = True
                        break

                    if should_pause_for_emergency():
                        paused = True
                        break

                    zone_result = self.request_intersection_zones(
                        process_id,
                        direction
                    )

                    if not zone_result["granted"]:
                        time.sleep(0.1)

                # Vehicles can move only after all required zones
                # have been acquired successfully.
                if zone_result["granted"] and not aborted and not paused:

                    print(
                        f"Normal traffic process P{process_id} "
                        f"acquired road and intersection resources."
                    )

                    self.switch_to_green(direction)

                    while remaining > 0:

                        if self.stop_event.is_set():
                            aborted = True
                            break

                        if should_pause_for_emergency():
                            paused = True
                            break

                        # Release one vehicle into the intersection.
                        remaining -= 1

                        with self.traffic_state_lock:
                            self.active_normal_requests[request_id]["remaining"] = remaining

                        in_flight.append(
                            self._dispatch_vehicle(
                                direction=direction,
                                request_id=request_id,
                                movement_type="NORMAL",
                                vehicle_type="",
                                vehicle_number=vehicle_count - remaining,
                                batch_count=vehicle_count,
                            )
                        )

                        if remaining > 0:
                            time.sleep(VEHICLE_HEADWAY)

            # Wait until all already-dispatched vehicles have exited.
            self._wait_for_vehicle_exits(in_flight)

            if paused and not aborted:

                print(
                    f"Normal traffic {request_id} paused. "
                    f"Remaining vehicles: {remaining}"
                )

                # Release road and zone resources for emergency traffic.
                release_process_resources(process_id)

                with self.traffic_state_lock:
                    self.active_normal_requests.pop(request_id, None)

                # Preserve the process ID so the same process can resume.
                return (
                    priority,
                    sequence,
                    request_id,
                    direction,
                    density,
                    remaining
                )

            elif remaining == 0 and not aborted:

                completed = True

                print(
                    f"Normal traffic {request_id} COMPLETED."
                )

            elif aborted:

                print(
                    f"Normal traffic {request_id} ABORTED."
                )

        except Exception as error:

            print("Normal traffic error:", error)
            aborted = True

        finally:

            if completed or aborted:

                try:
                    finish_process(
                        process_id,
                        "COMPLETED" if completed else "ABORTED"
                    )
                except Exception as error:
                    print("Normal process cleanup error:", error)

                self.normal_process_ids.pop(request_id, None)

                with self.traffic_state_lock:
                    self.active_normal_requests.pop(request_id, None)

        return None

        # --------------------------------------------------
    # EMERGENCY TRAFFIC SERVICE
    # --------------------------------------------------
    def serve_emergency(self, request):

        (
            type_priority,
            arrival_time,
            vehicles_ahead,
            tie_break,
            sequence,
            request_id,
            incident_id,
            direction,
            vehicle_type,
            vehicle_count
        ) = request

        direction = direction.upper()
        vehicle_type = vehicle_type.upper()

        remaining = vehicle_count
        process_id = None
        in_flight = []

        resource_acquired = False
        paused = False
        aborted = False
        completed = False
        failed = False

        process_id = create_process(
            f"EMERGENCY_{request_id}"
        )

        if process_id is None:
            print("Could not create emergency process.")
            return remaining

        # Show this emergency in the active frontend state.
        with self.traffic_state_lock:
            self.active_emergency_requests[request_id] = {
                "sequence": sequence,
                "request_id": request_id,
                "incident_id": incident_id,
                "direction": direction,
                "vehicle_type": vehicle_type,
                "remaining": remaining,
            }

        try:
            # 1. Register maximum resource claims.
            register_max_claims(process_id, direction)

            # 2. Acquire the road resource using Banker's Algorithm.
            while not resource_acquired:

                if self.stop_event.is_set():
                    aborted = True
                    break

                if self._higher_priority_emergency_waiting(request):
                    paused = True
                    break

                resource_acquired = self.request_resource_with_bankers(
                    process_id,
                    direction
                )

                if not resource_acquired:
                    time.sleep(0.1)

            # 3. Acquire the required intersection zones.
            if resource_acquired and not aborted and not paused:

                zone_result = {"granted": False}

                while not zone_result["granted"]:

                    if self.stop_event.is_set():
                        aborted = True
                        break

                    if self._higher_priority_emergency_waiting(request):
                        paused = True
                        break

                    zone_result = self.request_intersection_zones(
                        process_id,
                        direction
                    )

                    if not zone_result["granted"]:
                        time.sleep(0.1)

                # 4. Start emergency servicing only after all resources
                # have been safely acquired.
                if zone_result["granted"] and not aborted and not paused:

                    print(
                        f"Emergency process P{process_id} "
                        f"acquired intersection zones."
                    )

                    connection = None
                    cursor = None

                    try:
                        connection = get_connection()
                        cursor = connection.cursor()

                        cursor.execute(
                            """
                            UPDATE emergency_requests
                            SET status = 'IN_PROGRESS'
                            WHERE request_id = %s
                              AND status IN ('PENDING', 'IN_PROGRESS')
                            """,
                            (request_id,)
                        )

                        cursor.execute(
                            """
                            UPDATE incidents
                            SET status = 'ASSIGNED'
                            WHERE incident_id = %s
                            """,
                            (incident_id,)
                        )

                        connection.commit()

                    finally:
                        if cursor is not None:
                            cursor.close()

                        if connection is not None:
                            connection.close()

                    # 5. Give the emergency direction a green signal.
                    self.switch_to_green(direction)

                    print(
                        f"EMERGENCY GREEN: {direction} | "
                        f"{vehicle_type} | remaining={remaining}"
                    )

                    # 6. Dispatch emergency vehicles one by one.
                    while remaining > 0:

                        if self.stop_event.is_set():
                            aborted = True
                            break

                        if self._higher_priority_emergency_waiting(request):
                            paused = True
                            break

                        remaining -= 1

                        with self.traffic_state_lock:
                            self.active_emergency_requests[
                                request_id
                            ]["remaining"] = remaining

                        in_flight.append(
                            self._dispatch_vehicle(
                                direction=direction,
                                request_id=request_id,
                                movement_type="EMERGENCY",
                                vehicle_type=vehicle_type,
                                vehicle_number=vehicle_count - remaining,
                                batch_count=vehicle_count,
                            )
                        )

                        if remaining > 0:
                            time.sleep(VEHICLE_HEADWAY)

            # 7. Wait for all already-dispatched vehicles to leave
            # the intersection before releasing resources.
            self._wait_for_vehicle_exits(in_flight)

            completed = remaining == 0 and not aborted and not paused

            # 8. Update database status.
            connection = None
            cursor = None

            try:
                connection = get_connection()
                cursor = connection.cursor()

                if completed:

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

                    self.log_event(
                        "EMERGENCY_COMPLETED",
                        f"{direction}: {vehicle_count} {vehicle_type}(s)"
                    )

                    print(
                        f"Emergency request {request_id} COMPLETED."
                    )

                elif paused or (aborted and self.stop_event.is_set()):

                    cursor.execute(
                        """
                        UPDATE emergency_requests
                        SET status = 'PENDING'
                        WHERE request_id = %s
                        """,
                        (request_id,)
                    )

                    connection.commit()

                    print(
                        f"Emergency request {request_id} paused. "
                        f"Remaining vehicles: {remaining}"
                    )

            finally:
                if cursor is not None:
                    cursor.close()

                if connection is not None:
                    connection.close()

        except Exception as error:

            print("Emergency servicing error:", error)
            failed = True
            aborted = True

            try:
                self.set_all_red()
            except Exception:
                pass

        finally:

            # Remove active frontend state.
            with self.traffic_state_lock:
                self.active_emergency_requests.pop(request_id, None)

            # This process is not reused after an emergency pause.
            # finish_process also releases its allocated resources.
            try:
                finish_process(
                    process_id,
                    "COMPLETED" if completed else "ABORTED"
                )
            except Exception as error:
                print("Emergency process cleanup error:", error)

        if failed:
            return 0

        return remaining
    # --------------------------------------------------
    # DEADLOCK DETECTION / RECOVERY
    # --------------------------------------------------
    
    
    def check_deadlock_and_recover(self):
        """
        Detect deadlocks only among active traffic processes.
        Browser demo and standalone test processes are excluded.
        Recovery is performed through the dashboard button.
        """

        connection = None
        cursor = None

        try:
            connection = get_connection()
            cursor = connection.cursor()

            # Fetch only active normal and emergency traffic processes.
            cursor.execute("""
                SELECT process_id
                FROM os_processes
                WHERE status IN ('RUNNING', 'WAITING')
                AND (
                    LEFT(process_name, 15) = 'NORMAL_TRAFFIC_'
                    OR LEFT(process_name, 10) = 'EMERGENCY_'
                )
            """)

            process_ids = [row[0] for row in cursor.fetchall()]

        except Exception as error:
            print("Could not fetch active traffic processes:", error)
            return None

        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        # No active traffic processes means nothing to check.
        if not process_ids:
            print("Deadlock check: No active traffic processes.")
            return []

        # Check only the selected traffic processes.
        cycle = detect_deadlock(process_ids)

        if cycle is None:
            print("Deadlock detection failed.")
            return None

        if cycle:
            print("TRAFFIC DEADLOCK DETECTED:", cycle)
            return cycle

        print("Traffic deadlock check: No deadlock detected.")
        return []

    # --------------------------------------------------
    # CENTRAL ADAPTIVE SCHEDULER
    # --------------------------------------------------
    def run_until_idle(self):
        while not self.stop_event.is_set():
            emergency = self._pop_next_emergency()

            if emergency is not None:
                try:
                    # Vehicles that arrived earlier in this same lane are
                    # physically ahead of the emergency vehicle. Let them
                    # clear under this direction's emergency green first.
                    interrupted = False
                    while True:
                        next_emergency = self._peek_next_emergency()
                        if (
                            next_emergency is not None
                            and next_emergency[:5] < emergency[:5]
                        ):
                            self.emergency_queue.put(emergency)
                            interrupted = True
                            break

                        normal_ahead = self._take_normal_ahead_of_emergency(
                            emergency
                        )
                        if normal_ahead is None:
                            break

                        remainder = self.serve_normal(
                            normal_ahead,
                            emergency_context=emergency
                        )
                        if remainder is not None:
                            self.normal_queue.put(remainder)
                        self.normal_queue.task_done()

                        # A higher-priority emergency may have arrived while
                        # the current front-of-lane vehicles were crossing.
                        next_emergency = self._peek_next_emergency()
                        if (
                            next_emergency is not None
                            and next_emergency[:5] < emergency[:5]
                        ):
                            self.emergency_queue.put(emergency)
                            interrupted = True
                            break

                    if not interrupted:
                        remaining = self.serve_emergency(emergency)
                        if remaining > 0:
                            self._requeue_emergency(emergency, remaining)

                finally:
                    self.emergency_queue.task_done()
                    self._refresh_emergency_event()
                    self.check_deadlock_and_recover()

                continue

            normal = self._pop_next_normal()
            if normal is None:
                self._refresh_emergency_event()
                break

            try:
                remainder = self.serve_normal(normal)
                if remainder is not None:
                    self.normal_queue.put(remainder)
            finally:
                self.normal_queue.task_done()
                self.check_deadlock_and_recover()

        print("\nAll queued requests have been processed or are waiting for new input.")
