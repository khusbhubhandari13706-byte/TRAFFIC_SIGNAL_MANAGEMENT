from flask import Flask, render_template, jsonify, request
from queue import PriorityQueue, Empty
from datetime import datetime
import threading

from database.connection import get_connection
from modules.traffic_density import add_request
from modules.emergency_priority import register_emergency
from modules.signal_controller import SignalController

from modules.deadlock_manager import (
    create_process,
    create_resource,
    request_resource,
    detect_deadlock,
    recover_deadlock,
    get_process_status,
    finish_process
)

app = Flask(
    __name__,
    template_folder="frontend/templates",
    static_folder="frontend/static"
)

DIRECTIONS = ("NORTH", "SOUTH", "EAST", "WEST")
DENSITIES = ("LOW", "MEDIUM", "HIGH")
EMERGENCY_TYPES = ("AMBULANCE", "FIRE_TRUCK", "POLICE")

# One shared controller is used by the Flask application.
controller = SignalController()

# Separate sequence numbers are used by the two existing
# database registration modules.
normal_sequence = 0
emergency_sequence = 0
controller_arrival_sequence = 0
controller_arrival_lock = threading.Lock()
normal_sequence_lock = threading.Lock()
emergency_sequence_lock = threading.Lock()

# Simulation lifecycle state.
simulation_state = "NOT_STARTED"
simulation_started_at = None
state_lock = threading.Lock()

# Recent arrivals are kept for the frontend event log.
arrival_events = []
arrival_events_lock = threading.Lock()
MAX_ARRIVAL_EVENTS = 200

# -----------------------------------------
# DEADLOCK DEMONSTRATION STATE
# -----------------------------------------

deadlock_demo = {
    "active": False,
    "status": "IDLE",
    "process_ids": [],
    "resource_ids": [],
    "resource_names": [],
    "cycle": []
}

deadlock_demo_lock = threading.Lock()

def current_timestamp():
    """Return a local, timezone-aware timestamp for the UI."""
    return datetime.now().astimezone().isoformat(timespec="microseconds")


def add_arrival_event(
    request_type,
    direction,
    count,
    timestamp,
    request_id,
    density=None,
    vehicle_type=None,
    batch=False
):
    """Keep a small in-memory record of arrivals for the frontend."""
    event = {
        "event_id": f"{request_type}-{request_id}",
        "type": request_type,
        "direction": direction,
        "vehicle_count": count,
        "arrival_time": timestamp,
        "request_id": request_id,
        "batch": batch
    }

    if density is not None:
        event["density"] = density

    if vehicle_type is not None:
        event["vehicle_type"] = vehicle_type

    with arrival_events_lock:
        arrival_events.append(event)

        if len(arrival_events) > MAX_ARRIVAL_EVENTS:
            del arrival_events[:-MAX_ARRIVAL_EVENTS]

    return event


def parse_count(value, field_name, minimum=0, maximum=50):
    """Validate an integer count received from JSON."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a whole number.")

    if isinstance(value, int):
        count = value
    elif isinstance(value, str) and value.strip().isdigit():
        count = int(value.strip())
    else:
        raise ValueError(f"{field_name} must be a whole number.")

    if not minimum <= count <= maximum:
        raise ValueError(
            f"{field_name} must be between {minimum} and {maximum}."
        )

    return count


def is_simulation_running():
    with state_lock:
        return simulation_state == "RUNNING"


def next_controller_arrival_sequence():
    """One shared sequence preserves FIFO order across normal/emergency arrivals."""
    global controller_arrival_sequence
    with controller_arrival_lock:
        value = controller_arrival_sequence
        controller_arrival_sequence += 1
        return value


def register_normal_traffic(direction, density, count, arrival_time, batch=False):
    """
    Register normal traffic through the existing traffic_density module,
    then submit its database-backed request to the shared controller.
    """
    global normal_sequence

    temporary_queue = PriorityQueue()

    with normal_sequence_lock:
        old_sequence = normal_sequence

        normal_sequence = add_request(
            temporary_queue,
            normal_sequence,
            direction,
            density,
            str(count)
        )

        if normal_sequence == old_sequence:
            return None

        try:
            (
                priority,
                sequence,
                reading_id,
                registered_direction,
                registered_density,
                vehicle_count
            ) = temporary_queue.get_nowait()

        except Empty:
            normal_sequence = old_sequence
            return None

    controller_sequence = next_controller_arrival_sequence()

    controller.submit_normal(
        priority,
        controller_sequence,
        reading_id,
        registered_direction,
        registered_density,
        vehicle_count
    )

    add_arrival_event(
        "NORMAL",
        registered_direction,
        vehicle_count,
        arrival_time,
        reading_id,
        density=registered_density,
        batch=batch
    )

    return {
        "request_id": reading_id,
        "direction": registered_direction,
        "density": registered_density,
        "vehicle_count": vehicle_count
    }


def register_emergency_traffic(
    direction,
    vehicle_type,
    count,
    arrival_time,
    batch=False
):
    """
    Register an emergency through the existing emergency_priority module,
    then submit it to the shared signal controller.
    """
    global emergency_sequence

    temporary_queue = PriorityQueue()

    with emergency_sequence_lock:
        old_sequence = emergency_sequence

        emergency_sequence = register_emergency(
            temporary_queue,
            emergency_sequence,
            direction,
            vehicle_type,
            str(count)
        )

        if emergency_sequence == old_sequence:
            return None

        try:
            (
                type_priority,
                negative_count,
                sequence,
                request_id,
                incident_id,
                registered_direction,
                registered_vehicle_type,
                vehicle_count
            ) = temporary_queue.get_nowait()

        except Empty:
            emergency_sequence = old_sequence
            return None

    controller_sequence = next_controller_arrival_sequence()

    controller.submit_emergency(
        type_priority,
        vehicle_count,
        controller_sequence,
        request_id,
        incident_id,
        registered_direction,
        registered_vehicle_type,
        arrival_time=arrival_time
    )

    add_arrival_event(
        "EMERGENCY",
        registered_direction,
        vehicle_count,
        arrival_time,
        request_id,
        vehicle_type=registered_vehicle_type,
        batch=batch
    )

    return {
        "request_id": request_id,
        "incident_id": incident_id,
        "direction": registered_direction,
        "vehicle_type": registered_vehicle_type,
        "vehicle_count": vehicle_count
    }


def get_signal_states():
    """Read the actual signal states from MySQL."""
    connection = get_connection()

    if connection is None:
        raise RuntimeError("Database connection failed.")

    cursor = None

    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT direction, current_state
            FROM signals
            """
        )

        rows = cursor.fetchall()

        return {
            direction: state
            for direction, state in rows
        }

    finally:
        if cursor is not None:
            cursor.close()

        connection.close()


def get_normal_queue_counts():
    """Include pending and currently active normal requests."""
    return controller.get_normal_queue_counts()


def get_emergency_queue_counts():
    """Include pending and currently active emergency vehicles."""
    return controller.get_emergency_queue_counts()


def get_recent_arrivals():
    with arrival_events_lock:
        return list(reversed(arrival_events[-50:]))


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/api/start", methods=["POST"])
def start_simulation():
    """
    Register the four-direction initial configuration as one batch.
    All accepted initial requests receive the same arrival timestamp.
    """
    global simulation_state, simulation_started_at

    data = request.get_json(silent=True) or {}
    directions_data = data.get("directions", {})

    if not isinstance(directions_data, dict):
        return jsonify({"error": "'directions' must be an object."}), 400

    # Validate the complete initial configuration before writing to MySQL.
    validated = []

    try:
        for direction in DIRECTIONS:
            entry = (
                directions_data.get(direction)
                or directions_data.get(direction.lower())
                or {}
            )

            if not isinstance(entry, dict):
                raise ValueError(f"Invalid configuration for {direction}.")

            normal_count = parse_count(
                entry.get("vehicle_count", entry.get("normal_count", 0)),
                f"{direction} normal vehicle count"
            )

            emergency_count = parse_count(
                entry.get("emergency_count", 0),
                f"{direction} emergency vehicle count"
            )

            density = str(entry.get("density", "LOW")).strip().upper()
            emergency_type = str(
                entry.get("emergency_type", "AMBULANCE")
            ).strip().upper()

            if normal_count > 0 and density not in DENSITIES:
                raise ValueError(f"Invalid density for {direction}.")

            if emergency_count > 0 and emergency_type not in EMERGENCY_TYPES:
                raise ValueError(
                    f"Invalid emergency vehicle type for {direction}."
                )

            validated.append({
                "direction": direction,
                "normal_count": normal_count,
                "density": density,
                "emergency_count": emergency_count,
                "emergency_type": emergency_type
            })

    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    with state_lock:
        if simulation_state in ("STARTING", "RUNNING"):
            return jsonify({
                "error": "The simulation has already been started. "
                         "Use Reset before starting a new simulation."
            }), 409

        simulation_state = "STARTING"

    batch_timestamp = current_timestamp()
    accepted_normal = []
    accepted_emergency = []

    try:
        # Register all initial requests before waking the scheduler.
        # This gives the entire initial batch one shared arrival time.
        for item in validated:
            direction = item["direction"]

            if item["normal_count"] > 0:
                result = register_normal_traffic(
                    direction,
                    item["density"],
                    item["normal_count"],
                    batch_timestamp,
                    batch=True
                )

                if result is None:
                    raise RuntimeError(
                        f"Could not register normal traffic for {direction}."
                    )

                accepted_normal.append(result)

            if item["emergency_count"] > 0:
                result = register_emergency_traffic(
                    direction,
                    item["emergency_type"],
                    item["emergency_count"],
                    batch_timestamp,
                    batch=True
                )

                if result is None:
                    raise RuntimeError(
                        f"Could not register emergency traffic for {direction}."
                    )

                accepted_emergency.append(result)

        with state_lock:
            simulation_started_at = batch_timestamp
            simulation_state = "RUNNING"

        # Wake the worker only after every initial request is registered.
        controller.wake_event.set()

        return jsonify({
            "message": "Simulation started successfully.",
            "status": simulation_state,
            "started_at": simulation_started_at,
            "normal_requests": accepted_normal,
            "emergency_requests": accepted_emergency
        }), 200

    except Exception as error:
        print("Start simulation error:", error)

        # If some records were already committed, keep the simulation
        # running so those requests are not left stranded in the queues.
        if accepted_normal or accepted_emergency:
            with state_lock:
                simulation_started_at = batch_timestamp
                simulation_state = "RUNNING"

            controller.wake_event.set()
        else:
            with state_lock:
                simulation_state = "NOT_STARTED"

        return jsonify({
            "error": str(error),
            "status": simulation_state,
            "accepted_normal_requests": accepted_normal,
            "accepted_emergency_requests": accepted_emergency
        }), 500


@app.route("/api/traffic", methods=["POST"])
def add_traffic():
    """Add normal traffic while the simulation is running."""
    if not is_simulation_running():
        return jsonify({
            "error": "Start the simulation before adding live traffic."
        }), 409

    data = request.get_json(silent=True) or {}

    direction = str(data.get("direction", "")).strip().upper()
    density = str(data.get("density", "")).strip().upper()

    if direction not in DIRECTIONS:
        return jsonify({"error": "Invalid direction."}), 400

    if density not in DENSITIES:
        return jsonify({"error": "Invalid density."}), 400

    try:
        count = parse_count(
            data.get("vehicle_count"),
            "Vehicle count",
            minimum=1,
            maximum=50
        )
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    result = register_normal_traffic(
        direction,
        density,
        count,
        current_timestamp(),
        batch=False
    )

    if result is None:
        return jsonify({
            "error": "Unable to register normal traffic request."
        }), 500

    return jsonify({
        "message": f"{count} vehicles added to {direction}.",
        "status": "accepted",
        "request": result
    }), 200


@app.route("/api/emergency", methods=["POST"])
def add_emergency():
    """Add emergency traffic while the simulation is running."""
    if not is_simulation_running():
        return jsonify({
            "error": "Start the simulation before adding live emergencies."
        }), 409

    data = request.get_json(silent=True) or {}

    direction = str(data.get("direction", "")).strip().upper()
    vehicle_type = str(data.get("vehicle_type", "")).strip().upper()

    if direction not in DIRECTIONS:
        return jsonify({"error": "Invalid direction."}), 400

    if vehicle_type not in EMERGENCY_TYPES:
        return jsonify({"error": "Invalid emergency vehicle type."}), 400

    try:
        count = parse_count(
            data.get("vehicle_count"),
            "Emergency vehicle count",
            minimum=1,
            maximum=50
        )
    except ValueError as error:
        return jsonify({"error": str(error)}), 400

    result = register_emergency_traffic(
        direction,
        vehicle_type,
        count,
        current_timestamp(),
        batch=False
    )

    if result is None:
        return jsonify({
            "error": "Unable to register emergency request."
        }), 500

    return jsonify({
        "message": (
            f"{count} {vehicle_type}(s) added to {direction}."
        ),
        "status": "accepted",
        "request": result
    }), 200


@app.route("/api/vehicle-exited", methods=["POST"])
def vehicle_exited():
    """Acknowledge that a vehicle animation has completely left the intersection."""
    data = request.get_json(silent=True) or {}
    event_id = str(data.get("event_id", "")).strip()

    if not event_id:
        return jsonify({"error": "event_id is required."}), 400

    acknowledged = controller.acknowledge_vehicle_exit(event_id)
    if not acknowledged:
        # A timeout or a duplicate browser event may already have completed it.
        return jsonify({"message": "Vehicle event already completed."}), 200

    return jsonify({"message": "Vehicle exit acknowledged."}), 200


@app.route("/api/status", methods=["GET"])
def status():
    """Return live data for the dashboard."""
    try:
        signals = get_signal_states()
        normal_queues = get_normal_queue_counts()
        emergency_queues = get_emergency_queue_counts()

        # Movement events are consumed once so the frontend animates
        # each release event only once.
        movement_events = controller.get_movement_events()

        with state_lock:
            current_state = simulation_state
            started_at = simulation_started_at

        total_normal = sum(normal_queues.values())
        total_emergency = sum(emergency_queues.values())

        return jsonify({
            "status": current_state,
            "simulation_state": current_state,
            "started_at": started_at,
            "signals": signals,
            "queues": normal_queues,
            "normal_queues": normal_queues,
            "emergency_queues": emergency_queues,
            "vehicle_queues": controller.get_vehicle_queue_snapshot(),
            "total_normal_waiting": total_normal,
            "total_emergency_waiting": total_emergency,
            "total_waiting": total_normal + total_emergency,
            "movement_events": movement_events,
            "arrival_events": get_recent_arrivals()
        })

    except Exception as error:
        print("Status error:", error)

        return jsonify({
            "error": "Unable to fetch system status."
        }), 500


# -----------------------------------------
# DEADLOCK MANAGEMENT APIs
# -----------------------------------------

@app.route("/api/deadlock/demo", methods=["POST"])
def start_deadlock_demo():

    with deadlock_demo_lock:

        if deadlock_demo["active"]:
            return jsonify({
                "error": "A deadlock demonstration is already active."
            }), 409

        suffix = datetime.now().strftime("%Y%m%d%H%M%S%f")

        p1 = create_process(f"BROWSER_DL_P1_{suffix}")
        p2 = create_process(f"BROWSER_DL_P2_{suffix}")

        r1 = create_resource(f"BROWSER_DL_R1_{suffix}", 1)
        r2 = create_resource(f"BROWSER_DL_R2_{suffix}", 1)

        process_ids = [p1, p2]

        if None in (p1, p2, r1, r2):
            for pid in process_ids:
                if pid is not None:
                    finish_process(pid, "ABORTED")

            return jsonify({
                "error": "Could not create deadlock test resources."
            }), 500

        # Phase 1: Allocate different resources.
        result1 = request_resource(p1, r1)
        result2 = request_resource(p2, r2)

        if result1 is not True or result2 is not True:
            finish_process(p1, "ABORTED")
            finish_process(p2, "ABORTED")

            return jsonify({
                "error": "Initial resource allocation failed."
            }), 500

        # Phase 2: Each process requests the other's resource.
        result3 = request_resource(p1, r2)
        result4 = request_resource(p2, r1)

        if result3 is not False or result4 is not False:
            finish_process(p1, "ABORTED")
            finish_process(p2, "ABORTED")

            return jsonify({
                "error": "Could not create the expected circular wait."
            }), 500

        # Phase 3: Detect the actual database deadlock.
        cycle = detect_deadlock([p1, p2])

        if cycle is None or not cycle:
            finish_process(p1, "ABORTED")
            finish_process(p2, "ABORTED")

            return jsonify({
                "error": "Deadlock detection failed."
            }), 500

        # Save the active demonstration.
        deadlock_demo.update({
            "active": True,
            "status": "DETECTED",
            "process_ids": [p1, p2],
            "resource_ids": [r1, r2],
            "resource_names": ["R1", "R2"],
            "cycle": cycle
        })

        return jsonify({
            "message": "Deadlock successfully detected!",
            "status": "DETECTED",
            "processes": [
                {"id": p1, "name": "P1"},
                {"id": p2, "name": "P2"}
            ],
            "resources": [
                {"id": r1, "name": "R1", "held_by": p1},
                {"id": r2, "name": "R2", "held_by": p2}
            ],
            "waiting": [
                {"process_id": p1, "resource": "R2"},
                {"process_id": p2, "resource": "R1"}
            ],
            "cycle": cycle
        }), 200


@app.route("/api/deadlock/recover", methods=["POST"])
def recover_deadlock_api():

    with deadlock_demo_lock:

        if not deadlock_demo["active"]:
            return jsonify({
                "error": "No active deadlock demonstration."
            }), 409

        cycle = deadlock_demo["cycle"]
        process_ids = deadlock_demo["process_ids"]

        victim_id = cycle[-2]

        # Run the existing recovery algorithm.
        recover_deadlock(cycle)

        # Verify that the circular wait has disappeared.
        remaining_cycle = detect_deadlock(process_ids)

        if remaining_cycle is None:
            return jsonify({
                "error": "Could not verify deadlock recovery."
            }), 500

        if remaining_cycle:
            return jsonify({
                "error": "Deadlock still exists after recovery.",
                "cycle": remaining_cycle
            }), 500

        victim_status = get_process_status(victim_id)

        if victim_status != "ABORTED":
            return jsonify({
                "error": "Victim process was not successfully aborted.",
                "victim_status": victim_status
            }), 500

        deadlock_demo["active"] = False
        deadlock_demo["status"] = "RECOVERED"

        return jsonify({
            "message": "Deadlock successfully recovered!",
            "status": "RECOVERED",
            "victim_id": victim_id,
            "victim_status": victim_status,
            "cycle": cycle,
            "remaining_cycle": [],
            "process_status": {
                str(pid): get_process_status(pid)
                for pid in process_ids
            }
        }), 200


@app.route("/api/deadlock/status", methods=["GET"])
def deadlock_status_api():

    with deadlock_demo_lock:

        if not deadlock_demo["process_ids"]:
            return jsonify({
                "active": False,
                "status": "IDLE",
                "processes": [],
                "allocations": [],
                "waiting": []
            })

        process_ids = deadlock_demo["process_ids"]

        placeholders = ",".join(["%s"] * len(process_ids))

        connection = get_connection()

        if connection is None:
            return jsonify({
                "error": "Database connection failed."
            }), 500

        cursor = None

        try:
            cursor = connection.cursor()

            # Read process information.
            cursor.execute(
                f"""
                SELECT process_id, process_name, status
                FROM os_processes
                WHERE process_id IN ({placeholders})
                """,
                tuple(process_ids)
            )

            processes = [
                {
                    "id": row[0],
                    "name": row[1],
                    "status": row[2]
                }
                for row in cursor.fetchall()
            ]

            # Read allocated resources.
            cursor.execute(
                f"""
                SELECT a.process_id, r.resource_name
                FROM os_resource_allocations a
                JOIN os_resources r
                    ON a.os_resource_id = r.os_resource_id
                WHERE a.process_id IN ({placeholders})
                """,
                tuple(process_ids)
            )

            allocations = [
                {
                    "process_id": row[0],
                    "resource": row[1]
                }
                for row in cursor.fetchall()
            ]

            # Read waiting requests.
            cursor.execute(
                f"""
                SELECT q.process_id, r.resource_name
                FROM os_resource_requests q
                JOIN os_resources r
                    ON q.os_resource_id = r.os_resource_id
                WHERE q.process_id IN ({placeholders})
                  AND q.request_status = 'WAITING'
                """,
                tuple(process_ids)
            )

            waiting = [
                {
                    "process_id": row[0],
                    "resource": row[1]
                }
                for row in cursor.fetchall()
            ]

            return jsonify({
                "active": deadlock_demo["active"],
                "status": deadlock_demo["status"],
                "processes": processes,
                "allocations": allocations,
                "waiting": waiting,
                "cycle": deadlock_demo["cycle"]
            })

        finally:
            if cursor:
                cursor.close()

            connection.close()


# Background worker. It starts processing only after /api/start.
def controller_worker():
    print("Signal controller worker started.")

    while not controller.stop_event.is_set():
        controller.wake_event.wait(timeout=0.5)

        if controller.stop_event.is_set():
            break

        # Clear before processing. If a new request arrives while the
        # controller is busy, submit_normal/submit_emergency sets it again.
        controller.wake_event.clear()

        if not is_simulation_running():
            continue

        try:
            controller.run_until_idle()

        except Exception as error:
            print("Controller worker error:", error)


if __name__ == "__main__":
    print("Starting Traffic Management System...")

    worker = threading.Thread(
        target=controller_worker,
        name="TrafficControllerWorker",
        daemon=True
    )

    worker.start()

    # Disable the reloader to prevent duplicate background workers.
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True,
        use_reloader=False
    )
