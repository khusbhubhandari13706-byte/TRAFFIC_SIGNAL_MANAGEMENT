from queue import PriorityQueue

from modules.traffic_density import add_request
from modules.emergency_priority import register_emergency
from modules.signal_controller import SignalController
from modules.deadlock_manager import ensure_road_resources
from modules.deadlock_manager import run_deadlock_demo
def main():
    ensure_road_resources()

    # Temporary queues used to collect user input.
    normal_input_queue = PriorityQueue()
    emergency_input_queue = PriorityQueue()

    controller = SignalController()

    normal_sequence = 0
    emergency_sequence = 0

    print("\n===== TRAFFIC SIGNAL AND EMERGENCY MANAGER =====")

    # -----------------------------------------
    # 1. REGISTER NORMAL TRAFFIC
    # -----------------------------------------
    try:
        normal_count = int(input("\nHow many normal traffic requests? "))

        if normal_count < 0:
            print("Request count cannot be negative.")
            return

        for _ in range(normal_count):
            print("\n--- Normal Traffic Request ---")

            direction = input("Direction (NORTH/SOUTH/EAST/WEST): ")
            density = input("Density (LOW/MEDIUM/HIGH): ")
            vehicle_count = input("Number of vehicles: ")

            normal_sequence = add_request(
                normal_input_queue,
                normal_sequence,
                direction,
                density,
                vehicle_count
            )

        # -----------------------------------------
        # 2. REGISTER EMERGENCY REQUESTS
        # -----------------------------------------
        emergency_count = int(
            input("\nHow many emergency requests? ")
        )

        if emergency_count < 0:
            print("Request count cannot be negative.")
            return

        for i in range(emergency_count):
            print(f"\n--- Emergency Request {i + 1} ---")

            direction = input("Direction (NORTH/SOUTH/EAST/WEST): ")
            vehicle_type = input(
                "Vehicle type (AMBULANCE/FIRE_TRUCK/POLICE): "
            )
            vehicle_count = input("Number of emergency vehicles: ")

            emergency_sequence = register_emergency(
                emergency_input_queue,
                emergency_sequence,
                direction,
                vehicle_type,
                vehicle_count
            )

        # -----------------------------------------
        # 3. TRANSFER NORMAL REQUESTS TO CONTROLLER
        # -----------------------------------------
        while not normal_input_queue.empty():
            (
                priority,
                sequence,
                reading_id,
                direction,
                density,
                vehicle_count
            ) = normal_input_queue.get()

            try:
                controller.submit_normal(
                    priority,
                    sequence,
                    reading_id,
                    direction,
                    density,
                    vehicle_count
                )
            finally:
                normal_input_queue.task_done()

        # -----------------------------------------
        # 4. TRANSFER EMERGENCY REQUESTS
        # -----------------------------------------
        while not emergency_input_queue.empty():
            (
                type_priority,
                negative_count,
                sequence,
                request_id,
                incident_id,
                direction,
                vehicle_type,
                vehicle_count
            ) = emergency_input_queue.get()

            try:
                controller.submit_emergency(
                    type_priority,
                    vehicle_count,
                    sequence,
                    request_id,
                    incident_id,
                    direction,
                    vehicle_type
                )
            finally:
                emergency_input_queue.task_done()

        # -----------------------------------------
        # 5. START THE SHARED SIGNAL CONTROLLER
        # -----------------------------------------
        print("\n===== STARTING SHARED SIGNAL CONTROLLER =====")

        controller.run_until_idle()
        print("\n===== DEADLOCK DETECTION DEMONSTRATION =====")
        choice = input("Run the deadlock demonstration? (y/n): ").strip().lower()

        if choice == "y":
            run_deadlock_demo()
        print("\n===== SIMULATION FINISHED =====")

    except ValueError:
        print("Please enter valid numeric counts.")

    except KeyboardInterrupt:
        print("\nSimulation interrupted by user.")


if __name__ == "__main__":
    main()
