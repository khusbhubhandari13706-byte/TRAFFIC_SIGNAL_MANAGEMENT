
from modules.deadlock_manager import finish_process, get_process_status

old_process_ids = [
    14274, 14318, 14320, 14322, 14328,
    14330, 14334, 14336, 14340, 14344,
    14346, 14348, 14350, 14352, 14354, 14356
]

for process_id in old_process_ids:
    success = finish_process(process_id, "ABORTED")

    print(
        f"Process {process_id}: "
        f"{get_process_status(process_id)} "
        f"| Cleanup success: {success}"
    )
