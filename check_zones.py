
from database.connection import get_connection

conn = get_connection()
cursor = conn.cursor(dictionary=True)

print("\n--- INTERSECTION RESOURCE STATUS ---")

cursor.execute("""
    SELECT
        r.resource_name,
        r.total_instances,
        COUNT(a.allocation_id) AS allocated,
        GROUP_CONCAT(
            CONCAT(p.process_id, ':', p.process_name, ':', p.status)
        ) AS holders
    FROM os_resources r
    LEFT JOIN os_resource_allocations a
        ON r.os_resource_id = a.os_resource_id
    LEFT JOIN os_processes p
        ON a.process_id = p.process_id
    WHERE r.resource_name IN (
        'ZONE_NW', 'ZONE_NE', 'ZONE_SW', 'ZONE_SE'
    )
    GROUP BY r.os_resource_id, r.resource_name, r.total_instances
""")

for row in cursor.fetchall():
    print(row)

print("\n--- WAITING RESOURCE REQUESTS ---")

cursor.execute("""
    SELECT
        p.process_id,
        p.process_name,
        p.status,
        r.resource_name,
        rr.request_status
    FROM os_resource_requests rr
    JOIN os_processes p ON rr.process_id = p.process_id
    JOIN os_resources r ON rr.os_resource_id = r.os_resource_id
    WHERE r.resource_name IN (
        'ZONE_NW', 'ZONE_NE', 'ZONE_SW', 'ZONE_SE'
    )
    AND rr.request_status = 'WAITING'
""")

for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
