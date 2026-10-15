
CREATE DATABASE IF NOT EXISTS traffic_manager;
USE traffic_manager;

-- 1. Traffic signal status for each direction
CREATE TABLE IF NOT EXISTS signals (
    signal_id INT AUTO_INCREMENT PRIMARY KEY,
    direction ENUM('NORTH','SOUTH','EAST','WEST') NOT NULL UNIQUE,
    current_state ENUM('RED','YELLOW','GREEN')
        NOT NULL DEFAULT 'RED',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP
);

-- 2. Manual traffic density readings
CREATE TABLE IF NOT EXISTS traffic_readings (
    reading_id INT AUTO_INCREMENT PRIMARY KEY,
    signal_id INT NOT NULL,
    density ENUM('LOW','MEDIUM','HIGH') NOT NULL,
    vehicle_count INT DEFAULT 0,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (signal_id) REFERENCES signals(signal_id)
);

-- 3. Reported incidents
CREATE TABLE IF NOT EXISTS incidents (
    incident_id INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(150) NOT NULL,
    incident_type VARCHAR(50) NOT NULL,
    status ENUM('OPEN','ASSIGNED','RESOLVED')
        NOT NULL DEFAULT 'OPEN',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Emergency requests
CREATE TABLE IF NOT EXISTS emergency_requests (
    request_id INT AUTO_INCREMENT PRIMARY KEY,
    incident_id INT NOT NULL,
    vehicle_type VARCHAR(30) NOT NULL,
    direction ENUM('NORTH','SOUTH','EAST','WEST') NOT NULL,
    priority INT NOT NULL DEFAULT 1,
    status ENUM('PENDING','IN_PROGRESS','COMPLETED','CANCELLED')
        NOT NULL DEFAULT 'PENDING',
    requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id)
);

-- 5. Emergency resources such as ambulances
CREATE TABLE IF NOT EXISTS resources (
    resource_id INT AUTO_INCREMENT PRIMARY KEY,
    resource_type VARCHAR(50) NOT NULL,
    status ENUM('AVAILABLE','BUSY','OFFLINE')
        NOT NULL DEFAULT 'AVAILABLE',
    location VARCHAR(150)
);

-- 6. Resource-to-incident assignments
CREATE TABLE IF NOT EXISTS resource_allocations (
    allocation_id INT AUTO_INCREMENT PRIMARY KEY,
    resource_id INT NOT NULL,
    incident_id INT NOT NULL,
    allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    released_at TIMESTAMP NULL,
    FOREIGN KEY (resource_id) REFERENCES resources(resource_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(incident_id)
);

-- 7. Simulated OS processes for deadlock detection
CREATE TABLE IF NOT EXISTS os_processes (
    process_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(50) NOT NULL,
    status ENUM('RUNNING','WAITING','COMPLETED','ABORTED')
        NOT NULL DEFAULT 'RUNNING'
);

-- 8. Resources used in the OS deadlock simulation
CREATE TABLE IF NOT EXISTS os_resources (
    os_resource_id INT AUTO_INCREMENT PRIMARY KEY,
    resource_name VARCHAR(50) NOT NULL UNIQUE,
    total_instances INT NOT NULL DEFAULT 1
);

-- 9. Record which process holds an OS resource
CREATE TABLE IF NOT EXISTS os_resource_allocations (
    allocation_id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT NOT NULL,
    os_resource_id INT NOT NULL,
    allocated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (process_id) REFERENCES os_processes(process_id),
    FOREIGN KEY (os_resource_id) REFERENCES os_resources(os_resource_id)
);

-- 10. Record which resource a process is waiting for
CREATE TABLE IF NOT EXISTS os_resource_requests (
    request_id INT AUTO_INCREMENT PRIMARY KEY,
    process_id INT NOT NULL,
    os_resource_id INT NOT NULL,
    request_status ENUM('WAITING','GRANTED','CANCELLED')
        NOT NULL DEFAULT 'WAITING',
    FOREIGN KEY (process_id) REFERENCES os_processes(process_id),
    FOREIGN KEY (os_resource_id) REFERENCES os_resources(os_resource_id)
);

-- 11. Event log for debugging and demonstration
CREATE TABLE IF NOT EXISTS system_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    details VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initial four traffic signals
INSERT IGNORE INTO signals (direction)
VALUES ('NORTH'), ('SOUTH'), ('EAST'), ('WEST');

-- Initial ambulance resources
INSERT INTO resources (resource_type, status, location)
SELECT 'AMBULANCE', 'AVAILABLE', 'Central Station'
WHERE NOT EXISTS (
    SELECT 1 FROM resources
    WHERE resource_type = 'AMBULANCE'
);

INSERT INTO resources (resource_type, status, location)
SELECT 'AMBULANCE', 'AVAILABLE', 'North Station'
WHERE (SELECT COUNT(*) FROM resources
       WHERE resource_type = 'AMBULANCE') < 2;
