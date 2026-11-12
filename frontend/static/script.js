const DIRECTIONS = ["NORTH", "SOUTH", "EAST", "WEST"];
let simulationStartedAt = null;
let simulationRunning = false;
let lastArrivalIds = new Set();
let nextVehicleAnimationStart = 0;
const VEHICLE_ANIMATION_HEADWAY_MS = 350;
const VEHICLE_ANIMATION_DURATION_MS = 2600;
let statusUpdateInProgress = false;
let deadlockRecoveryRunning = false;
let deadlockDemoData = null;
let deadlockDemoProcessIds = [];


function getNumber(id) {
    const value = Number(document.getElementById(id).value);
    return Number.isInteger(value) && value >= 0 && value <= 50 ? value : null;
}

function getDirectionValues(direction) {
    return {
        normalCount: getNumber(`normal-${direction}`),
        density: document.getElementById(`density-${direction}`).value,
        emergencyCount: getNumber(`emergency-count-${direction}`),
        emergencyType: document.getElementById(`emergency-type-${direction}`).value
    };
}

function showMainMessage(message, isError = false) {
    const element = document.getElementById("message");
    element.textContent = message;
    element.style.color = isError ? "#fb7185" : "#4ade80";
}

async function postJSON(url, body) {
    const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
    return data;
}

async function startSimulation() {
    const button = document.getElementById("startSimulation");
    const directions = {};

    for (const direction of DIRECTIONS) {
        const values = getDirectionValues(direction);
        if (values.normalCount === null || values.emergencyCount === null) {
            showMainMessage(`${direction}: enter a whole number between 0 and 50.`, true);
            return;
        }
        directions[direction] = {
            vehicle_count: values.normalCount,
            density: values.density,
            emergency_count: values.emergencyCount,
            emergency_type: values.emergencyType
        };
    }

    button.disabled = true;
    button.textContent = "Starting...";

    try {
        const data = await postJSON("/api/start", { directions });
        simulationRunning = true;
        simulationStartedAt = data.started_at || new Date().toISOString();
        document.getElementById("simulationState").textContent = "RUNNING";
        document.getElementById("simulationState").style.color = "#4ade80";
        button.textContent = "Simulation Started";
        DIRECTIONS.forEach(direction => {
            document.getElementById(`add-${direction}`).disabled = false;
        });
        showMainMessage(data.message || "Simulation started successfully.");
        await updateSignals();
    } catch (error) {
        button.disabled = false;
        button.textContent = "Start Simulation";
        showMainMessage(error.message, true);
    }
}

async function addDirectionTraffic(direction) {
    if (!simulationRunning) {
        showMainMessage("Start the simulation first.", true);
        return;
    }

    const values = getDirectionValues(direction);
    const message = document.getElementById(`message-${direction}`);
    if (values.normalCount === null || values.emergencyCount === null) {
        message.textContent = "Enter counts from 0 to 50.";
        return;
    }
    if (values.normalCount === 0 && values.emergencyCount === 0) {
        message.textContent = "Enter at least one vehicle count.";
        return;
    }

    const button = document.getElementById(`add-${direction}`);
    button.disabled = true;
    message.textContent = "Adding vehicles...";

    try {
        const results = [];
        if (values.normalCount > 0) {
            const normal = await postJSON("/api/traffic", {
                direction,
                density: values.density,
                vehicle_count: values.normalCount
            });
            results.push(normal.message);
        }
        if (values.emergencyCount > 0) {
            const emergency = await postJSON("/api/emergency", {
                direction,
                vehicle_type: values.emergencyType,
                vehicle_count: values.emergencyCount
            });
            results.push(emergency.message);
        }
        message.textContent = results.join(" | ");
        document.getElementById(`normal-${direction}`).value = 0;
        document.getElementById(`emergency-count-${direction}`).value = 0;
        await updateSignals();
    } catch (error) {
        message.textContent = error.message;
    } finally {
        button.disabled = false;
    }
}

function setSignalLights(signals) {
    Object.entries(signals || {}).forEach(([direction, state]) => {
        const light = document.getElementById(direction);
        if (!light) return;
        light.classList.remove("red", "green", "yellow");
        light.classList.add(String(state).toLowerCase());
    });
}

function renderQueuedVehicles(vehicleQueues, normalQueues, emergencyQueues) {
    DIRECTIONS.forEach(direction => {
        const container = document.getElementById(`vehicles-${direction}`);
        if (!container) return;
        container.replaceChildren();

        let vehicles = Array.isArray(vehicleQueues?.[direction])
            ? vehicleQueues[direction]
            : null;

        // Fallback for an older backend response. The updated backend sends
        // vehicle_queues in actual arrival order, so emergencies are not
        // artificially moved to the front of the displayed lane.
        if (!vehicles) {
            vehicles = [];
            const emergencyCount = Number(emergencyQueues?.[direction] || 0);
            const normalCount = Number(normalQueues?.[direction] || 0);
            for (let i = 0; i < normalCount; i++) {
                vehicles.push({ type: "NORMAL" });
            }
            for (let i = 0; i < emergencyCount; i++) {
                vehicles.push({ type: "EMERGENCY", vehicle_type: "AMBULANCE" });
            }
        }

        const visibleVehicles = vehicles.slice(0, 10);
        visibleVehicles.forEach((vehicle, index) => {
            const car = document.createElement("span");
            const isEmergency = vehicle.type === "EMERGENCY";
            car.className = isEmergency ? "queued-car emergency" : "queued-car";
            
            setQueueCarPosition(car, direction, index);

            const emergencyEmoji = {
                AMBULANCE: "🚑",
                FIRE_TRUCK: "🚒",
                POLICE: "🚓"
            };
            const normalEmojis = ["🚗", "🚙", "🚕", "🚘"];
            car.textContent = isEmergency
                ? (emergencyEmoji[vehicle.vehicle_type] || "🚑")
                : normalEmojis[index % normalEmojis.length];
            car.title = isEmergency
                ? `${vehicle.vehicle_type || "Emergency"} waiting in lane`
                : "Normal vehicle waiting in lane";
            container.appendChild(car);
        });

        const hidden = Math.max(0, vehicles.length - visibleVehicles.length);
        if (hidden > 0) {
            const more = document.createElement("span");
            more.className = "queue-overflow";
            more.textContent = `+${hidden}`;
            setQueueCarPosition(more, direction, visibleVehicles.length);
            container.appendChild(more);
        }
    });
}

function setQueueCarPosition(element, direction, index) {
    if (direction === "NORTH") {
        element.style.left = "calc(50% - 12px)";
        element.style.top = `calc(50% - ${104 + index * 27}px)`;
    } else if (direction === "SOUTH") {
        element.style.left = "calc(50% + 10px)";
        element.style.top = `calc(50% + ${82 + index * 27}px)`;
    } else if (direction === "EAST") {
        element.style.left = `calc(50% + ${88 + index * 28}px)`;
        element.style.top = "calc(50% - 12px)";
    } else if (direction === "WEST") {
        element.style.left = `calc(50% - ${112 + index * 28}px)`;
        element.style.top = "calc(50% + 8px)";
    }
}

function acknowledgeVehicleExit(eventId) {
    if (!eventId) return;
    fetch("/api/vehicle-exited", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event_id: eventId })
    }).catch(error => {
        console.error("Vehicle exit acknowledgement failed:", error);
    });
}

function animateVehicle(movement) {
    const direction = movement.direction;
    const movementType = movement.type || "NORMAL";
    const vehicleType = movement.vehicle_type || "";
    const eventId = movement.event_id;
    const intersection = document.getElementById("intersection");

    let acknowledged = false;
    let fallbackTimer = null;

    const finishCrossing = () => {
        if (acknowledged) return;
        acknowledged = true;
        if (fallbackTimer) clearTimeout(fallbackTimer);
        if (car && car.isConnected) car.remove();
        acknowledgeVehicleExit(eventId);
    };

    if (!intersection || !DIRECTIONS.includes(direction)) {
        acknowledgeVehicleExit(eventId);
        return;
    }

    const emergencyEmoji = {
        AMBULANCE: "🚑",
        FIRE_TRUCK: "🚒",
        POLICE: "🚓"
    };
    const normalEmojis = ["🚗", "🚙", "🚕", "🚘"];

    const car = document.createElement("div");
    car.classList.add("vehicle", "dynamic-car", `${direction.toLowerCase()}-car`);
    if (movementType === "EMERGENCY") car.classList.add("emergency-moving");
    car.textContent = movementType === "EMERGENCY"
        ? (emergencyEmoji[vehicleType] || "🚑")
        : normalEmojis[(Number(movement.vehicle_number || 1) - 1) % normalEmojis.length];

    // If the status poll receives several movement events together, stagger
    // their starts so the vehicles still flow with realistic spacing.
    const now = performance.now();
    const startAt = Math.max(now, nextVehicleAnimationStart);
    const animationDelayMs = Math.max(0, startAt - now);

    car.style.animationDelay = `${animationDelayMs}ms`;
    nextVehicleAnimationStart = startAt + VEHICLE_ANIMATION_HEADWAY_MS;
    intersection.appendChild(car);

    // The backend waits for this acknowledgement before scheduling the next
    // vehicle. This keeps signal changes synchronized with the full crossing.
    car.addEventListener("animationend", finishCrossing, { once: true });

    // Fallback for browser throttling or a missing CSS animation event.
    fallbackTimer = setTimeout(
    finishCrossing,
    animationDelayMs + VEHICLE_ANIMATION_DURATION_MS + 1000
);
}

function renderArrivalLog(events) {
    const container = document.getElementById("arrivalLog");
    if (!container || !Array.isArray(events)) return;
    const fresh = events.filter(event => event.event_id && !lastArrivalIds.has(event.event_id));
    fresh.forEach(event => lastArrivalIds.add(event.event_id));
    if (lastArrivalIds.size > 300) lastArrivalIds = new Set(Array.from(lastArrivalIds).slice(-150));

    if (!events.length) {
        container.innerHTML = '<p class="empty-log">No traffic added yet.</p>';
        return;
    }
    container.replaceChildren();
    events.slice(0, 8).forEach(event => {
        const row = document.createElement("div");
        row.className = "log-entry";
        const kind = event.type === "EMERGENCY" ? "emergency-tag" : "";
        const detail = event.type === "EMERGENCY"
            ? `${event.vehicle_type || "Emergency"} · ${event.vehicle_count} vehicle(s)`
            : `Normal traffic · ${event.density || ""} · ${event.vehicle_count} vehicle(s)`;
        const time = event.arrival_time ? new Date(event.arrival_time).toLocaleTimeString() : "";
        const description = document.createElement("span");
        const typeLabel = document.createElement("strong");
        typeLabel.className = kind;
        typeLabel.textContent = event.type;
        description.appendChild(typeLabel);
        description.append(` — ${event.direction}: ${detail}`);
        const timeLabel = document.createElement("span");
        timeLabel.textContent = time;
        row.append(description, timeLabel);
        container.appendChild(row);
    });
}

function updateTimer() {
    if (!simulationStartedAt) return;
    const seconds = Math.max(0, Math.floor((Date.now() - new Date(simulationStartedAt).getTime()) / 1000));
    const minutes = String(Math.floor(seconds / 60)).padStart(2, "0");
    const remainder = String(seconds % 60).padStart(2, "0");
    document.getElementById("simulationTimer").textContent = `${minutes}:${remainder}`;
}

async function updateSignals() {
    if (deadlockRecoveryRunning || statusUpdateInProgress) return;

    statusUpdateInProgress = true;

    try {
        const response = await fetch("/api/status", { cache: "no-store" });
        if (!response.ok) throw new Error("Unable to fetch signal status");
        const data = await response.json();

        document.getElementById("connection").textContent = "Backend Connected";
        document.getElementById("connection").style.color = "#4ade80";
        setSignalLights(data.signals);

        const normalQueues = data.normal_queues || data.queues || {};
        const emergencyQueues = data.emergency_queues || {};
        renderQueuedVehicles(data.vehicle_queues || {}, normalQueues, emergencyQueues);

        document.getElementById("totalNormal").textContent = data.total_normal_waiting ?? Object.values(normalQueues).reduce((a, b) => a + Number(b || 0), 0);
        document.getElementById("totalEmergency").textContent = data.total_emergency_waiting ?? Object.values(emergencyQueues).reduce((a, b) => a + Number(b || 0), 0);
        document.getElementById("totalWaiting").textContent = data.total_waiting ?? (Number(document.getElementById("totalNormal").textContent) + Number(document.getElementById("totalEmergency").textContent));

        const state = data.simulation_state || data.status || "NOT_STARTED";
        if (state === "RUNNING" || state === "STARTING") {
            simulationRunning = true;
            simulationStartedAt = data.started_at || simulationStartedAt;
            document.getElementById("simulationState").textContent = state;
            document.getElementById("simulationState").style.color = "#4ade80";
            document.getElementById("startSimulation").disabled = true;
            document.getElementById("startSimulation").textContent = "Simulation Started";
            DIRECTIONS.forEach(direction => {
                document.getElementById(`add-${direction}`).disabled = false;
            });
        }

        if (Array.isArray(data.movement_events)) {
            data.movement_events.forEach(event => {
                animateVehicle(event);
            });
        }
        renderArrivalLog(data.arrival_events || []);
        updateTimer();
        } catch (error) {
        document.getElementById("connection").textContent = "Backend Disconnected";
        document.getElementById("connection").style.color = "#fb7185";
        console.error("Signal update error:", error);
        } finally {
        statusUpdateInProgress = false;
    }
}

// ==========================================
// DEADLOCK MANAGEMENT - FRONTEND INTEGRATION
// ==========================================



function renderDeadlockScene(data) {
    const intersection = document.getElementById("intersection");

    if (!intersection) {
        console.error("Intersection element not found!");
        return;
    }

    clearDeadlockScene();

    const processes = data.processes || [];
    const resources = data.resources || [];
    const waiting = data.waiting || [];

    if (processes.length !== 2 || resources.length !== 2) {
        console.error("Invalid demo response:", data);
        return;
    }

    // Create demo vehicles.
    processes.forEach((process, index) => {
        const car = document.createElement("div");

        car.id = `deadlock-car-${process.id}`;
        car.className = "deadlock-demo-car";

        car.style.setProperty("position", "absolute", "important");
        car.style.setProperty("display", "block", "important");
        car.style.setProperty("z-index", "30", "important");

        if (index === 0) {
            // P1: North road, just before stop line, facing South
            car.style.setProperty("left", "calc(50% - 16px)", "important");
            car.style.setProperty("top", "130px", "important");
            car.style.setProperty("transform", "rotate(180deg)", "important");
        } else {
            // P2: South road, just before stop line, facing North
            car.style.setProperty("left", "calc(50% - 16px)", "important");
            car.style.setProperty("top", "340px", "important");
            car.style.setProperty("transform", "rotate(0deg)", "important");
        }

        car.textContent = index === 0 ? "🚗" : "🚙";

        const label = document.createElement("span");
        label.className = "process-label";
        label.textContent = `${process.name} (P${index + 1})`;

        car.appendChild(label);
        intersection.appendChild(car);
    });
    // Create explanation panel.
    const explanation = document.createElement("div");
    explanation.id = "deadlock-demo-explanation";
    explanation.className = "deadlock-demo-explanation";

    const heading = document.createElement("strong");
    heading.textContent = "DEADLOCK DETECTED";

    explanation.appendChild(heading);

    processes.forEach(process => {
        const held = resources.find(r => r.held_by === process.id);
        const requested = waiting.find(w => w.process_id === process.id);

        const detail = document.createElement("p");
        detail.textContent =
            `${process.name}: Holds ${held?.name || "?"}, ` +
            `waiting for ${requested?.resource || "?"}`;

        explanation.appendChild(detail);
    });

    const cycle = document.createElement("p");
    cycle.textContent = "Circular Wait: P1 → P2 → P1";
    explanation.appendChild(cycle);

    intersection.appendChild(explanation);

    console.log("Deadlock vehicles added:", intersection.querySelectorAll(".deadlock-demo-car").length);
}


function clearDeadlockScene() {
    document.querySelectorAll(
        ".deadlock-demo-car, #deadlock-demo-explanation"
    ).forEach(element => element.remove());
}


async function simulateDeadlock() {
    const button = document.getElementById("simulateDeadlock");
    const message = document.getElementById("deadlockMessage");

    button.disabled = true;
    message.textContent = "Creating processes and allocating resources...";

    try {
        const response = await fetch("/api/deadlock/demo", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({})
        });

        const data = await response.json();
        console.log("DEADLOCK API RESPONSE:", JSON.stringify(data, null, 2));
        if (!response.ok) {
            throw new Error(data.error || data.message || "Request failed.");
        }

        deadlockDemoData = data;
        deadlockDemoProcessIds = (data.processes || []).map(p => p.id);

        renderDeadlockScene(data);

        await refreshDeadlockStatus();

        message.textContent = data.message;

        document.getElementById("deadlockState").textContent =
            data.status;

        document.getElementById("recoverDeadlock").disabled = false;

    } catch (error) {
        message.textContent = "Error: " + error.message;
    } finally {
        const status = document.getElementById("deadlockState").textContent;

        button.disabled = status === "DETECTED";
    }
}


// Fetch current deadlock information
async function refreshDeadlockStatus() {
    try {
        const response = await fetch("/api/deadlock/status");
        const data = await response.json();

        if (!response.ok) {
            throw new Error("Could not fetch deadlock status.");
        }

        document.getElementById("deadlockState").textContent =
            data.status;

        // Display processes
        const processes = document.getElementById("deadlockProcesses");

        processes.innerHTML = data.processes.length
            ? data.processes.map(p => `
                <p>
                    <strong>${escapeDeadlockHTML(p.name)}</strong>
                    (ID: ${p.id})
                    - ${escapeDeadlockHTML(p.status)}
                </p>
            `).join("")
            : "No processes available.";

        // Display resource allocations
        const allocations =
            document.getElementById("deadlockAllocations");

        allocations.innerHTML = data.allocations.length
            ? data.allocations.map(a => `
                <p>
                    Process ${a.process_id} →
                    ${escapeDeadlockHTML(a.resource)}
                </p>
            `).join("")
            : "No resources allocated.";

        // Display waiting requests
        const waiting =
            document.getElementById("deadlockWaiting");

        waiting.innerHTML = data.waiting.length
            ? data.waiting.map(w => `
                <p>
                    Process ${w.process_id} waiting for
                    ${escapeDeadlockHTML(w.resource)}
                </p>
            `).join("")
            : "No waiting requests.";

        // Display circular wait
        const cycle = document.getElementById("deadlockCycle");

        cycle.textContent = data.cycle && data.cycle.length
            ? data.cycle.join(" → ")
            : "No cycle detected.";

        document.getElementById("recoverDeadlock").disabled =
            !data.active || data.status !== "DETECTED";

    } catch (error) {
        console.error("Deadlock status error:", error);
    }
}

function setDeadlockSignals(activeDirection) {
    const directions = ["NORTH", "SOUTH", "EAST", "WEST"];

    directions.forEach(direction => {
        const light = document.getElementById(direction);

        light.classList.remove("red", "green", "yellow");

        light.classList.add(
            direction === activeDirection ? "green" : "red"
        );
    });
}

// Recover detected deadlock

async function recoverDeadlock() {
    const button = document.getElementById("recoverDeadlock");
    const message = document.getElementById("deadlockMessage");

    button.disabled = true;
    deadlockRecoveryRunning = true;

    message.textContent = "Starting deadlock recovery...";

    try {
        const response = await fetch("/api/deadlock/recover", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({})
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || data.message || "Recovery failed.");
        }

        const victimId = Number(data.victim_id);
        const processes = deadlockDemoData?.processes || [];

        const victim = processes.find(
            p => Number(p.id) === victimId
        );

        const survivor = processes.find(
            p => Number(p.id) !== victimId
        );

        const victimCar = document.getElementById(
            `deadlock-car-${victimId}`
        );

        const survivorCar = survivor
            ? document.getElementById(`deadlock-car-${survivor.id}`)
            : null;

        const victimName = victim?.name || "Unknown";
        const survivorName = survivor?.name || "Unknown";

        // STEP 1: Show which process is aborted.
        message.textContent =
            `Victim selected: ${victimName} (P2). Aborting process and releasing resources...`;

        if (victimCar) {
            victimCar.classList.add("victim-aborted");
        }

        await new Promise(resolve => setTimeout(resolve, 800));

        // STEP 2: Give green signal to the survivor's approach.
        // P1 travels from NORTH to SOUTH.
        message.textContent =
            `Victim ${victimName} aborted. Resources released. ${survivorName} can now proceed.`;

        setDeadlockSignals("NORTH");

        await new Promise(resolve => setTimeout(resolve, 500));

        // STEP 3: Move the surviving vehicle.
        if (survivorCar) {
            const survivorIndex = processes.findIndex(
                p => Number(p.id) === Number(survivor.id)
            );

            if (survivorIndex === 0) {
                // P1 starts from North and moves towards South.
                survivorCar.style.setProperty("top", "130px");
                setDeadlockSignals("NORTH");
                survivorCar.classList.add("demo-move-south");
            } else {
                // P2 starts from South and moves towards North.
                survivorCar.style.setProperty("top", "340px");
                setDeadlockSignals("SOUTH");
                survivorCar.classList.add("demo-move-north");
            }
        }

        message.textContent =
            `${survivorName} is moving through the intersection.`;

        await new Promise(resolve => setTimeout(resolve, 1900));

        // STEP 4: Finish visual demo and restore normal signals.
        clearDeadlockScene();

        await refreshDeadlockStatus();

        deadlockRecoveryRunning = false;
        await updateSignals();

        message.textContent =
            `${data.message} Victim: ${victimName} (ABORTED). ` +
            `${survivorName} completed movement. Deadlock recovered.`;

        document.getElementById("deadlockState").textContent =
            data.status;

        button.disabled = true;

    } catch (error) {
        deadlockRecoveryRunning = false;
        await updateSignals();

        message.textContent = "Recovery error: " + error.message;
        button.disabled = false;
    }
}


// Escape dynamic text before inserting it into HTML
function escapeDeadlockHTML(value) {
    return String(value).replace(/[&<>"']/g, character => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
    })[character]);
}


// Load existing deadlock status when the page opens
document.addEventListener("DOMContentLoaded", function () {
    refreshDeadlockStatus();
});
// Continuously fetch traffic and vehicle movement updates.
setInterval(() => {
    updateSignals();
}, 500);


