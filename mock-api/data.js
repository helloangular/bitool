// ---------------------------------------------------------------------------
// Realistic fake data generators for the mock Samsara Fleet API
// Covers ~70 endpoints worth of entity data for a US-based fleet company
// with ~30 trucks, ~25 drivers, trailers, assets, HOS, safety, IFTA, etc.
// ---------------------------------------------------------------------------

const MAKES_MODELS = [
  { make: "Freightliner", model: "Cascadia", years: [2019, 2020, 2021, 2022, 2023] },
  { make: "Kenworth", model: "T680", years: [2020, 2021, 2022, 2023] },
  { make: "Peterbilt", model: "579", years: [2019, 2020, 2021, 2022] },
  { make: "Volvo", model: "VNL 860", years: [2021, 2022, 2023] },
  { make: "Mack", model: "Anthem", years: [2020, 2021, 2022] },
  { make: "International", model: "LT Series", years: [2019, 2020, 2021, 2022] },
  { make: "Western Star", model: "5700XE", years: [2020, 2021, 2022] },
  { make: "Freightliner", model: "M2 106", years: [2019, 2020, 2021, 2022, 2023] },
  { make: "Kenworth", model: "W990", years: [2021, 2022, 2023] },
  { make: "Peterbilt", model: "389", years: [2019, 2020, 2021] },
];

const TRAILER_MAKES = [
  { make: "Great Dane", model: "Everest", types: ["Dry Van"] },
  { make: "Wabash", model: "DuraPlate", types: ["Dry Van", "Refrigerated"] },
  { make: "Utility Trailer", model: "4000D-X", types: ["Dry Van"] },
  { make: "Hyundai Translead", model: "HT-200", types: ["Refrigerated"] },
  { make: "Stoughton", model: "Z-Plate", types: ["Dry Van"] },
  { make: "Vanguard", model: "VXP", types: ["Dry Van", "Refrigerated"] },
  { make: "Fontaine", model: "Revolution", types: ["Flatbed"] },
  { make: "MAC Trailer", model: "Pneumatic Dry Bulk", types: ["Tanker"] },
];

const FIRST_NAMES = [
  "James", "Maria", "Robert", "Patricia", "David", "Linda", "Carlos",
  "Jennifer", "Michael", "Sarah", "William", "Jessica", "Daniel", "Ashley",
  "Jose", "Emily", "Thomas", "Stephanie", "Kevin", "Nicole", "Brian",
  "Angela", "Richard", "Melissa", "Christopher", "Rebecca", "Anthony",
  "Laura", "Steven", "Amanda", "Mark", "Samantha", "Paul", "Katherine",
  "Andrew", "Christine",
];

const LAST_NAMES = [
  "Johnson", "Williams", "Garcia", "Martinez", "Brown", "Davis", "Miller",
  "Wilson", "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris",
  "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
  "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
  "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
  "Carter",
];

const STATES = [
  { abbr: "TX", plates: ["ABC", "DEF", "GHI"] },
  { abbr: "CA", plates: ["JKL", "MNO", "PQR"] },
  { abbr: "OH", plates: ["STU", "VWX", "YZA"] },
  { abbr: "PA", plates: ["BCD", "EFG", "HIJ"] },
  { abbr: "IL", plates: ["KLM", "NOP", "QRS"] },
  { abbr: "GA", plates: ["TUV", "WXY", "ZAB"] },
  { abbr: "FL", plates: ["CDE", "FGH", "IJK"] },
  { abbr: "IN", plates: ["LMN", "OPQ", "RST"] },
];

const LICENSE_TYPES = ["Class A CDL", "Class B CDL", "Class C CDL"];

const DOCUMENT_TYPES = [
  { name: "Bill of Lading", description: "Shipping document for freight transport" },
  { name: "Proof of Delivery", description: "Confirmation of successful delivery" },
  { name: "Pre-Trip Inspection", description: "Vehicle inspection before trip start" },
  { name: "Post-Trip Inspection", description: "Vehicle inspection after trip end" },
  { name: "Fuel Receipt", description: "Receipt for fuel purchase" },
  { name: "Accident Report", description: "Report filed after an incident" },
  { name: "Load Confirmation", description: "Confirmation of load details and pickup" },
  { name: "Customs Declaration", description: "Cross-border customs documentation" },
  { name: "Hazmat Shipping Paper", description: "Documentation for hazardous materials" },
  { name: "Weight Ticket", description: "Certified weight measurement at scale" },
  { name: "Lumper Receipt", description: "Receipt for warehouse unloading services" },
  { name: "Rate Confirmation", description: "Agreed rate between carrier and broker" },
  { name: "Toll Receipt", description: "Receipt for toll road charges" },
  { name: "Maintenance Work Order", description: "Vehicle maintenance request and record" },
  { name: "Driver Vehicle Inspection Report", description: "DVIR per FMCSA requirements" },
];

const TAG_NAMES = [
  "East Coast", "West Coast", "Midwest", "Southeast", "Long Haul",
  "Regional", "Dedicated", "Hazmat", "Refrigerated", "Flatbed",
];

// Bounding box roughly covering the continental US
const US_BOUNDS = { latMin: 25.0, latMax: 48.5, lonMin: -124.5, lonMax: -67.0 };

// Major US cities with coordinates for realistic location clustering
const CITY_CENTERS = [
  { name: "Dallas", state: "TX", lat: 32.7767, lon: -96.7970 },
  { name: "Chicago", state: "IL", lat: 41.8781, lon: -87.6298 },
  { name: "Atlanta", state: "GA", lat: 33.7490, lon: -84.3880 },
  { name: "Los Angeles", state: "CA", lat: 34.0522, lon: -118.2437 },
  { name: "Houston", state: "TX", lat: 29.7604, lon: -95.3698 },
  { name: "Phoenix", state: "AZ", lat: 33.4484, lon: -112.0740 },
  { name: "Philadelphia", state: "PA", lat: 39.9526, lon: -75.1652 },
  { name: "Columbus", state: "OH", lat: 39.9612, lon: -82.9988 },
  { name: "Indianapolis", state: "IN", lat: 39.7684, lon: -86.1581 },
  { name: "Memphis", state: "TN", lat: 35.1495, lon: -90.0490 },
  { name: "Jacksonville", state: "FL", lat: 30.3322, lon: -81.6557 },
  { name: "Denver", state: "CO", lat: 39.7392, lon: -104.9903 },
  { name: "Nashville", state: "TN", lat: 36.1627, lon: -86.7816 },
  { name: "Kansas City", state: "MO", lat: 39.0997, lon: -94.5786 },
  { name: "Charlotte", state: "NC", lat: 35.2271, lon: -80.8431 },
];

const STREET_NAMES = [
  "Main St", "Highway 70", "Interstate 40", "Route 66", "Industrial Blvd",
  "Commerce Dr", "Logistics Pkwy", "Distribution Way", "Warehouse Rd",
  "Terminal Ave", "Freight Ln", "Depot St", "Carrier Way", "Transport Blvd",
  "Dispatch Dr", "Loading Dock Rd",
];

const SAFETY_EVENT_TYPES = [
  "harshBrake", "harshAccel", "harshTurn", "crash", "nearCollision",
  "rollStability", "laneDepart", "followToo", "didNotYield",
];

const SAFETY_SEVERITY = ["minor", "major", "critical"];

const SAFETY_BEHAVIOR_LABELS = [
  "Distracted driving", "Following too closely", "Aggressive driving",
  "Fatigued driving", "Speeding", "Rolling stop", "Unsafe lane change",
  "Running red light", "Hard braking - load shift risk", "Jackknife risk",
];

const HOS_STATUSES = ["driving", "sleeper", "on_duty", "off_duty"];

const HOS_VIOLATION_TYPES = ["hoursExceeded", "breakRequired", "cycleExceeded"];
const HOS_REGULATION_TYPES = ["property70", "property60", "passenger"];

const DVIR_INSPECTION_TYPES = ["pre_trip", "post_trip", "ad_hoc"];
const DVIR_STATUSES = ["safe", "defects_found", "defects_corrected"];
const DEFECT_TYPES = [
  "tires", "brakes", "lights", "engine", "mirrors",
  "windshield", "wipers", "horn", "coupling", "fluid_levels",
];

const ROUTE_STATUSES = ["scheduled", "in_progress", "completed", "skipped"];
const STOP_STATES = ["en_route", "arrived", "departed", "skipped"];

const IFTA_JURISDICTIONS = [
  "TX", "CA", "OH", "PA", "IL", "GA", "FL", "IN", "TN", "MO",
  "NC", "CO", "AZ", "OK", "AR", "NM", "LA", "MS", "AL", "KY",
];

const ASSET_TYPES = ["trailer", "container", "powered_asset", "unpowered_asset"];

const SENSOR_TYPES = ["temperature", "humidity", "door", "gps"];

const ALERT_TYPES = ["vehicle", "driver", "geofence"];
const ALERT_SEVERITY = ["info", "warning", "critical"];
const ALERT_CONFIG_TYPES = [
  "speeding", "geofence_entry", "geofence_exit", "idle",
  "harsh_event", "engine_fault",
];

const WEBHOOK_EVENT_TYPES = [
  "vehicle.created", "vehicle.updated", "driver.created", "driver.updated",
  "route.completed", "alert.triggered", "document.submitted", "dvir.submitted",
  "hos.violation", "safety.event", "geofence.entry", "geofence.exit",
];

const USER_ROLES = ["admin", "standard", "viewer", "restricted"];
const AUTH_TYPES = ["default", "saml"];

const MACHINE_NAMES = [
  "Conveyor Belt A1", "Forklift #12", "Loading Crane B3", "Compressor Unit 7",
  "Hydraulic Press C2", "Welding Station D4", "CNC Mill E5", "Packaging Line F1",
  "Paint Booth G2", "Assembly Robot H3", "Pallet Jack K1", "Dock Leveler L2",
  "Sorting Machine M4", "Stretch Wrapper N1", "Generator Set P3",
];

const INDUSTRIAL_DATAPOINT_NAMES = [
  { name: "rpm", unit: "RPM", min: 500, max: 3500 },
  { name: "temperature", unit: "°F", min: 60, max: 250 },
  { name: "pressure", unit: "PSI", min: 10, max: 150 },
  { name: "vibration", unit: "mm/s", min: 0.1, max: 12.0 },
  { name: "current", unit: "A", min: 1, max: 80 },
  { name: "voltage", unit: "V", min: 110, max: 480 },
  { name: "flow_rate", unit: "GPM", min: 0.5, max: 50 },
  { name: "power", unit: "kW", min: 1, max: 200 },
  { name: "humidity", unit: "%", min: 20, max: 95 },
  { name: "cycle_count", unit: "cycles", min: 100, max: 50000 },
];

const DOCUMENT_STATES = ["submitted", "reviewed", "archived"];

const DISPATCH_STATUSES = ["unassigned", "scheduled", "in_progress", "completed"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function pickN(arr, n) {
  const shuffled = [...arr].sort(() => 0.5 - Math.random());
  return shuffled.slice(0, Math.min(n, arr.length));
}

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randFloat(min, max, decimals = 6) {
  return parseFloat((Math.random() * (max - min) + min).toFixed(decimals));
}

function generateVIN() {
  const chars = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789";
  let vin = "";
  for (let i = 0; i < 17; i++) {
    vin += chars[Math.floor(Math.random() * chars.length)];
  }
  return vin;
}

function generateMAC() {
  const hex = "0123456789ABCDEF";
  const parts = [];
  for (let i = 0; i < 6; i++) {
    parts.push(hex[randInt(0, 15)] + hex[randInt(0, 15)]);
  }
  return parts.join(":");
}

function generateSecretToken() {
  const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let token = "";
  for (let i = 0; i < 32; i++) {
    token += chars[Math.floor(Math.random() * chars.length)];
  }
  return token;
}

function isoNow() {
  return new Date().toISOString();
}

function isoMinutesAgo(minutes) {
  return new Date(Date.now() - minutes * 60 * 1000).toISOString();
}

function isoDaysAgo(days) {
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();
}

function isoHoursAgo(hours) {
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
}

function randomLocation() {
  const city = pick(CITY_CENTERS);
  return {
    latitude: city.lat + randFloat(-0.15, 0.15, 6),
    longitude: city.lon + randFloat(-0.15, 0.15, 6),
    city: city.name,
    state: city.state,
    formattedAddress: `${randInt(100, 9999)} ${pick(STREET_NAMES)}, ${city.name}, ${city.state}`,
  };
}

function buildTags_internal() {
  return TAG_NAMES.map((name, i) => ({
    id: String(1000 + i),
    name,
  }));
}

function assignTags(minCount = 0, maxCount = 3) {
  const tags = [];
  const tagCount = randInt(minCount, maxCount);
  const shuffled = [...TAG_NAMES].sort(() => 0.5 - Math.random());
  for (let t = 0; t < tagCount; t++) {
    tags.push({ id: String(1000 + TAG_NAMES.indexOf(shuffled[t])), name: shuffled[t] });
  }
  return tags;
}

function msFromHours(h) {
  return Math.round(h * 3600 * 1000);
}

// ---------------------------------------------------------------------------
// Dataset builders (called once at startup to keep data stable per run)
// ---------------------------------------------------------------------------

function buildVehicles(count = 30) {
  const vehicles = [];
  for (let i = 1; i <= count; i++) {
    const mm = pick(MAKES_MODELS);
    const state = pick(STATES);
    const platePrefix = pick(state.plates);
    const tags = assignTags(1, 3);

    vehicles.push({
      id: String(100000 + i),
      name: `Truck-${String(i).padStart(3, "0")}`,
      vin: generateVIN(),
      make: mm.make,
      model: mm.model,
      year: pick(mm.years),
      licensePlate: `${platePrefix}${randInt(1000, 9999)}`,
      serial: `SN-${String(i).padStart(5, "0")}`,
      harshAccelSetting: { type: "automatic" },
      harshBrakeSetting: { type: "automatic" },
      notes: "",
      tags,
      externalIds: { maintenanceId: `MNT-${String(i).padStart(4, "0")}` },
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)), // 30 days - 1 year ago
      updatedAtTime: isoMinutesAgo(randInt(0, 43200)),
    });
  }
  return vehicles;
}

function buildDrivers(count = 25) {
  const drivers = [];
  const usedNames = new Set();

  for (let i = 1; i <= count; i++) {
    let first, last, fullName;
    do {
      first = pick(FIRST_NAMES);
      last = pick(LAST_NAMES);
      fullName = `${first} ${last}`;
    } while (usedNames.has(fullName));
    usedNames.add(fullName);

    const state = pick(STATES);
    const tags = assignTags(0, 2);

    drivers.push({
      id: String(200000 + i),
      name: fullName,
      username: `${first.toLowerCase()}.${last.toLowerCase()}`,
      phone: `+1${randInt(200, 999)}${randInt(1000000, 9999999)}`,
      email: `${first.toLowerCase()}.${last.toLowerCase()}@fleet-demo.com`,
      licenseNumber: `${state.abbr}${randInt(10000000, 99999999)}`,
      licenseState: state.abbr,
      licenseType: pick(LICENSE_TYPES),
      eldExempt: Math.random() < 0.1,
      eldExemptReason: "",
      tags,
      driverActivationStatus: "active",
      currentVehicleId: Math.random() < 0.8 ? String(100000 + randInt(1, 30)) : null,
      externalIds: { payrollId: `PAY-${String(i).padStart(4, "0")}` },
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)),
      updatedAtTime: isoMinutesAgo(randInt(0, 43200)),
    });
  }
  return drivers;
}

function buildVehicleStats(vehicles) {
  const stats = [];
  const now = Date.now();

  for (const v of vehicles) {
    const city = pick(CITY_CENTERS);
    const engineRunning = Math.random() < 0.6;
    const speedMph = engineRunning ? randInt(0, 72) : 0;
    const obdOdometer = randInt(50000, 400000);

    stats.push({
      id: v.id,
      name: v.name,
      time: new Date(now - randInt(0, 600) * 1000).toISOString(),
      engineStates: [
        {
          value: engineRunning ? "On" : "Off",
          time: isoMinutesAgo(randInt(0, 120)),
        },
      ],
      fuelPercents: [
        {
          value: randFloat(5, 98, 1),
          time: isoMinutesAgo(randInt(0, 30)),
        },
      ],
      obdOdometerMeters: [
        {
          value: obdOdometer * 1609.34,
          time: isoMinutesAgo(randInt(0, 30)),
        },
      ],
      gps: [
        {
          latitude: city.lat + randFloat(-0.15, 0.15, 6),
          longitude: city.lon + randFloat(-0.15, 0.15, 6),
          headingDegrees: randInt(0, 359),
          speedMilesPerHour: speedMph,
          reverseGeo: {
            formattedLocation: `Near ${city.name}`,
          },
          time: isoMinutesAgo(randInt(0, 5)),
        },
      ],
      obdEngineSeconds: [
        {
          value: randInt(500000, 5000000),
          time: isoMinutesAgo(randInt(0, 30)),
        },
      ],
    });
  }
  return stats;
}

function buildVehicleLocations(vehicles) {
  const locations = [];

  for (const v of vehicles) {
    const city = pick(CITY_CENTERS);
    locations.push({
      id: v.id,
      name: v.name,
      time: isoMinutesAgo(randInt(0, 10)),
      location: {
        latitude: city.lat + randFloat(-0.2, 0.2, 6),
        longitude: city.lon + randFloat(-0.2, 0.2, 6),
        headingDegrees: randInt(0, 359),
        speedMilesPerHour: randInt(0, 68),
        reverseGeo: {
          formattedLocation: `${randInt(100, 9999)} ${pick(STREET_NAMES)}${city.name ? `, ${city.name}` : ""}`,
        },
        time: isoMinutesAgo(randInt(0, 5)),
      },
    });
  }
  return locations;
}

function buildDocumentTypes() {
  return DOCUMENT_TYPES.map((dt, i) => ({
    id: String(300000 + i + 1),
    name: dt.name,
    description: dt.description,
    orgId: "org_mock_001",
    fieldTypes: [
      {
        label: "Notes",
        type: "string",
        required: false,
      },
      {
        label: "Reference Number",
        type: "string",
        required: true,
      },
    ],
    createdAtTime: isoMinutesAgo(randInt(100000, 525600)),
    updatedAtTime: isoMinutesAgo(randInt(0, 100000)),
  }));
}

// ---------------------------------------------------------------------------
// New generators
// ---------------------------------------------------------------------------

function buildTrailers(count = 20) {
  const trailers = [];
  for (let i = 1; i <= count; i++) {
    const tm = pick(TRAILER_MAKES);
    const state = pick(STATES);
    const platePrefix = pick(state.plates);
    const loc = randomLocation();
    const connected = Math.random() < 0.6;

    trailers.push({
      id: String(400000 + i),
      name: `Trailer-${String(i).padStart(3, "0")}`,
      serial: `TRL-${String(i).padStart(5, "0")}`,
      make: tm.make,
      model: tm.model,
      trailerType: pick(tm.types),
      tags: assignTags(0, 2),
      licensePlate: `${platePrefix}${randInt(1000, 9999)}`,
      licenseState: state.abbr,
      lengthFeet: pick([48, 53, 28, 40, 45]),
      status: connected ? "connected" : "disconnected",
      currentVehicleId: connected ? String(100000 + randInt(1, 30)) : null,
      gps: {
        latitude: loc.latitude,
        longitude: loc.longitude,
        reverseGeo: {
          formattedLocation: loc.formattedAddress,
        },
        time: isoMinutesAgo(randInt(0, 60)),
      },
      notes: "",
      externalIds: {},
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)),
      updatedAtTime: isoMinutesAgo(randInt(0, 43200)),
    });
  }
  return trailers;
}

function buildAssets(count = 15) {
  const assets = [];
  for (let i = 1; i <= count; i++) {
    const loc = randomLocation();
    const assetType = pick(ASSET_TYPES);
    const prefix = assetType === "trailer" ? "TRL" : assetType === "container" ? "CTN" : "AST";

    assets.push({
      id: String(410000 + i),
      name: `${prefix}-${String(i).padStart(4, "0")}`,
      serial: `${prefix}SN-${String(i).padStart(5, "0")}`,
      assetType,
      tags: assignTags(0, 2),
      location: {
        latitude: loc.latitude,
        longitude: loc.longitude,
        formattedAddress: loc.formattedAddress,
        time: isoMinutesAgo(randInt(0, 120)),
      },
      notes: "",
      externalIds: {},
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)),
      updatedAtTime: isoMinutesAgo(randInt(0, 43200)),
    });
  }
  return assets;
}

function buildSafetyEvents(vehicles, drivers, count = 50) {
  const events = [];
  for (let i = 1; i <= count; i++) {
    const v = pick(vehicles);
    const d = pick(drivers);
    const loc = randomLocation();
    const eventType = pick(SAFETY_EVENT_TYPES);
    const severity = eventType === "crash" ? "critical"
      : eventType === "nearCollision" || eventType === "rollStability" ? pick(["major", "critical"])
      : pick(SAFETY_SEVERITY);

    events.push({
      id: String(500000 + i),
      time: isoMinutesAgo(randInt(10, 43200)),
      type: eventType,
      severity,
      vehicle: { id: v.id, name: v.name },
      driver: { id: d.id, name: d.name },
      location: {
        latitude: loc.latitude,
        longitude: loc.longitude,
        address: loc.formattedAddress,
      },
      speedMph: randInt(5, 75),
      maxGForce: randFloat(0.2, 2.5, 2),
      behaviorLabel: pick(SAFETY_BEHAVIOR_LABELS),
      coachable: Math.random() < 0.7,
      downloadForwardVideoUrl: Math.random() < 0.5 ? `https://mock-videos.samsara.com/forward/${500000 + i}.mp4` : null,
      downloadInwardVideoUrl: Math.random() < 0.3 ? `https://mock-videos.samsara.com/inward/${500000 + i}.mp4` : null,
    });
  }
  return events.sort((a, b) => new Date(b.time) - new Date(a.time));
}

function buildHosLogs(drivers, count = 100) {
  const logs = [];
  for (let i = 1; i <= count; i++) {
    const d = pick(drivers);
    const daysAgo = randInt(0, 30);
    const logDate = isoDaysAgo(daysAgo).slice(0, 10); // YYYY-MM-DD

    // Generate a realistic sequence of status changes for the day
    const statusChanges = [];
    let currentMinute = randInt(0, 360); // start between midnight and 6am
    const changeCount = randInt(4, 12);

    for (let s = 0; s < changeCount; s++) {
      const status = pick(HOS_STATUSES);
      const durationMinutes = randInt(15, 240);
      const startMinute = currentMinute;
      const endMinute = Math.min(startMinute + durationMinutes, 1440);
      const loc = randomLocation();

      statusChanges.push({
        status,
        startTime: `${logDate}T${String(Math.floor(startMinute / 60)).padStart(2, "0")}:${String(startMinute % 60).padStart(2, "0")}:00Z`,
        endTime: `${logDate}T${String(Math.floor(endMinute / 60)).padStart(2, "0")}:${String(endMinute % 60).padStart(2, "0")}:00Z`,
        location: {
          latitude: loc.latitude,
          longitude: loc.longitude,
          name: loc.formattedAddress,
        },
      });

      currentMinute = endMinute;
      if (currentMinute >= 1440) break;
    }

    // Calculate totals from status changes
    let totalDrivingMs = 0, totalOnDutyMs = 0, totalSleeperMs = 0, totalOffDutyMs = 0;
    for (const sc of statusChanges) {
      const start = new Date(sc.startTime).getTime();
      const end = new Date(sc.endTime).getTime();
      const dur = end - start;
      if (sc.status === "driving") totalDrivingMs += dur;
      else if (sc.status === "on_duty") totalOnDutyMs += dur;
      else if (sc.status === "sleeper") totalSleeperMs += dur;
      else totalOffDutyMs += dur;
    }

    // Some logs have violations
    const violations = [];
    if (Math.random() < 0.15) {
      violations.push({
        type: pick(HOS_VIOLATION_TYPES),
        regulationType: pick(HOS_REGULATION_TYPES),
        startTime: statusChanges.length > 0 ? statusChanges[statusChanges.length - 1].startTime : `${logDate}T12:00:00Z`,
        durationMs: randInt(60000, 7200000),
      });
    }

    logs.push({
      id: String(510000 + i),
      driverId: d.id,
      driverName: d.name,
      logDate,
      statusChanges,
      totalDrivingMs,
      totalOnDutyMs,
      totalSleeperMs,
      totalOffDutyMs,
      certified: Math.random() < 0.85,
      violations,
      vehicleId: d.currentVehicleId || String(100000 + randInt(1, 30)),
    });
  }
  return logs.sort((a, b) => b.logDate.localeCompare(a.logDate));
}

function buildHosViolations(drivers, count = 30) {
  const violations = [];
  for (let i = 1; i <= count; i++) {
    const d = pick(drivers);
    violations.push({
      id: String(520000 + i),
      driverId: d.id,
      driverName: d.name,
      type: pick(HOS_VIOLATION_TYPES),
      regulationType: pick(HOS_REGULATION_TYPES),
      time: isoMinutesAgo(randInt(60, 43200)),
      durationMs: randInt(60000, 10800000), // 1 min to 3 hours
      vehicleId: d.currentVehicleId || String(100000 + randInt(1, 30)),
    });
  }
  return violations.sort((a, b) => new Date(b.time) - new Date(a.time));
}

function buildHosClocks(drivers) {
  return drivers.map((d) => {
    const currentStatus = pick(HOS_STATUSES);
    const driveRemainingHrs = currentStatus === "driving" ? randFloat(0.5, 10.5, 1) : randFloat(5, 11, 1);
    const shiftRemainingHrs = randFloat(1, 14, 1);
    const cycleRemainingHrs = randFloat(10, 70, 1);

    return {
      driverId: d.id,
      driverName: d.name,
      currentDutyStatus: currentStatus,
      driveRemaining: msFromHours(driveRemainingHrs),
      shiftRemaining: msFromHours(shiftRemainingHrs),
      cycleRemaining: msFromHours(cycleRemainingHrs),
      cycleTomorrow: msFromHours(cycleRemainingHrs + randFloat(8, 11, 1)),
      breakDuration: msFromHours(currentStatus === "off_duty" || currentStatus === "sleeper" ? randFloat(0.5, 10, 1) : 0),
      timeInCurrentStatus: msFromHours(randFloat(0.1, 8, 1)),
      timeUntilBreak: msFromHours(currentStatus === "driving" || currentStatus === "on_duty" ? randFloat(0.5, 8, 1) : 0),
    };
  });
}

function buildDvirs(vehicles, drivers, count = 40) {
  const dvirs = [];
  for (let i = 1; i <= count; i++) {
    const v = pick(vehicles);
    const d = pick(drivers);
    const loc = randomLocation();
    const inspectionType = pick(DVIR_INSPECTION_TYPES);
    const hasDefects = Math.random() < 0.35;
    const status = hasDefects ? (Math.random() < 0.6 ? "defects_corrected" : "defects_found") : "safe";

    const defects = [];
    if (hasDefects) {
      const defectCount = randInt(1, 4);
      for (let j = 0; j < defectCount; j++) {
        const defectType = pick(DEFECT_TYPES);
        const isResolved = status === "defects_corrected" || Math.random() < 0.3;
        defects.push({
          id: String(530000 + i * 10 + j),
          comment: `${defectType.charAt(0).toUpperCase() + defectType.slice(1).replace(/_/g, " ")} issue noted during ${inspectionType.replace(/_/g, " ")} inspection`,
          defectType,
          isResolved,
          resolvedAt: isResolved ? isoMinutesAgo(randInt(0, 1440)) : null,
          resolvedBy: isResolved ? pick(drivers).name : null,
        });
      }
    }

    const startMinsAgo = randInt(60, 43200);
    const endMinsAgo = startMinsAgo - randInt(10, 45);

    dvirs.push({
      id: String(530000 + i),
      vehicleId: v.id,
      vehicleName: v.name,
      driverId: d.id,
      driverName: d.name,
      inspectionType,
      status,
      startTime: isoMinutesAgo(startMinsAgo),
      endTime: isoMinutesAgo(Math.max(endMinsAgo, 0)),
      location: {
        latitude: loc.latitude,
        longitude: loc.longitude,
        formattedAddress: loc.formattedAddress,
      },
      defects,
      trailerName: Math.random() < 0.5 ? `Trailer-${String(randInt(1, 20)).padStart(3, "0")}` : null,
      mechanicNotes: hasDefects && status === "defects_corrected"
        ? pick(["Replaced worn part", "Adjusted and retorqued", "Topped off fluids", "Repaired and tested OK", "Swapped component per SOP"])
        : null,
    });
  }
  return dvirs.sort((a, b) => new Date(b.startTime) - new Date(a.startTime));
}

function buildDefects(count = 40) {
  const defects = [];
  for (let i = 1; i <= count; i++) {
    const defectType = pick(DEFECT_TYPES);
    const isResolved = Math.random() < 0.55;

    defects.push({
      id: String(540000 + i),
      dvirId: String(530000 + randInt(1, 40)),
      comment: `${defectType.charAt(0).toUpperCase() + defectType.slice(1).replace(/_/g, " ")} requires attention`,
      defectType,
      isResolved,
      resolvedAt: isResolved ? isoMinutesAgo(randInt(0, 4320)) : null,
      resolvedBy: isResolved ? `${pick(FIRST_NAMES)} ${pick(LAST_NAMES)}` : null,
      createdAtTime: isoMinutesAgo(randInt(60, 43200)),
    });
  }
  return defects;
}

function buildRoutes(count = 20) {
  const routes = [];
  for (let i = 1; i <= count; i++) {
    const status = pick(ROUTE_STATUSES);
    const startCity = pick(CITY_CENTERS);
    const endCity = pick(CITY_CENTERS);
    const scheduledStartMins = randInt(60, 10080); // up to 7 days ago
    const scheduledEndMins = scheduledStartMins - randInt(120, 2880); // 2-48 hrs after start

    // Build 2-6 stops
    const stopCount = randInt(2, 6);
    const stops = [];
    for (let s = 0; s < stopCount; s++) {
      const stopCity = pick(CITY_CENTERS);
      const stopLoc = randomLocation();
      const isFirst = s === 0;
      const isLast = s === stopCount - 1;
      const stopState = status === "completed" ? "departed"
        : status === "in_progress" ? pick(STOP_STATES)
        : status === "skipped" ? "skipped"
        : "en_route";

      const scheduledArrivalMins = scheduledStartMins - (s * randInt(60, 480));
      const scheduledDepartureMins = scheduledArrivalMins - randInt(15, 120);

      stops.push({
        id: String(550000 + i * 100 + s),
        name: isFirst ? `${startCity.name} Depot` : isLast ? `${endCity.name} Terminal` : `Stop ${s} - ${stopCity.name}`,
        address: stopLoc.formattedAddress,
        latitude: stopLoc.latitude,
        longitude: stopLoc.longitude,
        scheduledArrival: isoMinutesAgo(scheduledArrivalMins),
        scheduledDeparture: isoMinutesAgo(scheduledDepartureMins),
        actualArrival: (status === "completed" || (status === "in_progress" && stopState === "departed"))
          ? isoMinutesAgo(scheduledArrivalMins - randInt(-30, 30))
          : null,
        actualDeparture: (status === "completed" || stopState === "departed")
          ? isoMinutesAgo(scheduledDepartureMins - randInt(-15, 15))
          : null,
        state: stopState,
      });
    }

    const hasActual = status === "completed" || status === "in_progress";

    routes.push({
      id: String(550000 + i),
      name: `Route ${startCity.name} → ${endCity.name} #${i}`,
      description: `${pick(["Standard freight", "Priority shipment", "LTL consolidation", "Dedicated run", "Backhaul"])} from ${startCity.name} to ${endCity.name}`,
      scheduledStartTime: isoMinutesAgo(scheduledStartMins),
      scheduledEndTime: isoMinutesAgo(scheduledEndMins),
      actualStartTime: hasActual ? isoMinutesAgo(scheduledStartMins - randInt(-15, 15)) : null,
      actualEndTime: status === "completed" ? isoMinutesAgo(scheduledEndMins - randInt(-30, 30)) : null,
      status,
      driverId: String(200000 + randInt(1, 25)),
      vehicleId: String(100000 + randInt(1, 30)),
      stops,
      notes: Math.random() < 0.3 ? pick(["Dock appointment required", "Call receiver 30 min prior", "Lumper needed at delivery", "Must arrive before 7am", "Seal required at origin"]) : "",
    });
  }
  return routes;
}

function buildFuelEnergy(vehicles, count = 30) {
  const records = [];
  for (let i = 1; i <= count; i++) {
    const v = pick(vehicles);
    const endMinsAgo = randInt(0, 10080);
    const startMinsAgo = endMinsAgo + randInt(60, 1440);
    const distanceMiles = randFloat(50, 800, 1);
    const fuelEconomy = randFloat(4.5, 8.5, 2);
    const fuelConsumed = parseFloat((distanceMiles / fuelEconomy).toFixed(2));
    const engineHours = parseFloat((distanceMiles / randFloat(35, 55, 1)).toFixed(1));
    const idleHours = parseFloat((engineHours * randFloat(0.08, 0.25, 2)).toFixed(1));
    const idleFuelGallons = parseFloat((idleHours * randFloat(0.6, 1.2, 2)).toFixed(2));
    const co2Kg = parseFloat((fuelConsumed * 10.18).toFixed(1)); // ~10.18 kg CO2 per gallon of diesel

    records.push({
      vehicleId: v.id,
      vehicleName: v.name,
      startTime: isoMinutesAgo(startMinsAgo),
      endTime: isoMinutesAgo(endMinsAgo),
      fuelConsumedGallons: fuelConsumed,
      distanceMiles,
      fuelEconomyMpg: fuelEconomy,
      idleHours,
      idleFuelGallons,
      engineHours,
      co2EmissionsKg: co2Kg,
    });
  }
  return records.sort((a, b) => new Date(b.endTime) - new Date(a.endTime));
}

function buildIftaSummary(vehicles, count = 10) {
  const summaries = [];
  for (let i = 1; i <= count; i++) {
    const v = pick(vehicles);
    const jurisdiction = pick(IFTA_JURISDICTIONS);
    const totalMiles = randFloat(200, 5000, 1);
    const fuelEconomy = randFloat(4.5, 8.0, 2);
    const totalGallons = parseFloat((totalMiles / fuelEconomy).toFixed(2));
    const taxablePct = randFloat(0.7, 1.0, 2);

    const quarterStart = new Date();
    quarterStart.setMonth(quarterStart.getMonth() - 3);
    quarterStart.setDate(1);

    summaries.push({
      vehicleId: v.id,
      vehicleName: v.name,
      jurisdiction,
      totalMiles,
      totalGallons,
      taxableMiles: parseFloat((totalMiles * taxablePct).toFixed(1)),
      taxableGallons: parseFloat((totalGallons * taxablePct).toFixed(2)),
      startDate: quarterStart.toISOString().slice(0, 10),
      endDate: new Date().toISOString().slice(0, 10),
    });
  }
  return summaries;
}

function buildAddresses(count = 25) {
  const addresses = [];
  for (let i = 1; i <= count; i++) {
    const loc = randomLocation();
    const radiusMeters = pick([100, 150, 200, 250, 500, 1000]);
    const addressType = pick(["warehouse", "customer", "terminal", "yard", "fuel_stop", "maintenance_shop"]);

    addresses.push({
      id: String(600000 + i),
      name: `${loc.city} ${addressType.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())} ${i}`,
      formattedAddress: loc.formattedAddress,
      latitude: loc.latitude,
      longitude: loc.longitude,
      radius: radiusMeters,
      geofence: {
        type: "circle",
        latitude: loc.latitude,
        longitude: loc.longitude,
        radiusMeters,
      },
      tags: assignTags(0, 2),
      contacts: Math.random() < 0.4 ? [{
        firstName: pick(FIRST_NAMES),
        lastName: pick(LAST_NAMES),
        phone: `+1${randInt(200, 999)}${randInt(1000000, 9999999)}`,
        email: `contact${i}@fleet-demo.com`,
      }] : [],
      notes: Math.random() < 0.3 ? pick(["Rear dock only", "Check in at guard shack", "Must have appointment", "24-hour access", "No overnight parking"]) : "",
      externalIds: {},
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)),
      updatedAtTime: isoMinutesAgo(randInt(0, 43200)),
    });
  }
  return addresses;
}

function buildTags() {
  const tags = [];
  for (let i = 0; i < TAG_NAMES.length; i++) {
    const name = TAG_NAMES[i];
    // Some tags are nested under a parent
    let parentTagId = null;
    if (name === "Long Haul" || name === "Regional" || name === "Dedicated") {
      // These could be children of a region
      parentTagId = null; // top-level operation types
    }
    if (name === "Hazmat" || name === "Refrigerated" || name === "Flatbed") {
      parentTagId = null; // top-level equipment types
    }

    tags.push({
      id: String(1000 + i),
      name,
      parentTagId,
      addresses: [],
      vehicles: [],
      drivers: [],
      sensors: [],
    });
  }

  // Add a couple of nested sub-tags
  tags.push({
    id: "1010",
    name: "East Coast - North",
    parentTagId: "1000", // child of East Coast
    addresses: [],
    vehicles: [],
    drivers: [],
    sensors: [],
  });
  tags.push({
    id: "1011",
    name: "East Coast - South",
    parentTagId: "1000", // child of East Coast
    addresses: [],
    vehicles: [],
    drivers: [],
    sensors: [],
  });
  tags.push({
    id: "1012",
    name: "West Coast - CA",
    parentTagId: "1001", // child of West Coast
    addresses: [],
    vehicles: [],
    drivers: [],
    sensors: [],
  });
  tags.push({
    id: "1013",
    name: "West Coast - Pacific NW",
    parentTagId: "1001", // child of West Coast
    addresses: [],
    vehicles: [],
    drivers: [],
    sensors: [],
  });

  return tags;
}

function buildUsers(count = 10) {
  const users = [];
  const usedEmails = new Set();

  for (let i = 1; i <= count; i++) {
    let first, last, email;
    do {
      first = pick(FIRST_NAMES);
      last = pick(LAST_NAMES);
      email = `${first.toLowerCase()}.${last.toLowerCase()}@fleet-demo.com`;
    } while (usedEmails.has(email));
    usedEmails.add(email);

    const role = i === 1 ? "admin" : pick(USER_ROLES);

    users.push({
      id: String(700000 + i),
      name: `${first} ${last}`,
      email,
      role,
      authType: role === "admin" && Math.random() < 0.3 ? "saml" : "default",
      tags: assignTags(0, 3),
    });
  }
  return users;
}

function buildContacts(count = 15) {
  const contacts = [];
  const usedEmails = new Set();

  for (let i = 1; i <= count; i++) {
    let first, last, email;
    do {
      first = pick(FIRST_NAMES);
      last = pick(LAST_NAMES);
      email = `${first.toLowerCase()}.${last.toLowerCase()}@fleet-demo.com`;
    } while (usedEmails.has(email));
    usedEmails.add(email);

    contacts.push({
      id: String(710000 + i),
      firstName: first,
      lastName: last,
      email,
      phone: `+1${randInt(200, 999)}${randInt(1000000, 9999999)}`,
      tags: assignTags(0, 2),
    });
  }
  return contacts;
}

function buildWebhooks(count = 5) {
  const webhooks = [];
  const endpointNames = [
    "Fleet Dashboard Sync", "Dispatch Notification Service", "Safety Alert Relay",
    "Maintenance Tracker", "Compliance Logger", "Analytics Pipeline",
    "Customer Portal Updates", "Billing Integration",
  ];

  for (let i = 1; i <= count; i++) {
    const eventCount = randInt(2, 5);
    const events = pickN(WEBHOOK_EVENT_TYPES, eventCount);

    webhooks.push({
      id: String(720000 + i),
      url: `https://hooks.fleet-demo.com/samsara/${String(i).padStart(3, "0")}`,
      name: endpointNames[i - 1] || `Webhook ${i}`,
      events,
      secretToken: generateSecretToken(),
      enabled: Math.random() < 0.8,
      version: "2024-01-01",
      createdAtTime: isoMinutesAgo(randInt(43200, 525600)),
    });
  }
  return webhooks;
}

function buildDocuments(documentTypes, drivers, vehicles, count = 30) {
  const documents = [];
  for (let i = 1; i <= count; i++) {
    const dt = pick(documentTypes);
    const d = pick(drivers);
    const v = pick(vehicles);
    const state = pick(DOCUMENT_STATES);

    const fields = [
      {
        label: "Notes",
        type: "string",
        value: pick([
          "Standard delivery", "No issues noted", "Receiver signed off",
          "Load inspected and verified", "Partial delivery - see notes",
          "Damaged freight noted on BOL", "Arrived on time", "",
        ]),
      },
      {
        label: "Reference Number",
        type: "string",
        value: `REF-${randInt(10000, 99999)}`,
      },
    ];

    // Some document types get extra fields
    if (dt.name.includes("Fuel")) {
      fields.push({ label: "Gallons", type: "number", value: String(randFloat(50, 300, 1)) });
      fields.push({ label: "Price Per Gallon", type: "number", value: String(randFloat(2.5, 4.5, 2)) });
      fields.push({ label: "Station", type: "string", value: pick(["Pilot", "Love's", "Flying J", "TA", "Petro"]) });
    }
    if (dt.name.includes("Weight")) {
      fields.push({ label: "Gross Weight (lbs)", type: "number", value: String(randInt(20000, 80000)) });
      fields.push({ label: "Tare Weight (lbs)", type: "number", value: String(randInt(12000, 18000)) });
    }

    documents.push({
      id: String(730000 + i),
      documentTypeId: dt.id,
      documentTypeName: dt.name,
      driverId: d.id,
      driverName: d.name,
      vehicleId: v.id,
      vehicleName: v.name,
      fields,
      state,
      notes: "",
      createdAtTime: isoMinutesAgo(randInt(60, 43200)),
      updatedAtTime: isoMinutesAgo(randInt(0, 4320)),
    });
  }
  return documents.sort((a, b) => new Date(b.createdAtTime) - new Date(a.createdAtTime));
}

function buildAlerts(vehicles, drivers, count = 20) {
  const alerts = [];
  const alertDescriptions = {
    vehicle: [
      { title: "Engine fault code detected", description: "Check engine light triggered - code P0301 (cylinder misfire)" },
      { title: "Low fuel warning", description: "Fuel level below 15% threshold" },
      { title: "Battery voltage low", description: "Battery voltage dropped below 11.8V" },
      { title: "High coolant temperature", description: "Engine coolant temperature exceeded 230°F" },
      { title: "Tire pressure warning", description: "Left rear outer tire pressure below 90 PSI" },
      { title: "DEF level low", description: "Diesel exhaust fluid below 10% - refill required" },
    ],
    driver: [
      { title: "Harsh braking event", description: "Severe braking detected at 0.65g deceleration" },
      { title: "Speeding alert", description: "Vehicle exceeded posted speed limit by 15+ mph" },
      { title: "HOS violation warning", description: "Driver approaching 11-hour driving limit" },
      { title: "Unassigned driving detected", description: "Vehicle moving with no driver assigned to ELD" },
      { title: "Seatbelt unbuckled", description: "Driver seatbelt not engaged while vehicle in motion" },
    ],
    geofence: [
      { title: "Geofence entry", description: "Vehicle entered customer delivery zone" },
      { title: "Geofence exit", description: "Vehicle departed from yard" },
      { title: "Unauthorized area entry", description: "Vehicle entered restricted zone" },
      { title: "Late arrival at geofence", description: "Vehicle arrived 45 minutes past scheduled window" },
    ],
  };

  for (let i = 1; i <= count; i++) {
    const type = pick(ALERT_TYPES);
    const severity = pick(ALERT_SEVERITY);
    const alertInfo = pick(alertDescriptions[type]);
    const isResolved = Math.random() < 0.6;
    const eventTime = isoMinutesAgo(randInt(10, 10080));

    let entityType, entityId, entityName;
    if (type === "vehicle" || type === "geofence") {
      const v = pick(vehicles);
      entityType = "vehicle";
      entityId = v.id;
      entityName = v.name;
    } else {
      const d = pick(drivers);
      entityType = "driver";
      entityId = d.id;
      entityName = d.name;
    }

    alerts.push({
      id: String(740000 + i),
      type,
      severity,
      title: alertInfo.title,
      description: alertInfo.description,
      time: eventTime,
      resolvedTime: isResolved ? isoMinutesAgo(randInt(0, 1440)) : null,
      isResolved,
      entityType,
      entityId,
      entityName,
    });
  }
  return alerts.sort((a, b) => new Date(b.time) - new Date(a.time));
}

function buildAlertConfigurations(count = 10) {
  const configs = [];
  const configTemplates = [
    { name: "Speeding over 70 mph", type: "speeding", conditions: { speedThresholdMph: 70, durationSeconds: 10 } },
    { name: "Speeding over 80 mph", type: "speeding", conditions: { speedThresholdMph: 80, durationSeconds: 5 } },
    { name: "Customer site entry", type: "geofence_entry", conditions: { geofenceIds: ["600001", "600005", "600010"] } },
    { name: "Customer site exit", type: "geofence_exit", conditions: { geofenceIds: ["600001", "600005", "600010"] } },
    { name: "Idle over 15 minutes", type: "idle", conditions: { idleDurationMinutes: 15 } },
    { name: "Idle over 30 minutes", type: "idle", conditions: { idleDurationMinutes: 30 } },
    { name: "Harsh event - any severity", type: "harsh_event", conditions: { severities: ["minor", "major", "critical"] } },
    { name: "Harsh event - critical only", type: "harsh_event", conditions: { severities: ["critical"] } },
    { name: "Engine fault code", type: "engine_fault", conditions: { faultSeverities: ["warning", "critical"] } },
    { name: "Engine critical fault", type: "engine_fault", conditions: { faultSeverities: ["critical"] } },
    { name: "Yard departure alert", type: "geofence_exit", conditions: { geofenceIds: ["600002", "600008"] } },
    { name: "Speeding in school zone", type: "speeding", conditions: { speedThresholdMph: 25, durationSeconds: 3, geofenceIds: ["600015"] } },
  ];

  for (let i = 0; i < Math.min(count, configTemplates.length); i++) {
    const tmpl = configTemplates[i];
    configs.push({
      id: String(750000 + i + 1),
      name: tmpl.name,
      type: tmpl.type,
      enabled: Math.random() < 0.8,
      conditions: tmpl.conditions,
      notifications: {
        email: Math.random() < 0.7 ? [`alerts@fleet-demo.com`, `dispatch@fleet-demo.com`] : [],
        sms: Math.random() < 0.4 ? [`+1${randInt(200, 999)}${randInt(1000000, 9999999)}`] : [],
        webhook: Math.random() < 0.3 ? [`https://hooks.fleet-demo.com/samsara/alerts`] : [],
      },
      tags: assignTags(0, 2),
    });
  }
  return configs;
}

function buildIndustrialData(count = 15) {
  const machines = [];
  for (let i = 1; i <= count; i++) {
    const machineName = i <= MACHINE_NAMES.length ? MACHINE_NAMES[i - 1] : `Machine-${i}`;
    const pointCount = randInt(3, 7);
    const selectedPoints = pickN(INDUSTRIAL_DATAPOINT_NAMES, pointCount);

    const dataPoints = selectedPoints.map((dp) => ({
      name: dp.name,
      value: randFloat(dp.min, dp.max, 2),
      unit: dp.unit,
      time: isoMinutesAgo(randInt(0, 60)),
    }));

    machines.push({
      id: String(800000 + i),
      machineName,
      dataPoints,
    });
  }
  return machines;
}

function buildSensors(count = 10) {
  const sensors = [];
  for (let i = 1; i <= count; i++) {
    const sensorType = pick(SENSOR_TYPES);
    const sensorNames = {
      temperature: `Temp Sensor ${i}`,
      humidity: `Humidity Sensor ${i}`,
      door: `Door Sensor ${i}`,
      gps: `GPS Tracker ${i}`,
    };

    sensors.push({
      id: String(810000 + i),
      name: sensorNames[sensorType],
      macAddress: generateMAC(),
      sensorType,
      containerOrTrailerId: sensorType === "temperature" || sensorType === "door"
        ? String(400000 + randInt(1, 20))
        : null,
      assets: Math.random() < 0.4 ? [{ id: String(410000 + randInt(1, 15)), name: `AST-${String(randInt(1, 15)).padStart(4, "0")}` }] : [],
    });
  }
  return sensors;
}

function buildReeferStats(count = 10) {
  const stats = [];
  for (let i = 1; i <= count; i++) {
    const zoneCount = pick([1, 2, 3]);
    const zones = [];
    for (let z = 1; z <= zoneCount; z++) {
      const setPointC = pick([-18000, -5000, 0, 2000, 4000, 7000]); // in milliCelsius
      const currentC = setPointC + randInt(-2000, 2000);

      zones.push({
        zoneId: String(z),
        tempMilliC: currentC,
        setPointMilliC: setPointC,
        status: Math.abs(currentC - setPointC) < 3000 ? "at_setpoint" : "cooling",
      });
    }

    stats.push({
      id: String(820000 + i),
      assetName: `Reefer-${String(i).padStart(3, "0")}`,
      trailerId: String(400000 + randInt(1, 20)),
      zones,
      ambientTempMilliC: randInt(15000, 40000),
      engineHours: randFloat(500, 15000, 1),
      fuelPercent: randFloat(10, 100, 1),
      returnAirTempMilliC: zones.length > 0 ? zones[0].tempMilliC + randInt(500, 3000) : null,
      time: isoMinutesAgo(randInt(0, 60)),
    });
  }
  return stats;
}

function buildDispatchJobs(routes, vehicles, drivers, count = 15) {
  const jobs = [];
  for (let i = 1; i <= count; i++) {
    const route = i <= routes.length ? routes[i - 1] : pick(routes);
    const status = pick(DISPATCH_STATUSES);
    const v = pick(vehicles);
    const d = pick(drivers);

    const scheduledStartMins = randInt(60, 10080);
    const scheduledEndMins = scheduledStartMins - randInt(120, 2880);

    jobs.push({
      id: String(900000 + i),
      routeId: route.id,
      status,
      fleet_viewer_url: `https://cloud.samsara.com/fleet/viewer/job/${900000 + i}`,
      scheduled_start: isoMinutesAgo(scheduledStartMins),
      scheduled_end: isoMinutesAgo(Math.max(scheduledEndMins, 0)),
      driverId: status === "unassigned" ? null : d.id,
      driverName: status === "unassigned" ? null : d.name,
      vehicleId: status === "unassigned" ? null : v.id,
      vehicleName: status === "unassigned" ? null : v.name,
      notes: Math.random() < 0.3 ? pick([
        "High priority delivery", "Fragile cargo - handle with care",
        "Requires signature on delivery", "Time-sensitive medical supplies",
        "Oversize load - permit required", "Customer requested early AM delivery",
      ]) : "",
    });
  }
  return jobs;
}

// ---------------------------------------------------------------------------
// Generate all datasets (called once at startup to keep data stable per run)
// ---------------------------------------------------------------------------

const vehicles = buildVehicles(30);
const drivers = buildDrivers(25);
const vehicleStats = buildVehicleStats(vehicles);
const vehicleLocations = buildVehicleLocations(vehicles);
const documentTypes = buildDocumentTypes();
const trailers = buildTrailers(20);
const assets = buildAssets(15);
const safetyEvents = buildSafetyEvents(vehicles, drivers, 50);
const hosLogs = buildHosLogs(drivers, 100);
const hosViolations = buildHosViolations(drivers, 30);
const hosClocks = buildHosClocks(drivers);
const dvirs = buildDvirs(vehicles, drivers, 40);
const defects = buildDefects(40);
const routes = buildRoutes(20);
const fuelEnergy = buildFuelEnergy(vehicles, 30);
const iftaSummary = buildIftaSummary(vehicles, 10);
const addresses = buildAddresses(25);
const tags = buildTags();
const users = buildUsers(10);
const contacts = buildContacts(15);
const webhooks = buildWebhooks(5);
const documents = buildDocuments(documentTypes, drivers, vehicles, 30);
const alerts = buildAlerts(vehicles, drivers, 20);
const alertConfigurations = buildAlertConfigurations(10);
const industrialData = buildIndustrialData(15);
const sensors = buildSensors(10);
const reeferStats = buildReeferStats(10);
const dispatchJobs = buildDispatchJobs(routes, vehicles, drivers, 15);

// ---------------------------------------------------------------------------
// Export all datasets
// ---------------------------------------------------------------------------

module.exports = {
  // Original datasets
  vehicles,
  drivers,
  vehicleStats,
  vehicleLocations,
  documentTypes,
  // New datasets
  trailers,
  assets,
  safetyEvents,
  hosLogs,
  hosViolations,
  hosClocks,
  dvirs,
  defects,
  routes,
  fuelEnergy,
  iftaSummary,
  addresses,
  tags,
  users,
  contacts,
  webhooks,
  documents,
  alerts,
  alertConfigurations,
  industrialData,
  sensors,
  reeferStats,
  dispatchJobs,
};
