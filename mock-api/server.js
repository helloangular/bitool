const express = require("express");
const {
  vehicles,
  drivers,
  vehicleStats,
  vehicleLocations,
  documentTypes,
  trailers,
  assets,
  safetyEvents,
  hosLogs,
  hosViolations,
  hosClocks,
  dvirs,
  defects,
  routes: fleetRoutes,
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
} = require("./data");

const app = express();
const PORT = process.env.PORT || 3001;
const VALID_TOKEN = process.env.MOCK_TOKEN || "mock-samsara-token";
const DEFAULT_PAGE_SIZE = 10;

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

app.use(express.json());

// Bearer token auth
app.use((req, res, next) => {
  // Allow health check without auth
  if (req.path === "/health") return next();

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({
      message: "Unauthorized. Provide a valid Bearer token in the Authorization header.",
    });
  }

  const token = authHeader.slice(7);
  if (token !== VALID_TOKEN) {
    return res.status(401).json({
      message: "Unauthorized. Invalid token.",
    });
  }

  next();
});

// Request logging
app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.originalUrl}`);
  next();
});

// ---------------------------------------------------------------------------
// Pagination helper
// ---------------------------------------------------------------------------

/**
 * Samsara-style cursor pagination.
 * Cursors are of the form "cursor_<offset>".
 * Query params: `limit` (page size), `after` (cursor from previous response).
 */
function paginate(dataset, req) {
  const limit = Math.min(
    parseInt(req.query.limit, 10) || DEFAULT_PAGE_SIZE,
    100
  );
  let offset = 0;

  if (req.query.after) {
    const match = req.query.after.match(/^cursor_(\d+)$/);
    if (match) {
      offset = parseInt(match[1], 10);
    }
  }

  const page = dataset.slice(offset, offset + limit);
  const hasNextPage = offset + limit < dataset.length;
  const endCursor = hasNextPage ? `cursor_${offset + limit}` : "";

  return {
    data: page,
    pagination: {
      endCursor,
      hasNextPage,
    },
  };
}

// ---------------------------------------------------------------------------
// Helper: generate unique id
// ---------------------------------------------------------------------------

let _idCounter = Date.now();
function generateId() {
  return String(++_idCounter);
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health check (no auth required)
app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "mock-samsara-api" });
});

// ===========================================================================
// Vehicles
// ===========================================================================

// GET /fleet/vehicles
app.get("/fleet/vehicles", (req, res) => {
  let filtered = vehicles;

  // Watermark filter: accept both the UI/runtime default `updated_since`
  // and older local/mock parameter names used by earlier ingestion paths.
  const updatedAfter =
    req.query.updated_since ||
    req.query.updatedAfter ||
    req.query.data_items_updatedAtTime;
  if (updatedAfter) {
    const threshold = new Date(updatedAfter);
    filtered = filtered.filter((v) => new Date(v.updatedAtTime) > threshold);
  }

  // Optional tag filter (Samsara supports tagIds param)
  if (req.query.tagIds) {
    const tagIds = req.query.tagIds.split(",");
    filtered = filtered.filter((v) =>
      v.tags.some((t) => tagIds.includes(t.id))
    );
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/vehicles/stats (must be before :id to avoid matching "stats")
app.get("/fleet/vehicles/stats", (req, res) => {
  let filtered = vehicleStats;

  // Optional: filter by vehicle IDs (comma-separated)
  if (req.query.vehicleIds) {
    const ids = req.query.vehicleIds.split(",");
    filtered = filtered.filter((s) => ids.includes(s.id));
  }

  // Samsara requires types param; we accept it but don't filter by it
  // since our mock always returns all stat types.

  res.json(paginate(filtered, req));
});

// GET /fleet/vehicles/locations (must be before :id)
app.get("/fleet/vehicles/locations", (req, res) => {
  let filtered = vehicleLocations;

  if (req.query.vehicleIds) {
    const ids = req.query.vehicleIds.split(",");
    filtered = filtered.filter((l) => ids.includes(l.id));
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/vehicles/fuel-energy (must be before :id)
app.get("/fleet/vehicles/fuel-energy", (req, res) => {
  let filtered = fuelEnergy;

  if (req.query.vehicleId) {
    filtered = filtered.filter((f) => f.vehicleId === req.query.vehicleId);
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/vehicles/:id/stats/history
app.get("/fleet/vehicles/:id/stats/history", (req, res) => {
  const filtered = vehicleStats.filter((s) => s.id === req.params.id);
  if (filtered.length === 0) {
    return res.status(404).json({ message: `Vehicle ${req.params.id} not found or has no stats` });
  }
  res.json({ data: filtered });
});

// GET /fleet/vehicles/:id/safety/score
app.get("/fleet/vehicles/:id/safety/score", (req, res) => {
  const vehicle = vehicles.find((v) => v.id === req.params.id);
  if (!vehicle) {
    return res.status(404).json({ message: `Vehicle ${req.params.id} not found` });
  }
  const eventsForVehicle = safetyEvents.filter(
    (e) => e.vehicle && e.vehicle.id === req.params.id
  );
  const score = Math.max(0, 100 - eventsForVehicle.length * 5);
  res.json({
    data: {
      vehicleId: req.params.id,
      name: vehicle.name,
      safetyScore: score,
      totalEvents: eventsForVehicle.length,
      timeRange: { startTime: "2024-01-01T00:00:00Z", endTime: "2024-12-31T23:59:59Z" },
    },
  });
});

// GET /fleet/vehicles/:id
app.get("/fleet/vehicles/:id", (req, res) => {
  const vehicle = vehicles.find((v) => v.id === req.params.id);
  if (!vehicle) {
    return res.status(404).json({ message: `Vehicle ${req.params.id} not found` });
  }
  res.json({ data: vehicle });
});

// PATCH /fleet/vehicles/:id
app.patch("/fleet/vehicles/:id", (req, res) => {
  const vehicle = vehicles.find((v) => v.id === req.params.id);
  if (!vehicle) {
    return res.status(404).json({ message: `Vehicle ${req.params.id} not found` });
  }
  Object.assign(vehicle, req.body);
  res.json({ data: vehicle });
});

// ===========================================================================
// Drivers
// ===========================================================================

// GET /fleet/drivers
app.get("/fleet/drivers", (req, res) => {
  let filtered = drivers;

  // Optional status filter
  if (req.query.driverActivationStatus) {
    filtered = filtered.filter(
      (d) => d.driverActivationStatus === req.query.driverActivationStatus
    );
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/drivers/:id/hos/daily-logs
app.get("/fleet/drivers/:id/hos/daily-logs", (req, res) => {
  const filtered = hosLogs.filter((l) => l.driverId === req.params.id);
  res.json(paginate(filtered, req));
});

// GET /fleet/drivers/:id/hos/violations
app.get("/fleet/drivers/:id/hos/violations", (req, res) => {
  const filtered = hosViolations.filter((v) => v.driverId === req.params.id);
  res.json(paginate(filtered, req));
});

// GET /fleet/drivers/:id/safety/score
app.get("/fleet/drivers/:id/safety/score", (req, res) => {
  const driver = drivers.find((d) => d.id === req.params.id);
  if (!driver) {
    return res.status(404).json({ message: `Driver ${req.params.id} not found` });
  }
  const eventsForDriver = safetyEvents.filter(
    (e) => e.driver && e.driver.id === req.params.id
  );
  const score = Math.max(0, 100 - eventsForDriver.length * 5);
  res.json({
    data: {
      driverId: req.params.id,
      name: driver.name,
      safetyScore: score,
      totalEvents: eventsForDriver.length,
      timeRange: { startTime: "2024-01-01T00:00:00Z", endTime: "2024-12-31T23:59:59Z" },
    },
  });
});

// GET /fleet/drivers/:id/tachograph-files
app.get("/fleet/drivers/:id/tachograph-files", (req, res) => {
  const driver = drivers.find((d) => d.id === req.params.id);
  if (!driver) {
    return res.status(404).json({ message: `Driver ${req.params.id} not found` });
  }
  res.json({
    data: [
      {
        id: `tacho-${req.params.id}-001`,
        driverId: req.params.id,
        driverName: driver.name,
        downloadedAtTime: "2024-06-15T08:30:00Z",
        fileType: "ddd",
        period: { startTime: "2024-06-01T00:00:00Z", endTime: "2024-06-15T00:00:00Z" },
      },
    ],
  });
});

// GET /fleet/drivers/:id
app.get("/fleet/drivers/:id", (req, res) => {
  const driver = drivers.find((d) => d.id === req.params.id);
  if (!driver) {
    return res.status(404).json({ message: `Driver ${req.params.id} not found` });
  }
  res.json({ data: driver });
});

// PATCH /fleet/drivers/:id
app.patch("/fleet/drivers/:id", (req, res) => {
  const driver = drivers.find((d) => d.id === req.params.id);
  if (!driver) {
    return res.status(404).json({ message: `Driver ${req.params.id} not found` });
  }
  Object.assign(driver, req.body);
  res.json({ data: driver });
});

// ===========================================================================
// Document Types (existing)
// ===========================================================================

// GET /fleet/document-types
app.get("/fleet/document-types", (req, res) => {
  res.json(paginate(documentTypes, req));
});

// ===========================================================================
// Safety
// ===========================================================================

// GET /fleet/safety/events
app.get("/fleet/safety/events", (req, res) => {
  let filtered = safetyEvents;

  if (req.query.vehicleId) {
    filtered = filtered.filter(
      (e) => e.vehicle && e.vehicle.id === req.query.vehicleId
    );
  }
  if (req.query.driverId) {
    filtered = filtered.filter(
      (e) => e.driver && e.driver.id === req.query.driverId
    );
  }
  if (req.query.type) {
    filtered = filtered.filter((e) => e.type === req.query.type);
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/safety/scores/vehicles
app.get("/fleet/safety/scores/vehicles", (_req, res) => {
  const scores = vehicles.map((v) => {
    const count = safetyEvents.filter(
      (e) => e.vehicle && e.vehicle.id === v.id
    ).length;
    return {
      vehicleId: v.id,
      name: v.name,
      safetyScore: Math.max(0, 100 - count * 5),
      totalEvents: count,
    };
  });
  res.json({ data: scores });
});

// GET /fleet/safety/scores/drivers
app.get("/fleet/safety/scores/drivers", (_req, res) => {
  const scores = drivers.map((d) => {
    const count = safetyEvents.filter(
      (e) => e.driver && e.driver.id === d.id
    ).length;
    return {
      driverId: d.id,
      name: d.name,
      safetyScore: Math.max(0, 100 - count * 5),
      totalEvents: count,
    };
  });
  res.json({ data: scores });
});

// GET /fleet/safety/events/:id
app.get("/fleet/safety/events/:id", (req, res) => {
  const event = safetyEvents.find((e) => e.id === req.params.id);
  if (!event) {
    return res.status(404).json({ message: `Safety event ${req.params.id} not found` });
  }
  res.json({ data: event });
});

// ===========================================================================
// HOS / Compliance
// ===========================================================================

// GET /fleet/hos/logs
app.get("/fleet/hos/logs", (req, res) => {
  let filtered = hosLogs;
  if (req.query.driverId) {
    filtered = filtered.filter((l) => l.driverId === req.query.driverId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/hos/violations
app.get("/fleet/hos/violations", (req, res) => {
  let filtered = hosViolations;
  if (req.query.driverId) {
    filtered = filtered.filter((v) => v.driverId === req.query.driverId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/hos/clocks
app.get("/fleet/hos/clocks", (req, res) => {
  let filtered = hosClocks;
  if (req.query.driverId) {
    filtered = filtered.filter((c) => c.driverId === req.query.driverId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/hos/daily-logs (alias for /fleet/hos/logs)
app.get("/fleet/hos/daily-logs", (req, res) => {
  let filtered = hosLogs;
  if (req.query.driverId) {
    filtered = filtered.filter((l) => l.driverId === req.query.driverId);
  }
  res.json(paginate(filtered, req));
});

// ===========================================================================
// Trailers / Equipment
// ===========================================================================

// GET /fleet/trailers/locations (must be before :id)
app.get("/fleet/trailers/locations", (_req, res) => {
  const data = trailers.map((t) => ({
    id: t.id,
    name: t.name,
    location: t.location || {
      latitude: 39.7392 + Math.random() * 0.1,
      longitude: -104.9903 + Math.random() * 0.1,
      time: new Date().toISOString(),
    },
  }));
  res.json({ data });
});

// GET /fleet/trailers/stats (must be before :id)
app.get("/fleet/trailers/stats", (_req, res) => {
  const data = trailers.map((t) => ({
    id: t.id,
    name: t.name,
    odometerMeters: t.odometerMeters || 150000,
    engineHours: t.engineHours || 3200,
    gps: t.location || {
      latitude: 39.7392,
      longitude: -104.9903,
      time: new Date().toISOString(),
    },
  }));
  res.json({ data });
});

// GET /fleet/trailers
app.get("/fleet/trailers", (req, res) => {
  let filtered = trailers;

  if (req.query.tagIds) {
    const tagIds = req.query.tagIds.split(",");
    filtered = filtered.filter(
      (t) => t.tags && t.tags.some((tag) => tagIds.includes(tag.id))
    );
  }

  res.json(paginate(filtered, req));
});

// GET /fleet/trailers/:id
app.get("/fleet/trailers/:id", (req, res) => {
  const trailer = trailers.find((t) => t.id === req.params.id);
  if (!trailer) {
    return res.status(404).json({ message: `Trailer ${req.params.id} not found` });
  }
  res.json({ data: trailer });
});

// POST /fleet/trailers
app.post("/fleet/trailers", (req, res) => {
  const newTrailer = { id: generateId(), ...req.body };
  trailers.push(newTrailer);
  res.status(201).json({ data: newTrailer });
});

// PATCH /fleet/trailers/:id
app.patch("/fleet/trailers/:id", (req, res) => {
  const trailer = trailers.find((t) => t.id === req.params.id);
  if (!trailer) {
    return res.status(404).json({ message: `Trailer ${req.params.id} not found` });
  }
  Object.assign(trailer, req.body);
  res.json({ data: trailer });
});

// ===========================================================================
// Assets
// ===========================================================================

// GET /fleet/assets/locations (must be before :id)
app.get("/fleet/assets/locations", (_req, res) => {
  const data = assets.map((a) => ({
    id: a.id,
    name: a.name,
    location: a.location || {
      latitude: 40.7128 + Math.random() * 0.1,
      longitude: -74.006 + Math.random() * 0.1,
      time: new Date().toISOString(),
    },
  }));
  res.json({ data });
});

// GET /fleet/assets/reefers/stats
app.get("/fleet/assets/reefers/stats", (req, res) => {
  res.json(paginate(reeferStats, req));
});

// GET /fleet/assets
app.get("/fleet/assets", (req, res) => {
  res.json(paginate(assets, req));
});

// GET /fleet/assets/:id
app.get("/fleet/assets/:id", (req, res) => {
  const asset = assets.find((a) => a.id === req.params.id);
  if (!asset) {
    return res.status(404).json({ message: `Asset ${req.params.id} not found` });
  }
  res.json({ data: asset });
});

// ===========================================================================
// Maintenance / DVIR
// ===========================================================================

// GET /fleet/maintenance/dvirs
app.get("/fleet/maintenance/dvirs", (req, res) => {
  let filtered = dvirs;
  if (req.query.vehicleId) {
    filtered = filtered.filter((d) => d.vehicleId === req.query.vehicleId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/maintenance/dvirs/:id
app.get("/fleet/maintenance/dvirs/:id", (req, res) => {
  const dvir = dvirs.find((d) => d.id === req.params.id);
  if (!dvir) {
    return res.status(404).json({ message: `DVIR ${req.params.id} not found` });
  }
  res.json({ data: dvir });
});

// POST /fleet/maintenance/dvirs
app.post("/fleet/maintenance/dvirs", (req, res) => {
  const newDvir = { id: generateId(), ...req.body };
  dvirs.push(newDvir);
  res.status(201).json({ data: newDvir });
});

// GET /fleet/defects
app.get("/fleet/defects", (req, res) => {
  let filtered = defects;
  if (req.query.isResolved !== undefined) {
    const resolved = req.query.isResolved === "true";
    filtered = filtered.filter((d) => d.isResolved === resolved);
  }
  res.json(paginate(filtered, req));
});

// ===========================================================================
// Routing / Dispatch
// ===========================================================================

// GET /fleet/routes
app.get("/fleet/routes", (req, res) => {
  let filtered = fleetRoutes;
  if (req.query.status) {
    filtered = filtered.filter((r) => r.status === req.query.status);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/dispatch/routes
app.get("/fleet/dispatch/routes", (req, res) => {
  let filtered = fleetRoutes;
  if (req.query.status) {
    filtered = filtered.filter((r) => r.status === req.query.status);
  }
  const data = filtered.map((r) => ({
    ...r,
    dispatchInfo: {
      scheduledStartTime: r.scheduledStartTime || "2024-06-01T08:00:00Z",
      scheduledEndTime: r.scheduledEndTime || "2024-06-01T17:00:00Z",
    },
  }));
  res.json(paginate(data, req));
});

// GET /fleet/dispatch/jobs
app.get("/fleet/dispatch/jobs", (req, res) => {
  res.json(paginate(dispatchJobs, req));
});

// GET /fleet/routes/:id
app.get("/fleet/routes/:id", (req, res) => {
  const route = fleetRoutes.find((r) => r.id === req.params.id);
  if (!route) {
    return res.status(404).json({ message: `Route ${req.params.id} not found` });
  }
  res.json({ data: route });
});

// POST /fleet/routes
app.post("/fleet/routes", (req, res) => {
  const newRoute = { id: generateId(), status: "scheduled", ...req.body };
  fleetRoutes.push(newRoute);
  res.status(201).json({ data: newRoute });
});

// ===========================================================================
// Fuel / Efficiency
// ===========================================================================

// GET /fleet/fuel-energy/vehicle-report (alias)
app.get("/fleet/fuel-energy/vehicle-report", (req, res) => {
  let filtered = fuelEnergy;
  if (req.query.vehicleId) {
    filtered = filtered.filter((f) => f.vehicleId === req.query.vehicleId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/ifta/summary
app.get("/fleet/ifta/summary", (req, res) => {
  res.json(paginate(iftaSummary, req));
});

// ===========================================================================
// Addresses / Geofences
// ===========================================================================

// GET /fleet/geofences (must be before /addresses routes since it's a different path)
app.get("/fleet/geofences", (_req, res) => {
  const data = addresses.map((a) => ({
    id: a.id,
    name: a.name,
    formattedAddress: a.formattedAddress,
    geofence: a.geofence || {
      circle: {
        latitude: a.latitude || 39.7392,
        longitude: a.longitude || -104.9903,
        radiusMeters: a.radiusMeters || 200,
      },
    },
    tags: a.tags || [],
  }));
  res.json({ data });
});

// GET /addresses
app.get("/addresses", (req, res) => {
  res.json(paginate(addresses, req));
});

// GET /addresses/:id
app.get("/addresses/:id", (req, res) => {
  const address = addresses.find((a) => a.id === req.params.id);
  if (!address) {
    return res.status(404).json({ message: `Address ${req.params.id} not found` });
  }
  res.json({ data: address });
});

// POST /addresses
app.post("/addresses", (req, res) => {
  const newAddress = { id: generateId(), ...req.body };
  addresses.push(newAddress);
  res.status(201).json({ data: newAddress });
});

// ===========================================================================
// Tags
// ===========================================================================

// GET /tags
app.get("/tags", (req, res) => {
  res.json(paginate(tags, req));
});

// GET /tags/:id
app.get("/tags/:id", (req, res) => {
  const tag = tags.find((t) => t.id === req.params.id);
  if (!tag) {
    return res.status(404).json({ message: `Tag ${req.params.id} not found` });
  }
  res.json({ data: tag });
});

// POST /tags
app.post("/tags", (req, res) => {
  const newTag = { id: generateId(), ...req.body };
  tags.push(newTag);
  res.status(201).json({ data: newTag });
});

// ===========================================================================
// Organization / Users
// ===========================================================================

// GET /me
app.get("/me", (_req, res) => {
  if (users.length === 0) {
    return res.status(404).json({ message: "No users configured" });
  }
  res.json({ data: users[0] });
});

// GET /fleet/users
app.get("/fleet/users", (req, res) => {
  res.json(paginate(users, req));
});

// GET /fleet/users/:id
app.get("/fleet/users/:id", (req, res) => {
  const user = users.find((u) => u.id === req.params.id);
  if (!user) {
    return res.status(404).json({ message: `User ${req.params.id} not found` });
  }
  res.json({ data: user });
});

// ===========================================================================
// Contacts
// ===========================================================================

// GET /contacts
app.get("/contacts", (req, res) => {
  res.json(paginate(contacts, req));
});

// GET /contacts/:id
app.get("/contacts/:id", (req, res) => {
  const contact = contacts.find((c) => c.id === req.params.id);
  if (!contact) {
    return res.status(404).json({ message: `Contact ${req.params.id} not found` });
  }
  res.json({ data: contact });
});

// POST /contacts
app.post("/contacts", (req, res) => {
  const newContact = { id: generateId(), ...req.body };
  contacts.push(newContact);
  res.status(201).json({ data: newContact });
});

// ===========================================================================
// Webhooks
// ===========================================================================

// GET /webhooks
app.get("/webhooks", (req, res) => {
  res.json(paginate(webhooks, req));
});

// POST /webhooks
app.post("/webhooks", (req, res) => {
  const newWebhook = { id: generateId(), ...req.body };
  webhooks.push(newWebhook);
  res.status(201).json({ data: newWebhook });
});

// ===========================================================================
// Documents
// ===========================================================================

// GET /fleet/documents
app.get("/fleet/documents", (req, res) => {
  let filtered = documents;
  if (req.query.driverId) {
    filtered = filtered.filter((d) => d.driverId === req.query.driverId);
  }
  if (req.query.vehicleId) {
    filtered = filtered.filter((d) => d.vehicleId === req.query.vehicleId);
  }
  res.json(paginate(filtered, req));
});

// GET /fleet/documents/:id
app.get("/fleet/documents/:id", (req, res) => {
  const doc = documents.find((d) => d.id === req.params.id);
  if (!doc) {
    return res.status(404).json({ message: `Document ${req.params.id} not found` });
  }
  res.json({ data: doc });
});

// POST /fleet/documents
app.post("/fleet/documents", (req, res) => {
  const newDoc = { id: generateId(), ...req.body };
  documents.push(newDoc);
  res.status(201).json({ data: newDoc });
});

// ===========================================================================
// Alerts
// ===========================================================================

// GET /alerts/configurations (must be before /alerts/:id pattern if alerts had one)
app.get("/alerts/configurations", (req, res) => {
  res.json(paginate(alertConfigurations, req));
});

// GET /alerts
app.get("/alerts", (req, res) => {
  let filtered = alerts;
  if (req.query.type) {
    filtered = filtered.filter((a) => a.type === req.query.type);
  }
  if (req.query.severity) {
    filtered = filtered.filter((a) => a.severity === req.query.severity);
  }
  if (req.query.isResolved !== undefined) {
    const resolved = req.query.isResolved === "true";
    filtered = filtered.filter((a) => a.isResolved === resolved);
  }
  res.json(paginate(filtered, req));
});

// ===========================================================================
// Industrial / Sensors
// ===========================================================================

// GET /industrial/data
app.get("/industrial/data", (req, res) => {
  res.json(paginate(industrialData, req));
});

// GET /sensors/list (legacy Samsara endpoint)
app.get("/sensors/list", (req, res) => {
  res.json(paginate(sensors, req));
});

// GET /sensors/temperature
app.get("/sensors/temperature", (_req, res) => {
  const data = sensors.map((s) => ({
    id: s.id,
    name: s.name,
    ambientTemperature: s.ambientTemperature || 22.5,
    probeTemperature: s.probeTemperature || -18.0,
    currentTime: new Date().toISOString(),
  }));
  res.json({ data });
});

// GET /sensors/humidity
app.get("/sensors/humidity", (_req, res) => {
  const data = sensors.map((s) => ({
    id: s.id,
    name: s.name,
    humidity: s.humidity || 45,
    currentTime: new Date().toISOString(),
  }));
  res.json({ data });
});

// GET /sensors/door
app.get("/sensors/door", (_req, res) => {
  const data = sensors.map((s) => ({
    id: s.id,
    name: s.name,
    doorClosed: s.doorClosed !== undefined ? s.doorClosed : true,
    currentTime: new Date().toISOString(),
  }));
  res.json({ data });
});

// ---------------------------------------------------------------------------
// Catch-all for unmatched routes
// ---------------------------------------------------------------------------

app.use((_req, res) => {
  res.status(404).json({ message: "Endpoint not found" });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`Mock Samsara API listening on port ${PORT}`);
  console.log(`  Vehicles:        ${vehicles.length} records`);
  console.log(`  Drivers:         ${drivers.length} records`);
  console.log(`  Vehicle Stats:   ${vehicleStats.length} records`);
  console.log(`  Locations:       ${vehicleLocations.length} records`);
  console.log(`  Doc Types:       ${documentTypes.length} records`);
  console.log(`  Trailers:        ${trailers.length} records`);
  console.log(`  Assets:          ${assets.length} records`);
  console.log(`  Safety Events:   ${safetyEvents.length} records`);
  console.log(`  HOS Logs:        ${hosLogs.length} records`);
  console.log(`  HOS Violations:  ${hosViolations.length} records`);
  console.log(`  HOS Clocks:      ${hosClocks.length} records`);
  console.log(`  DVIRs:           ${dvirs.length} records`);
  console.log(`  Defects:         ${defects.length} records`);
  console.log(`  Routes:          ${fleetRoutes.length} records`);
  console.log(`  Dispatch Jobs:   ${dispatchJobs.length} records`);
  console.log(`  Fuel/Energy:     ${fuelEnergy.length} records`);
  console.log(`  IFTA Summary:    ${iftaSummary.length} records`);
  console.log(`  Addresses:       ${addresses.length} records`);
  console.log(`  Tags:            ${tags.length} records`);
  console.log(`  Users:           ${users.length} records`);
  console.log(`  Contacts:        ${contacts.length} records`);
  console.log(`  Webhooks:        ${webhooks.length} records`);
  console.log(`  Documents:       ${documents.length} records`);
  console.log(`  Alerts:          ${alerts.length} records`);
  console.log(`  Alert Configs:   ${alertConfigurations.length} records`);
  console.log(`  Industrial Data: ${industrialData.length} records`);
  console.log(`  Sensors:         ${sensors.length} records`);
  console.log(`  Reefer Stats:    ${reeferStats.length} records`);
  console.log(`  Auth token:      ${VALID_TOKEN}`);
});
