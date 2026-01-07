'use strict';

const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const path = require('path');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const compression = require('compression');
const { performance } = require('perf_hooks');

const app = express();

// Trust first proxy (Render uses proxies) - MUST BE FIRST
app.set('trust proxy', 1);

// Configuration
const HOST = process.env.HOST || '0.0.0.0';
const PORT = parseInt(process.env.PORT, 10) || 4000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/leveldb_converted?retryWrites=true&w=majority';
const DB_NAME = process.env.DB_NAME || 'leveldb_converted';
const NODE_ENV = process.env.NODE_ENV || 'development';

const SLOT_CAPACITY = 250;
const MAX_LIKED_BY_PER_POST = 1000;
const PORT_2000_URL = process.env.PORT_2000_URL || 'https://samir-hgr9.onrender.com';
const PORT_4000_URL = process.env.PORT_4000_URL || 'https://database-22io.onrender.com';

class SimpleCache {
  constructor(ttlMs = 60000) { // Default 1 minute TTL
    this.cache = new Map();
    this.ttl = ttlMs;
  }

  set(key, value) {
    this.cache.set(key, {
      value,
      expiry: Date.now() + this.ttl
    });
  }

  get(key) {
    const item = this.cache.get(key);
    if (!item) return null;
    
    if (Date.now() > item.expiry) {
      this.cache.delete(key);
      return null;
    }
    
    return item.value;
  }

  delete(key) {
    this.cache.delete(key);
  }

  clear() {
    this.cache.clear();
  }

  size() {
    return this.cache.size;
  }
}



class MongoMetrics {
  constructor() {
    this.reads = 0;
    this.writes = 0;
    this.docsExamined = 0;
    this.docsReturned = 0;
    this.queryTimes = [];
    this.slowQueries = [];
  }

  trackQuery(operation, duration, docsExamined, docsReturned, query) {
    if (operation.includes('find') || operation.includes('findOne') || operation.includes('count')) {
      this.reads++;
    } else {
      this.writes++;
    }
    
    this.docsExamined += docsExamined || 0;
    this.docsReturned += docsReturned || 0;
    this.queryTimes.push(duration);

    if (duration > 100) { // Slow query threshold: 100ms
      this.slowQueries.push({
        operation,
        duration,
        docsExamined,
        docsReturned,
        query: JSON.stringify(query).substring(0, 200),
        timestamp: new Date().toISOString()
      });
    }
  }

  getStats() {
    const avgQueryTime = this.queryTimes.length > 0 
      ? this.queryTimes.reduce((a, b) => a + b, 0) / this.queryTimes.length 
      : 0;

    return {
      totalReads: this.reads,
      totalWrites: this.writes,
      totalDocsExamined: this.docsExamined,
      totalDocsReturned: this.docsReturned,
      avgQueryTime: avgQueryTime.toFixed(2) + 'ms',
      slowQueriesCount: this.slowQueries.length,
      efficiency: this.docsReturned > 0 
        ? ((this.docsReturned / this.docsExamined) * 100).toFixed(2) + '%'
        : '0%'
    };
  }

  getSlowQueries() {
    return this.slowQueries.slice(-50); // Last 50 slow queries
  }

  reset() {
    this.reads = 0;
    this.writes = 0;
    this.docsExamined = 0;
    this.docsReturned = 0;
    this.queryTimes = [];
    // Keep slow queries for analysis
  }
}


class EnhancedMongoMetrics {
  constructor() {
    // Read/Write counters by collection
    this.readsByCollection = new Map();
    this.writesByCollection = new Map();
    this.totalReads = 0;
    this.totalWrites = 0;
    
    // Document tracking
    this.docsExaminedByCollection = new Map();
    this.docsReturnedByCollection = new Map();
    this.documentsReadByCollection = new Map(); // NEW: Track actual document IDs
    
    // Full collection scan tracking
    this.fullScansDetected = new Map(); // Changed to Map to track query details
    this.indexedQueries = new Map();
    
    // Performance tracking
    this.queryTimes = [];
    this.slowQueries = [];
    
    // Resource tracking
    this.startCpuUsage = process.cpuUsage();
    this.startTime = Date.now();
    this.startMemory = process.memoryUsage();
  }

  trackOperation(collection, operation, docsExamined, docsReturned, duration, query, executionStats, documentIds = []) {
    // Determine if read or write
    const isRead = ['find', 'findOne', 'count', 'aggregate', 'distinct', 'countDocuments', 'estimatedDocumentCount'].some(op => operation.toLowerCase().includes(op.toLowerCase()));
    const isWrite = ['insert', 'update', 'delete', 'replace', 'remove', 'findOneAndUpdate', 'findOneAndReplace', 'findOneAndDelete'].some(op => operation.toLowerCase().includes(op.toLowerCase()));
    
    if (isRead) {
      this.totalReads++;
      this.readsByCollection.set(collection, (this.readsByCollection.get(collection) || 0) + 1);
      
      // Track documents examined and returned
      this.docsExaminedByCollection.set(collection, 
        (this.docsExaminedByCollection.get(collection) || 0) + (docsExamined || 0));
      this.docsReturnedByCollection.set(collection, 
        (this.docsReturnedByCollection.get(collection) || 0) + (docsReturned || 0));
      
      // Track actual document IDs/names read
      if (documentIds && documentIds.length > 0) {
        if (!this.documentsReadByCollection.has(collection)) {
          this.documentsReadByCollection.set(collection, new Set());
        }
        documentIds.forEach(docId => {
          this.documentsReadByCollection.get(collection).add(String(docId));
        });
      }
      
      // Detect full collection scans from executionStats
      if (executionStats && executionStats.executionStages) {
        const hasCollScan = this.checkForCollScan(executionStats.executionStages);
        if (hasCollScan) {
          if (!this.fullScansDetected.has(collection)) {
            this.fullScansDetected.set(collection, []);
          }
          this.fullScansDetected.get(collection).push({
            query: JSON.stringify(query).substring(0, 200),
            timestamp: new Date().toISOString()
          });
        } else {
          if (!this.indexedQueries.has(collection)) {
            this.indexedQueries.set(collection, 0);
          }
          this.indexedQueries.set(collection, this.indexedQueries.get(collection) + 1);
        }
      }
    } else if (isWrite) {
      this.totalWrites++;
      this.writesByCollection.set(collection, (this.writesByCollection.get(collection) || 0) + 1);
    }
    
    this.queryTimes.push(duration);
    
    if (duration > 100) {
      this.slowQueries.push({
        operation,
        collection,
        duration: duration.toFixed(2),
        docsExamined,
        docsReturned,
        query: JSON.stringify(query).substring(0, 200),
        timestamp: new Date().toISOString()
      });
    }
  }

  checkForCollScan(stage) {
    if (!stage) return false;
    
    // Check current stage
    if (stage.stage === 'COLLSCAN') return true;
    
    // Check nested stages recursively
    if (stage.inputStage) {
      return this.checkForCollScan(stage.inputStage);
    }
    if (stage.inputStages && Array.isArray(stage.inputStages)) {
      return stage.inputStages.some(s => this.checkForCollScan(s));
    }
    if (stage.shards && Array.isArray(stage.shards)) {
      return stage.shards.some(s => this.checkForCollScan(s.executionStages));
    }
    
    return false;
  }

  logSummary() {
    const cpuUsage = process.cpuUsage(this.startCpuUsage);
    const memUsage = process.memoryUsage();
    const duration = Date.now() - this.startTime;
    
    console.log('\n========== [samir_mongo_debug] MongoDB Operations Summary ==========');
    
    // Read operations
    console.log('\n--- READ OPERATIONS ---');
    if (this.readsByCollection.size > 0) {
      this.readsByCollection.forEach((count, collection) => {
        const examined = this.docsExaminedByCollection.get(collection) || 0;
        const returned = this.docsReturnedByCollection.get(collection) || 0;
        console.log(`Collection "${collection}": ${count} read operations, ${examined} documents examined, ${returned} documents returned`);
        
        // Show actual documents read
        if (this.documentsReadByCollection.has(collection)) {
          const docIds = Array.from(this.documentsReadByCollection.get(collection));
          if (docIds.length > 0) {
            const displayIds = docIds.slice(0, 10); // Show first 10
            const remaining = docIds.length - displayIds.length;
            console.log(`  Documents read: ${displayIds.join(', ')}${remaining > 0 ? ` (and ${remaining} more)` : ''}`);
          }
        }
      });
    } else {
      console.log('No read operations performed');
    }
    
    // Full collection scan detection
    const collectionsWithScans = Array.from(this.fullScansDetected.keys());
    const collectionsWithIndexes = Array.from(this.indexedQueries.keys());
    
    if (collectionsWithScans.length > 0) {
      console.log(`\nFull collection scan happens to [${collectionsWithScans.join(', ')}]`);
      collectionsWithScans.forEach(col => {
        const scans = this.fullScansDetected.get(col);
        console.log(`  "${col}" had ${scans.length} full collection scan(s)`);
      });
    } else {
      const allReadCollections = Array.from(this.readsByCollection.keys());
      if (allReadCollections.length > 0) {
        console.log(`\nNo full collection scan to [${allReadCollections.join(', ')}]`);
      }
    }
    
    // Write operations
    console.log('\n--- WRITE OPERATIONS ---');
    if (this.writesByCollection.size > 0) {
      this.writesByCollection.forEach((count, collection) => {
        console.log(`Collection "${collection}": ${count} write operations`);
      });
    } else {
      console.log('No write operations performed');
    }
    
    // Totals
    console.log('\n--- TOTALS ---');
    console.log(`Total reads: ${this.totalReads}`);
    console.log(`Total writes: ${this.totalWrites}`);
    
    // Resource usage
    console.log('\n--- MONGODB RESOURCE USAGE ---');
    const cpuPercent = ((cpuUsage.user + cpuUsage.system) / (duration * 1000)) * 100;
    console.log(`MongoDB CPU usage: ${cpuPercent.toFixed(2)}% (user: ${(cpuUsage.user / 1000).toFixed(2)}ms, system: ${(cpuUsage.system / 1000).toFixed(2)}ms)`);
    console.log(`MongoDB RAM usage: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB (Heap), ${(memUsage.rss / 1024 / 1024).toFixed(2)} MB (RSS)`);
    
    // Memory delta
    const memoryDelta = memUsage.heapUsed - this.startMemory.heapUsed;
    console.log(`Memory change since start: ${(memoryDelta / 1024 / 1024).toFixed(2)} MB`);
    
    // System pressure assessment
    const avgQueryTime = this.queryTimes.length > 0 
      ? this.queryTimes.reduce((a, b) => a + b, 0) / this.queryTimes.length 
      : 0;
    
    let pressureStatus = 'Normal';
    if (avgQueryTime > 100 || this.slowQueries.length > 10) {
      pressureStatus = 'High - Many slow queries detected';
    } else if (collectionsWithScans.length > 0) {
      pressureStatus = 'Moderate - Full collection scans detected';
    } else if (avgQueryTime > 50) {
      pressureStatus = 'Moderate - Average query time elevated';
    }
    
    console.log(`\nSystem pressure on MongoDB: ${pressureStatus}`);
    console.log(`Average query time: ${avgQueryTime.toFixed(2)}ms`);
    console.log(`Slow queries (>100ms): ${this.slowQueries.length}`);
    
    if (this.slowQueries.length > 0) {
      console.log('\nSlowest queries:');
      this.slowQueries.slice(0, 5).forEach(q => {
        console.log(`  ${q.operation} on "${q.collection}" - ${q.duration}ms`);
      });
    }
    
    console.log('\n=====================================================================\n');
  }

  reset() {
    this.readsByCollection.clear();
    this.writesByCollection.clear();
    this.docsExaminedByCollection.clear();
    this.docsReturnedByCollection.clear();
    this.documentsReadByCollection.clear();
    this.fullScansDetected.clear();
    this.indexedQueries.clear();
    this.totalReads = 0;
    this.totalWrites = 0;
    this.queryTimes = [];
    this.slowQueries = [];
    this.startCpuUsage = process.cpuUsage();
    this.startTime = Date.now();
    this.startMemory = process.memoryUsage();
  }
}

class ServerMetrics {
  constructor() {
    this.totalRequests = 0;
    this.getRequests = 0;
    this.postRequests = 0;
    this.putRequests = 0;
    this.deleteRequests = 0;
    this.patchRequests = 0;
    this.optionsRequests = 0;
    this.endpointCounts = new Map();
    this.activeRequests = 0;
    this.responseTimes = [];
    this.startCpuUsage = process.cpuUsage();
    this.startTime = Date.now();
    this.startMemory = process.memoryUsage();
    this.requestsByStatusCode = new Map();
  }

  trackRequest(method, endpoint) {
    this.totalRequests++;
    this.activeRequests++;
    
    // Track by method
    switch (method.toUpperCase()) {
      case 'GET': this.getRequests++; break;
      case 'POST': this.postRequests++; break;
      case 'PUT': this.putRequests++; break;
      case 'DELETE': this.deleteRequests++; break;
      case 'PATCH': this.patchRequests++; break;
      case 'OPTIONS': this.optionsRequests++; break;
    }
    
    // Track by endpoint
    const endpointKey = `${method} ${endpoint}`;
    if (!this.endpointCounts.has(endpointKey)) {
      this.endpointCounts.set(endpointKey, { count: 0, totalTime: 0 });
    }
    const endpointData = this.endpointCounts.get(endpointKey);
    endpointData.count++;
  }

  trackResponse(duration, statusCode, endpoint, method) {
    this.activeRequests = Math.max(0, this.activeRequests - 1);
    this.responseTimes.push(duration);
    
    // Track status codes
    this.requestsByStatusCode.set(statusCode, (this.requestsByStatusCode.get(statusCode) || 0) + 1);
    
    // Track endpoint timing
    const endpointKey = `${method} ${endpoint}`;
    if (this.endpointCounts.has(endpointKey)) {
      this.endpointCounts.get(endpointKey).totalTime += duration;
    }
  }

  logSummary() {
    const cpuUsage = process.cpuUsage(this.startCpuUsage);
    const memUsage = process.memoryUsage();
    const duration = Date.now() - this.startTime;
    
    console.log('\n========== [samir_server_debug] Node.js Server Summary ==========');
    
    // Request metrics
    console.log('\n--- REQUEST METRICS ---');
    console.log(`Total requests: ${this.totalRequests}`);
    console.log(`GET requests: ${this.getRequests}`);
    console.log(`POST requests: ${this.postRequests}`);
    if (this.putRequests > 0) console.log(`PUT requests: ${this.putRequests}`);
    if (this.deleteRequests > 0) console.log(`DELETE requests: ${this.deleteRequests}`);
    if (this.patchRequests > 0) console.log(`PATCH requests: ${this.patchRequests}`);
    if (this.optionsRequests > 0) console.log(`OPTIONS requests: ${this.optionsRequests}`);
    console.log(`Active requests: ${this.activeRequests}`);
    
    // Status codes
    if (this.requestsByStatusCode.size > 0) {
      console.log('\n--- RESPONSE STATUS CODES ---');
      const sortedStatusCodes = Array.from(this.requestsByStatusCode.entries())
        .sort((a, b) => b[1] - a[1]);
      sortedStatusCodes.forEach(([code, count]) => {
        console.log(`${code}: ${count} requests`);
      });
    }
    
    // Endpoints
    console.log('\n--- ENDPOINT BREAKDOWN ---');
    const sortedEndpoints = Array.from(this.endpointCounts.entries())
      .sort((a, b) => b[1].count - a[1].count)
      .slice(0, 20); // Top 20 endpoints
    
    sortedEndpoints.forEach(([endpoint, data]) => {
      const avgTime = data.count > 0 ? (data.totalTime / data.count).toFixed(2) : '0.00';
      console.log(`${endpoint}: ${data.count} requests (avg ${avgTime}ms)`);
    });
    
    // Server resource usage
    console.log('\n--- SERVER RESOURCE USAGE ---');
    const cpuPercent = ((cpuUsage.user + cpuUsage.system) / (duration * 1000)) * 100;
    console.log(`Node.js CPU usage: ${cpuPercent.toFixed(2)}% (user: ${(cpuUsage.user / 1000).toFixed(2)}ms, system: ${(cpuUsage.system / 1000).toFixed(2)}ms)`);
    console.log(`Node.js RAM usage: ${(memUsage.heapUsed / 1024 / 1024).toFixed(2)} MB (Heap Used)`);
    console.log(`Total RAM allocated: ${(memUsage.heapTotal / 1024 / 1024).toFixed(2)} MB (Heap Total)`);
    console.log(`RSS (Resident Set Size): ${(memUsage.rss / 1024 / 1024).toFixed(2)} MB`);
    console.log(`External memory: ${(memUsage.external / 1024 / 1024).toFixed(2)} MB`);
    
    // Memory delta
    const memoryDelta = memUsage.heapUsed - this.startMemory.heapUsed;
    console.log(`Memory change since start: ${(memoryDelta / 1024 / 1024).toFixed(2)} MB`);
    
    // Server load assessment
    const avgResponseTime = this.responseTimes.length > 0
      ? this.responseTimes.reduce((a, b) => a + b, 0) / this.responseTimes.length
      : 0;
    
    const maxResponseTime = this.responseTimes.length > 0
      ? Math.max(...this.responseTimes)
      : 0;
    
    const minResponseTime = this.responseTimes.length > 0
      ? Math.min(...this.responseTimes)
      : 0;
    
    let loadStatus = 'Normal';
    if (avgResponseTime > 1000 || cpuPercent > 80) {
      loadStatus = 'High - Server is under heavy load';
    } else if (avgResponseTime > 500 || cpuPercent > 60) {
      loadStatus = 'Moderate - Server load is elevated';
    }
    
    console.log(`\nServer load and pressure: ${loadStatus}`);
    console.log(`Average response time: ${avgResponseTime.toFixed(2)}ms`);
    console.log(`Min response time: ${minResponseTime.toFixed(2)}ms`);
    console.log(`Max response time: ${maxResponseTime.toFixed(2)}ms`);
    console.log(`Server uptime: ${(duration / 1000).toFixed(2)} seconds`);
    console.log(`Requests per second: ${(this.totalRequests / (duration / 1000)).toFixed(2)}`);
    
    console.log('\n================================================================\n');
  }

  reset() {
    this.totalRequests = 0;
    this.getRequests = 0;
    this.postRequests = 0;
    this.putRequests = 0;
    this.deleteRequests = 0;
    this.patchRequests = 0;
    this.optionsRequests = 0;
    this.endpointCounts.clear();
    this.activeRequests = 0;
    this.responseTimes = [];
    this.requestsByStatusCode.clear();
    this.startCpuUsage = process.cpuUsage();
    this.startTime = Date.now();
    this.startMemory = process.memoryUsage();
  }
}

// Initialize server metrics
const serverMetrics = new ServerMetrics();

// Replace the old mongoMetrics initialization
const enhancedMongoMetrics = new EnhancedMongoMetrics();


const likeCountCache = new SimpleCache(30000); // 30 seconds
const commentCountCache = new SimpleCache(30000);
const userSlotsCache = new SimpleCache(60000); // 1 minute

const mongoMetrics = new MongoMetrics();





let totalRequests = 0;
let activeRequests = 0;

let client = null;
let db = null;
let server = null;
const sseClients = new Set();
let isShuttingDown = false;

// Event-driven sync tracking
const pendingSync = new Map();
let syncTimeoutId = null;
const SYNC_DEBOUNCE_MS = 2000;

// Logging utility (safe wrapper)
const log = (level, msg, ...args) => {
  const timestamp = new Date().toISOString();
  const fn = console[level] || console.log;
  try {
    fn(`[${timestamp}] ${level.toUpperCase()}: ${msg}`, ...args);
  } catch {
    console.log(`[${timestamp}] ${level.toUpperCase()}: ${msg}`, ...args);
  }
  // Keep debug messages quiet in non-development
  if (level === 'debug' && NODE_ENV !== 'development') return;
};

// Validation & sanitization
const validate = {
  userId: (id) => {
    return id && typeof id === 'string' && id.trim().length > 0 && id.trim().length <= 100;
  },
  sanitize: (input) => typeof input === 'string' ? input.trim() : input
};



const requestStats = {
  byEndpoint: {},
  byMethod: {},
  responseTimeSum: 0,
  responseTimeCount: 0
};







setInterval(() => {
  const before = likeCountCache.size() + commentCountCache.size() + userSlotsCache.size();
  
  // Trigger cleanup by attempting to get non-existent key
  likeCountCache.get('__cleanup__');
  commentCountCache.get('__cleanup__');
  userSlotsCache.get('__cleanup__');
  
  const after = likeCountCache.size() + commentCountCache.size() + userSlotsCache.size();
  
  if (before !== after) {
    log('debug', `[CACHE-CLEANUP] Removed ${before - after} expired entries`);
  }
}, 60000);


// Middleware setup
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'"]
    }
  }
}));



app.use((req, res, next) => {
  if (isShuttingDown) return res.status(503).json({ error: 'Server is shutting down' });
  
  const start = performance.now();
  const endpoint = req.route ? req.route.path : req.path;
  const method = req.method;

  // Track request start
  serverMetrics.trackRequest(method, endpoint);

  // Store original end function
  const originalEnd = res.end;
  
  // Override res.end to capture completion
  res.end = function(...args) {
    // Restore original end
    res.end = originalEnd;
    
    // Calculate duration
    const duration = performance.now() - start;
    
    // Track response
    serverMetrics.trackResponse(duration, res.statusCode, endpoint, method);
    
    // Log to console
    log('debug', `[samir_server_debug] ${method} ${endpoint} - ${res.statusCode} - ${duration.toFixed(2)}ms`);
    
    // Call original end
    return res.end.apply(res, args);
  };

  next();
});


app.use(compression());

app.use(rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 1000,
  message: { error: 'Too many requests, please try again later.' }
}));

const writeLimit = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: { error: 'Too many write requests, please try again later.' }
});

app.use(express.json({ limit: '10mb' }));
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));


// Change tracker for event-driven sync
function trackChange(postId, metrics, isReel) {
  pendingSync.set(postId, {
    metrics,
    isReel,
    lastUpdate: Date.now()
  });

  // Reset debounce timer
  if (syncTimeoutId) {
    clearTimeout(syncTimeoutId);
  }

  syncTimeoutId = setTimeout(() => {
    executeBatchSync();
  }, SYNC_DEBOUNCE_MS);

  log('debug', `[TRACK] ${postId} marked, pending: ${pendingSync.size}`);
}


function logPeriodicSummaries() {
  setInterval(() => {
    console.log('\n\n╔══════════════════════════════════════════════════════════════════════╗');
    console.log('║          PERIODIC SYSTEM METRICS SUMMARY                             ║');
    console.log('╚══════════════════════════════════════════════════════════════════════╝\n');
    
    // Log server metrics
    serverMetrics.logSummary();
    
    // Log MongoDB metrics
    enhancedMongoMetrics.logSummary();
    
    // Optional: Reset metrics after logging (comment out if you want cumulative stats)
    // serverMetrics.reset();
    // enhancedMongoMetrics.reset();
    
  }, 10 * 1000); // Every 5 minutes
}




async function trackedFind(collection, query, options = {}) {
  const start = performance.now();
  
  try {
    // Execute query with explain to get execution stats
    const explainResult = await db.collection(collection)
      .find(query, options)
      .explain('executionStats');
    
    // Now execute the actual query
    const cursor = db.collection(collection).find(query, options);
    const results = await cursor.toArray();
    
    const duration = performance.now() - start;
    const executionStats = explainResult.executionStats;
    
    // Extract document IDs from results
    const documentIds = results.map(doc => doc._id || doc.postId || doc.userId || doc.id || 'unknown').filter(Boolean);
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'find',
      executionStats.totalDocsExamined || 0,
      executionStats.nReturned || results.length,
      duration,
      query,
      executionStats,
      documentIds
    );
    
    log('debug', `[samir_mongo_debug] ${collection}.find: examined=${executionStats.totalDocsExamined || 0}, returned=${results.length}, time=${duration.toFixed(2)}ms, stage=${executionStats.executionStages?.stage || 'unknown'}`);
    
    return results;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.find failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedFindOne(collection, query, options = {}) {
  const start = performance.now();
  
  try {
    const result = await db.collection(collection).findOne(query, options);
    const duration = performance.now() - start;
    
    // Extract document ID
    const documentId = result ? (result._id || result.postId || result.userId || result.id || 'unknown') : null;
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'findOne',
      result ? 1 : 0,
      result ? 1 : 0,
      duration,
      query,
      null,
      documentId ? [documentId] : []
    );

    log('debug', `[samir_mongo_debug] ${collection}.findOne: time=${duration.toFixed(2)}ms, found=${!!result}${result ? `, document=${documentId}` : ''}`);

    return result;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.findOne failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedUpdateOne(collection, filter, update, options = {}) {
  const start = performance.now();
  
  try {
    const result = await db.collection(collection).updateOne(filter, update, options);
    const duration = performance.now() - start;
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'updateOne',
      result.matchedCount,
      result.modifiedCount,
      duration,
      filter,
      null
    );

    log('debug', `[samir_mongo_debug] ${collection}.updateOne: matched=${result.matchedCount}, modified=${result.modifiedCount}, time=${duration.toFixed(2)}ms`);

    return result;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.updateOne failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedInsertOne(collection, document) {
  const start = performance.now();
  
  try {
    const result = await db.collection(collection).insertOne(document);
    const duration = performance.now() - start;
    
    const documentId = result.insertedId || document._id || 'unknown';
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'insertOne',
      0,
      1,
      duration,
      {},
      null,
      [documentId]
    );

    log('debug', `[samir_mongo_debug] ${collection}.insertOne: document=${documentId}, time=${duration.toFixed(2)}ms`);

    return result;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.insertOne failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedDeleteOne(collection, filter) {
  const start = performance.now();
  
  try {
    const result = await db.collection(collection).deleteOne(filter);
    const duration = performance.now() - start;
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'deleteOne',
      result.deletedCount,
      result.deletedCount,
      duration,
      filter,
      null
    );

    log('debug', `[samir_mongo_debug] ${collection}.deleteOne: deleted=${result.deletedCount}, time=${duration.toFixed(2)}ms`);

    return result;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.deleteOne failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedCountDocuments(collection, query) {
  const start = performance.now();
  
  try {
    const count = Object.keys(query).length === 0
      ? await db.collection(collection).estimatedDocumentCount()
      : await db.collection(collection).countDocuments(query);
      
    const duration = performance.now() - start;
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'countDocuments',
      count,
      1,
      duration,
      query,
      null
    );

    log('debug', `[samir_mongo_debug] ${collection}.countDocuments: result=${count}, time=${duration.toFixed(2)}ms`);

    return count;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.countDocuments failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedUpdateOneWithSync(collection, filter, update, options = {}) {
  const start = performance.now();
  
  try {
    const result = await db.collection(collection).findOneAndUpdate(
      filter,
      update,
      {
        ...options,
        returnDocument: 'after',
        includeResultMetadata: true
      }
    );
    
    const duration = performance.now() - start;
    
    const documentId = result.value ? (result.value._id || result.value.postId || result.value.userId || 'unknown') : null;
    
    enhancedMongoMetrics.trackOperation(
      collection,
      'findOneAndUpdate',
      result.lastErrorObject?.n || 0,
      result.value ? 1 : 0,
      duration,
      filter,
      null,
      documentId ? [documentId] : []
    );

    log('debug', `[samir_mongo_debug] ${collection}.findOneAndUpdate: time=${duration.toFixed(2)}ms${documentId ? `, document=${documentId}` : ''}`);

    // Auto-detect changes and track for sync
    if (result.value && collection === 'user_slots') {
      detectAndTrackChanges(result.value, filter);
    }

    return result;
  } catch (error) {
    const duration = performance.now() - start;
    log('error', `[samir_mongo_debug] ${collection}.findOneAndUpdate failed: ${error.message}, time=${duration.toFixed(2)}ms`);
    throw error;
  }
}

async function trackedFindOne(collection, query, options = {}) {
  const start = performance.now();
  const result = await db.collection(collection).findOne(query, options);
  const duration = performance.now() - start;
  
  enhancedMongoMetrics.trackOperation(
    collection,
    'findOne',
    result ? 1 : 0,
    result ? 1 : 0,
    duration,
    query,
    null
  );

  log('debug', `[samir_mongo_debug] ${collection}.findOne: time=${duration.toFixed(2)}ms, found=${!!result}`);

  return result;
}

async function trackedUpdateOne(collection, filter, update, options = {}) {
  const start = performance.now();
  const result = await db.collection(collection).updateOne(filter, update, options);
  const duration = performance.now() - start;
  
  enhancedMongoMetrics.trackOperation(
    collection,
    'updateOne',
    result.matchedCount,
    result.modifiedCount,
    duration,
    filter,
    null
  );

  log('debug', `[samir_mongo_debug] ${collection}.updateOne: matched=${result.matchedCount}, modified=${result.modifiedCount}, time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedInsertOne(collection, document) {
  const start = performance.now();
  const result = await db.collection(collection).insertOne(document);
  const duration = performance.now() - start;
  
  enhancedMongoMetrics.trackOperation(
    collection,
    'insertOne',
    0,
    1,
    duration,
    {},
    null
  );

  log('debug', `[samir_mongo_debug] ${collection}.insertOne: time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedDeleteOne(collection, filter) {
  const start = performance.now();
  const result = await db.collection(collection).deleteOne(filter);
  const duration = performance.now() - start;
  
  enhancedMongoMetrics.trackOperation(
    collection,
    'deleteOne',
    result.deletedCount,
    result.deletedCount,
    duration,
    filter,
    null
  );

  log('debug', `[samir_mongo_debug] ${collection}.deleteOne: deleted=${result.deletedCount}, time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedCountDocuments(collection, query) {
  const start = performance.now();
  
  const count = Object.keys(query).length === 0
    ? await db.collection(collection).estimatedDocumentCount()
    : await db.collection(collection).countDocuments(query);
    
  const duration = performance.now() - start;
  
  enhancedMongoMetrics.trackOperation(
    collection,
    'count',
    count,
    1,
    duration,
    query,
    null
  );

  log('debug', `[samir_mongo_debug] ${collection}.count: result=${count}, time=${duration.toFixed(2)}ms`);

  return count;
}

async function trackedFindOne(collection, query, options = {}) {
  const start = performance.now();
  const result = await db.collection(collection).findOne(query, options);
  const duration = performance.now() - start;
  
  // Estimate: findOne typically examines 1 doc if indexed, more if not
  mongoMetrics.trackQuery(
    `findOne:${collection}`,
    duration,
    result ? 1 : 0,
    result ? 1 : 0,
    query
  );

  log('debug', `[MONGO-READ] ${collection}.findOne: time=${duration.toFixed(2)}ms, found=${!!result}`);

  return result;
}

async function trackedUpdateOne(collection, filter, update, options = {}) {
  const start = performance.now();
  const result = await db.collection(collection).updateOne(filter, update, options);
  const duration = performance.now() - start;
  
  mongoMetrics.trackQuery(
    `updateOne:${collection}`,
    duration,
    result.matchedCount,
    result.modifiedCount,
    filter
  );

  log('debug', `[MONGO-WRITE] ${collection}.updateOne: matched=${result.matchedCount}, modified=${result.modifiedCount}, time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedInsertOne(collection, document) {
  const start = performance.now();
  const result = await db.collection(collection).insertOne(document);
  const duration = performance.now() - start;
  
  mongoMetrics.trackQuery(
    `insertOne:${collection}`,
    duration,
    0,
    1,
    {}
  );

  log('debug', `[MONGO-WRITE] ${collection}.insertOne: time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedDeleteOne(collection, filter) {
  const start = performance.now();
  const result = await db.collection(collection).deleteOne(filter);
  const duration = performance.now() - start;
  
  mongoMetrics.trackQuery(
    `deleteOne:${collection}`,
    duration,
    result.deletedCount,
    result.deletedCount,
    filter
  );

  log('debug', `[MONGO-WRITE] ${collection}.deleteOne: deleted=${result.deletedCount}, time=${duration.toFixed(2)}ms`);

  return result;
}

async function trackedCountDocuments(collection, query) {
  const start = performance.now();
  
  // Use estimatedDocumentCount for empty queries, much faster
  const count = Object.keys(query).length === 0
    ? await db.collection(collection).estimatedDocumentCount()
    : await db.collection(collection).countDocuments(query);
    
  const duration = performance.now() - start;
  
  // countDocuments examines all matching documents
  mongoMetrics.trackQuery(
    `count:${collection}`,
    duration,
    count, // Examines all matching docs
    1, // Returns 1 number
    query
  );

  log('debug', `[MONGO-READ] ${collection}.count: result=${count}, time=${duration.toFixed(2)}ms`);

  return count;
}





async function executeBatchSync() {
  if (pendingSync.size === 0) return;

  const batch = Array.from(pendingSync.entries());
  pendingSync.clear();

  log('info', `[BATCH-SYNC] Syncing ${batch.length} posts to PORT 2000`);

  try {
    const metricsMap = {};

    batch.forEach(([postId, data]) => {
      metricsMap[postId] = {
        likeCount: data.metrics.likeCount || 0,
        commentCount: data.metrics.commentCount || 0,
        viewCount: data.metrics.viewCount || 0,
        retention: data.metrics.retention || 0,
        isReel: data.isReel
      };
    });

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 15000);

    const response = await fetch(`${PORT_2000_URL}/api/sync/batch-metrics-from-4000`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        metrics: metricsMap,
        timestamp: new Date().toISOString()
      }),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const result = await response.json();
    log('info', `[BATCH-SYNC-OK] ${result.updated}/${batch.length} synced to PORT 2000`);

  } catch (error) {
    log('error', `[BATCH-SYNC-FAIL] ${error.message}`);
    // Re-add failed items for retry
    batch.forEach(([postId, data]) => {
      pendingSync.set(postId, data);
    });
    // Retry after 5 seconds
    setTimeout(executeBatchSync, 5000);
  }
}



async function setupCriticalIndexes() {
  try {
    const indexDefinitions = [
      // ===== USER_SLOTS COLLECTION =====
      {
        collection: 'user_slots',
        indexes: [
          // Existing unique index
          { 
            index: { userId: 1, slotIndex: 1 }, 
            options: { unique: true, background: true } 
          },
          
          // NEW: For fast user content lookups
          { 
            index: { userId: 1 }, 
            options: { background: true } 
          },
          
          // NEW: For finding posts by postId in postList array
          { 
            index: { 'postList.postId': 1 }, 
            options: { background: true } 
          },
          
          // NEW: For finding reels by postId in reelsList array
          { 
            index: { 'reelsList.postId': 1 }, 
            options: { background: true } 
          },
          
          // NEW: Compound index for efficient array element updates
          { 
            index: { userId: 1, 'postList.postId': 1 }, 
            options: { background: true, sparse: true }
          },
          { 
            index: { userId: 1, 'reelsList.postId': 1 }, 
            options: { background: true, sparse: true }
          },
          
          // NEW: For sorting by updatedAt
          { 
            index: { userId: 1, updatedAt: -1 }, 
            options: { background: true } 
          }
        ]
      },

      // ===== POST_LIKES COLLECTION =====
      {
        collection: 'post_likes',
        indexes: [
          { 
            index: { postId: 1, userId: 1 }, 
            options: { unique: true, background: true } 
          },
          { 
            index: { postId: 1 }, 
            options: { background: true } 
          },
          { 
            index: { userId: 1 }, 
            options: { background: true } 
          },
          
          // NEW: Covered query for like count
          { 
            index: { postId: 1, _id: 1 }, 
            options: { background: true }
          }
        ]
      },

      // ===== POST_RETENTION COLLECTION =====
      {
        collection: 'post_retention',
        indexes: [
          { 
            index: { postId: 1, userId: 1 }, 
            options: { unique: true, background: true } 
          },
          { 
            index: { postId: 1 }, 
            options: { background: true } 
          },
          { 
            index: { userId: 1 }, 
            options: { background: true } 
          }
        ]
      },

      // ===== COMMENTS COLLECTION =====
      {
        collection: 'comments',
        indexes: [
          { 
            index: { postId: 1, createdAt: -1 }, 
            options: { background: true } 
          },
          { 
            index: { parentId: 1, createdAt: 1 }, 
            options: { background: true } 
          },
          { 
            index: { userId: 1, createdAt: -1 }, 
            options: { background: true } 
          },
          { 
            index: { postId: 1, parentId: 1 }, 
            options: { background: true } 
          }
        ]
      },

      // ===== CONTRIBUTED_VIEWS_FOLLOWING =====
      {
        collection: 'contributed_views_following',
        indexes: [
          { 
            index: { userId: 1 }, 
            options: { background: true } 
          },
          { 
            index: { documentName: 1 }, 
            options: { background: true } 
          },
          { 
            index: { userId: 1, documentName: 1 }, 
            options: { background: true } 
          },
          
          // NEW: For array lookups
          { 
            index: { PostList: 1 }, 
            options: { background: true, sparse: true } 
          },
          { 
            index: { reelsList: 1 }, 
            options: { background: true, sparse: true } 
          }
        ]
      }
    ];

    for (const { collection, indexes } of indexDefinitions) {
      for (const { index, options } of indexes) {
        try {
          await db.collection(collection).createIndex(index, options);
          log('info', `✓ Created index on ${collection}:`, JSON.stringify(index));
        } catch (error) {
          // Ignore duplicate index errors (code 85 or 86)
          if (error.code !== 85 && error.code !== 86) {
            log('warn', `Index creation warning for ${collection}:`, error.message);
          }
        }
      }
    }

    log('info', '✅ All critical indexes ensured successfully');
  } catch (error) {
    log('error', 'Critical index setup failed:', error.message);
    throw error; // Fail startup if critical indexes can't be created
  }
}




async function initMongo() {
  try {
    client = new MongoClient(MONGODB_URI, {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
      family: 4,
      retryWrites: true,
      w: 'majority',
      monitorCommands: true // ✅ Enable command monitoring
    });

    await client.connect();
    db = client.db(DB_NAME);
    
    // ✅ Add command monitoring listeners
    client.on('commandStarted', (event) => {
      // Store start time for duration calculation
      if (!client._commandStarts) client._commandStarts = new Map();
      client._commandStarts.set(event.requestId, performance.now());
    });
    
    client.on('commandSucceeded', async (event) => {
      const startTime = client._commandStarts?.get(event.requestId);
      const duration = startTime ? performance.now() - startTime : 0;
      if (client._commandStarts) client._commandStarts.delete(event.requestId);
      
      const commandName = event.commandName;
      const collection = event.reply?.cursor?.ns?.split('.')[1] || 
                        event.command?.insert || 
                        event.command?.update || 
                        event.command?.delete || 
                        event.command?.find || 
                        'unknown';
      
      // Get execution stats from reply
      const docsExamined = event.reply?.executionStats?.totalDocsExamined || 0;
      const docsReturned = event.reply?.executionStats?.nReturned || 
                          event.reply?.n || 
                          event.reply?.cursor?.firstBatch?.length || 0;
      
      enhancedMongoMetrics.trackOperation(
        collection,
        commandName,
        docsExamined,
        docsReturned,
        duration,
        event.command,
        event.reply?.executionStats
      );
    });
    
    client.on('commandFailed', (event) => {
      const startTime = client._commandStarts?.get(event.requestId);
      const duration = startTime ? performance.now() - startTime : 0;
      if (client._commandStarts) client._commandStarts.delete(event.requestId);
      
      log('warn', `[samir_mongo_debug] Command ${event.commandName} failed: ${event.failure?.message}`);
    });
    
    await db.admin().ping();
    log('info', 'MongoDB connection established successfully');

    await setupCriticalIndexes();
    await createFollowingFeedIndexes();

  } catch (error) {
    log('error', 'MongoDB initialization failed:', error && error.message ? error.message : error);
    throw error;
  }
}

// ✅ ADD THIS NEW FUNCTION (after setupCriticalIndexes function)
async function createFollowingFeedIndexes() {
  try {
    log('info', '[FOLLOWING-INDEXES] Creating indexes for following feed...');
    
    const followingIndexes = [
      // ===== USER_SLOTS COLLECTION (Following Content Fetch) =====
      {
        collection: 'user_slots',
        indexes: [
          // Fast user lookup with recency sort
          { 
            index: { userId: 1, updatedAt: -1 }, 
            options: { 
              name: 'userId_updatedAt_following',
              background: true 
            }
          },
          
          // Fast postId lookup in arrays
          { 
            index: { userId: 1, 'postList.postId': 1 }, 
            options: { 
              name: 'userId_postList_postId_following',
              background: true,
              sparse: true 
            }
          },
          
          { 
            index: { userId: 1, 'reelsList.postId': 1 }, 
            options: { 
              name: 'userId_reelsList_postId_following',
              background: true,
              sparse: true 
            }
          },
          
          // Retention-based sorting for following content
          { 
            index: { userId: 1, 'postList.retention': -1 }, 
            options: { 
              name: 'userId_postList_retention_following',
              background: true,
              sparse: true 
            }
          },
          
          { 
            index: { userId: 1, 'reelsList.retention': -1 }, 
            options: { 
              name: 'userId_reelsList_retention_following',
              background: true,
              sparse: true 
            }
          }
        ]
      },

      // ===== CONTRIBUTED_VIEWS_FOLLOWING (View Tracking) =====
      {
        collection: 'contributed_views_following',
        indexes: [
          // Fast user lookup
          { 
            index: { userId: 1 }, 
            options: { 
              name: 'userId_following_views',
              background: true 
            }
          },
          
          // Document-specific lookup
          { 
            index: { userId: 1, documentName: 1 }, 
            options: { 
              name: 'userId_documentName_following',
              background: true 
            }
          },
          
          // Fast array membership checks
          { 
            index: { PostList: 1 }, 
            options: { 
              name: 'postList_array_following',
              background: true,
              sparse: true 
            }
          },
          
          { 
            index: { reelsList: 1 }, 
            options: { 
              name: 'reelsList_array_following',
              background: true,
              sparse: true 
            }
          }
        ]
      }
    ];

    let successCount = 0;
    let skipCount = 0;
    let errorCount = 0;

    for (const { collection, indexes } of followingIndexes) {
      for (const { index, options } of indexes) {
        try {
          await db.collection(collection).createIndex(index, options);
          successCount++;
          log('info', `[FOLLOWING-INDEX] ✓ ${collection}.${options.name}`);
        } catch (error) {
          // Silently skip "already exists" errors (code 85 or 86)
          if (error.code === 85 || error.code === 86 || 
              error.message?.includes('already exists')) {
            skipCount++;
          } else {
            errorCount++;
            log('warn', `[FOLLOWING-INDEX] ✗ ${collection}.${options.name}: ${error.message}`);
          }
        }
      }
    }

    log('info', `[FOLLOWING-INDEXES] ✅ Created: ${successCount} | Existed: ${skipCount} | Errors: ${errorCount}`);
    
  } catch (error) {
    log('error', '[FOLLOWING-INDEXES] Failed to create indexes:', error.message);
    // Don't throw - allow server to continue with existing indexes
  }
}



const MAX_SSE_CLIENTS = 10000; // Hard limit
let lastBroadcastTime = 0;
const BROADCAST_THROTTLE_MS = 1000; // Max 1 broadcast per second

function broadcastUpdate(data) {
  if (sseClients.size === 0) return;
  
  // Throttle broadcasts
  const now = Date.now();
  if (now - lastBroadcastTime < BROADCAST_THROTTLE_MS) {
    log('debug', `[BROADCAST-THROTTLED] Skipping, last broadcast ${now - lastBroadcastTime}ms ago`);
    return;
  }
  lastBroadcastTime = now;

  const message = `data: ${JSON.stringify(data)}\n\n`;
  const toRemove = [];

  for (const res of sseClients) {
    try {
      if (res.writableEnded || res.destroyed) {
        toRemove.push(res);
      } else {
        res.write(message);
      }
    } catch (err) {
      log('debug', 'Failed to write to SSE client:', err.message);
      toRemove.push(res);
    }
  }

  toRemove.forEach(client => sseClients.delete(client));
  
  log('debug', `[BROADCAST] Sent to ${sseClients.size} clients, removed ${toRemove.length} dead connections`);
}

// Database utilities
async function getNextUserIndex(userId) {
  const maxDoc = await db.collection('user_slots').findOne(
    { userId },
    { sort: { slotIndex: -1 }, projection: { slotIndex: 1 } }
  );
  return maxDoc ? (maxDoc.slotIndex || 0) + 1 : 1;
}

async function fetchViewerViewedSets(viewerId) {
  const viewedPosts = new Set();
  const viewedReels = new Set();

  if (!viewerId || !validate.userId(viewerId)) {
    return { viewedPosts, viewedReels };
  }

  try {
    const docs = await db.collection('contributed_views_following')
      .find({ userId: viewerId })
      .project({ PostList: 1, reelsList: 1 })
      .toArray();

    docs.forEach(doc => {
      if (Array.isArray(doc.PostList)) {
        doc.PostList.forEach(pid => pid != null && viewedPosts.add(String(pid)));
      }
      if (Array.isArray(doc.reelsList)) {
        doc.reelsList.forEach(rid => rid != null && viewedReels.add(String(rid)));
      }
    });

    log('debug', `Loaded viewed sets for viewer=${viewerId}: posts=${viewedPosts.size}, reels=${viewedReels.size}`);
  } catch (error) {
    log('error', 'Error fetching contributed_views_following for viewerId:', viewerId, error && error.message ? error.message : error);
  }

  return { viewedPosts, viewedReels };
}

function extractIdCandidates(item) {
  if (!item) return [];

  const fields = ['postId', 'id', '_id', 'post_id', 'reelId', 'reel_id'];
  const candidates = fields
    .filter(field => item && item[field] != null)
    .map(field => String(item[field]));

  if (typeof item === 'string' || typeof item === 'number') {
    candidates.push(String(item));
  }

  return [...new Set(candidates)];
}

function isItemViewed(item, viewedPosts, viewedReels, treatAsReel = false) {
  const ids = extractIdCandidates(item);
  if (ids.length === 0) return false;

  return ids.some(id =>
    (!treatAsReel && viewedPosts.has(id)) ||
    (treatAsReel && viewedReels.has(id)) ||
    viewedPosts.has(id) || viewedReels.has(id)
  );
}

async function streamUserSlotsSummary(limit = 100, skip = 0) {
  try {
    const slots = await trackedFind(
      'user_slots',
      {},
      {
        projection: {
          userId: 1,
          slotIndex: 1,
          postCount: 1,
          reelCount: 1,
          updatedAt: 1
        },
        limit,
        skip,
        sort: { updatedAt: -1 }
      }
    );

    return {
      slots,
      count: slots.length,
      hasMore: slots.length === limit
    };
  } catch (error) {
    log('error', 'Error streaming user slots:', error.message);
    return { slots: [], count: 0, hasMore: false };
  }
}

// Async broadcast helper
const asyncBroadcast = () => {
  setImmediate(async () => {
    try {
      const allData = await getAllDataForBroadcast();
      broadcastUpdate(allData);
    } catch (error) {
      log('warn', 'Broadcast failed:', error && error.message ? error.message : error);
    }
  });
};




// Add metrics endpoint
app.get('/api/metrics', (req, res) => {
  const mongoStats = mongoMetrics.getStats();
  const slowQueries = mongoMetrics.getSlowQueries();
  
  const avgResponseTime = requestStats.responseTimeCount > 0
    ? (requestStats.responseTimeSum / requestStats.responseTimeCount).toFixed(2)
    : 0;

  // Calculate top endpoints by request count
  const topEndpoints = Object.entries(requestStats.byEndpoint)
    .map(([endpoint, stats]) => ({
      endpoint,
      count: stats.count,
      avgTime: (stats.totalTime / stats.count).toFixed(2) + 'ms'
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  res.json({
    server: {
      totalRequests,
      activeRequests,
      avgResponseTime: avgResponseTime + 'ms',
      requestsByMethod: requestStats.byMethod,
      topEndpoints,
      uptime: process.uptime() + 's',
      memoryUsage: {
        rss: (process.memoryUsage().rss / 1024 / 1024).toFixed(2) + ' MB',
        heapUsed: (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2) + ' MB',
        heapTotal: (process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2) + ' MB'
      }
    },
    mongodb: mongoStats,
    slowQueries: slowQueries,
    sseClients: sseClients.size,
    timestamp: new Date().toISOString()
  });
});



// ===== BATCH METRICS SYNC ENDPOINT (called by PORT 2000) =====
app.post('/api/sync/batch-metrics', async (req, res) => {
  try {
    const { metrics, timestamp } = req.body;

    if (!metrics || typeof metrics !== 'object') {
      return res.status(400).json({ error: 'metrics object required' });
    }

    log('info', `[BATCH-METRICS-RECEIVED] ${Object.keys(metrics).length} posts from PORT 2000`);

    let updated = 0;
    let failed = 0;

    for (const [postId, data] of Object.entries(metrics)) {
      try {
        const { isReel, likeCount, commentCount, viewCount, retention } = data;
        const arrayField = isReel ? 'reelsList' : 'postList';

        const result = await trackedUpdateOne('user_slots',
          { [`${arrayField}.postId`]: postId },
          {
            $set: {
              [`${arrayField}.$.likeCount`]: likeCount || 0,
              [`${arrayField}.$.commentCount`]: commentCount || 0,
              [`${arrayField}.$.viewCount`]: viewCount || 0,
              [`${arrayField}.$.retention`]: retention || 0,
              [`${arrayField}.$.lastSynced`]: timestamp || new Date().toISOString(),
              'updatedAt': new Date().toISOString()
            }
          }
        );

        if (result.matchedCount > 0) {
          updated++;
        } else {
          failed++;
          log('warn', `[BATCH-SYNC-MISS] ${postId} not found in user_slots`);
        }

      } catch (error) {
        failed++;
        log('error', `[BATCH-SYNC-ERROR] ${postId}: ${error.message}`);
      }
    }

    log('info', `[BATCH-METRICS-COMPLETE] Updated: ${updated}, Failed: ${failed}`);

    return res.json({
      success: true,
      updated,
      failed,
      total: Object.keys(metrics).length
    });

  } catch (error) {
    log('error', '[BATCH-METRICS-ENDPOINT-ERROR]', error.message);
    return res.status(500).json({ error: 'Failed to sync metrics' });
  }
});


// Periodic metrics logging (every 5 minutes)
setInterval(() => {
  const stats = mongoMetrics.getStats();
  log('info', `[METRICS-SNAPSHOT] Reads: ${stats.totalReads}, Writes: ${stats.totalWrites}, Efficiency: ${stats.efficiency}, SlowQueries: ${stats.slowQueriesCount}, Requests: ${totalRequests}, Active: ${activeRequests}`);
}, 5 * 60 * 1000);


app.get('/api/posts/comment-count/:postId', async (req, res) => {
  try {
    const { postId } = req.params;

    if (!postId) {
      return res.status(400).json({ error: 'postId required' });
    }

    log('debug', `[GET-COMMENT-COUNT] Fetching for ${postId}`);

    // Option 1: Get from user_slots (faster with proper index)
    const existingSlot = await trackedFindOne('user_slots',
      {
        $or: [
          { 'postList.postId': postId },
          { 'reelsList.postId': postId }
        ]
      },
      {
        projection: { 
          'postList.$': 1, 
          'reelsList.$': 1 
        }
      }
    );

    if (!existingSlot) {
      // Option 2: Fallback to counting comments collection
      const commentCount = await trackedCountDocuments('comments', { 
        postId,
        parentId: null // Only top-level comments
      });

      log('info', `[GET-COMMENT-COUNT-FALLBACK] ${postId} -> ${commentCount} (from comments collection)`);

      return res.json({
        success: true,
        commentCount,
        postId,
        found: false,
        source: 'comments_collection'
      });
    }

    let commentCount = 0;
    let found = false;

    if (existingSlot.postList && Array.isArray(existingSlot.postList)) {
      const post = existingSlot.postList.find(p => p && p.postId === postId);
      if (post) {
        commentCount = Math.max(0, post.commentCount || 0);
        found = true;
      }
    }

    if (!found && existingSlot.reelsList && Array.isArray(existingSlot.reelsList)) {
      const reel = existingSlot.reelsList.find(r => r && r.postId === postId);
      if (reel) {
        commentCount = Math.max(0, reel.commentCount || 0);
        found = true;
      }
    }

    log('info', `[GET-COMMENT-COUNT] ${postId} -> ${commentCount}`);

    return res.json({
      success: true,
      commentCount,
      postId,
      found,
      source: 'user_slots'
    });

  } catch (error) {
    log('error', '[GET-COMMENT-COUNT-ERROR]', error);
    return res.status(500).json({ error: 'Failed to get comment count' });
  }
});


app.get("/health", (req, res) => {
  res.status(200).send("OK");
});

app.get('/ping', (req, res) => {
  res.status(200).send('pong');
});

app.post('/api/posts/record-retention', writeLimit, async (req, res) => {
  try {
    const { userId, postId, retentionPercent, watchedDuration, totalDuration, isReel } = req.body;

    log('info', `[RETENTION] ${userId} -> ${postId}, ${retentionPercent}%`);

    if (!validate.userId(userId) || !postId || typeof retentionPercent !== 'number') {
      return res.status(400).json({ error: 'userId, postId, and retentionPercent required' });
    }

    if (retentionPercent < 0 || retentionPercent > 100) {
      return res.status(400).json({ error: 'retentionPercent must be between 0 and 100' });
    }

    const cleanUserId = validate.sanitize(userId);
    const arrayField = isReel ? 'reelsList' : 'postList';

    // Check duplicate (single read)
    const existingRetention = await trackedFindOne('post_retention', {
      postId: postId,
      userId: cleanUserId
    });

    if (existingRetention) {
      log('info', `[RETENTION-DUPLICATE] ${cleanUserId} -> ${postId}`);
      
      // Still return current metrics from slot
      const slot = await trackedFindOne('user_slots',
        { [`${arrayField}.postId`]: postId },
        { projection: { [`${arrayField}.$`]: 1 } }
      );

      const viewCount = slot?.[arrayField]?.[0]?.viewCount || 0;

      return res.json({
        success: true,
        message: 'Retention already contributed',
        duplicate: true,
        viewCount
      });
    }

    // Record retention
    await trackedInsertOne('post_retention', {
      postId: postId,
      userId: cleanUserId,
      retentionPercent: Math.round(retentionPercent * 100) / 100,
      watchedDuration,
      totalDuration,
      createdAt: new Date().toISOString()
    });

    // ✅ CRITICAL: Update user_slots with change detection (single operation)
    const result = await trackedUpdateOneWithSync('user_slots',
      { [`${arrayField}.postId`]: postId },
      {
        $set: {
          [`${arrayField}.$.retention`]: Math.round(retentionPercent * 100) / 100,
          [`${arrayField}.$.watchedDuration`]: watchedDuration,
          [`${arrayField}.$.totalDuration`]: totalDuration,
          [`${arrayField}.$.retentionUpdatedAt`]: new Date().toISOString(),
          'updatedAt': new Date().toISOString()
        },
        $inc: { [`${arrayField}.$.viewCount`]: 1 }
      }
    );

    if (!result.value) {
      log('warn', `[RETENTION-NOT-FOUND] ${postId}`);
      return res.status(404).json({ error: 'Post not found' });
    }

    const updatedPost = result.value[arrayField]?.find(p => p.postId === postId);
    const viewCount = updatedPost?.viewCount || 1;

    log('info', `[RETENTION-SUCCESS] ${cleanUserId} -> ${postId}, viewCount: ${viewCount}`);

    // Change already tracked by trackedUpdateOneWithSync
    asyncBroadcast();

    res.json({
      success: true,
      message: 'Retention recorded successfully',
      postId,
      retentionPercent: Math.round(retentionPercent * 100) / 100,
      viewCount
    });

  } catch (error) {
    log('error', '[RETENTION-ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});




app.post('/api/posts/toggle-like', writeLimit, async (req, res) => {
  try {
    const { userId, postId, isReel, currentlyLiked } = req.body;

    log('info', `[LIKE] ${userId} -> ${postId}, currently: ${currentlyLiked}`);

    if (!validate.userId(userId) || !postId) {
      return res.status(400).json({ error: 'userId and postId required' });
    }

    const cleanUserId = validate.sanitize(userId);

    // Check if like exists (single read)
    const existingLike = await trackedFindOne('post_likes', {
      postId: postId,
      userId: cleanUserId
    });

    const hasLiked = !!existingLike;

    // State mismatch correction
    if (currentlyLiked !== hasLiked) {
      const actualCount = await trackedCountDocuments('post_likes', { postId });
      
      log('info', `[LIKE-STATE-CORRECT] ${cleanUserId} -> ${postId}, server=${hasLiked}, client=${currentlyLiked}, count=${actualCount}`);
      
      return res.json({
        success: true,
        message: 'State corrected',
        isLiked: hasLiked,
        likeCount: actualCount,
        action: hasLiked ? 'like' : 'unlike'
      });
    }

    // Perform like/unlike
    let newLikeCount;
    
    if (!currentlyLiked) {
      await trackedInsertOne('post_likes', {
        postId: postId,
        userId: cleanUserId,
        createdAt: new Date().toISOString()
      });
      newLikeCount = await trackedCountDocuments('post_likes', { postId });
      log('info', `[LIKE-ADDED] ${cleanUserId} -> ${postId}`);
    } else {
      await trackedDeleteOne('post_likes', {
        postId: postId,
        userId: cleanUserId
      });
      newLikeCount = await trackedCountDocuments('post_likes', { postId });
      log('info', `[LIKE-REMOVED] ${cleanUserId} -> ${postId}`);
    }

    // ✅ CRITICAL: Update user_slots with change detection (single operation)
    const arrayField = isReel ? 'reelsList' : 'postList';
    
    await trackedUpdateOneWithSync('user_slots',
      { [`${arrayField}.postId`]: postId },
      {
        $set: {
          [`${arrayField}.$.likeCount`]: newLikeCount,
          'updatedAt': new Date().toISOString()
        }
      }
    );

    // Invalidate cache
    likeCountCache.delete(postId);

    // Change already tracked by trackedUpdateOneWithSync
    asyncBroadcast();

    res.json({
      success: true,
      action: currentlyLiked ? 'unlike' : 'like',
      isLiked: !currentlyLiked,
      likeCount: newLikeCount,
      message: `Successfully ${currentlyLiked ? 'unliked' : 'liked'}`
    });

  } catch (error) {
    log('error', '[LIKE-ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});


app.post('/api/posts/increment-view', writeLimit, async (req, res) => {
  try {
    const { userId, postId, isReel } = req.body;

    if (!userId || !postId) {
      return res.status(400).json({ error: 'userId and postId required' });
    }

    log('info', `[VIEW] ${userId} -> ${postId}`);

    const cleanUserId = validate.sanitize(userId);
    const arrayField = isReel ? 'reelsList' : 'postList';

    // Check if retention already recorded (single read)
    const retentionCheck = await trackedFindOne('post_retention', {
      postId: postId,
      userId: cleanUserId
    }, { projection: { _id: 1 } });

    if (retentionCheck) {
      log('info', `[VIEW-SKIP] ${cleanUserId} already has retention for ${postId}`);

      const slot = await trackedFindOne('user_slots',
        { [`${arrayField}.postId`]: postId },
        { projection: { [`${arrayField}.$`]: 1 } }
      );

      const viewCount = slot?.[arrayField]?.[0]?.viewCount || 0;

      return res.json({
        success: true,
        viewCount,
        message: 'View already counted with retention',
        duplicate: true
      });
    }

    // ✅ CRITICAL: Increment view with change detection (single operation)
    const result = await trackedUpdateOneWithSync('user_slots',
      { [`${arrayField}.postId`]: postId },
      {
        $inc: { [`${arrayField}.$.viewCount`]: 1 },
        $set: { 'updatedAt': new Date().toISOString() }
      }
    );

    if (!result.value) {
      log('warn', `[VIEW-NOT-FOUND] ${postId}`);
      return res.status(404).json({ error: 'Post not found' });
    }

    const updatedPost = result.value[arrayField]?.find(p => p.postId === postId);
    const newViewCount = updatedPost?.viewCount || 1;

    log('info', `[VIEW-SUCCESS] ${cleanUserId} -> ${postId}, count: ${newViewCount}`);

    // Change already tracked by trackedUpdateOneWithSync
    asyncBroadcast();

    return res.json({
      success: true,
      viewCount: newViewCount,
      message: 'View counted successfully'
    });

  } catch (error) {
    log('error', '[VIEW-ERROR]', error);
    return res.status(500).json({ error: 'Failed to increment view count' });
  }
});

app.post('/api/posts/check-view-contributions', async (req, res) => {
  try {
    const { userId, postIds } = req.body;

    if (!userId || !Array.isArray(postIds)) {
      return res.status(400).json({ error: 'userId and postIds array required' });
    }

    log('debug', `[BATCH-VIEW-CHECK] ${userId} checking ${postIds.length} posts`);

    const cleanUserId = validate.sanitize(userId);
    const result = {};

    const retentionRecords = await db.collection('post_retention')
      .find({
        postId: { $in: postIds },
        userId: cleanUserId
      })
      .project({ postId: 1 })
      .toArray();

    const hasRetentionSet = new Set(retentionRecords.map(r => r.postId));

    for (const postId of postIds) {
      const hasRetention = hasRetentionSet.has(postId);

      result[postId] = {
        hasRetention,
        hasViewCounted: hasRetention,
        canIncrementView: !hasRetention
      };
    }

    return res.json({
      success: true,
      contributions: result
    });

  } catch (error) {
    log('error', '[BATCH-VIEW-CHECK-ERROR]', error);
    return res.status(500).json({ error: 'Failed to check view contributions' });
  }
});

app.get('/api/posts/check-liked/:userId/:postId', async (req, res) => {
  try {
    const { userId, postId } = req.params;

    if (!userId || !postId) {
      return res.status(400).json({ error: 'userId and postId required' });
    }

    log('debug', `[CHECK-LIKED] ${userId} -> ${postId}`);

    const existingLike = await db.collection('post_likes').findOne({
      postId: postId,
      userId: userId
    }, { projection: { _id: 1 } });

    const isLiked = !!existingLike;

    return res.json({
      success: true,
      isLiked,
      postId,
      userId
    });

  } catch (error) {
    log('error', '[CHECK-LIKED-ERROR]', error);
    return res.status(500).json({ error: 'Failed to check like' });
  }
});

app.get('/api/comments/:postId', async (req, res) => {
  try {
    const { postId } = req.params;
    const { page = 1, limit = 20 } = req.query;

    if (!postId) {
      return res.status(400).json({ error: 'postId required' });
    }

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    const [comments, totalCount] = await Promise.all([
      db.collection('comments')
        .find({ postId, parentId: null })
        .sort({ createdAt: -1 })
        .skip(skip)
        .limit(limitNum)
        .project({
          _id: 1,
          userId: 1,
          username: 1,
          profilePictureUrl: 1,
          text: 1,
          createdAt: 1,
          likeCount: 1,
          replyCount: 1
        })
        .toArray(),

      db.collection('comments').countDocuments({ postId, parentId: null })
    ]);

    const hasMore = (skip + comments.length) < totalCount;

    res.json({
      success: true,
      comments,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: totalCount,
        hasMore
      }
    });

  } catch (error) {
    log('error', 'Get comments error:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/comments/:commentId/replies', async (req, res) => {
  try {
    const { commentId } = req.params;
    const { page = 1, limit = 20 } = req.query;

    if (!commentId) {
      return res.status(400).json({ error: 'commentId required' });
    }

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    const [replies, totalCount] = await Promise.all([
      db.collection('comments')
        .find({ parentId: commentId })
        .sort({ createdAt: 1 })
        .skip(skip)
        .limit(limitNum)
        .project({
          _id: 1,
          userId: 1,
          username: 1,
          profilePictureUrl: 1,
          text: 1,
          createdAt: 1,
          likeCount: 1,
          replyToUsername: 1
        })
        .toArray(),

      db.collection('comments').countDocuments({ parentId: commentId })
    ]);

    const hasMore = (skip + replies.length) < totalCount;

    res.json({
      success: true,
      replies,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: totalCount,
        hasMore
      }
    });

  } catch (error) {
    log('error', 'Get replies error:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/comments', writeLimit, async (req, res) => {
  try {
    const { postId, userId, username, profilePictureUrl, text, parentId, replyToUsername, ownerId, isReel } = req.body;

    if (!postId || !userId || !username || !text) {
      return res.status(400).json({ error: 'postId, userId, username, and text required' });
    }

    if (!validate.userId(userId)) {
      return res.status(400).json({ error: 'Invalid userId' });
    }

    const commentId = require('crypto').randomUUID();
    const now = new Date().toISOString();

    const comment = {
      _id: commentId,
      postId: validate.sanitize(postId),
      userId: validate.sanitize(userId),
      username: validate.sanitize(username),
      profilePictureUrl: profilePictureUrl || null,
      text: validate.sanitize(text),
      createdAt: now,
      likeCount: 0,
      replyCount: 0,
      parentId: parentId ? validate.sanitize(parentId) : null,
      replyToUsername: replyToUsername ? validate.sanitize(replyToUsername) : null
    };

    await trackedInsertOne('comments', comment);

    if (parentId) {
      await trackedUpdateOne('comments',
        { _id: parentId },
        { $inc: { replyCount: 1 } }
      );
    }

    // ✅ CRITICAL: Update user_slots with change detection (single operation)
    const arrayField = isReel ? 'reelsList' : 'postList';
    
    await trackedUpdateOneWithSync('user_slots',
      { [`${arrayField}.postId`]: postId },
      {
        $inc: { [`${arrayField}.$.commentCount`]: 1 },
        $set: { updatedAt: now }
      }
    );

    log('info', `[COMMENT-ADDED] ${postId}`);

    // Change already tracked by trackedUpdateOneWithSync
    asyncBroadcast();

    res.json({
      success: true,
      comment,
      message: parentId ? 'Reply added successfully' : 'Comment added successfully'
    });

  } catch (error) {
    log('error', '[COMMENT-ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.patch('/api/comments/:commentId', writeLimit, async (req, res) => {
  try {
    const { commentId } = req.params;
    const { userId, text } = req.body;

    if (!commentId || !userId || !text) {
      return res.status(400).json({ error: 'commentId, userId, and text required' });
    }

    const result = await db.collection('comments').updateOne(
      { _id: commentId, userId: validate.sanitize(userId) },
      {
        $set: {
          text: validate.sanitize(text),
          updatedAt: new Date().toISOString()
        }
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Comment not found or unauthorized' });
    }

    asyncBroadcast();

    res.json({
      success: true,
      message: 'Comment updated successfully'
    });

  } catch (error) {
    log('error', 'Update comment error:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.delete('/api/comments/:commentId', writeLimit, async (req, res) => {
  try {
    const { commentId } = req.params;
    const { userId, postId, isReel } = req.query;

    if (!commentId || !userId || !postId) {
      return res.status(400).json({ error: 'commentId, userId, and postId required' });
    }

    const comment = await trackedFindOne('comments', {
      _id: commentId,
      userId: validate.sanitize(userId)
    });

    if (!comment) {
      return res.status(404).json({ error: 'Comment not found or unauthorized' });
    }

    // Count replies recursively
    const countReplies = async (parentId) => {
      const replies = await trackedFind('comments',
        { parentId },
        { projection: { _id: 1 } }
      );

      let count = replies.length;
      for (const reply of replies) {
        count += await countReplies(reply._id);
      }
      return count;
    };

    const totalReplies = await countReplies(commentId);
    const totalDeleted = 1 + totalReplies;

    await trackedDeleteOne('comments', {
      $or: [
        { _id: commentId },
        { parentId: commentId }
      ]
    });

    if (comment.parentId) {
      await trackedUpdateOne('comments',
        { _id: comment.parentId },
        { $inc: { replyCount: -1 } }
      );
    }

    // ✅ CRITICAL: Update user_slots with change detection (single operation)
    const arrayField = isReel === 'true' ? 'reelsList' : 'postList';
    
    await trackedUpdateOneWithSync('user_slots',
      { [`${arrayField}.postId`]: postId },
      {
        $inc: { [`${arrayField}.$.commentCount`]: -totalDeleted },
        $set: { updatedAt: new Date().toISOString() }
      }
    );

    log('info', `[COMMENT-DELETED] ${postId}, deleted ${totalDeleted}`);

    // Change already tracked by trackedUpdateOneWithSync
    asyncBroadcast();

    res.json({
      success: true,
      message: 'Comment deleted successfully',
      deletedCount: totalDeleted
    });

  } catch (error) {
    log('error', '[COMMENT-DELETE-ERROR]', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/comments/:commentId/like', writeLimit, async (req, res) => {
  try {
    const { commentId } = req.params;
    const { userId, currentlyLiked } = req.body;

    if (!commentId || !userId) {
      return res.status(400).json({ error: 'commentId and userId required' });
    }

    const increment = currentlyLiked ? -1 : 1;

    const result = await db.collection('comments').updateOne(
      { _id: commentId },
      { $inc: { likeCount: increment } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: 'Comment not found' });
    }

    const updatedComment = await db.collection('comments')
      .findOne({ _id: commentId }, { projection: { likeCount: 1 } });

    res.json({
      success: true,
      isLiked: !currentlyLiked,
      likeCount: Math.max(0, updatedComment.likeCount || 0)
    });

  } catch (error) {
    log('error', 'Toggle comment like error:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/following-views', writeLimit, async (req, res) => {
  try {
    const { userId, documentName, postIds, reelIds, postCount, reelCount } = req.body;

    if (!validate.userId(userId)) {
      return res.status(400).json({ error: 'Valid userId is required' });
    }
    if (!documentName || typeof documentName !== 'string') {
      return res.status(400).json({ error: 'documentName is required' });
    }

    const cleanUserId = validate.sanitize(userId);
    const cleanDocumentName = validate.sanitize(documentName);

    const doc = {
      _id: `${cleanUserId}_${cleanDocumentName}`,
      userId: cleanUserId,
      documentName: cleanDocumentName,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };

    if (Array.isArray(postIds) && postIds.length > 0) {
      doc.PostList = postIds.map(id => String(id)).slice(0, 10000);
      doc.postCount = postCount || doc.PostList.length;
    }
    if (Array.isArray(reelIds) && reelIds.length > 0) {
      doc.reelsList = reelIds.map(id => String(id)).slice(0, 10000);
      doc.reelCount = reelCount || doc.reelsList.length;
    }

    const result = await db.collection('contributed_views_following')
      .replaceOne({ _id: doc._id }, doc, { upsert: true });

    log('info', 'Following views upsert result:', {
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      upsertedId: result.upsertedId || null,
      userId: cleanUserId
    });

    asyncBroadcast();

    res.json({
      success: true,
      message: 'Saved successfully',
      documentId: doc._id,
      postsCount: doc.postCount || 0,
      reelsCount: doc.reelCount || 0
    });
  } catch (error) {
    log('error', 'Following views upsert error:', error && error.message ? error.message : error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/following-views/mark', writeLimit, async (req, res) => {
  try {
    const { viewerId, postId, reelId, documentName } = req.body;

    if (!validate.userId(viewerId)) {
      return res.status(400).json({ error: 'Valid viewerId is required' });
    }
    if (!postId && !reelId) {
      return res.status(400).json({ error: 'Either postId or reelId is required' });
    }

    const cleanViewerId = validate.sanitize(viewerId);
    const docName = documentName ? validate.sanitize(documentName) : `auto_${Date.now()}`;

    const update = {
      $set: { updatedAt: new Date().toISOString() },
      $setOnInsert: {
        createdAt: new Date().toISOString(),
        userId: cleanViewerId,
        documentName: docName
      }
    };

    if (postId) update.$addToSet = { ...(update.$addToSet || {}), PostList: String(postId) };
    if (reelId) update.$addToSet = { ...(update.$addToSet || {}), reelsList: String(reelId) };

    const result = await db.collection('contributed_views_following')
      .updateOne({ _id: `${cleanViewerId}_${docName}` }, update, { upsert: true });

    asyncBroadcast();

    res.json({
      success: true,
      viewerId: cleanViewerId,
      postId: postId || null,
      reelId: reelId || null,
      upserted: result.upsertedId ? true : false
    });
  } catch (error) {
    log('error', 'Mark viewed error:', error && error.message ? error.message : error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/following-views/user/:userId', async (req, res) => {
  try {
    const { userId } = req.params;

    if (!validate.userId(userId)) {
      return res.status(400).json({ error: 'Valid userId is required' });
    }

    const cleanUserId = validate.sanitize(userId);
    const docs = await db.collection('contributed_views_following')
      .find({ userId: cleanUserId })
      .limit(100)
      .toArray();

    res.json({
      success: true,
      userId: cleanUserId,
      documents: docs
    });
  } catch (error) {
    log('error', 'Error getting user docs:', error && error.message ? error.message : error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.delete('/api/following-views/:target', writeLimit, async (req, res) => {
  try {
    const { target } = req.params;
    let result;

    if (target === 'clear') {
      result = await db.collection('contributed_views_following').deleteMany({});
    } else if (target.startsWith('user/')) {
      const userId = target.substring(5);
      if (!validate.userId(userId)) {
        return res.status(400).json({ error: 'Valid userId is required' });
      }
      const cleanUserId = validate.sanitize(userId);
      result = await db.collection('contributed_views_following').deleteMany({ userId: cleanUserId });
    } else {
      return res.status(400).json({ error: 'Invalid target' });
    }

    asyncBroadcast();

    res.json({
      success: true,
      deletedCount: result.deletedCount
    });
  } catch (error) {
    log('error', 'Error in delete operation:', error && error.message ? error.message : error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/posts', writeLimit, async (req, res) => {
  try {
    const post = req.body || {};
    const { userId, isReel } = post;
    if (!validate.userId(userId)) {
      return res.status(400).json({ error: 'Valid userId is required' });
    }
    const cleanUserId = validate.sanitize(userId);
    const slotKey = isReel ? 'reelsList' : 'postList';
    const countField = isReel ? 'reelCount' : 'postCount';
    log('info', `[POST-UPLOAD] userId=${cleanUserId}, isReel=${isReel}, slotCapacity=${SLOT_CAPACITY}`);

    let targetSlot = await db.collection('user_slots').findOne(
      { userId: cleanUserId, [countField]: { $lt: SLOT_CAPACITY } },
      { sort: { slotIndex: 1 } }
    );
    let targetIndex;
    if (!targetSlot) {
      targetIndex = await getNextUserIndex(cleanUserId);
      log('info', `[POST-UPLOAD] Creating new slot ${targetIndex} for user ${cleanUserId}`);
      targetSlot = {
        _id: `${cleanUserId}_${targetIndex}`,
        userId: cleanUserId,
        slotIndex: targetIndex,
        postCount: 0,
        reelCount: 0,
        postList: [],
        reelsList: [],
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      };
    } else {
      targetIndex = targetSlot.slotIndex;
      log('info', `[POST-UPLOAD] Using existing slot ${targetIndex} for user ${cleanUserId}, current ${countField}=${targetSlot[countField]}`);
    }

    const item = {
      ...post,
      userId: cleanUserId,
      savedAt: new Date().toISOString()
    };
    const arr = Array.isArray(targetSlot[slotKey]) ? [...targetSlot[slotKey]] : [];
    arr.push(item);
    const newCount = (targetSlot[countField] || 0) + 1;
    const updateDoc = {
      ...targetSlot,
      [countField]: newCount,
      [slotKey]: arr,
      updatedAt: new Date().toISOString()
    };

    await db.collection('user_slots').replaceOne(
      { _id: `${cleanUserId}_${targetIndex}` },
      updateDoc,
      { upsert: true }
    );

    log('info', `[POST-UPLOAD-SUCCESS] Slot ${targetIndex}, new ${countField}=${newCount}, slotCapacity=${SLOT_CAPACITY}`);

    asyncBroadcast();

    res.json({
      success: true,
      slotIndex: targetIndex,
      totalItems: updateDoc[countField],
      docName: `${cleanUserId}_${targetIndex}`,
      contentType: isReel ? 'reel' : 'post',
      slotCapacity: SLOT_CAPACITY
    });
  } catch (error) {
    log('error', '[POST-UPLOAD-ERROR]', error && error.message ? error.message : error);
    res.status(500).json({ error: 'Internal server error' });
  }
});



app.get('/api/content/user/:uid', async (req, res) => {
  try {
    const { uid } = req.params;
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const contentType = req.query.type || 'all'; // 'posts', 'reels', or 'all'

    if (!uid || typeof uid !== 'string' || uid.trim().length === 0) {
      return res.status(400).json({ success: false, error: 'Invalid UID' });
    }

    const cleanUserId = uid.trim();
    const skip = (page - 1) * limit;

    log('info', `[GET-USER-CONTENT] userId: ${cleanUserId}, page: ${page}, limit: ${limit}, type: ${contentType}`);

    // Fetch only required slots with pagination
    const userSlots = await trackedFind('user_slots',
      { userId: cleanUserId },
      {
        sort: { updatedAt: -1 },
        limit: Math.ceil(limit / 50), // Estimate slots needed (assuming ~50 posts per slot)
        skip: Math.floor(skip / 50)
      }
    );

    if (!userSlots || userSlots.length === 0) {
      log('warn', `[GET-USER-CONTENT] No slots found for userId: ${cleanUserId}`);
      return res.status(200).json({
        success: true,
        posts: [],
        reels: [],
        totalPosts: 0,
        totalReels: 0,
        page,
        hasMore: false
      });
    }

    const posts = [];
    const reels = [];

    userSlots.forEach(slot => {
      if (contentType === 'all' || contentType === 'posts') {
        if (Array.isArray(slot.postList) && slot.postList.length > 0) {
          slot.postList.forEach(post => {
            posts.push({
              postId: post.postId || post.id,
              userId: cleanUserId,
              imageUrl: post.imageUrl || '',
              multiple_posts: post.multiple_posts || false,
              media_count: post.media_count || 0,
              imageUrl1: post.imageUrl1 || post.imageUrl || null,
              imageUrl2: post.imageUrl2 || null,
              imageUrl3: post.imageUrl3 || null,
              imageUrl4: post.imageUrl4 || null,
              imageUrl5: post.imageUrl5 || null,
              imageUrl6: post.imageUrl6 || null,
              imageUrl7: post.imageUrl7 || null,
              imageUrl8: post.imageUrl8 || null,
              imageUrl9: post.imageUrl9 || null,
              imageUrl10: post.imageUrl10 || null,
              imageUrl11: post.imageUrl11 || null,
              imageUrl12: post.imageUrl12 || null,
              imageUrl13: post.imageUrl13 || null,
              imageUrl14: post.imageUrl14 || null,
              imageUrl15: post.imageUrl15 || null,
              imageUrl16: post.imageUrl16 || null,
              imageUrl17: post.imageUrl17 || null,
              imageUrl18: post.imageUrl18 || null,
              imageUrl19: post.imageUrl19 || null,
              imageUrl20: post.imageUrl20 || null,
              caption: post.caption || '',
              description: post.description || '',
              likeCount: post.likeCount || 0,
              commentCount: post.commentCount || 0,
              viewCount: post.viewCount || 0,
              retention: post.retention || 0,
              timestamp: post.timestamp || new Date().toISOString(),
              ratio: post.ratio || '4:5',
              rankingScore: calculateRankingScore({
                retention: post.retention || 0,
                likeCount: post.likeCount || 0,
                commentCount: post.commentCount || 0,
                viewCount: post.viewCount || 0
              })
            });
          });
        }
      }

      if (contentType === 'all' || contentType === 'reels') {
        if (Array.isArray(slot.reelsList) && slot.reelsList.length > 0) {
          slot.reelsList.forEach(reel => {
            let actualPostId = reel.postId || reel.reelId || reel.id;

            if (actualPostId && actualPostId.includes('_reel_')) {
              const reelIndex = actualPostId.indexOf('_reel_');
              actualPostId = actualPostId.substring(0, reelIndex);
            }

            reels.push({
              postId: actualPostId,
              reelId: reel.reelId || actualPostId,
              userId: cleanUserId,
              imageUrl: reel.imageUrl || '',
              videoUrl: reel.videoUrl || '',
              caption: reel.caption || '',
              description: reel.description || '',
              likeCount: reel.likeCount || 0,
              commentCount: reel.commentCount || 0,
              viewCount: reel.viewCount || 0,
              retention: reel.retention || 0,
              timestamp: reel.timestamp || new Date().toISOString(),
              ratio: reel.ratio || '9:16',
              rankingScore: calculateRankingScore({
                retention: reel.retention || 0,
                likeCount: reel.likeCount || 0,
                commentCount: reel.commentCount || 0,
                viewCount: reel.viewCount || 0
              })
            });
          });
        }
      }
    });

    // Sort by ranking score
    posts.sort((a, b) => b.rankingScore - a.rankingScore);
    reels.sort((a, b) => b.rankingScore - a.rankingScore);

    // Apply pagination to results
    const paginatedPosts = posts.slice(0, limit);
    const paginatedReels = reels.slice(0, limit);

    log('info', `[GET-USER-CONTENT] Returning for ${cleanUserId}: ${paginatedPosts.length} posts, ${paginatedReels.length} reels (page ${page})`);

    res.status(200).json({
      success: true,
      posts: paginatedPosts,
      reels: paginatedReels,
      totalPosts: paginatedPosts.length,
      totalReels: paginatedReels.length,
      userId: cleanUserId,
      page,
      limit,
      hasMore: posts.length > limit || reels.length > limit
    });

  } catch (err) {
    log('error', '[GET-USER-CONTENT-ERROR]', err.message, err.stack);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});




function calculateRankingScore(metrics) {
  const RETENTION_WEIGHT = 100;
  const LIKE_WEIGHT = 10;
  const COMMENT_WEIGHT = 5;
  const VIEW_WEIGHT = 1;

  return (
    (metrics.retention || 0) * RETENTION_WEIGHT +
    (metrics.likeCount || 0) * LIKE_WEIGHT +
    (metrics.commentCount || 0) * COMMENT_WEIGHT +
    (metrics.viewCount || 0) * VIEW_WEIGHT
  );
}


app.get('/api/posts/user/:uid', async (req, res) => {
  try {
    const { uid } = req.params;

    if (!uid || typeof uid !== 'string' || uid.trim().length === 0) {
      return res.status(400).json({ success: false, error: 'Invalid UID' });
    }

    const cleanUserId = uid.trim();

    const userSlots = await db.collection('user_slots')
      .find({ userId: cleanUserId })
      .sort({ slotIndex: 1 })
      .toArray();

    if (!userSlots || userSlots.length === 0) {
      return res.status(200).json({
        success: true,
        posts: [],
        totalPosts: 0
      });
    }

    const posts = [];

    userSlots.forEach(slot => {
      if (Array.isArray(slot.postList)) {
        slot.postList.forEach(post => {
          posts.push({
            postId: post.postId || post.id || `${cleanUserId}_post_${Date.now()}_${Math.random()}`,
            userId: cleanUserId,
            imageUrl: post.imageUrl || post.image_url || '',
            caption: post.caption || '',
            description: post.description || '',
            likeCount: post.likeCount || post.like_count || 0,
            commentCount: post.commentCount || post.comment_count || 0,
            timestamp: post.timestamp || post.savedAt || new Date().toISOString()
          });
        });
      }
    });

    posts.sort((a, b) => {
      try {
        return new Date(b.timestamp) - new Date(a.timestamp);
      } catch {
        return 0;
      }
    });

    log('info', `Fetched ${posts.length} posts for user: ${cleanUserId}`);

    res.status(200).json({
      success: true,
      posts,
      totalPosts: posts.length,
      userId: cleanUserId
    });

  } catch (err) {
    log('error', 'Get user posts error:', err && err.message ? err.message : err);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});



const processFollowingContent = async (userIds, viewerId, req, res) => {
  try {
    if (!Array.isArray(userIds) || userIds.length === 0) {
      return res.status(400).json({ error: 'userIds array is required' });
    }

    const cleanUserIds = userIds
      .filter(validate.userId)
      .map(validate.sanitize)
      .slice(0, 50);

    if (cleanUserIds.length === 0) {
      return res.status(400).json({ error: 'No valid userIds provided' });
    }

    const page = parseInt(req.query?.page) || 1;
    const limit = Math.min(parseInt(req.query?.limit) || 20, 100);
    const skip = (page - 1) * limit;

    const cleanViewerId = viewerId ? validate.sanitize(viewerId) : null;
    
    log('info', `[BATCH-FOLLOWING] ${cleanUserIds.length} users, viewer: ${cleanViewerId || 'none'}, page: ${page}`);

    // Fetch viewer's viewed content in parallel
    const viewedSetsPromise = cleanViewerId 
      ? fetchViewerViewedSets(cleanViewerId)
      : Promise.resolve({ viewedPosts: new Set(), viewedReels: new Set() });

    // Fetch user slots with projection to minimize data transfer
    const slotsPromise = trackedFind('user_slots',
      { userId: { $in: cleanUserIds } },
      {
        projection: {
          userId: 1,
          slotIndex: 1,
          postList: 1,
          reelsList: 1,
          updatedAt: 1
        },
        sort: { updatedAt: -1 },
        limit: Math.ceil(limit / 10) * cleanUserIds.length // Estimate slots needed
      }
    );

    const [{ viewedPosts, viewedReels }, slots] = await Promise.all([
      viewedSetsPromise,
      slotsPromise
    ]);

    log('info', `[BATCH-FOLLOWING] Found ${slots.length} user slots`);

    const content = [];
    let filteredOut = 0;

    slots.forEach(slot => {
      const uid = slot.userId;
      const slotIndex = slot.slotIndex;
      const docName = `${uid}_${slotIndex}`;

      if (Array.isArray(slot.postList)) {
        slot.postList.forEach(post => {
          try {
            if (cleanViewerId && isItemViewed(post, viewedPosts, viewedReels, false)) {
              filteredOut++;
              return;
            }

            content.push({
              ...post,
              postId: post.postId || post.id,
              sourceDocument: docName,
              documentSlot: slotIndex,
              followedUserId: uid,
              contentType: 'following',
              isReel: false,
              slotIndex,
              fetchedAt: new Date().toISOString(),
              userId: uid
            });
          } catch (error) {
            log('error', `[POST-PROCESS-ERROR] ${error.message}`);
          }
        });
      }

      if (Array.isArray(slot.reelsList)) {
  slot.reelsList.forEach(reel => {
    try {
      if (cleanViewerId && isItemViewed(reel, viewedPosts, viewedReels, true)) {
        filteredOut++;
        return;
      }

      let actualPostId = reel.postId || reel.reelId || reel.id;

      content.push({
        ...reel,
        postId: actualPostId,
        reelId: reel.reelId || actualPostId,
        sourceDocument: docName,
        documentSlot: slotIndex,
        followedUserId: uid,
        contentType: 'following',
        isReel: true, // ✅ ENSURE THIS IS EXPLICITLY SET
        slotIndex,
        fetchedAt: new Date().toISOString(),
        userId: uid
      });
    } catch (error) {
      log('error', `[REEL-PROCESS-ERROR] ${error.message}`);
    }
  });
}
    });

    // Sort by engagement
    content.sort((a, b) => {
      const engagementA = (a.likeCount || 0) * 2 + (a.commentCount || 0) * 3;
      const engagementB = (b.likeCount || 0) * 2 + (b.commentCount || 0) * 3;
      if (engagementA !== engagementB) return engagementB - engagementA;

      try {
        const dateA = new Date(a.timestamp || a.savedAt || 0);
        const dateB = new Date(b.timestamp || b.savedAt || 0);
        return dateB - dateA;
      } catch {
        return 0;
      }
    });

    // Apply pagination
    const paginatedContent = content.slice(skip, skip + limit);
    const hasMore = content.length > (skip + limit);

    log('info', `[BATCH-FOLLOWING] Returning ${paginatedContent.length} items (page ${page}, total ${content.length})`);

    return res.json({
      success: true,
      viewerId: cleanViewerId,
      filteredOut,
      viewerViewedCounts: {
        posts: viewedPosts.size,
        reels: viewedReels.size
      },
      requestedUsers: cleanUserIds,
      content: paginatedContent,
      totalItems: paginatedContent.length,
      userCount: cleanUserIds.length,
      page,
      limit,
      hasMore,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    log('error', '[BATCH-FOLLOWING-PROCESS-ERROR]', error.message, error.stack);

    return res.status(200).json({
      success: true,
      content: [],
      totalItems: 0,
      page: 1,
      hasMore: false,
      message: 'Error processing content',
      error: error.message
    });
  }
};



app.get('/api/posts/following/:userId', async (req, res) => {
  const { userId } = req.params;
  const { viewerId } = req.query;

  if (!validate.userId(userId)) {
    return res.status(400).json({ error: 'Valid userId is required' });
  }

  await processFollowingContent([userId], viewerId, req, res);
});

app.post('/api/posts/batch-following', async (req, res) => {
  try {
    const { userIds, viewerId } = req.body;

    if (!Array.isArray(userIds) || userIds.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'userIds array is required'
      });
    }

    log('info', `[BATCH-FOLLOWING] Request for ${userIds.length} users, viewer: ${viewerId || 'none'}`);

    await processFollowingContent(userIds, viewerId, req, res);

  } catch (error) {
    log('error', '[BATCH-FOLLOWING-ERROR]', error.message, error.stack);

    return res.status(200).json({
      success: true,
      content: [],
      totalItems: 0,
      message: 'No content available',
      error: error.message
    });
  }
});

app.get('/api/posts/following', async (req, res) => {
  const { userIds, viewerId } = req.query;

  if (!userIds) {
    return res.status(400).json({ error: 'userIds query parameter is required' });
  }

  const followedUserIds = userIds.split(',')
    .map(s => s.trim())
    .filter(Boolean);

  if (followedUserIds.length === 0) {
    return res.json({
      success: true,
      posts: [],
      reels: [],
      metadata: {
        followedUsersCount: 0,
        totalPosts: 0,
        totalReels: 0,
        fetchedAt: new Date().toISOString(),
        filteredOut: 0,
        viewerViewedCounts: { posts: 0, reels: 0 }
      }
    });
  }

  await processFollowingContent(followedUserIds, viewerId, req, res);
});

app.get('/api/posts/all', async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100); // Max 100 per page
    const skip = (page - 1) * limit;

    const data = await streamUserSlotsSummary(limit, skip);
    
    res.json({
      success: true,
      page,
      limit,
      count: data.count,
      hasMore: data.hasMore,
      slots: data.slots,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    log('error', 'Get all posts error:', error.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/posts/stream', (req, res) => {
  // Check client limit
  if (sseClients.size >= MAX_SSE_CLIENTS) {
    return res.status(503).json({ 
      error: 'Maximum SSE connections reached',
      limit: MAX_SSE_CLIENTS 
    });
  }

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Cache-Control, Content-Type'
  });

  sseClients.add(res);
  log('debug', `New SSE client connected. Total: ${sseClients.size}/${MAX_SSE_CLIENTS}`);

  // Send lightweight summary instead of all data
  streamUserSlotsSummary(50, 0)
    .then(data => {
      try {
        if (!res.writableEnded && !res.destroyed) {
          res.write(`data: ${JSON.stringify({
            type: 'summary',
            totalSlots: data.count,
            timestamp: new Date().toISOString()
          })}\n\n`);
        }
      } catch (error) {
        sseClients.delete(res);
      }
    })
    .catch(error => {
      log('error', 'Error sending initial SSE data:', error.message);
    });

  const pingInterval = setInterval(() => {
    try {
      if (res.writableEnded || res.destroyed) {
        clearInterval(pingInterval);
        sseClients.delete(res);
        return;
      }
      res.write(`: ping\n\n`);
    } catch (error) {
      clearInterval(pingInterval);
      sseClients.delete(res);
    }
  }, 30000);

  req.on('close', () => {
    sseClients.delete(res);
    clearInterval(pingInterval);
    log('debug', 'SSE client disconnected. Total clients:', sseClients.size);
  });

  req.on('error', (error) => {
    log('debug', 'SSE client error:', error.message);
    sseClients.delete(res);
    clearInterval(pingInterval);
  });
});



app.get('/api/posts/get-like-count/:postId', async (req, res) => {
  try {
    const { postId } = req.params;

    if (!postId) {
      return res.status(400).json({ error: 'postId required' });
    }

    // Check cache first
    const cached = likeCountCache.get(postId);
    if (cached !== null) {
      log('debug', `[GET-COUNT-CACHED] ${postId} -> ${cached}`);
      return res.json({
        success: true,
        likeCount: cached,
        postId,
        found: true,
        cached: true
      });
    }

    log('debug', `[GET-COUNT] ${postId}`);

    const likeCount = await trackedCountDocuments('post_likes', { postId });

    // Store in cache
    likeCountCache.set(postId, likeCount);

    log('info', `[GET-COUNT] ${postId} -> ${likeCount}`);

    return res.json({
      success: true,
      likeCount,
      postId,
      found: true,
      cached: false
    });

  } catch (error) {
    log('error', '[GET-COUNT-ERROR]', error);
    return res.status(500).json({ error: 'Failed to get count' });
  }
});




app.post('/api/posts/batch-check-liked', async (req, res) => {
  try {
    const { userId, postIds } = req.body;

    if (!userId || !Array.isArray(postIds)) {
      return res.status(400).json({ error: 'userId and postIds array required' });
    }

    // Limit batch size
    const limitedPostIds = postIds.slice(0, 100);
    
    if (postIds.length > 100) {
      log('warn', `[BATCH-CHECK] Request for ${postIds.length} posts, limited to 100`);
    }

    log('debug', `[BATCH-CHECK] ${userId} checking ${limitedPostIds.length} posts`);

    const cleanUserId = validate.sanitize(userId);

    // Use projection to only fetch postId field
    const likes = await trackedFind('post_likes',
      {
        postId: { $in: limitedPostIds },
        userId: cleanUserId
      },
      {
        projection: { postId: 1, _id: 0 }
      }
    );

    const likedPostIds = new Set(likes.map(like => like.postId));

    const result = {};
    limitedPostIds.forEach(postId => {
      result[postId] = {
        isLiked: likedPostIds.has(postId)
      };
    });

    log('info', `[BATCH-CHECK] ${limitedPostIds.length} posts, ${likes.length} liked`);

    return res.json({
      success: true,
      likes: result,
      userId,
      checkedCount: limitedPostIds.length
    });

  } catch (error) {
    log('error', '[BATCH-CHECK-ERROR]', error);
    return res.status(500).json({ error: 'Failed to check likes' });
  }
});



app.use((err, req, res, next) => {
  log('error', 'Unhandled error:', err && err.message ? err.message : err);
  if (res.headersSent) return next(err);
  res.status(500).json({
    error: 'Internal server error',
    ...(NODE_ENV === 'development' && { details: err && err.message ? err.message : String(err) })
  });
});

app.use('*', (req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

const gracefulShutdown = async (signal) => {
  try {
    log('info', `Received ${signal}, initiating graceful shutdown...`);
    isShuttingDown = true;

    for (const clientRes of sseClients) {
      try {
        if (!clientRes.writableEnded) {
          clientRes.end();
        }
      } catch (error) {
        log('debug', 'Error closing SSE client:', error && error.message ? error.message : error);
      }
    }
    sseClients.clear();

    if (server && server.close) {
      await new Promise((resolve) => server.close(resolve));
      log('info', 'HTTP server closed');
    }

    try {
      if (client && typeof client.close === 'function') {
        await client.close();
        log('info', 'MongoDB connection closed');
      }
    } catch (error) {
      log('error', 'Error closing MongoDB connection:', error && error.message ? error.message : error);
    }

    process.exit(0);
  } catch (err) {
    log('error', 'Error during gracefulShutdown:', err && err.message ? err.message : err);
    process.exit(1);
  }
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (error) => {
  log('error', 'Uncaught Exception:', error && error.stack ? error.stack : error);
  gracefulShutdown('uncaughtException').catch(() => process.exit(1));
});
process.on('unhandledRejection', (reason, promise) => {
  log('error', 'Unhandled Rejection at:', promise, 'reason:', reason);
  gracefulShutdown('unhandledRejection').catch(() => process.exit(1));
});



async function syncMetricsToPort2000(postId, metrics, isReel) {
  const PORT_2000_URL = process.env.PORT_2000_URL || 'https://samir-hgr9.onrender.com';
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000); // 5s timeout
    
    const response = await fetch(`${PORT_2000_URL}/api/sync/retention-metrics`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        postId,
        metrics,
        isReel,
        timestamp: new Date().toISOString()
      }),
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (response.ok) {
      log('debug', `[RETENTION-SYNC-OK] ${postId} synced to PORT 2000`);
    } else {
      log('warn', `[RETENTION-SYNC-FAILED] ${postId} - HTTP ${response.status}`);
    }
  } catch (error) {
    // Silent fail - retention is already saved in PORT 4000
    log('debug', `[RETENTION-SYNC-ERROR] ${postId}: ${error.message}`);
  }
}



// ===== REAL-TIME CHANGE DETECTION SYSTEM =====
// Tracks BEFORE and AFTER state with single read
async function trackedUpdateOneWithSync(collection, filter, update, options = {}) {
  const start = performance.now();
  
  // Execute update and get BOTH old and new values in ONE operation
  const result = await db.collection(collection).findOneAndUpdate(
    filter,
    update,
    {
      ...options,
      returnDocument: 'after', // Get new document
      includeResultMetadata: true
    }
  );
  
  const duration = performance.now() - start;
  
  mongoMetrics.trackQuery(
    `findOneAndUpdate:${collection}`,
    duration,
    result.lastErrorObject?.n || 0,
    result.value ? 1 : 0,
    filter
  );

  log('debug', `[MONGO-WRITE] ${collection}.findOneAndUpdate: time=${duration.toFixed(2)}ms`);

  // Auto-detect changes and track for sync
  if (result.value && collection === 'user_slots') {
    detectAndTrackChanges(result.value, filter);
  }

  return result;
}

// Intelligent change detector - extracts metrics from updated document
function detectAndTrackChanges(updatedSlot, filter) {
  if (!updatedSlot) return;

  const { postList = [], reelsList = [] } = updatedSlot;

  // Process posts
  if (Array.isArray(postList) && postList.length > 0) {
    postList.forEach(post => {
      if (post && post.postId) {
        trackChange(post.postId, {
          likeCount: post.likeCount || 0,
          commentCount: post.commentCount || 0,
          viewCount: post.viewCount || 0,
          retention: post.retention || 0
        }, false);
      }
    });
  }

  // Process reels
  if (Array.isArray(reelsList) && reelsList.length > 0) {
    reelsList.forEach(reel => {
      if (reel && reel.postId) {
        trackChange(reel.postId, {
          likeCount: reel.likeCount || 0,
          commentCount: reel.commentCount || 0,
          viewCount: reel.viewCount || 0,
          retention: reel.retention || 0
        }, true);
      }
    });
  }

  log('debug', `[AUTO-DETECT] Tracked ${postList.length} posts + ${reelsList.length} reels from slot ${updatedSlot._id}`);
}



async function startServer() {
  try {
    await initMongo();

    server = app.listen(PORT, HOST, () => {
      log('info', `🚀 Server listening on http://${HOST}:${PORT}/`);
      log('info', `Environment: ${NODE_ENV}`);
      log('info', `Database: ${DB_NAME} at ${MONGODB_URI}`);
      log('info', 'Health check: GET /health');
      log('info', 'Database viewer: GET /database-viewer');
      
      // ✅ Start periodic logging
      logPeriodicSummaries();
      
      // ✅ Log initial state
      console.log('\n[samir_server_debug] Server started successfully');
      console.log('[samir_mongo_debug] MongoDB monitoring enabled');
    });

    server.on('error', (error) => {
      log('error', 'Server error:', error && error.message ? error.message : error);
      process.exit(1);
    });
  } catch (error) {
    log('error', 'Failed to start server:', error && error.message ? error.message : error);
    process.exit(1);
  }
}

startServer();
