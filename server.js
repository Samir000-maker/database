'use strict';

const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const path = require('path');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const compression = require('compression');

const app = express();

// Configuration
const PORT = parseInt(process.env.PORT, 10) || 4000;
const HOST = process.env.HOST || '0.0.0.0';
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/leveldb_converted?retryWrites=true&w=majority';
const DB_NAME = process.env.DB_NAME || 'leveldb_converted';
const NODE_ENV = process.env.NODE_ENV || 'development';

const SLOT_CAPACITY = 250; // Changed from 2 to 6

let client = null;
let db = null;
let server = null;
const sseClients = new Set();
let isShuttingDown = false;

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
app.use(cors({
    origin: NODE_ENV === 'production' ? ['https://yourdomain.com'] : true,
    credentials: true
}));
app.use(express.static(path.join(__dirname, 'public')));

// Shutdown check middleware
app.use((req, res, next) => {
    if (isShuttingDown) return res.status(503).json({ error: 'Server is shutting down' });
    next();
});

// MongoDB initialization
async function initMongo() {
    try {
        client = new MongoClient(MONGODB_URI, {
            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
            family: 4,
            retryWrites: true,
            w: 'majority'
        });

        await client.connect();
        db = client.db(DB_NAME);
        await db.admin().ping();
        log('info', 'MongoDB connection established successfully');

        // Create indexes (best-effort; don't fail startup on index errors)
        const indexes = [
            { collection: 'user_slots', index: { userId: 1, slotIndex: 1 }, options: { unique: true, background: true } },
            { collection: 'contributed_views_following', index: { userId: 1 }, options: { background: true } },
            { collection: 'contributed_views_following', index: { documentName: 1 }, options: { background: true } },
            { collection: 'contributed_views_following', index: { userId: 1, documentName: 1 }, options: { background: true } }
        ];

        for (const { collection, index, options } of indexes) {
            try {
                await db.collection(collection).createIndex(index, options);
                log('info', `Ensured index for ${collection}`);
            } catch (e) {
                log('warn', `Index creation error for ${collection}: ${e && e.message ? e.message : e}`);
            }
        }

        log('info', `Connected to MongoDB at ${MONGODB_URI}, db: ${DB_NAME}`);
    } catch (error) {
        log('error', 'MongoDB initialization failed:', error && error.message ? error.message : error);
        throw error;
    }
    
    
    
    
       const likeCheckIndexes = [
        { 
            collection: 'user_slots', 
            index: { 'postList.postId': 1, 'postList.likedBy': 1 }, 
            options: { background: true } 
        },
        { 
            collection: 'user_slots', 
            index: { 'reelsList.postId': 1, 'reelsList.likedBy': 1 }, 
            options: { background: true } 
        }
    ];

    for (const { collection, index, options } of likeCheckIndexes) {
        try {
            await db.collection(collection).createIndex(index, options);
            log('info', `Created like-check index for ${collection}`);
        } catch (e) {
            log('warn', `Like-check index error for ${collection}: ${e.message}`);
        }
    }
    
    
    await setupCommentIndexes();
    
    
    // Add this inside initMongo() after existing indexes
const commentCountIndexes = [
    { 
        collection: 'user_slots', 
        index: { 'postList.postId': 1 }, 
        options: { background: true } 
    },
    { 
        collection: 'user_slots', 
        index: { 'reelsList.postId': 1 }, 
        options: { background: true } 
    }
];

for (const { collection, index, options } of commentCountIndexes) {
    try {
        await db.collection(collection).createIndex(index, options);
        log('info', `Created comment-count index for ${collection}`);
    } catch (e) {
        log('warn', `Comment-count index error for ${collection}: ${e.message}`);
    }
}
    
    
    
    
    // Add after existing comment indexes setup
const viewCountIndexes = [
    { 
        collection: 'user_slots', 
        index: { 'postList.postId': 1, 'postList.viewedBy': 1 }, 
        options: { background: true } 
    },
    { 
        collection: 'user_slots', 
        index: { 'reelsList.postId': 1, 'reelsList.viewedBy': 1 }, 
        options: { background: true } 
    }
];

for (const { collection, index, options } of viewCountIndexes) {
    try {
        await db.collection(collection).createIndex(index, options);
        log('info', `Created view-count index for ${collection}`);
    } catch (e) {
        log('warn', `View-count index error for ${collection}: ${e.message}`);
    }
}
    
    
    
    
    
}






async function setupCommentIndexes() {
    try {
        const commentsCollection = db.collection('comments');
        
        // Compound index for fetching comments by post with pagination
        await commentsCollection.createIndex(
            { postId: 1, createdAt: -1 },
            { background: true }
        );
        
        // Index for finding replies to a comment
        await commentsCollection.createIndex(
            { parentId: 1, createdAt: 1 },
            { background: true }
        );
        
        // Index for user's comments
        await commentsCollection.createIndex(
            { userId: 1, createdAt: -1 },
            { background: true }
        );
        
        // Compound index for comment count aggregation
        await commentsCollection.createIndex(
            { postId: 1, parentId: 1 },
            { background: true }
        );
        
        log('info', 'Comment indexes created successfully');
    } catch (error) {
        log('error', 'Comment index creation error:', error.message);
    }
}



// SSE broadcast utility
function broadcastUpdate(data) {
    if (sseClients.size === 0) return;

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
            log('debug', 'Failed to write to SSE client:', err && err.message ? err.message : err);
            toRemove.push(res);
        }
    }

    toRemove.forEach(client => sseClients.delete(client));
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

async function getAllDataForBroadcast() {
    // Be cautious: this pulls all user_slots. In big DBs you might want a limit/aggregation.
    const allSlots = await db.collection('user_slots').find({}).toArray();

    const byUser = {};
    let totalPosts = 0, totalReels = 0, totalSlots = 0;

    allSlots.forEach(slot => {
        if (!byUser[slot.userId]) byUser[slot.userId] = {};
        byUser[slot.userId][slot.slotIndex] = slot;
        totalPosts += Number(slot.postCount || 0);
        totalReels += Number(slot.reelCount || 0);
        totalSlots++;
    });

    const hierarchical = {
        users_posts: {},
        metadata: {
            totalUsers: Object.keys(byUser).length,
            totalSlots,
            totalPosts,
            totalReels,
            lastUpdated: new Date().toISOString()
        }
    };

    for (const [userId, slots] of Object.entries(byUser)) {
        hierarchical.users_posts[userId] = { user_post: {} };
        for (const [slotIndex, doc] of Object.entries(slots)) {
            hierarchical.users_posts[userId].user_post[`${userId}_${slotIndex}`] = doc;
        }
    }

    let contributed = [];
    try {
        contributed = await db.collection('contributed_views_following').find({}).limit(500).toArray();
    } catch (error) {
        log('warn', 'Could not fetch contributed_views_following:', error && error.message ? error.message : error);
    }

    return {
        hierarchical,
        contributed_following: {
            count: contributed.length,
            items: contributed,
            fetchedAt: new Date().toISOString()
        }
    };
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




// GET comment count for a specific post - O(1) lookup using index
app.get('/api/posts/comment-count/:postId', async (req, res) => {
    try {
        const { postId } = req.params;
        
        if (!postId) {
            return res.status(400).json({ error: 'postId required' });
        }

        log('debug', `[GET-COMMENT-COUNT] Fetching for ${postId}`);

        // First, find which array contains the post
        const existingSlot = await db.collection('user_slots').findOne({
            $or: [
                { 'postList.postId': postId },
                { 'reelsList.postId': postId }
            ]
        }, {
            projection: { postList: 1, reelsList: 1 }
        });

        if (!existingSlot) {
            log('warn', `[GET-COMMENT-COUNT] Post not found: ${postId}`);
            return res.json({
                success: true,
                commentCount: 0,
                postId,
                found: false
            });
        }

        // Find the post in either array using JavaScript
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
            found
        });

    } catch (error) {
        log('error', '[GET-COMMENT-COUNT-ERROR]', error);
        return res.status(500).json({ error: 'Failed to get comment count' });
    }
});


app.get('/health', (req, res) => {
  const healthData = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: {
      used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
      unit: 'MB'
    },
    server: {
      name: 'Your Server Name', // Change this for each server
      nodeVersion: process.version,
      platform: process.platform
    }
  };
  
  // Return 200 OK with health data
  res.status(200).json(healthData);
});

// Alternative simple health check (if you prefer minimal response)
app.get('/ping', (req, res) => {
  res.status(200).send('pong');
});



// In your PORT 4000 server (document index 2)

app.post('/api/posts/record-retention', writeLimit, async (req, res) => {
    try {
        const { userId, postId, retentionPercent, watchedDuration, totalDuration, isReel } = req.body;

        log('info', `Retention request: ${userId} -> ${postId}, ${retentionPercent}%`);

        if (!validate.userId(userId) || !postId || typeof retentionPercent !== 'number') {
            return res.status(400).json({ error: 'userId, postId, and retentionPercent required' });
        }

        if (retentionPercent < 0 || retentionPercent > 100) {
            return res.status(400).json({ error: 'retentionPercent must be between 0 and 100' });
        }

        const cleanUserId = validate.sanitize(userId);
        const arrayField = isReel ? 'reelsList' : 'postList';

        // ✅ CRITICAL FIX: Use $elemMatch to check THIS SPECIFIC POST's contributors
        const existingSlot = await db.collection('user_slots').findOne({
            [arrayField]: {
                $elemMatch: {
                    postId: postId,
                    retentionContributors: cleanUserId
                }
            }
        });

        if (existingSlot) {
            log('info', `Retention duplicate blocked: ${cleanUserId} -> ${postId}`);
            return res.json({ 
                success: true, 
                message: 'Retention already contributed', 
                duplicate: true 
            });
        }

        // ✅ Now update the retention data for THIS SPECIFIC POST
        const updateResult = await db.collection('user_slots').updateOne(
            {
                [`${arrayField}.postId`]: postId
            },
            {
                $set: {
                    [`${arrayField}.$.retention`]: Math.round(retentionPercent * 100) / 100,
                    [`${arrayField}.$.watchedDuration`]: watchedDuration,
                    [`${arrayField}.$.totalDuration`]: totalDuration,
                    [`${arrayField}.$.retentionUpdatedAt`]: new Date().toISOString(),
                    'updatedAt': new Date().toISOString()
                },
                $addToSet: {
                    [`${arrayField}.$.retentionContributors`]: cleanUserId
                }
            }
        );

        if (updateResult.matchedCount === 0) {
            log('warn', `Post not found in ANY user slots: ${postId}`);
            return res.status(404).json({ error: 'Post not found in database' });
        }

        log('info', `Retention recorded successfully: ${cleanUserId} -> ${postId}, ${retentionPercent}%`);
        
        // Sync to PORT 2000
        syncMetricsToPort2000(postId, isReel).catch(err => {
            log('error', '[RETENTION-SYNC-ERROR]', err.message);
        });
        
        asyncBroadcast();

        res.json({
            success: true,
            message: 'Retention recorded successfully',
            postId,
            retentionPercent: Math.round(retentionPercent * 100) / 100
        });

    } catch (error) {
        log('error', 'Retention recording error:', error && error.message ? error.message : error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Replace the existing toggle-like endpoint  
app.post('/api/posts/toggle-like', writeLimit, async (req, res) => {
    try {
        const { userId, postId, isReel, currentlyLiked } = req.body;

        log('info', `Like request: ${userId} -> ${postId}, currently: ${currentlyLiked}`);

        if (!validate.userId(userId) || !postId) {
            return res.status(400).json({ error: 'userId and postId required' });
        }

        const cleanUserId = validate.sanitize(userId);
        const action = currentlyLiked ? 'unlike' : 'like';

        const existingSlot = await db.collection('user_slots').findOne({
            $or: [
                { 'postList.postId': postId },
                { 'reelsList.postId': postId }
            ]
        });

        if (!existingSlot) {
            log('warn', `Post not found in any user slots: ${postId}`);
            return res.json({
                success: true,
                action,
                isLiked: !currentlyLiked,
                likeCount: currentlyLiked ? 0 : 1,
                message: `Post not found in database, using fallback behavior`,
                fallback: true
            });
        }

        let arrayField = null;
        let isInPostList = existingSlot.postList && existingSlot.postList.some(item => item.postId === postId);
        let isInReelsList = existingSlot.reelsList && existingSlot.reelsList.some(item => item.postId === postId);

        if (isInPostList) {
            arrayField = 'postList';
            log('info', `Found post ${postId} in postList of ${existingSlot._id}`);
        } else if (isInReelsList) {
            arrayField = 'reelsList';
            log('info', `Found post ${postId} in reelsList of ${existingSlot._id}`);
        } else {
            log('error', `Post ${postId} exists in slot but not in either array`);
            return res.status(500).json({ error: 'Data inconsistency error' });
        }

        const alreadyLiked = await db.collection('user_slots').findOne({
            [`${arrayField}.postId`]: postId,
            [`${arrayField}.likedBy`]: cleanUserId
        });

        const hasLiked = !!alreadyLiked;

        if (currentlyLiked && !hasLiked) {
            return res.json({ 
                success: true, 
                message: 'State corrected', 
                isLiked: false, 
                likeCount: 0,
                action: 'unlike'
            });
        }
        if (!currentlyLiked && hasLiked) {
            return res.json({ 
                success: true, 
                message: 'State corrected', 
                isLiked: true,
                action: 'like'
            });
        }

        let updateOperation;
        if (!currentlyLiked) {
            updateOperation = {
                $inc: { [`${arrayField}.$.likeCount`]: 1 },
                $addToSet: { [`${arrayField}.$.likedBy`]: cleanUserId },
                $set: { 'updatedAt': new Date().toISOString() }
            };
        } else {
            updateOperation = {
                $inc: { [`${arrayField}.$.likeCount`]: -1 },
                $pull: { [`${arrayField}.$.likedBy`]: cleanUserId },
                $set: { 'updatedAt': new Date().toISOString() }
            };
        }

        const result = await db.collection('user_slots').updateOne(
            {
                [`${arrayField}.postId`]: postId
            },
            updateOperation
        );

        if (result.matchedCount === 0) {
            log('warn', `Failed to update like for post: ${postId} in ${arrayField}`);
            return res.status(500).json({ error: 'Failed to update post' });
        }

        const updatedSlot = await db.collection('user_slots').findOne(
            {
                [`${arrayField}.postId`]: postId
            },
            { projection: { [`${arrayField}.$`]: 1 } }
        );

        const newLikeCount = updatedSlot && updatedSlot[arrayField] && updatedSlot[arrayField][0] 
            ? Math.max(0, updatedSlot[arrayField][0].likeCount || 0)
            : 0;

        log('info', `Like ${action} successful: ${cleanUserId} -> ${postId} in ${arrayField}, new count: ${newLikeCount}`);

        // Sync to PORT 2000
        const isReelContent = arrayField === 'reelsList';
        syncMetricsToPort2000(postId, isReelContent).catch(err => {
            log('error', '[LIKE-SYNC-ERROR]', err.message);
        });

        asyncBroadcast();

        res.json({
            success: true,
            action,
            isLiked: !currentlyLiked,
            likeCount: newLikeCount,
            message: `Successfully ${action}d`,
            foundIn: arrayField
        });

    } catch (error) {
        log('error', 'Like toggle error:', error && error.message ? error.message : error);
        res.status(500).json({ error: 'Internal server error' });
    }
});



// POST increment view count (only for retention contributors)
app.post('/api/posts/increment-view', writeLimit, async (req, res) => {
    try {
        const { userId, postId, isReel } = req.body;
        
        if (!userId || !postId) {
            return res.status(400).json({ error: 'userId and postId required' });
        }

        log('info', `[VIEW-INCREMENT] ${userId} -> ${postId}`);

        const cleanUserId = validate.sanitize(userId);
        const arrayField = isReel ? 'reelsList' : 'postList';

        const existingView = await db.collection('user_slots').findOne({
            [`${arrayField}.postId`]: postId,
            [`${arrayField}.viewedBy`]: cleanUserId
        });

        if (existingView) {
            log('info', `[VIEW-DUPLICATE] ${cleanUserId} already viewed ${postId}`);
            return res.json({ 
                success: true, 
                message: 'View already counted', 
                duplicate: true 
            });
        }

        const retentionCheck = await db.collection('user_slots').findOne({
            [`${arrayField}.postId`]: postId,
            [`${arrayField}.retentionContributors`]: cleanUserId
        });

        if (!retentionCheck) {
            log('warn', `[VIEW-NO-RETENTION] ${cleanUserId} has not contributed retention for ${postId}`);
            return res.status(403).json({ 
                error: 'View count only incremented for retention contributors' 
            });
        }

        const updateResult = await db.collection('user_slots').updateOne(
            { [`${arrayField}.postId`]: postId },
            {
                $inc: { [`${arrayField}.$.viewCount`]: 1 },
                $addToSet: { [`${arrayField}.$.viewedBy`]: cleanUserId },
                $set: { 'updatedAt': new Date().toISOString() }
            }
        );

        if (updateResult.matchedCount === 0) {
            log('warn', `[VIEW-NOT-FOUND] Post ${postId} not found`);
            return res.status(404).json({ error: 'Post not found' });
        }

        const updatedSlot = await db.collection('user_slots').findOne(
            { [`${arrayField}.postId`]: postId },
            { projection: { [`${arrayField}.$`]: 1 } }
        );

        const newViewCount = updatedSlot && updatedSlot[arrayField] && updatedSlot[arrayField][0]
            ? Math.max(0, updatedSlot[arrayField][0].viewCount || 1)
            : 1;

        log('info', `[VIEW-SUCCESS] ${cleanUserId} -> ${postId}, new count: ${newViewCount}`);

        // Sync to PORT 2000
        syncMetricsToPort2000(postId, isReel).catch(err => {
            log('error', '[VIEW-SYNC-ERROR]', err.message);
        });

        asyncBroadcast();

        return res.json({
            success: true,
            viewCount: newViewCount,
            message: 'View counted successfully'
        });

    } catch (error) {
        log('error', '[VIEW-INCREMENT-ERROR]', error);
        return res.status(500).json({ error: 'Failed to increment view count' });
    }
});


// Helper function to sync metrics to PORT 2000
async function syncMetricsToPort2000(postId, isReel) {
    try {
        const arrayField = isReel ? 'reelsList' : 'postList';
        
        // Get current metrics from PORT 4000
        const slot = await db.collection('user_slots').findOne({
            [`${arrayField}.postId`]: postId
        });
        
        if (!slot) {
            log('warn', `[SYNC] Post not found for sync: ${postId}`);
            return;
        }
        
        const content = slot[arrayField]?.find(item => item.postId === postId);
        if (!content) {
            log('warn', `[SYNC] Content not found in array: ${postId}`);
            return;
        }
        
        const metrics = {
            likeCount: content.likeCount || 0,
            commentCount: content.commentCount || 0,
            viewCount: content.viewCount || 0,
            retention: content.retention || 0
        };
        
        log('info', `[SYNC] Syncing metrics to PORT 2000: ${postId}`, metrics);
        
        // Sync to PORT 2000
        //const response = await fetch('http://192.168.50.123:7000/api/sync/metrics', {
        const response = await fetch('https://samir-hgr9.onrender.com/api/sync/metrics', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ postId, metrics, isReel }),
            signal: AbortSignal.timeout(5000) // 5 second timeout
        });
        
        if (!response.ok) {
            const errorText = await response.text();
            log('error', `[SYNC-FAILED] ${postId}:`, errorText);
        } else {
            const result = await response.json();
            log('info', `[SYNC-SUCCESS] ${postId}:`, result);
        }
    } catch (error) {
        log('error', `[SYNC-ERROR] ${postId}:`, error.message);
    }
}



// Batch check view contributions
app.post('/api/posts/check-view-contributions', async (req, res) => {
    try {
        const { userId, postIds } = req.body;
        
        if (!userId || !Array.isArray(postIds)) {
            return res.status(400).json({ error: 'userId and postIds array required' });
        }

        log('debug', `[BATCH-VIEW-CHECK] ${userId} checking ${postIds.length} posts`);

        const cleanUserId = validate.sanitize(userId);
        const result = {};

        // Single optimized query for all posts
        const [postSlots, reelSlots] = await Promise.all([
            db.collection('user_slots').find({
                'postList.postId': { $in: postIds }
            }).project({ 'postList.$': 1 }).toArray(),
            
            db.collection('user_slots').find({
                'reelsList.postId': { $in: postIds }
            }).project({ 'reelsList.$': 1 }).toArray()
        ]);

        // Process results
        for (const postId of postIds) {
            let hasRetention = false;
            let hasViewCounted = false;

            // Check in postList
            for (const slot of postSlots) {
                const post = slot.postList && slot.postList[0];
                if (post && post.postId === postId) {
                    hasRetention = post.retentionContributors && post.retentionContributors.includes(cleanUserId);
                    hasViewCounted = post.viewedBy && post.viewedBy.includes(cleanUserId);
                    break;
                }
            }

            // Check in reelsList if not found
            if (!hasRetention && !hasViewCounted) {
                for (const slot of reelSlots) {
                    const reel = slot.reelsList && slot.reelsList[0];
                    if (reel && reel.postId === postId) {
                        hasRetention = reel.retentionContributors && reel.retentionContributors.includes(cleanUserId);
                        hasViewCounted = reel.viewedBy && reel.viewedBy.includes(cleanUserId);
                        break;
                    }
                }
            }

            result[postId] = {
                hasRetention,
                hasViewCounted,
                canIncrementView: hasRetention && !hasViewCounted
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



// Add this debug endpoint to your MongoDB server
app.get('/api/posts/debug-exists/:postId', async (req, res) => {
    try {
        const { postId } = req.params;
        
        const existingSlot = await db.collection('user_slots').findOne({
            $or: [
                { 'reelsList.postId': postId },
                { 'postList.postId': postId }
            ]
        });

        let foundDetails = null;
        if (existingSlot) {
            const inPostList = existingSlot.postList && existingSlot.postList.some(item => item.postId === postId);
            const inReelsList = existingSlot.reelsList && existingSlot.reelsList.some(item => item.postId === postId);
            
            foundDetails = {
                slotId: existingSlot._id,
                userId: existingSlot.userId,
                inPostList,
                inReelsList,
                arrayField: inPostList ? 'postList' : (inReelsList ? 'reelsList' : 'unknown')
            };
        }

        res.json({
            exists: !!existingSlot,
            postId,
            details: foundDetails
        });
        
    } catch (error) {
        log('error', 'Debug check failed:', error);
        res.status(500).json({ error: 'Debug check failed' });
    }
});





// Ultra-fast like state check - O(1) lookup using compound index
app.get('/api/posts/check-liked/:userId/:postId', async (req, res) => {
    try {
        const { userId, postId } = req.params;
        
        if (!userId || !postId) {
            return res.status(400).json({ error: 'userId and postId required' });
        }

        log('debug', `[CHECK-LIKED] ${userId} -> ${postId}`);

        // Single optimized query using compound index on likedBy array
        const existingSlot = await db.collection('user_slots').findOne({
            $or: [
                { 'postList.postId': postId, 'postList.likedBy': userId },
                { 'reelsList.postId': postId, 'reelsList.likedBy': userId }
            ]
        }, { projection: { _id: 1 } });

        const isLiked = !!existingSlot;

        return res.json({
            success: true,
            isLiked,
            postId,
            userId,
            queryTime: Date.now()
        });

    } catch (error) {
        log('error', '[CHECK-LIKED-ERROR]', error);
        return res.status(500).json({ error: 'Failed to check like state' });
    }
});




// Add new endpoint after existing routes in document index 1
app.post('/api/posts/record-retention', async (req, res) => {
    try {
        const { userId, postId, retentionPercent, watchedDuration, totalDuration, isReel } = req.body;

        if (!userId || !postId || typeof retentionPercent !== 'number') {
            return res.status(400).json({ error: 'userId, postId, and retentionPercent required' });
        }

        // Validate retention percentage
        if (retentionPercent < 0 || retentionPercent > 100) {
            return res.status(400).json({ error: 'retentionPercent must be between 0 and 100' });
        }

        const cleanUserId = validate.sanitize(userId);
        const collection = isReel ? 'user_slots' : 'user_slots'; // Same collection for both
        const arrayField = isReel ? 'reelsList' : 'postList';

        // Check if user already contributed retention for this post
        const existingSlot = await db.collection(collection).findOne({
            userId: cleanUserId,
            [`${arrayField}.postId`]: postId,
            [`${arrayField}.retentionContributors`]: cleanUserId
        });

        if (existingSlot) {
            return res.json({ 
                success: true, 
                message: 'Retention already contributed', 
                duplicate: true 
            });
        }

        // Update the specific post/reel with retention data
        const updateResult = await db.collection(collection).updateOne(
            {
                userId: cleanUserId,
                [`${arrayField}.postId`]: postId
            },
            {
                $set: {
                    [`${arrayField}.$.retention`]: Math.round(retentionPercent * 100) / 100,
                    [`${arrayField}.$.watchedDuration`]: watchedDuration,
                    [`${arrayField}.$.totalDuration`]: totalDuration,
                    [`${arrayField}.$.retentionUpdatedAt`]: new Date().toISOString(),
                    'updatedAt': new Date().toISOString()
                },
                $addToSet: {
                    [`${arrayField}.$.retentionContributors`]: cleanUserId
                }
            }
        );

        if (updateResult.matchedCount === 0) {
            return res.status(404).json({ error: 'Post not found in user slots' });
        }

        asyncBroadcast();

        res.json({
            success: true,
            message: 'Retention recorded successfully',
            postId,
            retentionPercent: Math.round(retentionPercent * 100) / 100
        });

    } catch (error) {
        log('error', 'Retention recording error:', error && error.message ? error.message : error);
        res.status(500).json({ error: 'Internal server error' });
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
        
        // Single optimized query using compound index
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

// GET replies for a comment (20 per page)
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

// POST new comment or reply
app.post('/api/comments', writeLimit, async (req, res) => {
    try {
        const { postId, userId, username, profilePictureUrl, text, parentId, replyToUsername, ownerId } = req.body;
        
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
        
        // Insert comment
        await db.collection('comments').insertOne(comment);
        
        // Update parent comment's reply count if this is a reply
        if (parentId) {
            await db.collection('comments').updateOne(
                { _id: parentId },
                { $inc: { replyCount: 1 } }
            );
        }
        
        // Update post's comment count in user_slots
        const arrayField = req.body.isReel ? 'reelsList' : 'postList';
        await db.collection('user_slots').updateOne(
            { [`${arrayField}.postId`]: postId },
            { 
                $inc: { [`${arrayField}.$.commentCount`]: 1 },
                $set: { updatedAt: now }
            }
        );
        
        asyncBroadcast();
        
        res.json({
            success: true,
            comment,
            message: parentId ? 'Reply added successfully' : 'Comment added successfully'
        });
        
    } catch (error) {
        log('error', 'Add comment error:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// UPDATE comment/reply
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

// DELETE comment/reply
app.delete('/api/comments/:commentId', writeLimit, async (req, res) => {
    try {
        const { commentId } = req.params;
        const { userId, postId, isReel } = req.query;
        
        if (!commentId || !userId || !postId) {
            return res.status(400).json({ error: 'commentId, userId, and postId required' });
        }
        
        // Get comment to check parentId
        const comment = await db.collection('comments').findOne({ 
            _id: commentId, 
            userId: validate.sanitize(userId) 
        });
        
        if (!comment) {
            return res.status(404).json({ error: 'Comment not found or unauthorized' });
        }
        
        // Count all nested replies recursively
        const countReplies = async (parentId) => {
            const replies = await db.collection('comments')
                .find({ parentId })
                .project({ _id: 1 })
                .toArray();
            
            let count = replies.length;
            for (const reply of replies) {
                count += await countReplies(reply._id);
            }
            return count;
        };
        
        const totalReplies = await countReplies(commentId);
        const totalDeleted = 1 + totalReplies;
        
        // Delete comment and all its replies
        await db.collection('comments').deleteMany({
            $or: [
                { _id: commentId },
                { parentId: commentId }
            ]
        });
        
        // Update parent comment's reply count if this was a reply
        if (comment.parentId) {
            await db.collection('comments').updateOne(
                { _id: comment.parentId },
                { $inc: { replyCount: -1 } }
            );
        }
        
        // Update post's comment count
        const arrayField = isReel === 'true' ? 'reelsList' : 'postList';
        await db.collection('user_slots').updateOne(
            { [`${arrayField}.postId`]: postId },
            { 
                $inc: { [`${arrayField}.$.commentCount`]: -totalDeleted },
                $set: { updatedAt: new Date().toISOString() }
            }
        );
        
        asyncBroadcast();
        
        res.json({
            success: true,
            message: 'Comment deleted successfully',
            deletedCount: totalDeleted
        });
        
    } catch (error) {
        log('error', 'Delete comment error:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// TOGGLE comment like
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





// Following views endpoints
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

// Posts endpoints
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
        // Try to find a slot with space (count < SLOT_CAPACITY)
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
                index: targetIndex,
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

// Enhanced content fetching with ranking - CORRECTED SECTION
app.get('/api/content/user/:uid', async (req, res) => {
    try {
        const { uid } = req.params;
        
        if (!uid || typeof uid !== 'string' || uid.trim().length === 0) {
            return res.status(400).json({ success: false, error: 'Invalid UID' });
        }

        const cleanUserId = uid.trim();
        
        log('info', `[GET-USER-CONTENT] Fetching content for userId: ${cleanUserId}`);

        const userSlots = await db.collection('user_slots')
            .find({ userId: cleanUserId })
            .sort({ slotIndex: 1 })
            .toArray();

        if (!userSlots || userSlots.length === 0) {
            log('warn', `[GET-USER-CONTENT] No slots found for userId: ${cleanUserId}`);
            return res.status(200).json({
                success: true,
                posts: [],
                reels: [],
                totalPosts: 0,
                totalReels: 0
            });
        }

        log('info', `[GET-USER-CONTENT] Found ${userSlots.length} slots for userId: ${cleanUserId}`);

        const posts = [];
        const reels = [];

        userSlots.forEach((slot, slotIdx) => {
            log('debug', `[GET-USER-CONTENT] Processing slot ${slotIdx}: postList=${slot.postList?.length || 0}, reelsList=${slot.reelsList?.length || 0}`);
            
            // ✅ CRITICAL FIX: Process posts with ALL multiple_posts fields
            if (Array.isArray(slot.postList) && slot.postList.length > 0) {
                slot.postList.forEach((post, postIdx) => {
                    log('debug', `[GET-USER-CONTENT] Adding post ${postIdx}: ${post.postId}, imageUrl: ${post.imageUrl?.substring(0, 50)}..., multiple_posts: ${post.multiple_posts}`);
                    
                    posts.push({
                        postId: post.postId || post.id,
                        userId: cleanUserId,
                        imageUrl: post.imageUrl || '',
                        
                        // ✅ CRITICAL: Include multiple_posts fields
                        multiple_posts: post.multiple_posts || false,
                        media_count: post.media_count || 0,
                        
                        // ✅ CRITICAL: Include all imageUrl1-20 fields
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

            // Process reels from reelsList
            if (Array.isArray(slot.reelsList) && slot.reelsList.length > 0) {
                log('info', `[GET-USER-CONTENT] Processing ${slot.reelsList.length} reels from slot ${slotIdx}`);
                
                slot.reelsList.forEach((reel, reelIdx) => {
                    // Extract clean UUID (remove _reel_ suffix if composite ID)
                    let actualPostId = reel.postId || reel.reelId || reel.id;
                    
                    if (actualPostId && actualPostId.includes('_reel_')) {
                        const reelIndex = actualPostId.indexOf('_reel_');
                        actualPostId = actualPostId.substring(0, reelIndex);
                    }
                    
                    log('debug', `[GET-USER-CONTENT] Adding reel ${reelIdx}: reelId=${reel.reelId}, postId=${actualPostId}, videoUrl: ${reel.videoUrl?.substring(0, 50)}..., imageUrl: ${reel.imageUrl?.substring(0, 50)}...`);
                    
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
                
                log('info', `[GET-USER-CONTENT] Added ${slot.reelsList.length} reels from slot ${slotIdx}. Total reels now: ${reels.length}`);
            } else {
                log('debug', `[GET-USER-CONTENT] Slot ${slotIdx} has no reels or empty reelsList`);
            }
        });

        // Sort by ranking score (highest first)
        posts.sort((a, b) => b.rankingScore - a.rankingScore);
        reels.sort((a, b) => b.rankingScore - a.rankingScore);

        log('info', `[GET-USER-CONTENT] Returning for ${cleanUserId}: ${posts.length} posts, ${reels.length} reels`);

        res.status(200).json({
            success: true,
            posts,
            reels,
            totalPosts: posts.length,
            totalReels: reels.length,
            userId: cleanUserId
        });

    } catch (err) {
        log('error', '[GET-USER-CONTENT-ERROR]', err.message, err.stack);
        res.status(500).json({ success: false, error: 'Internal server error' });
    }
});

// Ranking calculation function
function calculateRankingScore(metrics) {
    // Priority weights (Instagram-like algorithm)
    const RETENTION_WEIGHT = 100;  // Highest priority
    const LIKE_WEIGHT = 10;        // Second priority
    const COMMENT_WEIGHT = 5;      // Third priority
    const VIEW_WEIGHT = 1;         // Fourth priority
    
    return (
        (metrics.retention || 0) * RETENTION_WEIGHT +
        (metrics.likeCount || 0) * LIKE_WEIGHT +
        (metrics.commentCount || 0) * COMMENT_WEIGHT +
        (metrics.viewCount || 0) * VIEW_WEIGHT
    );
}

// Helper function to extract clean post ID
function extractCleanPostId(postId) {
    if (!postId) return null;
    const reelIndex = postId.indexOf('_reel_');
    if (reelIndex > 0) {
        return postId.substring(0, reelIndex);
    }
    return postId;
}

// GET posts only for a user
app.get('/api/posts/user/:uid', async (req, res) => {
    try {
        const { uid } = req.params;

        if (!uid || typeof uid !== 'string' || uid.trim().length === 0) {
            return res.status(400).json({ success: false, error: 'Invalid UID' });
        }

        const cleanUserId = uid.trim();

        // Find all slots for this user in user_slots collection
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

        // Collect all posts from all user slots
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

        // Sort by timestamp (newest first)
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

// Combined following content endpoints
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

        const cleanViewerId = viewerId ? validate.sanitize(viewerId) : null;
        const { viewedPosts, viewedReels } = await fetchViewerViewedSets(cleanViewerId);

        const content = [];
        const allPosts = [];
        const allReels = [];
        let filteredOut = 0;
        const userStats = {};

        const slots = await db.collection('user_slots')
            .find({ userId: { $in: cleanUserIds } })
            .toArray();

        log('info', `[BATCH-FOLLOWING] Found ${slots.length} user slots for ${cleanUserIds.length} users`);

        slots.forEach(slot => {
            const uid = slot.userId;
            const slotIndex = slot.slotIndex;
            const docName = `${uid}_${slotIndex}`;

            if (!userStats[uid]) {
                userStats[uid] = { posts: 0, reels: 0, latestPostTime: null, latestReelTime: null };
            }

            // Process posts
            if (Array.isArray(slot.postList)) {
                slot.postList.forEach(post => {
                    try {
                        if (cleanViewerId && isItemViewed(post, viewedPosts, viewedReels, false)) {
                            filteredOut++;
                            return;
                        }

                        const processedPost = {
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
                        };

                        content.push(processedPost);
                        allPosts.push(processedPost);
                        userStats[uid].posts++;

                        const timestamp = new Date(post.timestamp || post.savedAt || 0);
                        if (!userStats[uid].latestPostTime || timestamp > userStats[uid].latestPostTime) {
                            userStats[uid].latestPostTime = timestamp;
                        }
                    } catch (error) {
                        log('error', `[POST-PROCESS-ERROR] ${error.message}`);
                    }
                });
            }

            // Process reels
            if (Array.isArray(slot.reelsList)) {
                slot.reelsList.forEach(reel => {
                    try {
                        if (cleanViewerId && isItemViewed(reel, viewedPosts, viewedReels, true)) {
                            filteredOut++;
                            return;
                        }

                        let actualPostId = reel.postId || reel.reelId || reel.id;

                        const processedReel = {
                            ...reel,
                            postId: actualPostId,
                            reelId: reel.reelId || actualPostId,
                            sourceDocument: docName,
                            documentSlot: slotIndex,
                            followedUserId: uid,
                            contentType: 'following',
                            isReel: true,
                            slotIndex,
                            fetchedAt: new Date().toISOString(),
                            userId: uid
                        };

                        content.push(processedReel);
                        allReels.push(processedReel);
                        userStats[uid].reels++;

                        const timestamp = new Date(reel.timestamp || reel.savedAt || 0);
                        if (!userStats[uid].latestReelTime || timestamp > userStats[uid].latestReelTime) {
                            userStats[uid].latestReelTime = timestamp;
                        }
                    } catch (error) {
                        log('error', `[REEL-PROCESS-ERROR] ${error.message}`);
                    }
                });
            }
        });

        // Sort by engagement for batch-following
        const sortByEngagement = req.url && req.url.includes('/batch-following');
        content.sort((a, b) => {
            if (sortByEngagement) {
                const engagementA = (a.likeCount || 0) * 2 + (a.commentCount || 0) * 3;
                const engagementB = (b.likeCount || 0) * 2 + (b.commentCount || 0) * 3;
                if (engagementA !== engagementB) return engagementB - engagementA;
            }

            try {
                const dateA = new Date(a.timestamp || a.savedAt || 0);
                const dateB = new Date(b.timestamp || b.savedAt || 0);
                return dateB - dateA;
            } catch {
                return 0;
            }
        });

        const baseResponse = {
            success: true,
            viewerId: cleanViewerId,
            filteredOut,
            viewerViewedCounts: {
                posts: viewedPosts.size,
                reels: viewedReels.size
            },
            timestamp: new Date().toISOString()
        };

        log('info', `[BATCH-FOLLOWING] Returning ${content.length} items (${allPosts.length} posts, ${allReels.length} reels)`);

        return res.json({
            ...baseResponse,
            requestedUsers: cleanUserIds,
            content,
            totalItems: content.length,
            userCount: cleanUserIds.length,
            stats: {
                posts: allPosts.length,
                reels: allReels.length,
                filteredOut
            }
        });

    } catch (error) {
        log('error', '[BATCH-FOLLOWING-PROCESS-ERROR]', error.message, error.stack);
        
        // Return empty result instead of 500
        return res.status(200).json({
            success: true,
            content: [],
            totalItems: 0,
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
        
        // Return empty content instead of 500 error
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
        const data = await getAllDataForBroadcast();
        res.json(data);
    } catch (error) {
        log('error', 'Get all posts error:', error && error.message ? error.message : error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// SSE Stream endpoint
app.get('/api/posts/stream', (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Cache-Control, Content-Type'
    });

    // Add to set
    sseClients.add(res);
    log('debug', 'New SSE client connected. Total clients:', sseClients.size);

    // Send initial data
    getAllDataForBroadcast()
        .then(data => {
            try {
                if (!res.writableEnded && !res.destroyed) {
                    res.write(`data: ${JSON.stringify(data)}\n\n`);
                }
            } catch (error) {
                log('debug', 'Failed to send initial SSE data:', error && error.message ? error.message : error);
                sseClients.delete(res);
            }
        })
        .catch(error => {
            log('error', 'Error sending initial SSE data:', error && error.message ? error.message : error);
            try {
                if (!res.writableEnded && !res.destroyed) {
                    res.write(`data: ${JSON.stringify({ error: 'Failed to load initial data' })}\n\n`);
                }
            } catch {
                sseClients.delete(res);
            }
        });

    // Keep-alive ping
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
        log('debug', 'SSE client error:', error && error.message ? error.message : error);
        sseClients.delete(res);
        clearInterval(pingInterval);
    });
});



// Add this to your PORT 4000 server (document index 2)
app.get('/api/posts/get-like-count/:postId', async (req, res) => {
    try {
        const { postId } = req.params;
        
        if (!postId) {
            return res.status(400).json({ error: 'postId required' });
        }

        log('debug', `[GET-LIKE-COUNT] Fetching for ${postId}`);

        // Search in BOTH postList and reelsList
        const existingSlot = await db.collection('user_slots').findOne({
            $or: [
                { 'postList.postId': postId },
                { 'reelsList.postId': postId }
            ]
        });

        if (!existingSlot) {
            log('warn', `[GET-LIKE-COUNT] Post not found: ${postId}`);
            return res.json({
                success: true,
                likeCount: 0,
                postId,
                found: false
            });
        }

        // Find the post in either array
        let likeCount = 0;
        let found = false;

        if (existingSlot.postList) {
            const post = existingSlot.postList.find(p => p.postId === postId);
            if (post) {
                likeCount = Math.max(0, post.likeCount || 0);
                found = true;
            }
        }

        if (!found && existingSlot.reelsList) {
            const reel = existingSlot.reelsList.find(r => r.postId === postId);
            if (reel) {
                likeCount = Math.max(0, reel.likeCount || 0);
                found = true;
            }
        }

        log('info', `[GET-LIKE-COUNT] ${postId} -> ${likeCount}`);

        return res.json({
            success: true,
            likeCount,
            postId,
            found
        });

    } catch (error) {
        log('error', '[GET-LIKE-COUNT-ERROR]', error);
        return res.status(500).json({ error: 'Failed to get like count' });
    }
});



// Database viewer endpoint (serves a single-file UI)
app.get('/database-viewer', (req, res) => {
    // The same HTML as before — kept compact. If you prefer, serve a static file instead.
    res.send(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>MongoDB Database Viewer</title><style>body{font-family:system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif;background:#111;color:#eee;margin:0;line-height:1.6}.header{padding:16px;display:flex;justify-content:space-between;align-items:center;background:linear-gradient(90deg,#1e1e2f,#2a2a3a);box-shadow:0 2px 4px rgba(0,0,0,0.3)}.header h1{margin:0;font-size:1.2em}.status{padding:4px 12px;border-radius:16px;font-size:0.9em;font-weight:500}.status.connected{background:#10b981;color:white}.status.loading{background:#f59e0b;color:white}.status.error{background:#ef4444;color:white}.container{padding:16px;height:calc(100vh - 80px);overflow:auto}.controls{margin-bottom:16px;display:flex;gap:12px;flex-wrap:wrap}.btn{padding:8px 16px;background:#374151;border:none;border-radius:6px;color:#fff;cursor:pointer;font-size:0.9em}.btn:hover{background:#4b5563}.btn.active{background:#3b82f6}pre{white-space:pre-wrap;word-wrap:break-word;background:#0b0b0b;padding:16px;border-radius:8px;border:1px solid #374151;font-size:0.9em;max-height:80vh;overflow:auto}.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px}.stat-card{background:#1f2937;padding:12px;border-radius:8px;border:1px solid #374151}.stat-value{font-size:1.5em;font-weight:bold;color:#10b981}.stat-label{font-size:0.9em;color:#9ca3af}</style></head><body><div class="header"><h1>🍃 MongoDB Database Viewer</h1><div class="status loading" id="status">Loading...</div></div><div class="container"><div class="stats" id="stats"></div><div class="controls"><button class="btn active" id="btn-all" onclick="showData('all')">All Data</button><button class="btn" id="btn-hierarchical" onclick="showData('hierarchical')">User Posts</button><button class="btn" id="btn-contributed" onclick="showData('contributed')">Viewed Content</button><button class="btn" onclick="refreshData()">Refresh</button></div><pre id="payload">Loading...</pre></div><script>let currentData=null;let currentView='all';function updateStats(data){const statsEl=document.getElementById('stats');if(!data||!data.hierarchical||!data.hierarchical.metadata){statsEl.innerHTML='';return}const meta=data.hierarchical.metadata;const contrib=data.contributed_following||{};statsEl.innerHTML=\`<div class="stat-card"><div class="stat-value">\${meta.totalUsers||0}</div><div class="stat-label">Total Users</div></div><div class="stat-card"><div class="stat-value">\${meta.totalPosts||0}</div><div class="stat-label">Total Posts</div></div><div class="stat-card"><div class="stat-value">\${meta.totalReels||0}</div><div class="stat-label">Total Reels</div></div><div class="stat-card"><div class="stat-value">\${contrib.count||0}</div><div class="stat-label">Viewed Records</div></div>\`}function showData(view){currentView=view;document.querySelectorAll('.btn').forEach(btn=>btn.classList.remove('active'));document.getElementById(\`btn-\${view}\`).classList.add('active');if(!currentData)return;const payloadEl=document.getElementById('payload');let displayData;switch(view){case 'hierarchical':displayData=currentData.hierarchical;break;case 'contributed':displayData=currentData.contributed_following;break;default:displayData=currentData}payloadEl.textContent=JSON.stringify(displayData,null,2)}function refreshData(){const statusEl=document.getElementById('status');const payloadEl=document.getElementById('payload');statusEl.textContent='Loading...';statusEl.className='status loading';fetch('/api/posts/all').then(response=>{if(!response.ok)throw new Error(\`HTTP \${response.status}\`);return response.json()}).then(data=>{currentData=data;updateStats(data);showData(currentView);statusEl.textContent='Loaded';statusEl.className='status connected'}).catch(error=>{payloadEl.textContent=\`Failed to load: \${error.message}\`;statusEl.textContent='Error';statusEl.className='status error';console.error('Load error:',error)})}function setupEventSource(){const statusEl=document.getElementById('status');try{const eventSource=new EventSource('/api/posts/stream');eventSource.onopen=function(){statusEl.textContent='Connected';statusEl.className='status connected'};eventSource.onerror=function(){statusEl.textContent='Disconnected';statusEl.className='status error'};eventSource.onmessage=function(event){try{const data=JSON.parse(event.data);currentData=data;updateStats(data);showData(currentView);statusEl.textContent='Connected (Live)';statusEl.className='status connected'}catch(error){console.error('SSE parse error:',error)}};eventSource.addEventListener('error',function(){setTimeout(()=>{if(eventSource.readyState===EventSource.CLOSED){setupEventSource()}},5000)})}catch(error){console.error('EventSource error:',error);refreshData();setInterval(refreshData,10000)}}refreshData();setupEventSource()</script></body></html>`);
});

// Error handling middleware
app.use((err, req, res, next) => {
    log('error', 'Unhandled error:', err && err.message ? err.message : err);
    if (res.headersSent) return next(err);
    res.status(500).json({
        error: 'Internal server error',
        ...(NODE_ENV === 'development' && { details: err && err.message ? err.message : String(err) })
    });
});

// Handle 404
app.use('*', (req, res) => {
    res.status(404).json({ error: 'Endpoint not found' });
});

// Graceful shutdown
const gracefulShutdown = async (signal) => {
    try {
        log('info', `Received ${signal}, initiating graceful shutdown...`);
        isShuttingDown = true;

        // Close SSE connections
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

        // Close server
        if (server && server.close) {
            await new Promise((resolve) => server.close(resolve));
            log('info', 'HTTP server closed');
        }

        // Close MongoDB connection
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

// Handle shutdown signals and errors
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (error) => {
    log('error', 'Uncaught Exception:', error && error.stack ? error.stack : error);
    // attempt graceful shutdown, then exit
    gracefulShutdown('uncaughtException').catch(() => process.exit(1));
});
process.on('unhandledRejection', (reason, promise) => {
    log('error', 'Unhandled Rejection at:', promise, 'reason:', reason);
    // attempt graceful shutdown, then exit
    gracefulShutdown('unhandledRejection').catch(() => process.exit(1));
});

// Server startup
async function startServer() {
    try {
        await initMongo();

        server = app.listen(PORT, HOST, () => {
            log('info', `🚀 Server listening on http://${HOST}:${PORT}/`);
            log('info', `Environment: ${NODE_ENV}`);
            log('info', `Database: ${DB_NAME} at ${MONGODB_URI}`);
            log('info', 'Health check: GET /health');
            log('info', 'Database viewer: GET /database-viewer');
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
