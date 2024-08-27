const express = require('express');
const { RateLimiterRedis } = require('rate-limiter-flexible');
const Queue = require('bull');
const fs = require('fs');
const path = require('path');
const Redis = require('redis');

// Initialize Redis client
const redisClient = Redis.createClient();

const app = express();
const logFilePath = path.join(__dirname, 'task_log.txt');

// Middleware to parse JSON body
app.use(express.json());

// Rate limiter configuration (1 task per second, 20 tasks per minute)
const rateLimiter = new RateLimiterRedis({
    storeClient: redisClient,
    points: 20,  // 20 tasks per minute
    duration: 60,  // Per minute
    blockDuration: 1, // Block for 1 second if rate limit exceeded
    keyPrefix: 'user_rate_limit',
});

const taskQueue = new Queue('taskQueue', {
    redis: {
        host: '127.0.0.1',
        port: 6379,
    },
});

// The task function provided by you
async function task(user_id) {
    const logMessage = `${user_id} - task completed at - ${Date.now()}\n`;
    console.log(logMessage);

    // Store the log message in a log file
    fs.appendFileSync(logFilePath, logMessage);
}

// Task processing function that integrates the provided task function
taskQueue.process(async (job) => {
    const { userId } = job.data;

    // Call the provided task function
    await task(userId);
});

// Route to handle tasks
app.post('/task', async (req, res) => {
    const { userId } = req.body;  // Extract userId from JSON body

    if (!userId) {
        return res.status(400).send('userId is required');
    }

    try {
        // Consume 1 point, which represents a task request
        await rateLimiter.consume(userId, 1);

        // Add task to queue if within rate limits
        taskQueue.add({ userId });

        res.status(200).send('Task added to queue');
    } catch (rateLimiterRes) {
        if (rateLimiterRes.remainingPoints === 0) {
            res.status(429).send('Rate limit exceeded');
        } else {
            // If the request is within rate limits but blocked by 1-second limit, add to queue
            taskQueue.add({ userId });
            res.status(200).send('Task added to queue');
        }
    }
});

// Start the server
app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
