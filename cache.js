const logger = require("winston");

logger.level = process.env.LOG_LEVEL || "info";

/**
 * Use this function for getting the redis conection 
 * 
 * Retry strategy: log errors if redis connection is refused. This can happen
 * if redis went down or no redis server exists on the given host and port. 
 * Log this kind of errors. MAY BE WE NEED TO WRITE AN EVENT ON THIS ERROR?
 * 
 * The maximum allowed retry time on the redis is redisTotalRetryTime. We can 
 * either use this to exit after this time is exceeeded or we can use attempts made
 * to get the connection. 
 * 
 * If the redisTotalRetryTime is less than (redisAllowedRetryAttempts * redisReconnectWaitTime)
 * then the priority is taken by redisTotalRetryTime else the later takes the priority.
 * 
 * Both these configs are added here and can be changed using the config.
 * 
 */
function getRedisConnection(config) {
    const redis = require('redis');
    const redisHost = process.env.redis_host || "127.0.0.1";
    const redisPort = process.env.redis_port || "6379";
    let redisTotalRetryTime = 1000 * 60 * 2; // defaulting the total retry time to 2 minutes
    let redisAllowedRetryAttempts = 10;
    let redisReconnectWaitTime = 3000; // 3sec

    if (config && config.redisConfiguration) {
        redisTotalRetryTime = config.redisConfiguration.redisTotalRetryTime;
        redisAllowedRetryAttempts = config.redisConfiguration.redisAllowedRetryAttempts;
        redisReconnectWaitTime = config.redisConfiguration.redisReconnectWaitTime;
    }

    let redisInitiationTime = new Date().getTime();

    let client = redis.createClient({
        host: redisHost,
        port: redisPort,
        retry_strategy: function(options) {
            if (options.error && options.error.code === 'ECONNREFUSED') {
                logger.log("error", "The server at host:" + redisHost + " and port:" + redisPort + " is refusing connection!", new Error('The server refused the connection'));
            }
            if (options.total_retry_time > redisTotalRetryTime) {
                logger.log("error", "Total retry time exhausted!!", new Error('Retry time exhausted'));
                client.emit('error', new Error('Retry time exhausted'));
            }
            if (options.attempt > redisAllowedRetryAttempts) {
                logger.log("error", "Total retry attempts exhausted!!", new Error('Retry Attempts Exceeded'));
                client.emit('error', new Error('Retry Attempts Exceeded'));
            }
            // reconnect after 
            return redisReconnectWaitTime;
        }
    });

    return new Promise((resolve, reject) => {
        client.on('error', function(err) {
            logger.log("error", "All the attempts are exhausted or the total retry time is finished", err);
            let failure = {status: "failed", type:"REDIS_ERROR", code:"redis_attempts_exhausted", message:"All the attempts are exhausted or the total retry time is finished"};
            client.quit();
            reject(failure);
        });

        client.on('connect', function() {
            logger.log("debug", "Time took to initialize redis:", (new Date().getTime() - redisInitiationTime) + " ms");
            resolve(client);
        });
    })
}

/**
 * This function returns a redis connection locker. 
 * The default usage of setnx cannot be used as the redis community doesnt recommend it. https://redis.io/commands/setnx
 * When we move to redis cluster mechanism this method may need to change. Currently we are supporting only standalone redis.
 * The config gives us the retry count for locking and the delay between attempts
 * The clients is an array which can take multiple clients.
 * 
 */
function getRedisConnectionLocker(config, clients) {
    var Redlock = require('redlock');
    let lockAttempts = 3;
    let lockRetryDelay = 400; //in ms

    if (config && config.redisConfiguration) {
        lockAttempts = config.redisConfiguration.lockAttempts;
        lockRetryDelay = config.redisConfiguration.lockRetryDelay; // this value needs to be in ms
    }

    let connectionLock = new Redlock(clients, {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01, // time in ms

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount: lockAttempts,

        // the time in ms between attempts
        retryDelay: lockRetryDelay, // time in ms

        // the max time in ms randomly added to retries
        // to improve performance under high contention
        // see https://www.awsarchitectureblog.com/2015/03/backoff.html
        retryJitter: 200 // time in ms
    });

    return new Promise((resolve, reject) => {
        connectionLock.on('clientError', function(err) {
            logger.log("error", "failed to get a client connection!", err);
            let failure = {status: "failed", type:"REDIS_ERROR", code:"redis_lock_client_error", message:"All the attempts are exhausted or the total retry time is finished"};
            reject(failure);
        });

        resolve(connectionLock);
    });
}

/**
 * gets a redis connection and then a lock connection and then tries to lock the given lockName.
 * Any level failure will initiate fail over on each level and quits all the connections.
 * 
 */
function createRedisLockOnLockName(config, lockName) {
    return new Promise((resolve, reject) => {
        logger.log("debug", "trying to get redis connection");
        // get redis connection to lock the incoming request
        getRedisConnection(config).then((connection) => {
            logger.log("debug", "Obtained redis connection! Trying to create redis lock connection!");
            //get redis lock connection
            getRedisConnectionLocker(config, [connection]).then((connectionLock) => {
                logger.log("debug", "Obtained redis Lock connection! Trying to lock on given lock name !", lockName);
                let lockExpiry = (config && config.redisConfiguration && config.redisConfiguration.lockExpiry) || 30000; // in ms
                connectionLock.lock(lockName, lockExpiry).then((lockedResource) => {
                    logger.log("debug", "Obtained redis Lock!");
                    resolve({ redisClient: connection, redisLockClient: connectionLock, lockedResource: lockedResource });
                }).catch((unableToLockException) => {
                    logger.log("error", "unable to get lock on the given lock name", { "error": unableToLockException, "lockName": lockName });
                    let failure = {status:"failed", type:"REDIS_ERROR", code:"redis_duplicate_lock", stack: unableToLockException};
                    connectionLock.quit();
                    connection.quit();
                    reject(failure);
                });
            }).catch((redisLockConnectionException) => {
                if (redisLockConnectionException.name === 'RedisException') {
                    connection.quit();
                    reject(redisLockConnectionException);
                }
                else {
                    logger.log("error", "unable to get lock connection", { "error": redisLockConnectionException });
                    connection.quit();
                    reject(redisLockConnectionException);
                }
            });
        }).catch((redisConnectionException) => {
            reject(redisConnectionException);
        });
    });
}

/**
 * Mandatory function after carrying out respective event functionalities.
 * This function will close all the existing redis and lock connections and also unlock the resource.
 * 
 * connections contains { redisClient: connection, redisLockClient: connectionLock, lockedResource: lockedResource }
 */
function closeConnections(connections) {
    return new Promise((resolve, reject) => {
        if (connections) {
            connections.redisLockClient.quit();
            connections.redisClient.quit();
            resolve({ "status": "success" });
        }
        else {
            reject({ "status": "failed", type:"REDIS_ERROR", code:"no_connections_exist", "message": "no connections exist!!" });
        }
    });
}

function unlockRedisLock(lockedResource) {
    return new Promise((resolve, reject) => {
        if(!lockedResource)
            reject({"status": "failed", type:"REDIS_ERROR", code:"no_locked_resource", "message": "locked resource doesnt exist"});
        logger.log("debug", "Trying to unlock locked resource");
        lockedResource.unlock().then((unlockedResource) => {
            logger.log("debug", "unlocked resource!", unlockedResource);
            resolve({ "status": "success" });
        }).catch((unlockError) => {
            logger.log("error", "failed to release lock! The lock will eventually expire!!", unlockError);
            resolve({ "status": "success", "message": "unlock failed!" });
        });
    });
}

module.exports = {
    closeConnections,
    createRedisLockOnLockName,
    getRedisConnection,
    getRedisConnectionLocker,
    unlockRedisLock
};