const cluster = require('cluster');
const os = require('os');
const numCPUs = os.cpus().length;

if (cluster.isMaster) {
    for (let i = 0; i < Math.min(2, numCPUs); i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
        cluster.fork();  // Restart the worker if it dies
    });
} else {
    require('./server');
}