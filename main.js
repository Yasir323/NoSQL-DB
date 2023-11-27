const net = require("net");
const { EventEmitter } = require("stream");

const db = {};
const HOST = "127.0.0.1";
const PORT = 50505;
const requestQueue = [];
let processing = false;


const stats = {
    PUT: {success: 0, error: 0},
    PUTLIST: {success: 0, error: 0},
    GET: {success: 0, error: 0},
    GETLIST: {success: 0, error: 0},
    GETALL: {success: 0, error: 0},
    INCREMENT: {success: 0, error: 0},
    APPEND: {success: 0, error: 0},
    DELETE: {success: 0, error: 0},
    STATS: {success: 0, error: 0},
};

const valid_commands = ["PUT", "GET", "PUTLIST", "GETLIST", "GETALL", "INCREMENT", "APPEND", "DELETE", "STATS", "EXIT"];

// Callbacks for commands
const handlePut = (key, value) => {
    db[key] = value;
    return { isSuccessful: true, output: `Key [${key}] set to value [${value}]` };
};

const handleGet = (key) => {
    if (!db.hasOwnProperty(key)) {
        return { isSuccessful: false, output: `ERROR: Key [${key}] not found.`};
    } else {
        return { isSuccessful: true, output: db[key]};
    }
};
  
const handlePutList = (key, value) => {
    value = value.split(",");
    return handlePut(key, value);
};
  
const handleGetList = (key) => {
    let returnValue = handleGet(key);
    let { isSuccessful: exists, output: value } = returnValue;
  
    if (!exists) {
        return returnValue;
    } else if (!Array.isArray(value)) {
        return { isSuccessful: false, output: `ERROR: Key [${key}] contains non-list value ([${value}])` };
    }
  
    return returnValue;
};

const handleGetAll = () => {
    return { isSuccessful: true, output: JSON.stringify(db) }
};

const handleIncrement = (key) => {
    let returnValue = handleGet(key);
    let { isSuccessful: exists, output: value } = returnValue;
  
    if (!exists) {
        return returnValue;
    } else if (typeof value !== 'number') {
        return { isSuccessful: false, output: `ERROR: Key [${key}] contains non-int value ([${value}])` };
    } else {
        db[key] = value + 1;
        return { isSuccessful: true, output: `Key [${key}] incremented` };
    }
};
  
const handleAppend = (key, value) => {
    let returnValue = handleGet(key);
    let { isSuccessful: exists, output: valueStored } = returnValue;
    
    if (!exists) {
        return returnValue;
    } else if (!Array.isArray(valueStored)) {
        return { isSuccessful: false, output: `ERROR: Key [${key}] contains non-list value ([${value}])` };
    } else {
        value = JSON.parse(JSON.stringify(value));
        db[key].push(value);
        return { isSuccessful: true, output: `Key [${key}] had value [${value}] appended` };
    }
};
  
const handleDelete = (key) => {
    if (!db.hasOwnProperty(key)) {
        return { isSuccessful: false, output: `ERROR: Key [${key}] not found and could not be deleted` };
    } else {
        delete db[key];
        return { isSuccessful: true, output: `Key [${key}] removed successfully.` };
    }
};
  
const handleStats = () => {
    return { isSuccessful: true, output: JSON.stringify(stats) };
};

const commandHandlers = {
    PUT: handlePut,
    PUTLIST: handlePutList,
    GET: handleGet,
    GETLIST: handleGetList,
    GETALL: handleGetAll,
    INCREMENT: handleIncrement,
    APPEND: handleAppend,
    DELETE: handleDelete,
    STATS: handleStats
};

class CommandNotValidError extends Error {
    constructor(message) {
        super(message);
        this.name = 'CommandNotValidError';
    }
}

class WrongNumberOfArgumentsError extends Error {
    constructor(message) {
        super(message);
        this.name = 'WrongNumberOfArgumentsError';
    }
}

const parseMessage = (data) => {
    data = data.trim().split(';');
    const command = data[0].toUpperCase();
  
    if (!valid_commands.includes(command)) {
        throw new CommandNotValidError('Invalid command');
    }

    let key = '';
    let value = '';

    if (["STATS", "EXIT", "GETALL"].includes(command)) {
        if (data.length !== 1) {
            throw new WrongNumberOfArgumentsError(`Wrong number of arguments for ${command}`);
        }
    } else {
        if (["GET", "GETLIST", "INCREMENT", "DELETE"].includes(command)) {
            if (data.length !== 2) {
                throw new WrongNumberOfArgumentsError(`Wrong number of arguments for ${command}`);
            }
            key = data[1];
        } else {
            if (data.length !== 3) {
                throw new WrongNumberOfArgumentsError(`Wrong number of arguments for ${command}`);
            }
            key = data[1];
            value = JSON.parse(data[2]); // Assuming it's JSON data
        }
    }

    return [command, key, value];
};

const getResponse = (command, key, value) => {
    let response;
    if (command === 'STATS') {
        response = commandHandlers[command]();
    } else if (['GET', 'GETLIST', 'INCREMENT', 'DELETE'].includes(command)) {
        response = commandHandlers[command](key);
    } else {
        response = commandHandlers[command](key, value);
    }
    return response;
}

const updateStats = (command, status) => {
    if (valid_commands.includes(command) && command !== "EXIT") {
        if (status) stats[command]["success"] += 1;
        else stats[command]["error"] += 1;
    }
};

const processRequests = () => {
    if (!processing && requestQueue.length > 0) {
        processing = true;
        const { socket, data } = requestQueue.shift();
        let command, key, value, response;
        try {
            [command, key, value] = parseMessage(data.toString("utf8"));
            if (command === "EXIT") socket.emit("end");
            response = getResponse(command, key, value);
            socket.write(`${response.isSuccessful};${response.output}\n`);
            statsUpdateEvent.emit("update", command, true);
        } catch (error) {
            response = { isSuccessful: false, output: error.message };
            socket.write(`${response.isSuccessful};${response.output}\n`);
            statsUpdateEvent.emit("update", command, false);
        } finally {
            processing = false;
        }
        process.nextTick(processRequests);
    }
}

const statsUpdateEvent = new EventEmitter();
statsUpdateEvent.on("update", updateStats);
const server = net.createServer((socket) => {
    console.log("Connect to NoSQL Database");
    socket.on("data", (data) => {
        requestQueue.push({ socket, data });
        if (!processing) {
            process.nextTick(processRequests);
        }
    });

    socket.on("end", () => {
        socket.destroy();
        console.log("Client disconnected");
    });

    socket.on("error", (err) => {
        console.error(`Socket error: ${err.message}`);
    });
});

function main() {
    server.listen(PORT, HOST, () => {
        console.log(`Server listening on ${HOST}:${PORT}`);
    });
}

main();
