const net = require('net');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

let cacheDir = "cache";
const version = "4.6";
const port = 8125;
const PROTOCOL_VERSION = 255;
const PROTOCOL_VERSION_SIZE = 2;

//
const d2h = d => d.toString(16);
const h2d = h => parseInt(h, 16);

// Little endian
const readUInt32 = data => h2d(data.toString('ascii', 0, 8));

const writeUInt32 = (indata, outbuf) => {
	let str = d2h(indata);
	for (let i = 8 - str.length; i > 0; i--) {
		str = '0' + str;
	}
	outbuf.write(str, 0, 'ascii');
};

// All numbers in js is 64 floats which means
// man 2^52 is the max integer size that does not
// use the exponent. This should not be a problem.
const readUInt64 = data => h2d(data.toString('ascii', 0, 16));

const writeUInt64 = (indata, outbuf) => {
	let str = d2h(indata);
	for (let i = 16 - str.length; i > 0; i--) {
		str = '0' + str;
	}
	outbuf.write(str, 0, 'ascii');
};

const readHex = (len, data) => {
	let res = '';
	let tmp;
	for (let i = 0; i < len; i++) {
		tmp = data[i];
		res += tmp < 0x10 ? '0' + tmp.toString(16) : tmp.toString(16);
	}
	return res;
};

const uuid = () => 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
	const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
	return v.toString(16);
});

const LOG_LEVEL = 10;
const ERR = 2;
const WARN = 4;
const INFO = 5;
const DBG = 6;

let log = (lvl, msg) => {
	if (LOG_LEVEL < lvl)
		return;
	console.log(msg);
};

const CMD_GET = 'g'.charCodeAt(0);
const CMD_PUT = 'p'.charCodeAt(0);
const CMD_GETOK = '+'.charCodeAt(0);
const CMD_GETNOK = '-'.charCodeAt(0);

const UINT_SIZE = 8;			// hex encoded
const LEN_SIZE = 16;			// hex
const HASH_SIZE = 16;			// bin
const GUID_SIZE = 16;			// bin
const ID_SIZE = GUID_SIZE + HASH_SIZE;	// bin
const CMD_SIZE = 1;			// bin

let gTotalDataSize = -1;
let maxCacheSize = 1024 * 1024 * 1024 * 50;
const freeCacheSizeRatio = 0.9;
const freeCacheSizeRatioWriteFailure = 0.8;

let gFreeingSpaceLock = 0;


const maximumHeapSocketBufferSize = 1024 * 1024 * 25;

const walkDirectory = (dir, done) => {
	let results = [];
	fs.readdir(dir, (err, list) => {
		if (err)
			return done(err);

		let pending = list.length;
		if (pending === 0)
			done (null, results);
		else {
			list.forEach(file => {
				file = dir + '/' + file;
				fs.stat(file, (err, stat) => {
					if (!err && stat) {
						if (stat.isDirectory()) {
							walkDirectory(file, (err, res) => {
								results = results.concat(res);
								if (!--pending)
									done(null, results);
							});
						} else {
							results.push({ name : file, date : stat.mtime, size : stat.size });
							if (!--pending)
								done(null, results);
						}
					} else {
						log(DBG, "Freeing space failed to extract stat from file: " + name);
					}
				});
			});
		}
	});
};

const lockFreeSpace = () => {
	gFreeingSpaceLock++;
};

const unlockFreeSpace = () => {
	gFreeingSpaceLock--;
	if (gFreeingSpaceLock === 0)
	{
		log(DBG, "Completed freeing cache space. Current size: " + gTotalDataSize);
	}
};

const freeSpaceOfFile = removeParam => {
	lockFreeSpace();

	fs.unlink (removeParam.name, err => {
		if (err) {
			log(DBG, "Freeing cache space file can not be accessed: " + removeParam.name + err);

			// If removing the file fails, then we have to adjust the total data size back
			gTotalDataSize += removeParam.size;
		} else {
			log(DBG, "  Did remove: " + removeParam.name + ". ("  + removeParam.size + ")");
		}

		unlockFreeSpace ();
	});
};

let freeSpace = freeSize => {
	if (gFreeingSpaceLock !== 0) {
		log(DBG, "Skip free cache space because it is already in progress: " + gFreeingSpaceLock);
		return;
	}

	lockFreeSpace();

	log(DBG, "Begin freeing cache space. Current size: " + gTotalDataSize);

	walkDirectory (cacheDir, (err, files) => {
		if (err)
			throw err;

		files.sort ((a, b) => {
			if (a.date === b.date)
				return 0;
			else if (a.date < b.date)
				return 1;
			else
				return -1;
		});

		while (gTotalDataSize > freeSize) {
			const remove = files.pop();
			if (!remove)
				break;

			gTotalDataSize -= remove.size;
			freeSpaceOfFile (remove);
		}
		unlockFreeSpace ();
	});
}, size;

const getDirectorySize = dir => {
	size = 0;
	fs.readdirSync(dir).forEach(file => {
		file = dir + "/" + file;
		const stats = fs.statSync(file);
		if (stats.isFile())
			size += stats.size;
		else
			size += getDirectorySize(file);
	});
	return size;
};

const getFreeCacheSize = () => freeCacheSizeRatio * maxCacheSize;

const ShouldIgnoreFile = file => {
	if (file.length <= 2) return true; // Skip "00" to "ff" directories
	if (file.length >= 4 && file.toLowerCase().indexOf("temp") === 0) return true; // Skip Temp directory
	if (file.length >= 9 && file.toLowerCase().indexOf(".ds_store") === 0) return true; // Skip .DS_Store file on MacOSX
	if (file.length >= 11 && file.toLowerCase().indexOf("desktop.ini") === 0) return true; // Skip Desktop.ini file on Windows
	return false;
};

// To make sure we are not working on a directory which is not cache data, and we delete all the files in it
// during LRU.
const checkCacheDirectory = dir => {
	size = 0;
	fs.readdirSync(dir).forEach(file => {
		if (!ShouldIgnoreFile (file)) {
			throw new Error ("The file "+dir+"/"+file+" does not seem to be a valid cache file. Please delete it or choose another cache directory.");
		}
	});
};

const initCache = () => {
	if (!fs.existsSync(cacheDir))
		fs.mkdirSync (cacheDir, 0o777);
	checkCacheDirectory (cacheDir);
	gTotalDataSize = getDirectorySize (cacheDir);

	log (DBG, "Cache Server directory " + path.resolve (cacheDir));
	log (DBG, "Cache Server size " + gTotalDataSize);
	log (DBG, "Cache Server max cache size " + maxCacheSize);

	if (gTotalDataSize > maxCacheSize)
		freeSpace (getFreeCacheSize ());
};

const addFileToCache = bytes => {
	gTotalDataSize += bytes;
	log(DBG, "Total Cache Size " + gTotalDataSize);

	if (gTotalDataSize > maxCacheSize)
		freeSpace (getFreeCacheSize ());
};



const getCachePath = (guid, hash, create) => {
	const dir = cacheDir + "/" + hash.substring(0, 2);
	if (create) {
		log(DBG, "Create directory " + dir);
		fs.existsSync(dir) || fs.mkdirSync(dir, 0o777);
	}
	return dir +"/"+ guid + "-" + hash;
};
/*
Protocol
========

client --- (version <uint32>) --> server      (using version)
client <-- (version <uint32>) --- server      (echo version if supported or 0)

# request cached item
client --- 'g' (id <128bit GUID><128bit HASH>) --> server
client <-- '+' (size <uint64>) (id <128bit GUID><128bit HASH>) + size bytes  --- server    (found in cache)
client <-- '-' (id <128bit GUID><128bit HASH>) --- server    (not found in cache)

# put cached item
client  -- 'p' size <uint64> id <128bit GUID><128bit HASH> + size bytes --> server

*/

const sendNextGetFile = socket => {
	if (socket.getFileQueue.length === 0)
	{
		socket.activeGetFile = null;
		return;
	}
	const next = socket.getFileQueue.pop();
	const resbuf = next.buffer;
	const file = fs.createReadStream(next.cacheStream);
	// make sure no data is read and lost before we have called file.pipe().
	file.pause();
	socket.activeGetFile = file;
	const errfunc = () => {
		const buf = Buffer.alloc(CMD_SIZE + ID_SIZE);
		buf[0] = CMD_GETNOK;
		const id_offset = CMD_SIZE + LEN_SIZE;
		resbuf.copy(buf, 1, id_offset, id_offset + ID_SIZE);
		try {
			socket.write(buf);
		} catch (err) {
			log(ERR, "Error sending file data to socket " + err);
		} finally {
			if (socket.isActive) {
				sendNextGetFile(socket);
			} else {
				log(ERR, "Socket close, close active file");
				file.close();
			}
		}
	};

	file.on ('close', () => {
		socket.activeGetFile = null;
		if (socket.isActive) {
			sendNextGetFile(socket);
		}
	});

	file.on('open', fd => {
		fs.fstat(fd, (err, stats) => {
			if (err)
				errfunc();
			else {
				resbuf[0] = CMD_GETOK;
				writeUInt64(stats.size, resbuf.slice(1));
				log(INFO, "found: "+next.cacheStream + " size:" + stats.size);

				// The ID is already written
				try {
					socket.write(resbuf);
					file.resume();
					file.pipe(socket, { end: false });
				} catch (err) {
					log(ERR, "Error sending file data to socket " + err);
					file.close();
				}
			}
		});
	});
	file.on('error', errfunc);
};

const handleData = (socket, data) => {
	let hash;
	let guid;
	let size;
	let sizeMB;
	let buf;
	// There is pending data, add it to the data buffer
	if (socket.pendingData != null)
	{
		buf = Buffer.alloc(data.length + socket.pendingData.length);
		socket.pendingData.copy (buf, 0, 0);
		data.copy (buf, socket.pendingData.length, 0);
		data = buf;
		socket.pendingData = null;
	}

	while (true) {
		assert(socket.pendingData == null, "pending data must be null")

		if (data.length > maximumHeapSocketBufferSize * 2) {
			sizeMB = data.length / (1024 * 1024);
			log(DBG, "incoming data exceeds buffer limit " + sizeMB + " mb.");
		}

		// Get the version as the first thing
		let idx = 0;
		if (!socket.protocolVersion) {
			if (data.length < PROTOCOL_VERSION_SIZE) {
				// We need more data
				socket.pendingData = data;
				return false;
			}

			socket.protocolVersion = readUInt32(data);
			log(INFO, "Client protocol version", socket.protocolVersion);
			buf = Buffer.alloc(UINT_SIZE);
			if (socket.protocolVersion === PROTOCOL_VERSION) {
				writeUInt32(socket.protocolVersion, buf);
				socket.write(buf);
				idx += PROTOCOL_VERSION_SIZE;
			} else {
				log (ERR, "Bad Client protocol version");
				writeUInt32(0, buf);
				if (socket.isActive)
					socket.write(buf);
				socket.end();
				socket.forceQuit = true;
				return false;
			}
		}

		// Write a a file to a temp location and move it in place when it has completed
		if (socket.activePutFile != null) {
			size = data.length;
			if (socket.bytesToBeWritten < size)
				size = socket.bytesToBeWritten;
			socket.activePutFile.write (data.slice(0, size), "binary");
			socket.bytesToBeWritten -= size;

			// If we have written all data for this file. We can close the file.
			if (socket.bytesToBeWritten <= 0) {
				socket.activePutFile.on('close', function() {
					fs.stat(socket.activePutTarget, function (statsErr, stats) {
						// We are replacing a file, we need to subtract this from the totalFileSize
						let size = 0;
						if (!statsErr && stats)
						{
							size = stats.size;
						}

						fs.rename(socket.tempPath, socket.activePutTarget, function (err)
						{
							if (err)
							{
								log(DBG, "Failed to move file in place " + socket.tempPath + " to " + socket.activePutTarget + err);
							}
							else
							{
								addFileToCache (socket.totalFileSize - size);
							}

							socket.activePutTarget = null;
							socket.totalFileSize = 0;

							if (socket.isPaused && socket.isActive)
								socket.resume();
						});
					});
				});

				socket.activePutFile.end();
				socket.activePutFile.destroySoon();
				socket.activePutFile = null;

				data = data.slice(size);
				continue;
			}
				// We need more data to write the file completely
			// Return and wait for the next call to handleData to receive more data.
			else {
				return true;
			}
		}
		//  Serve a file from the cache server to the client
		else if (data[idx] === CMD_GET) {
			///@TODO: What does this do?
			if (data.length < CMD_SIZE + ID_SIZE)
			{
				socket.pendingData = data;
				return true;
			}
			idx += 1;
			guid = readHex(GUID_SIZE, data.slice(idx));
			hash = readHex(HASH_SIZE, data.slice(idx+GUID_SIZE));
			log(DBG, "Get " + guid + "_" + hash);

			const resbuf = Buffer.alloc(CMD_SIZE + LEN_SIZE + ID_SIZE);
			data.copy(resbuf, CMD_SIZE + LEN_SIZE, idx, idx + ID_SIZE); // copy guid+hash

			socket.getFileQueue.unshift( { buffer : resbuf, cacheStream : getCachePath(guid, hash, false) } );

			if (!socket.activeGetFile) {
				sendNextGetFile(socket);
			}

			data = data.slice(idx+GUID_SIZE+HASH_SIZE);
			continue;
		}
		// Put a file from the client to the cache server
		else if (data[idx] === CMD_PUT) {
			/// * We don't have enough data to start the put request. (wait for more data)
			if (data.length < CMD_SIZE + LEN_SIZE + ID_SIZE)
			{
				socket.pendingData = data;
				return true;
			}

			// We have not completed writing the previous file
			if (socket.activePutTarget != null) {
				// If we are using excessive amounts of memory
				if (data.length > maximumHeapSocketBufferSize) {
					sizeMB = data.length / (1024 * 1024);
					log(DBG, "Pausing socket for in progress file to be written in order to keep memory usage low... " + sizeMB + " mb");
					socket.isPaused = true;
					if (socket.isActive)
						socket.pause();
				}

				// Keep the data in pending for the next handleData call
				socket.pendingData = data;
				return true;
			}

			idx += 1;
			size = readUInt64(data.slice(idx));
			guid = readHex(GUID_SIZE, data.slice(idx + LEN_SIZE));
			hash = readHex(HASH_SIZE, data.slice(idx + LEN_SIZE + GUID_SIZE));
			log(DBG, "PUT " + guid + "_" + hash + " (size " + size + ")");

			socket.activePutTarget = getCachePath(guid, hash, true);
			socket.tempPath = cacheDir + "/Temp"+uuid();
			socket.activePutFile = fs.createWriteStream(socket.tempPath);

			socket.activePutFile.on ('error', err => {
				// Test that this codepath works correctly
				log(ERR, "Error writing to file " + err + ". Possibly the disk is full? Please adjust --cacheSize with a more accurate maximum cache size");
				freeSpace (gTotalDataSize * freeCacheSizeRatioWriteFailure);
				socket.destroy();
				return false;
			});
			socket.bytesToBeWritten = size;
			socket.totalFileSize = size;

			data = data.slice(idx+LEN_SIZE+GUID_SIZE+HASH_SIZE);
			continue;
		}
		// We need more data to write the file completely
		return true;
	}
};

const server = net.createServer(socket => {
	socket["getFileQueue"] = [];
	socket["protocolVersion"] = null;
	socket["activePutFile"] = null;
	socket["activeGetFile"] = null;
	socket["pendingData"] = null;
	socket["bytesToBeWritten"] = 0;
	socket["totalFileSize"] = 0;
	socket.isPaused = 0;
	socket["isActive"] = true;
	socket["forceQuit"] = false;

	socket.on('data', function (data) {
		socket["isActive"] = true;
		handleData(socket, data);
	});
	socket.on('close', function (had_errors) {
		log(ERR, "Socket closed");
		socket["isActive"] = false;
		const checkFunc = () => {
			const data = Buffer.alloc(0);
			if (handleData(socket, data)) {
				setTimeout(checkFunc, 1);
			}
		};

		if (!had_errors && !socket["forceQuit"])
			checkFunc();
	});
	socket.on('error', function (err) {
		log(ERR, "Socket error " + err);
	});
});



exports.log = log;

exports.ERR = ERR;
exports.WARN = WARN;
exports.INFO = INFO;
exports.DBG = DBG;

/**
 * Get version
 *
 * @return version
 */
exports.getVersion = () => version

/**
 * Get server port
 *
 * @return server port
 */
exports.getPort = () => port

/**
 * Get cache directory
 *
 * @return cache directory
 */
exports.getCacheDir = () => path.resolve(cacheDir)

/**
 * start the cache server
 *
 * @param a_cacheSize maximum cache size
 * @param a_path cache path
 * @param a_logFn log function (optional)
 * @param a_errCallback error callback (optional)
 */
exports.start = (a_cacheSize, a_path, a_logFn, a_errCallback) => {
	if (a_logFn) {
		log = a_logFn;
	}
	maxCacheSize = a_cacheSize || maxCacheSize;
	cacheDir = a_path || cacheDir;
	initCache ();
	server.on ('error', function (e) {
		if (e.code === 'EADDRINUSE') {
			log (ERR, 'Port ' + port +' is already in use...');
			if (a_errCallback) {
				a_errCallback (e);
			}
		}
	});
	server.listen (port);
};
