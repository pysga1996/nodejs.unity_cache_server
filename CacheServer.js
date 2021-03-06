const net = require('net');
const fs = require('fs');
const path = require('path');
const assert = require('assert');

let cacheDir = "cache5.0";
const version = "5.3";
let port = 8126;
const PROTOCOL_VERSION = 254;
const PROTOCOL_VERSION_MIN_SIZE = 2;
let verificationFailed = false;
let verificationNumErrors = 0;

const d2h = d => d.toString(16);
const h2d = h => parseInt(h, 16);

// Little endian
const readUInt32 = data => h2d(data.toString('ascii', 0, 8));

const writeUInt32 = (indata, outbuf) => {
	let str = d2h(indata);
	for (let i = 8 - str.length; i > 0; i--) {
		str = '0' + str;
	}
	outbuf.write (str, 0, 'ascii');
};

// All numbers in js is 64 floats which means
// man 2^52 is the max integer size that does not
// use the exponent. This should not be a problem.
const readUInt64 = data => h2d(data.toString('ascii', 0, 16));

const writeUInt64 = (indata, outbuf) => {
	let str = d2h(indata);
	for (let i = 16 - str.length; i > 0; i--)
	{
		str = '0' + str;
	}
	outbuf.write (str, 0, 'ascii');
};

const readHex = (len, data) => {
	let res = '';
	let tmp;
	for (let i = 0; i < len; i++)
	{
		tmp = data[i];
		tmp = ( (tmp & 0x0F) << 4) | ( (tmp >> 4) & 0x0F );
		res += tmp < 0x10 ? '0' + tmp.toString (16) : tmp.toString (16);
	}
	return res;
};

const uuid = () => 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,
	c => {
		const r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
		return v.toString(16);
	});

const LOG_LEVEL = 4; //Required for integration tests which scan for log messages
const ERR = 1;
const WARN = 2;
const INFO = 3;
const TEST = 4;
const DBG = 5;

let log = (lvl, msg) => {
	if (LOG_LEVEL < lvl)
		return;

	console.log (msg);
};

const CMD_QUIT = 'q'.charCodeAt(0);

const CMD_GET = 'g'.charCodeAt(0);
const CMD_PUT = 'p'.charCodeAt(0);
const CMD_GETOK = '+'.charCodeAt(0);
const CMD_GETNOK = '-'.charCodeAt(0);

const TYPE_ASSET = 'a'.charCodeAt(0);
const TYPE_INFO = 'i'.charCodeAt(0);
const TYPE_RESOURCE = 'r'.charCodeAt(0);

const CMD_TRX = 't'.charCodeAt(0);
const TRX_START = 's'.charCodeAt(0);
const TRX_END = 'e'.charCodeAt(0);

const CMD_INTEGRITY = 'i'.charCodeAt(0);
const CMD_CHECK = 'c'.charCodeAt(0);
const OPT_VERIFY = 'v'.charCodeAt(0);
const OPT_FIX = 'f'.charCodeAt(0);

const UINT32_SIZE = 8;					// hex encoded
const UINT64_SIZE = 16;					// hex
const HASH_SIZE = 16;						// bin
const GUID_SIZE = 16;						// bin
const ID_SIZE = GUID_SIZE + HASH_SIZE;	// bin
const CMD_SIZE = 2;						// bin

let gTotalDataSize = -1;
let maxCacheSize = 1024 * 1024 * 1024 * 50; // 50Go
const freeCacheSizeRatio = 0.9;
const freeCacheSizeRatioWriteFailure = 0.8;

let gFreeingSpaceLock = 0;

const walkDirectory = (dir, done) => {
	let results = [];
	fs.readdir (dir, (err, list) => {
		if (err)
			return done (err);

		let pending = list.length;
		if (pending === 0) {
			done (null, results);
		} else {
			list.forEach (file => {
				file = dir + '/' + file;
				fs.stat (file, function (err, stat) {
					if (!err && stat) {
						if (stat.isDirectory ()) {
							walkDirectory (file, function (err, res) {
								results = results.concat (res);
								if (!--pending)
									done (null, results);
							});
						} else {
							results.push ({ name : file, date : stat.mtime, size : stat.size });
							if (!--pending) {
								done (null, results);
							}
						}
					} else {
						log (DBG, "Freeing space failed to extract stat from file.");
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
	if (gFreeingSpaceLock === 0) {
		log (TEST, "Completed freeing cache space. Current size: " + gTotalDataSize);
	}
};

const freeSpaceOfFile = removeParam => {
	lockFreeSpace ();
	fs.unlink (removeParam.name, err => {
		if (err) {
			log (DBG, "Freeing cache space file can not be accessed: " + removeParam.name + err);

			// If removing the file fails, then we have to adjust the total data size back
			gTotalDataSize += removeParam.size;
		} else {
			log (TEST, " Did remove: " + removeParam.name + ". (" + removeParam.size + ")");
		}
		unlockFreeSpace ();
	});
};

const freeSpace = freeSize => {
	if (gFreeingSpaceLock !== 0) {
		log (DBG, "Skip free cache space because it is already in progress: " + gFreeingSpaceLock);
		return;
	}

	lockFreeSpace ();

	log (TEST, "Begin freeing cache space. Current size: " + gTotalDataSize);

	walkDirectory (cacheDir, (err, files) => {
		if (err)
			throw err;

		files.sort ( function (a, b) {
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
};

let ShouldIgnoreFile = file => {
	if (file.length <= 2) return true; // Skip "00" to "ff" directories
	if (file.length >= 4 && file.toLowerCase().indexOf("temp") === 0) return true; // Skip Temp directory
	if (file.length >= 9 && file.toLowerCase().indexOf(".ds_store") === 0) return true; // Skip .DS_Store file on MacOSX
	if (file.length >= 11 && file.toLowerCase().indexOf("desktop.ini") === 0) return true; // Skip Desktop.ini file on Windows
	return false;
}, size;

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

const getFreeCacheSize = () => freeCacheSizeRatio * maxCacheSize;

const getDirectorySize = dir => {
	let size = 0;
	fs.readdirSync (dir).forEach (function (file)
	{
		file = dir + "/" + file;
		const stats = fs.statSync(file);
		if (stats.isFile ())
			size += stats.size;
		else
			size += getDirectorySize (file);
	});
	return size;
};

const initCache = () => {
	if (!fs.existsSync (cacheDir))
		fs.mkdirSync (cacheDir, 0o777);
	const hexDigits = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"];
	let cacheSubDir;
	for (let outer = 0; outer < hexDigits.length; outer++) {
		for (let inner = 0; inner < hexDigits.length; inner++) {
			cacheSubDir = cacheDir + "/" + hexDigits[outer] + hexDigits[inner];
			if (!fs.existsSync(cacheSubDir))
				fs.mkdirSync(cacheSubDir, 0o777);
		}
	}

	checkCacheDirectory (cacheDir);
	gTotalDataSize = getDirectorySize (cacheDir);

	log (DBG, "Cache Server directory " + path.resolve (cacheDir));
	log (DBG, "Cache Server size " + gTotalDataSize);
	log (DBG, "Cache Server max cache size " + maxCacheSize);

	if (gTotalDataSize > maxCacheSize)
		freeSpace (getFreeCacheSize ());
};

const fixFileIfRequired = (path, msg, fix) => {
	if (fix) {
		try {
			const stat = fs.statSync(path);
			if (stat.isDirectory()) {
				fs.rmdirSync(path);
				log(DBG, msg + " Directory deleted.");
			} else {
				fs.unlinkSync(path);
				log(DBG, msg + " File deleted.");
			}
		} catch(err) {
			log(ERR, err);
		}
	} else {
		log (DBG, msg + " Please delete it.");
	}
};

const validateFile = (dir, file, fix) => {
	let checkedPath;
	let msg;
	let path;
	if (ShouldIgnoreFile (file)) {
		return;
	}

	// Check file name
	const pattern = new RegExp(/^([0-9a-f]{2})([0-9a-f]{30})-([0-9a-f]{32})\.(bin|info|resource)$/i);
	const matches = file.match(pattern);
	if (matches == null)
	{
		path = dir ? cacheDir+"/"+dir+"/"+file : cacheDir+"/"+file;
		msg = "File "+path+" doesn't match valid pattern.";
		fixFileIfRequired (path, msg, fix);
		verificationFailed = true;
		verificationNumErrors++;
		return;
	}

	// Check if first 2 characters of file corresponds to dir
	if (matches[1].toLowerCase() !== dir.toLowerCase()) {
		path = cacheDir + "/" + dir + "/" + file;
		msg = "File " + path + " should not be in dir " + dir + ".";
		fixFileIfRequired (path, msg, fix);
		verificationFailed = true;
		verificationNumErrors++;
		return;
	}

	// Check if bin file exists for info or resource file
	if (matches[4].toLowerCase() === "info" || matches[4].toLowerCase() === "resource") {
		checkedPath = cacheDir + "/" + dir + "/" + matches[1] + matches[2] + "-" + matches[3] + ".bin";
		try {
			fs.statSync(checkedPath);
		} catch (e) {
			path = cacheDir+"/"+dir+"/"+file;
			msg = "Missing file "+checkedPath+" for "+path+".";
			fixFileIfRequired (path, msg, fix);
			verificationFailed = true;
			verificationNumErrors++;
		}
	}

	// Check if info file exists for bin or resource file
	if (matches[4].toLowerCase() === "bin" || matches[4].toLowerCase() === "resource") {
		checkedPath = cacheDir+"/"+dir+"/"+matches[1]+matches[2]+"-"+matches[3]+".info";
		try {
			fs.statSync(checkedPath);
		} catch (e) {
			path = cacheDir+"/"+dir+"/"+file;
			msg = "Missing file "+checkedPath+" for "+path+".";
			fixFileIfRequired (path, msg, fix);
			verificationFailed = true;
			verificationNumErrors++;
		}
	}

	// check if resource file exists for audio
	if (matches[4].toLowerCase() === "info")
	{
		try {
			const contents = fs.readFileSync(cacheDir + "/" + dir + "/" + file, "ascii");
			if (contents.indexOf ("assetImporterClassID: 1020") > 0) {
				checkedPath = cacheDir+"/"+dir+"/"+matches[1]+matches[2]+"-"+matches[3]+".resource";
				try {
					fs.statSync(checkedPath);
				} catch (e) {
					path = cacheDir+"/"+dir+"/"+file;
					msg = "Missing audio file "+checkedPath+" for "+path+".";
					fixFileIfRequired (path, msg, fix);
					path = cacheDir+"/"+dir+"/"+matches[1]+matches[2]+"-"+matches[3]+".bin";
					msg = "Missing audio file "+checkedPath+" for "+path+".";
					fixFileIfRequired (path, msg, fix);

					verificationFailed = true;
					verificationNumErrors++;
				}
			}
		}
		catch (e) {
			console.log(e);
		}
	}
};

const verifyCacheDirectory = (parent, dir, fix) => {
	fs.readdirSync (dir).forEach (function (file) {
		const path = dir + "/" + file;
		const stats = fs.statSync(path);
		if (stats.isDirectory ()) {
			if (!ShouldIgnoreFile (file)) {
				const msg = "The path " + path + " does not seem to be a valid cache path.";
				fixFileIfRequired (path, msg, fix);
				verificationFailed = true;
				verificationNumErrors++;
			} else {
				if (parent == null)
					verifyCacheDirectory (file, path, fix)
			}
		} else if (stats.isFile ()) {
			validateFile (parent, file, fix);
		}
	});
};

const verifyCache = fix => {
	verificationNumErrors = 0;
	if (!fs.existsSync (cacheDir))
		fs.mkdirSync (cacheDir, 0o777);

	verifyCacheDirectory (null, cacheDir, fix);
	return verificationNumErrors;
};

const addFileToCache = bytes => {
	if (bytes !== 0) {
		gTotalDataSize += bytes;
		log (DBG, "Total Cache Size " + gTotalDataSize);
		if (gTotalDataSize > maxCacheSize)
			freeSpace (getFreeCacheSize ());
	}
};

const getCachePath = (guid, hash, extension, create) => {
	const dir = cacheDir + "/" + guid.substring(0, 2);
	if (create) {
		log (DBG, "Create directory " + dir);
		fs.existsSync(dir) || fs.mkdirSync(dir, 0o777);
	}
	return dir + "/" + guid + "-" + hash + "." + extension;
};

/*
Protocol
========

client --- (version <uint32>) --> server	  (using version)
client <-- (version <uint32>) --- server	  (echo version if supported or 0)

# request cached item
client --- 'ga' (id <128bit GUID><128bit HASH>) --> server
client <-- '+a' (size <uint64>) (id <128bit GUID><128bit HASH>) + size bytes --- server (found in cache)
client <-- '-a' (id <128bit GUID><128bit HASH>) --- server (not found in cache)

client --- 'gi' (id <128bit GUID><128bit HASH>) --> server
client <-- '+i' (size <uint64>) (id <128bit GUID><128bit HASH>) + size bytes --- server (found in cache)
client <-- '-i' (id <128bit GUID><128bit HASH>) --- server (not found in cache)

client --- 'gr' (id <128bit GUID><128bit HASH>) --> server
client <-- '+r' (size <uint64>) (id <128bit GUID><128bit HASH>) + size bytes --- server	(found in cache)
client <-- '-r' (id <128bit GUID><128bit HASH>) --- server (not found in cache)

# start transaction
client --- 'ts' (id <128bit GUID><128bit HASH>) --> server

# put cached item
client --- 'pa' (size <uint64>) + size bytes --> server
client --- 'pi' (size <uint64>) + size bytes --> server
client --- 'pr' (size <uint64>) + size bytes --> server

# end transaction (ie rename targets to their final names)
client --- 'te' --> server

# cache server integrity
client --- 'ic' (<char 'v' or 'f'>) --> server
client <-- 'ic' (errors <uint64>) --- server

# quit
client --- 'q' --> server

*/

const sendNextGetFile = socket => {
	if (socket.getFileQueue.length === 0) {
		socket.activeGetFile = null;
		return;
	}

	if (socket.isActive)
		socket.resume();

	const next = socket.getFileQueue.pop();
	const resbuf = next.buffer;
	const type = next.type;
	const file = fs.createReadStream(next.cacheStream);
	// make sure no data is read and lost before we have called file.pipe ().
	file.pause ();
	socket.activeGetFile = file;
	const errFunc = () => {
		const buf = Buffer.alloc(CMD_SIZE + ID_SIZE);
		buf[0] = CMD_GETNOK;
		buf[1] = type;
		resbuf.copy(buf, CMD_SIZE, CMD_SIZE + UINT64_SIZE, CMD_SIZE + UINT64_SIZE + ID_SIZE);
		try {
			socket.write(buf);
		} catch (err) {
			log(ERR, "Error sending file data to socket " + err);
		} finally {
			if (socket.isActive) {
				sendNextGetFile(socket);
			} else {
				log(ERR, "Socket closed, close active file");
				file.close();
			}
		}
	};

	file.on ('close', function () {
		socket.activeGetFile = null;
		if (socket.isActive) {
			sendNextGetFile (socket);
		}
		let dateNow;
		try {
			// Touch the file, so that it becomes the newest accessed file for LRU cleanup - utimes expects a Unix timestamp in seconds, Date.now() returns millis
			dateNow = Date.now() / 1000;
			log(DBG, "Updating mtime of " + next.cacheStream + " to: " + dateNow);
			fs.utimesSync(next.cacheStream, dateNow, dateNow);
		} catch (err) {
			log(ERR, "Failed to update mtime of " + next.cacheStream + ": " + err);
		}
	});

	file.on ('open', function (fd) {
		fs.fstat (fd, (err, stats) => {
			if (err)
				errFunc ();
			else {
				resbuf[0] = CMD_GETOK;
				resbuf[1] = type;

				log (INFO, "Found: " + next.cacheStream + " size:" + stats.size);
				writeUInt64 (stats.size, resbuf.slice (CMD_SIZE));

				// The ID is already written
				try {
					socket.write (resbuf);
					file.resume ();
					file.pipe (socket, { end: false });
				} catch (err) {
					log (ERR, "Error sending file data to socket " + err + ", close active file");
					file.close();
				}
			}
		});
	});

	file.on ('error', errFunc);
};

const replaceFile = (from, to, size) => {
	fs.stat (to, (statsErr, stats) => {
		// We are replacing a file, we need to subtract this from the totalFileSize
		let oldSize = 0;
		if (!statsErr && stats) {
			oldSize = stats.size;
			fs.unlink (to, err => {
				// When the delete fails. We just delete the temp file. The size of the cache has not changed.
				if (err) {
					log (DBG, "Failed to delete file " + to + " (" + err + ")");
					fs.unlinkSync (from);
				}
				// When delete succeeds. We rename the file..
				else {
					renameFile (from, to, size, oldSize);
				}
			});
		} else {
			renameFile (from, to, size, 0);
		}
	});
};

const handleData = (socket, data) => {
	let i;
	let reqType;
	let size;
	let buf;
	// There is pending data, add it to the data buffer
	if (socket.pendingData != null) {
		buf = new Buffer (data.length + socket.pendingData.length);
		socket.pendingData.copy (buf, 0, 0);
		data.copy (buf, socket.pendingData.length, 0);
		data = buf;
		socket.pendingData = null;
	}

	while (true) {
		assert (socket.pendingData == null, "pending data must be null")

		// Get the version as the first thing
		let idx = 0;
		if (!socket.protocolVersion) {
			if (data.length < PROTOCOL_VERSION_MIN_SIZE) {
				// We need more data
				socket.pendingData = data;
				return false;
			}

			socket.protocolVersion = readUInt32 (data);
			buf = Buffer.alloc(UINT32_SIZE);
			if (socket.protocolVersion === PROTOCOL_VERSION) {
				log (INFO, "Client protocol version " + socket.protocolVersion);
				writeUInt32 (socket.protocolVersion, buf);
				if (socket.isActive)
					socket.write (buf);
				data = data.slice(UINT32_SIZE);
			} else {
				log (ERR, "Bad Client protocol version");
				writeUInt32 (0, buf);
				if (socket.isActive)
					socket.write (buf);
				socket.end ();
				socket.forceQuit = true;
				return false;
			}
		}

		// Write a a file to a temp location and move it in place when it has completed
		if (socket.activePutFile != null) {
			size = data.length;
			if (size > socket.bytesToBeWritten) {
				size = socket.bytesToBeWritten;
			}
			socket.activePutFile.write (data.slice (0, size), "binary");
			socket.bytesToBeWritten -= size;

			// If we have written all data for this file. We can close the file.
			if (socket.bytesToBeWritten <= 0) {
				socket.activePutFile.end (function ()
				{
					socket.targets.push ( { from: socket.tempPath, to: socket.activePutTarget, size: socket.totalFileSize } );
					socket.tempPath = null;
					socket.activePutTarget = null;
					socket.totalFileSize = 0;
					if (socket.isActive) {
						socket.resume();

						// It's possible to have already processed a 'te' (transaction end) event before this callback is called.
						// Call handleData again to ensure the 'te' event is re-processed now that we finished
						// saving this file
						if(socket.inTransaction)
							handleData(socket, Buffer.from([]));
					}
				});
				socket.activePutFile = null;

				data = data.slice (size);
				continue;
			}

			// We need more data to write the file completely
			// Return and wait for the next call to handleData to receive more data.
			return true;
		}

		if (data.length === 0)
		{
			// No more data
			return false;
		}

		if (data[idx] === CMD_QUIT)
		{
			socket.end ();
			socket.forceQuit = true;
			return false;
		}

		if (data[idx] === CMD_GET)
		{
			if (data.length < CMD_SIZE + ID_SIZE)
			{
				socket.pendingData = data;
				return true;
			}
			idx += 1;
			reqType = data[idx];
			idx += 1;

			const guid = readHex(GUID_SIZE, data.slice(idx));
			const hash = readHex(HASH_SIZE, data.slice(idx + GUID_SIZE));

			const resbuf = Buffer.alloc(CMD_SIZE + UINT64_SIZE + ID_SIZE);
			data.copy (resbuf, CMD_SIZE + UINT64_SIZE, idx, idx + ID_SIZE); // copy guid + hash

			if (reqType === TYPE_ASSET) {
				log (TEST, "Get Asset Binary " + guid + "/" + hash);
				socket.getFileQueue.unshift ( { buffer : resbuf, type : TYPE_ASSET, cacheStream : getCachePath (guid, hash, 'bin', false) } );
			} else if (reqType === TYPE_INFO) {
				log (TEST, "Get Asset Info " + guid + "/" + hash);
				socket.getFileQueue.unshift ( { buffer : resbuf, type : TYPE_INFO, cacheStream : getCachePath (guid, hash, 'info', false) } );
			} else if (reqType === TYPE_RESOURCE) {
				log (TEST, "Get Asset Resource " + guid + "/" + hash);
				socket.getFileQueue.unshift ( { buffer : resbuf, type : TYPE_RESOURCE, cacheStream : getCachePath (guid, hash, 'resource', false) } );
			}
			else {
				log (ERR, "Invalid data receive");
				socket.destroy ();
				return false;
			}

			if (!socket.activeGetFile) {
				sendNextGetFile (socket);
			}

			data = data.slice (idx + ID_SIZE);
			continue;
		}
		// handle a transaction
		else if (data[idx] === CMD_TRX) {
			if (data.length < CMD_SIZE) {
				socket.pendingData = data;
				return true;
			}
			idx += 1;

			if (data[idx] === TRX_START) {
				if (data.length < CMD_SIZE + ID_SIZE) {
					socket.pendingData = data;
					return true;
				}

				// Error: The previous transaction was not completed
				if (socket.inTransaction) {
					log (DBG, "Cancel previous transaction");
					for (i = 0; i < socket.targets.length ; i++) {
						fs.unlinkSync (socket.targets[i].from);
					}
				}

				idx += 1;

				socket.targets = [];
				socket.inTransaction = true;
				socket.currentGuid = readHex (GUID_SIZE, data.slice (idx));
				socket.currentHash = readHex (HASH_SIZE, data.slice (idx + GUID_SIZE));

				log (DBG, "Start transaction for " + socket.currentGuid + "-" + socket.currentHash);

				data = data.slice (idx + ID_SIZE);
				continue;
			} else if (data[idx] === TRX_END) {
				if (!socket.inTransaction) {
					log (ERR, "Invalid transaction isolation");
					socket.destroy ();
					return false;
				}

				// We have not completed writing the previous file
				if (socket.activePutTarget != null) {
					// Keep the data in pending for the next handleData call
					if (socket.isActive)
						socket.pause();
					socket.pendingData = data;
					return true;
				}

				idx += 1;

				log (DBG, "End transaction for " + socket.currentGuid + "-" + socket.currentHash);
				for (i = 0; i < socket.targets.length ; i++) {
					log (DBG, "Rename " + socket.targets[i].from + " to " + socket.targets[i].to);
					replaceFile (socket.targets[i].from, socket.targets[i].to, socket.targets[i].size);
				}

				socket.targets = [];
				socket.inTransaction = false;
				socket.currentGuid = null;
				socket.currentHash = null;

				data = data.slice (idx);

				continue;
			}
			else
			{
				log (ERR, "Invalid data receive");
				socket.destroy ();
				return false;
			}
		}
		// Put a file from the client to the cache server
		else if (data[idx] === CMD_PUT) {
			if (!socket.inTransaction) {
				log (ERR, "Not in a transaction");
				socket.destroy ();
				return false;
			}

			// We have not completed writing the previous file
			if (socket.activePutTarget != null) {
				// Keep the data in pending for the next handleData call
				if (socket.isActive)
					socket.pause();
				socket.pendingData = data;
				return true;
			}

			/// * We don't have enough data to start the put request. (wait for more data)
			if (data.length < CMD_SIZE + UINT64_SIZE) {
				socket.pendingData = data;
				return true;
			}

			idx += 1;
			reqType = data[idx];
			idx += 1;

			size = readUInt64(data.slice(idx));

			if (reqType === TYPE_ASSET) {
				log (TEST, "Put Asset Binary " + socket.currentGuid + "-" + socket.currentHash + " (size " + size + ")");
				socket.activePutTarget = getCachePath (socket.currentGuid, socket.currentHash, 'bin', true);
			} else if (reqType === TYPE_INFO) {
				log (TEST, "Put Asset Info " + socket.currentGuid + "-" + socket.currentHash + " (size " + size + ")");
				socket.activePutTarget = getCachePath (socket.currentGuid, socket.currentHash, 'info', true);
			} else if (reqType === TYPE_RESOURCE) {
				log (TEST, "Put Asset Resource " + socket.currentGuid + "-" + socket.currentHash + " (size " + size + ")");
				socket.activePutTarget = getCachePath (socket.currentGuid, socket.currentHash, 'resource', true);
			} else {
				log (ERR, "Invalid data receive");
				socket.destroy ();
				return false;
			}

			socket.tempPath = cacheDir + "/Temp" + uuid ();
			socket.activePutFile = fs.createWriteStream (socket.tempPath);

			socket.activePutFile.on ('error', function (err) {
				// Test that this codepath works correctly
				log (ERR, "Error writing to file " + err + ". Possibly the disk is full? Please adjust --cacheSize with a more accurate maximum cache size");
				freeSpace (gTotalDataSize * freeCacheSizeRatioWriteFailure);
				socket.destroy ();
				return false;
			});
			socket.bytesToBeWritten = size;
			socket.totalFileSize = size;

			data = data.slice (idx + UINT64_SIZE);
			continue;
		}

		// handle check integrity
		else if (data[idx] === CMD_INTEGRITY) {
			if (data.length < CMD_SIZE + 1) {
				socket.pendingData = data;
				return true;
			}
			idx += 1;

			if (socket.inTransaction) {
				log (ERR, "In a transaction");
				socket.destroy ();
				return false;
			}

			if (data[idx] === CMD_CHECK && (data[idx + 1] === OPT_VERIFY || data[idx + 1] === OPT_FIX)) {
				const fixIt = (data[idx + 1] === OPT_FIX);

				verificationNumErrors = 0;
				log (DBG, "Cache Server integrity check ("+(fixIt?"fix it":"verify only")+")");
				verifyCacheDirectory (null, cacheDir, fixIt);
				if (fixIt)
					log (DBG, "Cache Server integrity fix "+verificationNumErrors+" issue(s)");
				else
					log (DBG, "Cache Server integrity found "+verificationNumErrors+" error(s)");

				buf = Buffer.alloc(CMD_SIZE + UINT64_SIZE);
				buf[0] = CMD_INTEGRITY;
				buf[1] = CMD_CHECK;

				writeUInt64 (verificationNumErrors, buf.slice (CMD_SIZE));
				if (socket.isActive)
					socket.write (buf);

				idx += 2;
			} else {
				log (ERR, "Invalid data receive");
				socket.destroy ();
				return false;
			}
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
	socket["activePutTarget"] = null;
	socket["pendingData"] = null;
	socket["bytesToBeWritten"] = 0;
	socket["totalFileSize"] = 0;
	socket["isActive"] = true;
	socket["targets"] = [];
	socket["inTransaction"] = false;
	socket["currentGuid"] = null;
	socket["currentHash"] = null;
	socket["forceQuit"] = false;

	socket.on('data', data => {
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

const renameFile = (from, to, size, oldSize) => {
	fs.rename (from, to, function (err) {
		// When the rename fails. We just delete the temp file. The size of the cache has not changed.
		if (err) {
			log (DBG, "Failed to rename file " + from + " to " + to + " (" + err + ")");
			fs.unlinkSync (from);
		}
		// When replace succeeds. We reduce the cache size by previous file size and increase by new file size.
		else {
			addFileToCache (size - oldSize);
		}
	});
};



exports.log = function(lvl, msg) { log(lvl, msg); };

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
 * Get cache max size
 *
 * @return cache max size
 */
exports.getMaxCacheSize = () => maxCacheSize

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
exports.getCacheDir = function () {
	return path.resolve (cacheDir);
}

/**
 * start the cache server
 *
 * @param a_cacheSize maximum cache size
 * @param a_port server port
 * @param a_path cache path
 * @param a_logFn log function (optional)
 * @param a_errCallback error callback (optional)
 */
exports.start = (a_cacheSize, a_port, a_path, a_logFn, a_errCallback) => {
	if (a_logFn) {
		log = a_logFn;
	}
	maxCacheSize = a_cacheSize || maxCacheSize;
	port = a_port || port;
	cacheDir = a_path || cacheDir;

	initCache ();

	server.on('error', e => {
		if (e.code === 'EADDRINUSE') {
			log (ERR, 'Port '+ port + ' is already in use...');
			if (a_errCallback) {
				a_errCallback (e);
			}
		}
	});
	server.listen (port);
};

exports.verify = function (a_path, a_logFn, a_fix) {
	if (a_logFn) {
		log = a_logFn;
	}
	cacheDir = a_path || cacheDir;
	return verifyCache (a_fix);
}
