/*jslint node: true */
/*jshint sub:true*/

const TEST_MODE_LENGTH = 100;

const os = require("os");
let hostname = os.hostname();
hostname = hostname.replace(/.tld/g, ""); // amtrak wifi
hostname = hostname.replace(/.local/g, "");
hostname = hostname.replace(/.home/g, "");
hostname = hostname.replace(/.at.net/g, "");
hostname = hostname.replace(/.fios-router.home/g, "");
hostname = hostname.replace(/word0-instance-1/g, "google");
hostname = hostname.replace(/word/g, "google");

const PRIMARY_HOST = process.env.PRIMARY_HOST || "google";
const HOST = (hostname === PRIMARY_HOST) ? "default" : "local";

let DROPBOX_ROOT_FOLDER;

if (hostname === "google") {
  DROPBOX_ROOT_FOLDER = "/home/tc/Dropbox/Apps/wordAssociation";
}
else {
  DROPBOX_ROOT_FOLDER = "/Users/tc/Dropbox/Apps/wordAssociation";
}

const DEFAULT_INPUT_TYPES = [
  "emoji",
  "friends",
  "hashtags",
  "images",
  "locations",
  "media",
  "mentions",
  "places",
  "sentiment",
  "urls",
  "userMentions",
  "words"
];

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;

const ONE_KILOBYTE = 1024;
const ONE_MEGABYTE = 1024 * ONE_KILOBYTE;

const compactDateTimeFormat = "YYYYMMDD_HHmmss";

const DEFAULT_SERVER_MODE = false;

const DEFAULT_FIND_CAT_USER_CURSOR_LIMIT = 100;
const DEFAULT_CURSOR_BATCH_SIZE = process.env.DEFAULT_CURSOR_BATCH_SIZE || 100;

const DEFAULT_WAIT_UNLOCK_INTERVAL = 15*ONE_SECOND;
const DEFAULT_WAIT_UNLOCK_TIMEOUT = 10*ONE_MINUTE;
const DEFAULT_FILELOCK_RETRY_WAIT = DEFAULT_WAIT_UNLOCK_INTERVAL;
const DEFAULT_FILELOCK_STALE = 2*DEFAULT_WAIT_UNLOCK_TIMEOUT;
const DEFAULT_FILELOCK_WAIT = DEFAULT_WAIT_UNLOCK_TIMEOUT;

const fileLockOptions = { 
  retries: DEFAULT_FILELOCK_WAIT,
  retryWait: DEFAULT_FILELOCK_RETRY_WAIT,
  stale: DEFAULT_FILELOCK_STALE,
  wait: DEFAULT_FILELOCK_WAIT
};

const moment = require("moment");
const lockFile = require("lockfile");
const merge = require("deepmerge");
const treeify = require("treeify");
const archiver = require("archiver");
const fs = require("fs");
const atob = require("atob");
const btoa = require("btoa");
const objectPath = require("object-path");
const validUrl = require("valid-url");
const MergeHistograms = require("@threeceelabs/mergehistograms");
const mergeHistograms = new MergeHistograms();


let archive;

const MODULE_ID_PREFIX = "GTS";
const MODULE_ID = MODULE_ID_PREFIX + "_node_" + hostname;
const GLOBAL_TRAINING_SET_ID = "globalTrainingSet";

const DEFAULT_QUIT_ON_COMPLETE = false;
const DEFAULT_TEST_RATIO = 0.20;

const JSONParse = require("json-parse-safe");
const util = require("util");
const _ = require("lodash");
const writeJsonFile = require("write-json-file");
const sizeof = require("object-sizeof");
const fetch = require("isomorphic-fetch"); // or another library of choice.
const Dropbox = require("dropbox").Dropbox;
const pick = require("object.pick");
const Slack = require("slack-node");
const EventEmitter3 = require("eventemitter3");
const async = require("async");

const chalk = require("chalk");
const chalkAlert = chalk.red;
const chalkBlue = chalk.blue;
const chalkBlueBold = chalk.bold.blue;
const chalkGreen = chalk.green;
const chalkRedBold = chalk.bold.red;
const chalkError = chalk.bold.red;
const chalkWarn = chalk.red;
const chalkLog = chalk.gray;
const chalkInfo = chalk.black;

const debug = require("debug")("gts");
const commandLineArgs = require("command-line-args");

let prevHostConfigFileModifiedMoment = moment("2010-01-01");
let prevDefaultConfigFileModifiedMoment = moment("2010-01-01");
let prevConfigFileModifiedMoment = moment("2010-01-01");

let categorizedUsersPercent = 0;
const categorizedUsersStartMoment = moment();
let categorizedUsersEndMoment = moment();
let categorizedUsersElapsed = 0;
let categorizedUsersRemain = 0;
let categorizedUsersRate = 0;
let categorizedNodeIdsArray = [];

const maxInputHashMap = {};
const globalhistograms = {};

DEFAULT_INPUT_TYPES.forEach(function(type){
  globalhistograms[type] = {};
  maxInputHashMap[type] = {};
});

const trainingSetUsersArray = [];

const statsObj = {};
let statsObjSmall = {};

statsObj.status = "LOAD";
statsObj.hostname = hostname;
statsObj.pid = process.pid;
statsObj.cpus = os.cpus().length;
statsObj.commandLineArgsLoaded = false;

statsObj.archiveOpen = false;
statsObj.archiveModifiedMoment = moment("2010-01-01");

statsObj.serverConnected = false;

statsObj.startTimeMoment = moment();
statsObj.startTime = moment().valueOf();
statsObj.elapsed = moment().valueOf() - statsObj.startTime;

statsObj.users = {};
statsObj.users.notCategorized = 0;
statsObj.users.updatedCategorized = 0;
statsObj.users.notFound = 0;
statsObj.users.screenNameUndefined = 0;
statsObj.users.unzipped = 0;
statsObj.users.zipHashMapHit = 0;

statsObj.errors = {};
statsObj.errors.users = {};
statsObj.errors.users.findOne = 0;


const statsPickArray = [
  "pid", 
  "startTime", 
  "elapsed", 
  "serverConnected", 
  "status", 
  "authenticated", 
  "numChildren", 
  "userReadyAck", 
  "userReadyAckWait", 
  "userReadyTransmitted"
];

statsObjSmall = pick(statsObj, statsPickArray);

process.on("unhandledRejection", function(err, promise) {
  console.trace("Unhandled rejection (promise: ", promise, ", reason: ", err, ").");
  process.exit();
});

function jsonPrint(obj) {
  if (obj) {
    return treeify.asTree(obj, true, true);
  } 
  else {
    return obj;
  }
}

function getTimeStamp(inputTime) {
  let currentTimeStamp;

  if (inputTime === undefined) {
    currentTimeStamp = moment().format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else if (moment.isMoment(inputTime)) {
    currentTimeStamp = moment(inputTime).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else if (moment.isDate(new Date(inputTime)) && moment(new Date(inputTime)).isValid()) {
    currentTimeStamp = moment(new Date(inputTime)).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else if (Number.isInteger(inputTime)) {
    currentTimeStamp = moment(parseInt(inputTime)).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else {
    return "NOT VALID TIMESTAMP: " + inputTime;
  }
}

const slackChannel = "#gts";
let slackText = "";

let initMainInterval;

let configuration = {}; // merge of defaultConfiguration & hostConfiguration
// configuration.normalization = null;
configuration.verbose = false;
configuration.testMode = false; // per tweet test mode
configuration.testSetRatio = DEFAULT_TEST_RATIO;
configuration.maxTestCount = TEST_MODE_LENGTH;

let defaultConfiguration = {}; // general configuration for GTS
let hostConfiguration = {}; // host-specific configuration for GTS

configuration.serverMode = DEFAULT_SERVER_MODE;

console.log(chalkLog("GTS | SERVER MODE: " + configuration.serverMode));

configuration.processName = process.env.GTS_PROCESS_NAME || "node_gts";

configuration.interruptFlag = false;
configuration.enableRequiredTrainingSet = false;
configuration.quitOnComplete = DEFAULT_QUIT_ON_COMPLETE;
configuration.globalTrainingSetId = GLOBAL_TRAINING_SET_ID;

configuration.archiveFileUploadCompleteFlagFile = "usersZipUploadComplete.json";
configuration.trainingSetFile = "trainingSet.json";
configuration.requiredTrainingSetFile = "requiredTrainingSet.txt";
configuration.userArchiveFile = hostname + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat) + "_users.zip";

const DROPBOX_CONFIG_FOLDER = "/config/utility";
const DROPBOX_CONFIG_DEFAULT_FOLDER = DROPBOX_CONFIG_FOLDER + "/default";
const DROPBOX_CONFIG_HOST_FOLDER = DROPBOX_CONFIG_FOLDER + "/" + hostname;

const DROPBOX_LIST_FOLDER_LIMIT = 50;

configuration.local = {};
configuration.local.trainingSetsFolder = DROPBOX_CONFIG_HOST_FOLDER + "/trainingSets";
configuration.local.histogramsFolder = DROPBOX_CONFIG_HOST_FOLDER + "/histograms";
configuration.local.userArchiveFolder = DROPBOX_CONFIG_HOST_FOLDER + "/trainingSets/users";
configuration.local.userArchivePath = configuration.local.userArchiveFolder + "/" + configuration.userArchiveFile;

configuration.default = {};
configuration.default.trainingSetsFolder = DROPBOX_CONFIG_DEFAULT_FOLDER + "/trainingSets";
configuration.default.histogramsFolder = DROPBOX_CONFIG_DEFAULT_FOLDER + "/histograms";
configuration.default.userArchiveFolder = DROPBOX_CONFIG_DEFAULT_FOLDER + "/trainingSets/users";
configuration.default.userArchivePath = configuration.default.userArchiveFolder + "/" + configuration.userArchiveFile;

configuration.trainingSetsFolder = configuration[HOST].trainingSetsFolder;
configuration.archiveFileUploadCompleteFlagFolder = configuration[HOST].trainingSetsFolder + "/users";
configuration.histogramsFolder = configuration[HOST].histogramsFolder;
configuration.userArchiveFolder = configuration[HOST].userArchiveFolder;
configuration.userArchivePath = DROPBOX_ROOT_FOLDER + configuration[HOST].userArchivePath;

configuration.DROPBOX = {};
configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN = process.env.DROPBOX_WORD_ASSO_ACCESS_TOKEN;
configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_KEY = process.env.DROPBOX_WORD_ASSO_APP_KEY;
configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_SECRET = process.env.DROPBOX_WORD_ASSO_APP_SECRET;
configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE = process.env.DROPBOX_GTS_CONFIG_FILE || "generateTrainingSetConfig.json";
configuration.DROPBOX.DROPBOX_GTS_STATS_FILE = process.env.DROPBOX_GTS_STATS_FILE || "generateTrainingSetStats.json";

const drbx = require("@davvo/drbx")({
  token: configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN
});


const slackOAuthAccessToken = "xoxp-3708084981-3708084993-206468961315-ec62db5792cd55071a51c544acf0da55";

function toMegabytes(sizeInBytes) {
  return sizeInBytes/ONE_MEGABYTE;
}

function msToTime(d) {

  let duration = d;
  let sign = 1;

  if (duration < 0) {
    sign = -1;
    duration = -duration;
  }

  let seconds = parseInt((duration / 1000) % 60);
  let minutes = parseInt((duration / (1000 * 60)) % 60);
  let hours = parseInt((duration / (1000 * 60 * 60)) % 24);
  let days = parseInt(duration / (1000 * 60 * 60 * 24));
  days = (days < 10) ? "0" + days : days;
  hours = (hours < 10) ? "0" + hours : hours;
  minutes = (minutes < 10) ? "0" + minutes : minutes;
  seconds = (seconds < 10) ? "0" + seconds : seconds;

  if (sign > 0) return days + ":" + hours + ":" + minutes + ":" + seconds;
  return "- " + days + ":" + hours + ":" + minutes + ":" + seconds;
}

const DEFAULT_RUN_ID = hostname + "_" + process.pid + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat);

if (process.env.GTS_RUN_ID !== undefined) {
  statsObj.runId = process.env.GTS_RUN_ID;
  console.log(chalkLog("GTS | ENV RUN ID: " + statsObj.runId));
}
else {
  statsObj.runId = DEFAULT_RUN_ID;
  console.log(chalkLog("GTS | DEFAULT RUN ID: " + statsObj.runId));
}

const categorizedNodeIdsSet = new Set();

const categorizedUserHistogram = {};
categorizedUserHistogram.left = 0;
categorizedUserHistogram.right = 0;
categorizedUserHistogram.neutral = 0;
categorizedUserHistogram.positive = 0;
categorizedUserHistogram.negative = 0;
categorizedUserHistogram.none = 0;
categorizedUserHistogram.total = 0;

const categorizedUserHistogramTotal = function(){
  categorizedUserHistogram.total = 0;
  categorizedUserHistogram.total = categorizedUserHistogram.left;
  categorizedUserHistogram.total += categorizedUserHistogram.right;
  categorizedUserHistogram.total += categorizedUserHistogram.neutral;
  categorizedUserHistogram.total += categorizedUserHistogram.positive;
  categorizedUserHistogram.total += categorizedUserHistogram.negative;
  categorizedUserHistogram.total += categorizedUserHistogram.none;

  return categorizedUserHistogram.total;
}


const configEvents = new EventEmitter3({
  wildcard: true,
  newListener: true,
  maxListeners: 20,
  verboseMemoryLeak: true
});

let stdin;

const slack = new Slack(slackOAuthAccessToken);

function slackPostMessage(channel, text, callback){

  if (configuration.offlineMode) {
    console.log(chalkAlert("GTS | SLACK DISABLED"
      + " | OFFLINE_MODE: " + configuration.offlineMode
      + " | SERVER CONNECTED: " + statsObj.serverConnected
    ));
    if (callback !== undefined) { 
      return callback(null, null);
    }
    else{
      return;
    }
  }

  slack.api("chat.postMessage", {
    text: text,
    channel: channel
  }, function(err, response){
    if (err){
      console.error(chalkError("GTS | *** SLACK POST MESSAGE ERROR"
        + " | CH: " + channel
        + "\nGTS | TEXT: " + text
        + "\nGTS | ERROR: " + err
      ));
    }
    else {
      debug(response);
    }
    if (callback !== undefined) { callback(err, response); }
  });
}

const help = { name: "help", alias: "h", type: Boolean};

const enableStdin = { name: "enableStdin", alias: "S", type: Boolean, defaultValue: true };
const quitOnComplete = { name: "quitOnComplete", alias: "q", type: Boolean };
const quitOnError = { name: "quitOnError", alias: "Q", type: Boolean, defaultValue: true };
const verbose = { name: "verbose", alias: "v", type: Boolean };

const testMode = { name: "testMode", alias: "X", type: Boolean };

const optionDefinitions = [
  enableStdin, 
  quitOnComplete, 
  quitOnError, 
  verbose, 
  testMode,
  help
];

const commandLineConfig = commandLineArgs(optionDefinitions);
console.log(chalkInfo("GTS | COMMAND LINE CONFIG\nGTS | " + jsonPrint(commandLineConfig)));
console.log("GTS | COMMAND LINE OPTIONS\nGTS | " + jsonPrint(commandLineConfig));

if (Object.keys(commandLineConfig).includes("help")) {
  console.log("GTS |optionDefinitions\n" + jsonPrint(optionDefinitions));
  quit("help");
}

process.on("message", function(msg) {
  if (msg === "shutdown") {
    console.log("\n\nGTS | !!!!! RECEIVED PM2 SHUTDOWN !!!!!\n\n***** Closing all connections *****\n\n");
    setTimeout(function() {
      console.log("GTS | **** Finished closing connections ****"
        + "\n\n GTS | ***** RELOADING generateTrainingSet.js NOW *****\n\n");
      process.exit(0);
    }, 1500);
  }
  else {
    console.log("GTS | R<\n" + jsonPrint(msg));
  }
});

statsObj.commandLineConfig = commandLineConfig;

statsObj.normalization = {};
statsObj.normalization.score = {};
statsObj.normalization.comp = {};
statsObj.normalization.magnitude = {};

statsObj.normalization.score.min = 1.0;
statsObj.normalization.score.max = -1.0;
statsObj.normalization.comp.min = Infinity;
statsObj.normalization.comp.max = -Infinity;
statsObj.normalization.magnitude.min = 0;
statsObj.normalization.magnitude.max = -Infinity;

const testObj = {};
testObj.testRunId = statsObj.runId;
testObj.results = {};
testObj.testSet = [];

process.title = "node_gts";
console.log("\n\nGTS | =================================");
console.log("GTS | HOST:          " + hostname);
console.log("GTS | PROCESS TITLE: " + process.title);
console.log("GTS | PROCESS ID:    " + process.pid);
console.log("GTS | RUN ID:        " + statsObj.runId);
console.log("GTS | PROCESS ARGS:  " + util.inspect(process.argv, {showHidden: false, depth: 1}));
console.log("GTS | =================================");

// ==================================================================
// DROPBOX
// ==================================================================

const dropboxConfigFolder = "/config/utility";
const dropboxConfigDefaultFolder = "/config/utility/default";
const dropboxConfigHostFolder = "/config/utility/" + hostname;

const dropboxConfigDefaultFile = "default_" + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE;
const dropboxConfigHostFile = hostname + "_" + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE;

const statsFolder = "/stats/" + hostname + "/generateTrainingSet";
const statsFile = "generateTrainingSetStats_" + statsObj.runId + ".json";

debug("statsFolder : " + statsFolder);
debug("statsFile : " + statsFile);

console.log("GTS | DROPBOX_GTS_CONFIG_FILE: " + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE);
console.log("GTS | DROPBOX_GTS_STATS_FILE : " + configuration.DROPBOX.DROPBOX_GTS_STATS_FILE);

debug("dropboxConfigFolder : " + dropboxConfigFolder);
debug("dropboxConfigHostFolder : " + dropboxConfigHostFolder);
debug("dropboxConfigDefaultFile : " + dropboxConfigDefaultFile);
debug("dropboxConfigHostFile : " + dropboxConfigHostFile);

debug("GTS | DROPBOX_WORD_ASSO_ACCESS_TOKEN :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN);
debug("GTS | DROPBOX_WORD_ASSO_APP_KEY :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_KEY);
debug("GTS | DROPBOX_WORD_ASSO_APP_SECRET :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_SECRET);

global.globalDbConnection = false;
const mongoose = require("mongoose");
mongoose.set("useFindAndModify", false);

const emojiModel = require("@threeceelabs/mongoose-twitter/models/emoji.server.model");
const hashtagModel = require("@threeceelabs/mongoose-twitter/models/hashtag.server.model");
const locationModel = require("@threeceelabs/mongoose-twitter/models/location.server.model");
const mediaModel = require("@threeceelabs/mongoose-twitter/models/media.server.model");
const neuralNetworkModel = require("@threeceelabs/mongoose-twitter/models/neuralNetwork.server.model");
const placeModel = require("@threeceelabs/mongoose-twitter/models/place.server.model");
const tweetModel = require("@threeceelabs/mongoose-twitter/models/tweet.server.model");
const urlModel = require("@threeceelabs/mongoose-twitter/models/url.server.model");
const userModel = require("@threeceelabs/mongoose-twitter/models/user.server.model");
const wordModel = require("@threeceelabs/mongoose-twitter/models/word.server.model");

global.globalWordAssoDb = require("@threeceelabs/mongoose-twitter");

const UserServerController = require("@threeceelabs/user-server-controller");
let userServerController;

const dropboxRemoteClient = new Dropbox({ 
  accessToken: configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN,
  fetch: fetch
});

const dropboxLocalClient = { // offline mode
  filesListFolder: filesListFolderLocal,
  filesUpload: function(){},
  filesDownload: function(){},
  filesGetMetadata: filesGetMetadataLocal,
  filesDelete: function(){}
};

function filesGetMetadataLocal(options){

  return new Promise(function(resolve, reject) {

    console.log("filesGetMetadataLocal options\n" + jsonPrint(options));

    const fullPath = DROPBOX_ROOT_FOLDER + options.path;

    fs.stat(fullPath, function(err, stats){
      if (err) {
        reject(err);
      }
      else {
        const response = {
          client_modified: stats.mtimeMs
        };
        
        resolve(response);
      }
    });
  });
}

function filesListFolderLocal(options){
  return new Promise(function(resolve, reject) {

    debug("filesListFolderLocal options\n" + jsonPrint(options));

    const fullPath = DROPBOX_ROOT_FOLDER + options.path;

    fs.readdir(fullPath, function(err, items){
      if (err) {
        reject(err);
      }
      else {

        const itemArray = [];

        async.each(items, function(item, cb){

          itemArray.push(
            {
              name: item, 
              client_modified: false,
              content_hash: false,
              path_display: fullPath + "/" + item
            }
          );
          cb();

        }, function(err){

          if (err){
            return reject(err);
          }

          const response = {
            cursor: false,
            has_more: false,
            entries: itemArray
          };

          resolve(response);
        });
        }
    });
  });
}

let dropboxClient;

if (configuration.offlineMode) {
  dropboxClient = dropboxLocalClient;
}
else {
  dropboxClient = dropboxRemoteClient;
}

const globalCategorizedUsersFolder = dropboxConfigDefaultFolder + "/categorizedUsers";
const categorizedUsersFile = "categorizedUsers_manual.json";


function showStats(options){

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  statsObjSmall = pick(statsObj, statsPickArray);


  if (options) {
    console.log("GTS | STATS\nGTS | " + jsonPrint(statsObjSmall));
  }
  else {
    console.log(chalkLog("GTS | ============================================================"
      + "\nGTS | S"
      + " | STATUS: " + statsObj.status
      + " | CPUs: " + statsObj.cpus
      + " | " + testObj.testRunId
      + " | RUN " + msToTime(statsObj.elapsed)
      + " | NOW " + moment().format(compactDateTimeFormat)
      + " | STRT " + moment(parseInt(statsObj.startTime)).format(compactDateTimeFormat)
      + "\nGTS | ============================================================"
    ));

    categorizedUserHistogramTotal();

    console.log(chalkLog("GTS | CL U HIST"
      + " | TOTAL: " + categorizedUserHistogram.total
      + " | L: " + categorizedUserHistogram.left
      + " | R: " + categorizedUserHistogram.right
      + " | N: " + categorizedUserHistogram.neutral
      + " | +: " + categorizedUserHistogram.positive
      + " | -: " + categorizedUserHistogram.negative
      + " | 0: " + categorizedUserHistogram.none
    ));

  }
}

function quit(options){

  console.log(chalkAlert("GTS | QUITTING ..." ));

  clearInterval(initMainInterval);

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  if (options !== undefined) {

    if (options === "help") {
      process.exit();
    }
    else {
      slackText = "\n*" + statsObj.runId + "*";
      slackText = slackText + " | RUN " + msToTime(statsObj.elapsed);
      slackText = slackText + " | QUIT CAUSE: " + options;

      debug("GTS | SLACK TEXT: " + slackText);

      slackPostMessage(slackChannel, slackText);
    }
  }

  showStats();

  setTimeout(function(){
    global.globalDbConnection.close(function () {
      console.log(chalkAlert(
            "GTS | =========================="
        + "\nGTS | MONGO DB CONNECTION CLOSED"
        + "\nGTS | =========================="
      ));
      process.exit();
    });
  }, 1000);
}

process.on( "SIGINT", function() {
  quit("SIGINT");
});

process.on("exit", function() {
  quit("SIGINT");
});


function connectDb(){

  return new Promise(function(resolve, reject){

    try {

      statsObj.status = "CONNECTING MONGO DB";

      global.globalWordAssoDb.connect(MODULE_ID + "_" + process.pid, async function(err, db){

        if (err) {
          console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR: " + err));
          statsObj.status = "MONGO CONNECTION ERROR";
          quit({cause: "MONGO DB ERROR: " + err});
          return reject(err);
        }

        db.on("close", async function(){
          statsObj.status = "MONGO CLOSED";
          console.error.bind(console, MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION CLOSED");
          console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION CLOSED"));
        });

        db.on("error", async function(){
          statsObj.status = "MONGO ERROR";
          console.error.bind(console, MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR");
          console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR"));
        });

        db.on("disconnected", async function(){
          statsObj.status = "MONGO DISCONNECTED";
          console.error.bind(console, MODULE_ID_PREFIX + " | *** MONGO DB DISCONNECTED");
          console.log(chalkAlert(MODULE_ID_PREFIX + " | *** MONGO DB DISCONNECTED"));
        });

        global.globalDbConnection = db;

        console.log(chalk.green(MODULE_ID_PREFIX + " | MONGOOSE DEFAULT CONNECTION OPEN"));

        global.globalEmoji = global.globalDbConnection.model("Emoji", emojiModel.EmojiSchema);
        global.globalHashtag = global.globalDbConnection.model("Hashtag", hashtagModel.HashtagSchema);
        global.globalLocation = global.globalDbConnection.model("Location", locationModel.LocationSchema);
        global.globalMedia = global.globalDbConnection.model("Media", mediaModel.MediaSchema);
        global.globalNeuralNetwork = global.globalDbConnection.model("NeuralNetwork", neuralNetworkModel.NeuralNetworkSchema);
        global.globalPlace = global.globalDbConnection.model("Place", placeModel.PlaceSchema);
        global.globalTweet = global.globalDbConnection.model("Tweet", tweetModel.TweetSchema);
        global.globalUrl = global.globalDbConnection.model("Url", urlModel.UrlSchema);
        global.globalUser = global.globalDbConnection.model("User", userModel.UserSchema);
        global.globalWord = global.globalDbConnection.model("Word", wordModel.WordSchema);

        const uscChildName = MODULE_ID_PREFIX + "_USC";
        userServerController = new UserServerController(uscChildName);

        userServerController.on("ready", function(appname){
          statsObj.status = "MONGO DB CONNECTED";
          console.log(chalkLog(MODULE_ID_PREFIX + " | " + uscChildName + " READY | " + appname));
          resolve(db);
        });
      });
    }
    catch(err){
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECT ERROR: " + err));
      reject(err);
    }
  });
}


function saveFile(params){

  return new Promise(function(resolve, reject){

    const fullPath = params.folder + "/" + params.file;

    debug(chalkInfo("LOAD FOLDER " + params.folder));
    debug(chalkInfo("LOAD FILE " + params.file));
    debug(chalkInfo("FULL PATH " + fullPath));

    const options = {};

    if (params.localFlag) {

      options.access_token = configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN;
      options.file_size = sizeof(params.obj);
      options.destination = params.folder + "/" + params.file;
      options.autorename = true;
      options.mode = params.mode || "overwrite";
      options.mode = "overwrite";

      const objSizeMBytes = options.file_size/ONE_MEGABYTE;

      showStats();
      console.log(chalkInfo("GTS | ... SAVING LOCALLY | " + objSizeMBytes.toFixed(2) + " MB | " + fullPath));

      writeJsonFile(fullPath, params.obj).
      then(function() {

        console.log(chalkInfo("GTS | SAVED LOCALLY | " + objSizeMBytes.toFixed(2) + " MB | " + fullPath));

        setTimeout(function(){

          console.log(chalkInfo("GTS | ... DROPBOX UPLOADING | " + objSizeMBytes.toFixed(2) + " MB | " + fullPath + " > " + options.destination));

          const stats = fs.statSync(fullPath);
          const fileSizeInBytes = stats.size;
          const savedSize = fileSizeInBytes/ONE_MEGABYTE;

          console.log(chalkLog("GTS | ... SAVING DROPBOX JSON"
            + " | " + getTimeStamp()
            + " | " + savedSize.toFixed(2) + " MBYTES"
            + "\n SRC: " + fullPath
            + "\n DST: " + options.destination
          ));

          const localReadStream = fs.createReadStream(fullPath);
          const remoteWriteStream = drbx.file(options.destination).createWriteStream();

          let bytesRead = 0;
          let chunksRead = 0;
          let mbytesRead = 0;
          let percentRead = 0;

          localReadStream.pipe(remoteWriteStream);

          localReadStream.on("data", function(chunk){
            bytesRead += chunk.length;
            mbytesRead = bytesRead/ONE_MEGABYTE;
            percentRead = 100 * bytesRead/fileSizeInBytes;
            chunksRead += 1;
            if (chunksRead % 100 === 0){
              console.log(chalkInfo("GTS | LOCAL READ"
                + " | " + mbytesRead.toFixed(2) + " / " + savedSize.toFixed(2) + " MB"
                + " (" + percentRead.toFixed(2) + "%)"
              ));
            }
          });

          localReadStream.on("close", function(){
            console.log(chalkInfo("GTS | LOCAL STREAM READ CLOSED | SOURCE: " + fullPath));
          });

          remoteWriteStream.on("close", function(){
            console.log(chalkInfo("GTS | REMOTE STREAM WRITE CLOSED | DEST: " + options.destination));
          });

          localReadStream.on("end", function(){
            console.log(chalkInfo("GTS | LOCAL READ COMPLETE"
              + " | SOURCE: " + fullPath
              + " | " + mbytesRead.toFixed(2) + " / " + savedSize.toFixed(2) + " MB"
              + " (" + percentRead.toFixed(2) + "%)"
            ));
            localReadStream.close();
          });

          localReadStream.on("error", function(err){
            console.error("GTS | *** LOCAL STREAM READ ERROR | " + err);
            return reject(err);
          });

          remoteWriteStream.on("end", function(){
            console.log(chalkInfo("GTS | REMOTE STREAM WRITE END | DEST: " + options.destination));
            return resolve();
          });

          remoteWriteStream.on("error", function(err){
            console.error("GTS | *** REMOTE STREAM WRITE ERROR | DEST: " + options.destination + "\n" + err);
            return reject(err);
          });

        }, configuration.waitInterval);

      }).
      catch(function(err){
        console.trace(chalkError("GTS | " + moment().format(compactDateTimeFormat) 
          + " | !!! ERROR DROBOX JSON WRITE | FILE: " + fullPath 
          + " | ERROR: " + err
          + " | ERROR\n" + jsonPrint(err)
        ));
        return reject(err);
      });
    }
    else {

      if (params.text) {
        options.contents = params.text;
      }
      else {
        options.contents = JSON.stringify(params.obj, null, 2);
      }
      options.autorename = params.autorename || false;
      options.mode = params.mode || "overwrite";
      options.path = fullPath;

      const dbFileUpload = function () {

        dropboxClient.filesUpload(options).
        then(function(){
          console.log(chalkLog("GTS | SAVED DROPBOX JSON | " + options.path));
          resolve();
        }).
        catch(function(err){
          if (err.status === 413){
            console.log(chalkError("GTS | " + moment().format(compactDateTimeFormat) 
              + " | !!! ERROR DROBOX JSON WRITE | FILE: " + fullPath 
              + " | ERROR: 413"
            ));
            reject(err);
          }
          else if (err.status === 429){
            console.log(chalkError("GTS | " + moment().format(compactDateTimeFormat) 
              + " | !!! ERROR DROBOX JSON WRITE | FILE: " + fullPath 
              + " | ERROR: TOO MANY WRITES"
            ));
            resolve(err.error_summary);
          }
          else if (err.status === 500){
            console.log(chalkError("GTS | " + moment().format(compactDateTimeFormat) 
              + " | !!! ERROR DROBOX JSON WRITE | FILE: " + fullPath 
              + " | ERROR: DROPBOX SERVER ERROR"
            ));
            resolve(err.error_summary);
          }
          else {
            console.log(chalkError("GTS | " + moment().format(compactDateTimeFormat) 
              + " | !!! ERROR DROBOX JSON WRITE | FILE: " + fullPath 
              + " | ERROR: " + err
            ));
            reject(err);
          }
        });
      };

      if (options.mode === "add") {

        dropboxClient.filesListFolder({path: params.folder, limit: DROPBOX_LIST_FOLDER_LIMIT}).
        then(function(response){

          debug(chalkLog("DROPBOX LIST FOLDER"
            + " | ENTRIES: " + response.entries.length
            + " | MORE: " + response.has_more
            + " | PATH:" + options.path
          ));

          let fileExits = false;

          async.each(response.entries, function(entry, cb){

            console.log(chalkInfo("GTS | DROPBOX FILE"
              + " | " + params.folder
              + " | LAST MOD: " + moment(new Date(entry.client_modified)).format(compactDateTimeFormat)
              + " | " + entry.name
            ));

            if (entry.name === params.file) {
              fileExits = true;
            }

            async.setImmediate(function() { cb(); });

          }, function(err){
            if (err) {
              console.log(chalkError("GTS | *** ERROR DROPBOX SAVE FILE: " + err));
              return reject(err);
            }
            if (fileExits) {
              console.log(chalkInfo("GTS | ... DROPBOX FILE EXISTS ... SKIP SAVE | " + fullPath));
              resolve(err);
            }
            else {
              console.log(chalkInfo("GTS | ... DROPBOX DOES NOT FILE EXIST ... SAVING | " + fullPath));
              dbFileUpload();
            }
          });

        }).
        catch(function(err){
          console.log(chalkError("GTS | *** DROPBOX SAVE FILE ERROR: " + err));
          return reject(err);
        });
      }
      else {
        dbFileUpload();
      }
    }

  });
}

async function loadFileRetry(params){

  const maxRetries = params.maxRetries || 5;
  let i;

  for (i = 0;i < maxRetries;++i) {
    try {
      
      if (i > 0) { console.log(chalkAlert("TNN | FILE LOAD RETRY: " + i + " OF " + maxRetries)); }

      const fileObj = await loadFile(params);
      return fileObj;
    } 
    catch(err) {
      console.log(chalkAlert("TNN | *** FILE LOAD FAILED | RETRY: " + i + " OF " + maxRetries));
      throw err;
    }
  }

  throw new Error("FILE LOAD ERROR | RETRIES " + maxRetries);
}

function loadFile(params) {

  return new Promise(function(resolve, reject){

    let fullPath = params.folder + "/" + params.file

    debug(chalkInfo("LOAD FOLDER " + params.folder));
    debug(chalkInfo("LOAD FILE " + params.file));
    debug(chalkInfo("FULL PATH " + fullPath));


    if (configuration.offlineMode || params.loadLocalFile) {

      if (hostname === PRIMARY_HOST) {
        fullPath = DROPBOX_ROOT_FOLDER + fullPath;
        console.log(chalkInfo("OFFLINE_MODE: FULL PATH " + fullPath));
      }

      if ((hostname === "mbp3") || (hostname === "mbp2")) {
        fullPath = DROPBOX_ROOT_FOLDER + fullPath;
        console.log(chalkInfo("OFFLINE_MODE: FULL PATH " + fullPath));
      }

      fs.readFile(fullPath, "utf8", function(err, data) {

        if (err) {
          console.log(chalkError("fs readFile ERROR: " + err));
          return reject(err);
        }

        console.log(chalkInfo(getTimeStamp()
          + " | LOADING FILE FROM DROPBOX"
          + " | " + fullPath
        ));

        if (params.file.match(/\.json$/gi)) {

          const fileObj = JSONParse(data);

          if (fileObj.value) {

            const fileObjSizeMbytes = sizeof(fileObj)/ONE_MEGABYTE;

            console.log(chalkInfo(getTimeStamp()
              + " | LOADED FILE FROM DROPBOX"
              + " | " + fileObjSizeMbytes.toFixed(2) + " MB"
              + " | " + fullPath
            ));

            return resolve(fileObj.value);
          }

          console.log(chalkError(getTimeStamp()
            + " | *** LOAD FILE FROM DROPBOX ERROR"
            + " | " + fullPath
            + " | " + fileObj.error
          ));

          return reject(fileObj.error);

        }

        console.log(chalkError(getTimeStamp()
          + " | ... SKIP LOAD FILE FROM DROPBOX"
          + " | " + fullPath
        ));
        resolve();

      });

     }
    else {

      dropboxClient.filesDownload({path: fullPath}).
      then(function(data) {

        debug(chalkLog(getTimeStamp()
          + " | LOADING FILE FROM DROPBOX FILE: " + fullPath
        ));

        if (params.file.match(/\.json$/gi)) {

          const payload = data.fileBinary;

          if (!payload || (payload === undefined)) {
            return reject(new Error("GTS LOAD FILE PAYLOAD UNDEFINED"));
          }

          const fileObj = JSONParse(payload);

          if (fileObj.value) {
            return resolve(fileObj.value);
          }

          console.log(chalkError("GTS | DROPBOX loadFile ERROR: " + fullPath));
          return reject(fileObj.error);
        }
        else {
          resolve();
        }
      }).
      catch(function(error) {

        console.log(chalkError("GTS | DROPBOX loadFile ERROR: " + fullPath));
        
        if ((error.status === 409) || (error.status === 404)) {
          console.log(chalkError("GTS | !!! DROPBOX READ FILE " + fullPath + " NOT FOUND"
            + " ... SKIPPING ...")
          );
          return reject(error);
        }
        
        if (error.status === 0) {
          console.log(chalkError("GTS | !!! DROPBOX NO RESPONSE"
            + " ... NO INTERNET CONNECTION? ... SKIPPING ..."));
          return reject(error);
        }

        reject(error);

      });
    }
  });
}

function getFileMetadata(params) {

  return new Promise(function(resolve, reject){

    const fullPath = params.folder + "/" + params.file;

    debug(chalkInfo("FOLDER " + params.folder));
    debug(chalkInfo("FILE " + params.file));
    debug(chalkInfo("getFileMetadata FULL PATH: " + fullPath));

    if (configuration.offlineMode) {
      dropboxClient = dropboxLocalClient;
    }
    else {
      dropboxClient = dropboxRemoteClient;
    }

    dropboxClient.filesGetMetadata({path: fullPath}).
      then(function(response) {
        debug(chalkInfo("FILE META\n" + jsonPrint(response)));
        return resolve(response);
      }).
      catch(function(err) {

        console.log(chalkError("GTS | DROPBOX getFileMetadata ERROR: " + fullPath));
        console.log(chalkError("GTS | !!! DROPBOX READ " + fullPath + " ERROR"));

        if ((err.status === 404) || (err.status === 409)) {
          console.error(chalkError("GTS | !!! DROPBOX READ FILE " + fullPath + " NOT FOUND"));
          if (params.skipOnNotFound) {
            return resolve(false);
          }
          return reject(err);
        }

        if (err.status === 0) {
          console.error(chalkError("GTS | !!! DROPBOX NO RESPONSE"
            + " ... NO INTERNET CONNECTION? ... SKIPPING ..."));
        }
        
        return reject(err);
      });

  });
}

function initStdIn(){

  return new Promise(function(resolve){

    console.log("GTS | STDIN ENABLED");

    stdin = process.stdin;
    if(stdin.setRawMode !== undefined) {
      stdin.setRawMode( true );
    }
    stdin.resume();
    stdin.setEncoding( "utf8" );
    stdin.on( "data", function( key ){

      switch (key) {
        case "\u0003":
          process.exit();
        break;
        case "v":
          configuration.verbose = !configuration.verbose;
          console.log(chalkRedBold("GTS | VERBOSE: " + configuration.verbose));
        break;
        case "q":
          quit();
        break;
        case "Q":
          quit();
        break;
        case "s":
          showStats();
        break;
        case "S":
          showStats(true);
        break;
        default:
          console.log(chalkAlert(
            "\n" + "q/Q: quit"
            + "\n" + "s: showStats"
            + "\n" + "S: showStats verbose"
            + "\n" + "v: verbose log"
          ));
      }
    });

    resolve(stdin);

  });
}

function loadCommandLineArgs(){

  return new Promise(function(resolve){

    statsObj.status = "LOAD COMMAND LINE ARGS";

    const commandLineConfigKeys = Object.keys(commandLineConfig);

    commandLineConfigKeys.forEach(function(arg){
      configuration[arg] = commandLineConfig[arg];
      console.log("GTS | --> COMMAND LINE CONFIG | " + arg + ": " + configuration[arg]);
    });

    statsObj.commandLineArgsLoaded = true;
    resolve(commandLineConfig);

  });
}

async function loadConfigFile(params) {

  if (params.file === dropboxConfigDefaultFile) {
    prevConfigFileModifiedMoment = moment(prevDefaultConfigFileModifiedMoment);
  }
  else {
    prevConfigFileModifiedMoment = moment(prevHostConfigFileModifiedMoment);
  }

  if (configuration.offlineMode) {
    await loadCommandLineArgs();
    return;
  }

  const fullPath = params.folder + "/" + params.file;

  const response = await getFileMetadata(params);

  let fileModifiedMoment;
    
  if (response) {

    fileModifiedMoment = moment(new Date(response.client_modified));

    if (fileModifiedMoment.isSameOrBefore(prevConfigFileModifiedMoment)){

      console.log(chalkLog("GTS | CONFIG FILE BEFORE OR EQUAL"
        + " | " + fullPath
        + " | PREV: " + prevConfigFileModifiedMoment.format(compactDateTimeFormat)
        + " | " + fileModifiedMoment.format(compactDateTimeFormat)
      ));
      return;
    }

    console.log(chalkAlert("GTS | +++ CONFIG FILE AFTER ... LOADING"
      + " | " + fullPath
      + " | PREV: " + prevConfigFileModifiedMoment.format(compactDateTimeFormat)
      + " | " + fileModifiedMoment.format(compactDateTimeFormat)
    ));

    prevConfigFileModifiedMoment = moment(fileModifiedMoment);

    if (params.file === dropboxConfigDefaultFile) {
      prevDefaultConfigFileModifiedMoment = moment(fileModifiedMoment);
    }
    else {
      prevHostConfigFileModifiedMoment = moment(fileModifiedMoment);
    }

    const loadedConfigObj = await loadFileRetry(params);

    if ((loadedConfigObj === undefined) || !loadedConfigObj) {
      console.log(chalkError("GTS | DROPBOX CONFIG LOAD FILE ERROR | JSON UNDEFINED ??? "));
      throw new Error("LOAD FILE JSON UNDEFINED: " + params.file);
    }

    console.log(chalkInfo("GTS | LOADED CONFIG FILE: " + fullPath + "\n" + jsonPrint(loadedConfigObj)));

    const newConfiguration = {};
    newConfiguration.evolve = {};

    if (loadedConfigObj.GTS_TEST_MODE !== undefined){
      console.log("GTS | LOADED GTS_TEST_MODE: " + loadedConfigObj.GTS_TEST_MODE);

      if ((loadedConfigObj.GTS_TEST_MODE === true) || (loadedConfigObj.GTS_TEST_MODE === "true")) {
        newConfiguration.testMode = true;
      }
      else if ((loadedConfigObj.GTS_TEST_MODE === false) || (loadedConfigObj.GTS_TEST_MODE === "false")) {
        newConfiguration.testMode = false;
      }
      else {
        newConfiguration.testMode = false;
      }

      console.log("GTS | LOADED newConfiguration.testMode: " + newConfiguration.testMode);
    }


    if (loadedConfigObj.GTS_OFFLINE_MODE !== undefined){
      console.log("GTS | LOADED GTS_OFFLINE_MODE: " + loadedConfigObj.GTS_OFFLINE_MODE);

      if ((loadedConfigObj.GTS_OFFLINE_MODE === false) || (loadedConfigObj.GTS_OFFLINE_MODE === "false")) {
        newConfiguration.offlineMode = false;
      }
      else if ((loadedConfigObj.GTS_OFFLINE_MODE === true) || (loadedConfigObj.GTS_OFFLINE_MODE === "true")) {
        newConfiguration.offlineMode = true;
      }
      else {
        newConfiguration.offlineMode = false;
      }
    }

    if (loadedConfigObj.GTS_QUIT_ON_COMPLETE !== undefined) {
      console.log("GTS | LOADED GTS_QUIT_ON_COMPLETE: " + loadedConfigObj.GTS_QUIT_ON_COMPLETE);
      if (!loadedConfigObj.GTS_QUIT_ON_COMPLETE || (loadedConfigObj.GTS_QUIT_ON_COMPLETE === "false")) {
        newConfiguration.quitOnComplete = false;
      }
      else {
        newConfiguration.quitOnComplete = true;
      }
    }

    if (loadedConfigObj.GTS_VERBOSE_MODE !== undefined){
      console.log("GTS | LOADED GTS_VERBOSE_MODE: " + loadedConfigObj.GTS_VERBOSE_MODE);
      newConfiguration.verbose = loadedConfigObj.GTS_VERBOSE_MODE;
    }

    if (loadedConfigObj.GTS_ENABLE_STDIN !== undefined){
      console.log("GTS | LOADED GTS_ENABLE_STDIN: " + loadedConfigObj.GTS_ENABLE_STDIN);
      newConfiguration.enableStdin = loadedConfigObj.GTS_ENABLE_STDIN;
    }

    return newConfiguration;
  }

  console.log(chalkAlert("GTS | ??? CONFIG FILE NOT FOUND ... SKIPPING | " + fullPath ));
  return false;
}

async function loadAllConfigFiles(){

  statsObj.status = "LOAD CONFIG";

  console.log(chalkLog("GTS | LOAD ALL CONFIG FILES"));

  const defaultConfig = await loadConfigFile({folder: dropboxConfigDefaultFolder, file: dropboxConfigDefaultFile, skipOnNotFound: true});

  if (defaultConfig) {
    defaultConfiguration = defaultConfig;
    console.log(chalkAlert("GTS | +++ LOADED DEFAULT CONFIG " + dropboxConfigDefaultFolder + "/" + dropboxConfigDefaultFile));
  }

  const hostConfig = await loadConfigFile({folder: dropboxConfigHostFolder, file: dropboxConfigHostFile, skipOnNotFound: true});

  if (hostConfig) {
    hostConfiguration = hostConfig;
    console.log(chalkAlert("GTS | +++ LOADED HOST CONFIG " + dropboxConfigHostFolder + "/" + dropboxConfigHostFile));
  }

  const defaultAndHostConfig = merge(defaultConfiguration, hostConfiguration); // host settings override defaults
  console.log("defaultAndHostConfig.testMode: " + defaultAndHostConfig.testMode);
  const tempConfig = merge(configuration, defaultAndHostConfig); // any new settings override existing config
  console.log("tempConfig.testMode: " + tempConfig.testMode);

  return tempConfig;
}

console.log(chalkInfo("GTS | " + getTimeStamp() 
  + " | WAIT 5 SEC FOR MONGO BEFORE INITIALIZE CONFIGURATION"
));

configEvents.once("INIT_MONGODB", function(){

  console.log(chalkLog("GTS | INIT_MONGODB"));
});

function encodeHistogramUrls(params){
  return new Promise(function(resolve, reject){

    const user = params.user;

    async.eachSeries(["histograms", "profileHistograms", "tweetHistograms"], function(histogram, cb0){

      const urls = objectPath.get(user, [histogram, "urls"]);

      if (urls) {

        debug("URLS\n" + jsonPrint(urls));

        async.eachSeries(Object.keys(urls), function(url, cb1){

          if (validUrl.isUri(url)){
            const urlB64 = btoa(url);
            debug(chalkAlert("HISTOGRAM " + histogram + ".urls | " + url + " -> " + urlB64));
            urls[urlB64] = urls[url];
            delete urls[url];
            return cb1();
          }

          if (url === "url") {
            debug(chalkAlert("HISTOGRAM " + histogram + ".urls | XXX URL: " + url));
            delete urls[url];
            return cb1();
          }

          if (validUrl.isUri(atob(url))) {
            debug(chalkGreen("HISTOGRAM " + histogram + ".urls | IS B64: " + url));
            return cb1();
          }


          const httpsUrl = "https://" + url;

          if (validUrl.isUri(httpsUrl)){
            const urlB64 = btoa(httpsUrl);
            debug(chalkAlert("HISTOGRAM " + histogram + ".urls | " + httpsUrl + " -> " + urlB64));
            urls[urlB64] = urls[url];
            delete urls[url];
            return cb1();
          }

          debug(chalkAlert("HISTOGRAM " + histogram + ".urls |  XXX NOT URL NOR B64: " + url));

          delete urls[url];
          cb1();

        }, function(err){
          if (err) {
            return cb0(err);
          }
          if (Object.keys(urls).length > 0){
            debug("CONVERTED URLS | @" + user.screenName + "\n" + jsonPrint(urls));
          }
          user[histogram].urls = urls;
          cb0();
        });

      }
      else {
        cb0();
      }

    }, function(err){
      if (err) {
        return reject(err);
      }
      resolve(user);
    });

  });
}

function resetMaxInputsHashMap(){
  return new Promise(function(resolve){

    console.log(chalkInfo("GTS | RESET MAX INPUT HASHMAP"));

    DEFAULT_INPUT_TYPES.forEach(function(type){
      maxInputHashMap[type] = {};
    });

    resolve();

  });
}

async function updateMaxInputHashMap(params){

  const mergedHistograms = await mergeHistograms.merge({ histogramA: params.user.profileHistograms, histogramB: params.user.tweetHistograms });
  const histogramTypes = Object.keys(mergedHistograms);

  async.each(histogramTypes, function(type, cb0){

    if (type === "sentiment") { return cb0(); }

    if (!maxInputHashMap[type] || maxInputHashMap[type] === undefined) { maxInputHashMap[type] = {}; }

    const histogramTypeEntities = Object.keys(mergedHistograms[type]);

    async.each(histogramTypeEntities, function(entity, cb1){

      if (mergedHistograms[type][entity] === undefined){
        return cb1();
      }

      if (maxInputHashMap[type][entity] === undefined){
        maxInputHashMap[type][entity] = Math.max(1, mergedHistograms[type][entity]);
        return cb1();
      }

      maxInputHashMap[type][entity] = Math.max(maxInputHashMap[type][entity], mergedHistograms[type][entity]);
      cb1();

    }, function(err){
      if (err) { throw err; }
      cb0();
    });

  }, function(err){
    if (err) { throw err; }
    return;
  });
}

async function updateCategorizedUser(params){

  if (!params.nodeId || params.nodeId === undefined) {
    console.error(chalkError("GTS | *** UPDATE CATEGORIZED USERS: NODE ID UNDEFINED"));
    statsObj.errors.users.findOne += 1;
    throw new Error("NODE ID UNDEFINED");
  }

  try {
    const user = await global.globalUser.findOne({ nodeId: params.nodeId }).lean();

    if (!user || user === undefined){
      console.log(chalkLog("GTS | *** UPDATE CATEGORIZED USERS: USER NOT FOUND: NID: " + params.nodeId));
      statsObj.users.notFound += 1;
      statsObj.users.notCategorized += 1;
      throw new Error("USER NOT FOUND | NODE ID: " + params.nodeId);
    }

    if (!user.category || user.category === undefined) {
      console.log(chalkError("GTS | *** UPDATE CATEGORIZED USERS: USER CATEGORY UNDEFINED\n" + jsonPrint(user)));
      statsObj.users.notCategorized += 1;
      throw new Error("USER NOT CATEGORIZED | NODE ID: " + params.nodeId);
    }

    if (user.screenName === undefined) {
      console.log(chalkError("GTS | *** UPDATE CATEGORIZED USERS: USER SCREENNAME UNDEFINED\n" + jsonPrint(user)));
      statsObj.users.screenNameUndefined += 1;
      statsObj.users.notCategorized += 1;
      throw new Error("USER SCREENNAME UNDEFINED | NODE ID: " + params.nodeId);
    }

    await updateMaxInputHashMap({user: user});

    if (!user.profileHistograms || (user.profileHistograms === undefined)){
      user.profileHistograms = {};
    }

    if (user.profileHistograms.sentiment && (user.profileHistograms.sentiment !== undefined)) {

      if (user.profileHistograms.sentiment.magnitude !== undefined){
        if (user.profileHistograms.sentiment.magnitude < 0){
          console.log(chalkAlert("GTS | !!! NORMALIZATION MAG LESS THAN 0 | CLAMPED: " + user.profileHistograms.sentiment.magnitude));
          user.profileHistograms.sentiment.magnitude = 0;
        }
        statsObj.normalization.magnitude.max = Math.max(statsObj.normalization.magnitude.max, user.profileHistograms.sentiment.magnitude);
      }

      if (user.profileHistograms.sentiment.score !== undefined){
        if (user.profileHistograms.sentiment.score < -1.0){
          console.log(chalkAlert("GTS | !!! NORMALIZATION SCORE LESS THAN -1.0 | CLAMPED: " + user.profileHistograms.sentiment.score));
          user.profileHistograms.sentiment.score = -1.0;
        }

        if (user.profileHistograms.sentiment.score > 1.0){
          console.log(chalkAlert("GTS | !!! NORMALIZATION SCORE GREATER THAN 1.0 | CLAMPED: " + user.profileHistograms.sentiment.score));
          user.profileHistograms.sentiment.score = 1.0;
        }

        statsObj.normalization.score.max = Math.max(statsObj.normalization.score.max, user.profileHistograms.sentiment.score);
        statsObj.normalization.score.min = Math.min(statsObj.normalization.score.min, user.profileHistograms.sentiment.score);
      }

      if (user.profileHistograms.sentiment.comp !== undefined){
        statsObj.normalization.comp.max = Math.max(statsObj.normalization.comp.max, user.profileHistograms.sentiment.comp);
        statsObj.normalization.comp.min = Math.min(statsObj.normalization.comp.min, user.profileHistograms.sentiment.comp);
      }
    }

    let classText = "";
    let currentChalk = chalkLog;

    switch (user.category) {
      case "left":
        categorizedUserHistogram.left += 1;
        classText = "L";
        currentChalk = chalk.blue;
      break;
      case "right":
        categorizedUserHistogram.right += 1;
        classText = "R";
        currentChalk = chalk.yellow;
      break;
      case "neutral":
        categorizedUserHistogram.neutral += 1;
        classText = "N";
        currentChalk = chalk.black;
      break;
      case "positive":
        categorizedUserHistogram.positive += 1;
        classText = "+";
        currentChalk = chalk.green;
      break;
      case "negative":
        categorizedUserHistogram.negative += 1;
        classText = "-";
        currentChalk = chalk.red;
      break;
      default:
        categorizedUserHistogram.none += 1;
        classText = "O";
        currentChalk = chalk.bold.gray;
    }

    debug(chalkLog("\n==============================\n"));
    debug(currentChalk("ADD  | U"
      + " | " + classText
      + " | " + user.screenName
      + " | " + user.nodeId
      + " | " + user.name
      + " | 3C FOLLOW: " + user.threeceeFollowing
      + " | FLLWs: " + user.followersCount
      + " | FRNDs: " + user.friendsCount
    ));

    statsObj.users.updatedCategorized += 1;

    categorizedUsersPercent = 100 * (statsObj.users.notCategorized + statsObj.users.updatedCategorized)/categorizedNodeIdsArray.length;
    categorizedUsersElapsed = (moment().valueOf() - categorizedUsersStartMoment.valueOf()); // mseconds
    categorizedUsersRate = categorizedUsersElapsed/statsObj.users.updatedCategorized; // msecs/userCategorized
    categorizedUsersRemain = (categorizedNodeIdsArray.length - (statsObj.users.notCategorized + statsObj.users.updatedCategorized)) * categorizedUsersRate; // mseconds
    categorizedUsersEndMoment = moment();
    categorizedUsersEndMoment.add(categorizedUsersRemain, "ms");

    if ((statsObj.users.notCategorized + statsObj.users.updatedCategorized) % 1000 === 0){

      console.log(chalkLog("GTS"
        + " | START: " + categorizedUsersStartMoment.format(compactDateTimeFormat)
        + " | ELAPSED: " + msToTime(categorizedUsersElapsed)
        + " | REMAIN: " + msToTime(categorizedUsersRemain)
        + " | ETC: " + categorizedUsersEndMoment.format(compactDateTimeFormat)
        + " | " + (statsObj.users.notCategorized + statsObj.users.updatedCategorized) + "/" + categorizedNodeIdsArray.length
        + " (" + categorizedUsersPercent.toFixed(1) + "%)"
        + " USERS CATEGORIZED"
      ));

      categorizedUserHistogramTotal();

      console.log(chalkLog("GTS | CL U HIST"
        + " | TOTAL: " + categorizedUserHistogram.total
        + " | L: " + categorizedUserHistogram.left 
        + " | R: " + categorizedUserHistogram.right
        + " | N: " + categorizedUserHistogram.neutral
        + " | +: " + categorizedUserHistogram.positive
        + " | -: " + categorizedUserHistogram.negative
        + " | 0: " + categorizedUserHistogram.none
      ));
    }

    const updatedUser = await encodeHistogramUrls({user: user});

    const subUser = pick(
      updatedUser,
      [
        "userId", 
        "screenName", 
        "nodeId", 
        "name",
        "lang",
        "statusesCount",
        "followersCount",
        "friendsCount",
        "friends",
        "languageAnalysis", 
        "category", 
        "categoryAuto", 
        "histograms", 
        "profileHistograms", 
        "tweetHistograms", 
        "location", 
        "ignored", 
        "following", 
        "threeceeFollowing"
      ]
    );

    trainingSetUsersArray.push(subUser);

    return updatedUser;
  }
  catch(err){
    console.error(chalkError("GTS | *** UPDATE CATEGORIZED USER ERROR: " + err));
    statsObj.errors.users.findOne += 1;
    throw err;
  }
}

function updateCategorizedUsers(){

  return new Promise(function(resolve, reject){

    console.log(chalkInfo("GTS | ... UPDATING " + categorizedNodeIdsArray.length + " CATEGORIZED USERS ..."));

    statsObj.status = "UPDATE CATEGORIZED USERS";

    // try {
    //   await resetMaxInputsHashMap();
    // }
    // catch(err){
    //   console.log(chalkError("GTS | *** RESET MAX INPUT HASHMAP ERROR: " + err));
    //   throw err;
    // }

    if (configuration.testMode) {
      categorizedNodeIdsArray.length = Math.min(categorizedNodeIdsArray.length, configuration.maxTestCount);
      console.log(chalkAlert("GTS | *** TEST MODE *** | CATEGORIZE MAX " + categorizedNodeIdsArray.length + " USERS"));
    }

    statsObj.normalization.magnitude.max = -Infinity;
    statsObj.normalization.score.min = 1.0;
    statsObj.normalization.score.max = -1.0;
    statsObj.normalization.comp.min = Infinity;
    statsObj.normalization.comp.max = -Infinity;
    statsObj.users.updatedCategorized = 0;
    statsObj.users.notCategorized = 0;

    let userIndex = 0;

    async.eachSeries(categorizedNodeIdsArray, function(nodeId, cb){

      updateCategorizedUser({nodeId: nodeId})
      .then(function(user){
        userIndex += 1;

        if (configuration.verbose || configuration.testMode) {
          console.log(chalkInfo("GTS | UPDATE CL USR <DB"
            + " [" + userIndex + "/" + categorizedNodeIdsArray.length + "]"
            + " | " + user.nodeId
            + " | @" + user.screenName
          ));
        }
        cb();
      })
      .catch(function(err){
        return cb(err);
      });
    }, function(err){

      if (err) {
        if (err === "INTERRUPT") {
          console.log(chalkAlert("GTS | INTERRUPT"));
        }
        else {
          console.log(chalkError("GTS | UPDATE CATEGORIZED USERS ERROR: " + err));
        }

        return reject(err);
      }

      categorizedUsersPercent = 100 * (statsObj.users.notCategorized + statsObj.users.updatedCategorized)/categorizedNodeIdsArray.length;
      categorizedUsersElapsed = (moment().valueOf() - categorizedUsersStartMoment.valueOf()); // mseconds
      categorizedUsersRate = categorizedUsersElapsed/statsObj.users.updatedCategorized; // msecs/userCategorized
      categorizedUsersRemain = (categorizedNodeIdsArray.length - (statsObj.users.notCategorized + statsObj.users.updatedCategorized)) * categorizedUsersRate; // mseconds
      categorizedUsersEndMoment = moment();
      categorizedUsersEndMoment.add(categorizedUsersRemain, "ms");

      console.log(chalkBlueBold("\nGTS | END CATEGORIZE USERS ============================================== "
        + "\nGTS | ==== START:   " + categorizedUsersStartMoment.format(compactDateTimeFormat)
        + "\nGTS | ==== ELAPSED: " + msToTime(categorizedUsersElapsed)
        + "\nGTS | ==== REMAIN:  " + msToTime(categorizedUsersRemain)
        + "\nGTS | ==== ETC:     " + categorizedUsersEndMoment.format(compactDateTimeFormat)
        + "\nGTS | ==== USERS:   " + (statsObj.users.notCategorized + statsObj.users.updatedCategorized) + "/" + categorizedNodeIdsArray.length
        + " (" + categorizedUsersPercent.toFixed(1) + "%)" + " CATEGORIZED"
      ));

      categorizedUserHistogramTotal();

      console.log(chalkLog("GTS | CL U HIST"
        + " | TOTAL: " + categorizedUserHistogram.total
        + " | L: " + categorizedUserHistogram.left 
        + " | R: " + categorizedUserHistogram.right
        + " | N: " + categorizedUserHistogram.neutral
        + " | +: " + categorizedUserHistogram.positive
        + " | -: " + categorizedUserHistogram.negative
        + " | 0: " + categorizedUserHistogram.none
      ));

      console.log(chalkLog("GTS | CL U HIST | NORMALIZATION"
        + " | MAG " + statsObj.normalization.magnitude.min.toFixed(5) + " MIN / " + statsObj.normalization.magnitude.max.toFixed(5) + " MAX"
        + " | SCORE " + statsObj.normalization.score.min.toFixed(5) + " MIN / " + statsObj.normalization.score.max.toFixed(5) + " MAX"
        + " | COMP " + statsObj.normalization.comp.min.toFixed(5) + " MIN / " + statsObj.normalization.comp.max.toFixed(5) + " MAX"
      ));

      resolve();
    });

  });

}

function initCategorizedNodeIds(){

  return new Promise(function(resolve, reject){

    statsObj.status = "INIT CATEGORIZED NODE IDS";

    console.log("GTS | ... INIT CATEGORIZED NODE IDs ...");

    // const query = (params.query) ? params.query : { $or: [ { "category": { $nin: [ false, null ] } } , { "categoryAuto": { $nin: [ false, null ] } } ] };

    const p = {};

    p.skip = 0;
    p.limit = DEFAULT_FIND_CAT_USER_CURSOR_LIMIT;
    p.batchSize = DEFAULT_CURSOR_BATCH_SIZE;
    p.query = { 
      "$and": [{ "ignored": { "$nin": [true, "true"] } }, { "category": { "$in": ["left", "right", "neutral"] } }]
    };

    let more = true;
    let totalCount = 0;
    let totalManual = 0;
    let totalAuto = 0;
    let totalMatched = 0;
    let totalMismatched = 0;
    let totalMatchRate = 0;

    async.whilst(

      function test(cbTest) { cbTest(null, more); },

      function(cb){

        userServerController.findCategorizedUsersCursor(p, function(err, results){

          if (err) {
            console.error(chalkError("GTS | ERROR: initCategorizedNodeIds: " + err));
            cb(err);
          }
          else if (configuration.testMode && (totalCount >= configuration.maxTestCount)) {

            more = false;

            console.log(chalkLog("GTS | +++ LOADED CATEGORIZED USERS FROM DB"
              + " | TOTAL CATEGORIZED: " + totalCount
              + " | LIMIT: " + p.limit
              + " | SKIP: " + p.skip
              + " | " + totalManual + " MAN"
              + " | " + totalAuto + " AUTO"
              + " | " + totalMatched + " MATCHED"
              + " / " + totalMismatched + " MISMATCHED"
              + " | " + totalMatchRate.toFixed(2) + "% MATCHRATE"
            ));

            cb();
          }
          else if (results) {

            more = true;
            totalCount += results.count;
            totalManual += results.manual;
            totalAuto += results.auto;
            totalMatched += results.matched;
            totalMismatched += results.mismatched;

            totalMatchRate = 100*(totalMatched/totalCount);

            Object.keys(results.obj).forEach(function(nodeId){
              if (results.obj[nodeId].category) { 
                // categorizedUserHashmap.set(nodeId, results.obj[nodeId]);
                categorizedNodeIdsSet.add(nodeId);
              }
              else {
                console.log(chalkAlert("GTS | ??? UNCATEGORIZED USER FROM DB\n" + jsonPrint(results.obj[nodeId])));
              }
            });

            if (configuration.verbose || (totalCount % 1000 === 0)) {

              console.log(chalkLog("GTS | ... LOADING CATEGORIZED USERS FROM DB"
                + " | TOTAL: " + totalCount
                + " | " + totalManual + " MAN"
                + " | " + totalAuto + " AUTO"
                + " | " + totalMatched + " MATCHED"
                + " / " + totalMismatched + " MISMATCHED"
                + " | " + totalMatchRate.toFixed(2) + "% MATCHRATE"
              ));

            }

            p.skip += results.count;

            cb();
          }
          else {

            more = false;

            console.log(chalkLog("GTS | LOADING CATEGORIZED USERS FROM DB"
              + " | TOTAL: " + totalCount
              + " | " + totalManual + " MAN"
              + " | " + totalAuto + " AUTO"
              + " | " + totalMatched + " MATCHED"
              + " / " + totalMismatched + " MISMATCHED"
              + " | " + totalMatchRate.toFixed(2) + "% MATCHRATE"
            ));

            cb();
          }

        });

      },

      function(err){
        if (err) {
          console.log(chalkError("GTS | INIT CATEGORIZED USER HASHMAP ERROR: " + err + "\n" + jsonPrint(err)));
          return reject(err);
        }

        categorizedNodeIdsArray = [...categorizedNodeIdsSet];

        resolve();
      }
    );

  });
}

function archiveUsers(){

  return new Promise(function(resolve, reject){

    if (archive === undefined) { 
      return reject(new Error("ARCHIVE UNDEFINED"));
    }

    let usersAppended = 0;
    const totalUsers = trainingSetUsersArray.length;
    let percentAppended = 0;

    console.log(chalkLog("GTS | ... START ARCHIVE " + totalUsers + " USERS ..."));

    async.eachSeries(trainingSetUsersArray, function(user, cb){

      const userFile = "user_" + user.userId + ".json";
      const userBuffer = Buffer.from(JSON.stringify(user));

      archive.append(userBuffer, { name: userFile });

      usersAppended += 1;
      percentAppended = 100 * usersAppended/totalUsers;

      if (configuration.verbose || (usersAppended % 1000 === 0)) {

        console.log(chalkLog("GTS | ARCHIVE"
          + " | " + usersAppended 
          + "/" + trainingSetUsersArray.length
          + " (" + percentAppended.toFixed(2) + "%) USERS APPENDED"
        ));
      }

      cb();

    }, function(err){
        if (err) {
          return reject(err);
        }
        resolve();
    });

    // async.whilst(

    //   function test(cbTest) { cbTest(null, more); },

    //   function(cb){

    //     const user = trainingSetUsersArray.shift();

    //     more = (trainingSetUsersArray.length > 0);

    //     const userFile = "user_" + user.userId + ".json";
    //     const userBuffer = Buffer.from(JSON.stringify(user));

    //     archive.append(userBuffer, { name: userFile });

    //     usersAppended += 1;
    //     percentAppended = 100 * usersAppended/totalUsers;

    //     if (configuration.verbose || (usersAppended % 1000 === 0)) {

    //       console.log(chalkLog("GTS | ARCHIVE"
    //         + " | " + usersAppended 
    //         + "/" + trainingSetUsersArray.length
    //         + " (" + percentAppended.toFixed(2) + "%) USERS APPENDED"
    //       ));

    //     }

    //     // async.setImmediate(function() {
    //       cb();
    //     // });

    //   }, 

    //   function(err){
    //     if (err) {
    //       return reject(err);
    //     }
    //     resolve();
    //   }
    // );


  });
}

async function generateGlobalTrainingTestSet(){

  statsObj.status = "GENERATE TRAINING SET";

  console.log(chalkBlueBold("GTS | ==================================================================="));
  console.log(chalkBlueBold("GTS | GENERATE TRAINING SET | " + getTimeStamp()));
  console.log(chalkBlueBold("GTS | ==================================================================="));

  await initCategorizedNodeIds();
  await updateCategorizedUsers();

  await initArchiver();
  await archiveUsers();

  console.log("generateGlobalTrainingTestSet | after archiveUsers");

  const mihmObj = {};

  mihmObj.maxInputHashMap = {};
  mihmObj.maxInputHashMap = maxInputHashMap;

  mihmObj.normalization = {};
  mihmObj.normalization = statsObj.normalization;

  let maxInputHashMapFile = "maxInputHashMap.json";

  if (configuration.testMode) { maxInputHashMapFile = "maxInputHashMapFile_test.json"; }

  console.log(MODULE_ID_PREFIX
    + " | ... SAVING MAX INPUT HASHMAP FILE | "
    + configuration.trainingSetsFolder + "/" + maxInputHashMapFile
  );

  await saveFile({folder: configuration.trainingSetsFolder, file: maxInputHashMapFile, obj: mihmObj });

  const buf = Buffer.from(JSON.stringify(mihmObj));

  archive.append(buf, { name: maxInputHashMapFile });
  archive.finalize();

  return;
}

slackText = "\n*GTS START | " + hostname + "*";
slackText = slackText + "\n" + getTimeStamp();

slackPostMessage(slackChannel, slackText);


function getFileLock(params){

  return new Promise(function(resolve, reject){

    try {

      console.log(chalkBlue("GTS | ... GET LOCK FILE | " + params.file));

      lockFile.lock(params.file, params.options, function(err){

        if (err) {
          console.log(chalkError("GTS | *** FILE LOCK FAIL: " + params.file + "\n" + err));
          // return reject(err);
          return resolve(false);
        }

        console.log(chalkGreen("GTS | +++ FILE LOCK: " + params.file));
        resolve(true);
      });

    }
    catch(err){
      console.log(chalkError("GTS | *** GET FILE LOCK ERROR: " + err));
      return reject(err);
    }

  });
}

function delay(params){
  return new Promise(function(resolve){

    if (params.message) {
      console.log(chalkLog("GTS | " + params.message + " | PERIOD: " + msToTime(params.period)));
    }
    setTimeout(function(){
      resolve(true);
    }, params.period);
  });
}

async function releaseFileLock(params){

  const delayPeriod = params.delay || 5;

  await delay(delayPeriod);

  const fileIsLocked = lockFile.checkSync(params.file);

  if (!fileIsLocked) {
    return true;
  }

  lockFile.unlock(params.file, function(err){

    if (err) {
      console.log(chalkError("GTS | *** FILE UNLOCK FAIL: " + params.file + "\n" + err));
      throw err;
    }

    console.log(chalkLog("GTS | --- FILE UNLOCK: " + params.file));
    return true;

  });
}

configEvents.on("ARCHIVE_OUTPUT_CLOSED", async function(userArchivePath){

  try{

    await delay({message: "... WAIT FOR DROPBOX FILE SYNC | " + userArchivePath, period: ONE_MINUTE});

    await releaseFileLock({file: userArchivePath + ".lock"});

    const stats = fs.statSync(userArchivePath);
    const fileSizeInBytes = stats.size;
    const savedSize = fileSizeInBytes/ONE_MEGABYTE;

    if (configuration.testMode) {
      configuration.userArchiveFile = configuration.userArchiveFile.replace(/\.zip/, "_test.zip");
      configuration.archiveFileUploadCompleteFlagFile = configuration.archiveFileUploadCompleteFlagFile.replace(/\.json/, "_test.json");
    }

    console.log(chalkLog("GTS | ... SAVING FLAG FILE" 
      + " | " + configuration.userArchiveFolder + "/" + configuration.archiveFileUploadCompleteFlagFile 
      + " | " + fileSizeInBytes + " B | " + savedSize.toFixed(3) + " MB"
    ));


    const fileSizeObj = {};
    fileSizeObj.file = configuration.userArchiveFile;
    fileSizeObj.size = fileSizeInBytes;
    fileSizeObj.histogram = {};
    fileSizeObj.histogram = categorizedUserHistogram;

    await saveFile({folder: configuration.userArchiveFolder, file: configuration.archiveFileUploadCompleteFlagFile, obj: fileSizeObj });

    await delay({message: "... WAIT FOR DROPBOX FLAG FILE SYNC", period: ONE_MINUTE});

    quit("DONE");
  }
  catch(err){
    console.log(chalkError("GTS | *** ARCHIVE_OUTPUT_CLOSED ERROR", err));
    quit();
  }

});

function initArchiver(){

  return new Promise(function(resolve, reject){

    let userArchivePath = configuration.userArchivePath;

    if (configuration.testMode) {
      userArchivePath = configuration.userArchivePath.replace(/\.zip/, "_test.zip");
    }

    console.log(chalkBlue("GTS | ... INIT ARCHIVER | " + userArchivePath));

    if (archive && archive.isOpen) {
      console.log(chalkAlert("GTS | ARCHIVE ALREADY OPEN | " + userArchivePath));
      return resolve();
    }

    const lockFileName = userArchivePath + ".lock";
    // const archiveFileLocked = await getFileLock({file: lockFileName, options: fileLockOptions});

    getFileLock({file: lockFileName, options: fileLockOptions})
    .then(function(archiveFileLocked){

      if (!archiveFileLocked) {
        console.log(chalkAlert("GTS | *** FILE LOCK FAILED | SKIP INIT ARCHIVE: " + userArchivePath));
        statsObj.archiveOpen = false;
        return resolve();
      }

      // create a file to stream archive data to.
      const output = fs.createWriteStream(userArchivePath);

      archive = archiver("zip", {
        zlib: { level: 9 } // Sets the compression level.
      });
       
      output.on("close", function() {
        const archiveSize = toMegabytes(archive.pointer());
        console.log(chalkGreen("GTS | ARCHIVE OUTPUT | CLOSED | " + archiveSize.toFixed(2) + " MB"));
        configEvents.emit("ARCHIVE_OUTPUT_CLOSED", userArchivePath);
      });
       
      output.on("end", function() {
        const archiveSize = toMegabytes(archive.pointer());
        console.log(chalkBlueBold("GTS | ARCHIVE OUTPUT | END | " + archiveSize.toFixed(2) + " MB"));
      });
       
      archive.on("warning", function(err) {
        console.log(chalkAlert("GTS | ARCHIVE | WARNING\n" + jsonPrint(err)));
        if (err.code !== "ENOENT") {
          throw err;
        }
      });
       
      archive.on("progress", function(progress) {

        statsObj.progress = progress;

        statsObj.progressMbytes = toMegabytes(progress.fs.processedBytes);
        statsObj.totalMbytes = toMegabytes(archive.pointer());

        if ((statsObj.progress.entries.processed % 1000 === 0) || configuration.testMode) {
          console.log(chalkLog("GTS | ARCHIVE | PROGRESS"
            + " | TEST MODE: " + configuration.testMode
            + " | " + getTimeStamp()
            + " | ENTRIES: " + statsObj.progress.entries.processed + " PROCESSED / " + statsObj.progress.entries.total + " TOTAL"
            + " (" + (100*statsObj.progress.entries.processed/statsObj.progress.entries.total).toFixed(2) + "%)"
            + " | SIZE: " + statsObj.totalMbytes.toFixed(2) + " MB"
          ));
        }
      });
       
      archive.on("close", function() {
        console.log(chalkBlueBold("GTS | ARCHIVE | CLOSED | " + userArchivePath));
      });
       
      archive.on("finish", function() {

        console.log(chalkBlueBold("GTS | +++ ARCHIVE | FINISHED | " + getTimeStamp()
          + "\nGTS | +++ ARCHIVE | FINISHED | TEST MODE: " + configuration.testMode
          + "\nGTS | +++ ARCHIVE | FINISHED | ARCHIVE:   " + userArchivePath
          + "\nGTS | +++ ARCHIVE | FINISHED | ENTRIES:   " + statsObj.progress.entries.processed + " PROCESSED / " + statsObj.progress.entries.total + " TOTAL"
          + " (" + (100*statsObj.progress.entries.processed/statsObj.progress.entries.total).toFixed(2) + "%)"
          + " | SIZE:      " + statsObj.totalMbytes.toFixed(2) + " MB"
        ));
      });
       
      archive.on("error", function(err) {
        console.log(chalkError("GTS | ARCHIVE | ERROR\n" + jsonPrint(err)));
        throw err;
      });
       
      archive.pipe(output);
      statsObj.archiveOpen = true;
      
      resolve();

    })
    .catch(function(err){
      reject(err);
    });

  });
}

async function initialize(cnf){

  statsObj.status = "INITIALIZE";

  debug(chalkBlue("INITIALIZE cnf\n" + jsonPrint(cnf)));

  if (debug.enabled){
    console.log("\nGTS | %%%%%%%%%%%%%%\nGTS |  DEBUG ENABLED \nGTS | %%%%%%%%%%%%%%\n");
  }

  cnf.processName = process.env.GTS_PROCESS_NAME || "node_gts";
  cnf.runId = process.env.GTS_RUN_ID || statsObj.runId;

  cnf.verbose = process.env.GTS_VERBOSE_MODE || false;
  cnf.quitOnError = process.env.GTS_QUIT_ON_ERROR || false;
  cnf.enableStdin = process.env.GTS_ENABLE_STDIN || true;

  if (process.env.GTS_QUIT_ON_COMPLETE !== undefined) {
    console.log("GTS | ENV GTS_QUIT_ON_COMPLETE: " + process.env.GTS_QUIT_ON_COMPLETE);
    if (!process.env.GTS_QUIT_ON_COMPLETE || (process.env.GTS_QUIT_ON_COMPLETE === false) || (process.env.GTS_QUIT_ON_COMPLETE === "false")) {
      cnf.quitOnComplete = false;
    }
    else {
      cnf.quitOnComplete = true;
    }
  }

  cnf.categorizedUsersFile = process.env.GTS_CATEGORIZED_USERS_FILE || categorizedUsersFile;
  cnf.categorizedUsersFolder = globalCategorizedUsersFolder;

  debug(chalkWarn("dropboxConfigDefaultFolder: " + dropboxConfigDefaultFolder));
  debug(chalkWarn("dropboxConfigDefaultFile  : " + dropboxConfigDefaultFile));

  await initStdIn();
  
  configuration = await loadAllConfigFiles();

  await loadCommandLineArgs();
  
  const configArgs = Object.keys(configuration);

  configArgs.forEach(function(arg){
    if (_.isObject(configuration[arg])) {
      console.log("GTS | _FINAL CONFIG | " + arg + "\n" + jsonPrint(configuration[arg]));
    }
    else {
      console.log("GTS | _FINAL CONFIG | " + arg + ": " + configuration[arg]);
    }
  });
  
  statsObj.commandLineArgsLoaded = true;

  global.globalDbConnection = await connectDb();

  return configuration;
}

setTimeout(async function(){

  try{
    configuration = await initialize(configuration);
    await generateGlobalTrainingTestSet();
  }
  catch(err){
    console.log(chalkError("GTS | *** INITIALIZE ERROR: " + err));
  }

}, 1000);
