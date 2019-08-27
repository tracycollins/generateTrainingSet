/*jslint node: true */
/*jshint sub:true*/

const TEST_MODE_LENGTH = 100;
const DEFAULT_INTERVAL = 0.5;

const catUsersQuery = { 
  "$and": [{ "ignored": { "$nin": [true, "true"] } }, { "category": { "$in": ["left", "right", "neutral"] } }]
};

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
const DEFAULT_QUIT_ON_COMPLETE = false;
const DEFAULT_TEST_RATIO = 0.20;
const MODULE_ID_PREFIX = "GTS";
const MODULE_ID = MODULE_ID_PREFIX + "_node_" + hostname;
const GLOBAL_TRAINING_SET_ID = "globalTrainingSet";

const fileLockOptions = { 
  retries: DEFAULT_FILELOCK_WAIT,
  retryWait: DEFAULT_FILELOCK_RETRY_WAIT,
  stale: DEFAULT_FILELOCK_STALE,
  wait: DEFAULT_FILELOCK_WAIT
};

const path = require("path");
const moment = require("moment");
const lockFile = require("lockfile");
const merge = require("deepmerge");
const archiver = require("archiver");
const fs = require("fs");
const MergeHistograms = require("@threeceelabs/mergehistograms");
const mergeHistograms = new MergeHistograms();
const util = require("util");
const _ = require("lodash");
const sizeof = require("object-sizeof");
const pick = require("object.pick");
const Slack = require("slack-node");
const EventEmitter3 = require("eventemitter3");
const async = require("async");
const ThreeceeUtilities = require("@threeceelabs/threecee-utilities");
const tcUtils = new ThreeceeUtilities("GTS_TCU");
const debug = require("debug")("gts");
const commandLineArgs = require("command-line-args");

let archive;

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

let categorizedUsersPercent = 0;

const maxInputHashMap = {};
const globalhistograms = {};

DEFAULT_INPUT_TYPES.forEach(function(type){
  globalhistograms[type] = {};
  maxInputHashMap[type] = {};
});

const statsObj = {};
let statsObjSmall = {};

statsObj.status = "LOAD";
statsObj.hostname = hostname;
statsObj.pid = process.pid;
statsObj.cpus = os.cpus().length;
statsObj.commandLineArgsLoaded = false;
statsObj.usersAppendedToArchive = 0;
statsObj.usersProcessed = 0;
statsObj.userErrorCount = 0;

statsObj.archiveOpen = false;
statsObj.archiveModifiedMoment = moment("2010-01-01");
statsObj.endAppendUsersFlag = false;

statsObj.archiveEntries = 0;
statsObj.archiveTotal = 0;
statsObj.archiveElapsed = 0;
statsObj.archiveRate = 0;
statsObj.archiveRemainUsers = Infinity;
statsObj.archiveRemainMS = 0;
statsObj.archiveStartMoment = 0;
statsObj.archiveEndMoment = moment();
statsObj.totalMbytes = 0;
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

const slackChannel = "#gts";
let slackText = "";

let initMainInterval;

let configuration = {}; // merge of defaultConfiguration & hostConfiguration
configuration.categorizedNodeIdsQueueInterval = DEFAULT_INTERVAL;
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

configuration.DROPBOX = {};

configuration.DROPBOX.DROPBOX_CONFIG_FILE = process.env.DROPBOX_GTS_CONFIG_FILE || "generateTrainingSetConfig.json";
configuration.DROPBOX.DROPBOX_GTS_STATS_FILE = process.env.DROPBOX_GTS_STATS_FILE || "generateTrainingSetStats.json";

const configDefaultFolder = path.join(DROPBOX_ROOT_FOLDER, "config/utility/default");
const configHostFolder = path.join(DROPBOX_ROOT_FOLDER, "config/utility", hostname);

const localHistogramsFolder = configHostFolder + "/histograms";
const defaultHistogramsFolder = configDefaultFolder + "/histograms";

const configDefaultFile = "default_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;
const configHostFile = hostname + "_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;

configuration.archiveFileUploadCompleteFlagFile = "usersZipUploadComplete.json";
configuration.trainingSetFile = "trainingSet.json";
configuration.requiredTrainingSetFile = "requiredTrainingSet.txt";
configuration.userArchiveFile = hostname + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat) + "_users.zip";


configuration.local = {};
configuration.local.trainingSetsFolder = path.join(configHostFolder, "trainingSets");
configuration.local.histogramsFolder = path.join(configHostFolder, "histograms");
configuration.local.userArchiveFolder = path.join(configHostFolder, "trainingSets/users");
configuration.local.userArchivePath = path.join(configuration.local.userArchiveFolder, configuration.userArchiveFile);

configuration.default = {};
configuration.default.trainingSetsFolder = path.join(configDefaultFolder, "trainingSets");
configuration.default.histogramsFolder = path.join(configDefaultFolder, "histograms");
configuration.default.userArchiveFolder = path.join(configDefaultFolder, "trainingSets/users");
configuration.default.userArchivePath = path.join(configuration.default.userArchiveFolder, configuration.userArchiveFile);

configuration.trainingSetsFolder = configuration[HOST].trainingSetsFolder;
configuration.archiveFileUploadCompleteFlagFolder = path.join(configuration[HOST].trainingSetsFolder, "users");
configuration.histogramsFolder = configuration[HOST].histogramsFolder;
configuration.userArchiveFolder = configuration[HOST].userArchiveFolder;
configuration.userArchivePath = configuration[HOST].userArchivePath;


const slackOAuthAccessToken = "xoxp-3708084981-3708084993-206468961315-ec62db5792cd55071a51c544acf0da55";

function toMegabytes(sizeInBytes) {
  return sizeInBytes/ONE_MEGABYTE;
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

  const offlineMode = configuration.offlineMode || false;

  if (offlineMode) {
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
console.log(chalkInfo("GTS | COMMAND LINE CONFIG\nGTS | " + tcUtils.jsonPrint(commandLineConfig)));
console.log("GTS | COMMAND LINE OPTIONS\nGTS | " + tcUtils.jsonPrint(commandLineConfig));

if (Object.keys(commandLineConfig).includes("help")) {
  console.log("GTS |optionDefinitions\n" + tcUtils.jsonPrint(optionDefinitions));
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
    console.log("GTS | R<\n" + tcUtils.jsonPrint(msg));
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

statsObj.url = {};
statsObj.url.errors = 0;

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

const globalCategorizedUsersFolder = dropboxConfigDefaultFolder + "/categorizedUsers";
const categorizedUsersFile = "categorizedUsers_manual.json";

function showStats(options){

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  statsObjSmall = pick(statsObj, statsPickArray);


  if (options) {
    console.log("GTS | STATS\nGTS | " + tcUtils.jsonPrint(statsObjSmall));
  }
  else {
    console.log(chalkLog("GTS | ============================================================"
      + "\nGTS | S"
      + " | STATUS: " + statsObj.status
      + " | CPUs: " + statsObj.cpus
      + " | " + testObj.testRunId
      + " | RUN " + tcUtils.msToTime(statsObj.elapsed)
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

    console.log(chalkInfo("GTS | ============================================================"
      + "\nGTS | >+- ARCHIVE | PROGRESS"
      + " | TEST: " + configuration.testMode
      + " | " + tcUtils.getTimeStamp()
      // + " | AUQ: " + archiveUserQueue.length
      + " | APPNDD: " + statsObj.usersAppendedToArchive
      + " | ENTRIES PRCSSD/REM/TOT: " + statsObj.usersProcessed + "/" + statsObj.archiveRemainUsers + "/" + statsObj.archiveTotal
      + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
      + " (" + (100*statsObj.usersProcessed/statsObj.archiveTotal).toFixed(2) + "%)"
      + " [ RATE: " + (statsObj.archiveRate/1000).toFixed(3) + " SEC/USER ]"
      + " S: " + tcUtils.getTimeStamp(statsObj.archiveStartMoment)
      + " E: " + tcUtils.msToTime(statsObj.archiveElapsed)
      + " | ETC: " + tcUtils.msToTime(statsObj.archiveRemainMS) + " " + statsObj.archiveEndMoment.format(compactDateTimeFormat)
      + "\nGTS | ============================================================"
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
      slackText = slackText + " | RUN " + tcUtils.msToTime(statsObj.elapsed);
      slackText = slackText + " | QUIT CAUSE: " + options;
      debug("GTS | SLACK TEXT: " + slackText);
      slackPostMessage(slackChannel, slackText);
    }
  }

  showStats();

  setTimeout(function(){
    console.log(chalkBlueBold(
          "GTS | ================"
      + "\nGTS | *** QUIT GTS ***"
      + "\nGTS | ================"
    ));
    process.exit();
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

  const fullPath = path.join(params.folder, params.file);

  try {

    if (configuration.offlineMode) {
      await loadCommandLineArgs();
      return;
    }

    const newConfiguration = {};
    newConfiguration.evolve = {};

    const loadedConfigObj = await tcUtils.loadFile({folder: params.folder, file: params.file, noErrorNotFound: params.noErrorNotFound });

    if (loadedConfigObj === undefined) {
      if (params.noErrorNotFound) {
        console.log(chalkAlert(MODULE_ID_PREFIX + " | ... SKIP LOAD CONFIG FILE: " + params.folder + "/" + params.file));
        return newConfiguration;
      }
      else {
        console.log(chalkError(MODULE_ID_PREFIX + " | *** CONFIG LOAD FILE ERROR | JSON UNDEFINED ??? "));
        throw new Error("JSON UNDEFINED");
      }
    }

    if (loadedConfigObj instanceof Error) {
      console.log(chalkError(MODULE_ID_PREFIX + " | *** CONFIG LOAD FILE ERROR: " + loadedConfigObj));
    }

    console.log(chalkInfo(MODULE_ID_PREFIX + " | LOADED CONFIG FILE: " + params.file + "\n" + tcUtils.jsonPrint(loadedConfigObj)));

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
  catch(err){
    console.error(chalkError(MODULE_ID_PREFIX + " | ERROR LOAD CONFIG: " + fullPath
      + "\n" + tcUtils.jsonPrint(err)
    ));
    throw err;
  }
}

async function loadAllConfigFiles(cnf){

  statsObj.status = "LOAD CONFIG";

  const defaultConfig = await loadConfigFile({folder: configDefaultFolder, file: configDefaultFile});

  if (defaultConfig) {
    defaultConfiguration = defaultConfig;
    console.log(chalkInfo(MODULE_ID_PREFIX + " | <<< LOADED DEFAULT CONFIG " + configDefaultFolder + "/" + configDefaultFile));
  }
  
  const hostConfig = await loadConfigFile({folder: configHostFolder, file: configHostFile, noErrorNotFound: true});

  if (hostConfig) {
    hostConfiguration = hostConfig;
    console.log(chalkInfo(MODULE_ID_PREFIX + " | <<< LOADED HOST CONFIG " + configHostFolder + "/" + configHostFile));
  }
  
  console.log("hostConfiguration\n" + tcUtils.jsonPrint(hostConfiguration));
  console.log("defaultConfiguration\n" + tcUtils.jsonPrint(defaultConfiguration));

  const defaultAndHostConfig = merge(defaultConfiguration, hostConfiguration); // host settings override defaults

  console.log("defaultAndHostConfig\n" + tcUtils.jsonPrint(defaultAndHostConfig));
  console.log("cnf\n" + tcUtils.jsonPrint(cnf));

  const tempConfig = merge(cnf, defaultAndHostConfig); // any new settings override existing config

  console.log("tempConfig\n" + tcUtils.jsonPrint(tempConfig));

  tempConfig.twitterUsers = _.uniq(tempConfig.twitterUsers);

  return tempConfig;
}

configEvents.once("INIT_MONGODB", function(){
  console.log(chalkLog("GTS | INIT_MONGODB"));
});

async function updateMaxInputHashMap(params){

  const histograms = await mergeHistograms.merge({ histogramA: params.user.profileHistograms, histogramB: params.user.tweetHistograms });

  const histogramTypes = Object.keys(histograms);

  for (const type of histogramTypes){

    if (type !== "sentiment") {

      if (!maxInputHashMap[type] || maxInputHashMap[type] === undefined) { maxInputHashMap[type] = {}; }

      const histogramTypeEntities = Object.keys(histograms[type]);

      if (histogramTypeEntities.length > 0) {

        for (const entity of histogramTypeEntities){

          if (histograms[type][entity] !== undefined){

            if (maxInputHashMap[type][entity] === undefined){
              maxInputHashMap[type][entity] = Math.max(1, histograms[type][entity]);
            }
            else{
              maxInputHashMap[type][entity] = Math.max(maxInputHashMap[type][entity], histograms[type][entity]);
            }
          }

        }

      }

    }

  }

  return;
}

async function updateCategorizedUser(params){

  if (!params.nodeId || params.nodeId === undefined) {
    console.error(chalkError("GTS | *** UPDATE CATEGORIZED USERS: NODE ID UNDEFINED"));
    statsObj.errors.users.findOne += 1;
    throw new Error("NODE ID UNDEFINED");
  }

  try {
    const dbUser = await global.globalUser.findOne({ nodeId: params.nodeId }).lean().exec();

    if (!dbUser || dbUser === undefined){
      console.log(chalkLog("GTS | *** UPDATE CATEGORIZED USERS: USER NOT FOUND: NID: " + params.nodeId));
      statsObj.users.notFound += 1;
      statsObj.users.notCategorized += 1;
      return;
    }

    if (!dbUser.category || dbUser.category === undefined) {
      console.log(chalkError("GTS | *** UPDATE CATEGORIZED USERS: USER CATEGORY UNDEFINED | UID: " + dbUser.nodeId));
      statsObj.users.notCategorized += 1;
      return;
    }

    if (dbUser.screenName === undefined) {
      console.log(chalkError("GTS | *** UPDATE CATEGORIZED USERS: USER SCREENNAME UNDEFINED | UID: " + dbUser.nodeId));
      statsObj.users.screenNameUndefined += 1;
      statsObj.users.notCategorized += 1;
      return;
    }

    if (!dbUser.profileHistograms || (dbUser.profileHistograms === undefined)){
      dbUser.profileHistograms = {};
    }

    if (dbUser.friends && (dbUser.friends.length > 5000)){
      dbUser.friends = _.slice(dbUser.friends, 0,5000);
    }

    const user = await tcUtils.encodeHistogramUrls({user: dbUser});

    await updateMaxInputHashMap({user: user});

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

    categorizedUsersPercent = 100 * (statsObj.users.notCategorized + statsObj.users.updatedCategorized)/statsObj.archiveTotal;

    if (configuration.verbose 
      || configuration.testMode 
      || ((statsObj.users.notCategorized + statsObj.users.updatedCategorized) % 1000 === 0)){

      categorizedUserHistogramTotal();

      console.log(chalkLog("GTS | CATEGORIZED"
        + " | " + (statsObj.users.notCategorized + statsObj.users.updatedCategorized) + "/" + statsObj.archiveTotal
        + " (" + categorizedUsersPercent.toFixed(1) + "%)"
        + " | TOTAL: " + categorizedUserHistogram.total
        + " | L: " + categorizedUserHistogram.left 
        + " | R: " + categorizedUserHistogram.right
        + " | N: " + categorizedUserHistogram.neutral
        + " | +: " + categorizedUserHistogram.positive
        + " | -: " + categorizedUserHistogram.negative
        + " | 0: " + categorizedUserHistogram.none
      ));
    }

    return user;

  }
  catch(err){
    console.error(chalkError("GTS | *** UPDATE CATEGORIZED USER ERROR: " + err));
    statsObj.errors.users.findOne += 1;
    throw err;
  }
}

// let archiveUserQueueInterval;
// const archiveUserQueue = [];
// let archiveUserQueueReady = false;

// function initArchiveUserQueue(i){

//   return new Promise(function(resolve){

//     const interval = i || 20;

//     console.log(chalkInfo("GTS | INIT ARCHIVE USER QUEUE"
//       + " | INTERVAL: " + interval + " MS"
//     ));

//     statsObj.status = "ARCHIVE USERS";

//     archiveUserQueueReady = true;

//     archiveUserQueueInterval = setInterval(async function(){

//       if (archiveUserQueueReady && (archiveUserQueue.length > 0)){

//         archiveUserQueueReady = false;

//         const subUser = archiveUserQueue.shift();

//         try{
//           await archiveUser({user: subUser});
//           archiveUserQueueReady = true;
//         }
//         catch(err){
//           console.log(chalkError("GTS | *** ARCHIVE USER ERROR | @" + subUser.screenName + " | ERROR: " + err));
//           archiveUserQueueReady = true;
//         }

//       }

//     }, interval);

//     resolve();

//   });
// }

let categorizedNodeIdsQueueInterval;
const categorizedNodeIdsQueue = [];
let categorizedNodeIdsQueueReady = false;

function initCategorizedNodeIdsQueue(params){

  return new Promise(function(resolve){

    const interval = params.interval || 20;

    console.log(chalkInfo("GTS | INIT CATEGORIZE NODE IDS QUEUE"
      + " | " + statsObj.archiveTotal + " CATEGORIZED USERS"
      + " | INTERVAL: " + interval + " MS"
    ));

    statsObj.status = "UPDATE CATEGORIZED USERS";

    statsObj.normalization.magnitude.max = -Infinity;
    statsObj.normalization.score.min = 1.0;
    statsObj.normalization.score.max = -1.0;
    statsObj.normalization.comp.min = Infinity;
    statsObj.normalization.comp.max = -Infinity;
    statsObj.users.updatedCategorized = 0;
    statsObj.users.notCategorized = 0;

    let userIndex = 0;

    categorizedNodeIdsQueueReady = true;

    categorizedNodeIdsQueueInterval = setInterval(async function(){

      if (categorizedNodeIdsQueueReady && (categorizedNodeIdsQueue.length > 0)){

        categorizedNodeIdsQueueReady = false;

        const nodeId = categorizedNodeIdsQueue.shift();

        try{

          const user = await updateCategorizedUser({nodeId: nodeId});

          if (!user) {

            statsObj.userErrorCount += 1;

            console.log(chalkAlert("GTS | *** UPDATE CL USR NOT FOUND: "
              + " [ CNIDQ: " + categorizedNodeIdsQueue.length + "]"
              + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.userErrorCount + " ]"
              + " | USER ID: " + nodeId
            ));

            categorizedNodeIdsQueueReady = true;
          }
          else {

            userIndex += 1;

            await tcUtils.updateGlobalHistograms({user: user});

            const subUser = pick(
              user,
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

            if (configuration.verbose || configuration.testMode) {
              console.log(chalkInfo("GTS | -<- UPDATE CL USR <DB"
                + " [ CNIDQ: " + categorizedNodeIdsQueue.length + "]"
                + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.userErrorCount + "]"
                + " | " + user.nodeId
                + " | @" + user.screenName
              ));
            }

            if (statsObj.archiveStartMoment === 0) { statsObj.archiveStartMoment = moment(); }

            // archiveUserQueue.push(subUser);
            await archiveUser({user: subUser});

            categorizedNodeIdsQueueReady = true;
          }
        }
        catch(err){
          console.log(chalkError("GTS | *** UPDATE CATEGORIZED USER ERROR | USER ID: " + nodeId + " | ERROR: " + err));
          categorizedNodeIdsQueueReady = true;
        }

      }

    }, interval);

    resolve();

  });
}

function categoryCursor(params){
  return new Promise(function(resolve, reject){

    let more = true;
    let totalCount = 0;
    let totalManual = 0;
    let totalAuto = 0;
    let totalMatched = 0;
    let totalMismatched = 0;
    let totalMatchRate = 0;
    let totalQueued = 0;

    async.whilst(

      function test(cbTest) { cbTest(null, more); },

      function(cb){

        userServerController.findCategorizedUsersCursor(params, function(err, results){

          if (err) {
            console.error(chalkError("GTS | ERROR: initCategorizedNodeIds: " + err));
            cb(err);
          }
          else if (configuration.testMode && (totalCount >= configuration.maxTestCount)) {

            more = false;

            console.log(chalkAlert("GTS | +++ LOADED CATEGORIZED USERS FROM DB"
              + " | *** TEST MODE ***"
              + " [ CNIDQ: " + categorizedNodeIdsQueue.length + "]"
              + " | TOTAL CATEGORIZED: " + totalCount
              + " | LIMIT: " + params.limit
              + " | SKIP: " + params.skip
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

            // Object.keys(results.obj).forEach(function(nodeId){
            for (const nodeId of Object.keys(results.obj)){

              if (results.obj[nodeId].category) { 

                totalQueued += 1;
                categorizedNodeIdsQueue.push(nodeId);

                if (configuration.testMode && totalQueued >= configuration.maxTestCount) {
                  console.log(chalkAlert("GTS | *** TEST MODE | MAX TEST QUEUED: " + totalQueued));
                  break;
                }

              }
              else {
                console.log(chalkAlert("GTS | ??? UNCATEGORIZED USER FROM DB\n" + tcUtils.jsonPrint(results.obj[nodeId])));
              }
            }

            if (configuration.verbose || (totalCount % 1000 === 0)) {

              console.log(chalkLog("GTS | ... LOADING CATEGORIZED USERS FROM DB"
                + " [ CNIDQ: " + categorizedNodeIdsQueue.length + "]"
                + " | TOTAL: " + totalCount
                + " | " + totalManual + " MAN"
                + " | " + totalAuto + " AUTO"
                + " | " + totalMatched + " MATCHED"
                + " / " + totalMismatched + " MISMATCHED"
                + " | " + totalMatchRate.toFixed(2) + "% MATCHRATE"
              ));
            }

            params.skip += results.count;

            cb();
          }
          else {

            more = false;

            console.log(chalkGreen("GTS | +++ LOADED CATEGORIZED USERS FROM DB"
              + " [ CNIDQ: " + categorizedNodeIdsQueue.length + "]"
              + " | TOTAL CATEGORIZED: " + totalCount
              + " | LIMIT: " + params.limit
              + " | SKIP: " + params.skip
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
          console.log(chalkError("GTS | INIT CATEGORIZED USER HASHMAP ERROR: " + err + "\n" + tcUtils.jsonPrint(err)));
          return reject(err);
        }
        console.log(chalkBlueBold("GTS | INIT CATEGORIZED USERS: " + totalCount));
        resolve();
      }
    );
  });
}

async function initCategorizedNodeIds(){

  statsObj.status = "INIT CATEGORIZED NODE IDS";

  console.log(chalkInfo("GTS | ... INIT CATEGORIZED NODE IDs ..."));

  const p = {};

  p.skip = 0;
  p.limit = DEFAULT_FIND_CAT_USER_CURSOR_LIMIT;
  p.batchSize = DEFAULT_CURSOR_BATCH_SIZE;
  p.query = { 
    "$and": [{ "ignored": { "$nin": [true, "true"] } }, { "category": { "$in": ["left", "right", "neutral"] } }]
  };

  await categoryCursor(p);

  return;
}

async function archiveUser(params){

  if (archive === undefined) { 
    throw new Error("ARCHIVE UNDEFINED");
  }

  let fileName;

  try{
    fileName = "user_" + params.user.userId + ".json";
    const userBuffer = Buffer.from(JSON.stringify(params.user));
    archive.append(userBuffer, { name: fileName});

    statsObj.usersAppendedToArchive += 1;

    if (configuration.verbose) {
      console.log(chalkLog("GTS | >-- ARCHIVE | USER"
        + " [" + statsObj.usersAppendedToArchive + " APPENDED]"
        + " | @" + params.user.screenName
      ));
    }
    return;
  }
  catch(err){
    console.log(chalkError("GTS | *** ARCHIVE USER ERROR"
      + " [" + statsObj.usersAppendedToArchive + " APPENDED]"
      + " | @" + params.user.screenName
      + " | ERR: " + err
    ));
    throw err;
  }

}

let endArchiveUsersInterval;

function endAppendUsers(){
  return new Promise(function(resolve){

    endArchiveUsersInterval = setInterval(function(){

      if ((statsObj.usersAppendedToArchive > 0) && (statsObj.archiveRemainUsers <= 0)){
        console.log(chalkAlert("GTS | XXX END APPEND"
          + " | " + statsObj.archiveTotal + " USERS"
          + " | " + statsObj.usersAppendedToArchive + " APPENDED"
          + " | " + statsObj.usersProcessed + " PROCESSED"
          + " | " + statsObj.userErrorCount + " ERRORS"
          + " | " + statsObj.archiveRemainUsers + " REMAIN"
        ));
        clearInterval(endArchiveUsersInterval);
        return resolve();
      }

    }, 10*ONE_SECOND);

  });
}

slackText = "\n*GTS START | " + hostname + "*";
slackText = slackText + "\n" + tcUtils.getTimeStamp();

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
      console.log(chalkLog("GTS | " + params.message + " | PERIOD: " + tcUtils.msToTime(params.period)));
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

    await tcUtils.saveFile({localFlag: true, folder: configuration.userArchiveFolder, file: configuration.archiveFileUploadCompleteFlagFile, obj: fileSizeObj });

    await delay({message: "... WAIT FOR DROPBOX FLAG FILE SYNC", period: ONE_MINUTE});

    quit("DONE");
  }
  catch(err){
    console.log(chalkError("GTS | *** ARCHIVE_OUTPUT_CLOSED ERROR", err));
    quit();
  }

});

async function initArchiver(){

  let userArchivePath = configuration.userArchivePath;

  if (configuration.testMode) {
    userArchivePath = configuration.userArchivePath.replace(/\.zip/, "_test.zip");
  }

  console.log(chalkBlue("GTS | ... INIT ARCHIVER | " + userArchivePath));

  if (archive && archive.isOpen) {
    console.log(chalkAlert("GTS | ARCHIVE ALREADY OPEN | " + userArchivePath));
    return;
  }

  const lockFileName = userArchivePath + ".lock";

  const archiveFileLocked = await getFileLock({file: lockFileName, options: fileLockOptions});

  if (!archiveFileLocked) {
    console.log(chalkAlert("GTS | *** FILE LOCK FAILED | SKIP INIT ARCHIVE: " + userArchivePath));
    statsObj.archiveOpen = false;
    return;
  }

  const output = fs.createWriteStream(userArchivePath);

  archive = archiver("zip", {
    zlib: { level: 9 } // Sets the compression level.
  });
   
  output.on("close", function() {
    const archiveSize = toMegabytes(archive.pointer());
    console.log(chalkGreen("GTS | XXX ARCHIVE OUTPUT | CLOSED | " + archiveSize.toFixed(2) + " MB"));
    configEvents.emit("ARCHIVE_OUTPUT_CLOSED", userArchivePath);
  });
   
  output.on("end", function() {
    const archiveSize = toMegabytes(archive.pointer());
    console.log(chalkBlueBold("GTS | XXX ARCHIVE OUTPUT | END | " + archiveSize.toFixed(2) + " MB"));
  });
   
  archive.on("warning", function(err) {
    console.log(chalkAlert("GTS | !!! ARCHIVE | WARNING\n" + tcUtils.jsonPrint(err)));
    if (err.code !== "ENOENT") {
      throw err;
    }
  });
   
  archive.on("progress", function(progress) {

    statsObj.progress = progress;

    statsObj.usersProcessed = statsObj.progress.entries.processed;

    statsObj.progressMbytes = toMegabytes(progress.fs.processedBytes);
    statsObj.totalMbytes = toMegabytes(archive.pointer());

    statsObj.archiveElapsed = (moment().valueOf() - statsObj.archiveStartMoment.valueOf()); // mseconds
    statsObj.archiveRate = statsObj.archiveElapsed/statsObj.usersProcessed; // msecs/usersArchived
    statsObj.archiveRemainUsers = statsObj.archiveTotal - (statsObj.usersProcessed + statsObj.userErrorCount);
    statsObj.archiveRemainMS = statsObj.archiveRemainUsers * statsObj.archiveRate; // mseconds
    statsObj.archiveEndMoment = moment();
    statsObj.archiveEndMoment.add(statsObj.archiveRemainMS, "ms");

    if ((statsObj.usersProcessed % 100 === 0) || configuration.verbose || configuration.testMode) {
      console.log(chalkInfo("GTS | >+- ARCHIVE | PROGRESS"
        + " | TEST: " + configuration.testMode
        + " | " + tcUtils.getTimeStamp()
        // + " | AUQ: " + archiveUserQueue.length
        + " | APPNDD: " + statsObj.usersAppendedToArchive
        + " | ENTRIES PRCSSD/REM/TOT: " + statsObj.usersProcessed + "/" + statsObj.archiveRemainUsers + "/" + statsObj.archiveTotal
        + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
        + " (" + (100*statsObj.usersProcessed/statsObj.archiveTotal).toFixed(2) + "%)"
        + " [ RATE: " + (statsObj.archiveRate/1000).toFixed(3) + " SEC/USER ]"
        + " S: " + tcUtils.getTimeStamp(statsObj.archiveStartMoment)
        + " E: " + tcUtils.msToTime(statsObj.archiveElapsed)
        + " | ETC: " + tcUtils.msToTime(statsObj.archiveRemainMS) + " " + statsObj.archiveEndMoment.format(compactDateTimeFormat)
      ));
    }
  });
   
  archive.on("entry", function(entryData) {

    statsObj.archiveEntries += 1;

    if (configuration.verbose || configuration.testMode) {
      console.log(chalkLog("GTS | >-- ARCHIVE | ENTRY"
        + " [ " + statsObj.usersAppendedToArchive + " APPENDED ]"
        + " [ " + statsObj.archiveEntries + " ENTRIES ]"
        + " | " + entryData.name
      ));
    }
  });
   
  archive.on("close", function() {
    console.log(chalkBlueBold("GTS | XXX ARCHIVE | CLOSED | " + userArchivePath));
  });
   
  archive.on("finish", function() {

    console.log(chalkBlueBold("GTS | +++ ARCHIVE | FINISHED | " + tcUtils.getTimeStamp()
      + "\nGTS | +++ ARCHIVE | FINISHED | TEST MODE: " + configuration.testMode
      + "\nGTS | +++ ARCHIVE | FINISHED | ARCHIVE:   " + userArchivePath
      + "\nGTS | +++ ARCHIVE | FINISHED | ENTRIES:   " + statsObj.usersAppendedToArchive + "/" + statsObj.archiveTotal + " APPENDED"
      + " (" + (100*statsObj.usersAppendedToArchive/statsObj.archiveTotal).toFixed(2) + "%)"
      + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
    ));
  });
   
  archive.on("error", function(err) {
    console.log(chalkError("GTS | *** ARCHIVE | ERROR\n" + tcUtils.jsonPrint(err)));
    throw err;
  });
   
  archive.pipe(output);
  statsObj.archiveOpen = true;
  
  return;
}

async function initialize(cnf){

  statsObj.status = "INITIALIZE";

  debug(chalkBlue("INITIALIZE cnf\n" + tcUtils.jsonPrint(cnf)));

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
  
  configuration = await loadAllConfigFiles(configuration);

  await loadCommandLineArgs();
  
  const configArgs = Object.keys(configuration);

  configArgs.forEach(function(arg){
    if (_.isObject(configuration[arg])) {
      console.log("GTS | _FINAL CONFIG | " + arg + "\n" + tcUtils.jsonPrint(configuration[arg]));
    }
    else {
      console.log("GTS | _FINAL CONFIG | " + arg + ": " + configuration[arg]);
    }
  });
  
  statsObj.commandLineArgsLoaded = true;

  global.globalDbConnection = await connectDb();

  return configuration;
}

async function generateGlobalTrainingTestSet(){

  statsObj.status = "GENERATE TRAINING SET";

  console.log(chalkBlueBold("GTS | ==================================================================="));
  console.log(chalkBlueBold("GTS | GENERATE TRAINING SET | " + tcUtils.getTimeStamp()));
  console.log(chalkBlueBold("GTS | ==================================================================="));

  statsObj.totalCategorizedUsersInDB = await global.globalUser.find(catUsersQuery).countDocuments().exec();
  statsObj.archiveTotal = statsObj.totalCategorizedUsersInDB;

  console.log(chalkBlue("GTS | CATEGORIZED USERS IN DB: " + statsObj.totalCategorizedUsersInDB));

  if (configuration.testMode) {
    statsObj.archiveTotal = Math.min(statsObj.archiveTotal, configuration.maxTestCount);
    console.log(chalkAlert("GTS | *** TEST MODE *** | CATEGORIZE MAX " + statsObj.archiveTotal + " USERS"));
  }

  await initArchiver();
  await initCategorizedNodeIdsQueue({interval: configuration.categorizedNodeIdsQueueInterval});
  await initCategorizedNodeIds();
  await endAppendUsers();

  const mihmObj = {};

  mihmObj.maxInputHashMap = {};
  mihmObj.maxInputHashMap = maxInputHashMap;

  mihmObj.normalization = {};
  mihmObj.normalization = statsObj.normalization;

  let maxInputHashMapFile = "maxInputHashMap.json";

  if (configuration.testMode) { maxInputHashMapFile = "maxInputHashMapFile_test.json"; }

  console.log(MODULE_ID_PREFIX
    + " | ... SAVING MAX INPUT HASHMAP FILE | "
    + " | " + (sizeof(mihmObj)/ONE_MEGABYTE).toFixed(3) + " MB"
    + " | " + configuration.trainingSetsFolder + "/" + maxInputHashMapFile
  );

  await tcUtils.saveFile({folder: configuration.trainingSetsFolder, file: maxInputHashMapFile, obj: mihmObj });

  const buf = Buffer.from(JSON.stringify(mihmObj));

  archive.append(buf, { name: maxInputHashMapFile });
  archive.finalize();

  return;
}

setTimeout(async function(){
  try{

    setInterval(function(){
      showStats();
    }, ONE_MINUTE);

    configuration = await initialize(configuration);
    await tcUtils.initSaveFileQueue();
    await tcUtils.redisFlush();
    await generateGlobalTrainingTestSet();

    let rootFolder;

    if (configuration.testMode) {
      rootFolder = (hostname == PRIMARY_HOST) 
      ? defaultHistogramsFolder + "_test/types/" 
      : localHistogramsFolder + "_test/types/";
    }
    else {
      rootFolder = (hostname == PRIMARY_HOST) 
      ? defaultHistogramsFolder + "/types/" 
      : localHistogramsFolder + "/types/";
    }

    console.log(chalkInfo("TFE | ... SAVING HISTOGRAMS"));

    await tcUtils.saveGlobalHistograms({rootFolder: rootFolder});

    await tcUtils.saveGlobalHistograms({rootFolder: rootFolder, pruneFlag: true});
    tcUtils.redisFlush();
    tcUtils.redisQuit();
    clearInterval(categorizedNodeIdsQueueInterval);
    console.log(chalkBlueBold("GTS | XXX MAIN END XXX "));
  }
  catch(err){
    tcUtils.redisFlush();
    tcUtils.redisQuit();
    console.log(chalkError("GTS | *** MAIN ERROR: " + err));
  }
}, 1000);
