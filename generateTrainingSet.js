const dotenv = require("dotenv");
const envConfig = dotenv.config({ path: process.env.WORD_ENV_VARS_FILE })

if (envConfig.error) {
  throw envConfig.error
}
 
console.log("WAS | +++ ENV CONFIG LOADED")

const MODULE_NAME = "generateTrainingSet  ";
const MODULE_ID_PREFIX = "GTS";
const GLOBAL_TRAINING_SET_ID = "globalTrainingSet";

const DEFAULT_CURSOR_PARALLEL = 4;
const DEFAULT_MAX_GLOBAL_HISTOGRAM_USERS = 10000;

const DEFAULT_PRUNE_FLAG = true;
const DEFAULT_SAVE_GLOBAL_HISTOGRAMS_ONLY = false;
const DEFAULT_CURSOR_BATCH_SIZE = 128;
const DEFAULT_SAVE_FILE_MAX_PARALLEL = 16;
const DEFAULT_SAVE_FILE_BACKPRESSURE_PERIOD = 10; // ms
const DEFAULT_ENABLE_CREATE_USER_ARCHIVE = false;

const DEFAULT_MAX_SAVE_FILE_QUEUE = 100;
// const DEFAULT_MAX_CURSOR_DATA_HANDLER_QUEUE = 100;

const DEFAULT_INTERVAL = 2;
const DEFAULT_REDIS_SCAN_COUNT = 1000;
const DEFAULT_SAVE_FILE_QUEUE_INTERVAL = 2;
const DEFAULT_MAX_HISTOGRAM_VALUE = 1000;
const DEFAULT_MAX_USER_FRIENDS = 10000;

const DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP = {};
const DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP = {};

DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.emoji = 10;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.friends = 250;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.hashtags = 25;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.images = 10;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.locations = 5;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.media = 2;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.ngrams = 250;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.places = 2;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.sentiment = 1;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.urls = 3;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.userMentions = 20;
DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.words = 100;

for(const inputType of Object.keys(DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP)){
  DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP[inputType] = 1 + Math.floor(0.1 * DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP[inputType])
}
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.friends = 5;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.hashtags = 2;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.locations = 2;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.media = 2;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.ngrams = 20;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.urls = 2;
DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP.words = 20;

const DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP = {};
const DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP = {};

DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.emoji = 5;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.hashtags = 10;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.ngrams = 25;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.images = 15;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.locations = 5;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.media = 1;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.places = 1;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.sentiment = 1;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.urls = 1;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.userMentions = 5;
DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.words = 100;

for(const inputType of Object.keys(DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP)){
  DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP[inputType] = 1 + Math.floor(0.1 * DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP[inputType])
}
DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.images = 1;
DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.ngrams = 2;
DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP.words = 2;

const TOTAL_MAX_TEST_COUNT = 1000;

const os = require("os");
let hostname = os.hostname();
hostname = hostname.replace(/.tld/g, ""); // amtrak wifi
hostname = hostname.replace(/.local/g, "");
hostname = hostname.replace(/.home/g, "");
hostname = hostname.replace(/.at.net/g, "");
hostname = hostname.replace(/.fios-router.home/g, "");
hostname = hostname.replace(/word0-instance-1/g, "google");
hostname = hostname.replace(/word-1/g, "google");
hostname = hostname.replace(/word/g, "google");

const PRIMARY_HOST = process.env.PRIMARY_HOST || "google";
const DATABASE_HOST = process.env.DATABASE_HOST || "macpro2";
const HOST = (hostname === PRIMARY_HOST || hostname === DATABASE_HOST) ? "default" : "local";

const DATA_ROOT_FOLDER = "/Volumes/RAID1/data";

let DROPBOX_ROOT_FOLDER;

if (hostname === "google") {
  DROPBOX_ROOT_FOLDER = "/home/tc/Dropbox/Apps/wordAssociation";
}
else {
  DROPBOX_ROOT_FOLDER = "/Users/tc/Dropbox/Apps/wordAssociation";
}

console.log("\n\n");
console.log(MODULE_ID_PREFIX + " | ==================================================================================");
console.log(MODULE_ID_PREFIX + " | ==================================================================================");
console.log(MODULE_ID_PREFIX + " | HOST:                " + HOST);
console.log(MODULE_ID_PREFIX + " | PRIMARY_HOST:        " + PRIMARY_HOST);
console.log(MODULE_ID_PREFIX + " | DATABASE_HOST:       " + DATABASE_HOST);
console.log(MODULE_ID_PREFIX + " | hostname:            " + hostname);
console.log(MODULE_ID_PREFIX + " | DROPBOX_ROOT_FOLDER: " + DROPBOX_ROOT_FOLDER);
console.log(MODULE_ID_PREFIX + " | DATA_ROOT_FOLDER:    " + DATA_ROOT_FOLDER);
console.log(MODULE_ID_PREFIX + " | ==================================================================================");
console.log(MODULE_ID_PREFIX + " | ==================================================================================");
console.log("\n\n");

const MODULE_ID = MODULE_ID_PREFIX + "_node_" + hostname;

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;

const ONE_KILOBYTE = 1024;
const ONE_MEGABYTE = 1024 * ONE_KILOBYTE;
const ONE_GIGABYTE = 1024 * ONE_MEGABYTE;

const compactDateTimeFormat = "YYYYMMDD_HHmmss";

const DEFAULT_QUIT_ON_COMPLETE = true;
const DEFAULT_TEST_RATIO = 0.20;

const path = require("path");
const moment = require("moment");
const merge = require("deepmerge");
const archiver = require("archiver");
const watch = require("watch");
const fs = require("fs");
const util = require("util");
const _ = require("lodash");
const HashMap = require("hashmap").HashMap;
const pick = require("object.pick");
const debug = require("debug")("gts");
const commandLineArgs = require("command-line-args");
const empty = require("is-empty");

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

let fetchUserInterval;
let showStatsInterval;
let endSaveFileQueueInterval;
// let waitInterval;

let archive;

const subFolderSet = new Set();

const DEFAULT_USER_PROPERTY_PICK_ARRAY = [
  "ageDays", 
  "category", 
  "categoryAuto",
  "categorizeNetwork", 
  "description",
  "followersCount",
  "following", 
  "friends",
  "friendsCount",
  "ignored", 
  "lang",
  "languageAnalysis", 
  "location", 
  "name",
  "nodeId", 
  "profileHistograms",
  "rate",
  "mentions",
  "screenName", 
  "statusesCount",
  "threeceeFollowing",
  "tweetHistograms", 
  "tweetsPerDay", 
  "userId"
];

const statsObj = {};

statsObj.heap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
statsObj.maxHeap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
statsObj.redis = {};
statsObj.redis.keys = 0;

statsObj.cursor = {};
statsObj.cursor.left = {};
statsObj.cursor.left.lastFetchedNodeId = false;
statsObj.cursor.right = {};
statsObj.cursor.right.lastFetchedNodeId = false;
statsObj.cursor.neutral = {};
statsObj.cursor.neutral.lastFetchedNodeId = false;

statsObj.fetchReady = {};
statsObj.fetchReady.maxPeriod = 0;
statsObj.fetchReady.period = 0;

statsObj.fetchReady.saveFileBackPressurePeriod = 0;
statsObj.fetchReady.maxSaveFileBackPressurePeriod = 0;

statsObj.fetchReady.saveFileQueue = 0;
statsObj.fetchReady.maxSaveFileQueue = 0;

statsObj.fetchReady.queueOverShoot = 0;
statsObj.fetchReady.maxQueueOverShoot = 0;


let statsObjSmall = {};

statsObj.status = "LOAD";
statsObj.hostname = hostname;
statsObj.pid = process.pid;
statsObj.cpus = os.cpus().length;
statsObj.commandLineArgsLoaded = false;

statsObj.users = {};
statsObj.users.grandTotal = 0;
statsObj.users.notCategorized = 0;
statsObj.users.notFound = 0;
statsObj.users.screenNameUndefined = 0;
statsObj.users.processed = {};
statsObj.users.processed.total = 0;
statsObj.users.processed.updatedGlobalHistograms = 0;
statsObj.users.processed.percent = 0;
statsObj.users.processed.empty = 0;
statsObj.users.processed.errors = 0;
statsObj.users.processed.elapsed = 0;
statsObj.users.processed.rate = 0;
statsObj.users.processed.remain = 0;
statsObj.users.processed.remainMS = 0;
statsObj.users.processed.startMoment = 0;
statsObj.users.processed.endMoment = moment();

statsObj.serverConnected = false;

statsObj.startTimeMoment = moment();
statsObj.startTime = moment().valueOf();
statsObj.elapsed = moment().valueOf() - statsObj.startTime;

statsObj.endAppendUsersFlag = false;
// statsObj.initcategorizedNodeQueueFlag = false;

statsObj.errors = {};
statsObj.errors.users = {};
statsObj.errors.users.findOne = 0;

const statsPickArray = [
  "pid",
  "heap",
  "maxHeap",
  "startTime", 
  "elapsed", 
  "serverConnected", 
  "status", 
  "authenticated", 
  "numChildren", 
  "userReadyAck", 
  "userReadyAckWait", 
  "userReadyTransmitted",
  "redis",
  "fetchReady"
];

statsObjSmall = pick(statsObj, statsPickArray);

let configuration = {}; // merge of defaultConfiguration & hostConfiguration
configuration.cursorParallel = DEFAULT_CURSOR_PARALLEL;
configuration.maxGlobalHistogramUsers = DEFAULT_MAX_GLOBAL_HISTOGRAM_USERS; // max users used to update global histogtrams due to mem contraints
configuration.pruneFlag = DEFAULT_PRUNE_FLAG;
configuration.maxUserFriends = DEFAULT_MAX_USER_FRIENDS;
configuration.saveFileBackPressurePeriod = DEFAULT_SAVE_FILE_BACKPRESSURE_PERIOD;
configuration.saveGlobalHistogramsOnly = DEFAULT_SAVE_GLOBAL_HISTOGRAMS_ONLY;
configuration.enableCreateUserArchive = DEFAULT_ENABLE_CREATE_USER_ARCHIVE;
// configuration.maxCursorDataHandlerQueue = DEFAULT_MAX_CURSOR_DATA_HANDLER_QUEUE;
configuration.redisScanCount = DEFAULT_REDIS_SCAN_COUNT;
configuration.saveFileMaxParallel = DEFAULT_SAVE_FILE_MAX_PARALLEL;
configuration.cursorBatchSize = DEFAULT_CURSOR_BATCH_SIZE;
configuration.saveFileQueueInterval = DEFAULT_SAVE_FILE_QUEUE_INTERVAL;
configuration.maxSaveFileQueue = DEFAULT_MAX_SAVE_FILE_QUEUE;
configuration.verbose = false;
configuration.testMode = false; // per tweet test mode
configuration.testSetRatio = DEFAULT_TEST_RATIO;
configuration.totalMaxTestCount = TOTAL_MAX_TEST_COUNT;

configuration.inputTypeMinTweetsHashMap = DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP;
configuration.testInputTypeMinTweetsHashMap = DEFAULT_TEST_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP;

configuration.inputTypeMinProfileHashMap = DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP;
configuration.testInputTypeMinProfileHashMap = DEFAULT_TEST_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP;

configuration.maxTestCount = {};
configuration.maxTestCount.left = parseInt(0.333333*TOTAL_MAX_TEST_COUNT);
configuration.maxTestCount.neutral = parseInt(0.333333*TOTAL_MAX_TEST_COUNT);
configuration.maxTestCount.right = parseInt(0.333333*TOTAL_MAX_TEST_COUNT);

let sum = configuration.maxTestCount.left + configuration.maxTestCount.neutral + configuration.maxTestCount.right;

while(sum < TOTAL_MAX_TEST_COUNT){
  configuration.maxTestCount.left += 1;
  sum = configuration.maxTestCount.left + configuration.maxTestCount.neutral + configuration.maxTestCount.right;
}

configuration.maxHistogramValue = DEFAULT_MAX_HISTOGRAM_VALUE;
configuration.slackChannel = {};

let defaultConfiguration = {}; // general configuration for GTS
let hostConfiguration = {}; // host-specific configuration for GTS

configuration.cursorInterval = DEFAULT_INTERVAL;

configuration.userPropertyPickArray = DEFAULT_USER_PROPERTY_PICK_ARRAY;
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

configuration.dataRootFolder = DATA_ROOT_FOLDER;
configuration.tempUserDataFolder = path.join(configuration.dataRootFolder, "trainingSets/users");
configuration.userDataFolder = path.join(configuration.dataRootFolder, "users");

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
// configuration.local.userArchivePath = path.join(configuration.local.userArchiveFolder, configuration.userArchiveFile);

configuration.default = {};
configuration.default.trainingSetsFolder = path.join(configDefaultFolder, "trainingSets");
configuration.default.histogramsFolder = path.join(configDefaultFolder, "histograms");
configuration.default.userArchiveFolder = path.join(configDefaultFolder, "trainingSets/users");
// configuration.default.userArchivePath = path.join(configuration.default.userArchiveFolder, configuration.userArchiveFile);

configuration.trainingSetsFolder = configuration[HOST].trainingSetsFolder;
configuration.archiveFileUploadCompleteFlagFolder = path.join(configuration[HOST].trainingSetsFolder, "users");
configuration.histogramsFolder = configuration[HOST].histogramsFolder;
configuration.userArchiveFolder = configuration[HOST].userArchiveFolder;
// configuration.userArchivePath = configuration[HOST].userArchivePath;

fs.mkdirSync(configuration.tempUserDataFolder, { recursive: true });
fs.mkdirSync(configuration[HOST].userArchiveFolder, { recursive: true });

global.wordAssoDb = require("@threeceelabs/mongoose-twitter");

const mguAppName = MODULE_ID_PREFIX + "_MGU";
const MongooseUtilities = require("@threeceelabs/mongoose-utilities");
const mgUtils = new MongooseUtilities(mguAppName);

mgUtils.on("ready", async () => {
  console.log(`${MODULE_ID_PREFIX} | +++ MONGOOSE UTILS READY: ${mguAppName}`);
})

const tcuAppName = MODULE_ID_PREFIX + "_TCU";
const ThreeceeUtilities = require("@threeceelabs/threecee-utilities");
const tcUtils = new ThreeceeUtilities(tcuAppName);

const redisClient = tcUtils.redisClient;
const jsonPrint = tcUtils.jsonPrint;
const getTimeStamp = tcUtils.getTimeStamp;
const msToTime = tcUtils.msToTime;

tcUtils.on("ready", async () => {
  console.log(`${MODULE_ID_PREFIX} | +++ THREECEE UTILS READY: ${tcuAppName}`);
})

tcUtils.on("error", async (err) => {
  console.log(`${MODULE_ID_PREFIX} | *** THREECEE UTILS ERROR ${tcuAppName} | ERR: ${err}`);
  console.trace(err)
})

const UserServerController = require("@threeceelabs/user-server-controller");
const userServerController = new UserServerController(MODULE_ID_PREFIX + "_USC");

userServerController.on("error", function(err){
  console.log(chalkError(MODULE_ID_PREFIX + " | *** USC ERROR | " + err));
});

userServerController.on("ready", function(appname){
  console.log(chalk.green(MODULE_ID_PREFIX + " | USC READY | " + appname));
});

process.on("unhandledRejection", async function(err, promise) {
  console.trace("Unhandled rejection (promise: ", promise, ", reason: ", err, ").");
  process.exit();
});

function toMegabytes(sizeInBytes) {
  return sizeInBytes/ONE_MEGABYTE;
}

const DEFAULT_RUN_ID = hostname + "_" + process.pid + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat);

if (process.env.GTS_RUN_ID !== undefined) {
  statsObj.runId = process.env.GTS_RUN_ID;
  console.log(chalkLog(MODULE_ID_PREFIX + " | ENV RUN ID: " + statsObj.runId));
}
else {
  statsObj.runId = DEFAULT_RUN_ID;
  console.log(chalkLog(MODULE_ID_PREFIX + " | DEFAULT RUN ID: " + statsObj.runId));
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

  return;
}

let stdin;

//=========================================================================
// SLACK
//=========================================================================
const { WebClient } = require('@slack/web-api');

console.log("process.env.SLACK_BOT_TOKEN: ", process.env.SLACK_BOT_TOKEN)
const slackBotToken = process.env.SLACK_BOT_TOKEN;

const slackWebClient = new WebClient(slackBotToken);

const slackChannel = "gts";
const channelsHashMap = new HashMap();

async function slackSendWebMessage(msgObj){
  try{
    const channel = msgObj.channel || configuration.slackChannel.id;
    const text = msgObj.text || msgObj;

    await slackWebClient.chat.postMessage({
      text: text,
      channel: channel,
    });
  }
  catch(err){
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** slackSendWebMessage ERROR: " + err));
    throw err;
  }
}

async function initSlackWebClient(){
  try {

    console.log(chalkLog(MODULE_ID + " | INIT SLACK WEB CLIENT"))

    const authTestResponse = await slackWebClient.auth.test()

    console.log({authTestResponse})

    const conversationsListResponse = await slackWebClient.conversations.list();

    conversationsListResponse.channels.forEach(async function(channel){

      debug(chalkLog(MODULE_ID + " | SLACK CHANNEL | " + channel.id + " | " + channel.name));

      if (channel.name === slackChannel) {
        configuration.slackChannel = channel;

        const message = {
          channel: configuration.slackChannel.id,
          text: "OP"
        };

        message.attachments = [];
        message.attachments.push({
          text: "INIT", 
          fields: [ 
            { title: "SRC", value: hostname + "_" + process.pid }, 
            { title: "MOD", value: MODULE_NAME }, 
            { title: "DST", value: "ALL" } 
          ]
        });

        await slackWebClient.chat.postMessage(message);
      }

      channelsHashMap.set(channel.id, channel);

    });

    return;
  }
  catch(err){
    console.log(chalkError(MODULE_ID + " | *** INIT SLACK WEB CLIENT ERROR: " + err));
    throw err;
  }
}

const saveGlobalHistogramsOnly = { name: "saveGlobalHistogramsOnly", alias: "H", type: Boolean };
const maxHistogramValue = { name: "maxHistogramValue", alias: "M", type: Number };
const help = { name: "help", alias: "h", type: Boolean};
const enableStdin = { name: "enableStdin", alias: "S", type: Boolean, defaultValue: true };
const quitOnComplete = { name: "quitOnComplete", alias: "q", type: Boolean };
const quitOnError = { name: "quitOnError", alias: "Q", type: Boolean, defaultValue: true };
const verbose = { name: "verbose", alias: "v", type: Boolean };
const testMode = { name: "testMode", alias: "X", type: Boolean };

const optionDefinitions = [
  saveGlobalHistogramsOnly,
  maxHistogramValue,
  enableStdin, 
  quitOnComplete, 
  quitOnError, 
  verbose, 
  testMode,
  help
];

const commandLineConfig = commandLineArgs(optionDefinitions);
console.log(chalkInfo(MODULE_ID_PREFIX + " | COMMAND LINE CONFIG\nGTS | " + jsonPrint(commandLineConfig)));
console.log(MODULE_ID_PREFIX + " | COMMAND LINE OPTIONS\nGTS | " + jsonPrint(commandLineConfig));

if (Object.keys(commandLineConfig).includes("help")) {
  console.log(MODULE_ID_PREFIX + " |optionDefinitions\n" + jsonPrint(optionDefinitions));
  quit("help");
}

process.on("message", function(msg) {
  if (msg === "shutdown") {
    console.log("\n\nGTS | !!!!! RECEIVED PM2 SHUTDOWN !!!!!\n\n***** Closing all connections *****\n\n");
    setTimeout(function() {
      console.log(MODULE_ID_PREFIX + " | **** Finished closing connections ****"
        + "\n\n" + MODULE_ID_PREFIX + " | ***** RELOADING generateTrainingSet.js NOW *****\n\n");
      process.exit(0);
    }, 1500);
  }
  else {
    console.log(MODULE_ID_PREFIX + " | R<\n" + jsonPrint(msg));
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
console.log(MODULE_ID_PREFIX + " | HOST:          " + hostname);
console.log(MODULE_ID_PREFIX + " | PROCESS TITLE: " + process.title);
console.log(MODULE_ID_PREFIX + " | PROCESS ID:    " + process.pid);
console.log(MODULE_ID_PREFIX + " | RUN ID:        " + statsObj.runId);
console.log(MODULE_ID_PREFIX + " | PROCESS ARGS:  " + util.inspect(process.argv, {showHidden: false, depth: 1}));
console.log(MODULE_ID_PREFIX + " | =================================");

// ==================================================================
// DROPBOX
// ==================================================================

const redisKeysRegex = /keys=(\d+)/;

async function showStats(options){

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;
  statsObj.heap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
  statsObj.maxHeap = Math.max(statsObj.maxHeap, statsObj.heap);

  statsObj.redis.server = await redisClient.info("server")
  statsObj.redis.clients = await redisClient.info("clients")
  statsObj.redis.cpu = await redisClient.info("cpu")
  statsObj.redis.stats = await redisClient.info("stats")
  statsObj.redis.memory = await redisClient.info("memory")

  const redisKeyspaceResults = await redisClient.info("keyspace")
  const matchArray = await redisKeyspaceResults.match(redisKeysRegex)
  statsObj.redis.keys = matchArray && matchArray !== undefined && matchArray[1] !== undefined ? parseInt(matchArray[1]) : 0;

  statsObjSmall = pick(statsObj, statsPickArray);

  const saveFileQueue = tcUtils.getSaveFileQueue();

  if (options) {
    console.log(MODULE_ID_PREFIX + " | STATS\nGTS | " + jsonPrint(statsObjSmall));
  }
  else {
    console.log(chalkLog(MODULE_ID_PREFIX
      + " | ==========================================================================================================="
      + "\n" + MODULE_ID_PREFIX
      + " | RUN " + msToTime(statsObj.elapsed)
      + " | NOW " + moment().format(compactDateTimeFormat)
      + " | STRT " + moment(parseInt(statsObj.startTime)).format(compactDateTimeFormat)
      + " | STATUS: " + statsObj.status
      + "\n" + MODULE_ID_PREFIX
      + " | REDIS KEYS: " + statsObj.redis.keys
      + "\n" + MODULE_ID_PREFIX
      + " | SFQ: " + saveFileQueue
      + " | CPUs: " + statsObj.cpus
      + " | HEAP: " + statsObj.heap.toFixed(3) + " GB"
      + " | MAX HEAP: " + statsObj.maxHeap.toFixed(3) + " GB"
      + "\n" + MODULE_ID_PREFIX
      + " | ==========================================================================================================="
    ));

    categorizedUserHistogramTotal();

    console.log(chalkLog(MODULE_ID_PREFIX + " | CL U HIST"
      + " | TOTAL: " + categorizedUserHistogram.total
      + " | L: " + categorizedUserHistogram.left
      + " | R: " + categorizedUserHistogram.right
      + " | N: " + categorizedUserHistogram.neutral
      + " | +: " + categorizedUserHistogram.positive
      + " | -: " + categorizedUserHistogram.negative
      + " | 0: " + categorizedUserHistogram.none
    ));

    console.log(chalkInfo(MODULE_ID_PREFIX
      + " | ==========================================================================================================="
      + "\n" + MODULE_ID_PREFIX + " | PROCESSED"
      + " | " + getTimeStamp()
      + " | PRCSD/REM/MT/ERR/TOT: " 
      + statsObj.users.processed.total 
      + "/" + statsObj.users.processed.remain 
      + "/" + statsObj.users.processed.empty 
      + "/" + statsObj.users.processed.errors
      + "/" + statsObj.users.grandTotal
      + " (" + (100*statsObj.users.processed.total/statsObj.users.grandTotal).toFixed(2) + "%)"
      + " [ " + (statsObj.users.processed.rate/1000).toFixed(3) + " SPU ]"
      + "\n" + MODULE_ID_PREFIX
      + " | NOW: " + getTimeStamp()
      + " | START: " + getTimeStamp(statsObj.users.processed.startMoment)
      + " | ELSD: " + msToTime(statsObj.users.processed.elapsed)
      + " | ETC: " + msToTime(statsObj.users.processed.remainMS)
      + " " + statsObj.users.processed.endMoment.format(compactDateTimeFormat)
      + "\nGTS | ==========================================================================================================="
    ));

    if (statsObj.status === "archiveFolder"){
      console.log(chalkGreen(MODULE_ID_PREFIX
        + " | ->- ARCHIVE" 
        + " | PROGRESS: " + statsObj.progressMbytes.toFixed(3) + " MB"
        + " | TOTAL: " + statsObj.totalMbytes.toFixed(3) + " MB"
        + " | DST: " + statsObj.archivePath
      ));
    }
  }
}

async function quit(options){

  console.log(chalkAlert(MODULE_ID_PREFIX + " | QUITTING ..." ));

  clearInterval(fetchUserInterval);
  clearInterval(endSaveFileQueueInterval);
  clearInterval(showStatsInterval);
  // clearInterval(waitInterval);

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  for(const categoryCursor of Object.values(categoryCursorHash)){
    console.log(chalkLog(`${MODULE_ID_PREFIX} | CLOSING CURSOR | ${categoryCursor.category}`))
    categoryCursor.cursor.close();
  }

  await tcUtils.stopSaveFileQueue();

  await redisClient.flushdb();
  await redisClient.disconnect();
  await redisClient.quit();

  if (options !== undefined) {

    if (options === "help") {
      process.exit();
    }
    else {
      let slackText = "\n*" + statsObj.runId + "*";
      slackText = slackText + " | RUN " + msToTime(statsObj.elapsed);
      slackText = slackText + " | QUIT CAUSE: " + options;
      debug(MODULE_ID + " | SLACK TEXT: " + slackText);
      slackSendWebMessage({channel: slackChannel, text: slackText});
    }
  }

  showStats();

  setTimeout(function(){
    
    showStats();

    console.log(chalkBlueBold(
               MODULE_ID_PREFIX + " | ==================================================================="
      + "\n" + MODULE_ID_PREFIX + " | *** QUIT GENERATE TRAINING SET ***"
      + "\n" + MODULE_ID_PREFIX + " | DATA_ROOT_FOLDER: " + configuration.dataRootFolder
      + "\n" + MODULE_ID_PREFIX + " | ==================================================================="
    ));
    process.exit();
  }, 1000);
}

process.on( "SIGINT", async function() {
  await quit("SIGINT");
});

process.on("exit", async function() {
  await quit("SIGINT");
});


function initStdIn(){

  return new Promise(function(resolve){

    console.log(MODULE_ID_PREFIX + " | STDIN ENABLED");

    stdin = process.stdin;
    if(stdin.setRawMode !== undefined) {
      stdin.setRawMode( true );
    }
    stdin.resume();
    stdin.setEncoding( "utf8" );
    stdin.on( "data", async function( key ){

      switch (key) {
        case "\u0003":
          process.exit();
        break;
        case "v":
          configuration.verbose = !configuration.verbose;
          console.log(chalkRedBold(MODULE_ID_PREFIX + " | VERBOSE: " + configuration.verbose));
        break;
        case "q":
          await quit();
        break;
        case "Q":
          await quit();
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
      console.log(MODULE_ID_PREFIX + " | --> COMMAND LINE CONFIG | " + arg + ": " + configuration[arg]);
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

    console.log(chalkInfo(MODULE_ID_PREFIX + " | LOADED CONFIG FILE: " + params.file + "\n" + jsonPrint(loadedConfigObj)));

    if (loadedConfigObj.GTS_TEST_MODE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_TEST_MODE: " + loadedConfigObj.GTS_TEST_MODE);

      if ((loadedConfigObj.GTS_TEST_MODE === true) || (loadedConfigObj.GTS_TEST_MODE === "true")) {
        newConfiguration.testMode = true;
      }
      else if ((loadedConfigObj.GTS_TEST_MODE === false) || (loadedConfigObj.GTS_TEST_MODE === "false")) {
        newConfiguration.testMode = false;
      }
      else {
        newConfiguration.testMode = false;
      }

      console.log(MODULE_ID_PREFIX + " | LOADED newConfiguration.testMode: " + newConfiguration.testMode);
    }

    if (loadedConfigObj.GTS_OFFLINE_MODE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_OFFLINE_MODE: " + loadedConfigObj.GTS_OFFLINE_MODE);

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

    if (loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY: " + loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY);

      if ((loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY === false) || (loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY === "false")) {
        newConfiguration.saveGlobalHistogramsOnly = false;
      }
      else if ((loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY === true) || (loadedConfigObj.GTS_SAVE_GLOBAL_HISTOGRAMS_ONLY === "true")) {
        newConfiguration.saveGlobalHistogramsOnly = true;
      }
      else {
        newConfiguration.saveGlobalHistogramsOnly = false;
      }
    }

    if (loadedConfigObj.GTS_QUIT_ON_COMPLETE !== undefined) {
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_QUIT_ON_COMPLETE: " + loadedConfigObj.GTS_QUIT_ON_COMPLETE);
      if (!loadedConfigObj.GTS_QUIT_ON_COMPLETE || (loadedConfigObj.GTS_QUIT_ON_COMPLETE === "false")) {
        newConfiguration.quitOnComplete = false;
      }
      else {
        newConfiguration.quitOnComplete = true;
      }
    }

    if (loadedConfigObj.GTS_VERBOSE_MODE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_VERBOSE_MODE: " + loadedConfigObj.GTS_VERBOSE_MODE);
      newConfiguration.verbose = loadedConfigObj.GTS_VERBOSE_MODE;
    }

    if (loadedConfigObj.GTS_ENABLE_STDIN !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_ENABLE_STDIN: " + loadedConfigObj.GTS_ENABLE_STDIN);
      newConfiguration.enableStdin = loadedConfigObj.GTS_ENABLE_STDIN;
    }

    if (loadedConfigObj.GTS_DATA_ROOT_FOLDER !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_DATA_ROOT_FOLDER: " + loadedConfigObj.GTS_DATA_ROOT_FOLDER);
      newConfiguration.dataRootFolder = loadedConfigObj.GTS_DATA_ROOT_FOLDER;
      newConfiguration.tempUserDataFolder = path.join(newConfiguration.dataRootFolder, "trainingSets/users");
      newConfiguration.userDataFolder = path.join(newConfiguration.dataRootFolder, "users");
    }

    if (loadedConfigObj.GTS_CURSOR_PARALLEL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_CURSOR_PARALLEL: " + loadedConfigObj.GTS_CURSOR_PARALLEL);
      newConfiguration.cursorParallel = loadedConfigObj.GTS_CURSOR_PARALLEL;
    }

    if (loadedConfigObj.GTS_MAX_GLOBAL_HISTOGRAM_USERS !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_GLOBAL_HISTOGRAM_USERS: " + loadedConfigObj.GTS_MAX_GLOBAL_HISTOGRAM_USERS);
      newConfiguration.maxGlobalHistogramUsers = loadedConfigObj.GTS_MAX_GLOBAL_HISTOGRAM_USERS;
    }

    if (loadedConfigObj.GTS_CURSOR_BATCH_SIZE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_CURSOR_BATCH_SIZE: " + loadedConfigObj.GTS_CURSOR_BATCH_SIZE);
      newConfiguration.cursorBatchSize = loadedConfigObj.GTS_CURSOR_BATCH_SIZE;
    }

    if (loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_TOTAL_MAX_TEST_COUNT: " + loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT);
      newConfiguration.totalMaxTestCount = loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT;
      newConfiguration.maxTestCount = {};
      newConfiguration.maxTestCount.left = parseInt(0.333333*loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT);
      newConfiguration.maxTestCount.neutral = parseInt(0.333333*loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT);
      newConfiguration.maxTestCount.right = parseInt(0.333333*loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT);

      let sum = newConfiguration.maxTestCount.left + newConfiguration.maxTestCount.neutral + newConfiguration.maxTestCount.right;

      while(sum < loadedConfigObj.GTS_TOTAL_MAX_TEST_COUNT){
        newConfiguration.maxTestCount.left += 1;
        sum = newConfiguration.maxTestCount.left + newConfiguration.maxTestCount.neutral + newConfiguration.maxTestCount.right;
      }
    }

    if (loadedConfigObj.GTS_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP: " + loadedConfigObj.GTS_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP);
      newConfiguration.inputTypeMinTweetsHashMap = loadedConfigObj.GTS_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP;
    }

    if (loadedConfigObj.GTS_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP: " + loadedConfigObj.GTS_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP);
      newConfiguration.inputTypeMinProfileHashMap = loadedConfigObj.GTS_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP
    }

    if (loadedConfigObj.GTS_MAX_USER_FRIENDS !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_USER_FRIENDS: " + loadedConfigObj.GTS_MAX_USER_FRIENDS);
      newConfiguration.maxUserFriends = loadedConfigObj.GTS_MAX_USER_FRIENDS;
    }

    // if (loadedConfigObj.GTS_USERS_PER_ARCHIVE !== undefined){
    //   console.log(MODULE_ID_PREFIX + " | LOADED GTS_USERS_PER_ARCHIVE: " + loadedConfigObj.GTS_USERS_PER_ARCHIVE);
    //   newConfiguration.usersPerArchive = loadedConfigObj.GTS_USERS_PER_ARCHIVE;
    // }

    if (loadedConfigObj.GTS_WAIT_VALUE_INTERVAL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_WAIT_VALUE_INTERVAL: " + loadedConfigObj.GTS_WAIT_VALUE_INTERVAL);
      newConfiguration.waitValueInterval = loadedConfigObj.GTS_WAIT_VALUE_INTERVAL;
    }

    if (loadedConfigObj.GTS_REDIS_SCAN_COUNT !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_REDIS_SCAN_COUNT: " + loadedConfigObj.GTS_REDIS_SCAN_COUNT);
      newConfiguration.redisScanCount = loadedConfigObj.GTS_REDIS_SCAN_COUNT;
    }

    if (loadedConfigObj.GTS_SAVE_FILE_MAX_PARALLEL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_SAVE_FILE_MAX_PARALLEL: " + loadedConfigObj.GTS_SAVE_FILE_MAX_PARALLEL);
      newConfiguration.saveFileMaxParallel = loadedConfigObj.GTS_SAVE_FILE_MAX_PARALLEL;
    }

    if (loadedConfigObj.GTS_SAVE_FILE_QUEUE_INTERVAL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_SAVE_FILE_QUEUE_INTERVAL: " + loadedConfigObj.GTS_SAVE_FILE_QUEUE_INTERVAL);
      newConfiguration.saveFileQueueInterval = loadedConfigObj.GTS_SAVE_FILE_QUEUE_INTERVAL;
    }

    if (loadedConfigObj.GTS_MAX_SAVE_FILE_QUEUE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_SAVE_FILE_QUEUE: " + loadedConfigObj.GTS_MAX_SAVE_FILE_QUEUE);
      newConfiguration.maxSaveFileQueue = loadedConfigObj.GTS_MAX_SAVE_FILE_QUEUE;
    }

    if (loadedConfigObj.GTS_SAVE_FILE_BACKPRESSURE_PERIOD !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_SAVE_FILE_BACKPRESSURE_PERIOD: " + loadedConfigObj.GTS_SAVE_FILE_BACKPRESSURE_PERIOD);
      newConfiguration.saveFileBackPressurePeriod = loadedConfigObj.GTS_SAVE_FILE_BACKPRESSURE_PERIOD;
    }


    if (loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_HISTOGRAM_VALUE: " + loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE);
      newConfiguration.maxHistogramValue = loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE;
    }

    return newConfiguration;
  }
  catch(err){
    console.error(chalkError(MODULE_ID_PREFIX + " | ERROR LOAD CONFIG: " + fullPath
      + "\n" + jsonPrint(err)
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
  
  console.log("hostConfiguration\n" + jsonPrint(hostConfiguration));
  console.log("defaultConfiguration\n" + jsonPrint(defaultConfiguration));

  const defaultAndHostConfig = merge(defaultConfiguration, hostConfiguration); // host settings override defaults

  console.log("defaultAndHostConfig\n" + jsonPrint(defaultAndHostConfig));
  console.log("cnf\n" + jsonPrint(cnf));

  const tempConfig = merge(cnf, defaultAndHostConfig); // any new settings override existing config

  console.log("tempConfig\n" + jsonPrint(tempConfig));

  tempConfig.twitterUsers = _.uniq(tempConfig.twitterUsers);

  return tempConfig;
}

async function initWatchAllConfigFolders(p){

  try{

    const params = p || {};

    console.log(chalkBlue(MODULE_ID_PREFIX + " | INIT WATCH ALL CONFIG FILES\n" + jsonPrint(params)));

    await loadAllConfigFiles();
    await loadCommandLineArgs();

    const options = {
      ignoreDotFiles: true,
      ignoreUnreadableDir: true,
      ignoreNotPermitted: true,
    }

    //========================
    // WATCH DEFAULT CONFIG
    //========================

    watch.createMonitor(configDefaultFolder, options, function (monitorDefaultConfig) {

      console.log(chalkBlue(MODULE_ID_PREFIX + " | INIT WATCH DEFAULT CONFIG FOLDER: " + configDefaultFolder));

      monitorDefaultConfig.on("created", async function(f){
        if (f.endsWith(configDefaultFile)){
          await delay({period: 30*ONE_SECOND});
          await loadAllConfigFiles();
          await loadCommandLineArgs();
        }

      });

      monitorDefaultConfig.on("changed", async function(f){

        if (f.endsWith(configDefaultFile)){
          await delay({period: 30*ONE_SECOND});
          await loadAllConfigFiles();
          await loadCommandLineArgs();
        }

      });

      monitorDefaultConfig.on("removed", function (f) {
        debug(chalkInfo(MODULE_ID_PREFIX + " | XXX FILE DELETED | " + getTimeStamp() + " | " + f));
      });
    });

    //========================
    // WATCH HOST CONFIG
    //========================

    watch.createMonitor(configHostFolder, options, function (monitorHostConfig) {

      console.log(chalkBlue(MODULE_ID_PREFIX + " | INIT WATCH HOST CONFIG FOLDER: " + configHostFolder));

      monitorHostConfig.on("created", async function(f){
        if (f.endsWith(configHostFile)){
          await loadAllConfigFiles();
          await loadCommandLineArgs();
        }
      });

      monitorHostConfig.on("changed", async function(f){
        if (f.endsWith(configHostFile)){
          await loadAllConfigFiles();
          await loadCommandLineArgs();
        }
      });

      monitorHostConfig.on("removed", function (f) {
        debug(chalkInfo(MODULE_ID_PREFIX + " | XXX FILE DELETED | " + getTimeStamp() + " | " + f));
      });
    });

    return;
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX
      + " | *** INIT LOAD ALL CONFIG INTERVAL ERROR: " + err
    ));
    throw err;
  }
}

async function updateCategorizedUser(params){

  if (!params.user || params.user === undefined) {
    console.error(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USERS: USER UNDEFINED"));
    statsObj.errors.users.findOne += 1;
    statsObj.users.processed.errors += 1;
    throw new Error("USER UNDEFINED");
  }

  const userIn = params.user;

  try {

    if (!userIn.category || userIn.category === undefined) {
      console.log(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USERS: USER CATEGORY UNDEFINED | UID: " + userIn.nodeId));
      statsObj.users.notCategorized += 1;
      return;
    }

    if (userIn.screenName === undefined) {
      console.log(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USERS: USER SCREENNAME UNDEFINED | UID: " + userIn.nodeId));
      statsObj.users.screenNameUndefined += 1;
      statsObj.users.notCategorized += 1;
      return;
    }

    if (!userIn.tweetHistograms || (userIn.tweetHistograms === undefined)){
      userIn.tweetHistograms = {};
    }

    if (!userIn.profileHistograms || (userIn.profileHistograms === undefined)){
      userIn.profileHistograms = {};
    }

    if (!userIn.friends || (userIn.friends === undefined)){
      userIn.friends = [];
    }

    const u = await tcUtils.encodeHistogramUrls({user: userIn});

    // const user = await updateUserHistograms({user: u, updateUserInDb: true});
    const user = await tcUtils.updateUserHistograms({ user: u });

    if (!empty(user.profileHistograms.sentiment)) {

      if (user.profileHistograms.sentiment.magnitude !== undefined){
        if (user.profileHistograms.sentiment.magnitude < 0){
          console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! NORMALIZATION MAG LESS THAN 0 | CLAMPED: " + user.profileHistograms.sentiment.magnitude));
          user.profileHistograms.sentiment.magnitude = 0;
        }
        statsObj.normalization.magnitude.max = Math.max(statsObj.normalization.magnitude.max, user.profileHistograms.sentiment.magnitude);
      }

      if (user.profileHistograms.sentiment.score !== undefined){
        if (user.profileHistograms.sentiment.score < -1.0){
          console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! NORMALIZATION SCORE LESS THAN -1.0 | CLAMPED: " + user.profileHistograms.sentiment.score));
          user.profileHistograms.sentiment.score = -1.0;
        }

        if (user.profileHistograms.sentiment.score > 1.0){
          console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! NORMALIZATION SCORE GREATER THAN 1.0 | CLAMPED: " + user.profileHistograms.sentiment.score));
          user.profileHistograms.sentiment.score = 1.0;
        }

        statsObj.normalization.score.max = Math.max(statsObj.normalization.score.max, user.profileHistograms.sentiment.score);
        statsObj.normalization.score.min = Math.min(statsObj.normalization.score.min, user.profileHistograms.sentiment.score);
      }

      if (user.profileHistograms.sentiment.comp !== undefined){
        statsObj.normalization.comp.max = Math.max(statsObj.normalization.comp.max, user.profileHistograms.sentiment.comp);
        statsObj.normalization.comp.min = Math.min(statsObj.normalization.comp.min, user.profileHistograms.sentiment.comp);
      }

      // console.log(chalkAlert(MODULE_ID_PREFIX + " | SENTIMENT: " + user.profileHistograms.sentiment));
      console.log(chalkLog(MODULE_ID_PREFIX
        + " | NID: " + user.nodeId
        + " | @" + user.screenName
        + " | SENTIMENT: SCORE: " + user.profileHistograms.sentiment.score
        + " MAG: " + user.profileHistograms.sentiment.magnitude
        + " COMP: " + user.profileHistograms.sentiment.comp
      ));
      console.log(chalkLog(MODULE_ID_PREFIX + " | NORMALIZATION"
        + " | SCORE min: " + statsObj.normalization.score.min + " max: " + statsObj.normalization.score.max
        + " | MAG min: " + statsObj.normalization.magnitude.min + " max: " + statsObj.normalization.magnitude.max
        + " | COMP min: " + statsObj.normalization.comp.min + " max: " + statsObj.normalization.comp.max
      ));
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

    return user;

  }
  catch(err){
    console.error(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USER ERROR: " + err));
    statsObj.errors.users.findOne += 1;
    statsObj.users.processed.errors += 1;
    throw err;
  }
}

let userIndex = 0;

async function categorizeUser(params){

  try{

    const user = await updateCategorizedUser({user: params.user});

    if (!user || user === undefined) {

      statsObj.users.processed.errors += 1;

      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USR NOT FOUND: "
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.users.processed.errors + " ]"
        + " | USER ID: " + params.nodeId
      ));

      throw new Error("USER NOT FOUND IN DB: " + params.nodeId);
    }

    const userPropertyPickArray = params.userPropertyPickArray || configuration.userPropertyPickArray;

    userIndex += 1;

    // await tcUtils.updateGlobalHistograms({user: user, verbose: true});

    const subUser = pick(
      user,
      userPropertyPickArray
    );

    if (params.verbose) {
      console.log(chalkInfo(MODULE_ID_PREFIX + " | -<- UPDATE CATEGORIZED USR <DB"
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.users.processed.errors + "]"
        + " | " + subUser.nodeId
        + " | @" + subUser.screenName
      ));
    }

    return subUser;

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX
      + " | *** UPDATE CATEGORIZED USER ERROR | USER ID: " + params.user.nodeId 
      + " | ERROR: " + err
    ));
    throw err;
  }
}

const categorizedUsers = {};
categorizedUsers.left = 0;
categorizedUsers.neutral = 0;
categorizedUsers.right = 0;
categorizedUsers.positive = 0;
categorizedUsers.negative = 0;
categorizedUsers.none = 0;

function isValidUser(user){
  if (!user || user === undefined || user === {} || typeof user !== "object") { return false; }
  if (!user.screenName || user.screenName === undefined) { return false; }
  if ((/[^\d]/).test(user.nodeId)) { return false; }
  return true;
}

const formatCategory = tcUtils.formatCategory;

// const handleMongooseEvent = (eventObj) => {

//   // console.log({eventObj})

//   switch (eventObj.event){
//     case "end":
//     case "close":
//       console.log(chalkBlueBold(`${MODULE_ID_PREFIX} | CATEGORY: ${eventObj.category} | CURSOR EVENT: ${eventObj.event.toUpperCase()}`))
//       categoryCursorHash[eventObj.category].complete = true;
//     break;

//     case "error":
//       console.error(chalkError(`${MODULE_ID_PREFIX} | CATEGORY: ${eventObj.category} | CURSOR ERROR: ${eventObj.err}`))
//       categoryCursorHash[eventObj.category].complete = true;
//       categoryCursorHash[eventObj.category].error = eventObj.err;
//     break;

//     default:
//       console.error(chalkError(`*** CATEGORY: ${eventObj.category} | UNKNOWN EVENT: ${eventObj.event}`))
//       throw new Error(`${MODULE_ID_PREFIX} | UNKNOWN CURSOR EVENT: ${eventObj.event}`)
//   }

//   return;
// }

async function cursorDataHandler(params){

  const user = params.user;
  
  statsObj.users.processed.total += 1;
  statsObj.users.processed.remain -= 1;

  statsObj.users.processed.elapsed = (moment().valueOf() - statsObj.users.processed.startMoment.valueOf()); // mseconds
  statsObj.users.processed.rate = (statsObj.users.processed.total >0) ? statsObj.users.processed.elapsed/statsObj.users.processed.total : 0; // msecs/usersArchived
  // statsObj.users.processed.remain = statsObj.users.grandTotal - (statsObj.users.processed.total + statsObj.users.processed.errors);
  statsObj.users.processed.remainMS = statsObj.users.processed.remain * statsObj.users.processed.rate; // mseconds
  statsObj.users.processed.endMoment = moment();
  statsObj.users.processed.endMoment.add(statsObj.users.processed.remainMS, "ms");
  statsObj.users.processed.percent = 100 * (statsObj.users.notCategorized + statsObj.users.processed.total)/statsObj.users.grandTotal;

  statsObj.cursor[user.category].lastFetchedNodeId = user.nodeId;    

  if (!isValidUser(user)){
    console.log(chalkWarn(`${MODULE_ID_PREFIX} | !!! INVALID USER ... SKIPPING | NID: ${user.nodeId} | @${user.screenName}`));
    statsObj.users.processed.errors += 1;
    return;
  }

  if (user.friends === 1){
    console.log(chalkAlert(`${MODULE_ID_PREFIX} | *** FRIENDS INVALID | NID: ${user.nodeId} ... FIXING ...`))
    user.friends = [];
    await global.wordAssoDb.User.updateOne({nodeId: user.nodeId}, { $set: {friends: [] }} ) 
  }

  if (user.tweetHistograms === 1){
    console.log(chalkAlert(`${MODULE_ID_PREFIX} | *** TWEETS HISTOGRAM INVALID | NID: ${user.nodeId} ... FIXING ...`))
    user.tweetHistograms = {};
    await global.wordAssoDb.User.updateOne({nodeId: user.nodeId}, { $set: {"tweetHistograms": {}} }) 
  }

  if (user.tweetHistograms.friends === null){

    console.log(chalkAlert(`${MODULE_ID_PREFIX} | *** TWEETS HISTOGRAM FRIENDS NULL | NID: ${user.nodeId} ... FIXING ...`))
    delete user.tweetHistograms.friends;

    await global.wordAssoDb.User.updateOne({nodeId: user.nodeId}, { $unset: {"tweetHistograms.friends": ""} }) 
  }

  if (user.profileHistograms.friends || user.tweetHistograms.friends){

    if (user.tweetHistograms.friends !== undefined && user.tweetHistograms.friends){
      debug(chalkAlert(`${MODULE_ID_PREFIX} | *** FRIENDS IN TWEETS HISTOGRAM | NID: ${user.nodeId} | ${Array.isArray(user.tweetHistograms.friends) ? user.tweetHistograms.friends.length : "NOT ARRAY"}`))

      if (Array.isArray(user.tweetHistograms.friends) && user.tweetHistograms.friends.length > 0){
        console.log(`${MODULE_ID_PREFIX} | *** FRIENDS: ${user.friends.length} | TW HIST FRIENDS: ${user.tweetHistograms.friends.length}`)
        user.friends = user.friends === undefined || user.friends.length === 0 ? _.union([], user.tweetHistograms.friends) : _.union(user.friends, user.tweetHistograms.friends)
        console.log(`${MODULE_ID_PREFIX} | *** FRIENDS MERGED: ${user.friends.length}`)
      }

      delete user.tweetHistograms.friends;

      await global.wordAssoDb.User.updateOne({nodeId: user.nodeId}, { $set: {friends: user.friends }, $unset: {"tweetHistograms.friends": ""} }) 

    }
  }

  if (
    (user.friends === undefined || !user.friends || user.friends.length === 0) 
    && (user.profileHistograms === undefined || !user.profileHistograms || Object.keys(user.profileHistograms).length === 0)
    && (user.tweetHistograms === undefined || !user.tweetHistograms || Object.keys(user.tweetHistograms).length === 0)
  ){

    statsObj.users.processed.empty += 1;

    if (configuration.testMode || statsObj.users.processed.empty % 100 === 0){
      console.log(chalkWarn(MODULE_ID_PREFIX 
        + " | --- EMPTY HISTOGRAMS"
        + " | SKIPPING"
        + " | PRCSD/REM/MT/ERR/TOT: " 
        + statsObj.users.processed.total 
        + "/" + statsObj.users.processed.remain 
        + "/" + statsObj.users.processed.empty 
        + "/" + statsObj.users.processed.errors
        + "/" + statsObj.users.grandTotal
        + " | FRNDs: " + (user.friends ? user.friends.length : 0)
        + " | PF HIST: " + (user.profileHistograms ? Object.keys(user.profileHistograms).length : 0)
        + " | TW HIST: " + (user.tweetHistograms ? Object.keys(user.tweetHistograms).length : 0)
        + " | CAT: " + formatCategory(user.category) 
        + " | FLWRs: " + user.followersCount
        + " | NID: " + user.nodeId 
        + " | @" + user.screenName 
      )); 
    }
    return;
  }

  if (!user.friends || user.friends === undefined) {
    user.friends = [];
  }
  else{
    user.friends = _.slice(user.friends, 0, configuration.maxUserFriends);
  }

  const catUser = await categorizeUser({
    user: user, 
    verbose: configuration.verbose, 
    testMode: configuration.testMode
  });

  if (Math.random() < configuration.userGlobalHistogramProbability){
    await tcUtils.updateGlobalHistograms({user: catUser, verbose: true});
    statsObj.users.processed.updatedGlobalHistograms += 1;
    debug(chalkLog(`${MODULE_ID_PREFIX} | ->- UPDATE GLOBAL HIST [${statsObj.users.processed.updatedGlobalHistograms}/${statsObj.users.grandTotal}] @${catUser.screenName}`))
  }

  const hash = await tcUtils.hashUserId({nodeId: catUser.nodeId}); // 1000 buckets/subfolders by default
  const subFolder = hash.toString().padStart(8,"0");

  const folder = path.join(configuration.userDataFolder, subFolder);
  const file = catUser.nodeId + ".json";

  if (configuration.enableCreateUserArchive){ subFolderSet.add(subFolder); }

  const saveFileQueue = tcUtils.saveFileQueue({
    folder: folder,
    file: file,
    obj: catUser,
    verbose: configuration.verbose
  });

  categorizedUsers[catUser.category] += 1;
  statsObj.categorizedCount += 1;

  if (statsObj.categorizedCount > 0 && statsObj.categorizedCount % 100 === 0){
    console.log(chalkInfo(MODULE_ID_PREFIX
      + " [ SFQ: " + saveFileQueue + " ]"
      + " | GLOBAL HIST: " + statsObj.users.processed.updatedGlobalHistograms
      + " | CATEGORIZED: " + statsObj.categorizedCount
      + " | L: " + categorizedUsers.left
      + " | N: " + categorizedUsers.neutral
      + " | R: " + categorizedUsers.right
      + " | +: " + categorizedUsers.positive
      + " | -: " + categorizedUsers.negative
      + " | 0: " + categorizedUsers.none
    ));
  }

  return;
}

const fetchReady = async () => {

  statsObj.fetchReady.saveFileQueue = tcUtils.getSaveFileQueue();
  statsObj.fetchReady.maxSaveFileQueue = Math.max(statsObj.fetchReady.maxSaveFileQueue, statsObj.fetchReady.saveFileQueue)

  statsObj.fetchReady.queueOverShoot = statsObj.fetchReady.saveFileQueue - configuration.maxSaveFileQueue;
  statsObj.fetchReady.maxQueueOverShoot = Math.max(statsObj.fetchReady.maxQueueOverShoot, statsObj.fetchReady.queueOverShoot)

  if (statsObj.fetchReady.queueOverShoot <= 0) {
    return;
  }

  statsObj.fetchReady.period = statsObj.fetchReady.queueOverShoot * configuration.saveFileBackPressurePeriod;
  statsObj.fetchReady.maxPeriod = Math.max(statsObj.fetchReady.maxPeriod, statsObj.fetchReady.period)

  await wait({
    message: "BK PRSSR | SFQ: " + statsObj.fetchReady.saveFileQueue, 
    period: statsObj.fetchReady.period,
    verbose: configuration.verbose
  });

  return;

}

async function categoryCursorStream(params){

  const category = params.category;
  const cursorParallel = params.cursorParallel || 1;

  statsObj.status = "categoryCursorStream";
  statsObj.categorizedCount = 0;

  const cursorBatchSize = params.cursorBatchSize || configuration.cursorBatchSize;

  let maxArchivedCount = null;

  if (configuration.testMode) {
    maxArchivedCount = configuration.maxTestCount[category];
  }


  console.log(chalkGreen("\n" + MODULE_ID_PREFIX
    + " | =============================================================================================================="
  ));
  console.log(chalkGreen(MODULE_ID_PREFIX 
    + " | CATEGORIZE | CATEGORY: " + category + ": " + statsObj.userCategoryTotal[category] 
    + "\n" + MODULE_ID_PREFIX
    + " | TEST MODE: " + configuration.testMode
    + " | MAX COUNT: " + maxArchivedCount
    + " | CURSOR BATCH SIZE: " + cursorBatchSize
    + " | CURSOR PARALLEL: " + cursorParallel
    + " | MAX SFQ: " + configuration.maxSaveFileQueue
    + " | SAVE BACK PRESSURE PERIOD: " + configuration.saveFileBackPressurePeriod
    + " | SFQ PARALLEL: " + configuration.saveFileMaxParallel
    + " | TOTAL USERS: " + statsObj.users.grandTotal
  ));
  console.log(chalkGreen(MODULE_ID_PREFIX
    + " | ==============================================================================================================\n"
  ));

  const query = {};
  query.ignored = false;
  query.category = category;

  console.log(chalkBlue(MODULE_ID_PREFIX
    + " | categoryCursorStream"
    + " | cursorBatchSize: " + cursorBatchSize
    + " | maxArchivedCount: " + maxArchivedCount
  ));

  const cursor = await mgUtils.initCursor({
    query: query,
    cursorBatchSize: cursorBatchSize,
    cursorLimit: maxArchivedCount,
    cursorLean: true,
  })

  await cursor.eachAsync(

    async function (user) {

      if (!user) {
        statsObj.status = `END CURSOR ${category}`
        console.log(chalkBlueBold(`${MODULE_ID_PREFIX} | categoryCursorStream | +++ ENDING FETCH USER INTERVAL | NO USER FROM DB CURSOR | CATEGORY: ${category}`))
        cursor.close();
      }

      await cursorDataHandler({user: user})

      const saveFileQueue = tcUtils.getSaveFileQueue();
      const queueOverShoot = saveFileQueue - configuration.maxSaveFileQueue;

      if (queueOverShoot > 0) {
        await wait({
          message: "BK PRSSR | SFQ: " + saveFileQueue, 
          period: queueOverShoot * configuration.saveFileBackPressurePeriod,
          verbose: configuration.verbose
        });
      }

    },
    { parallel: cursorParallel }
  );

  return;

  // let fetchUserReady = true;

  // if (statsObj.cursor[category] === undefined) { statsObj.cursor[category] = {}; }
  // if (statsObj.users.processed.startMoment === 0) { statsObj.users.processed.startMoment = moment(); }

  // fetchUserInterval = setInterval(async () => {

  //   if (fetchUserReady) {

  //     try {

  //       fetchUserReady = false;
  //       const user = await cursor.next();

  //       if (!user){
  //         statsObj.status = `END CURSOR ${category}`
  //         console.log(chalkBlueBold(`${MODULE_ID_PREFIX} | categoryCursorStream | +++ ENDING FETCH USER INTERVAL | NO USER FROM DB CURSOR | CATEGORY: ${category}`))
  //         await cursor.close();
  //         clearInterval(fetchUserInterval)
  //       }
  //       else{
  //         await cursorDataHandler({user: user})
  //         await fetchReady()
  //         fetchUserReady = true;
  //       }

  //     }
  //     catch (err) {
  //       console.error(`${MODULE_ID_PREFIX} | *** ERROR: ${err}`)
  //       fetchUserReady = true;
  //     }

  //   } 

  // }, configuration.cursorInterval);

  // return cursor;
}


const allCursorsComplete = () => Object.values(categoryCursorHash).every((categoryCursor) => categoryCursor.complete)

function endSaveFileQueue(){

  return new Promise(function(resolve){

    statsObj.status = "endSaveFileQueue";

    let saveFileQueue = tcUtils.getSaveFileQueue();

    console.log(chalkLog(MODULE_ID_PREFIX
      + " | " + getTimeStamp()
      + " | ... WAIT END SAVE FILE QUEUE"
      + " | SFQ: " + saveFileQueue
    ));

    endSaveFileQueueInterval = setInterval(function(){

      saveFileQueue = tcUtils.getSaveFileQueue();

      debug(chalkInfo(`${MODULE_ID_PREFIX} | allCursorsComplete: ${allCursorsComplete()} | saveFileQueue: ${saveFileQueue}`))

      if (saveFileQueue === 0 && allCursorsComplete()){
        clearInterval(endSaveFileQueueInterval);
        console.log(chalkBlueBold(MODULE_ID_PREFIX + " | +++ END SAVE FILE QUEUE"));
        resolve();
      }

    }, 10*ONE_SECOND);

  });
}

function delay(params){
  return new Promise(function(resolve){

    if (params.message) {
      console.log(chalkLog(MODULE_ID_PREFIX + " | " + params.message + " | PERIOD: " + params.period + " MS"));
    }

    setTimeout(function(){
      resolve(true);
    }, params.period);

  });
}

function wait(params){

  return new Promise(function(resolve){

    let saveFileQueue = tcUtils.getSaveFileQueue();

    if (saveFileQueue <= configuration.maxSaveFileQueue){
      return resolve(true);
    }

    if (params.message && params.verbose) {
      console.log(chalkLog(`${MODULE_ID_PREFIX} | ${params.message} | PERIOD: ${params.period} MS`));
    }

    const start = moment().valueOf();

    const waitInterval = setInterval(function(){

      saveFileQueue = tcUtils.getSaveFileQueue();

      if (saveFileQueue <= configuration.maxSaveFileQueue){

        const deltaMS = (moment().valueOf() - start);

        clearInterval(waitInterval);

        if (params.verbose) {
          console.log(chalkLog(`${MODULE_ID_PREFIX} | XXX WAIT END BACK PRESSURE | SFQ: ${saveFileQueue} | PERIOD: ${params.period} MS | TOTAL WAIT: ${deltaMS} MS`));
        }

        return resolve(true);
      }

    }, params.period);

  });
}

const fileSizeArrayObj = {};
fileSizeArrayObj.files = [];

async function updateArchiveFileUploadComplete(params){

  try{

    statsObj.status = "updateArchiveFileUploadComplete";

    fileSizeArrayObj.runId = statsObj.runId;

    const stats = fs.statSync(params.path);
    const fileSizeInBytes = stats.size;
    const savedSize = fileSizeInBytes/ONE_MEGABYTE;

    console.log(chalkLog(MODULE_ID_PREFIX + " | ... UPDATE FLAG" 
      + " | " + params.path
      + " | " + fileSizeInBytes + " B | " + savedSize.toFixed(3) + " MB"
    ));

    fileSizeArrayObj.files.push({
      path: params.path,
      size: fileSizeInBytes
    });

    return;

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** updateArchiveFileUploadComplete ERROR", err));
    throw err;
  }
}

function archiveFolder(params){

  return new Promise(function(resolve, reject){

    const verbose = params.verbose || configuration.verbose;

    statsObj.status = "archiveFolder";
    statsObj.archivePath = params.archivePath;

    console.log(chalkBlue(MODULE_ID_PREFIX + " | ARCHIVE FOLDER"
      + " | SRC: " + params.folder
      + " | DST: " + params.archivePath
    ));

    const output = fs.createWriteStream(params.archivePath);

    archive = archiver("zip", {
      zlib: { level: 9 } // Sets the compression level.
    });
     
    output.on("close", function() {
      const archiveSize = toMegabytes(archive.pointer());
      console.log(chalkGreen(MODULE_ID_PREFIX
        + " | +O+ ARCHIVE OUTPUT | CLOSED" 
        + " | SRC: " + params.folder
        + " | DST: " + params.archivePath
        + " | SIZE: " + archiveSize.toFixed(2) + " MB"
      ));
      resolve(archiveSize);
    });
     
    output.on("end", function() {
      const archiveSize = toMegabytes(archive.pointer());
      console.log(chalkGreen(MODULE_ID_PREFIX
        + " | 000 ARCHIVE OUTPUT | END" 
        + " | SRC: " + params.folder
        + " | DST: " + params.archivePath
        + " | SIZE: " + archiveSize.toFixed(2) + " MB"
      ));
    });
     
    archive.on("warning", function(err) {
      console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! ARCHIVE | WARNING\n" + jsonPrint(err)));
      // if (err.code !== "ENOENT") {
      // }
    });
     
    archive.on("progress", function(progress) {

      statsObj.progress = progress;
      statsObj.progressMbytes = toMegabytes(progress.fs.processedBytes);
      statsObj.totalMbytes = toMegabytes(archive.pointer());

      if (verbose){
        console.log(chalkGreen(MODULE_ID_PREFIX
          + " | ->- ARCHIVE" 
          + " | PROGRESS: " + statsObj.progressMbytes.toFixed(3) + " MB"
          + " | TOTAL: " + statsObj.totalMbytes.toFixed(3) + " MB"
          + " | DST: " + params.archivePath
        ));
      }

    });
     
    archive.on("close", function() {
      const archiveSize = toMegabytes(archive.pointer());
      console.log(chalkGreen(MODULE_ID_PREFIX
        + " | XXX ARCHIVE | CLOSED" 
        + " | SRC: " + params.folder
        + " | DST: " + params.archivePath
        + " | SIZE: " + archiveSize.toFixed(2) + " MB"
      ));
    });
     
    archive.on("finish", function() {
      const archiveSize = toMegabytes(archive.pointer());
      console.log(chalkGreen(MODULE_ID_PREFIX
        + " | XXX ARCHIVE | FINISHED" 
        + " | SRC: " + params.folder
        + " | DST: " + params.archivePath
        + " | SIZE: " + archiveSize.toFixed(2) + " MB"
      ));
    });
     
    archive.on("error", function(err) {
      console.log(chalkError(MODULE_ID_PREFIX
        + " | *** ARCHIVE | ERROR" 
        + " | SRC: " + params.folder
        + " | DST: " + params.archivePath
        + " | ERROR: " + err
      ));
      return reject(err);
    });
     
    archive.pipe(output);

    archive.directory(params.folder, false);
    
    archive.finalize();

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
    console.log(MODULE_ID_PREFIX + " | ENV GTS_QUIT_ON_COMPLETE: " + process.env.GTS_QUIT_ON_COMPLETE);
    if (!process.env.GTS_QUIT_ON_COMPLETE || (process.env.GTS_QUIT_ON_COMPLETE === false) || (process.env.GTS_QUIT_ON_COMPLETE === "false")) {
      cnf.quitOnComplete = false;
    }
    else {
      cnf.quitOnComplete = true;
    }
  }

  await initStdIn();

  configuration = await loadAllConfigFiles(configuration);

  await loadCommandLineArgs();
  
  const configArgs = Object.keys(configuration);

  configArgs.forEach(function(arg){
    if (_.isObject(configuration[arg])) {
      console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + "\n" + jsonPrint(configuration[arg]));
    }
    else {
      console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + ": " + configuration[arg]);
    }
  });
  
  statsObj.commandLineArgsLoaded = true;

  global.dbConnection = await mgUtils.connectDb()

  return configuration;
}

const categoryCursorHash = {}

async function generateGlobalTrainingTestSet(){

  statsObj.status = "generateGlobalTrainingTestSet";

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | GENERATE TRAINING SET | " + getTimeStamp()));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | USER DATA FOLDER: " + configuration.userDataFolder));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  statsObj.userCategoryTotal = {};

  for(const category of ["left", "neutral", "right"]){
    const query = { "category": category, ignored: false };
    statsObj.userCategoryTotal[category] = await global.wordAssoDb.User.countDocuments(query);
    statsObj.users.grandTotal += statsObj.userCategoryTotal[category];
  }

  statsObj.users.processed.remain = statsObj.users.grandTotal;

  configuration.userGlobalHistogramProbability = statsObj.users.grandTotal > 0 ? configuration.maxGlobalHistogramUsers/statsObj.users.grandTotal : 0;

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | CATEGORIZED USERS IN DB: " + statsObj.users.grandTotal));
  // console.log(chalkBlueBold(MODULE_ID_PREFIX + " | GLOBAL HISTOGRAM PROBABILITY: " + configuration.userGlobalHistogramProbability.toFixed(3)));
  console.log(chalkBlueBold(MODULE_ID_PREFIX 
    + " | GLOBAL HISTOGRAM TOTAL: " + configuration.maxGlobalHistogramUsers 
    + " | " + 100*configuration.userGlobalHistogramProbability.toFixed(3) + "%"
  ));
  console.log(chalkBlueBold(MODULE_ID_PREFIX 
    + " | L: " + statsObj.userCategoryTotal.left 
    + " | N: " + statsObj.userCategoryTotal.neutral
    + " | R: " + statsObj.userCategoryTotal.right
  ));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  if (configuration.testMode) {
    configuration.maxSaveFileQueue = 100;
    statsObj.users.grandTotal = Math.min(statsObj.users.grandTotal, configuration.totalMaxTestCount);
    statsObj.users.processed.remain = statsObj.users.grandTotal;
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** TEST MODE *** | CATEGORIZE MAX " + statsObj.users.grandTotal + " USERS"));
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** TEST MODE *** | MAX SAVE FILE QUEUE: " + configuration.maxSaveFileQueue));
  }
  statsObj.users.fetched = 0;
  statsObj.users.skipped = 0;

  for(const category of ["left", "neutral", "right"]){

    categoryCursorHash[category] = {};
    categoryCursorHash[category].category = category;
    categoryCursorHash[category].complete = false;
    await categoryCursorStream({ category: category, ignored: false, cursorParallel: configuration.cursorParallel });
    categoryCursorHash[category].complete = true;

    // categoryCursorHash[category].cursor = await categoryCursorStream({ category: category, ignored: false });
    
    // categoryCursorHash[category].cursor.on("error", async (err) => handleMongooseEvent({category: category, event: "error", err: err}));
    // categoryCursorHash[category].cursor.on("end", async () => handleMongooseEvent({category: category, event: "end"}));
    // categoryCursorHash[category].cursor.on("close", async () => handleMongooseEvent({category: category, event: "close"}));

  }

  return;
}

setTimeout(async function(){

  try{

    showStatsInterval = setInterval(function(){
      showStats();
    }, ONE_MINUTE);

    configuration = await initialize(configuration);

    if (configuration.saveGlobalHistogramsOnly){
      console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! SAVE GLOBAL HISTOGRAMS ONLY | NO REDIS FLUSH"));
    }
    else{
      console.log(chalkAlert(`${MODULE_ID_PREFIX} | FLUSHING REDIS ALL ...`));
      const redisResult = await redisClient.flushall();
      // const redisResult = await redisClient.flushdb();
      console.log(chalkAlert(MODULE_ID_PREFIX + " | REDIS FLUSH ALL RESULT: " + redisResult));
    }

    tcUtils.setSaveFileMaxParallel(configuration.saveFileMaxParallel);
    tcUtils.enableSaveFileMaxParallel(true);
    await tcUtils.initSaveFileQueue({interval: configuration.saveFileQueueInterval});

    await initSlackWebClient();

    if (configuration.enableCreateUserArchive && !configuration.saveGlobalHistogramsOnly){
      console.log(chalkAlert(MODULE_ID_PREFIX + " | XXX DELETE TEMP USER DATA FOLDER: " + configuration.tempUserDataFolder));

      fs.rmdirSync(configuration.tempUserDataFolder, { recursive: true });

      statsObj.runSubFolder = path.join(configuration.userArchiveFolder, statsObj.runId);

      console.log(chalkAlert(MODULE_ID_PREFIX + " | +++ CREATE USER ARCHIVE FOLDER: " + statsObj.runSubFolder));

      fs.mkdirSync(statsObj.runSubFolder, { recursive: true });
    }

    if (!configuration.saveGlobalHistogramsOnly){

      await initWatchAllConfigFolders();
      await generateGlobalTrainingTestSet();
      await endSaveFileQueue();

      categorizedUserHistogramTotal();

      const saveFileQueue = tcUtils.getSaveFileQueue();

      console.log(chalkInfo(MODULE_ID_PREFIX + " | ... SAVING NORMALIZATION FILE"
        + " [ SFQ: " + saveFileQueue
        + " | " + configuration.trainingSetsFolder + "/normalization.json"
      ));

      console.log(chalkLog(MODULE_ID_PREFIX + " | NORMALIZATION"
        + " | SCORE min: " + statsObj.normalization.score.min + " max: " + statsObj.normalization.score.max
        + " | MAG min: " + statsObj.normalization.magnitude.min + " max: " + statsObj.normalization.magnitude.max
        + " | COMP min: " + statsObj.normalization.comp.min + " max: " + statsObj.normalization.comp.max
      ));

      tcUtils.saveFileQueue({
        folder: configuration.trainingSetsFolder, 
        file: "normalization.json", 
        obj: statsObj.normalization,
        verbose: configuration.verbose
      });
    }

    fileSizeArrayObj.histograms = {};
    fileSizeArrayObj.histograms = categorizedUserHistogram;

    await endSaveFileQueue();

    if (configuration.enableCreateUserArchive){
      console.log(chalkLog(MODULE_ID_PREFIX + " | SAVE USER DATA ARCHIVES ..."));

      statsObj.status = "SAVE USER DATA ARCHIVES"

      for(const subFolderIndexString of [...subFolderSet] ){
        const folder = path.join(configuration.tempUserDataFolder, "data", subFolderIndexString);
        const archivePath = path.join(statsObj.runSubFolder, "userArchive" + subFolderIndexString + ".zip");
        await archiveFolder({folder: folder, archivePath: archivePath});
        await updateArchiveFileUploadComplete({path: archivePath});
      }
    }

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

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ... SAVING HISTOGRAMS | " + rootFolder));

    statsObj.status = "SAVE HISTOGRAMS"

    // will use default input min hashmaps

    const inputTypeMinProfileHashMap = configuration.testMode ? configuration.testInputTypeMinProfileHashMap : configuration.inputTypeMinProfileHashMap
    const inputTypeMinTweetsHashMap = configuration.testMode ? configuration.testInputTypeMinTweetsHashMap : configuration.inputTypeMinTweetsHashMap

    await tcUtils.saveGlobalHistograms({
      rootFolder: rootFolder, 
      pruneFlag: configuration.pruneFlag, 
      inputTypeMinProfileHashMap: inputTypeMinProfileHashMap, // DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP
      inputTypeMinTweetsHashMap: inputTypeMinTweetsHashMap, // DEFAULT_MIN_TOTAL_MIN_ _TYPE_HASHMAP
      verbose: configuration.verbose
    });

    if (configuration.testMode) {
      configuration.archiveFileUploadCompleteFlagFile = configuration.archiveFileUploadCompleteFlagFile.replace(/\.json/, "_test.json");
    }
    console.log(chalkInfo(MODULE_ID_PREFIX + " | ... SAVING FLAG FILE"
      + " | " + configuration.trainingSetsFolder + "/" + configuration.archiveFileUploadCompleteFlagFile
    ));

    tcUtils.saveFileQueue({
      folder: configuration.trainingSetsFolder,
      file: configuration.archiveFileUploadCompleteFlagFile,
      obj: fileSizeArrayObj,
      verbose: configuration.verbose
    });

    let slackText = "\n*" + MODULE_ID_PREFIX + " | TRAINING SET*";
    // slackText = slackText + "\n" + configuration.userArchivePath;
    slackText = slackText + "\nUSERS ARCHIVED: " + statsObj.users.grandTotal;
    slackText = slackText + "\nEMPTY: " + statsObj.users.processed.empty;
    slackText = slackText + "\nERRORS: " + statsObj.users.processed.errors;
    slackText = slackText + "\nLEFT: " + categorizedUsers.left;
    slackText = slackText + "\nRIGHT: " + categorizedUsers.right;
    slackText = slackText + "\nNEUTRAL: " + categorizedUsers.neutral;
    slackText = slackText + "\nPOSITIVE: " + categorizedUsers.positive;
    slackText = slackText + "\nNEGATIVE: " + categorizedUsers.negative;
    slackText = slackText + "\nNONE: " + categorizedUsers.none;

    await slackSendWebMessage({channel: slackChannel, text: slackText});

    await endSaveFileQueue();

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | XXX MAIN END XXX "));
    await quit("OK");
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** MAIN ERROR: " + err));
    await quit("MAIN ERROR: " + err);
  }
}, 1000);
