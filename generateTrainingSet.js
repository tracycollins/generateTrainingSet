const MODULE_ID_PREFIX = "GTS";
const GLOBAL_TRAINING_SET_ID = "globalTrainingSet";

const DEFAULT_SAVE_GLOBAL_HISTOGRAMS_ONLY = false;
const DEFAULT_CURSOR_PARALLEL = 8;
const DEFAULT_BATCH_SIZE = 100;
const DEFAULT_SAVE_FILE_MAX_PARALLEL = 16;
const DEFAULT_SAVE_FILE_BACKPRESSURE_PERIOD = 10; // ms
const DEFAULT_ENABLE_CREATE_USER_ARCHIVE = false;

const DEFAULT_MAX_SAVE_FILE_QUEUE = 100;
const DEFAULT_MAX_CURSOR_DATA_HANDLER_QUEUE = 100;

const DEFAULT_INTERVAL = 5;
const DEFAULT_REDIS_SCAN_COUNT = 1000;
const DEFAULT_USERS_PER_ARCHIVE = 10000;
const DEFAULT_SAVE_FILE_QUEUE_INTERVAL = 5;
const DEFAULT_RESAVE_USER_DOCS_FLAG = false;
const DEFAULT_MAX_HISTOGRAM_VALUE = 1000;

const DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP = {};
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

const DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP = {};
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


const TOTAL_MAX_TEST_COUNT = 347;

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

// const DATA_ROOT_FOLDER = process.env.DATA_ROOT_FOLDER || "/Volumes/RAID1/data";
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

// const DEFAULT_INPUT_TYPES = [
//   "emoji",
//   "friends",
//   "hashtags",
//   "images",
//   "locations",
//   "media",
//   "ngrams",
//   "places",
//   "sentiment",
//   "urls",
//   "userMentions",
//   "words"
// ];

// const defaultInputTypeMinHash = {
//   emoji: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   friends: DEFAULT_INPUT_TYPE_MIN_FRIENDS,
//   hashtags: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   images: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   locations: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   media: DEFAULT_INPUT_TYPE_MIN_MEDIA,
//   ngrams: DEFAULT_INPUT_TYPE_MIN_NGRAMS,
//   places: DEFAULT_INPUT_TYPE_MIN_PLACES,
//   sentiment: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   urls: DEFAULT_INPUT_TYPE_MIN_URLS,
//   userMentions: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
//   words: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM
// };

// const testInputTypeMinHash = {
//   emoji: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   friends: 20,
//   hashtags: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   images: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   locations: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   media: 1,
//   ngrams: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   places: 1,
//   sentiment: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   urls: 1,
//   userMentions: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
//   words: 10
// };

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;
// const ONE_HOUR = 60 * ONE_MINUTE;
// const ONE_DAY = 24 * ONE_HOUR;

const ONE_KILOBYTE = 1024;
const ONE_MEGABYTE = 1024 * ONE_KILOBYTE;
const ONE_GIGABYTE = 1024 * ONE_MEGABYTE;

const compactDateTimeFormat = "YYYYMMDD_HHmmss";

const DEFAULT_SERVER_MODE = false;
const DEFAULT_QUIT_ON_COMPLETE = false;
const DEFAULT_TEST_RATIO = 0.20;

const path = require("path");
const moment = require("moment");
const merge = require("deepmerge");
const archiver = require("archiver");
const watch = require("watch");
const fs = require("fs");
const util = require("util");
const _ = require("lodash");
// const sizeof = require("object-sizeof");
const pick = require("object.pick");
const EventEmitter3 = require("eventemitter3");
const debug = require("debug")("gts");
const commandLineArgs = require("command-line-args");
const empty = require("is-empty");
// const async = require("async");

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
  "histograms", 
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

statsObj.cursor = {};
statsObj.cursor.left = {};
statsObj.cursor.left.lastFetchedNodeId = false;
statsObj.cursor.right = {};
statsObj.cursor.right.lastFetchedNodeId = false;
statsObj.cursor.neutral = {};
statsObj.cursor.neutral.lastFetchedNodeId = false;

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
  "userReadyTransmitted"
];

statsObjSmall = pick(statsObj, statsPickArray);

let configuration = {}; // merge of defaultConfiguration & hostConfiguration

configuration.saveFileBackPressurePeriod = DEFAULT_SAVE_FILE_BACKPRESSURE_PERIOD;
configuration.saveGlobalHistogramsOnly = DEFAULT_SAVE_GLOBAL_HISTOGRAMS_ONLY;
// configuration.splitSizeMB = DEFAULT_SPLIT_SIZE_MB;
// configuration.splitSizeKeys = DEFAULT_SPLIT_SIZE_KEYS;
configuration.enableCreateUserArchive = DEFAULT_ENABLE_CREATE_USER_ARCHIVE;
configuration.maxCursorDataHandlerQueue = DEFAULT_MAX_CURSOR_DATA_HANDLER_QUEUE;
configuration.redisScanCount = DEFAULT_REDIS_SCAN_COUNT;
// configuration.updateMaxInputHashMapLimit = DEFAULT_MAX_INPUT_HASHMAP_LIMIT;
// configuration.waitValueInterval = DEFAULT_WAIT_VALUE_INTERVAL;
configuration.saveFileMaxParallel = DEFAULT_SAVE_FILE_MAX_PARALLEL;
configuration.usersPerArchive = DEFAULT_USERS_PER_ARCHIVE;
configuration.cursorParallel = DEFAULT_CURSOR_PARALLEL;
configuration.reSaveUserDocsFlag = DEFAULT_RESAVE_USER_DOCS_FLAG;
configuration.batchSize = DEFAULT_BATCH_SIZE;
configuration.saveFileQueueInterval = DEFAULT_SAVE_FILE_QUEUE_INTERVAL;
configuration.maxSaveFileQueue = DEFAULT_MAX_SAVE_FILE_QUEUE;
configuration.verbose = false;
configuration.testMode = false; // per tweet test mode
configuration.testSetRatio = DEFAULT_TEST_RATIO;
configuration.totalMaxTestCount = TOTAL_MAX_TEST_COUNT;

configuration.inputTypeMinTweetsHashMap = DEFAULT_MIN_TOTAL_MIN_TWEETS_TYPE_HASHMAP;
configuration.inputTypeMinProfileHashMap = DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP;

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

configuration.serverMode = DEFAULT_SERVER_MODE;

console.log(chalkLog(MODULE_ID_PREFIX + " | SERVER MODE: " + configuration.serverMode));

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

// const defaultDataFolder = "/Volumes/nas4/data";

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

fs.mkdirSync(configuration.tempUserDataFolder, { recursive: true });
fs.mkdirSync(configuration[HOST].userArchiveFolder, { recursive: true });

let mongooseDb;

global.wordAssoDb = require("@threeceelabs/mongoose-twitter");

const tcuChildName = MODULE_ID_PREFIX + "_TCU";
const ThreeceeUtilities = require("@threeceelabs/threecee-utilities");
const tcUtils = new ThreeceeUtilities(tcuChildName);
const redisClient = tcUtils.redisClient;
const jsonPrint = tcUtils.jsonPrint;
const getTimeStamp = tcUtils.getTimeStamp;
const msToTime = tcUtils.msToTime;
// const formatBoolean = tcUtils.formatBoolean;
// const formatCategory = tcUtils.formatCategory;

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
  // return categorizedUserHistogram.total;
}

const configEvents = new EventEmitter3({
  wildcard: true,
  newListener: true,
  maxListeners: 20,
  verboseMemoryLeak: true
});

let stdin;

//=========================================================================
// SLACK
//=========================================================================

// const slackChannel = "gts";
// const channelsHashMap = new HashMap();

// const slackOAuthAccessToken = "xoxp-3708084981-3708084993-206468961315-ec62db5792cd55071a51c544acf0da55";
// const slackConversationId = "D65CSAELX"; // wordbot
// const slackRtmToken = "xoxb-209434353623-bNIoT4Dxu1vv8JZNgu7CDliy";

// let slackRtmClient;
// let slackWebClient;

// async function slackSendRtmMessage(msg){
//   console.log(chalkBlue(MODULE_ID_PREFIX + " | SLACK RTM | SEND: " + msg));

//   const sendResponse = await slackRtmClient.sendMessage(msg, slackConversationId);

//   console.log(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM | >T\n" + jsonPrint(sendResponse)));
//   return sendResponse;
// }

// async function slackSendWebMessage(msgObj){

//   try{
    
//     const token = msgObj.token || slackOAuthAccessToken;
//     const channel = msgObj.channel || configuration.slackChannel.id;
//     const text = msgObj.text || msgObj;

//     const message = {
//       token: token, 
//       channel: channel,
//       text: text
//     };

//     if (msgObj.attachments !== undefined) {
//       message.attachments = msgObj.attachments;
//     }

//     if (slackWebClient && slackWebClient !== undefined) {
//       const sendResponse = await slackWebClient.chat.postMessage(message);
//       return sendResponse;
//     }
//     else {
//       console.log(chalkAlert(MODULE_ID_PREFIX + " | SLACK WEB NOT CONFIGURED | SKIPPING SEND SLACK MESSAGE\n" + jsonPrint(message)));
//       return;
//     }
//   }
//   catch(err){
//     console.log(chalkAlert(MODULE_ID_PREFIX + " | *** slackSendWebMessage ERROR: " + err));
//     console.log(chalkAlert(MODULE_ID_PREFIX + " | *** slackSendWebMessage msgObj\n" + jsonPrint(msgObj)));
//     throw err;
//   }
// }

// function slackMessageHandler(message){
//   return new Promise(function(resolve, reject){

//     try {

//       console.log(chalkInfo(MODULE_ID_PREFIX + " | MESSAGE | " + message.type + " | " + message.text));

//       if (message.type !== "message") {
//         console.log(chalkAlert("Unhandled MESSAGE TYPE: " + message.type));
//         return resolve();
//       }

//       const text = message.text.trim();
//       const textArray = text.split("|");

//       const sourceMessage = (textArray[2]) ? textArray[2].trim() : "NONE";

//       switch (sourceMessage) {
//         case "END FETCH ALL":
//         case "ERROR":
//         case "FETCH FRIENDS":
//         case "FSM INIT":
//         case "FSM FETCH_ALL":
//         case "GEN AUTO CAT":
//         case "INIT CHILD":
//         case "INIT LANG ANALYZER":
//         case "INIT MAX INPUT HASHMAP":
//         case "INIT NNs":
//         case "INIT RAN NNs":
//         case "INIT RNT CHILD":
//         case "INIT TWITTER USERS":
//         case "INIT TWITTER":
//         case "INIT UNFOLLOWABLE USER SET":
//         case "INIT UNFOLLOWABLE":
//         case "INIT":
//         case "LOAD BEST NN":
//         case "LOAD NN":
//         case "MONGO DB CONNECTED":
//         case "PONG":
//         case "QUIT":
//         case "QUITTING":
//         case "READY":
//         case "RESET":
//         case "SAV NN HASHMAP":
//         case "SLACK QUIT":
//         case "SLACK READY":
//         case "SLACK RTM READY":
//         case "START":
//         case "STATS":
//         case "TEXT": 
//         case "UPDATE HISTOGRAMS":
//         case "UPDATE NN STATS":
//         case "WAIT UPDATE STATS":
//         case "END UPDATE STATS":
//         case "UPDATE USER CAT STATS":
//           resolve();
//         break;
//         case "STATSUS":
//           console.log(chalkInfo(message.text));
//           resolve();
//         break;
//         case "PING":
//           console.log(chalkInfo("PING"));
//           resolve();
//         break;
//         case "NONE":
//           resolve();
//         break;
//         default:
//           console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UNDEFINED SLACK MESSAGE: " + message.text));
//           resolve({text: "UNDEFINED SLACK MESSAGE", message: message});
//       }
//     }
//     catch(err){
//       reject(err);
//     }

//   });
// }

// async function initSlackWebClient(){
//   try {

//     console.log(chalkLog(MODULE_ID_PREFIX + " | INIT SLACK WEB CLIENT"));

//     const { WebClient } = require("@slack/client");

//     slackWebClient = new WebClient(slackRtmToken);

//     const conversationsListResponse = await slackWebClient.conversations.list({token: slackOAuthAccessToken});

//     conversationsListResponse.channels.forEach(async function(channel){

//       debug(chalkLog(MODULE_ID_PREFIX + " | SLACK CHANNEL | " + channel.id + " | " + channel.name));

//       if (channel.name === slackChannel) {
//         configuration.slackChannel = channel;

//         const message = {
//           token: slackOAuthAccessToken, 
//           channel: configuration.slackChannel.id,
//           text: "OP"
//         };

//         message.attachments = [];
//         message.attachments.push({
//           text: "INIT", 
//           fields: [ 
//             { title: "SRC", value: hostname + "_" + process.pid }, 
//             { title: "MOD", value: MODULE_NAME }, 
//             { title: "DST", value: "ALL" } 
//           ]
//         });

//         await slackWebClient.chat.postMessage(message);
//       }

//       channelsHashMap.set(channel.id, channel);

//     });

//     return;
//   }
//   catch(err){
//     console.log(chalkError(MODULE_ID_PREFIX + " | *** INIT SLACK WEB CLIENT ERROR: " + err));
//     throw err;
//   }
// }

// async function initSlackRtmClient(){

//   const { RTMClient } = require("@slack/client");
//   slackRtmClient = new RTMClient(slackRtmToken);

//   await slackRtmClient.start();

//   slackRtmClient.on("slack_event", async function(eventType, event){
//     switch (eventType) {
//       case "pong":
//         debug(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM PONG | " + getTimeStamp() + " | " + event.reply_to));
//       break;
//       default: debug(chalkInfo(MODULE_ID_PREFIX + " | SLACK RTM EVENT | " + getTimeStamp() + " | " + eventType + "\n" + jsonPrint(event)));
//     }
//   });


//   slackRtmClient.on("message", async function(message){
//     if (configuration.verbose) { console.log(chalkLog(MODULE_ID_PREFIX + " | RTM R<\n" + jsonPrint(message))); }
//     debug(` | SLACK RTM MESSAGE | R< | CH: ${message.channel} | USER: ${message.user} | ${message.text}`);

//     try {
//       await slackMessageHandler(message);
//     }
//     catch(err){
//       console.log(chalkError(MODULE_ID_PREFIX + " | *** SLACK RTM MESSAGE ERROR: " + err));
//     }

//   });

//   slackRtmClient.on("ready", async function(){
//     if (configuration.verbose) { await slackSendRtmMessage(hostname + " | " + MODULE_ID_PREFIX + " | SLACK RTM READY"); }
//     return;
//   });
// }

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

async function showStats(options){

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;
  statsObj.heap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
  statsObj.maxHeap = Math.max(statsObj.maxHeap, statsObj.heap);

  statsObjSmall = pick(statsObj, statsPickArray);

  const saveFileQueue = tcUtils.getSaveFileQueue();

  if (options) {
    console.log(MODULE_ID_PREFIX + " | STATS\nGTS | " + jsonPrint(statsObjSmall));
  }
  else {
    console.log(chalkLog(MODULE_ID_PREFIX
      + " | ============================================================"
      + "\n" + MODULE_ID_PREFIX
      + " | RUN " + msToTime(statsObj.elapsed)
      + " | NOW " + moment().format(compactDateTimeFormat)
      + " | STRT " + moment(parseInt(statsObj.startTime)).format(compactDateTimeFormat)
      + " | STATUS: " + statsObj.status
      + "\n" + MODULE_ID_PREFIX
      + " | SFQ: " + saveFileQueue
      + " | CPUs: " + statsObj.cpus
      + " | HEAP: " + statsObj.heap.toFixed(3) + " GB"
      + " | MAX HEAP: " + statsObj.maxHeap.toFixed(3) + " GB"
      + "\n" + MODULE_ID_PREFIX
      + " | ============================================================"
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

function quit(options){

  console.log(chalkAlert(MODULE_ID_PREFIX + " | QUITTING ..." ));

  clearInterval(endSaveFileQueueInterval);

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  if (options !== undefined) {

    if (options === "help") {
      process.exit();
    }
    // else {
    //   let slackText = "\n*" + statsObj.runId + "*";
    //   slackText = slackText + " | RUN " + msToTime(statsObj.elapsed);
    //   slackText = slackText + " | QUIT CAUSE: " + options;
    //   debug(MODULE_ID_PREFIX + " | SLACK TEXT: " + slackText);
    //   slackSendWebMessage({channel: slackChannel, text: slackText});
    // }
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

process.on( "SIGINT", function() {
  quit("SIGINT");
});

process.on("exit", function() {
  quit("SIGINT");
});

async function connectDb(){

  try {

    statsObj.status = "CONNECTING MONGO DB";

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | CONNECT MONGO DB ..."));

    const db = await global.wordAssoDb.connect(MODULE_ID_PREFIX + "_" + process.pid);

    db.on("error", async function(err){
      statsObj.status = "MONGO ERROR";
      statsObj.dbConnectionReady = false;
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR: " + err));
    });

    db.on("close", async function(){
      statsObj.status = "MONGO CLOSED";
      statsObj.dbConnectionReady = false;
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION CLOSED"));
    });

    db.on("disconnected", async function(){
      statsObj.status = "MONGO DISCONNECTED";
      statsObj.dbConnectionReady = false;
      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** MONGO DB DISCONNECTED | RECONNECTING..."));
    });

    console.log(chalk.green(MODULE_ID_PREFIX + " | MONGOOSE DEFAULT CONNECTION OPEN"));

    statsObj.dbConnectionReady = true;

    return db;
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECT ERROR: " + err));
    throw err;
  }
}

function initStdIn(){

  return new Promise(function(resolve){

    console.log(MODULE_ID_PREFIX + " | STDIN ENABLED");

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
          console.log(chalkRedBold(MODULE_ID_PREFIX + " | VERBOSE: " + configuration.verbose));
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

    if (loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG !== undefined) {
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_RESAVE_USER_DOCS_FLAG: " + loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG);
      if ((loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG === false) || (loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG === "false")) {
        newConfiguration.reSaveUserDocsFlag = false;
      }
      else if ((loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG === true) || (loadedConfigObj.GTS_RESAVE_USER_DOCS_FLAG === "true")) {
        newConfiguration.reSaveUserDocsFlag = true;
      }
      else {
        newConfiguration.reSaveUserDocsFlag = false;
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

    if (loadedConfigObj.GTS_BATCH_SIZE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_BATCH_SIZE: " + loadedConfigObj.GTS_BATCH_SIZE);
      newConfiguration.batchSize = loadedConfigObj.GTS_BATCH_SIZE;
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

    if (loadedConfigObj.GTS_CURSOR_PARALLEL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_CURSOR_PARALLEL: " + loadedConfigObj.GTS_CURSOR_PARALLEL);
      newConfiguration.cursorParallel = loadedConfigObj.GTS_CURSOR_PARALLEL;
    }

    if (loadedConfigObj.GTS_USERS_PER_ARCHIVE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_USERS_PER_ARCHIVE: " + loadedConfigObj.GTS_USERS_PER_ARCHIVE);
      newConfiguration.usersPerArchive = loadedConfigObj.GTS_USERS_PER_ARCHIVE;
    }

    if (loadedConfigObj.GTS_WAIT_VALUE_INTERVAL !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_WAIT_VALUE_INTERVAL: " + loadedConfigObj.GTS_WAIT_VALUE_INTERVAL);
      newConfiguration.waitValueInterval = loadedConfigObj.GTS_WAIT_VALUE_INTERVAL;
    }

    // if (loadedConfigObj.GTS_MAX_INPUT_HASHMAP_LIMIT !== undefined){
    //   console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_INPUT_HASHMAP_LIMIT: " + loadedConfigObj.GTS_MAX_INPUT_HASHMAP_LIMIT);
    //   newConfiguration.updateMaxInputHashMapLimit = loadedConfigObj.GTS_MAX_INPUT_HASHMAP_LIMIT;
    // }

    // if (loadedConfigObj.GTS_SPLIT_SIZE_MB !== undefined){
    //   console.log(MODULE_ID_PREFIX + " | LOADED GTS_SPLIT_SIZE_MB: " + loadedConfigObj.GTS_SPLIT_SIZE_MB);
    //   newConfiguration.splitSizeMB = loadedConfigObj.GTS_SPLIT_SIZE_MB;
    // }

    // if (loadedConfigObj.GTS_SPLIT_SIZE_KEYS !== undefined){
    //   console.log(MODULE_ID_PREFIX + " | LOADED GTS_SPLIT_SIZE_KEYS: " + loadedConfigObj.GTS_SPLIT_SIZE_KEYS);
    //   newConfiguration.splitSizeKeys = loadedConfigObj.GTS_SPLIT_SIZE_KEYS;
    // }

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

async function clampHistogram(params){

  const histogramTypes = Object.keys(params.histogram);
  const maxValue = params.maxValue || configuration.maxHistogramValue;

  const histogram = {};

  let modifiedFlag = false;

  for (const type of histogramTypes){

    // if (type === "friends"){
    //   continue;
    // }

    if (type !== "friends"){
      histogram[type] = {};

      const entities = Object.keys(params.histogram[type]);

      for (const entity of entities){

        if (entity.startsWith("[") || typeof entity !== "string"){

          modifiedFlag = true;

          console.log(chalkAlert(MODULE_ID_PREFIX + " | *** ENTITY ERROR"
            + " | TYPE: " + type
            + " | NID: " + params.nodeId
            + " | @" + params.screenName
            + " | ENTITY: " + entity
            + " | typeof ENTITY: " + typeof entity
            + " | params.histogram[type][entity]: " + params.histogram[type][entity]
          ));

          if (type === "media" && entity.startsWith("[object Object]")){
            delete params.histogram[type][entity];
          }

          if (type === "hashtag" && entity.startsWith("[#")){
            const newEntity = entity.slice(1);
            // if (histogram[type][newEntity] === undefined) { histogram[type][newEntity] = 1; }
            histogram[type][newEntity] = Math.min(maxValue, params.histogram[type][entity]) || 1;
            params.histogram[type][newEntity] = histogram[type][newEntity];
            delete params.histogram[type][entity];
          }
        }
        else{

          if (params.histogram[type][entity] > maxValue){

            modifiedFlag = true;

            console.log(chalkAlert(MODULE_ID_PREFIX + " | -*- HISTOGRAM VALUE CLAMPED: " + maxValue
              + " | NID: " + params.nodeId
              + " | @" + params.screenName
              + " | TYPE: " + type
              + " | ENTITY: " + entity
              + " | MAX VALUE: " + maxValue
              + " | VALUE: " + params.histogram[type][entity]
            ));

            histogram[type][entity] = maxValue;
          }
          else if (params.histogram[type][entity] <= 0){

            modifiedFlag = true;

            console.log(chalkAlert(MODULE_ID_PREFIX + " | -*- HISTOGRAM VALUE <= 0 | SET TO 1"
              + " | NID: " + params.nodeId
              + " | @" + params.screenName
              + " | TYPE: " + type
              + " | ENTITY: " + entity
              + " | VALUE: " + params.histogram[type][entity]
            ));

            histogram[type][entity] = 1;
          }
          else{
            histogram[type][entity] = params.histogram[type][entity];
          }

        }
      }
    }

  }

  return { histogram: histogram, modifiedFlag: modifiedFlag };
}

configEvents.once("INIT_MONGODB", function(){
  console.log(chalkLog(MODULE_ID_PREFIX + " | INIT_MONGODB"));
});

async function updateUserHistograms(params){

  const user = params.user;

  const dbUpdateParams = {};

  dbUpdateParams.profileHistograms = {};
  dbUpdateParams.tweetHistograms = {};

  const resultsProfileHistograms = await clampHistogram({
    nodeId: params.user.nodeId, 
    screenName: params.user.screenName, 
    histogram: params.user.profileHistograms
  });

  const resultsTweetHistograms = await clampHistogram({
    nodeId: params.user.nodeId, 
    screenName: params.user.screenName,
    histogram: params.user.tweetHistograms
  });

  if (params.updateUserInDb && (resultsProfileHistograms.modifiedFlag || resultsTweetHistograms.modifiedFlag)){

    const update = {};

    if (resultsProfileHistograms.modifiedFlag){ 
      update.profileHistograms = resultsProfileHistograms.histogram; 
    }

    if (resultsTweetHistograms.modifiedFlag){ 
      update.tweetHistograms = resultsTweetHistograms.histogram; 
    }

    const dbUpdatedUser = await global.wordAssoDb.User.findOneAndUpdate({ nodeId: params.user.nodeId }, update);

    console.log(chalkInfo(MODULE_ID_PREFIX + " | +++ UPDATED "
      + " | NID: " + dbUpdatedUser.nodeId
      + " | @" + dbUpdatedUser.screenName
      + " | PROFILE HISTOGRAM: " + resultsProfileHistograms.modifiedFlag
      + " | TWEET HISTOGRAM: " + resultsTweetHistograms.modifiedFlag
    ));

  }

  // const mergedHistograms = merge(user.profileHistograms, user.tweetHistograms);

  // await updateMaxInputHashMap({ histograms: mergedHistograms });

  return user;
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

    if (!userIn.tweetHistograms || (userIn.tweetHistograms == undefined)){
      userIn.tweetHistograms = {};
    }

    if (!userIn.profileHistograms || (userIn.profileHistograms == undefined)){
      userIn.profileHistograms = {};
    }

    if (!userIn.friends || (userIn.friends == undefined)){
      userIn.friends = [];
    }

    const u = await tcUtils.encodeHistogramUrls({user: userIn});

    const user = await updateUserHistograms({user: u, updateUserInDb: true});

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

// const categorizedNodeQueue = [];

let userIndex = 0;

async function categorizeUser(params){

  try{

    const user = await updateCategorizedUser({user: params.user});

    if (!user || user === undefined) {

      statsObj.users.processed.errors += 1;

      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USR NOT FOUND: "
        // + " [ CNIDQ: " + categorizedNodeQueue.length + "]"
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.users.processed.errors + " ]"
        + " | USER ID: " + params.nodeId
      ));

      throw new Error("USER NOT FOUND IN DB: " + params.nodeId);
    }

    const userPropertyPickArray = params.userPropertyPickArray || configuration.userPropertyPickArray;

    userIndex += 1;

    await tcUtils.updateGlobalHistograms({user: user, verbose: true});

    const subUser = pick(
      user,
      userPropertyPickArray
    );

    if (params.verbose) {
      console.log(chalkInfo(MODULE_ID_PREFIX + " | -<- UPDATE CATEGORIZED USR <DB"
        // + " [ CNIDQ: " + categorizedNodeQueue.length + "]"
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.users.processed.errors + "]"
        + " | " + user.nodeId
        + " | @" + user.screenName
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

// const defaultPeriod = 5;

async function cursorDataHandlerPromise(user){

  try {

    await cursorDataHandler(user);

    statsObj.users.processed.total += 1;
    statsObj.users.processed.elapsed = (moment().valueOf() - statsObj.users.processed.startMoment.valueOf()); // mseconds
    statsObj.users.processed.rate = (statsObj.users.processed.total >0) ? statsObj.users.processed.elapsed/statsObj.users.processed.total : 0; // msecs/usersArchived
    statsObj.users.processed.remain = statsObj.users.grandTotal - (statsObj.users.processed.total + statsObj.users.processed.errors);
    statsObj.users.processed.remainMS = statsObj.users.processed.remain * statsObj.users.processed.rate; // mseconds
    statsObj.users.processed.endMoment = moment();
    statsObj.users.processed.endMoment.add(statsObj.users.processed.remainMS, "ms");
    statsObj.users.processed.percent = 100 * (statsObj.users.notCategorized + statsObj.users.processed.total)/statsObj.users.grandTotal;

    statsObj.cursor[user.category].lastFetchedNodeId = user.nodeId;    

    const saveFileQueue = tcUtils.getSaveFileQueue();
    const queueOverShoot = saveFileQueue - configuration.maxSaveFileQueue;

    if (queueOverShoot > 0){

      const period = queueOverShoot * configuration.saveFileBackPressurePeriod;

      await wait({
        message: "BK PRSSR | SFQ: " + saveFileQueue, 
        period: period,
        verbose: configuration.verbose
      });
    }

    return;

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX
      + " | *** cursorDataHandlerPromise ERROR: " + err
    ));
  }

}

function isValidUser(user){
  if (!user || user === undefined || user === {} || typeof user !== "object") { return false; }
  if (!user.screenName || user.screenName === undefined) { return false; }
  if ((/[^\d]/).test(user.nodeId)) { return false; }
  return true;
}

async function cursorDataHandler(user){
  
  if (!isValidUser(user)){
    console.log(chalkWarn(MODULE_ID_PREFIX + " | !!! INVALID USER ... SKIPPING\n" + jsonPrint(user)));
    statsObj.users.processed.errors += 1;
    return;
  }
  
  if (empty(user.friends) && empty(user.profileHistograms) && empty(user.tweetHistograms)){

    statsObj.users.processed.empty += 1;

    if (statsObj.users.processed.empty % 100 === 0){
      console.log(chalkWarn(MODULE_ID_PREFIX 
        + " | --- EMPTY HISTOGRAMS"
        + " | SKIPPING"
        + " | PRCSD/REM/MT/ERR/TOT: " 
        + statsObj.users.processed.total 
        + "/" + statsObj.users.processed.remain 
        + "/" + statsObj.users.processed.empty 
        + "/" + statsObj.users.processed.errors
        + "/" + statsObj.users.grandTotal
        + " | @" + user.screenName 
      )); 
    }
    return;
  }

  if (!user.friends || user.friends == undefined) {
    user.friends = [];
  }
  else{
    user.friends = _.slice(user.friends, 0,5000);
  }

  const catUser = await categorizeUser({
    user: user, 
    verbose: configuration.verbose, 
    testMode: configuration.testMode
  });

  const hash = await tcUtils.hashUserId({nodeId: user.nodeId}); // 1000 buckets/subfolders by default
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
      + " | CATEGORIZED: " + statsObj.categorizedCount
      + " | L: " + categorizedUsers.left
      + " | N: " + categorizedUsers.neutral
      + " | R: " + categorizedUsers.right
    ));
  }

  return;
}

async function categoryCursorStream(params){

  statsObj.status = "categoryCursorStream";

  statsObj.categorizedCount = 0;

  const batchSize = params.batchSize || configuration.batchSize;
  const cursorParallel = params.cursorParallel || configuration.cursorParallel;
  const reSaveUserDocsFlag = params.reSaveUserDocsFlag || configuration.reSaveUserDocsFlag;

  let maxArchivedCount;

  if (reSaveUserDocsFlag) {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! RESAVE_USER_DOCS_FLAG: " + reSaveUserDocsFlag));
  }

  if (configuration.testMode) {
    maxArchivedCount = configuration.maxTestCount[params.category];
  }
  else{
    maxArchivedCount = statsObj.userCategoryTotal[params.category];
  }

  console.log(chalkGreen("\n" + MODULE_ID_PREFIX
    + " | =============================================================================================================="
  ));
  console.log(chalkGreen(MODULE_ID_PREFIX 
    + " | CATEGORIZE | CATEGORY: " + params.category + ": " + statsObj.userCategoryTotal[params.category] 
    + "\n" + MODULE_ID_PREFIX
    + " | TEST MODE: " + configuration.testMode
    + " | MAX COUNT: " + maxArchivedCount
    + " | BATCH SIZE: " + batchSize
    + " | MAX SFQ: " + configuration.maxSaveFileQueue
    + " | SAVE BACK PRESSURE PERIOD: " + configuration.saveFileBackPressurePeriod
    + " | SFQ PARALLEL: " + configuration.saveFileMaxParallel
    + " | TOTAL USERS: " + statsObj.users.grandTotal
  ));
  console.log(chalkGreen(MODULE_ID_PREFIX
    + " | ==============================================================================================================\n"
  ));

  const query = {};
  query.category = params.category;

  let cursor;

  const session = await mongooseDb.startSession();

  debug("MONGO DB SESSION\n" + session.id);

  console.log(chalkBlue(MODULE_ID_PREFIX
    + " | categoryCursorStream"
    + " | batchSize: " + batchSize
    + " | cursorParallel: " + cursorParallel
    + " | maxArchivedCount: " + maxArchivedCount
    + " | reSaveUserDocsFlag: " + reSaveUserDocsFlag
  ));

  if (configuration.testMode) {
    cursor = global.wordAssoDb.User
    .find(query, {timeout: false})
    // .sort({nodeId: 1})
    .lean()
    .batchSize(batchSize)
    .limit(maxArchivedCount)
    .session(session)
    .cursor()
    .addCursorFlag("noCursorTimeout", true);
  }
  else{
    cursor = global.wordAssoDb.User
    .find(query, {timeout: false})
    // .sort({nodeId: 1})
    .lean()
    .batchSize(batchSize)
    .session(session)
    .cursor()
    .addCursorFlag("noCursorTimeout", true);
  }

  cursor.on("end", function() {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | --- categoryCursorStream CURSOR END"));
  });

  cursor.on("error", function(err) {
    console.log(chalkError(MODULE_ID_PREFIX + " | *** ERROR categoryCursorStream: CURSOR ERROR: " + err));
    throw err;
  });

  cursor.on("close", function() {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | XXX categoryCursorStream CURSOR CLOSE"));
  });

  if (statsObj.cursor[params.category] === undefined) { statsObj.cursor[params.category] = {}; }

  if (statsObj.users.processed.startMoment === 0) { statsObj.users.processed.startMoment = moment(); }


  await cursor.eachAsync(async function(user){
    await cursorDataHandlerPromise(user);
  }, {parallel: cursorParallel});

  console.log(chalkBlueBold(MODULE_ID_PREFIX 
    + " | categoryCursorStream CURSOR COMPLETE"
    + " | CATEGORY: " + params.category
  ));

  return;
}

let endSaveFileQueueInterval;

function endSaveFileQueue(){

  return new Promise(function(resolve){

    statsObj.status = "endSaveFileQueue";

    let saveFileQueue = tcUtils.getSaveFileQueue();

    console.log(chalkLog(MODULE_ID_PREFIX
      + " | ... WAIT END SAVE FILE QUEUE"
      + " | SFQ: " + saveFileQueue
    ));

    endSaveFileQueueInterval = setInterval(function(){

      saveFileQueue = tcUtils.getSaveFileQueue();

      if (saveFileQueue === 0){
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

    if (params.message) {
      console.log(chalkLog(MODULE_ID_PREFIX 
        + " | " + params.message 
        + " | PERIOD: " + params.period + " MS"
      ));
    }


    const start = moment().valueOf();

    const w = setInterval(function(){

      saveFileQueue = tcUtils.getSaveFileQueue();

      if (saveFileQueue < configuration.maxSaveFileQueue){

        const deltaMS = (moment().valueOf() - start);

        clearInterval(w);

        if (params.verbose) {
          console.log(chalkLog(MODULE_ID_PREFIX 
            + " | XXX WAIT END BACK PRESSURE"
            + " | SFQ: " + saveFileQueue
            + " | PERIOD: " + params.period + " MS"
            + " | TOTAL WAIT: " + deltaMS + " MS"
          ));
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

  mongooseDb = await connectDb();

  return configuration;
}

async function generateGlobalTrainingTestSet(){

  statsObj.status = "generateGlobalTrainingTestSet";

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | GENERATE TRAINING SET | " + getTimeStamp()));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | USER DATA FOLDER: " + configuration.userDataFolder));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  statsObj.userCategoryTotal = {};

  for(const category of ["left", "neutral", "right"]){
    const query = { "category": category };
    statsObj.userCategoryTotal[category] = await global.wordAssoDb.User.find(query).countDocuments();
    statsObj.users.grandTotal += statsObj.userCategoryTotal[category];
  }

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | CATEGORIZED USERS IN DB: " + statsObj.users.grandTotal));
  console.log(chalkBlueBold(MODULE_ID_PREFIX 
    + " | L: " + statsObj.userCategoryTotal.left 
    + " | N: " + statsObj.userCategoryTotal.neutral
    + " | R: " + statsObj.userCategoryTotal.right
  ));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  if (configuration.testMode) {
    configuration.maxSaveFileQueue = 100;
    statsObj.users.grandTotal = Math.min(statsObj.users.grandTotal, configuration.totalMaxTestCount);
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** TEST MODE *** | CATEGORIZE MAX " + statsObj.users.grandTotal + " USERS"));
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** TEST MODE *** | MAX SAVE FILE QUEUE: " + configuration.maxSaveFileQueue));
  }

  let maxCategoryArchivedCount;

  for(const category of ["left", "neutral", "right"]){
    await categoryCursorStream({
      category: category, 
      // query: query, 
      reSaveUserDocsFlag: configuration.reSaveUserDocsFlag, 
      maxArchivedCount: maxCategoryArchivedCount
    });

  }

  return;
}

let showStatsInterval;

setTimeout(async function(){

  try{

    showStatsInterval = setInterval(function(){
      showStats();
    }, ONE_MINUTE);

    configuration = await initialize(configuration);

    if (configuration.testMode) {
      configuration.usersPerArchive = 100;
      console.log(chalkAlert(MODULE_ID_PREFIX + " | TEST MODE | USERS PER ARCHIVE: " + configuration.usersPerArchive));
    }

    if (configuration.saveGlobalHistogramsOnly){
      console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! SAVE GLOBAL HISTOGRAMS ONLY | NO REDIS FLUSH"));
    }
    else{
      const redisResult = await redisClient.flushall();
      console.log(chalkAlert(MODULE_ID_PREFIX + " | REDIS FLUSH ALL RESULT: " + redisResult));
    }

    tcUtils.setSaveFileMaxParallel(configuration.saveFileMaxParallel);
    tcUtils.enableSaveFileMaxParallel(true);
    await tcUtils.initSaveFileQueue({interval: configuration.saveFileQueueInterval});

    // initSlackRtmClient();
    // initSlackWebClient();

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

      console.log(chalkAlert("TFE | ... WAIT 30 SEC FOR TMP USER DATA FILES TO STABILIZE ..."));
      await delay({period: 30*ONE_SECOND});

      const saveFileQueue = tcUtils.getSaveFileQueue();

      console.log(chalkInfo("TFE | ... SAVING NORMALIZATION FILE"
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
      console.log(chalkLog("TFE | SAVE USER DATA ARCHIVES ..."));

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

    console.log(chalkInfo("TFE | ... SAVING HISTOGRAMS | " + rootFolder));

    // will use default input min hashmaps

    await tcUtils.saveGlobalHistograms({
      rootFolder: rootFolder, 
      pruneFlag: true, 
      inputTypeMinProfileHashMap: configuration.inputTypeMinProfileHashMap, // DEFAULT_MIN_TOTAL_MIN_PROFILE_TYPE_HASHMAP
      inputTypeMinTweetsHashMap: configuration.inputTypeMinTweetsHashMap, // DEFAULT_MIN_TOTAL_MIN_ _TYPE_HASHMAP
      verbose: true
    });

    if (configuration.testMode) {
      configuration.archiveFileUploadCompleteFlagFile = configuration.archiveFileUploadCompleteFlagFile.replace(/\.json/, "_test.json");
    }
    console.log(chalkInfo("TFE | ... SAVING FLAG FILE"
      + " | " + configuration.trainingSetsFolder + "/" + configuration.archiveFileUploadCompleteFlagFile
    ));

    tcUtils.saveFileQueue({
      folder: configuration.trainingSetsFolder,
      file: configuration.archiveFileUploadCompleteFlagFile,
      obj: fileSizeArrayObj,
      verbose: configuration.verbose
    });

    // let slackText = "\n*" + MODULE_ID_PREFIX + " | TRAINING SET*";
    // slackText = slackText + "\n" + configuration.userArchivePath;
    // slackText = slackText + "\nUSERS ARCHIVED: " + statsObj.users.grandTotal;
    // slackText = slackText + "\nLEFT: " + categorizedUserHistogram.left;
    // slackText = slackText + "\nRIGHT: " + categorizedUserHistogram.right;
    // slackText = slackText + "\nNEUTRAL: " + categorizedUserHistogram.neutral;
    // slackText = slackText + "\nPOSITIVE: " + categorizedUserHistogram.positive;
    // slackText = slackText + "\nNEGATIVE: " + categorizedUserHistogram.negative;
    // slackText = slackText + "\nNONE: " + categorizedUserHistogram.none;

    // await slackSendWebMessage({channel: slackChannel, text: slackText});

    await endSaveFileQueue();
    console.log(chalkAlert("TFE | ... WAIT 30 SEC FOR FILES TO STABILIZE ..."));
    await delay({period: 30*ONE_SECOND});

    clearInterval(showStatsInterval);

    await tcUtils.stopSaveFileQueue();

    await redisClient.quit();

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | XXX MAIN END XXX "));
    quit("OK");
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** MAIN ERROR: " + err));
    quit("MAIN ERROR: " + err);
  }
}, 1000);
