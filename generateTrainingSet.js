const MODULE_NAME = "generateTrainingSet";
const DEFAULT_RESAVE_USER_DOCS_FLAG = false;
const DEFAULT_MAX_HISTOGRAM_VALUE = 1000;
const DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM = 5;
const DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM = 2;
const DEFAULT_INPUT_TYPE_MIN_FRIENDS = 1000;
const DEFAULT_INPUT_TYPE_MIN_MEDIA = 3;
const DEFAULT_INPUT_TYPE_MIN_NGRAMS = 10;
const DEFAULT_INPUT_TYPE_MIN_PLACES = 2;
const DEFAULT_INPUT_TYPE_MIN_URLS = 2;
const TEST_MODE_LENGTH = 1000;

// const catUsersQuery = { 
//   "$and": [
//     { "screenName": { "$nin": [false, null] } }, 
//     { "ignored": { "$nin": [true, "true"] } }, 
//     { "category": { "$in": ["left", "right", "neutral"] } }
//   ]
// };

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
  "ngrams",
  "places",
  "sentiment",
  "urls",
  "userMentions",
  "words"
];

const defaultInputTypeMinHash = {
  emoji: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  friends: DEFAULT_INPUT_TYPE_MIN_FRIENDS,
  hashtags: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  images: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  locations: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  media: DEFAULT_INPUT_TYPE_MIN_MEDIA,
  ngrams: DEFAULT_INPUT_TYPE_MIN_NGRAMS,
  places: DEFAULT_INPUT_TYPE_MIN_PLACES,
  sentiment: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  urls: DEFAULT_INPUT_TYPE_MIN_URLS,
  userMentions: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM,
  words: DEFAULT_HISTOGRAM_TOTAL_MIN_ITEM
};

const testInputTypeMinHash = {
  emoji: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  friends: 20,
  hashtags: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  images: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  locations: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  media: 1,
  ngrams: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  places: 1,
  sentiment: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  urls: 1,
  userMentions: DEFAULT_HISTOGRAM_TEST_TOTAL_MIN_ITEM,
  words: 10
};

const ONE_SECOND = 1000;
const ONE_MINUTE = 60 * ONE_SECOND;
const ONE_HOUR = 60 * ONE_MINUTE;
const ONE_DAY = 24 * ONE_HOUR;

const ONE_KILOBYTE = 1024;
const ONE_MEGABYTE = 1024 * ONE_KILOBYTE;
const ONE_GIGABYTE = 1024 * ONE_MEGABYTE;

const compactDateTimeFormat = "YYYYMMDD_HHmmss";

const DEFAULT_SERVER_MODE = false;
const DEFAULT_WAIT_UNLOCK_INTERVAL = 15*ONE_SECOND;
const DEFAULT_WAIT_UNLOCK_TIMEOUT = 10*ONE_MINUTE;
const DEFAULT_FILELOCK_RETRY_WAIT = DEFAULT_WAIT_UNLOCK_INTERVAL;
const DEFAULT_FILELOCK_STALE = 2*DEFAULT_WAIT_UNLOCK_TIMEOUT;
const DEFAULT_FILELOCK_WAIT = DEFAULT_WAIT_UNLOCK_TIMEOUT;
const DEFAULT_QUIT_ON_COMPLETE = false;
const DEFAULT_TEST_RATIO = 0.20;
const MODULE_ID_PREFIX = "GTS";
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
const { promisify } = require("util");
// const renameFileAsync = promisify(fs.rename);
const unlinkFileAsync = promisify(fs.unlink);
const MergeHistograms = require("@threeceelabs/mergehistograms");
const mergeHistograms = new MergeHistograms();
const util = require("util");
const _ = require("lodash");
const sizeof = require("object-sizeof");
const pick = require("object.pick");
// const Slack = require("slack-node");
const EventEmitter3 = require("eventemitter3");
const debug = require("debug")("gts");
const commandLineArgs = require("command-line-args");
const empty = require("is-empty");

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

const DEFAULT_USER_PROPERTY_PICK_ARRAY = [
  "ageDays", 
  "category", 
  "categoryAuto", 
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
  "screenName", 
  "statusesCount",
  "threeceeFollowing",
  "tweetHistograms", 
  "tweetsPerDay", 
  "userId"
];

const maxInputHashMap = {};

DEFAULT_INPUT_TYPES.forEach(function(type){
  maxInputHashMap[type] = {};
});

const statsObj = {};

statsObj.heap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
statsObj.maxHeap = process.memoryUsage().heapUsed/ONE_GIGABYTE;

let statsObjSmall = {};

statsObj.status = "LOAD";
statsObj.hostname = hostname;
statsObj.pid = process.pid;
statsObj.cpus = os.cpus().length;
statsObj.commandLineArgsLoaded = false;
statsObj.usersAppendedToArchive = 0;
statsObj.usersProcessed = 0;
statsObj.userErrorCount = 0;
statsObj.userEmptyCount = 0;

statsObj.archiveOpen = false;
statsObj.archiveModifiedMoment = moment("2010-01-01");
statsObj.endAppendUsersFlag = false;

statsObj.initcategorizedNodeQueueFlag = false;

statsObj.archiveEntries = 0;
statsObj.archiveGrandTotal = 0;
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

process.on("unhandledRejection", function(err, promise) {
  console.trace("Unhandled rejection (promise: ", promise, ", reason: ", err, ").");
  process.exit();
});

let initMainInterval;

let configuration = {}; // merge of defaultConfiguration & hostConfiguration
configuration.reSaveUserDocsFlag = DEFAULT_RESAVE_USER_DOCS_FLAG;
configuration.verbose = false;
configuration.testMode = false; // per tweet test mode
configuration.testSetRatio = DEFAULT_TEST_RATIO;
configuration.maxTestCount = TEST_MODE_LENGTH;
configuration.maxHistogramValue = DEFAULT_MAX_HISTOGRAM_VALUE;
configuration.slackChannel = {};

let defaultConfiguration = {}; // general configuration for GTS
let hostConfiguration = {}; // host-specific configuration for GTS

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

global.wordAssoDb = require("@threeceelabs/mongoose-twitter");

const tcuChildName = MODULE_ID_PREFIX + "_TCU";
const ThreeceeUtilities = require("@threeceelabs/threecee-utilities");
const tcUtils = new ThreeceeUtilities(tcuChildName);

const UserServerController = require("@threeceelabs/user-server-controller");
const userServerController = new UserServerController(MODULE_ID_PREFIX + "_USC");

userServerController.on("error", function(err){
  console.log(chalkError(MODULE_ID_PREFIX + " | *** USC ERROR | " + err));
});

userServerController.on("ready", function(appname){
  console.log(chalk.green(MODULE_ID_PREFIX + " | USC READY | " + appname));
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

const slackChannel = "gts";
const HashMap = require("hashmap").HashMap;
const channelsHashMap = new HashMap();

const slackOAuthAccessToken = "xoxp-3708084981-3708084993-206468961315-ec62db5792cd55071a51c544acf0da55";
const slackConversationId = "D65CSAELX"; // wordbot
const slackRtmToken = "xoxb-209434353623-bNIoT4Dxu1vv8JZNgu7CDliy";

let slackRtmClient;
let slackWebClient;

async function slackSendRtmMessage(msg){
  console.log(chalkBlue(MODULE_ID_PREFIX + " | SLACK RTM | SEND: " + msg));

  const sendResponse = await slackRtmClient.sendMessage(msg, slackConversationId);

  console.log(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM | >T\n" + tcUtils.jsonPrint(sendResponse)));
  return sendResponse;
}

async function slackSendWebMessage(msgObj){
  const token = msgObj.token || slackOAuthAccessToken;
  const channel = msgObj.channel || configuration.slackChannel.id;
  const text = msgObj.text || msgObj;

  const message = {
    token: token, 
    channel: channel,
    text: text
  };

  if (msgObj.attachments !== undefined) {
    message.attachments = msgObj.attachments;
  }

  if (slackWebClient && slackWebClient !== undefined) {
    const sendResponse = await slackWebClient.chat.postMessage(message);
    return sendResponse;
  }
  else {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | SLACK WEB NOT CONFIGURED | SKIPPING SEND SLACK MESSAGE\n" + tcUtils.jsonPrint(message)));
    return;
  }
}

function slackMessageHandler(message){
  return new Promise(function(resolve, reject){

    try {

      console.log(chalkInfo(MODULE_ID_PREFIX + " | MESSAGE | " + message.type + " | " + message.text));

      if (message.type !== "message") {
        console.log(chalkAlert("Unhandled MESSAGE TYPE: " + message.type));
        return resolve();
      }

      const text = message.text.trim();
      const textArray = text.split("|");

      const sourceMessage = (textArray[2]) ? textArray[2].trim() : "NONE";

      switch (sourceMessage) {
        case "END FETCH ALL":
        case "ERROR":
        case "FETCH FRIENDS":
        case "FSM INIT":
        case "FSM FETCH_ALL":
        case "GEN AUTO CAT":
        case "INIT CHILD":
        case "INIT LANG ANALYZER":
        case "INIT MAX INPUT HASHMAP":
        case "INIT NNs":
        case "INIT RAN NNs":
        case "INIT RNT CHILD":
        case "INIT TWITTER USERS":
        case "INIT TWITTER":
        case "INIT UNFOLLOWABLE USER SET":
        case "INIT UNFOLLOWABLE":
        case "INIT":
        case "LOAD BEST NN":
        case "LOAD NN":
        case "MONGO DB CONNECTED":
        case "PONG":
        case "QUIT":
        case "QUITTING":
        case "READY":
        case "RESET":
        case "SAV NN HASHMAP":
        case "SLACK QUIT":
        case "SLACK READY":
        case "SLACK RTM READY":
        case "START":
        case "STATS":
        case "TEXT": 
        case "UPDATE HISTOGRAMS":
        case "UPDATE NN STATS":
        case "WAIT UPDATE STATS":
        case "END UPDATE STATS":
        case "UPDATE USER CAT STATS":
          resolve();
        break;
        case "STATSUS":
          console.log(chalkInfo(message.text));
          resolve();
        break;
        case "PING":
          console.log(chalkInfo("PING"));
          // slackSendWebMessage(hostname + " | " + MODULE_ID_PREFIX + " | PONG");
          resolve();
        break;
        case "NONE":
          resolve();
        break;
        default:
          console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UNDEFINED SLACK MESSAGE: " + message.text));
          // reject(new Error("UNDEFINED SLACK MESSAGE TYPE: " + message.text));
          resolve({text: "UNDEFINED SLACK MESSAGE", message: message});
      }
    }
    catch(err){
      reject(err);
    }

  });
}

async function initSlackWebClient(){
  try {

    const { WebClient } = require("@slack/client");
    slackWebClient = new WebClient(slackRtmToken);

    const conversationsListResponse = await slackWebClient.conversations.list({token: slackOAuthAccessToken});

    conversationsListResponse.channels.forEach(async function(channel){

      console.log(chalkLog(MODULE_ID_PREFIX + " | CHANNEL | " + channel.id + " | " + channel.name));

      if (channel.name === slackChannel) {
        configuration.slackChannel = channel;

        const message = {
          token: slackOAuthAccessToken, 
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
    console.log(chalkError(MODULE_ID_PREFIX + " | *** INIT SLACK WEB CLIENT ERROR: " + err));
    throw err;
  }
}

async function initSlackRtmClient(){

  const { RTMClient } = require("@slack/client");
  slackRtmClient = new RTMClient(slackRtmToken);

  await slackRtmClient.start();

  slackRtmClient.on("slack_event", async function(eventType, event){
    switch (eventType) {
      case "pong":
        debug(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM PONG | " + tcUtils.getTimeStamp() + " | " + event.reply_to));
      break;
      default: debug(chalkInfo(MODULE_ID_PREFIX + " | SLACK RTM EVENT | " + tcUtils.getTimeStamp() + " | " + eventType + "\n" + tcUtils.jsonPrint(event)));
    }
  });


  slackRtmClient.on("message", async function(message){
    if (configuration.verbose) { console.log(chalkLog(MODULE_ID_PREFIX + " | RTM R<\n" + tcUtils.jsonPrint(message))); }
    debug(` | SLACK RTM MESSAGE | R< | CH: ${message.channel} | USER: ${message.user} | ${message.text}`);

    try {
      await slackMessageHandler(message);
    }
    catch(err){
      console.log(chalkError(MODULE_ID_PREFIX + " | *** SLACK RTM MESSAGE ERROR: " + err));
    }

  });

  slackRtmClient.on("ready", async function(){
    if (configuration.verbose) { await slackSendRtmMessage(hostname + " | " + MODULE_ID_PREFIX + " | SLACK RTM READY"); }
    return;
  });
}


const maxHistogramValue = { name: "maxHistogramValue", alias: "M", type: Number };

const help = { name: "help", alias: "h", type: Boolean};

const enableStdin = { name: "enableStdin", alias: "S", type: Boolean, defaultValue: true };
const quitOnComplete = { name: "quitOnComplete", alias: "q", type: Boolean };
const quitOnError = { name: "quitOnError", alias: "Q", type: Boolean, defaultValue: true };
const verbose = { name: "verbose", alias: "v", type: Boolean };

const testMode = { name: "testMode", alias: "X", type: Boolean };

const optionDefinitions = [
  maxHistogramValue,
  enableStdin, 
  quitOnComplete, 
  quitOnError, 
  verbose, 
  testMode,
  help
];

const commandLineConfig = commandLineArgs(optionDefinitions);
console.log(chalkInfo(MODULE_ID_PREFIX + " | COMMAND LINE CONFIG\nGTS | " + tcUtils.jsonPrint(commandLineConfig)));
console.log(MODULE_ID_PREFIX + " | COMMAND LINE OPTIONS\nGTS | " + tcUtils.jsonPrint(commandLineConfig));

if (Object.keys(commandLineConfig).includes("help")) {
  console.log(MODULE_ID_PREFIX + " |optionDefinitions\n" + tcUtils.jsonPrint(optionDefinitions));
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
    console.log(MODULE_ID_PREFIX + " | R<\n" + tcUtils.jsonPrint(msg));
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

const dropboxConfigFolder = "/config/utility";
const dropboxConfigDefaultFolder = "/config/utility/default";
const dropboxConfigHostFolder = "/config/utility/" + hostname;

const dropboxConfigDefaultFile = "default_" + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE;
const dropboxConfigHostFile = hostname + "_" + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE;

const statsFolder = "/stats/" + hostname + "/generateTrainingSet";
const statsFile = "generateTrainingSetStats_" + statsObj.runId + ".json";

debug("statsFolder : " + statsFolder);
debug("statsFile : " + statsFile);

console.log(MODULE_ID_PREFIX + " | DROPBOX_GTS_CONFIG_FILE: " + configuration.DROPBOX.DROPBOX_GTS_CONFIG_FILE);
console.log(MODULE_ID_PREFIX + " | DROPBOX_GTS_STATS_FILE : " + configuration.DROPBOX.DROPBOX_GTS_STATS_FILE);

debug("dropboxConfigFolder : " + dropboxConfigFolder);
debug("dropboxConfigHostFolder : " + dropboxConfigHostFolder);
debug("dropboxConfigDefaultFile : " + dropboxConfigDefaultFile);
debug("dropboxConfigHostFile : " + dropboxConfigHostFile);

debug(MODULE_ID_PREFIX + " | DROPBOX_WORD_ASSO_ACCESS_TOKEN :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_ACCESS_TOKEN);
debug(MODULE_ID_PREFIX + " | DROPBOX_WORD_ASSO_APP_KEY :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_KEY);
debug(MODULE_ID_PREFIX + " | DROPBOX_WORD_ASSO_APP_SECRET :" + configuration.DROPBOX.DROPBOX_WORD_ASSO_APP_SECRET);


const globalCategorizedUsersFolder = dropboxConfigDefaultFolder + "/categorizedUsers";
const categorizedUsersFile = "categorizedUsers_manual.json";

function showStats(options){

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;
  statsObj.heap = process.memoryUsage().heapUsed/ONE_GIGABYTE;
  statsObj.maxHeap = Math.max(statsObj.maxHeap, statsObj.heap);

  statsObjSmall = pick(statsObj, statsPickArray);

  if (options) {
    console.log(MODULE_ID_PREFIX + " | STATS\nGTS | " + tcUtils.jsonPrint(statsObjSmall));
  }
  else {
    console.log(chalkLog(MODULE_ID_PREFIX + " | ============================================================"
      + "\nGTS | S"
      + " | STATUS: " + statsObj.status
      + " | CPUs: " + statsObj.cpus
      + " | " + testObj.testRunId
      + " | HEAP: " + statsObj.heap.toFixed(3) + " GB"
      + " | MAX HEAP: " + statsObj.maxHeap.toFixed(3) + " GB"
      + " | RUN " + tcUtils.msToTime(statsObj.elapsed)
      + " | NOW " + moment().format(compactDateTimeFormat)
      + " | STRT " + moment(parseInt(statsObj.startTime)).format(compactDateTimeFormat)
      + "\nGTS | ============================================================"
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

    console.log(chalkInfo(MODULE_ID_PREFIX + " | ============================================================"
      + "\nGTS | >+- ARCHIVE | PROGRESS"
      + " | " + tcUtils.getTimeStamp()
      + " | APPNDD: " + statsObj.usersAppendedToArchive
      + " | ENTRIES ARCVD/REM/TOT: " + statsObj.usersAppendedToArchive + "/" + statsObj.archiveRemainUsers + "/" + statsObj.archiveGrandTotal
      + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
      + " (" + (100*statsObj.usersAppendedToArchive/statsObj.archiveGrandTotal).toFixed(2) + "%)"
      + " [ RATE: " + (statsObj.archiveRate/1000).toFixed(3) + " SEC/USER ]"
      + " S: " + tcUtils.getTimeStamp(statsObj.archiveStartMoment)
      + " E: " + tcUtils.msToTime(statsObj.archiveElapsed)
      + " | ETC: " + tcUtils.msToTime(statsObj.archiveRemainMS) + " " + statsObj.archiveEndMoment.format(compactDateTimeFormat)
      + "\nGTS | ============================================================"
    ));
  }
}

function quit(options){

  console.log(chalkAlert(MODULE_ID_PREFIX + " | QUITTING ..." ));

  clearInterval(initMainInterval);

  statsObj.elapsed = moment().valueOf() - statsObj.startTime;

  if (options !== undefined) {

    if (options === "help") {
      process.exit();
    }
    else {
      let slackText = "\n*" + statsObj.runId + "*";
      slackText = slackText + " | RUN " + tcUtils.msToTime(statsObj.elapsed);
      slackText = slackText + " | QUIT CAUSE: " + options;
      debug(MODULE_ID_PREFIX + " | SLACK TEXT: " + slackText);
      slackSendWebMessage(slackChannel, slackText);
    }
  }

  showStats();

  setTimeout(function(){
    console.log(chalkBlueBold(
               MODULE_ID_PREFIX + " | =================================="
      + "\n" + MODULE_ID_PREFIX + " | *** QUIT GENERATE TRAINING SET ***"
      + "\n" + MODULE_ID_PREFIX + " | =================================="
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
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR"));
      db.close();
      quit({cause: "MONGO DB ERROR: " + err});
    });

    db.on("close", async function(err){
      statsObj.status = "MONGO CLOSED";
      statsObj.dbConnectionReady = false;
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION CLOSED"));
      quit({cause: "MONGO DB CLOSED: " + err});
    });

    db.on("disconnected", async function(){
      statsObj.status = "MONGO DISCONNECTED";
      statsObj.dbConnectionReady = false;
      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** MONGO DB DISCONNECTED"));
      quit({cause: "MONGO DB DISCONNECTED"});
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

    console.log(chalkInfo(MODULE_ID_PREFIX + " | LOADED CONFIG FILE: " + params.file + "\n" + tcUtils.jsonPrint(loadedConfigObj)));

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

    if (loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE !== undefined){
      console.log(MODULE_ID_PREFIX + " | LOADED GTS_MAX_HISTOGRAM_VALUE: " + loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE);
      newConfiguration.maxHistogramValue = loadedConfigObj.GTS_MAX_HISTOGRAM_VALUE;
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

async function clampHistogram(params){

  const histogramTypes = Object.keys(params.histogram);
  const maxValue = params.maxValue || configuration.maxHistogramValue;

  const histogram = {};

  for (const type of histogramTypes){

    histogram[type] = {};

    const entities = Object.keys(params.histogram[type]);

    for (const entity of entities){

      if (params.histogram[type][entity] > configuration.maxHistogramValue){

        console.log(chalkAlert(MODULE_ID_PREFIX + " | -*- HISTOGRAM VALUE CLAMPED: " + maxValue
          + " | @" + params.screenName
          + " | TYPE: " + type
          + " | ENTITY: " + entity
          + " | VALUE: " + params.histogram[type][entity]
        ));

      }

      histogram[type][entity] = Math.min(maxValue, params.histogram[type][entity]) || 1;
    }
  }

  return histogram;
}

configEvents.once("INIT_MONGODB", function(){
  console.log(chalkLog(MODULE_ID_PREFIX + " | INIT_MONGODB"));
});

async function updateUserAndMaxInputHashMap(params){

  const user = params.user;

  const dbUpdateParams = {};

  dbUpdateParams.profileHistograms = {};
  dbUpdateParams.tweetHistograms = {};

  user.profileHistograms = await clampHistogram({screenName: params.user.screenName, histogram: params.user.profileHistograms});
  user.tweetHistograms = await clampHistogram({screenName: params.user.screenName, histogram: params.user.tweetHistograms});

  const dbUpdatedUser = await userServerController.findOneUserV2({user: user});

  const mergedHistograms = await mergeHistograms.merge({ histogramA: user.profileHistograms, histogramB: user.tweetHistograms });

  const histogramTypes = Object.keys(mergedHistograms);

  for (const type of histogramTypes){
    if (type !== "sentiment") {

      if (!maxInputHashMap[type] || maxInputHashMap[type] === undefined) { maxInputHashMap[type] = {}; }

      const histogramTypeEntities = Object.keys(mergedHistograms[type]);

      if (histogramTypeEntities.length > 0) {

        for (const entity of histogramTypeEntities){

          if (mergedHistograms[type][entity] !== undefined){

            if (maxInputHashMap[type][entity] === undefined){
              maxInputHashMap[type][entity] = Math.max(1, mergedHistograms[type][entity]);
            }
            else{
              maxInputHashMap[type][entity] = Math.max(maxInputHashMap[type][entity], mergedHistograms[type][entity]);
            }
          }
          else{
            console.log(chalkAlert(MODULE_ID_PREFIX + " | ??? UNDEFINED mergedHistograms[type][entity]"
              + " | TYPE: " + type
              + " | ENTITY: " + entity
            ));
            delete mergedHistograms[type][entity];
          }

        }

      }
    }
  }

  return dbUpdatedUser;
}

async function updateCategorizedUser(params){

  if (!params.user || params.user === undefined) {
    console.error(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USERS: USER UNDEFINED"));
    statsObj.errors.users.findOne += 1;
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
    else if (userIn.friends.length > 5000){
      userIn.friends = _.slice(userIn.friends, 0,5000);
    }

    const u = await tcUtils.encodeHistogramUrls({user: userIn});

    const user = await updateUserAndMaxInputHashMap({user: u});

    if (user.profileHistograms.sentiment && (user.profileHistograms.sentiment !== undefined)) {

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

    categorizedUsersPercent = 100 * (statsObj.users.notCategorized + statsObj.users.updatedCategorized)/statsObj.archiveGrandTotal;

    if (configuration.verbose 
      // || configuration.testMode 
      || ((statsObj.users.notCategorized + statsObj.users.updatedCategorized) % 1000 === 0)){

      categorizedUserHistogramTotal();

      console.log(chalkLog(MODULE_ID_PREFIX + " | CATEGORIZED"
        + " | " + (statsObj.users.notCategorized + statsObj.users.updatedCategorized) + "/" + statsObj.archiveGrandTotal
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
    console.error(chalkError(MODULE_ID_PREFIX + " | *** UPDATE CATEGORIZED USER ERROR: " + err));
    statsObj.errors.users.findOne += 1;
    throw err;
  }
}

const categorizedNodeQueue = [];
const archiveUserQueue = [];
let archiveUserQueueReady = true;
let archiveUserQueueInterval;

function initArchiveUserQueue(params){

  return new Promise(function(resolve){

    const interval = params.interval;

    clearInterval(archiveUserQueueInterval);

    archiveUserQueueInterval = setInterval(function(){

      if (archiveUserQueueReady && (archiveUserQueue.length > 0)) {

        archiveUserQueueReady = false;

        const user = archiveUserQueue.shift();

        archiveUser({user: user})
        .then(function(){

          debug(chalkAlert(MODULE_ID_PREFIX + " | +++ ARCHIVED USER"
            + " [ AUQ: " + archiveUserQueue.length + "]"
            + " | USER ID: " + user.nodeId
            + " | @" + user.screenName
          ));

          archiveUserQueueReady = true;
        })
        .catch(function(err){
          console.log(chalkError(MODULE_ID_PREFIX + " | *** archiveUser ERROR: " + err));
          archiveUserQueueReady = true;
        });
      }

    }, interval);

    resolve();

  });
}


let userIndex = 0;

async function categorizeUser(params){

  try{
    const user = await updateCategorizedUser({user: params.user});

    if (!user || user === undefined) {

      statsObj.userErrorCount += 1;

      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UPDATE CL USR NOT FOUND: "
        + " [ CNIDQ: " + categorizedNodeQueue.length + "]"
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.userErrorCount + " ]"
        + " | USER ID: " + params.nodeId
      ));

      throw new Error("USER NOT FOUND IN DB: " + params.nodeId);
    }

    const userPropertyPickArray = params.userPropertyPickArray || configuration.userPropertyPickArray;

    userIndex += 1;

    await tcUtils.updateGlobalHistograms({user: user});

    const subUser = pick(
      user,
      userPropertyPickArray
    );

    subUser.friends = _.slice(subUser.friends, 0,5000);

    if (params.verbose) {
      console.log(chalkInfo(MODULE_ID_PREFIX + " | -<- UPDATE CL USR <DB"
        + " [ CNIDQ: " + categorizedNodeQueue.length + "]"
        + " [ USERS: " + userIndex + " / ERRORS: " + statsObj.userErrorCount + "]"
        + " | " + user.nodeId
        + " | @" + user.screenName
      ));
    }

    if (statsObj.archiveStartMoment === 0) { statsObj.archiveStartMoment = moment(); }

    return subUser;

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX
      + " | *** UPDATE CATEGORIZED USER ERROR | USER ID: " + params.nodeId 
      + " | ERROR: " + err
    ));
    throw err;
  }
}

const archivedUsers = {};
archivedUsers.left = 0;
archivedUsers.neutral = 0;
archivedUsers.right = 0;

function categoryCursorStream(params){

  return new Promise(function(resolve){

    const catCursor = global.wordAssoDb.User.find(params.query).lean().cursor();
    let ready = true;
    let archivedCount = 0;
    const maxArchivedCount = (params.maxArchivedCount) ? params.maxArchivedCount : configuration.maxTestCount;

    const reSaveUserDocsFlag = params.reSaveUserDocsFlag || false;

    if (reSaveUserDocsFlag) {
      console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! RESAVE_USER_DOCS_FLAG: " + reSaveUserDocsFlag));
    }

    const catCursorInterval = setInterval(async function(){

      if (configuration.testMode && (archivedCount > maxArchivedCount)) {
        catCursor.close();
        console.log(chalkInfo(MODULE_ID_PREFIX
          + " | ARCHIVED: " + archivedCount
          + " | archivedUsers\n" + tcUtils.jsonPrint(archivedUsers)
        ));
        clearInterval(catCursorInterval);
        return resolve({archivedCount: archivedCount});
      }

      if (ready && (archiveUserQueue.length < 100)){

        ready = false;

        let user = await catCursor.next();

        if (reSaveUserDocsFlag) {
          try{
            await global.wordAssoDb.User.deleteOne({nodeId: user.nodeId});
            delete user._id;
            const newUserDoc = new global.wordAssoDb.User(user);
            await newUserDoc.save();
            user = await global.wordAssoDb.User.findOne({nodeId: newUserDoc.nodeId});
          }
          catch(e){
            console.log(chalkError(MODULE_ID_PREFIX + " | *** RESAVE USER DOC ERROR"
              + " | " + user.nodeId
              + " | @" + user.screenName
            ));
          }
        }

        if (!user || (user === undefined)) {
          catCursor.close();
          clearInterval(catCursorInterval);
          return resolve({archivedCount: archivedCount});
        }

        if (!user.screenName){
          console.log(chalkWarn(MODULE_ID_PREFIX + " | !!! USER SCREENNAME UNDEFINED\n" + tcUtils.jsonPrint(user)));
          statsObj.userErrorCount += 1;
          ready = true;
        }
        else if (empty(user.friends) && empty(user.profileHistograms) && empty(user.tweetHistograms)){
          statsObj.userEmptyCount += 1;
          console.log(chalkWarn(MODULE_ID_PREFIX 
            + " | --- EMPTY HISTOGRAMS"
            + " | SKIPPING"
            + " | PRCSD/REM/MT/ERR/TOT: " 
            + statsObj.usersAppendedToArchive 
            + "/" + statsObj.archiveRemainUsers 
            + "/" + statsObj.userEmptyCount 
            + "/" + statsObj.userErrorCount 
            + "/" + statsObj.archiveGrandTotal
            + " | @" + user.screenName 
          ));
          ready = true;
        }
        else {

          if (!user.friends || user.friends == undefined) {
            user.friends = [];
          }
          else{
            user.friends = _.slice(user.friends, 0,5000);
          }

          const catUser = await categorizeUser({user: user, verbose: configuration.verbose, testMode: configuration.testMode});

          if (!configuration.testMode){
            archiveUserQueue.push(catUser);
            archivedUsers[catUser.category] += 1;
            archivedCount += 1;
          }
          else if (configuration.testMode && (archivedUsers[user.category] < 0.34*configuration.maxTestCount)){
            archiveUserQueue.push(catUser);
            archivedUsers[catUser.category] += 1;
            archivedCount += 1;
          }

          ready = true;

          if (archivedCount % 100 === 0){
            console.log(chalkInfo(MODULE_ID_PREFIX
              + " | ARCHIVED: " + archivedCount
              + " | L: " + archivedUsers.left
              + " | N: " + archivedUsers.neutral
              + " | R: " + archivedUsers.right
              // + " | archivedUsers\n" + tcUtils.jsonPrint(archivedUsers)
            ));
          }
        }
      }

    }, 2);

  });
}

function archiveUser(params){

  return new Promise(function(resolve, reject){

    if (archive === undefined) { 
      return reject(new Error("ARCHIVE UNDEFINED"));
    }

    const fileName = "user_" + params.user.userId + ".json";

    const userBuffer = Buffer.from(JSON.stringify(params.user));

    archive.append(userBuffer, { name: fileName});

    tcUtils.waitEvent({ event: "append_" + fileName})
    .then(function(){

      statsObj.usersAppendedToArchive += 1;

      if (configuration.verbose) {
        console.log(chalkLog(MODULE_ID_PREFIX + " | >-- ARCHIVE | USER"
          + " [" + statsObj.usersAppendedToArchive + " APPENDED]"
          + " | @" + params.user.screenName
        ));
      }

      resolve();

    });

  });
}

let endArchiveUsersInterval;

function endAppendUsers(){
  return new Promise(function(resolve){

    endArchiveUsersInterval = setInterval(function(){

      if ((statsObj.usersAppendedToArchive > 0) && (statsObj.archiveRemainUsers <= 0)){
        console.log(chalkGreen(MODULE_ID_PREFIX + " | XXX END APPEND"
          + " | " + statsObj.archiveGrandTotal + " USERS"
          + " | " + statsObj.usersAppendedToArchive + " APPENDED"
          + " | " + statsObj.usersAppendedToArchive + " PROCESSED"
          + " | " + statsObj.userEmptyCount + " EMPTY SKIPPED"
          + " | " + statsObj.userErrorCount + " ERRORS"
          + " | " + statsObj.archiveRemainUsers + " REMAIN"
        ));
        clearInterval(endArchiveUsersInterval);
        return resolve();
      }

    }, 10*ONE_SECOND);

  });
}

function getFileLock(params){

  return new Promise(function(resolve, reject){

    try {

      console.log(chalkBlue(MODULE_ID_PREFIX + " | ... GET LOCK FILE | " + params.file));

      lockFile.lock(params.file, params.options, function(err){

        if (err) {
          console.log(chalkError(MODULE_ID_PREFIX + " | *** FILE LOCK FAIL: " + params.file + "\n" + err));
          // return reject(err);
          return resolve(false);
        }

        console.log(chalkGreen(MODULE_ID_PREFIX + " | +++ FILE LOCK: " + params.file));
        resolve(true);
      });

    }
    catch(err){
      console.log(chalkError(MODULE_ID_PREFIX + " | *** GET FILE LOCK ERROR: " + err));
      return reject(err);
    }

  });
}

function delay(params){
  return new Promise(function(resolve){

    if (params.message) {
      console.log(chalkLog(MODULE_ID_PREFIX + " | " + params.message + " | PERIOD: " + tcUtils.msToTime(params.period)));
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
      console.log(chalkError(MODULE_ID_PREFIX + " | *** FILE UNLOCK FAIL: " + params.file + "\n" + err));
      throw err;
    }

    console.log(chalkLog(MODULE_ID_PREFIX + " | --- FILE UNLOCK: " + params.file));
    return true;

  });
}

async function deleteOldArchives(p){

  const params = p || {};

  const maxAgeMs = params.maxAgeMs || ONE_DAY;
  const folder = params.folder || configuration.userArchiveFolder;

  const userArchiveEntryArray = await tcUtils.filesListFolder({folder: folder});

  for(const entry of userArchiveEntryArray.entries){
    // console.log(chalkLog(MODULE_ID_PREFIX + " | ENTRY: " + entry.name));

    if (!entry.name.startsWith(hostname + "_")) {
      console.log(chalkLog(MODULE_ID_PREFIX + " | ... SKIPPING DELETE OF " + entry.name));
    }
    else if (!entry.name.endsWith("users.zip")) {
      console.log(chalkInfo(MODULE_ID_PREFIX + " | ... SKIPPING DELETE OF " + entry.name));
    }
    else{

      // configuration.userArchiveFile = hostname + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat) + "_users.zip";

      const namePartsArray = entry.name.split("_");

      const entryDate = namePartsArray[1] + "_" + namePartsArray[2];

      // console.log(chalkInfo(MODULE_ID_PREFIX + " | ENTRY DATE: " + entryDate));

      const entryMoment = new moment(entryDate, "YYYYMMDD_HHmmss");

      const entryAge = moment.duration(statsObj.startTimeMoment.diff(entryMoment));

      if (entryAge > maxAgeMs) {
        console.log(chalkAlert(MODULE_ID_PREFIX
          + " | DELETE ENTRY: " + entry.path_display
          + " | MAX AGE: " + tcUtils.msToTime(maxAgeMs)
          + " | ENTRY AGE: " + tcUtils.msToTime(entryAge)
        ));

        await unlinkFileAsync(entry.path_display);

      }
      else{
        console.log(chalkInfo(MODULE_ID_PREFIX
          + " | SKIP DEL ENTRY: " + entry.name
          + " | MAX AGE: " + tcUtils.msToTime(maxAgeMs)
          + " | ENTRY AGE: " + tcUtils.msToTime(entryAge)
        ));
      }
      
    }
  }

  return;

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

    console.log(chalkLog(MODULE_ID_PREFIX + " | ... SAVING FLAG FILE" 
      + " | " + configuration.userArchiveFolder + "/" + configuration.archiveFileUploadCompleteFlagFile 
      + " | " + fileSizeInBytes + " B | " + savedSize.toFixed(3) + " MB"
    ));


    const fileSizeObj = {};
    fileSizeObj.file = configuration.userArchiveFile;
    fileSizeObj.size = fileSizeInBytes;
    fileSizeObj.histogram = {};
    fileSizeObj.histogram = categorizedUserHistogram;

    await tcUtils.saveFile({folder: configuration.userArchiveFolder, file: configuration.archiveFileUploadCompleteFlagFile, obj: fileSizeObj });

    await delay({message: "... WAIT FOR DROPBOX FLAG FILE SYNC | " + tcUtils.getTimeStamp(), period: ONE_MINUTE});

    await deleteOldArchives();

    quit("DONE");
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** ARCHIVE_OUTPUT_CLOSED ERROR", err));
    quit();
  }

});

async function initArchiver(){

  let userArchivePath = configuration.userArchivePath;

  if (configuration.testMode) {
    userArchivePath = configuration.userArchivePath.replace(/\.zip/, "_test.zip");
  }

  console.log(chalkBlue(MODULE_ID_PREFIX + " | ... INIT ARCHIVER | " + userArchivePath));

  if (archive && archive.isOpen) {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | ARCHIVE ALREADY OPEN | " + userArchivePath));
    return;
  }

  const lockFileName = userArchivePath + ".lock";

  const archiveFileLocked = await getFileLock({file: lockFileName, options: fileLockOptions});

  if (!archiveFileLocked) {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** FILE LOCK FAILED | SKIP INIT ARCHIVE: " + userArchivePath));
    statsObj.archiveOpen = false;
    return;
  }

  const output = fs.createWriteStream(userArchivePath);

  archive = archiver("zip", {
    zlib: { level: 9 } // Sets the compression level.
  });
   
  output.on("close", function() {
    const archiveSize = toMegabytes(archive.pointer());
    console.log(chalkGreen(MODULE_ID_PREFIX + " | XXX ARCHIVE OUTPUT | CLOSED | " + archiveSize.toFixed(2) + " MB"));
    configEvents.emit("ARCHIVE_OUTPUT_CLOSED", userArchivePath);
  });
   
  output.on("end", function() {
    const archiveSize = toMegabytes(archive.pointer());
    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | XXX ARCHIVE OUTPUT | END | " + archiveSize.toFixed(2) + " MB"));
  });
   
  archive.on("warning", function(err) {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | !!! ARCHIVE | WARNING\n" + tcUtils.jsonPrint(err)));
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
    statsObj.archiveRate = statsObj.archiveElapsed/statsObj.usersAppendedToArchive; // msecs/usersArchived
    statsObj.archiveRemainUsers = statsObj.archiveGrandTotal - (statsObj.usersProcessed + statsObj.userErrorCount);
    statsObj.archiveRemainMS = statsObj.archiveRemainUsers * statsObj.archiveRate; // mseconds
    statsObj.archiveEndMoment = moment();
    statsObj.archiveEndMoment.add(statsObj.archiveRemainMS, "ms");

    if ((statsObj.usersAppendedToArchive % 100 === 0) || configuration.verbose) {
      console.log(chalkInfo(MODULE_ID_PREFIX + " | >+- ARCHIVE | PROGRESS"
        + " | " + tcUtils.getTimeStamp()
        + " | APPNDD: " + statsObj.usersAppendedToArchive
        + " | ENTRIES ARCVD/REM/TOT: " + statsObj.usersAppendedToArchive + "/" + statsObj.archiveRemainUsers + "/" + statsObj.archiveGrandTotal
        + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
        + " (" + (100*statsObj.usersAppendedToArchive/statsObj.archiveGrandTotal).toFixed(2) + "%)"
        + " [ RATE: " + (statsObj.archiveRate/1000).toFixed(3) + " SEC/USER ]"
        + " S: " + tcUtils.getTimeStamp(statsObj.archiveStartMoment)
        + " E: " + tcUtils.msToTime(statsObj.archiveElapsed)
        + " | ETC: " + tcUtils.msToTime(statsObj.archiveRemainMS) + " " + statsObj.archiveEndMoment.format(compactDateTimeFormat)
      ));
    }
  });
   
  archive.on("entry", function(entryData) {

    statsObj.archiveEntries += 1;

    if (configuration.verbose || (statsObj.archiveEntries % 100 === 0)) {
      console.log(chalkLog(MODULE_ID_PREFIX + " | >-- ARCHIVE | ENTRY"
        + " [ " + statsObj.usersAppendedToArchive + " APPENDED ]"
        + " [ " + statsObj.archiveEntries + " ENTRIES ]"
        + " | " + entryData.name
      ));
    }

    tcUtils.emitter.emit("append_" + entryData.name);
  });
   
  archive.on("close", function() {
    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | XXX ARCHIVE | CLOSED | " + userArchivePath));
  });
   
  archive.on("finish", function() {

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | +++ ARCHIVE | FINISHED | " + tcUtils.getTimeStamp()
      + "\nGTS | +++ ARCHIVE | FINISHED | TEST MODE: " + configuration.testMode
      + "\nGTS | +++ ARCHIVE | FINISHED | ARCHIVE:   " + userArchivePath
      + "\nGTS | +++ ARCHIVE | FINISHED | ENTRIES:   " + statsObj.usersAppendedToArchive + "/" + statsObj.archiveGrandTotal + " APPENDED"
      + " (" + (100*statsObj.usersAppendedToArchive/statsObj.archiveGrandTotal).toFixed(2) + "%)"
      + " | " + statsObj.totalMbytes.toFixed(2) + " MB"
    ));
  });
   
  archive.on("error", function(err) {
    console.log(chalkError(MODULE_ID_PREFIX + " | *** ARCHIVE | ERROR\n" + tcUtils.jsonPrint(err)));
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
    console.log(MODULE_ID_PREFIX + " | ENV GTS_QUIT_ON_COMPLETE: " + process.env.GTS_QUIT_ON_COMPLETE);
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
      console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + "\n" + tcUtils.jsonPrint(configuration[arg]));
    }
    else {
      console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + ": " + configuration[arg]);
    }
  });
  
  statsObj.commandLineArgsLoaded = true;

  await connectDb();

  return configuration;
}

async function generateGlobalTrainingTestSet(){

  statsObj.status = "GENERATE TRAINING SET";

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | GENERATE TRAINING SET | " + tcUtils.getTimeStamp()));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  statsObj.archiveCategoryTotal = {};

  for(const category of ["left", "neutral", "right"]){
    const query = { 
      "$and": [
        { "screenName": { "$nin": [false, null] } }, 
        { "ignored": { "$nin": [true, "true"] } }, 
        { "category": category }
      ]
    };
    statsObj.archiveCategoryTotal[category] = await global.wordAssoDb.User.find(query).countDocuments();
    statsObj.archiveGrandTotal += statsObj.archiveCategoryTotal[category];
  }

  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | CATEGORIZED USERS IN DB: " + statsObj.archiveGrandTotal));
  console.log(chalkBlueBold(MODULE_ID_PREFIX + " | ==================================================================="));

  if (configuration.testMode) {
    statsObj.archiveGrandTotal = Math.min(statsObj.archiveGrandTotal, configuration.maxTestCount);
    console.log(chalkAlert(MODULE_ID_PREFIX + " | *** TEST MODE *** | CATEGORIZE MAX " + statsObj.archiveGrandTotal + " USERS"));
  }

  await initArchiveUserQueue({interval: 2});
  await initArchiver();

  let maxCategoryArchivedCount;

  if (configuration.testMode) {
    maxCategoryArchivedCount = parseInt(configuration.maxTestCount/3);
  }

  for(const category of ["left", "neutral", "right"]){

    console.log(chalkLog(MODULE_ID_PREFIX + " | ==================================================================="));
    console.log(chalkLog(MODULE_ID_PREFIX + " | CATEGORIZE USERS | CATEGORY: " + category + ": " + statsObj.archiveCategoryTotal[category] + " | MAX ARCHIVED COUNT: " + maxCategoryArchivedCount));
    console.log(chalkLog(MODULE_ID_PREFIX + " | ==================================================================="));

    const query = { 
      "$and": [
        { "screenName": { "$nin": [false, null] } }, 
        { "ignored": { "$nin": [true, "true"] } }, 
        { "category": category }
      ]
    };

    await categoryCursorStream({query: query, reSaveUserDocsFlag: configuration.reSaveUserDocsFlag, maxArchivedCount: maxCategoryArchivedCount});
  }

  await endAppendUsers();

  // inc total to account for future append
  statsObj.archiveGrandTotal += 1;

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

    await initSlackRtmClient();
    await initSlackWebClient();

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

    const inputTypeMinHash = (configuration.testMode) ? testInputTypeMinHash : defaultInputTypeMinHash;

    await tcUtils.saveGlobalHistograms({rootFolder: rootFolder, pruneFlag: true, inputTypeMinHash: inputTypeMinHash});

    let slackText = "\n*" + MODULE_ID_PREFIX + " | TRAINING SET*";
    slackText = slackText + "\n" + configuration.userArchivePath;

    await slackSendWebMessage({channel: slackChannel, text: slackText});

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | XXX MAIN END XXX "));
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** MAIN ERROR: " + err));
  }
}, 1000);
