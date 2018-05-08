var url = require('url')
var https = require('https')

// Date formatting function to YYYY-MM-DD
Date.prototype.yyyymmdd_dash = function() {
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();

  return [this.getFullYear(),
          (mm>9 ? '' : '0') + mm,
          (dd>9 ? '' : '0') + dd
         ].join('-')
}

// Date formatting function to YYYYMMDD
Date.prototype.yyyymmdd_nodash = function() {
  var mm = this.getMonth() + 1; // getMonth() is zero-based
  var dd = this.getDate();

  return [this.getFullYear(),
          (mm>9 ? '' : '0') + mm,
          (dd>9 ? '' : '0') + dd
         ].join('')
}

Array.prototype.isEmpty = function() {
  if (this == null || this.length == 0) return true
  else return false
}

// Messages to be sent to slack
const messageAll = "All files were received."
const messageSingle = "The following file was not found in yesterday's upload: \n"
const messagePlural = "The following files were not found in yesterday's upload: \n"
const messageError = "No missing or received files discovered.  Please investigate."
// Slack connection
const SLACK_URL = 'https://hooks.slack.com/services/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'
const SLACK_REQ_OPTS = url.parse(SLACK_URL)
const SLACK_CHANNEL_TO_POST = '#3p_logs_monitor'
SLACK_REQ_OPTS.method = 'POST'
SLACK_REQ_OPTS.headers = {
    'Content-Type': 'application/json'
}

// Replacement text key
const PLACEHOLDER_DASH = '%DATE_DASH%'
const PLACEHOLDER_NODASH = '%DATE_NODASH%'

// For 3rd Party log data, it will always be the same bucket
const BUCKET = 'p1-third-party-log-metadata'

// Get the check date value (yesterday)
var DATE = new Date()
DATE.setDate(DATE.getDate() - 1)
const checkDateDash = DATE.yyyymmdd_dash()
const checkDateNoDash = DATE.yyyymmdd_nodash()

// *****************************************************************************
// List of folder/filenames to validate receipt of
// *****************************************************************************
const FILE_NAMES = [
    `folder/filename_one_${PLACEHOLDER_DASH}.zip`,
    `folder/filename_two_${PLACEHOLDER_DASH}.zip`
    ]
// *****************************************************************************

// Connect to S3
var AWS = require('aws-sdk')
AWS.config.loadFromPath('./config.json')
var s3 = new AWS.S3()

// Declare arrays to track files received and not received
var missingFilesArry = [] // array of slack msg attachments - updated in getFile()
var receivedFilesArry = [] // array of strings of file names - updated in getFile()

exports.handler = (event, context, callback) => {
  callback(null, ace())
};


/* *****************************************************************************
    Helper functions
   *****************************************************************************/

function ace () {
  checkBucket().then( () => {
    var a = createMissingFileMessageAll(missingFilesArry, receivedFilesArry)
    postToSlack(a)
  }).catch((error) => { console.log(error)})
}

// Helper function to create our slack message based upon the files received/not received
// params
//  - missingFiles - array of slack msg attachments
  function createMissingFileMessage(missingFiles) {

    var slackMessage = new Object()

    slackMessage.channel = SLACK_CHANNEL_TO_POST
    slackMessage.text = "Third Party Logs - File Monitor Alert for " + checkDateDash + "\n"

    if (missingFiles.isEmpty()) {
      slackMessage.text += messageAll
      return slackMessage
    }
    else if (missingFiles.length > 1) {
      missingFiles.sort()
      slackMessage.text += messagePlural
    }
    else { slackMessage.text += messageSingle }

    slackMessage.attachments = missingFiles
/*
    missingFiles.forEach( function(value) {
      message += value + "\n"
    })
*/
    return slackMessage
}

// Helper function to create our slack message based upon the files received/not received
// params
//  - missingFiles - array of slack msg attachments
//  - receivedFiles - array of strings (file names for the received files)
  function createMissingFileMessageAll(missingFiles, receivedFiles) {

    var slackMessage = new Object()
    var rfAttachment = new Object()

    slackMessage.channel = SLACK_CHANNEL_TO_POST
    slackMessage.text = "Third Party Logs - File Monitor Alert\n"

    if (missingFiles.isEmpty() && receivedFiles.isEmpty()) {
      slackMessage.text = messageError
    }
    else {
      if (!missingFiles.isEmpty()) { missingFiles.sort() }
      if (!receivedFiles.isEmpty()) {
        receivedFiles.sort()
        rfAttachment.color = "#00B140"
        rfAttachment.title = "Files Received"
        receivedFiles.forEach( function(value) {
          if (rfAttachment.text == null) { rfAttachment.text = value }
          else { rfAttachment.text += "\n" + value }
        })
      }

      // combine all attachments
      slackMessage.attachments = missingFiles.concat(rfAttachment)

      if (missingFiles.isEmpty()) {
        slackMessage.text = "*" + receivedFiles.length + "* of *" + FILE_NAMES.length + "* were successfully received for *" + checkDateDash + "*."
      }
      else if (missingFiles.length > 1) {
        slackMessage.text += "*" + missingFiles.length + "* of *" + FILE_NAMES.length + "* files are missing from *" + checkDateDash + "*."
      }
      else {
        slackMessage.text += "*" + missingFiles.length + "* of *" + FILE_NAMES.length + "* files is missing from *" + checkDateDash + "*."
      }
    }

    return slackMessage
}

// Actually performs the check against S3 to see if the file exists (getObject())
function getFile (params) {
  return new Promise((resolve,reject) => s3.getObject(params, function(err, data) {
    if (err) {
      reject(err);
      // file does not exist
      var attachment = new Object()
      attachment.text = params["Key"]
      attachment.color = "#FF0000"
      attachment.title = "File Missing!"
      missingFilesArry.push(attachment)
    }
    else {
      resolve(data);
      //file exists
      receivedFilesArry.push(params["Key"])
    }
  }))
}

function postToSlack (message) {
  console.log("START FUNCTION postToSlack()")
  var r = https.request(SLACK_REQ_OPTS, function(res) {
    res.setEncoding('utf8')
    if (res.statusCode === 200) {console.log('POSTED TO SLACK')}
    else { console.log('FAILED TO POST TO SLACK - STATUS CODE: ' + res.statusCode)}
  })

  r.on('error', function(e) {
    console.log('PROBLEM WITH SLACK REQUEST: ' + e.message)
  })

  //console.log(message)
  r.write(JSON.stringify(message))

  r.end()
}

// Checks the bucket by iterating through the list of folder/filenames we are interested in
function checkBucket() {

  // reset the file arrays
  missingFilesArry = []
  receivedFilesArry = []

  const paramsArray = FILE_NAMES
      .map(file => file.replace(PLACEHOLDER_DASH, checkDateDash))
      .map(file => file.replace(PLACEHOLDER_NODASH, checkDateNoDash))
      .map(fileName => ({Bucket: BUCKET, Key: fileName}))

  const RESULTS = waitAllFailSlow(paramsArray.map(getFile))
    .then(results => {
          // this is what happens when no fails...
          //console.log("WOOT WOOT!!!")
          //console.log(results)
          console.log("SUCCESS: checkBucket() passed successfully.")
    })
    .catch(err => {
          // this is the error for fail fast
          console.log("ERROR ENCOUNTERED: checkBucket()")
          //console.log(err)
          //if ("code" in err[0] && err[0]["code"] == "NoSuchKey") { console.log("FILE NOT FOUND ACE")} else {console.log("No code Ace")}

    })
    //console.log("OUTSIDE=========1========")
    return RESULTS

}

function waitAllFailSlow (promises) {
  const getPromiseValue = ({value}) => value
  const isPromiseStatus = expectedStatus => ({status}) => status === expectedStatus
  const isErrorPromise = isPromiseStatus('error')
  const isValidPromise = isPromiseStatus('ok')

  return Promise.all(promises.map(promise => promise
    .then(value => ({value, status: 'ok'}))
    .catch(err => ({value: err, status: 'error'}))
  )).then(allPromises => {
    const errors = allPromises.filter(isErrorPromise)
    return errors.length > 0 ?
      Promise.reject(errors.map(getPromiseValue)) :
      Promise.resolve(allPromises.filter(isValidPromise).map(getPromiseValue))
  })
}
