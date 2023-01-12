//test comment.
var _ = require('lodash');
//Lodash is a JavaScript library that helps programmers write more concise and maintainable JavaScript. 
var moment = require('moment');
//The moment module is used for parsing, validating, manipulating, and displaying dates and times in JavaScript.
var async = require('async');
//A form of input/output processing that permits other processing to continue before the transmission has finished.
var fs = require('fs');
//fs(File System) is a module which help us to Read files,Create files,Update files,Delete files and Rename file.
var mysql = require('mysql');
//MySQL is a database management system.
var randomstring = require("randomstring");
//creates a series of numbers and letters that have no pattern.
var file_namex = null;
//The null value represents the intentional absence of any object value.
const csv = require('csv-parser');
//This package is a parser converting CSV text input into arrays or objects.
const assert = require('assert');
//The assert is a module which provides a way of testing expressions.
var js2xmlparser = require("js2xmlparser");
//js2xmlparser is a module that parses JavaScript objects into XML.
var util = require('util');
//util is a module which provides some functions to print formatted strings as well as some 'utility' functions that are helpful for debugging purposes.
var Request = require("request");
//request is a module is used to make HTTP calls.
const AWS = require('aws-sdk');
//simpliﬁes use of AWS Services by providing a set of libraries that are consistent and familiar for JavaScript developers.
const sns = new AWS.SNS({
  region:`${process.env.AWS_REGION}`
});
//provides details about a specific AWS region.
/**
 *
 *
 * @param {*} app
 * // provides the name, type, and description of a function parameter.
 */
function UserImport(app) {

    this.app = app;
    this.module_name = 'user_profile';

    this.app.allowed_actions.push(this.module_name);

    this.app.em.on('IMPORT_'+this.module_name, this.process_file, this);
    //this.mysql_connection = mysql.createConnection(process.env.MYSQL_TMS_DB_CONNECTOR+process.env.MYSQL_TMS_DB_CONNECTOR_OPTS);

    this.init();
}

/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * @returns
 * Downloading customer_data.json file first to get the sub_domain name, and customer_id.
 */
UserImport.prototype.download_customerdata_file = function (obj,wo,cb) {
  console.log('In download_customerdata_file: ');


  var key = obj.event.Records[0].s3.object.key.split("/incoming/");
  key = key[0];

  console.log(`Logging Key before no json file found check: ${key}`);
  console.log(`Printing Records as : ${util.inspect(obj.event.Records[0].s3.object)}`);

  if(!key){
    var err = new Error('No Customer data file found!');
    console.error('Error in download_customerdata_file ',err);
    return cb(err,null);
  }
  var params = {
   Bucket: obj.event.Records[0].s3.bucket.name,
   Key: key+'/customer_data.json'
  };

  obj.me.app.s3.getObject(params, function(err, data) {
    if(err){
      console.error('Error in download_file: ',err);
      return cb(err,null);
    }

    try{
      obj.event.customer_data = JSON.parse(data.Body.toString());
      console.log('Customer Data: ',obj.event.customer_data);
    }
    catch(e){
      console.error('Exp in parsing customer data ',e);
      return cb(e,null);
    }
    return cb(null,{msg:"Success in download_customerdata_file"})
    //console.log('Data: ',data);
  });
};//download_customerdata_file
/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * Downloading feed file.
 */
UserImport.prototype.checkingOpts = function (obj, wo, cb) {
  console.log('In checking Opts.');
  obj.event.clear_sf_whenBlank = obj.event.customer_data.customer.clear_standard_fields_when_blank;
  obj.event.clear_cf_whenBlank = obj.event.customer_data.customer.clear_custom_fields_when_blank;
  obj.event.donot_create_users_on_update  = obj.event.customer_data.customer.donot_create_users_on_update;
  obj.event.auto_approval_required_for_all_users  = obj.event.customer_data.customer.auto_approval_required_for_all_users;
  obj.event.email_notifications = obj.event.customer_data.customer.email_notifications;
  obj.event.do_not_reactivate_deleted_users = obj.event.customer_data.customer.do_not_reactivate_deleted_users;
  
  // defined global variable as "create_missing_approving_manager_users" for getting value from customer json.
  obj.event.create_missing_approving_manager_users = obj.event.customer_data.customer.create_missing_approving_manager_users;
  
  // defined global variable as "update_users_with_purchase_approver" for getting value from customer json.
  obj.event.update_users_with_purchase_approver = obj.event.customer_data.customer.update_users_with_purchase_approver.flag;
  obj.event.update_users_with_purchase_approver_id = obj.event.customer_data.customer.update_users_with_purchase_approver.registered_id;
  
  //full name generation config.
  obj.event.full_name_generation_ey = obj.event.customer_data.customer.full_name_generation_ey.flag;
  
  //additional information mention in customer json for full name generation.
  obj.event.full_name_generation_ey_rank_codes = obj.event.customer_data.customer.full_name_generation_ey.rank_codes; //Array of Rank codes

  obj.event.full_name_generation_ey_field_definition_id = obj.event.customer_data.customer.full_name_generation_ey.field_definition_id; //field definition id
  obj.event.full_name_generation_ey_user_admin = obj.event.customer_data.customer.full_name_generation_ey.user_admin; // user_admin is as added_by column in table 'tms_procurement_field_definition_enum_values' 


  console.log(`Logging Opts original for donot_create_users_on_update as: ${obj.event.donot_create_users_on_update}`);
  console.log(`Logging Opts original for sf: ${obj.event.clear_sf_whenBlank}`);
  console.log(`Logging Opts original for cf: ${obj.event.clear_cf_whenBlank}`);
  console.log(`Logging Opts original for auto_approval_required_for_all_users as: ${obj.event.auto_approval_required_for_all_users}`);
  console.log(`Logging Opts original for email_notifications as: ${obj.event.email_notifications}`);
  console.log(`Logging Opts original for do_not_reactivate_deleted_users as: ${obj.event.do_not_reactivate_deleted_users}`);
  
  // log "create_missing_approving_manager_users" value.
  console.log(`Logging Opts original for create_missing_approving_manager_users as: ${obj.event.create_missing_approving_manager_users}`);
 
  // log "update_users_with_purchase_approver" value.
  console.log(`Logging Opts original for update_users_with_purchase_approver as: ${obj.event.update_users_with_purchase_approver}`);

  //log "update_users_with_purchase_approver_id" value.
  console.log(`Logging Opts original for update_users_with_purchase_approver_id as: ${obj.event.update_users_with_purchase_approver_id}`);
  
  //logging "full_name_generation_ey" value
  console.log(`Logging Opts original for full_name_generation_ey: ${obj.event.full_name_generation_ey}`);
  console.log(`Logging Opts original for full_name_generation_ey_rank_codes: ${obj.event.full_name_generation_ey_rank_codes}`);
  console.log(`Logging Opts original for full_name_generation_ey_field_definition_id: ${obj.event.full_name_generation_ey_field_definition_id}`);
  console.log(`Logging Opts original for full_name_generation_ey_user_admin: ${obj.event.full_name_generation_ey_user_admin}`);


  // checking for donot_create_users_on_update opts.
  if (obj.event.donot_create_users_on_update  ==  '' || obj.event.donot_create_users_on_update == 'true'){
    obj.event.donot_create_users_on_update  = true;
    console.log(`Value for donot_create_users_on_update was specified and it is as: ${obj.event.donot_create_users_on_update}`);
  }else{
    obj.event.donot_create_users_on_update  = false;
    console.log(`Value for donot_create_users_on_update was not specified as true or empty and it's value is as: ${obj.event.donot_create_users_on_update}, will pass the same to SPROC!`);
  }

  //checking and setting Opts for do_not_reactivate_deleted_users.
  if (obj.event.do_not_reactivate_deleted_users == '' || obj.event.do_not_reactivate_deleted_users == 'true'){
    obj.event.do_not_reactivate_deleted_users = true;
    console.log(`Value for do_not_reactivate_deleted_users is now set as: ${obj.event.donot_create_users_on_update}, same will be passed to SPROC!`);
  }else{
    obj.event.do_not_reactivate_deleted_users  = false;
    console.log(`Value for do_not_reactivate_deleted_users was not specified as true or empty and it's value is as: ${obj.event.donot_create_users_on_update}, will pass the same to SPROC!`);
  }

  if (obj.event.auto_approval_required_for_all_users  ==  '' || obj.event.auto_approval_required_for_all_usersg == 'true'){
    obj.event.auto_approval_required_for_all_users  = true;
    console.log(`Value for auto_approval_required_for_all_users was specified and it is as: ${obj.event.auto_approval_required_for_all_users}`);
  }else{
    obj.event.auto_approval_required_for_all_users  = false;
    console.log(`Value for auto_approval_required_for_all_users was not specified as true or empty and it's value is as: ${obj.event.auto_approval_required_for_all_users}, will pass the same to SPROC!`);
  }

  if (obj.event.clear_sf_whenBlank == '' || obj.event.clear_sf_whenBlank == 'true') {
    obj.event.clear_sf_whenBlank = true; 
    console.log(`Value for clear_sf_whenBlank was specified and it is as "${obj.event.clear_sf_whenBlank}" has been set, will pass the same to SPROC!`);
  } else {
    obj.event.clear_sf_whenBlank = false;
    console.log(`Value for clear_sf_whenBlank was not specified as true or empty and it's value is as: ${obj.event.clear_sf_whenBlank}, will pass the same to SPROC!`);
  }
  
  if (obj.event.clear_cf_whenBlank == '' || obj.event.clear_cf_whenBlank == 'true') {
    obj.event.clear_cf_whenBlank = true;
    console.log(`Value for clear_cf_whenBlank was specified and it is as "${obj.event.clear_cf_whenBlank}" has been set, will pass the same to SPROC!`);
  } else {
    obj.event.clear_cf_whenBlank = false;
    console.log(`Value for clear_cf_whenBlank was not specified as true or empty and it's value is as: ${obj.event.clear_cf_whenBlank}, will pass the same to SPROC!`);
  }

  if (obj.event.email_notifications == '' || obj.event.email_notifications == 'true') {
    obj.event.email_notifications = true;
    console.log(`Value for email_notifications was specified and it is as "${obj.event.email_notifications}".`);
  } else {
    obj.event.email_notifications = false;
    console.log(`Value for email_notifications was not specified as true or empty and it's value is as: ${obj.event.email_notifications}.`);
  }

  // Checking the value of "create_missing_approving_manager_users" either it is true or false,
  if(obj.event.create_missing_approving_manager_users == '' || obj.event.create_missing_approving_manager_users == 'true'){ 
    obj.event.create_missing_approving_manager_users = true;
    console.log(`Value for create_missing_approving_manager_users was specifed and it is as "${obj.event.create_missing_approving_manager_users}", will pass the same to SPROC!`);
  }
  else{
    obj.event.create_missing_approving_manager_users = false;
    console.log(`Value for create_missing_approving_manager_users was not specified as true or empty and it's value is as "${obj.event.create_missing_approving_manager_users}", will pass the same to SPROC!`);
  }

  // Checking the value of "update_users_with_purchase_approver" either it is true or false,
  if(obj.event.update_users_with_purchase_approver == '' || obj.event.update_users_with_purchase_approver == 'true'){ 
    console.log(`Debug: value for obj.event.update_users_with_purchase_approver_id as: ${obj.event.update_users_with_purchase_approver_id}`);
    if(obj.event.update_users_with_purchase_approver_id != ''){
      obj.event.update_users_with_purchase_approver = true;
      console.log(`Value for update_users_with_purchase_approver_id was specifed thus update_users_with_purchase_approver is set as "${obj.event.update_users_with_purchase_approver}", will pass the same to SPROC!`);
    }
    else{
      obj.event.update_users_with_purchase_approver = false;
      console.log(`Value for update_users_with_purchase_approver_id was not specifed thus update_users_with_purchase_approver is set as "${obj.event.update_users_with_purchase_approver}", will pass the same to SPROC!`);
    }
    
  }
  else{
    obj.event.update_users_with_purchase_approver = false;
    console.log(`Value for update_users_with_purchase_approver was not specifed and it is as "${obj.event.update_users_with_purchase_approver}", will pass the same to SPROC!`);
  }

  //checking for full_name_generation_ey opts.
  if (obj.event.full_name_generation_ey ==  '' || obj.event.full_name_generation_ey == 'true'){
    obj.event.full_name_generation_ey = true;
    console.log(`Value for full_name_generation_ey will be treated as: ${obj.event.full_name_generation_ey}`);
  }else{
    obj.event.full_name_generation_ey = false;
    console.log(`Value for full_name_generation_ey was specified and it is as: ${obj.event.full_name_generation_ey}, will pass the same to SPROC!`);
  }

  return cb(null, { mgs: "Success in checkingOpts, info will be passed to SPROC" });
};
UserImport.prototype.checkingFileType = function (obj, wo, cb) {
  //obj.event.tasks = [this.checkingFileType];
  obj.event.file_type = '';
  console.log(`In File type check.`);
  var params = {
    Key: obj.event.Records[0].s3.object.key
  }
  obj.event.filePath = params.Key;
  console.log(`Logging File Actual Name and Path as: ${params.Key.toString()}`);
  obj.event.override_file_type = obj.event.customer_data.customer.override_file_type;
  console.log(`Logging override_file_type as : ${obj.event.override_file_type}`);
  let name = params.Key.toString().toLowerCase();

  name   =  _.trimEnd(name, '/') //will trim n times if char repeated at end, bullet proof!

  console.log(`splitted path and name as ${_.split(name, '/')}`);
  let arr = _.split(name, '/');
  const lastItem = arr[arr.length - 1]
  name  = lastItem.toString(); 

  console.log(`file name only as: ${name}`);

  let valid_target_file = _.startsWith(name, 'user_profile'); //verifying pre-fix at user import level. 
  console.log(`Logging valid target file check as: ${valid_target_file}`);
  if(!valid_target_file){
    console.log(`Uploaded file does not starts with user_profile.., processing will not initiate, and will be reported as error in file naming standard`);
    var params = "unsuccessfully (The file name was not according to the specified standard, and is missing the prefix as `user_profile`)";
    UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
    const filenameError = new Error(`missing file name prefix as user_profile`);
    return cb(filenameError, { msg: `Failure in checkingFileType` });
  }
  else if (name.includes(`test`)){ // verifying if a 'test' key word is placed anywehre in the file name. 
    console.log(`Uploaded file have a test keyword, and probably was for a test purpose, import process will not take a risk and will decide on halting the prcoessing.`);
    var params = "unsuccessfully (The file name had a `test` keyword at naming, was stopped for processing, was it added mistakenly during creation of file?)";
    UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
    const filenameError = new Error(`file name had a test keyword, stopped for processing`);
    return cb(filenameError, { msg: `Failure in checkingFileType` });
  }
  else if (name.includes(`delta`)) {
    if (obj.event.override_file_type == '') {
      obj.event.file_type = `delta`;
      console.log(`${obj.event.file_type} file has been Recieved, will pass info to SPROC.`);
    } else {
      console.log(`Going to override the file type value as : ${obj.event.override_file_type}`);
      obj.event.file_type = obj.event.override_file_type;
      console.log(`logging decided file type as : ${obj.event.file_type}`);
    }
  } else if (name.includes(`full`)) {
    if (obj.event.override_file_type == '') {
      obj.event.file_type = `full`;
      console.log(`${obj.event.file_type} file has been Recieved, will pass info to SPROC.`);
    } else {
      console.log(`Going to override the file type value as : ${obj.event.override_file_type}`);
      obj.event.file_type = obj.event.override_file_type;
      console.log(`logging decided file type as : ${obj.event.file_type}`);
    }
    
  } else {
    console.log(`File name was not according to standard, and did not have delta or full file identifier in filename.`);
    //obj.event.tasks.push[this.sendFeedProcessingNotification];
    //obj.event.tasks();
    var params = "unsuccessfully (The file name was not according to the specified standard, and is missing the delta or full file identifier)";
    UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
    const filenameError = new Error(`delta or full not found in file name.`);
    return cb(filenameError, { msg: `Failure in checkingFileType` });
  }
  return cb(null, { msg: "Success in checkingFileType" });
}

UserImport.prototype.download_file = function (obj, wo, cb) {
  console.log('In download_file: ');
  console.log(`Type of cb in download_file method: ${typeof(cb)}`);
  var params = {
   Bucket: obj.event.Records[0].s3.bucket.name,
   Key: obj.event.Records[0].s3.object.key
  };
  obj.me.app.s3.getObject(params, function(err, data) {
    if(err){
      console.error('Error in download_file: ',err);
      return cb(err,null);
    }
    obj.event.file_name = '/tmp/UI_'+randomstring.generate({length: 12,charset: 'alphabetic'}); //storing file inside temp storage.
    fs.writeFileSync(obj.event.file_name, data.Body.toString());
    file_namex = obj.event.file_name;
    console.log(`Downloaded File name is : ${file_namex} and type is ${typeof (file_namex)}`);
    return cb(null,{msg:"Success in download_file"})
    //console.log('Data: ',data);
  });
}//download_file

UserImport.prototype.detectingFile_termination = function (obj, wo, cb) {
  console.log(`detecting file line termination method`);
  fs.readFile(obj.event.file_name, function (err, data) {
    if (err) {
      console.error(err);
      var params = "unsuccessfully (The line termination within the file could not be detected)"; 
      UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
      const file_termination_err = new Error(`error while reading file termination`);
      return cb(file_termination_err, { msg: `error in detectingFile_termination` });
    }
    let CRLF_flag = data.includes('\r\n');
    let LF_flag = data.includes('\n');

    if (CRLF_flag === true) {
      console.log(`found CRLF termination, and will place this in load into command`);
      obj.event.line_termination = '\\r\\n';

    } else if (LF_flag === true) {
      console.log(`found LF termination, and will place this in load into command`);
      obj.event.line_termination  = '\\n'
    };
    return cb(null, { msg: "Success in deteing file line termination." });
  });
}

/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * Will parse through headers of csv file only.
 */
UserImport.prototype.parsingCSV = function (obj,wo,cb) {
  console.log(`Inside Parsing CSV method`);
  console.log(`Downloaded File name is : ${obj.event.file_name} and type is ${typeof (obj.event.file_name)}`);
  console.log(`Type of cb is: ${typeof (cb)}`);
      fs.createReadStream(obj.event.file_name)
        .pipe(csv({
          separator: ','
        }))
        .on('headers', (headers) => {
          obj.event.all_fieldNames = [];
          obj.event.all_hc_fields = '';
          obj.event.all_hardcoded_fields = '';
          obj.event.total_SF = null;
          obj.event.total_CF = null;
          obj.event.total_unmatched_fields_from_standard_fields = null;
          obj.event.total_Fields = null;
          for (let index = 0; index < headers.length; index++){
            obj.event.all_fieldNames.push(headers[index].toLowerCase().trim());
            let exp = headers[index].toLowerCase().trim();
            switch (exp) {
              case 'email':
              case 'last_name':
              case 'first_name':
              case 'region':
              case 'address_line_1':
              case 'address_line_2':
              case 'city':
              case 'state':
              case 'zipcode':
              case 'country':
              case 'employee_id':
              case 'approving_manager':
              case 'approving_manager_email':
              case 'department':
              case 'phone_office':
              case 'phone_fax':
              case 'phone_cell':
              case 'approval_required':
              case 'is_admin':
              case 'is_super_admin':
              case 'company_name':        
                console.log(`logging hardcoded field as : ${exp}`);
                obj.event.all_hc_fields += `${exp},`;
                break;
              default:
                console.log(`not hard-coded field.`);
            }
          }
          obj.event.total_Fields = obj.event.all_fieldNames.length;
          obj.event.all_hardcoded_fields  = obj.event.all_hc_fields.substring(0, obj.event.all_hc_fields.length - 1);
          obj.event.exceptions_for_missed_col = "";
          obj.event.exceptions_for_missed_check = null;
          obj.event.specific_missed_col = "";
          let check = null;
          check = _.includes(obj.event.all_hardcoded_fields, "country");
          console.log(`logging check against missing country as : ${check}`);
          if (!check){
            console.log(`going to add country as hardcoded field, as it will be also created as column in temp table with default value as US`);
            obj.event.exceptions_for_missed_col = ",country";
            obj.event.specific_missed_col = ",country varchar(50) DEFAULT 'US'";
            obj.event.all_hardcoded_fields  += obj.event.exceptions_for_missed_col;
            obj.event.exceptions_for_missed_check = true;
          }else{
            console.log(`country existed and will proceed normal`);
            obj.event.exceptions_for_missed_check = false;
          }
          console.log("Total Fields: " + obj.event.total_Fields);
          console.log(`All Fields From File are: ${obj.event.all_fieldNames}`);
          console.log(`All hard-coded fields names are: ${obj.event.all_hardcoded_fields}`);
      
          obj.event.sf_fromDoc = [
            {
              key: 'change_type', value: '', indexingreq: false
            },
            {
              key: 'employee_id', value: 'oak', indexingreq: ',KEY employee_id (employee_id)'
            },
            {
              key: 'email', value: '', indexingreq: ',KEY email (email)'
            },
            {
              key: 'first_name', value: '', indexingreq: ''
            },
            {
              key: 'last_name', value: '', indexingreq: ''
            },
            {
              key: 'company_name', value: '', indexingreq: ''
            },
            {
              key: 'phone_office', value: '', indexingreq: ''
            },
            {
              key: 'phone_cell', value: '', indexingreq: ''
            },
            {
              key: 'phone_fax', value: '', indexingreq: ''
            },
            {
              key: 'address_line_1', value: '', indexingreq: ''
            },
            {
              key: 'address_line_2', value: '', indexingreq: ''
            },
            {
              key: 'city', value: '', indexingreq: 'co-exists_with_state'
            },
            {
              key: 'state', value: 'state_id varchar(50) DEFAULT NULL', indexingreq: ',KEY state (state)'
            },
            {
              key: 'zipcode', value: '', indexingreq: ''
            },
            {
              key: 'country', value: 'oak', indexingreq: ',KEY country (country)'
            },
            {
              key: 'department', value: 'department_id varchar(50) DEFAULT NULL', indexingreq: ',KEY department_id (department_id)'
            },
            {
              key: 'region', value: 'region_id varchar(50) DEFAULT NULL', indexingreq: ',KEY region_id (region_id)'
            },
            {
              key: 'approval_required', value: '', indexingreq: ''
            },
            {
              key: 'approving_manager', value: 'approving_manager_id varchar(50) DEFAULT NULL', indexingreq: ',KEY approving_manager_id (approving_manager_id)'
            },
            {
              key: 'approving_manager_email', value: '', indexingreq: ''
            },
            {
              key: 'is_admin', value: '', indexingreq: ''
            },
            {
              key: 'is_super_admin', value: '', indexingreq: ''
            }
          ];
          return cb(null, { msg: "Success in parsing file" });
        });
};
/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * Will parse through headers of csv file only.
 */
UserImport.prototype.gettingResults_fromTPFD_table = function (obj, wo, cb) {

  obj.event.result_key_values_from_mysql = [];

  var query = `SELECT \`key\`, \`id\` FROM tms_procurement_field_definitions WHERE customer_id=${obj.event.customer_data.customer.id} AND field_type=2;`;
  console.log("query is: " + query);
  obj.me.mysql_connection = mysql.createConnection(process.env.MYSQL_TMS_DB_CONNECTOR + process.env.MYSQL_TMS_DB_CONNECTOR_OPTS);
  obj.me.mysql_connection.query(query, function (err, result, fields) {
    obj.me.mysql_connection.end();
    if (err) {
      console.error(`Error in query at cross check with Spec Doc method`);
      var params = "unsuccessfully"; 
      UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
      return cb(err);
    }
    console.log(`Query returned rows are: ${result.length}`);
    console.log('MySQL data: ', result);
    Object.keys(result)
      .forEach(function (key) {
        var row = result[key];
        console.log(`Key index from Result Object is: ${key}`);
        var x = 1;
        console.log(`Row name from Result Object at iteration ${x} is: ${row.key}`);
        console.log(`Id from Result Object at iteration ${x} is: ${row.id}`);
        x++;
        obj.event.result_key_values_from_mysql.push({
          key: row.key,
          value: row.id
        });
      });
    return cb(null, { msg: `Success from getting Results from TPFD table` });
  });
};
/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * Will parse through headers of csv file only.
 */
UserImport.prototype.checking_for_RequiredFields_inFile = function (obj, wo, cb) {
  let requireField_flag = null;
  let fvflag = _.indexOf(obj.event.all_fieldNames, 'change_type');
  let svflag = _.indexOf(obj.event.all_fieldNames, 'email');
  if (fvflag != -1 && svflag != -1) {
    requireField_flag = true;
  }else {
    requireField_flag = false;
  }
  if (requireField_flag) {
    console.log(`value of required flag: ${requireField_flag}`)
    console.log("Found Required Standard fields as change_type and email inside the feed import file");

    return cb(null, { msg: "Success with Locating Required fields in file" });
  } else {
    console.log("All required standard fields are not found inside the feed import file, will abort the feed process now.");
    const required_field_err = new Error(`Failure in presence of required standard field in feed file.`);
    console.error(required_field_err);
    var params = "unsuccessfully (One or more required fields were missing in the file)";
    UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
    return cb(required_field_err, { msg: `Halting the process, because no required fields found inside feed file.` });
  }
};
/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * Will parse through headers of csv file only.
 */
UserImport.prototype.comparison_construct = function (obj, wo, cb) {
  obj.event.matched_fields = [];
  obj.event.notFound_fields = [];
  obj.event.customfields_Ids_prefix = [];
  obj.event.staticFields = [];
  obj.event.indexing = [];
  obj.event.cf_for_xml_withPrefix = "";
  for (let iterator = 0; iterator < obj.event.total_Fields; iterator++){
    console.log(`logging iterartor of comparison_placement loop: ${iterator}`);
    let found_in_specDoc = obj.event.sf_fromDoc.some(function (elm) { return elm.key == obj.event.all_fieldNames[iterator]; });
    console.log(`Found in spec doc status of field named as ${obj.event.all_fieldNames[iterator]} is ${found_in_specDoc}.`);
    if (found_in_specDoc) {
      console.log(`Found field ${obj.event.all_fieldNames[iterator]} as standard field.`);
      obj.event.matched_fields.push(obj.event.all_fieldNames[iterator]);
      let arrobj = obj.event.sf_fromDoc.find(o => o.key === `${obj.event.all_fieldNames[iterator]}`);
      console.log(`logging found object custome field key value pair as: ${arrobj}`);
      let value_forStatic = arrobj.value;
      let value_forIndexing = arrobj.indexingreq;
      if (value_forStatic != '' && value_forStatic !='oak') {
        console.log(`Logging static field name for field column ${obj.event.all_fieldNames[iterator]} as: ${value_forStatic}`);
        console.log(`Going to add this value into array for static fields.`);
        obj.event.staticFields.push(value_forStatic);
      }
      if (value_forIndexing != '' && value_forIndexing != 'co-exists_with_state') {
        console.log(`Logging Indexing query part for field column ${obj.event.all_fieldNames[iterator]} as: ${value_forIndexing}`);
        console.log(`Going to add this value into array for indexing queries.`);
        obj.event.indexing.push(value_forIndexing);
      }
      if (value_forIndexing == 'co-exists_with_state') {
        let existence = obj.event.all_fieldNames.includes(`state`);
        if (existence) {
          obj.event.indexing.push(`,KEY state_city (city,state)`);
        } else {
          obj.event.indexing.push(`,KEY city (city)`);
        }
      }
      let keyToSearch = `${obj.event.customer_data.customer.sub_domain}_${obj.event.all_fieldNames[iterator]}`;
      console.log(`Key to search: ${keyToSearch}`);
      let co_existence = obj.event.result_key_values_from_mysql.some(function (elem) { return elem.key == keyToSearch; });
      console.log(`Logging result_key_values_from_mysql as: ${obj.event.result_key_values_from_mysql}`);
      console.log(`Logging val for co-existence as: ${co_existence}`);
      if (co_existence) {
        console.log(`Found standard field ${obj.event.all_fieldNames[iterator]} as custom defined as well, will pass this info to sproc`);
        let arrobj3 = obj.event.result_key_values_from_mysql.find(o => o.key === `${keyToSearch}`);
        console.log(`Logging object for co-existence proof ${arrobj3}`);
        let custom_fieldId_for_co_existed_field = arrobj3.value;
        console.log(`Logging custom_fieldId_for_co_existed_field as: ${custom_fieldId_for_co_existed_field}, will push this to array which will be used to pass xml to sproc.`);
        // obj.event.cf_for_xml_withPrefix += `field_${custom_fieldId_for_co_existed_field},`;
        obj.event.cf_for_xml_withPrefix += `${obj.event.all_fieldNames[iterator]},`;
        console.log(`printing cf_for_xml_withPrefix before all population: ${obj.event.cf_for_xml_withPrefix}`);
      }
    }
    else {
      console.log(`Not Found in spec, looking in results for customs fields.`);
      let custom_field_withSubDomain = `${obj.event.customer_data.customer.sub_domain}_${obj.event.all_fieldNames[iterator]}`;
      console.log(`formated field name with sub_domain as : ${custom_field_withSubDomain}`);
      let found_in_custom_flag = obj.event.result_key_values_from_mysql.some(function (elem) { return elem.key == custom_field_withSubDomain; });
      console.log(found_in_custom_flag);
      if (found_in_custom_flag) {
        console.log(`Found field ${obj.event.all_fieldNames[iterator]} as custom field.`);
        let arrobj1 = obj.event.result_key_values_from_mysql.find(o => o.key === `${custom_field_withSubDomain}`);
        console.log(`logging found object custome field key value pair as: ${arrobj1}`);
        let custom_fieldId = arrobj1.value;
        console.log(`logging custom field id: ${custom_fieldId}`)
        obj.event.matched_fields.push(`field_${custom_fieldId}`);
        obj.event.customfields_Ids_prefix.push(`field_${custom_fieldId}`);
        console.log(`formated custom field ${obj.event.all_fieldNames[iterator]} and added into the matched fields arr as: ${obj.event.matched_fields[iterator]}`);
      } else {
        console.log(`Did not found following field as Standard or Custom: ${obj.event.all_fieldNames[iterator]}`);
        var randomstring = require("randomstring");
        let dummy = `dummy${randomstring.generate(7)}`;
        console.log(`Adding ${dummy} field in array which will be dropped when from array temp table is created`);
        //obj.event.matched_fields.push('\`' + dummy + '\`');
        obj.event.matched_fields.push(dummy);
        obj.event.notFound_fields.push(obj.event.all_fieldNames[iterator]);
      }
    }
  }
  console.log(`Logging all macthed fields as: ${obj.event.matched_fields}`);
  console.log(`Logging all not found fields at standard or customs are: ${obj.event.notFound_fields}`);
  console.log(`Logging all formatted custom fields as: ${obj.event.customfields_Ids_prefix}`);
  console.log(`Logging all static fields names as: ${obj.event.staticFields}`);
  return cb(null, { msg: "Success with comparison and construct" });
};
UserImport.prototype.QueryGen = function (obj, wo, cb) {

  console.log(`Inside Querry Generator method`);
  console.log(`Printing Array ${obj.event.matched_fields}`);
  obj.event.queryPart = "";
  obj.event.queryPart_fieldNamesOnly = "";
  //obj.event.cf_for_xml_withPrefix = "";
  obj.event.staticFields_queryPart = "";
  obj.event.indexing_queryPart = "";
  obj.event.cf_for_xml_wp = "";
  for (let iterartor_a = 0; iterartor_a < obj.event.staticFields.length; iterartor_a++){
    obj.event.staticFields_queryPart += `${obj.event.staticFields[iterartor_a]},`;
  }
  for (let iterartor_b = 0; iterartor_b < obj.event.indexing.length; iterartor_b++){
    obj.event.indexing_queryPart += `${obj.event.indexing[iterartor_b]}`;
  }
  console.log(`Logging query Part for static fields as: ${obj.event.staticFields_queryPart}`);
  console.log(`Logging query Part for indexing as: ${obj.event.indexing_queryPart}`);
  for (let iterartor = 0; iterartor < obj.event.matched_fields.length; iterartor++){
    console.log(`Printing Array inside for ${obj.event.matched_fields} and value of iterator as: ${iterartor}`);
    let iscustom = obj.event.matched_fields[iterartor].includes(`field_`);
    console.log(`printing value of is custom at iterator as: ${iterartor} for value from matched array as: ${obj.event.matched_fields[iterartor]}`);
    let ischange_type = obj.event.matched_fields[iterartor].includes(`change_type`);
    if (iscustom) {
      obj.event.queryPart += ` ${obj.event.matched_fields[iterartor]} varchar(255) DEFAULT NULL,`;
      //obj.event.queryPart += '\`'+obj.event.matched_fields[iterartor]+'\` varchar(255) DEFAULT NULL,';
      obj.event.queryPart_fieldNamesOnly += ` ${obj.event.matched_fields[iterartor]},`;
      obj.event.cf_for_xml_withPrefix += `${obj.event.matched_fields[iterartor]},`
    }
    else if (ischange_type) {
      obj.event.queryPart += '\`ACTION\` varchar(50) DEFAULT NULL,'
      obj.event.queryPart_fieldNamesOnly += ' \`ACTION\`,';
    }
    else {
      // check in case when column of country is provided by customer, but has all empty values, we set default to US here as well!
      if (obj.event.matched_fields[iterartor] == 'country'){
        console.log(`counrty already exists as column, setting its default as US now`);
        obj.event.queryPart += ` ${obj.event.matched_fields[iterartor]} varchar(255) DEFAULT 'US',`;
        obj.event.queryPart_fieldNamesOnly  += ` ${obj.event.matched_fields[iterartor]},`;
      }else{
        obj.event.queryPart += ` ${obj.event.matched_fields[iterartor]} varchar(255) DEFAULT NULL,`;
        obj.event.queryPart_fieldNamesOnly += ` ${obj.event.matched_fields[iterartor]},`;
      }
    }
  }
  if (obj.event.queryPart != '') {
    obj.event.create_query = null;
    obj.event.into_query = null;
    obj.event.all_from_file = '';
    obj.event.all_from_file_onlyFields = '';
    console.log(`Logging queryPart as: ${obj.event.queryPart}`);

    obj.event.all_from_file = obj.event.queryPart.substring(0, obj.event.queryPart.length - 1);
    console.log(`Logging all_from_file: ${obj.event.all_from_file}`);

    obj.event.all_from_file_onlyFields = obj.event.queryPart_fieldNamesOnly.substring(0, obj.event.queryPart_fieldNamesOnly.length - 1);
    console.log(`Logging feild names only from query part as: ${obj.event.all_from_file_onlyFields}`);

    console.log(`going to create temp table query with country as added col with default value as US`);
    obj.event.create_query = `DROP table if exists temp_import_users; CREATE TEMPORARY TABLE temp_import_users ( id int(11) NOT NULL AUTO_INCREMENT, tms_user_id int(11) DEFAULT NULL, pre_existing tinyint(1) DEFAULT 0, pre_existing_username tinyint(1) DEFAULT 0, ${obj.event.staticFields_queryPart} ${obj.event.all_from_file} ${obj.event.specific_missed_col}, PRIMARY KEY (id), KEY tms_user_id (tms_user_id), KEY pre_existing (pre_existing), KEY pre_existing_username (pre_existing_username) ${obj.event.indexing_queryPart}) ENGINE=InnoDB AUTO_INCREMENT=512 DEFAULT CHARSET=utf8; `;
    obj.event.into_query = `LOAD DATA LOCAL INFILE '${obj.event.file_name}' INTO TABLE temp_import_users FIELDS ESCAPED BY '\\\\' TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '${obj.event.line_termination}' IGNORE 1 LINES (${obj.event.all_from_file_onlyFields}); `;
    console.log(`Logging create query as: ${obj.event.create_query}`);
    console.log(`Logging into query as: ${obj.event.into_query}`);
    obj.event.cf_for_xml_wp = obj.event.cf_for_xml_withPrefix.substring(0, obj.event.cf_for_xml_withPrefix.length - 1);
    console.log(`Logging cf_for_xml_withPrefix as: ${obj.event.cf_for_xml_wp}`);
    return cb(null, { mgs: "Success in Query Gen" });
  } else {
  const y = new Error(`Query Gen method failed at generating query becasue of No Single Standard Fields Found.`);
  var params = "unsuccessfully (No standard fields were found in the file)"; 
  UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
  return cb(y, { msg: `Failure in QueryGen Method` });
  }
};
/**
 *
 *
 * @param {*} obj
 * @param {*} wo
 * @param {*} cb
 * this function does execute generated queries for creation of temp table, and then loading data from file for matched fields either from spec doc or key, into the temp tabel, also this fucntion then passes the custom fields name which were also used to create fields names for temp table.
 */
UserImport.prototype.add_data = function (obj, wo, cb) {
  console.log('In add_data: ', obj.event.file_name);
  console.log(`Printing Create before xml generation from queryGen: ${obj.event.create_query}`);
  console.log(`Logging Opts for sf: ${obj.event.clear_sf_whenBlank}`);
  console.log(`Logging Opts for cf: ${obj.event.clear_cf_whenBlank}`);

  var jsondata = { // generation of xml data by js2xmlparser.
    "customer_id": obj.event.customer_data.customer.id,
    "cf": obj.event.cf_for_xml_wp, // custom field names as field_<custom_field_id>
    "file_type": obj.event.file_type,
    "clear_standard_fields_when_blank": obj.event.clear_sf_whenBlank,
    "clear_custom_fields_when_blank": obj.event.clear_cf_whenBlank,
    "donot_create_users_on_update": obj.event.donot_create_users_on_update,
    "auto_approval_required_for_all_users": obj.event.auto_approval_required_for_all_users,
    "all_hardcoded_fields": obj.event.all_hardcoded_fields,
    "create_missing_approving_manager_users": obj.event.create_missing_approving_manager_users, // passing "create_missing_approving_manager_users" to SPROC
    "update_users_with_purchase_approver": obj.event.update_users_with_purchase_approver, // passing "update_users_with_purchase_approver" to SPROC
    "update_users_with_purchase_approver_id": obj.event.update_users_with_purchase_approver_id,
    "do_not_reactivate_deleted_users": obj.event.do_not_reactivate_deleted_users,
    "full_name_generation_ey": obj.event.full_name_generation_ey,
    "full_name_generation_ey_rank_codes": obj.event.full_name_generation_ey_rank_codes,
    "full_name_generation_ey_field_definition_id": obj.event.full_name_generation_ey_field_definition_id,
    "full_name_generation_ey_user_admin": obj.event.full_name_generation_ey_user_admin,
  };

  let resultxml = js2xmlparser.parse("data", jsondata);
  let xml2 = resultxml.replace("<?xml version='1.0'?>", "");

  var query = obj.event.create_query
      query += obj.event.into_query;
      query += "select * from temp_import_users LIMIT 3; ";
      query +=  "select count(*) from temp_import_users; ";
      query += "SET @xml2 = \""+xml2+"\"; CALL spr_import_user(@xml2);";
    obj.me.mysql_connection = mysql.createConnection(process.env.MYSQL_TMS_DB_CONNECTOR + process.env.MYSQL_TMS_DB_CONNECTOR_OPTS);

    //console.log('MYSQL: ',obj.me.mysql_connection,'::',process.env.MYSQL_TMS_DB_CONNECTOR,'::',process.env.MYSQL_TMS_DB_CONNECTOR_OPTS);
    console.log("Concatenated query is: " + query);
    obj.me.mysql_connection.query(query, function (err, results, fields) {
    obj.me.mysql_connection.end();
    if (err){
      console.error('Error in add_data ',err);
      var params = "unsuccessfully"; 
      UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
      return cb(err);
    }
    
    console.log('MySQL data: ', results);
    var res = [];
    for(var i in results){
      if (results[i].fieldCount == undefined && results[i][0] && results[i][0].result){
        if(results[i][0].result != 'SUCCESS'){
          var err = new Error(results[i][0].message);
          console.error('Error in add_data ',err);
          //Send TMRIS alert
          var params = "unsuccessfully"; 
          UserImport.prototype.sendFeedProcessingNotification(obj, params, cb);
          return cb(err,null);
        }
      }
    }
    console.log('Final result: ',res);
    return cb(null,{msg:'Success in add_data'});
  });
}//add_data
UserImport.prototype.moveToArchive = function (obj, wo, cb) {
  console.log(`Inside move to archive.`);
  console.log(`Logging bucket folder path as: ${obj.event.Records[0].s3.bucket.name + '/' + obj.event.filePath}`);
  var key = obj.event.Records[0].s3.object.key.split("/incoming/");
  key = key[0];
  console.log(`logging key of zero index: ${key}`);
  console.log(`logging current folder name of current bucket as ${obj.event.Records[0].s3.bucket.name + '/' + key + '/archive/'}`);
  console.log(`Logging file key explicitly as ${obj.event.Records[0].s3.object.key}`);
  var str = obj.event.Records[0].s3.object.key;
  var rest = str.substring(0, str.lastIndexOf("/") + 1);
  var last = str.substring(str.lastIndexOf("/") + 1, str.length);
  console.log(`Printing only file name as : ${last}`);
  var params = {
    Bucket: obj.event.Records[0].s3.bucket.name + '/' + key + '/archive/',
    CopySource: obj.event.Records[0].s3.bucket.name + '/' + obj.event.filePath,
    Key: last
  };
  obj.me.app.s3.copyObject(params, function (err, data) {
    if (err) {
      console.log(err.stack);
      return cb(err, null);
    } else {
      console.log(data);
      return cb(null, { msg: 'Success in Archiving.' });
    }
  });
}//moveToArchive.
UserImport.prototype.deleteFromIncoming = function (obj, wo, cb) {
  console.log(`Inside delete current file from incoming`);
  var params = {
    Bucket: obj.event.Records[0].s3.bucket.name,
    Key: obj.event.Records[0].s3.object.key
  };
  obj.me.app.s3.deleteObject(params, function (err, data) {
    if (err) return cb(err.stack, null); // an error occurred
    else return cb(null, { mgs: 'Success in deleting object' });           // successful response
  }); 
}


UserImport.prototype.sendFeedProcessingNotification = function(obj, wo, cb){
  console.log(`Inisde sendFeedProcessingNotification..`);
  
  console.log(`Checking if emails are required to send out as: ${obj.event.email_notifications}`);
  
  obj.event.customer_email_lis = []; //array for customer email.
  obj.event.recipient_customer_id = obj.event.customer_data.customer.id;
  obj.event.customer_const_id = 181;
  obj.event.emailsFromResult = [];
  obj.event.recipients = [];
  obj.event.process_result = null;
  obj.event.flagfr = true;
  
  obj.event.param_extract = null;
  
  
  console.log(`Original value of wo is as : ${wo}`);
  console.log(`logging recipient customer id as : ${obj.event.recipient_customer_id}`);
  
  obj.event.param_extract = JSON.stringify(wo);
  
  console.log(`Logging param_extract is as: ${obj.event.param_extract} and type is as ${typeof(obj.event.param_extract)}`);
  
  if (obj.event.param_extract.includes('unsuccessfully')){
    console.log(`value of wo is as : ${util.inspect(wo)}`);
    if(obj.event.email_notifications != true){
      console.log(`checked options as not required to send an email, control will be passed back to parent function.`);
      return true;
    }
    obj.event.flagfr =false;
    obj.event.process_result = wo
    }else{
      if(obj.event.email_notifications != true){
      return cb(null, { msg: 'Success in porcessing file, please note no email notifications broadcasted as were not enabled for customer' });
    }
  obj.event.process_result = "successfully";
  }
  
  var query = `SELECT \`value\` FROM tms_customer_constants where customer_id=${obj.event.recipient_customer_id} AND constant_id=${obj.event.customer_const_id}`;
  console.log(`query for getting email is as : ${query}`);
  
  obj.me.mysql_connection = mysql.createConnection(process.env.MYSQL_TMS_DB_CONNECTOR + process.env.MYSQL_TMS_DB_CONNECTOR_OPTS);
  obj.me.mysql_connection.query(query, function(err, result){
    obj.me.mysql_connection.end();
    if(err){
      console.error(`Error in query at getting emails values from tms_customer_constants table`);
      if (obj.event.flagfr == true){
        return cb(err);
      }
    }
    console.log(`Query for getting emails returned rows are: ${result.length}`);
    console.log(`MySql data: ${util.inspect(result)}`);
    for (var i in result){
      console.log(`logging i as : ${i}`);
      obj.event.emailsFromResult = result[i].value;
    }
    console.log(`Logging emailsFromResult which is of type ${typeof(obj.event.emailsFromResult)} and values as : ${obj.event.emailsFromResult}`);
    obj.event.recipients = obj.event.emailsFromResult.split(",");
    console.log(`logging recipients array as : ${obj.event.recipients}`);
    
    var filename_with_path = obj.event.Records[0].s3.object.key;
    var filename_with_path_array = filename_with_path.split('/');
    var actual_filename = filename_with_path_array[2];
    
    console.log(`actual_filename: ${actual_filename}`);
    
    console.log(`APP_CRON_TOKEN: ${process.env.APP_CRON_TOKEN}`); // env variable

    console.log(`Logging route_url as: ${process.env.ROUTE_URL}`); // logging route url

    console.log(`route_url: ${process.env.ROUTE_URL+"/template_email/send_bee_email/131"}`);
    
    var AuthorizationString = `Bearer ${process.env.APP_CRON_TOKEN}`;
    console.log(`AuthorizationString: ${AuthorizationString}`); // env variable
    
    var urlString = process.env.ROUTE_URL+"/template_email/send_bee_email/131";
    
    console.log(`Logging urlString as: ${urlString}`); // logging urlstring 
    console.log(`value for tms_customer_id as: ${obj.event.customer_data.customer.id}`);
    
    //create post_data for  below request
    const post_data = {
      "tms_customer_id": obj.event.customer_data.customer.id,
      "emailTypeId": "131",
      "toEmail": obj.event.recipients,
      "file_type": actual_filename,
      "result": obj.event.process_result
    };
    console.log(`post_data as: ${util.inspect(post_data)}`);
    Request.post({
      "headers": { "content-type": "application/json" , "Authorization": AuthorizationString },
      "url": urlString,
      "body": JSON.stringify(post_data)
    }, (error, response) => {
    
      if (error){
        console.log(`an error came in while making post request, error is as : ${error}`);
        if (obj.event.flagfr == true){
          return cb(error, null);
        }
      }
      console.log(`Email sending API response: ${util.inspect(response)}`);
      console.log('Email send to customer successfully');
      if (obj.event.flagfr == true){
        return cb(null, { msg: 'Success in getting emails and sending email.' });
      }
    })
  })
}

/**
 *
 *
 * @param {*} obj
 */
UserImport.prototype.process_file = function (obj) {
  console.log('In process_file: ');
  var me = this;
  //download_file
  //SQL query
  //Run SP
  //Return results
  async.waterfall([
    async.apply(me.download_customerdata_file, { event: obj.event, me: me }, {}),
    async.apply(me.checkingOpts, { event: obj.event, me: me }),
    async.apply(me.checkingFileType, { event: obj.event, me: me }),
    async.apply(me.download_file, { event: obj.event, me: me }),
    async.apply(me.detectingFile_termination, { event: obj.event, me: me}),
    async.apply(me.parsingCSV, { event: obj.event, me: me }),
    async.apply(me.gettingResults_fromTPFD_table, { event: obj.event, me: me }),
    async.apply(me.checking_for_RequiredFields_inFile, { event: obj.event, me: me }),
    async.apply(me.comparison_construct, { event: obj.event, me: me }),
    async.apply(me.QueryGen, { event: obj.event, me: me }),
    async.apply(me.add_data, { event: obj.event, me: me }),
    async.apply(me.moveToArchive, { event: obj.event, me: me }),
    async.apply(me.deleteFromIncoming, { event: obj.event, me: me }),
    async.apply(me.sendFeedProcessingNotification, { event: obj.event, me: me })
  ],
  function(err,result){
    console.log('CB in process_file: ', err, result);
    if(err){
      //Generating TMRIS logs
      obj.result = {code_name:'ERROR',err:err};
      console.log({log_event_type:'TMRIS',module:'im_import',env:process.env.ENV_NAME,priority:"HIGH","risid":"",result:"ERROR",subject:'USERIMPORT_ERROR: Error occurred in User feed import for Customer with Sub-Domain name as: '+obj.event.customer_data.customer.sub_domain+'.',filename:obj.event.Records[0].s3.object.key,message:'User Feed Import Failed!'})
      
      let filenameWithPath = obj.event.Records[0].s3.object.key;
      let filenameWithPath_array = filenameWithPath.split('/');
      let actual_filename = filenameWithPath_array[2];

      console.log(`obj in error blog: ${util.inspect(obj)}`);

      console.log(`actual_filename: ${actual_filename}`);

      let message_params = {
          "Event Time":`${new Date().toISOString()}`,
          "Identifier Link":"",
          "Source ID":`Customer '${obj.event.customer_data.customer.sub_domain}'; \nFilename: '${actual_filename}'`,
          "Source ARN":"",
          "Event ID":"failed_user_import",
          "Event Message":`Failed User feed import processing for `,
          "event_tag":"event:userImport",
        };
      console.log(`message_params is as: ${util.inspect(message_params)} and type is as: ${typeof(message_params)} `);

      message_params = JSON.stringify(message_params);
      console.log(`message_params stringify is as: ${message_params} and type is as: ${typeof(message_params)}`);
      
      // let subject = `User feed import for Customer with Sub-Domain name as ${obj.event.customer_data.customer.sub_domain} and filename is as ${actual_filename}`;
      // console.log(`subject is as : ${subject}`);
      // subject = JSON.stringify(subject);
      // triggering SNS TOPIC for datadog
      var params = {
        Message: message_params, /* required */
        Subject: 'Failed user feed import',
        TopicArn: `${process.env.SNSTopicArn}`
      };
      console.log(`logging sns details as : ${util.inspect(params)}`);
      sns.publish(params, function(err, data) {
        if (err) {
            console.log(`inside error publshing message`);
            console.log(err, err.stack);
        }else {
            console.log(`Successfully published the SNS topic -> ${util.inspect(data)}`);
        }               
      });
      
    }
    obj.result = {code_name:'SUCCESS',data:result};
    console.log({log_event_type:'TMRIS',module:'im_import',env:process.env.ENV_NAME,priority:"HIGH","risid":"",result:"SUCCESS",subject:'USERIMPORT_SUCCESS: Success in User feed import for Customer with Sub-Domain name as: '+obj.event.customer_data.customer.sub_domain+'.',filename:obj.event.Records[0].s3.object.key,message:'User Feed Import Success!'})
    return obj.cb(obj);
  });

  //obj.result = {code_name:'SUCCESS',data:"Success"};
  //return (_.isFunction(obj.cb)) ? obj.cb(obj) : '';
};

UserImport.prototype.init = function () {

}; //init

module.exports = UserImport;
