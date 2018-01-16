var util = require('util');
const { initBars, initReqFields, reindexAll, verifyReindexAll, getChoosenIndexes, fetchIndices, checkMonth, checkInt, fetchCustomFields, decomposeNmKey, updateCustomField, finallyDoTheWork } = require('../lib');
const _progress = require('cli-progress');
const _colors = require('colors');

function createRecipientInfo(index, recipients) {
    var foundRecipientById = function (id) {
        for (let i = 0; i < recipients.length; i++) {
            if (recipients[i].id == id) {
                return recipients[i];
            }
        }
        return null
    }
    let tmp = new RegExp(/recipients_([0-9]+)_([0-9]+)/).exec(index);
    if (tmp !== null && (recipient = foundRecipientById(tmp[2])) !== null) {
        tmpIndex = {
            name: tmp[0],
            company: Number.parseInt(tmp[1]),
            recipient: tmp[2],
            year: recipient.year,
            month: recipient.month,
            day: recipient.day,
            list: {}

        }
        tmpIndex.list[tmp[2]] = []
        return tmpIndex
    }
    return null;
}

var transormHitToBulk = function (req, index, hit) {
    hit._source.old_index = index.name;
    hit._source.id_company = index.company;
    // hit._source.creation_date = index.year + '-' + index.month.toLocaleString('en-US', {minimumIntegerDigits: 2, useGrouping:false}) + '-' + index.day.toLocaleString('en-US', {minimumIntegerDigits: 2, useGrouping:false});

    updateCustomField(index, hit, index.recipient)
}

var getAllRecipientsByFilter = async function (req, type, selection, year) {
    return new Promise((resolve, reject) => {

        let sql = "SELECT id, month(creation_datetime) as month, year(creation_datetime) as year, day(creation_datetime) as day from si.import_list_information where ";
        let sqlVars = []
        switch (type) {
            case "month":
                sql += "month(creation_datetime) = ? and year(creation_datetime) = ?"
                sqlVars = [selection, year]
                break;
            case "index":
                sql += "id = ?"
                let idCampaign = selection.split("_")[2]
                sqlVars = [idCampaign]
                break;
            default:
                break;
                sql += "order by id"
        }
        req.db.mysql.query(sql, sqlVars, function (err, recipients) {
            if (err) return reject(err);
            resolve(recipients)
        });
    })
}

var updateILI = async function(req, reportIndex) {
    return new Promise((resolve, reject) => {
        let sqlVars = {
            id_import_list_information : reportIndex.recipient
        }
        req.db.mysql.query("DELETE FROM si.list_has_number_type WHERE ?",sqlVars, function(err, data) {
            if (err) return reject(err)
            // console.warn(req.numberTypes);
            let values = [] //TODO
            for (let i = 0; i < reportIndex.numberType.length; i++) {
                let numberType = reportIndex.numberType[i];
                values.push([reportIndex.recipient, req.numberTypes[numberType]])
            }
            // console.warn(values)
            req.db.mysql.query("INSERT INTO list_has_number_type VALUES ?", [values],function(err, data) {
                if (err) return reject(err)
                resolve()
            })
        })

    })
}

var retrievedNumberType = async function(req) {
    return new Promise((resolve, reject) => {
        req.db.mysql.query('SELECT id, name FROM number_type', function(err, dataRows) {
            if (err) return reject(err);
            let result = {}
            for(let i = 0; i < dataRows.length; i++) {
                result[dataRows[i].name] = dataRows[i].id
            }
            req.numberTypes = result;
            resolve(dataRows)
        });
    })
}
module.exports = async function (req, type, selection, command) {

    function defineIndexSearch() {
        let index;
        switch (type) {
            case "month":
                if (selection) {
                    index = "recipients_*";
                }
                break;
            case "index":
                index = selection;
                break;
            default:
                console.warn('default');
                process.exit(2);
                break;
        }
        return index
    }
    let filter = defineIndexSearch()
    req.originCluster = 'recipient_cluster'
    let [indexResponse, recipientsResponse] = await Promise.all([
        fetchIndices(req, filter),
        getAllRecipientsByFilter(req, type, selection, command.year)
    ]);
    // console.warn(recipientsResponse);
    req.verifyIndexPattern = 'recipients_[year]-[month]'
    let filteredReportIndex = getChoosenIndexes(req, indexResponse, createRecipientInfo, recipientsResponse)
    let customVerification = {
        aggs: {
            "count_by_number_type": {
                "terms": {
                    "field": "extranet_NUMBER_TYPE"
                }
            }
        },
        checkAgg : function(bucket, indexToCheck) {
            let bucketsNumberType = bucket.count_by_number_type.buckets
            indexToCheck.numberType = []
            for (let i = 0 ; i < bucketsNumberType.length ; i++) {
                if (indexToCheck.numberType.indexOf(bucketsNumberType[i].key) < 0) {
                    indexToCheck.numberType.push(bucketsNumberType[i].key)
                }
            }
        }
    }
    await finallyDoTheWork(req, filteredReportIndex, transormHitToBulk, customVerification)
    await retrievedNumberType(req)
    var formatVerif = 'progress' + _colors.yellow(' {bar}') + ' : {percentage}% | {value}/{total} | update number type : {index}';
    if (!req.debug)
        req.verificationBar = new _progress.Bar({
            format: formatVerif
        }, _progress.Presets.shades_classic);
        req.verificationBar.start(filteredReportIndex.length, 0, {
            index: filteredReportIndex[0].name
        })
    for(let i = 0; i < filteredReportIndex.length; i++) {
        let reportIndex = filteredReportIndex[0];
        await updateILI(req, reportIndex)
        if (!req.debug)
            req.verificationBar.update(i + 1, {
                index: reportIndex.name
            })
    }
    if (!req.debug)
        req.verificationBar.stop();
}    