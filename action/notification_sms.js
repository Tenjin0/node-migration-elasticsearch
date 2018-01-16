var util = require('util');
const { initBars, initReqFields, reindexAll, verifyReindexAll, getChoosenIndexes, fetchIndices,
        checkMonth, checkInt, fetchCustomFields, decomposeNmKey, updateCustomField,
        finallyDoTheWork, updateHistoryClick } = require('../lib');

function createCampaignInfo(index) {
    let tmp = new RegExp(/notification_sms_([0-9]+)_([0-9]+)-([0-9]+)-([0-9]+)/).exec(index);    
    if (tmp) {
        tmpIndex = {
            name: tmp[0],
            company: Number.parseInt(tmp[1]),
            year: Number.parseInt(tmp[2]),
            month: Number.parseInt(tmp[3]),
            day: Number.parseInt(tmp[4]),
            storeInType: "company_" + tmp[1]
            
        }
        return tmpIndex
    }
    return null;
}

var transormHitToBulk = function(req, index, hit) {
    delete hit._source.id_company
    hit._type = 'company_' + index.company    
    hit._source.old_index = index.name;
    updateHistoryClick(hit._source)
}

module.exports = async function (req, type, selection, command) {
    function defineIndexSearch() {
        let index;
        switch (type) {
            case "month":
                selection = checkMonth(selection);
                if (selection) {
                    index = "notification_sms_*_" + req.year + "-" + selection + '*';
                }
                break;
            case "year":
                // TODO NWY
                if (checkInt(selection))
                    index = "notification_sms_*_" + selection + "*";
                break;
            case "company":
                // TODO NWY
                if (checkInt(selection))
                    index = "notification_sms_" + selection + "*";
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
    let indexResponse = await fetchIndices(req, filter);
    req.verifyIndexPattern = 'notification_sms_[year]-[month]'    
    let filteredReportIndex = getChoosenIndexes(req, indexResponse, createCampaignInfo)
    await finallyDoTheWork(req, filteredReportIndex, transormHitToBulk)
    
}