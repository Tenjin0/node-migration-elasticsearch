var util = require('util');
const { initBars, initReqFields, reindexAll, verifyReindexAll, getChoosenIndexes, fetchIndices, checkMonth, checkInt, fetchCustomFields, decomposeNmKey, updateCustomField, finallyDoTheWork } = require('../lib');

function createCampaignInfo(index) {
    let tmp = new RegExp(/events_([0-9]+)_([0-9]+)_([0-9]+)-([0-9]+)-([0-9]+)/).exec(index);    
    if (tmp) {
        tmpIndex = {
            name: tmp[0],
            company: Number.parseInt(tmp[1]),
            year: Number.parseInt(tmp[3]),
            month: Number.parseInt(tmp[4]),
            day: Number.parseInt(tmp[5])
        }
        if (tmp[1] !== tmp[2]) {
            tmpIndex.campaign = tmp[2];
        }
        return tmpIndex
    }
    return null;
}

var transormHitToBulk = function(req, index, hit) {
    let nmKey = decomposeNmKey(hit._source.nmKey)
    hit._source.action_type = hit._type;
    hit._type = index.campaign !== undefined ? 'campaign_' + index.campaign : "notification_" + index.company
    hit._source.old_index = index.name;
    hit._source.id_company = index.company;
    if (hit._type === 'campaign_injection_event') {
        if (nmKey.recipient)
            updateCustomField(index, hit, nmKey.recipient, nmKey.media)
    } else {
        for (let field in hit._source) {
            if (field.toUpperCase() === field) {
                hit._source['test_' + index.campaign + "_" + field] = hit._source[field];
                delete hit._source[field];
            }
        }
    }
}

module.exports = async function (req, type, selection, command) {

    function defineIndexSearch() {
        let index;
        switch (type) {
            case "month":
                selection = checkMonth(selection);
                if (selection) {
                    index = "events_*_*_" + req.year + "-" + selection + '*';
                }
                break;
            case "year":
                // TODO NWY
                if (checkInt(selection))
                    index = "events_*_*_" + selection + "*";
                break;
            case "company":
                // TODO NWY
                if (checkInt(selection))
                    index = "events_*_*_" + selection + "*";
                break;
            case "campaign":
                if (checkInt(selection))
                    index = "events_*_" + selection + "*";
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
    let indexReponse = await fetchIndices(req, filter);
    req.verifyIndexPattern = 'events_[year]-[month]'    
    let filteredReportIndex = getChoosenIndexes(req, indexReponse, createCampaignInfo)
    await finallyDoTheWork(req, filteredReportIndex, transormHitToBulk)
    
}