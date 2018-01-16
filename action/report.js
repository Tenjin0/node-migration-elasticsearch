var util = require('util');
const { initBars, initReqFields, reindexAll, verifyReindexAll, getChoosenIndexes, fetchIndices, checkMonth, checkInt, fetchCustomFields, decomposeNmKey, updateCustomField, finallyDoTheWork, updateHistoryClick } = require('../lib');

function createCampaignInfo(index, campaigns) {
    var foundCampaignById = function (id) {
        for (let i = 0; i < campaigns.length; i++) {
            // console.warn(typeof campaigns[i].id, typeof id)
            if (campaigns[i].id === id)
                return campaigns[i];
        }
        return null
    }
    let tmp = new RegExp(/reports_([0-9]+)_([0-9]+)/).exec(index);
    let campaign = null;
    tmp[2] = Number.parseInt(tmp[2])
    if (tmp !== null && (campaign = foundCampaignById(tmp[2])) !== null) {
        tmpIndex = {
            name: tmp[0],
            company: Number.parseInt(tmp[1]),
            campaign: tmp[2],
            year: campaign.year,
            month: campaign.month,
            day: campaign.day,
            media: campaign.media.toLowerCase(),
            list: {}
        }
        if (campaign.lists_data) {
            if (typeof campaign.lists_data === 'string') {
                campaign.lists_data = JSON.parse(campaign.lists_data);
            }
            for (let i = 0; i < campaign.lists_data.length; i++) {
                tmpIndex.list[campaign.lists_data[i].id] = [];
            }
        }
        return tmpIndex
    }
    return null;
}

function transormHitToBulk(req, index, reportDoc) {
    let nmKey = decomposeNmKey(reportDoc._source.nmKey)
    reportDoc._source.old_index = index.name;
    reportDoc._source.id_company = index.company
    if (nmKey.recipient)
        updateCustomField(index, reportDoc, nmKey.recipient)
    updateHistoryClick(reportDoc._source)
    // index.media = nmKey.media.toLowerCase()
}

var getAllCampaignsByFilter = async function (req, type, selection, year) {
    return new Promise((resolve, reject) => {

        let sql = "SELECT id, month(campaign_start_date) as month, year(campaign_start_date) as year, day(campaign_start_date) as day, lists_data, media from si.campaign where ";
        let sqlVars = []
        switch (type) {
            case "month":
                sql += "month(campaign_start_date) = ? and year(campaign_start_date) = ?"
                sqlVars = [selection, year]
                break;
            case "company":
                sql += "id_company = ?"
                sqlVars = [selection]
                break;
            case "index":
                sql += "id = ?"
                let idCampaign = selection.split("_")[2]
                sqlVars = [idCampaign]
                break;
            case "campaign":
                sql += "id = ?"
                sqlVars = [selection]
                break;
            default:
                break;
            sql += " and status not IN('INVALID', 'VALID', 'CANCELLED') order by id"
        }
        req.db.mysql.query(sql, sqlVars, function (err, monthlyCampaignIds) {
            if (err) return reject(err);
            resolve(monthlyCampaignIds)
        });
    })
}

module.exports = async function (req, type, selection, command) {
    function defineIndexSearch() {
        let index;
        switch (type) {
            case "month":
                index = 'reports_*'
                break;
            case "company":
                // TODO NWY
                if (checkInt(selection))
                    index = "reports_" + selection + "_*";
                break;
            case "campaign":
                if (checkInt(selection))
                    index = "reports_*_" + selection;
                break;
            case "index":
                index = selection;
                break;
            default:
                throw new Error('type_unkonwn')
                break;
        }
        return index
    }
    let filter = defineIndexSearch()
    const [indexResponse, campaignsResponse] = await Promise.all([
        fetchIndices(req, filter),
        getAllCampaignsByFilter(req, type, selection, command.year)
    ]);
    req.verifyIndexPattern = 'reports_[media]_[year]-[month]'    
    let filteredReportIndex = getChoosenIndexes(req, indexResponse, createCampaignInfo, campaignsResponse)
    await finallyDoTheWork(req, filteredReportIndex, transormHitToBulk)
    
}    