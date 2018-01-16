const _progress = require('cli-progress');
const _colors = require('colors');
const util = require('util');
const fs = require('fs');
const logdir = "log";
const moment = require('moment');
const path = require('path')
const sharedCustomFields = ["SMS", "MOBILE", "PORTABLE"]; //Voir la config pour la liste
const regexIndexStoreIn = /\[(\w+)\]/g;

const decomposeNmKey = function (nmKey) {
    let tmp = nmKey.split('-_-');
    tmp = {
        recipient: tmp[tmp.length - 2],
        media: tmp[tmp.length - 1]
    }
    if (tmp.recipient === 'undefined') tmp.recipient = undefined;
    return tmp
}

const initBars = function (req, command) {
    var formatReindex = 'progress' + _colors.blue(' {bar}') + ' : {percentage}% | {value}/{total} | ' + (command.checkonly ? 'Retrieved index' : 'Current reindex') + ': {index}';
    req.reindexBar = new _progress.Bar({
        format: formatReindex
    }, _progress.Presets.shades_classic);
    var formatVerif = 'progress' + _colors.green(' {bar}') + ' : {percentage}% | {value}/{total} | Current check : {index}';
    if (!req.debug)
        req.verificationBar = new _progress.Bar({
            format: formatVerif
        }, _progress.Presets.shades_classic);
}

const checkMonth = function (monthString) {
    var month = Number.parseInt(monthString)
    if (Number.isNaN(month)) {
        month = null;
        if (monthString === 'current') {
            month = moment().month() + 1
        }
    } else if (month < 1 && month > 12) {
        month = null
    }
    return month
}

const initReqFields = function initReqFields(req, command, resultLength) {
    req.commandName = command._[0]
    req.type = command.type
    req.prod = command.prod
    req.year = command.year
    req.debug = command.debug ? command.debug : false
    req.checkonly = command.checkonly
    req.from = !command.from ? 0 : command.from
    req.notif = command.notif
    req.limit = !command.limit || command.limit < 1 ? 0 : command.limit
    req.cluster = req.prod ? "event_cluster" : 'local'
    req.from = req.type === 'index' || req.type === 'campaign' ? req.from = 0 : req.from
    req.verifyIndex = [];    
}

function generateStoreInIndex(req, index) {
    let result;
    if (!req.verifyIndexPattern)
        throw new Error('No verify index pattern')
    index.storeIn = req.verifyIndexPattern.replace(regexIndexStoreIn, function(...args) {
        if (args && args[1] && index[args[1]]) {
            if (args[1] === 'month') {
                return index[args[1]].toLocaleString('en-US', {minimumIntegerDigits: 2, useGrouping:false})
            } else {
                return  index[args[1]];
            }
        }
        return ""
    })
    
    updateVerifyIndex(req, index.storeIn)
}

const getChoosenIndexes = function (req, allIndexes, createCampaignInfo, campaigns) {
    let filteredIndex = [];
    let countIndexOpenChoosen = 0;
    for (let i = 0; i < allIndexes.length; i++) {
        if (allIndexes[i].length > 0) {
            let line = allIndexes[i].split(/[ ]+/);
            var indexSelected;
            if (line[0] === 'open' && (indexSelected = createCampaignInfo(line[1], campaigns))) {
                if (req.notif && indexSelected.campaign === undefined || !req.notif) {
                    countIndexOpenChoosen++
                    indexSelected.treated = 0;
                    indexSelected.scrolled = 0;
                    indexSelected.errors = []
                    indexSelected.docs_count = Number.parseInt(line[2]);
                    generateStoreInIndex(req, indexSelected)

                    if (req.checkonly) {
                        indexSelected.treated = indexSelected.scrolled = indexSelected.docs_count;
                    }
                    if (countIndexOpenChoosen > req.from && filteredIndex.length < req.limit) {
                        filteredIndex.push(indexSelected);
                    }
                }
                if (countIndexOpenChoosen === req.limit) {
                    break
                }
            }
        }
    }
    return filteredIndex;
}


const updateCustomField = function(index, hit, recipient) {
    let customFields = index.list[recipient];
    if (customFields.length > 0) {
        for (let i = 0; i < customFields.length; i++) {
            if (hit._source[customFields[i]] !== undefined) {
                hit._source['list_' + recipient + "[" + customFields[i] + ']'] = hit._source[customFields[i]];
                delete hit._source[customFields[i]];
            }
        }
    }
}

const reindexAll = async function(req, indexes, transormHitToBulk) {
    var reindexDatas = async function(index, hits) {
        let bulkData = [];
        let i = 0;
        for (; i < hits.length; i++) {
            transormHitToBulk(req, index, hits[i])
            bulkData.push({ "index": { "_index": index.storeIn, "_type": hits[i]._type, "_id": hits[i]._id } })
            bulkData.push(hits[i]._source)
        }
        let response = await req.db.elasticsearch[req.cluster].bulk({ body: bulkData });
        index.treated += response.items.length;
        return response
    }

    console.time('reindex report time')
    let i = 0;
    let index;
    var total = indexes.length;
    let size = 1000; // a mettre dans une config et pouvoir changer via option
    req.reindexBar.start(total, i, {
        index: "N/A"
    });
    for (; i < indexes.length; i++) {
        index = indexes[i];
         if (!req.debug) 
        req.reindexBar.update(i, {
            index: index.name
        });
        let fetchDocuments = req.db.elasticsearch[req.originCluster].search({
            index: index.name,
            scroll: '10s',
            "size": size,
            body: {
                query: {
                    "match_all": {}
                }
            }
        })
        if(index.campaign || index.recipient) {
            var [docResponse] = await Promise.all([
                fetchDocuments,
                fetchCustomFields(req, index),
            ]);
        } else {
            var docResponse = await fetchDocuments
        }
        let total = docResponse.hits.total
        let scrollId = docResponse._scroll_id;
        let countScroll = docResponse.hits.hits.length;
        index.scrolled = countScroll;
        if (total !== 0) {
            await reindexDatas(index, docResponse.hits.hits)
        }
        while (countScroll < total) {
            let scroll = await req.db.elasticsearch[req.originCluster].scroll({
                scrollId: scrollId,
                "size": size,
                scroll: '10s'
            })
            countScroll += scroll.hits.hits.length
            index.scrolled = countScroll;
            
            await reindexDatas(index, scroll.hits.hits)
        }
        
    }
    if (!req.debug) 
        req.reindexBar.update(i, {
            index: index.name
        });
    if (!req.debug)             
        req.reindexBar.stop()
    console.timeEnd('reindex report time')
}

const updateHistoryClick = function (hit) {
    if (hit.history_click && hit.history_click.length > 0) {
        for (let i = 0; i < hit.history_click.length; i++) {
            let history_click = hit.history_click[i];
            if (typeof history_click === 'string') {
                hit.history_click[i] = { name: 'unknown', date: history_click }
            }
        }
    }
}

const verifyReindexes = async function (req, indexes, customVerification) {
    const searchInBucket = function (indexName, buckets) {
        for (let i = 0; i < buckets.length; i++) {
            const bucket = buckets[i];
            if (bucket.key === indexName) {
                return bucket;
            }
        }
        return null;
    }

    let body = {
        "size": 0,
        "aggs": {
            "count_by_type": {
                "terms": {
                    "field": "old_index"
                }
            }
        }
    }
    console.time('verify reindex time')    

    body.aggs.count_by_type.terms.size = 0;
    if (customVerification && customVerification.aggs)
        body.aggs.count_by_type.aggs = customVerification.aggs

    // console.warn(util.inspect(body, false, 10));
    if (!req.debug)
        req.verificationBar.start(indexes.length, 0, {
            index: indexes[0].name
        })
    let response = await req.db.elasticsearch[req.cluster].search(
        {
            index: req.verifyIndex,
            "size": 0,
            body: body
        });
    let errors = {
        not_found: [],
        incoherent_data: [],
        total: 0
    };
    let buckets = response.aggregations.count_by_type.buckets;
    let validCount = 0;
    
    for (let i = 0; i < indexes.length; i++) {
        let indexToCheck = indexes[i];
        
        let bucket = searchInBucket(indexToCheck.name, buckets)
        if (!req.debug)
            req.verificationBar.update(validCount, {
                index: indexToCheck.name
            })
        if (bucket === null) {
            if (indexToCheck.treated === 0) {
                if (!req.debug)
                    req.verificationBar.update(++validCount)
                indexToCheck.check = "Ok"
            } else {
                errors.not_found.push(indexToCheck.name);
                errors.total++

            }
        } else {
            if (bucket.doc_count !== indexToCheck.treated) {
                let error = { index: bucket.key, found_in_reindex: bucket.doc_count, found_in_origin: indexToCheck.treated }
                errors.incoherent_data.push(error);
                errors.total++
                indexToCheck.check = "Ko"
                indexToCheck.errors.push(error)
            } else {
                if (!req.debug)
                    req.verificationBar.update(++validCount)
                indexToCheck.check = "Ok"
            }
            if (customVerification && customVerification.checkAgg) {
                customVerification.checkAgg(bucket, indexToCheck);
            }
        }
    }
    if (!req.debug)             
        req.verificationBar.stop()
    console.timeEnd('verify reindex time')    
    if (errors.total > 0) {
        let error = new Error('verification_failed')
        error.errors = errors;
        throw error
    }
}

const checkInt = function (arg) {
    var tmp = Number.parseInt(arg)
    if (Number.isNaN(tmp)) {
        return null;
    }
    return true
}

const updateVerifyIndex = function(req, indexStore) {
    
    if (req.verifyIndex.indexOf(indexStore) < 0) {
        req.verifyIndex.push(indexStore);
    }
}

const fetchIndices = async function(req, filter) {
    if (!req.originCluster)
        req.originCluster = 'event_cluster'
    let result = await req.db.elasticsearch[req.originCluster].cat.indices({ h: "status,index,docs.count", index: filter });
    result = result.split('\n');
    if(req.type === 'month') {
        req.totalIndex = result.length;
    } else {
        req.totalIndex = 10000;
    }
    if (req.type === 'index' || req.type === 'campaign') {
        req.limit = 1;
    } else if( req.limit > result.length || req.limit === 0) {
        req.limit = result.length
    }
    return result
}

const fetchCustomFields = async function (req, index) {
    return new Promise((resolve, reject) => {
        var thenFetchCustomFields = function() {
            let iILIds = Object.keys(index.list);
            let sql = 'SELECT id_import_list_information, name FROM si.import_list_custom_fields where id_import_list_information IN (?) and name not like \'extranet_%\' order by id_import_list_information';
            req.db.mysql.query(sql, [iILIds.join(',')], function (err, customFieldRows) {
                if (err) {
                    console.warn('callback: sql', err);
                    return reject(err);
                }
                for (let j = 0; j < customFieldRows.length; j++) {
                    // if (sharedCustomFields.indexOf(customFieldRows[j].name) < 0) {
                        index.list[customFieldRows[j].id_import_list_information].push(customFieldRows[j].name);
                    // }
                }
                return resolve(index);
            })
        }
        if (!index.list) {
            req.db.mysql.query('SELECT lists_data from si.campaign where id = ' + index.campaign + ' AND si.campaign.relationship_with_parent != \'TEST\'', function (err, data) {
                if (err) {
                    console.warn('callback: sql', err);
                    return reject(err);
                }
                index.list = {}
                var idIIL = [];
                if (data.length === 0) {
                    return resolve();
                }
                if (Array.isArray(data) && data.length > 0 && data[0].lists_data && typeof data[0].lists_data === 'string') {
                    lists_data = JSON.parse(data[0].lists_data)
                    for (let i = 0; i < lists_data.length; i++) {
                        idIIL.push(lists_data[i].id);
                        index.list[lists_data[i].id] = [];
                    }
                    thenFetchCustomFields()
                } else {
                    return reject(new Error('no lists_data found'))
                }
            })
        } else {
            thenFetchCustomFields()
        }
    });
}

var timeout = function(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

var finallyDoTheWork = async function(req, filteredReportIndex, transormHitToBulk, customVerify) {
    function defineVerifindex() {
        if (req.checkonly) {
            for (let i = 0; i < filteredReportIndex.length; i++) {
                let index = filteredReportIndex[i];
                updateVerifyIndex(req, index)
            }
        }
    }
    if (filteredReportIndex.length > 0) {
        if (!req.checkonly) {
            await reindexAll(req, filteredReportIndex, transormHitToBulk)
        }
        await timeout(1000);    
        await verifyReindexes(req, filteredReportIndex, customVerify);
        if (!req.debug)
            req.verificationBar.stop();
    } else {
        throw new Error('No indexes retrieved')
    }
}
module.exports = {
    initBars,
    initReqFields,
    verifyReindexes,
    updateCustomField,
    decomposeNmKey,
    reindexAll,
    getChoosenIndexes,
    fetchIndices,
    checkMonth,
    checkInt,
    updateHistoryClick,
    fetchCustomFields,
    finallyDoTheWork,
    generateStoreInIndex
}  