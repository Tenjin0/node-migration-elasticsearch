module.exports  = async function (req, company, campaign) {
    var totalFound;
    var totalFoundInTmp;
    var totalRetrieved;
    
    var searchAllDocumentWithCriteria = async function (index, criteria) {
            var exist = await req.db.elasticsearch.event_cluster.indices.exists({ index: index })
            if (exist) {
                return req.db.elasticsearch.event_cluster.search({ index: index, body : criteria }).then(result => {
                    // totalFound = result.hits.total
                    return result.hits
                }).catch(error => {
                    throw(error)
                })
            }
            console.warn("je no dois pas passer par la")
    }
    
    var generateAttributeTmpForSelectedDocument = async function (datas) {
        let bulkData = [];
        var total = datas.length;
        // var total = 1;
        for (var i = 0; i < total; i++) {
            let history_click_tmp = datas[i]._source.history_click.map(historyClick => {
                if (historyClick.date && historyClick.date.date) {
                    return { name : "unknown", date : historyClick.date.date}
                } else {
                    return { name : "unknown", date : historyClick}
                    
                }
            });
            if (clusterToSend === 'local') {
                bulkData.push({ "create": { "_index" : datas[i]._index, "_type": datas[i]._type, "_id" : datas[i]._id } })
                bulkData.push({"history_click_tmp" : history_click_tmp }) 
                // bulkData.push({ "update": { "_index" : datas[i]._index, "_type": datas[i]._type, "_id" : datas[i]._id } })
                // bulkData.push({ "doc" : {"history_click_tmp" : history_click_tmp } }) 
            } else {
                bulkData.push({ "update": { "_index" : datas[i]._index, "_type": datas[i]._type, "_id" : datas[i]._id } })
                bulkData.push({ "doc" : {"history_click_tmp" : history_click_tmp } }) 
            }
        }
        console.warn('bulkData', bulkData.length/ 2)
        var result = await req.db.elasticsearch[clusterToSend].bulk({ body: bulkData }).then(result => {
            return result;
        }).catch(error => {
            throw(error)
        })
        // console.warn(util.inspect(result,true, 6));
        // return result.then();
        return result;
    }
    
    var reindexAndReplaceAttribute = async function (index) {
        var body  = {	
            "source": {
                "index": index
            },
            "dest": {
                "index": index + "_tmp"
            },
            "script" : {
                "inline": "if(ctx._source.containsKey(\"history_click_tmp\")){ ctx._source.history_click = ctx._source.history_click_tmp; ctx._source.remove(\"history_click_tmp\"); } else { ctx._source.history_click = [] ;};"
            }
        }
        var result = await req.db.elasticsearch[clusterToSend].reindex({ body: body }).then(result => {
            return result;
        }).catch(error => {
            console.warn(error)
            return error
        })
        return result
    }

    // console.warn(company, campaign);
    const indexToReindex = 'reports_' + company + "_" + campaign
    const criteria = {
        "size" : 400,
        "_source" : ['history_click'],
        "query": {
            "bool": {
                "filter": {
                    "exists": {
                        "field": "history_click"
                    }
                }
            }
        }
    }
    try {
        let data = await searchAllDocumentWithCriteria(indexToReindex, criteria);
        console.warn('totalFound', totalFound = data.total);
        console.warn('totalRetrieved', totalRetrieved = data.hits.length);
        data = data.hits
        let dataResponse = await generateAttributeTmpForSelectedDocument(data);
        if (dataResponse.errors) {
            return console.warn(dataResponse.items);
        }
        let result = await reindexAndReplaceAttribute(indexToReindex)
        console.warn(indexToReindex + '_tmp')
        // console.warn(result)
        if (result.failures.length > 0) {
            return console.warn(result.failures);
        }
        
        data = await searchAllDocumentWithCriteria(indexToReindex + '_tmp', criteria)
        // console.warn(data);
        console.warn('totalFoundInTmp', totalFoundInTmp = data.total);
        if (totalFoundInTmp === totalFound && totalFound === totalRetrieved) {
            console.warn('Need to delete primary index and copy tmp_index in primary index')
            await req.db.elasticsearch[clusterToSend].indices.delete({ index:  indexToReindex})
                let body = {	
                    "source": {
                        "index": indexToReindex + "_tmp"
                    },
                    "dest": {
                        "index": indexToReindex
                    }
                }
            await req.db.elasticsearch[clusterToSend].reindex({ body:  body})
            console.warn(await req.db.elasticsearch[clusterToSend].indices.exists({ index:  indexToReindex}))
            console.warn('done');

        }
    } catch(error) {
        console.warn(error);
    }
    // searchAllDocumentWithCriteria(indexToReindex, criteria, function (err, data) {
}