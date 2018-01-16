// const program = require('commander')
const program = require('yargs')
const util = require('util')
// // const mysql = require('mysql')
// var dbElasticsearch = require('../../middleware/db.elasticsearch')
const elasticsearch = require('elasticsearch');
const history_click = require('./action/history_click')
const event = require('./action/event')
const report = require('./action/report')
const recipient = require('./action/recipient')
const notification_sms = require('./action/notification_sms')
const Promise = require('bluebird');
const mysql = require('mysql');
const moment = require('moment');
const logdir = "log";
const path = require('path')
const fs = require('fs');
const { checkMonth, initBars, initReqFields } = require("./lib")

global.clusterToSend = 'event_cluster'
global.Promise = Promise;
const closeAll = false;

var req = {
    db: {
        elasticsearch: {
            local : "localhost:9200",
            recipient_cluster : "",
            event_cluster : ""
        }
    }
}
function closeDatabaseconnection(exitCode) {
    if (!closeAll && req.db.mysql) {
        req.db.mysql.end((err) => {
            console.warn('Connection mysql ended\n')
            req.db.elasticsearch.recipient_cluster.close();
            req.db.elasticsearch.event_cluster.close();
            req.db.elasticsearch.local.close();
            console.log('Goodbye!');
            process.exit(exitCode)
        });
    }
}

// Catch exit
process.on('exit', function () {
    console.warn('exit')
    closeDatabaseconnection()
});

// Catch error
process.on('error', function () {
    console.warn('error')
    closeDatabaseconnection()
});

// Catch CTRL+C
process.on('SIGINT', () => {
    console.log('\nCTRL+C...');
    closeDatabaseconnection(0)
});

// Catch uncaught exception
process.on('uncaughtException', err => {
    console.warn('uncaughtException')
    console.dir(err, { depth: null });
    closeDatabaseconnection(1)
});

process.on("SIGTERM", function () {
    console.warn('sigterm')
    closeDatabaseconnection(1)
})

var action = {
    history_click: async function (args) {
        return await history_click(req, args.company, args.campaign)
    },
    event: async function (args) {
        return await event(req, args.type, args.selection, args)
    },
    report: async function (args) {
        return await report(req, args.type, args.selection, args)
    },
    notification_sms: async function (args) {
        return await notification_sms(req, args.type, args.selection, args)
    },
    recipient: async function (args) {
        return await recipient(req, args.type, args.selection, args)
    },
}

function doAction(command) {
    if (["event", "report", "notification_sms", "recipient"].indexOf(command._[0]) >= 0) {
        initBars(req, command)
        initReqFields(req, command)
    }
    if (command.type && command.type === 'month' && command.selection) {
        command.selection = checkMonth(command.selection)
    }
    if (action[command._[0]]) {
        let opts = {
            protocol: 'mysql',
            database: 'si',
            debug: command.debug
        }
        switch (command.origin) {
            case 'local':
                opts.host = 'localhost';
                opts.user = 'root';
                opts.password = 'yoda';
                req.db.elasticsearch.recipient_cluster = "localhost:9200"
                req.db.elasticsearch.event_cluster = "localhost:9200"
                break;
            case 'preprod':
                opts.host = 'preprod01';
                opts.user = 'v3';
                opts.password = 'V3/1215%';
                req.db.elasticsearch.recipient_cluster = "10.130.1.130:9200"
                req.db.elasticsearch.event_cluster = "10.130.1.130:9200"
                break;
            case 'prod':
                opts.host = 'mysql-sbg-01.paris08-nm.com';
                opts.user = 'v3';
                opts.password = '6!7jh%9';
                req.db.elasticsearch.recipient_cluster = 'web-rbx-01.paris08-nm.com:9200'
                req.db.elasticsearch.event_cluster = 'web-rbx-01.paris08-nm.com:9201'
                break;
            default:
                break;
        }

        req.db = {
            elasticsearch: {
                local: new elasticsearch.Client({
                    host: req.db.elasticsearch.local,
                    log: 'error'
                }),
                recipient_cluster: new elasticsearch.Client({
                    host: req.db.elasticsearch.recipient_cluster,
                    log: 'error'
                }),
                event_cluster: new elasticsearch.Client({
                    host: req.db.elasticsearch.event_cluster,
                    log: 'info'
                })
            }
        }
        req.db.mysql = mysql.createConnection(opts)
        req.db.mysql.connect((err) => {
            if (err) {
                console.log('Error connecting to Db');
                return;
            }
            console.log('Connection mysql established\n');
        });
        action[command._[0]](command)
            .then(() => {
                process.exit(0)
            })
            .catch(err => {
                closeDatabaseconnection(1)
                if (err.message === 'verification_failed') {
                    var todayDate = moment().format('YYYY-MM-DD_HH-mm-ss')
                    var pathRequestsErrorJson = path.resolve(__dirname, logdir, todayDate + '_' + req.commandName + '.json');
                    fs.writeFileSync(pathRequestsErrorJson, JSON.stringify(err.errors, null, 2))
                    console.warn('\nSee log :', pathRequestsErrorJson, '\n');
                } else {
                    console.warn(err);
                }
            })
    } else {
        console.warn('No Function assigned with this command')
    }
}

program
    .usage('$0 <cmd> [args]')
    .command('event [type] [selection]', 'change cluster_event event indexes to monthly events. type in(month, campaign, index)', (yargs) => {
        yargs.positional('type', {
            type: 'string',
            choices: ['month', 'campaign', 'index'],
            default: 'month',
            describe: 'define the type of indexes search (monthly, by campaign, by index'
        })
        yargs.positional('selection', {
            type: 'string',
            default: 'current',
            describe: 'define the selection. Depends on type. For month choose current or [1..12]. For campaign choose id. For index the index name'
        })
    }, function (argv) {
        doAction(argv)
    })
    .command('report [type] [selection]', 'change cluster_event report indexes to monthly events. type in(month, campaign, index)', (yargs) => {
        // console.warn(yargs)
        yargs.positional('type', {
            type: 'string',
            choices: ['month', 'campaign', 'index'],
            default: 'month',
            describe: 'define the type of indexes search (monthly, by campaign, by index'
        })
        yargs.positional('selection', {
            type: 'string',
            default: 'current',
            describe: 'define the selection. Depends on type. For month choose current or [1..12]. For campaign choose id. For index the index name'
        })
    }, function (argv) {
        doAction(argv)
    })
    .command('notification_sms [type] [selection]', 'change cluster_event notification_sms indexes to monthly events. type in(month, index)', (yargs) => {
        // console.warn(yargs)
        yargs.positional('type', {
            type: 'string',
            choices: ['month', 'index'],
            default: 'month',
            describe: 'define the type of indexes search (monthly, by index'
        })
        yargs.positional('selection', {
            type: 'string',
            default: 'current',
            describe: 'define the selection. Depends on type. For month choose current or [1..12]. For index the index name'
        })
    }, function (argv) {
        doAction(argv)
    })
    .command('recipient [type] [selection]', 'change cluster_event notification_sms indexes to monthly events. type in(month, index)', (yargs) => {
        yargs.positional('type', {
            type: 'string',
            choices: ['month', 'index'],
            default: 'month',
            describe: 'define the type of indexes search (monthly, by index)'
        })
        yargs.positional('selection', {
            type: 'string',
            default: 'current',
            describe: 'define the selection. Depends on type. For month choose current or [1..12]. For index the index name'
        })
    }, function (argv) {
        doAction(argv)
    })
    .option('checkonly', {
        alias: 'c',
        describe: 'onlycheck the output index',
        default: false

    })
    .option('prod', {
        alias: 'p',
        describe: 'create output index in [orgin] server else locally',
        default: false

    })
    .option('origin', {
        alias: 'o',
        describe: 'choose environnemnt',
        choices: ['local', 'preprod', 'prod'],
        default: 'local'

    })
    .option('from', {
        alias: 'f',
        describe: 'start at [int] index',
        default: 0,
        type: 'integer'

    })
    .option('limit', {
        alias: 'l',
        describe: 'stop after [int] index',
        default: 0,
        type: 'integer'

    })
    .option('debug', {
        alias: 'd',
        describe: 'debug mode (desactivate progress bar, activate sql debug mode)',
        default: false
    })
    .option('year', {
        alias: 'y',
        describe: 'change the year',
        default: 2017
    })
    .help()
    .argv

program.parse(process.argv)
if (process.argv.length < 3) {
    console.warn('rework.js --help for the list of the commands')
}