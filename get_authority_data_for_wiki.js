#!/usr/bin/env/node

var request = require('request');
var merge = require('merge');
var async = require('async');

var handle_err = function handle_err(err, cb) {
    if (err) {
        console.log(err);
        process.exit()
    }
}

var values = function values(obj) {
    var result = [];
    for (var key in obj) {
        result.push(obj[key]);
    }
    return result;
}

var api_url;

var wid = process.argv[2];

var get_all_revisions = function get_all_revisions(title_object, cb, all_revisions, rvstartid, err) {
    handle_err(err);

    all_revisions = all_revisions || [];
    var qs = {
        action: 'query',
        prop: 'revisions',
        titles: title_object.title,
        rvprop: 'ids|user|userid',
        rvlimit: 'max',
        rvdir: 'newer',
        format: 'json'
    };
    if (rvstartid) {
        qs.rvstartid = rvstartid;
    }
    request({url: api_url, json: true, qs: qs}, function(err, response, body) {
        if (err) {
            console.log(err);
            return [];
        }

        var pages = ((body.query || {}).pages || {0: {}});
        if ( title_object.pageid in pages) {
            all_revisions = all_revisions.concat(pages[title_object.pageid].revisions)
        }
        if ('query-continue' in response) {
            return get_all_revisions(title_object, cb, all_revisions, body['query-continue'].revisions.rvstartid, err);
        }
        var result = {}
        result[title_object.title] = all_revisions;
        return cb(err, result);
    });
};

var get_all_titles = function get_all_titles(props, cb, allpages, err) {
    handle_err(err);

    props = props || {};
    allpages = allpages || [];

    request({
        url: api_url,
        json: true,
        qs: merge({action: 'query',
               aplimit: 500,
               list: 'allpages',
               apfilterredir: 'nonredirects',
               format: 'json'},
            props)
        }, function(err, response, body) {
        if (err) {
            console.log(err);
            return [];
        }
        allpages = allpages.concat(((body.query || {}).allpages) || []);
        if ('query-continue' in body) {
            return get_all_titles({apfrom: body['query-continue']['allpages']['apfrom']}, cb, allpages, err);
        }
        console.log('got titles');
        return cb(allpages);
    });
};

var with_wikidata = function with_wikidata_response(err, response, body) {
    handle_err(err);

    api_url = body.items[wid].url + '/api.php';

    console.log('getting all titles');
    get_all_titles({}, function(all_pages){
        console.log(all_pages.length + ' titles');
        console.log('getting all revisions');
        async.map(all_pages, get_all_revisions, function(err, results) {
            handle_err(err);

            var composed = results.reduce(function(a, b) {
                for (var key in b) {
                    a[key] = b[key];
                }
                return a;
            });
            var total_revs = values(composed).map(function(a){return a.length;}).reduce(function(a,b){return a+b;});
            console.log(total_revs + ' revisions');
            process.exit();
        });
   });
}

console.log('starting');
(function() {
    return request({
        url: 'http://www.wikia.com/api/v1/Wikis/Details',
        qs: {'ids': wid},
        json: true
    }, with_wikidata);
})();

