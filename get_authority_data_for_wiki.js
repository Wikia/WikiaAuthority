#!/usr/bin/env/node

var request = require('request');
var merge = require('merge');
var async = require('async');

var wid = process.argv[2];




(function() {
    return request({url: 'http://www.wikia.com/api/v1/Wikis/Details', qs: {'ids': wid}, json: true}, function(err, response, body) {
        if (err) {
        console.log(err);
        return;
        }
        var wiki_data = body['items'][wid];
        var api_url = wiki_data['url'] + '/api.php';
        var get_all_titles = function get_all_titles(props, cb, allpages, err) {    
        if (err) {
            console.log(err);
            return [];
        }
        props = props || {};
        allpages = allpages || [];
        
        var res = request({
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
            return cb(allpages);
            });    
        };


        var get_all_revisions = function get_all_revisions(title_object, cb, all_revisions, rvstartid, err) {
        if (err) {
            console.log(err);
            return [];
        }
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
        request({url: api_url,
             json: true,
             qs: qs},
            function(err, response, body) {
            if (err) {
                console.log(err);
                return [];
            }
            var pages = ((body.query || {}).pages || {0: {}});
            for (revision in pages) {
                all_revisions = all_revisions.concat(pages[revision])
            }
            if ('query-continue' in response) {
                return get_all_revisions(title_object, cb, all_revisions, body['query-continue'].revisions.rvstartid, err);
            }
            var result = {}
            result[title_object.title] = all_revisions;
            console.log(result);
            return cb(err, result);
            });
        };

        get_all_titles({}, function(all_pages){
            var result = async.map(all_pages, get_all_revisions, function(err, results) {
                if (err) {
                console.log(err);
                }
                var composed = {};
                results.map(function(result) {
                    for (key in result) {
                    composed[key] = result[key];
                    }
                });
            });

        });
        
    });
})();

